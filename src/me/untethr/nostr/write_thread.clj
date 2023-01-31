(ns me.untethr.nostr.write-thread
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [me.untethr.nostr.common.domain :as domain]
    [me.untethr.nostr.common.metrics :as metrics]
    [me.untethr.nostr.common.store :as store]
    [me.untethr.nostr.util :as util]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs])
  (:import (com.google.common.util.concurrent FutureCallback Futures ListenableFuture ListeningScheduledExecutorService MoreExecutors ThreadFactoryBuilder)
           (java.sql Connection)
           (java.time Duration)
           (java.util.concurrent Executors RejectedExecutionException ScheduledExecutorService)
           (javax.sql DataSource)
           (org.sqlite SQLiteErrorCode SQLiteException)))

(defn create-single-thread-executor
  ^ScheduledExecutorService []
  (Executors/newSingleThreadScheduledExecutor
    (.build
      (doto (ThreadFactoryBuilder.)
        (.setDaemon true)
        (.setNameFormat "write-thread-%d")
        ;; uncaughtExceptionHandler not working when used in context of executor
        #_(.setUncaughtExceptionHandler
          ;; note, doesn't seem to catch for schedule tasks which simply get exceptions
          ;; swallowed and the scheduled task cancelled
          (reify Thread$UncaughtExceptionHandler
            (^void uncaughtException [_this ^Thread _th ^Throwable t]
              (log/error t "uncaught exeception in write thread"))))))))

(defonce ^ListeningScheduledExecutorService single-event-thread
  (MoreExecutors/listeningDecorator ^ScheduledExecutorService (create-single-thread-executor)))

;; --

;; note: must always use get-singleton-connection! accessors here - you never
;;  want to deadlock by going out-of-band as there is only one cxn in each
;;  write pool!

(defn get-singleton-connection!
  ^Connection [{:keys [^DataSource writeable-datasource
                       writeable-connection-singleton-atom] :as _db-cxns}]
  {:pre [(some? writeable-connection-singleton-atom)]
   :post [(instance? Connection %)]}
  (or @writeable-connection-singleton-atom
    (reset! writeable-connection-singleton-atom
      ;; worth noting this will hang if we already have checked-out a cxn.
      (.getConnection writeable-datasource))))

(defn commit-and-close!
  [^Connection cxn]
  (try
    (.commit cxn)
    (catch Throwable e
      (log/fatal e ".commit fail")))
  (try
    (.close cxn)
    (catch Throwable e
      (log/fatal e ".close fail"))))

(defn clear-singleton-connection!
  [{:keys [writeable-connection-singleton-atom] :as _db-cxns}]
  (swap! writeable-connection-singleton-atom
    (fn [curr-cxn]
      (when curr-cxn
        (commit-and-close! curr-cxn)
        nil))))

(defn get-singleton-kv-connection!
  ^Connection [{:keys [^DataSource writeable-kv-datasource
                       writeable-kv-connection-singleton-atom] :as _db-cxns}]
  {:pre [(some? writeable-kv-connection-singleton-atom)]
   :post [(instance? Connection %)]}
  (or @writeable-kv-connection-singleton-atom
    (reset! writeable-kv-connection-singleton-atom
      ;; worth noting this will hang if we already have checked-out a cxn.
      (.getConnection writeable-kv-datasource))))

(defn clear-singleton-kv-connection!
  [{:keys [writeable-kv-connection-singleton-atom] :as _db-cxns}]
  (swap! writeable-kv-connection-singleton-atom
    (fn [curr-cxn]
      (when curr-cxn
        (commit-and-close! curr-cxn)
        nil))))

;; --

(defonce next-sweep-limit-vol (volatile! 5))
(def sweep-min-limit 5)
(def sweep-max-limit 300)
(def sweep-period-seconds 1)
(def sweep-target-millis 50)

(defn schedule-sweep-job!
  [metrics db-cxns]
  ;; for now we are totally fine to not be airtight/transactional across
  ;; index and kv stores. we may have few kv orphans; we can impl something
  ;; airtight later if we care to.
  (.scheduleWithFixedDelay single-event-thread
    ^Runnable
    (fn []
      (try
        (let [start-nanos (System/nanoTime)]
          (metrics/db-sweep-limit! metrics @next-sweep-limit-vol)
          (metrics/time-purge-deleted! metrics
            (log/debug "sweeping..." {:limit @next-sweep-limit-vol})
            (when-let [to-delete-event-ids
                       (not-empty
                         (mapv :event_id
                           (jdbc/execute! (get-singleton-connection! db-cxns)
                             ["select event_id from n_events where deleted_ = 1 limit ?"
                              @next-sweep-limit-vol]
                             {:builder-fn rs/as-unqualified-lower-maps})))]
              ;; we use explicit transaction in this case and choose not to
              ;; participate in write-thread's auto-commit=false commit flushing
              ;; for other/event db updates
              (jdbc/with-transaction [tx (get-singleton-connection! db-cxns)]
                (jdbc/execute-one! tx
                  (apply vector
                    ;; triggers will delete corresponding entries from tags tables
                    (format "delete from n_events where event_id in (%s)"
                      (str/join ","
                        (apply str (repeat (count to-delete-event-ids) "?"))))
                    to-delete-event-ids)))
              (jdbc/with-transaction [tx (get-singleton-kv-connection! db-cxns)]
                (jdbc/execute-one! tx
                  (apply vector
                    (format "delete from n_kv_events where event_id in (%s)"
                      (str/join ","
                        (apply str (repeat (count to-delete-event-ids) "?"))))
                    to-delete-event-ids)))))
          (let [duration-millis (util/nanos-to-millis (- (System/nanoTime) start-nanos))]
            (vswap! next-sweep-limit-vol
              (fn [prev-limit]
                (max sweep-min-limit
                  (min sweep-max-limit
                    (int (* (/ sweep-target-millis
                              (if (zero? duration-millis) 1 duration-millis))
                           prev-limit)))))))
          (log/debug "...sweeping completed.")
          )
        (catch Throwable t
          (log/error t "while sweeping")
          ;; note: if we'd rethrow - the task would get cancelled
          )))
    (Duration/ofSeconds 30)
    (Duration/ofSeconds sweep-period-seconds)))

(def connection-recycle-period-minutes 15)

(defn schedule-connection-recycle!
  [metrics db-cxns]
  (.scheduleWithFixedDelay single-event-thread
    ^Runnable
    (fn []
      (try
        (let [_start-nanos (System/nanoTime)]
          (log/debug "recycling singleton write connections...")
          ;; we close these connections and expect hikari to close underlying and
          ;; evict if they are pass max lifetime. then we'll pick up new singleton
          ;; connections on next writes.
          (clear-singleton-connection! db-cxns)
          (clear-singleton-kv-connection! db-cxns)
          (log/debug "... done recycling singleton write connections."))
        (catch Throwable t
          (log/error t "while cxn recycling")
          ;; note: if we'd rethrow - the task would get cancelled
          )))
    (Duration/ofSeconds 60)
    (Duration/ofMinutes connection-recycle-period-minutes)))

(defonce commit-enqueued?-vol (volatile! false))
(defonce est-backlog-size-atom (atom 0))

(defn est-backlog-size ;; doesn't include outstanding scheduled tasks and some tests relay on this
  []
  @est-backlog-size-atom)

(defn add-callback!
  [^ListenableFuture fut success-fn failure-fn]
  {:pre [(fn? success-fn) (fn? failure-fn)]}
  (Futures/addCallback fut
    (reify FutureCallback
      (^void onSuccess [_this result]
        (success-fn result))
      (^void onFailure [_this ^Throwable t]
        (failure-fn t)))
    ;; for now, run listeners on our single thread executor
    single-event-thread))

(defn- db-table-locked-exception?
  [^SQLiteException e]
  (identical? SQLiteErrorCode/SQLITE_LOCKED (.getResultCode e)))

(defn- synchronous-checkpoint!
  [metrics db-cxns]
  (try
    (let [start-ns (System/nanoTime)]
      (let [{:keys [busy log checkpointed] :as checkpoint-result}
            (metrics/time-db-checkpoint! metrics
              (store/checkpoint! (get-singleton-connection! db-cxns)))]
        (log/log log/*logger-factory* "db.checkpoint" :info nil
          (str "[index] " checkpoint-result))
        (if (zero? busy)
          (metrics/mark-db-checkpoint-full! metrics)
          (metrics/mark-db-checkpoint-partial! metrics))
        (metrics/db-checkpoint-pages! metrics checkpointed))
      (log/debugf "checkpointed db (%d ms)"
        (util/nanos-to-millis (- (System/nanoTime) start-ns))))
    (catch SQLiteException e
      (cond
        (db-table-locked-exception? e)
        (log/warn "got table locked exception while checkpointing index db")
        :else (throw e)))))

(defn- synchronous-checkpoint-kv!
  [metrics db-cxns]
  (try
    (let [start-ns (System/nanoTime)]
      (let [{:keys [busy log checkpointed] :as checkpoint-result}
            (metrics/time-db-kv-checkpoint! metrics
              (store/checkpoint! (get-singleton-kv-connection! db-cxns)))]
        (log/log log/*logger-factory* "db.checkpoint" :info nil
          (str "[kv] " checkpoint-result))
        (if (zero? busy)
          (metrics/mark-db-kv-checkpoint-full! metrics)
          (metrics/mark-db-kv-checkpoint-partial! metrics))
        (metrics/db-kv-checkpoint-pages! metrics checkpointed))
      (log/debugf "checkpointed kv db (%d ms)"
        (util/nanos-to-millis (- (System/nanoTime) start-ns))))
    (catch SQLiteException e
      (cond
        (db-table-locked-exception? e)
        (log/warn "got table locked exception while checkpointing kv db")
        :else (throw e)))))

(defn- enq-commit!
  ;; note: we expect a singleton db connection and auto-commit = false
  [metrics db-cxns]
  (try
    (swap! est-backlog-size-atom inc)
    (.submit single-event-thread
      (common/wrap-runnable-handle-uncaught-exc
        "enq-commit!/0"
        (fn []
          (swap! est-backlog-size-atom dec)
          (vreset! commit-enqueued?-vol false)
          (let [use-cxn (get-singleton-connection! db-cxns)]
            (when-not (.getAutoCommit use-cxn)
              (metrics/time-commit! metrics
                (.commit use-cxn))))
          ;; we do our checkpoint along with commit - b/c we do not
          ;; want the db/tables to be locked, which would happen if we
          ;; otherwise allowed some updates/inserts to occur between
          ;; commit and checkpoint.
          (synchronous-checkpoint! metrics db-cxns)
          (let [use-cxn (get-singleton-kv-connection! db-cxns)]
            (when-not (.getAutoCommit use-cxn)
              (metrics/time-commit-kv! metrics
                (.commit use-cxn))))
          ;; we do our checkpoint along with commit - b/c we do not
          ;; want the db/tables to be locked, which would happen if we
          ;; otherwise allowed some updates/inserts to occur between
          ;; commit and checkpoint.
          (synchronous-checkpoint-kv! metrics db-cxns))))
    (catch Throwable t
      ;; ... RejectedExecutionException?
      (swap! est-backlog-size-atom dec)
      (throw t))))

(defn run-async!
  ^ListenableFuture [metrics db-cxns task-fn success-fn failure-fn]
  {:pre [(fn? task-fn) (fn? success-fn) (fn? failure-fn)]}
  (doto
    (try
      (swap! est-backlog-size-atom inc)
      (.submit single-event-thread
        (common/wrap-callable-handle-uncaught-exc
          "run-async!/0"
          (reify Callable
            (call [_this]
              (swap! est-backlog-size-atom dec)
              (task-fn
                (get-singleton-connection! db-cxns)
                (get-singleton-kv-connection! db-cxns))))))
      (catch RejectedExecutionException e
        (swap! est-backlog-size-atom dec)
        (throw (ex-info "unexpected write thread rejection" {} e))))
    ;; for now, after every write we'll *enqueue* a full checkpoint; if
    ;; there's other write work behind us our checkpoint won't delay any
    ;; of that. (consider if work patterns + this behavior have any impact
    ;; on readers and if we should have a diff. checkpoint strategy)
    ;;  disabled for now -- using autocheckpointing:
    (add-callback!
      (fn [_x]
        (when-not @commit-enqueued?-vol
          (enq-commit! metrics db-cxns)
          (vreset! commit-enqueued?-vol true)))
      (fn [_]
        ))
    (add-callback! success-fn failure-fn)))

;; --

(defn submit-continuation!
  [metrics
   db-cxns
   continuation
   maybe-finale-callback]
  (run-async!
    metrics
    db-cxns
    (fn [singleton-cxn _singleton-kv-cxn]
      (let [next-continuation
            (metrics/time-exec-continuation! metrics
              (store/continuation! singleton-cxn continuation))]
        (if-not (domain/empty-continuation? next-continuation)
          (submit-continuation! metrics
            db-cxns
            next-continuation
            maybe-finale-callback)
          (when maybe-finale-callback
            (maybe-finale-callback)))))
    (fn [_] :no-op)
    (fn [^Throwable t]
      (log/error t "while executing continuation" continuation))))

(defn submit-new-event!
  ([metrics
    db-cxns
    channel-id
    verified-event-obj
    raw-event
    duplicate-event-callback
    stored-or-replaced-callback]
   (submit-new-event!
     metrics
     db-cxns
     channel-id
     verified-event-obj
     raw-event
     duplicate-event-callback
     stored-or-replaced-callback
     nil))
  ([metrics
    db-cxns
    channel-id
    verified-event-obj
    raw-event
    duplicate-event-callback
    stored-or-replaced-callback
    maybe-finale-callback]
   (run-async!
     metrics
     db-cxns
     (fn [db-cxn db-kv-cxn]
       (let [continuation-or-terminal-result
             (metrics/time-store-event! metrics
               (store/index-and-store-event!
                 db-cxn
                 db-kv-cxn
                 channel-id
                 verified-event-obj
                 raw-event
                 ;; we'll do our own commit management on both db connections!
                 :transact? false))]
         ;; for super events with many tags we break them up into "continuation"
         ;; writes ... note that there is a question about deleted and/or replaceable
         ;; events arriving while their prior is still being processed as a continuation
         ;; b/c continuations are always inserting into tag tables, we have a trigger
         ;; on those that make sure they get inserted with the source_event_deleted_
         ;; status that matches the authoritative entry in the n_events table, which
         ;; is *always* inserted first before any others... so this should guarantee
         ;; that tag tables have the proper deleted status even in the face of
         ;; interleaving continuations...
         (if (domain/continuation? continuation-or-terminal-result)
           (if (not (domain/empty-continuation? continuation-or-terminal-result))
             (submit-continuation! metrics db-cxns continuation-or-terminal-result
               maybe-finale-callback)
             (do
               (when maybe-finale-callback
                 (maybe-finale-callback))
               :success))
           ;; terminal result:
           (do
             (when maybe-finale-callback
               (maybe-finale-callback))
             continuation-or-terminal-result))))
     (fn [store-result]
       (if (identical? store-result :duplicate)
         (duplicate-event-callback)
         ;; note: we aren't technically waiting for the db commit here (assuming
         ;; data-source is auto-commit = false and we're relying on the enq-commit
         ;; so we may want to have this callback await the commit in case we'd
         ;; like to support pure read-your-writes for clients, and risk losing
         ;; ok'd messages when racing process death eg..)
         (stored-or-replaced-callback)))
     (fn [^Throwable t]
       (log/error t "while storing event" verified-event-obj)))))
