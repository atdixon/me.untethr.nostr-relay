(ns me.untethr.nostr.write-thread
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common.domain :as domain]
    [me.untethr.nostr.common.metrics :as metrics]
    [me.untethr.nostr.common.store :as store]
    [me.untethr.nostr.util :as util]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs])
  (:import (com.google.common.util.concurrent FutureCallback Futures ListenableFuture ListeningScheduledExecutorService MoreExecutors ThreadFactoryBuilder)
           (java.time Duration)
           (java.util.concurrent Executors RejectedExecutionException ScheduledExecutorService)))

(defn create-single-thread-executor
  ^ScheduledExecutorService []
  (Executors/newSingleThreadScheduledExecutor
    (.build
      (doto (ThreadFactoryBuilder.)
        (.setDaemon true)
        (.setNameFormat "write-thread-%d")
        (.setUncaughtExceptionHandler
          (reify Thread$UncaughtExceptionHandler
            (^void uncaughtException [_this ^Thread _th ^Throwable t]
              (log/error t "uncaught exeception in write thread"))))))))

(defonce ^ListeningScheduledExecutorService single-event-thread
  (MoreExecutors/listeningDecorator ^ScheduledExecutorService (create-single-thread-executor)))

(defonce next-sweep-limit-vol (volatile! 5))
(def sweep-min-limit 5)
(def sweep-max-limit 250)
(def sweep-period-seconds 1)
(def sweep-target-millis 75)

(defn schedule-sweep-job!
  [metrics singleton-db-conn singleton-db-kv-conn]
  ;; for now we are totally fine to not be airtight/transactional across
  ;; index and kv stores. we may have few kv orphans; we can impl something
  ;; airtight later if we care to.
  (.scheduleWithFixedDelay single-event-thread
    ^Runnable
    (fn []
      (let [start-nanos (System/nanoTime)]
        (metrics/db-sweep-limit! metrics @next-sweep-limit-vol)
        (metrics/time-purge-deleted! metrics
          (when-let [to-delete-event-ids
                     (not-empty
                       (mapv :event_id
                         (jdbc/execute! singleton-db-conn
                           ["select event_id from n_events where deleted_ = 1 limit ?"
                            @next-sweep-limit-vol]
                           {:builder-fn rs/as-unqualified-lower-maps})))]
            (jdbc/execute-one! singleton-db-conn
              (apply vector
                (format "delete from n_events where event_id in (%s)"
                  (str/join ","
                    (apply str (repeat (count to-delete-event-ids) "?"))))
                to-delete-event-ids))
            (jdbc/execute-one! singleton-db-kv-conn
              (apply vector
                (format "delete from n_kv_events where event_id in (%s)"
                  (str/join ","
                    (apply str (repeat (count to-delete-event-ids) "?"))))
                to-delete-event-ids))))
        (let [duration-millis (util/nanos-to-millis (- (System/nanoTime) start-nanos))]
          (vswap! next-sweep-limit-vol
            (fn [prev-limit]
              (max sweep-min-limit
                (min sweep-max-limit
                  (int (* (/ sweep-target-millis
                            (if (zero? duration-millis) 1 duration-millis))
                         prev-limit)))))))))
    (Duration/ofSeconds 15)
    (Duration/ofSeconds sweep-period-seconds)))

(defonce checkpoint-enqueued?-vol (volatile! false))
(defonce run-async!-backlog-size-atom (atom 0))

(defn run-async!-backlog-size
  []
  @run-async!-backlog-size-atom)

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

(defn- enq-checkpoint!
  [metrics singleton-db-conn single-db-kv-conn _]
  (.submit single-event-thread
    ^Runnable
    (fn []
      (let [start-ns (System/nanoTime)]
        (let [{:keys [busy log checkpointed]} (metrics/time-db-checkpoint! metrics
                                                (store/checkpoint! singleton-db-conn))]
          (if (zero? busy)
            (metrics/mark-db-checkpoint-full! metrics)
            (metrics/mark-db-checkpoint-partial! metrics))
          (metrics/db-checkpoint-pages! metrics checkpointed))
        (log/debugf "checkpointed db (%d ms)"
          (util/nanos-to-millis (- (System/nanoTime) start-ns))))))
  (.submit single-event-thread
    ^Runnable
    (fn []
      (vreset! checkpoint-enqueued?-vol false)
      (let [start-ns (System/nanoTime)]
        (let [{:keys [busy log checkpointed]} (metrics/time-db-kv-checkpoint! metrics
                                                (store/checkpoint! single-db-kv-conn))]
          (if (zero? busy)
            (metrics/mark-db-kv-checkpoint-full! metrics)
            (metrics/mark-db-kv-checkpoint-partial! metrics))
          (metrics/db-kv-checkpoint-pages! metrics checkpointed))
        (log/debugf "checkpointed kv db (%d ms)"
          (util/nanos-to-millis (- (System/nanoTime) start-ns)))))))

(defn run-async!
  ^ListenableFuture [metrics singleton-db-conn single-db-kv-conn task-fn success-fn failure-fn]
  {:pre [(fn? task-fn) (fn? success-fn) (fn? failure-fn)]}
  (doto
    (try
      (swap! run-async!-backlog-size-atom inc)
      (.submit single-event-thread
        (reify Callable
          (call [_this]
            (task-fn singleton-db-conn single-db-kv-conn))))
      (catch RejectedExecutionException e
        (swap! run-async!-backlog-size-atom dec)
        (throw (ex-info "unexpected write thread rejection" {} e))))
    ;; for now, after every write we'll *enqueue* a full checkpoint; if
    ;; there's other write work behind us our checkpoint won't delay any
    ;; of that. (consider if work patterns + this behavior have any impact
    ;; on readers and if we should have a diff. checkpoint strategy)
    ;;  disabled for now -- using autocheckpointing:
    (add-callback!
      (fn [x]
        (swap! run-async!-backlog-size-atom dec)
        (when-not @checkpoint-enqueued?-vol
          (enq-checkpoint! metrics singleton-db-conn single-db-kv-conn x)
          (vreset! checkpoint-enqueued?-vol true)))
      (fn [_]
        (swap! run-async!-backlog-size-atom dec)))
    (add-callback! success-fn failure-fn)))

;; --

(defn submit-continuation!
  [metrics
   singleton-writeable-connection
   singleton-writeable-connection-kv
   continuation
   maybe-finale-callback]
  (run-async!
    metrics
    singleton-writeable-connection
    singleton-writeable-connection-kv
    (fn [singleton-writeable-connection
         singleton-writeable-connection-kv]
      (let [next-continuation
            (metrics/time-exec-continuation! metrics
              (store/continuation!
                singleton-writeable-connection
                singleton-writeable-connection-kv
                continuation))]
        (if-not (domain/empty-continuation? next-continuation)
          (submit-continuation! metrics
            singleton-writeable-connection
            singleton-writeable-connection-kv
            next-continuation
            maybe-finale-callback)
          (when maybe-finale-callback
            (maybe-finale-callback)))))
    (fn [_] :no-op)
    (fn [^Throwable t]
      (log/error t "while executing continuation" continuation))))

(defn submit-new-event!
  ([metrics
    singleton-writeable-connection
    singleton-writeable-connection-kv
    channel-id
    verified-event-obj
    raw-event
    duplicate-event-callback
    stored-or-replaced-callback]
   (submit-new-event!
     metrics
     singleton-writeable-connection
     singleton-writeable-connection-kv
     channel-id
     verified-event-obj
     raw-event
     duplicate-event-callback
     stored-or-replaced-callback
     nil))
  ([metrics
    singleton-writeable-connection
    singleton-writeable-connection-kv
    channel-id
    verified-event-obj
    raw-event
    duplicate-event-callback
    stored-or-replaced-callback
    maybe-finale-callback]
   (run-async!
     metrics
     singleton-writeable-connection
     singleton-writeable-connection-kv
     (fn [singleton-writeable-connection singleton-writeable-connection-kv]
       (metrics/time-store-event! metrics
         (let [continuation-or-terminal-result
               (store/index-and-store-event!
                 singleton-writeable-connection singleton-writeable-connection-kv
                 channel-id verified-event-obj raw-event)]
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
               (submit-continuation! metrics singleton-writeable-connection
                 singleton-writeable-connection-kv continuation-or-terminal-result
                 maybe-finale-callback)
               (do
                 (when maybe-finale-callback
                   (maybe-finale-callback))
                 :success))
             ;; terminal result:
             (do
               (when maybe-finale-callback
                 (maybe-finale-callback))
               continuation-or-terminal-result)))))
     (fn [store-result]
       (if (identical? store-result :duplicate)
         (duplicate-event-callback)
         (stored-or-replaced-callback)))
     (fn [^Throwable t]
       (log/error t "while storing event" verified-event-obj)))))
