(ns me.untethr.nostr.fulfill
  "Note: we protect concurrent read/writes to Registry via atom, but we assume
   that within the scope of a channel-id/websocket, access is always serial so
   that submit!, cancel! etc are never concurrently invoked for the same
   channel-id."
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [next.jdbc :as jdbc]
    [me.untethr.nostr.common.metrics :as metrics]
    [me.untethr.nostr.query.engine :as engine]
    [me.untethr.nostr.util :as util]
    [next.jdbc.result-set :as rs])
  (:import (com.google.common.util.concurrent ThreadFactoryBuilder)
           (java.lang Thread$UncaughtExceptionHandler)
           (java.util.concurrent CompletableFuture Executors ExecutorService Future)
           (java.util.function Function)))

(def ^:private batch-size 100)
(def ^:private num-fulfillment-threads 1)
;; ultimately would like to serve all fulfillments -- larger fulfillments from
;; via different process, perhaps. at least we'd like to move later-stages of
;; larger fulfillments to a lower priority lane (e.g., when serving records
;; from prior to today or last N days etc)
;; NOTE: if we update this threshold, we'll want to consider effect in combo with
;; setMaxOutgoingFrames; ie, we'll want to throttle fulfillment based on
;; outstanding sends that haven't received WriteCallback yet etc.
(def ^:private max-fulfillment-rows-per-filter 1000)

(defrecord FulfillHandle
  [^Future fut cancelled?-vol])

(defrecord Registry
  [channel-id->sids
   sid->future-handle])

(defn create-pool
  ^ExecutorService []
  (Executors/newFixedThreadPool num-fulfillment-threads
    (.build
      (doto (ThreadFactoryBuilder.)
        (.setDaemon true)
        (.setNameFormat "fulfillment-%d")
        ;; uncaughtExceptionHandler not working when used in context of executor
        #_(.setUncaughtExceptionHandler
          (reify Thread$UncaughtExceptionHandler
            (^void uncaughtException [_this ^Thread _th ^Throwable t]
              (log/error t "uncaught exeception in fulfillment thread"))))))))

(defonce ^ExecutorService global-pool (create-pool))

(defn create-empty-registry
  []
  (->Registry {} {}))

(defn- fulfill-entirely!
  [metrics db-cxns channel-id req-id cancelled?-vol filters table-max-ids observer completion-callback]
  (try
    (let [tally (volatile! 0)]
      (metrics/time-fulfillment! metrics
        (let [overall-limit 1000
              q (engine/active-filters->query
                  (mapv #(engine/init-active-filter
                           (cond-> %
                             (contains? % :limit) (update :limit min overall-limit))
                           :page-size overall-limit
                           :table-target-ids table-max-ids) filters))
              page-of-ids (vec
                            (distinct ;; if we have multiple filters there may be dupes in the page (see notes below)
                              (map :event_id
                                (jdbc/execute! (:readonly-datasource db-cxns) q
                                  {:builder-fn rs/as-unqualified-lower-maps}))))]
          ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.761/doc/getting-started#plan--reducing-result-sets
          (transduce
            ;; note: if observer throws exception we catch below and for
            ;; now call it unexpected
            (map #(when-not @cancelled?-vol
                    (vswap! tally inc)
                    (observer (:raw_event %))))
            (fn [& _]
              (when @cancelled?-vol
                (metrics/mark-fulfillment-interrupt! metrics)
                (reduced :cancelled)))
            (jdbc/plan (:readonly-kv-datasource db-cxns)
              (apply vector
                (format
                  "select raw_event from n_kv_events where event_id in (%s)"
                  (str/join "," (repeat (count page-of-ids) "?")))
                page-of-ids)))))
      (when-not @cancelled?-vol
        (completion-callback)
        (metrics/fulfillment-num-rows! metrics @tally)))
    (catch Exception e
      (metrics/mark-fulfillment-error! metrics)
      (log/error e "unexpected in fulfill-entirely!"
        {:channel-id channel-id :req-id req-id :filters filters}))))

(defn synchronous!!
  "In most cases we'll want to use `submit!` fn for asynchronous fulfillment. However,
   upstream caller/s may wish to perform synchronous fulfillment for certain requests that
   are expected to answer quickly. Use with care!"
  [metrics db-cxns channel-id req-id filters table-max-ids observer eose-callback]
  (let [cancelled?-vol (volatile! false)]
    (fulfill-entirely! metrics db-cxns channel-id req-id cancelled?-vol filters table-max-ids observer eose-callback)))

(defn- add-to-registry!
  [fulfill-atom channel-id sid ^Future fut cancelled?-vol]
  (try
    (swap! fulfill-atom
      (fn [registry]
        ;; significant: we track futures so we can cancel them in case of
        ;; abrupt subscription cancellations, but we don't actively remove
        ;; them from our registry when fulfillment is complete; `cancel!`
        ;; et al is expected to purge them from registry so we won't have
        ;; leaked memory when websockets close; otherwise we are completely
        ;; okay for them to stick around as zombies while corresponding
        ;; subscription is still alive.
        (-> registry
          (update-in [:channel-id->sids channel-id] (fnil conj #{}) sid)
          (assoc-in [:sid->future-handle sid] (->FulfillHandle fut cancelled?-vol)))))
    (catch Exception e
      (log/error e "unexpected in update-registry!")
      (vreset! cancelled?-vol true)
      (.cancel fut false)
      (throw e))))

(defn submit!
  ^Future [metrics db-cxns fulfill-atom channel-id req-id filters table-max-ids observer eose-callback]
  (let [sid (str channel-id ":" req-id)
        cancelled?-vol (volatile! false)
        f (.submit global-pool
            (common/wrap-runnable-handle-uncaught-exc
              "fulfill/submit!"
              (partial fulfill-entirely! metrics db-cxns channel-id req-id
                cancelled?-vol filters table-max-ids observer eose-callback)))]
    (add-to-registry! fulfill-atom channel-id sid f cancelled?-vol)
    f))

(defn- internal-fulfill-one-batch!
  [metrics db-cxns active-filters observer cancelled?-vol]
  ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.761/doc/getting-started#plan--reducing-result-sets
  ;; consider alternative: look into if we could somehow truly batch websocket events
  ;; back to clients?
  (metrics/time-fulfillment! metrics
    (let [q-results (engine/execute-active-filters
                      (:readonly-datasource db-cxns) active-filters)
          page-stats (engine/calculate-page-stats q-results)
          ;; note when we have multiple filters we might produce duplicate ids
          ;; across pages ... at least for each batch we will de-duplicate here
          ;; plus we use (count page-of-ids) to manage quota so we need this
          ;; distinct here:
          page-of-ids (vec (distinct (map :event_id q-results)))]
      (transduce
        ;; note: if observer throws exception we catch below and for
        ;; now call it unexpected
        (map #(when-not @cancelled?-vol
                (observer (:raw_event %))))
        (fn
          ([_]
           ;; final success result
           (if @cancelled?-vol
             :cancelled
             [(engine/next-active-filters active-filters page-stats) (count page-of-ids)]))
          ([_ _]
           (when @cancelled?-vol
             (metrics/mark-fulfillment-interrupt! metrics)
             (reduced :cancelled))))
        nil
        (jdbc/plan (:readonly-kv-datasource db-cxns)
          (apply vector
            (format
              "select raw_event from n_kv_events where event_id in (%s)"
              (str/join "," (repeat (count page-of-ids) "?")))
            page-of-ids))))))

(defn submit-use-batching!
  ^Future [metrics db-cxns fulfill-atom channel-id req-id filters table-max-ids observer eose-callback]
  (let [start-nanos (System/nanoTime)
        sid (str channel-id ":" req-id)
        cancelled?-vol (volatile! false)
        latest-task-future-vol (volatile! nil)
        ^CompletableFuture result-future (CompletableFuture.)
        _ (add-to-registry! fulfill-atom channel-id sid result-future cancelled?-vol)
        ^Runnable batch-fn
        (fn batch-fn [curr-active-filters iteration-num total-rows-so-far]
          (try
            (when (> iteration-num 1000)
              ;; for now... this should detect if our filter next/reduction strategy
              ;;  never reduces down to zero active filters
              (throw (ex-info "too many fulfillment iterations"
                       {:iteration-num iteration-num})))
            (let [one-batch-outcome (internal-fulfill-one-batch! metrics
                                      db-cxns curr-active-filters
                                      observer cancelled?-vol)]
              (when-not (identical? one-batch-outcome :cancelled)
                (let [[next-active-filters curr-page-size] one-batch-outcome]
                  (cond
                    (empty? next-active-filters)
                    (do
                      (eose-callback)
                      (.complete result-future :success)
                      (metrics/fulfillment-num-rows! metrics (+ total-rows-so-far curr-page-size))
                      (metrics/update-overall-fullfillment-millis! metrics
                        (util/nanos-to-millis (- (System/nanoTime) start-nanos))))
                    :else
                    (do
                      (->> (.submit global-pool
                             (common/wrap-runnable-handle-uncaught-exc
                               "fulfill/submit-use-batching!/batch-fn"
                               (partial batch-fn next-active-filters (inc iteration-num) (+ total-rows-so-far curr-page-size))))
                        (vreset! latest-task-future-vol)))))))
            (catch Throwable t
              (log/error t "during fulfill batch")
              (metrics/mark-fulfillment-error! metrics)
              (.completeExceptionally result-future t))))
        initial-active-filters (mapv #(engine/init-active-filter %
                                        :override-quota (min (or (:limit %) max-fulfillment-rows-per-filter)
                                                          max-fulfillment-rows-per-filter)
                                        :page-size batch-size
                                        :table-target-ids table-max-ids) filters)
        _first-batch-future (try
                              (->> (.submit global-pool
                                     (common/wrap-runnable-handle-uncaught-exc
                                       "fulfill/submit-use-batching!"
                                       (partial batch-fn initial-active-filters 0 0)))
                                (vreset! latest-task-future-vol))
                              (catch Exception e
                                (log/error e "failed to submit")
                                (metrics/mark-fulfillment-error! metrics)
                                (.completeExceptionally result-future e)))]
    ;; when our result-future gets cancelled, we'd like to cancel any outstanding
    ;; next batch task so that it gets freed asap from mem along with any observer
    ;; callbacks and associated channel state etc.
    (.exceptionally result-future
      (reify Function
        (apply [_this t]
          (some-> latest-task-future-vol ^Future deref (.cancel false)))))
    result-future))

(defn- cancel!* [registry channel-id sid]
  (-> registry
    (update-in [:channel-id->sids channel-id] disj sid)
    (util/dissoc-in-if-empty [:channel-id->sids channel-id])
    (update :sid->future-handle dissoc sid)))

(defn- cancel-sid!
  [fulfill-atom channel-id sid]
  (when-let [{:keys [^Future fut cancelled?-vol]} (get-in @fulfill-atom [:sid->future-handle sid])]
    (vreset! cancelled?-vol true)
    (.cancel fut false))
  (swap! fulfill-atom #(cancel!* % channel-id sid)))

(defn cancel!
  [fulfill-atom channel-id req-id]
  (let [sid (str channel-id ":" req-id)]
    (cancel-sid! fulfill-atom channel-id sid)))

(defn cancel-all!
  [fulfill-atom channel-id]
  (doseq [sid (get-in @fulfill-atom [:channel-id->sids channel-id])]
    (cancel-sid! fulfill-atom channel-id sid)))

(defn num-active-fulfillments
  [fulfill-atom]
  (let [snapshot @fulfill-atom]
    (reduce
      (fn [acc [_channel-id sids]]
        (+ acc
          (count
            (filter
              #(not (.isDone (get-in snapshot [:sid->future-handle % :fut])))
              sids))))
      0
      (:channel-id->sids snapshot))))
