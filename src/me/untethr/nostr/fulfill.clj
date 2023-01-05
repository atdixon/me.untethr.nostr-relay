(ns me.untethr.nostr.fulfill
  "Note: we protect concurrent read/writes to Registry via atom, but we assume
   that within the scope of a channel-id/websocket, access is always serial so
   that submit!, cancel! etc are never concurrently invoked for the same
   channel-id."
  (:require [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [me.untethr.nostr.common.metrics :as metrics]
            [me.untethr.nostr.query :as query]
            [me.untethr.nostr.util :as util])
  (:import (com.google.common.util.concurrent ThreadFactoryBuilder)
           (java.lang Thread$UncaughtExceptionHandler)
           (java.util.concurrent CompletableFuture Executors ExecutorService Future)
           (java.util.function Function)))

(def ^:private batch-size 500)
(def ^:private num-fulfillment-threads 1)
;; ultimately would like to serve all fulfillments -- larger fulfillments from
;; via different process, perhaps. at least we'd like to move later-stages of
;; larger fulfillments to a lower priority lane (e.g., when serving records
;; from prior to today or last N days etc)
;; NOTE: if we update this threshold, we'll want to consider effect in combo with
;; setMaxOutgoingFrames; ie, we'll want to throttle fulfillment based on
;; outstanding sends that haven't received WriteCallback yet etc.
(def ^:private max-fulfillment-rows 5000)

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
        (.setUncaughtExceptionHandler
          (reify Thread$UncaughtExceptionHandler
            (^void uncaughtException [_this ^Thread _th ^Throwable t]
              (log/error t "uncaught exeception in fulfillment thread"))))))))

(defonce ^ExecutorService global-pool (create-pool))

(defn create-empty-registry
  []
  (->Registry {} {}))

(defn- fulfill-entirely!
  [metrics db channel-id req-id cancelled?-vol filters target-row-id observer completion-callback]
  (try
    (let [tally (volatile! 0)]
      (metrics/time-fulfillment! metrics
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
          (jdbc/plan db (query/filters->query filters :target-row-id target-row-id :overall-limit 1000))))
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
  [metrics db channel-id req-id filters target-row-id observer eose-callback]
  (let [cancelled?-vol (volatile! false)]
    (fulfill-entirely! metrics db channel-id req-id cancelled?-vol filters target-row-id observer eose-callback)))

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
  ^Future [metrics db fulfill-atom channel-id req-id filters target-row-id observer eose-callback]
  (let [sid (str channel-id ":" req-id)
        cancelled?-vol (volatile! false)
        f (.submit global-pool
            ^Runnable (partial fulfill-entirely! metrics db channel-id req-id
                        cancelled?-vol filters target-row-id observer eose-callback))]
    (add-to-registry! fulfill-atom channel-id sid f cancelled?-vol)
    f))

(defn- internal-fulfill-one-batch!
  [metrics db filters target-row-id observer cancelled?-vol ^CompletableFuture overall-result-future]
  ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.761/doc/getting-started#plan--reducing-result-sets
  ;; consider alternative: look into if we could somehow truly batch websocket events
  ;; back to clients?
  (let [query-plan (jdbc/plan db
                     (query/filters->query filters
                       :target-row-id target-row-id
                       :overall-limit batch-size))]
    (metrics/time-fulfillment! metrics
      (transduce
        ;; note: if observer throws exception we catch below and for
        ;; now call it unexpected
        (map #(when-not @cancelled?-vol
                (observer (:raw_event %))
                (:rowid %)))
        (completing
          (fn
            [[running-count min-rowid] row-rowid]
            (if @cancelled?-vol
              (do
                (metrics/mark-fulfillment-interrupt! metrics)
                (reduced :cancelled))
              [(inc running-count)
               (if min-rowid
                 (min min-rowid row-rowid)
                 row-rowid)])))
        [0 nil] ;; [<count> <min-rowid>]
        query-plan))))

(defn submit-use-batching!
  ^Future [metrics db fulfill-atom channel-id req-id filters target-row-id observer eose-callback]
  (let [start-nanos (System/nanoTime)
        sid (str channel-id ":" req-id)
        cancelled?-vol (volatile! false)
        latest-task-future-vol (volatile! nil)
        ^CompletableFuture result-future (CompletableFuture.)
        _ (add-to-registry! fulfill-atom channel-id sid result-future cancelled?-vol)
        ^Runnable batch-fn
        (fn batch-fn [use-target-row-id iteration-num]
          (try
            (let [fulfill-result (internal-fulfill-one-batch! metrics
                                   db filters use-target-row-id observer
                                   cancelled?-vol result-future)]
              (when-not (identical? fulfill-result :cancelled)
                (let [[result-count min-rowid] fulfill-result]
                  (cond
                    (< result-count batch-size)
                    (do
                      (eose-callback)
                      (.complete result-future :success)
                      (metrics/fulfillment-num-rows! metrics
                        (+ (* iteration-num batch-size) result-count))
                      (metrics/update-overall-fullfillment-millis! metrics
                        (util/nanos-to-millis (- (System/nanoTime) start-nanos))))
                    (>= (* (inc iteration-num) batch-size) max-fulfillment-rows)
                    (do
                      (log/info "fulfilled maximum rows willing" {:iteration-num iteration-num})
                      (eose-callback)
                      (.complete result-future :success)
                      (metrics/fulfillment-num-rows! metrics (* (inc iteration-num) batch-size))
                      (metrics/update-overall-fullfillment-millis! metrics
                        (util/nanos-to-millis (- (System/nanoTime) start-nanos))))
                    :else
                    (do
                      (->> (.submit global-pool ^Runnable (partial batch-fn (dec min-rowid) (inc iteration-num)))
                        (vreset! latest-task-future-vol)))))))
            (catch Throwable t
              (log/error t "during fulfill batch")
              (metrics/mark-fulfillment-error! metrics)
              (.completeExceptionally result-future t))))
        _first-batch-future (try
                              (->> (.submit global-pool ^Runnable (partial batch-fn target-row-id 0))
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
