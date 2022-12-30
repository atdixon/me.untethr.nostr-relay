(ns me.untethr.nostr.fulfill
  "Note: we protect concurrent read/writes to Registry via atom, but we assume
   that within the scope of a channel-id/websocket, access is always serial so
   that submit!, cancel! etc are never concurrently invoked for the same
   channel-id."
  (:require [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [me.untethr.nostr.metrics :as metrics]
            [me.untethr.nostr.query :as query]
            [me.untethr.nostr.util :as util])
  (:import (java.util.concurrent CompletableFuture Executors ThreadFactory ExecutorService Future)))

(def ^:private batch-size 30)
(def ^:private num-fulfillment-threads 1)

(defrecord FulfillHandle
  [^Future fut cancelled?-vol])

(defrecord Registry
  [channel-id->sids
   sid->future-handle])

(defn create-pool
  ^ExecutorService []
  (Executors/newFixedThreadPool num-fulfillment-threads
    (reify ThreadFactory
      (newThread [_this r]
        (doto (Thread. ^Runnable r)
          (.setDaemon true))))))

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
          (jdbc/plan db (query/filters->query filters :target-row-id target-row-id))))
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

(defn submit-use-batching!
  ^Future [metrics db fulfill-atom channel-id req-id filters target-row-id observer eose-callback]
  (let [start-nanos (System/nanoTime)
        sid (str channel-id ":" req-id)
        cancelled?-vol (volatile! false)
        ^CompletableFuture result-future (CompletableFuture.)
        ^Runnable batch-fn
        (fn batch-fn [use-target-row-id]
          (let [query-plan (jdbc/plan db
                             (query/filters->query filters
                               :target-row-id use-target-row-id
                               :overall-limit batch-size))
                ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.761/doc/getting-started#plan--reducing-result-sets
                ;; consider alternative: look into if we could somehow truly batch websocket events
                ;; back to clients?
                [result-count min-rowid] (try
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
                                               query-plan))
                                           (catch Exception e
                                             (log/error e "unexpected from batched transduce")
                                             (metrics/mark-fulfillment-error! metrics)
                                             (.completeExceptionally result-future e)))]
            (if (< result-count batch-size)
              (try
                (eose-callback)
                (.complete result-future :success)
                (metrics/update-overall-fullfillment-millis! metrics
                  (util/nanos-to-millis (- (System/nanoTime) start-nanos)))
                (catch Exception e
                  (log/error e "failed on eose-callback")
                  (metrics/mark-fulfillment-error! metrics)
                  (.completeExceptionally result-future e)))
              (try
                (.submit global-pool ^Runnable (partial batch-fn (dec min-rowid)))
                (catch Exception e
                  (log/error e "failed to re-submit")
                  (metrics/mark-fulfillment-error! metrics)
                  (.completeExceptionally result-future e))))))
        f (try
            (.submit global-pool ^Runnable (partial batch-fn target-row-id))
            (catch Exception e
              (log/error e "failed to submit")
              (metrics/mark-fulfillment-error! metrics)
              (.completeExceptionally result-future e)))]
    (add-to-registry! fulfill-atom channel-id sid result-future cancelled?-vol)
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
