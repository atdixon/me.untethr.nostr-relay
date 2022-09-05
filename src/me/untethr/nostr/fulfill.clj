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
  (:import (java.util.concurrent Executors ThreadFactory ExecutorService Future)))

(defrecord Registry
  [channel-id->sids
   sid->future])

(defn create-pool
  ^ExecutorService []
  (Executors/newFixedThreadPool 50
    (reify ThreadFactory
      (newThread [_this r]
        (doto (Thread. ^Runnable r)
          (.setDaemon true))))))

(defonce ^ExecutorService global-pool (create-pool))

(defn create-empty-registry
  []
  (->Registry {} {}))

(defn- do-fulfill
  [metrics db channel-id req-id filters target-row-id observer eose-callback]
  (try
    (let [tally (volatile! 0)]
      (metrics/time-fulfillment! metrics
        ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.761/doc/getting-started#plan--reducing-result-sets
        (transduce
          ;; note: if observer throws exception we catch below and for
          ;; now call it unexpected
          (map #(do
                  (vswap! tally inc)
                  (observer (:raw_event %))))
          (fn [& _])
          (jdbc/plan db (query/filters->query filters target-row-id)))
        (eose-callback))
      (metrics/fulfillment-num-rows! metrics @tally))
    (catch InterruptedException _e
      (log/info "interrupted" {:channel-id channel-id :req-id req-id})
      (metrics/mark-fulfillment-interrupt! metrics))
    (catch Exception e
      (if (.isInterrupted (Thread/currentThread))
        (do
          (metrics/mark-fulfillment-interrupt! metrics)
          (log/warn "interrupted" {:channel-id channel-id :req-id req-id :symptom (ex-message e)}))
        (do
          (metrics/mark-fulfillment-error! metrics)
          (log/error e "unexpected" {:channel-id channel-id :req-id req-id}))))))

(defn submit!
  [metrics db fulfill-atom channel-id req-id filters target-row-id observer eose-callback]
  (let [sid (str channel-id ":" req-id)
        f (.submit global-pool
            ^Runnable (partial do-fulfill metrics db channel-id req-id filters target-row-id observer eose-callback))]
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
            (assoc-in [:sid->future sid] f))))
      f
      (catch Exception e
        (.cancel f true)
        (throw e)))))

(defn- cancel!* [registry channel-id sid]
  (-> registry
    (update-in [:channel-id->sids channel-id] disj sid)
    (util/dissoc-in-if-empty [:channel-id->sids channel-id])
    (update :sid->future dissoc sid)))

(defn- cancel-sid!
  [fulfill-atom channel-id sid]
  (when-let [^Future f (get-in @fulfill-atom [:sid->future sid])]
    (.cancel f true))
  (swap! fulfill-atom #(cancel!* % channel-id sid)))

(defn cancel!
  [fulfill-atom channel-id req-id]
  (let [sid (str channel-id ":" req-id)]
    (cancel-sid! fulfill-atom channel-id sid)))

(defn cancel-all!
  [fulfill-atom channel-id]
  (doseq [sid (get-in @fulfill-atom [:channel-id->sids channel-id])]
    (cancel-sid! fulfill-atom channel-id sid)))
