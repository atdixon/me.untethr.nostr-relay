(ns me.untethr.nostr.common.ws-registry
  (:require [me.untethr.nostr.common.domain :as domain])
  (:import (com.codahale.metrics Meter)
           (com.google.common.cache Cache CacheBuilder)
           (java.util UUID)
           (java.util.concurrent.atomic AtomicInteger)))

(defrecord AuthChallengeState
  [challenge
   challenge-birth-nanos
   challenge-satisfied-pubkey
   challenge-satisfied-nanos
   challenge-failure-count
   ;; note: this following value may be non-nil even if we have a successful
   ;; challenge -- e.g., if a client tried to auth after it was already auth'd
   most-recent-failure-type])

(defrecord WebSocketConnectionState
  [^String uuid
   ^long start-ns
   ^String ip-address
   auth-challenge-state-atom
   ^AtomicInteger outgoing-messages
   ^Meter bytes-in
   ^Meter bytes-out
   peak-1m-rate-bytes-in-atom
   peak-1m-rate-bytes-out-atom])

(defn create-auth-challenge-state
  ([]
   (->AuthChallengeState nil nil nil nil nil nil))
  ([new-challenge birth-nanos]
   (->AuthChallengeState new-challenge birth-nanos nil nil nil nil)))

(defn init-ws-connection-state
  [ip-addr]
  (->WebSocketConnectionState
    (str (UUID/randomUUID))
    (System/nanoTime)
    ip-addr
    (atom (create-auth-challenge-state))
    (AtomicInteger. 0)
    (when domain/track-bytes-in-out?  (Meter.))
    (when domain/track-bytes-in-out?  (Meter.))
    (atom -1.0)
    (atom -1.0)))

(defn update-peak-1m-rate-bytes-in!
  [{:keys [peak-1m-rate-bytes-in-atom] :as _ws-cxn-state} one-1m-rate]
  (swap! peak-1m-rate-bytes-in-atom max one-1m-rate))

(defn update-peak-1m-rate-bytes-out!
  [{:keys [peak-1m-rate-bytes-out-atom] :as _ws-cxn-state} one-1m-rate]
  (swap! peak-1m-rate-bytes-out-atom max one-1m-rate))

(defn update-peak-1m-rates!
  [{:keys [^Meter bytes-in ^Meter bytes-out] :as ws-cxn-state}]
  (update-peak-1m-rate-bytes-in! ws-cxn-state (.getOneMinuteRate bytes-in))
  (update-peak-1m-rate-bytes-out! ws-cxn-state (.getOneMinuteRate bytes-out)))

(defn bytes-in!
  [{:keys [^Meter bytes-in] :as ws-cxn-state} num-bytes]
  (.mark bytes-in num-bytes)
  (update-peak-1m-rates! ws-cxn-state))

(defn bytes-out!
  [{:keys [^Meter bytes-out] :as ws-cxn-state} num-bytes]
  (.mark bytes-out num-bytes)
  (update-peak-1m-rates! ws-cxn-state))

;; --

(defn create
  ^Cache []
  (.build
    (doto (CacheBuilder/newBuilder)
      ;; we'll use weakKeys for two reasons -- one as we'll take the safety of
      ;; having websocket state cleaned up whenever we lose a reference (however
      ;; we'll take pains to evict in all cases, so we should not be needing this)
      ;; but our main goal here is to get the identity-equality of cache keys that
      ;; is enabled when weakKeys are enabled.
      (.weakKeys))))

(defn add!
  [^Cache registry ^WebSocketConnectionState websocket-state]
  (.put registry websocket-state websocket-state))

(defn remove!
  [^Cache registry ^WebSocketConnectionState websocket-state]
  (.invalidate registry websocket-state))

(defn size-estimate
  [^Cache registry]
  (.size registry))
