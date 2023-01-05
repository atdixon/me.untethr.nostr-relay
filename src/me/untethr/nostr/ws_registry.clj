(ns me.untethr.nostr.ws-registry
  (:import (com.google.common.cache Cache CacheBuilder)
           (java.util.concurrent.atomic AtomicInteger)))

(defrecord WebSocketConnectionState
  [^String uuid ^long start-ns ^String ip-address ^AtomicInteger outgoing-messages])

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
