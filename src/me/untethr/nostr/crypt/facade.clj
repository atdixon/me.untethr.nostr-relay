(ns me.untethr.nostr.crypt.facade
  (:require
    [me.untethr.nostr.common.json-facade :as json-facade]
    [me.untethr.nostr.crypt.crypt :as crypt])
  (:import (java.nio.charset StandardCharsets)
           (java.security SecureRandom)))

(defonce ^SecureRandom secure-random (SecureRandom.))

(defn compute-event-id
  [pubkey created_at kind tags content]
  (-> [0 pubkey created_at kind tags content]
    json-facade/write-str*
    (.getBytes StandardCharsets/UTF_8)
    crypt/sha-256
    crypt/hex-encode))

(defn create-event
  [pubkey created_at kind tags content secret-key]
  (let [event-id (compute-event-id pubkey created_at kind tags content)
        aux-bytes (byte-array 32)
        _ (.nextBytes secure-random aux-bytes)
        sig (->
              (crypt/sign
                (crypt/hex-decode secret-key)
                (crypt/hex-decode event-id)
                aux-bytes)
              crypt/hex-encode)]
    {:id event-id
     :pubkey pubkey
     :created_at created_at
     :kind kind
     :tags tags
     :content content
     :sig sig}))
