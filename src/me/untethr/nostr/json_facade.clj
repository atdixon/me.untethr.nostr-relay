(ns me.untethr.nostr.json-facade
  (:require
    [jsonista.core :as json]
    [me.untethr.nostr.metrics :as metrics])
  (:import (com.fasterxml.jackson.databind SerializationFeature)))

(def json-mapper
  (json/object-mapper
    {:encode-key-fn name
     :decode-key-fn keyword
     :modules [(metrics/create-jackson-metrics-module)]}))

(def json-mapper-order-keys
  (doto
    ;; note: we'll only use this mapper for writing/serialization
    (json/object-mapper
      {:encode-key-fn name})
    (.enable SerializationFeature/ORDER_MAP_ENTRIES_BY_KEYS)))

(defn parse
  [payload]
  (json/read-value payload json-mapper))

(defn write-str*
  ^String [o]
  (json/write-value-as-string o json-mapper))

(defn write-str-order-keys*
  ^String [o]
  (json/write-value-as-string o json-mapper-order-keys))
