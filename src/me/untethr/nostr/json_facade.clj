(ns me.untethr.nostr.json-facade
  (:require
    [jsonista.core :as json]
    [me.untethr.nostr.metrics :as metrics]))

(def json-mapper
  (json/object-mapper
    {:encode-key-fn name
     :decode-key-fn keyword
     :modules [(metrics/create-jackson-metrics-module)]}))

(defn parse
  [payload]
  (json/read-value payload json-mapper))

(defn write-str*
  ^String [o]
  (json/write-value-as-string o json-mapper))
