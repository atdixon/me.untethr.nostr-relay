(ns test.support
  (:require [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [me.untethr.nostr.store :as store]
            [me.untethr.nostr.app :as app]
            [me.untethr.nostr.metrics :as metrics]))

(defmacro with-memory-db
  [bindings & body]
  `(with-open [db# (jdbc/get-connection "jdbc:sqlite::memory:")]
     (store/apply-schema! db#)
     (let [~bindings [db#]]
       ~@body)))

(defn load-data
  [db parsed-events]
  (let [m (metrics/create-metrics)]
    (doseq [o parsed-events]
      (#'app/store-event! m db o (#'app/write-str* o)))))
