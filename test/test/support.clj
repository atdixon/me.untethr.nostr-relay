(ns test.support
  (:require [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [me.untethr.nostr.store :as store]
            [me.untethr.nostr.app :as app]
            [me.untethr.nostr.json-facade :as json-facade]
            [clojure.java.io :as io]))

(def ^:private hex-chars "abcdef0123456789")

(def fake-hex-str (apply str (take 32 (cycle hex-chars))))

(defmacro with-memory-db
  [bindings & body]
  `(with-open [db# (jdbc/get-connection "jdbc:sqlite::memory:")]
     (store/apply-schema! db#)
     (let [~bindings [db#]]
       ~@body)))

(defn load-data
  [db parsed-events]
  (doseq [o parsed-events]
    (#'app/store-event! db o (#'json-facade/write-str* o))))

(defmacro with-regression-data
  [bindings & body]
  `(with-open [data-src# (io/reader
                           (io/resource "test/regression-data.txt"))]
     (let [data-vec# (vec (line-seq data-src#))
           ~bindings [data-vec#]]
       ~@body)))
