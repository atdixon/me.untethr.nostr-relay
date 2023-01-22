(ns test.support
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [me.untethr.nostr.common.store :as store]
            [me.untethr.nostr.app :as app]
            [me.untethr.nostr.common.json-facade :as json-facade]
            [clojure.java.io :as io]))

(def ^:private hex-chars "abcdef0123456789")

(def fake-hex-64-str (apply str (take 64 (cycle hex-chars))))

(defn random-hex-str
  ([] (random-hex-str 64))
  ([len]
   (apply str
     (take len
       (repeatedly #(nth hex-chars (rand-int (count hex-chars))))))))

(defmacro ^:deprecated with-memory-db
  [bindings & body]
  `(let [parsed-schema# (store/parse-schema "schema-deprecated.sql")
         ds# (store/create-sqlite-datasource ":memory:" parsed-schema#)]
     (with-open [db# (jdbc/get-connection ds#)]
       (store/apply-ddl-statements! db# parsed-schema#)
       (let [~bindings [db#]]
         ~@body))))

(defmacro with-memory-db-new-schema
  [bindings & body]
  `(let [parsed-schema# (store/parse-schema "schema-new.sql")
         ds# (store/create-sqlite-datasource ":memory:" parsed-schema#)]
     ;; big note: when using :memory:, must use singleton connection
     ;;  for everything - once connection closes db bye-bye
     ;;  (so, i.e., if you run operations through datasource, things will
     ;;    quietly work but next connection will not see ddl or inserts from
     ;;    priors)
     (with-open [db# (jdbc/get-connection ds#)]
       (store/apply-ddl-statements! db# parsed-schema#)
       (let [~bindings [db#]]
         ~@body))))

(defmacro with-memory-db-kv-schema
  [bindings & body]
  `(let [parsed-schema# (store/parse-schema "schema-kv.sql")
         ds# (store/create-sqlite-datasource ":memory:" parsed-schema#)]
     ;; big note: when using :memory:, must use singleton connection
     ;;  for everything - once connection closes db bye-bye
     ;;  (so, i.e., if you run operations through datasource, things will
     ;;    quietly work but next connection will not see ddl or inserts from
     ;;    priors)
     (with-open [db# (jdbc/get-connection ds#)]
       (store/apply-ddl-statements! db# parsed-schema#)
       (let [~bindings [db#]]
         ~@body))))

(defn ^:deprecated load-data
  [db parsed-events]
  (doseq [o parsed-events]
    (#'app/store-event! db o (#'json-facade/write-str* o))))

(defn load-data-new-schema
  [db db-kv parsed-events]
  (doseq [o parsed-events]
    (store/index-and-store-event! db db-kv o (#'json-facade/write-str* o))))

(defmacro with-regression-data
  [bindings & body]
  `(with-open [data-src# (io/reader
                           (io/resource "test/regression-data.txt"))]
     (let [data-vec# (vec (line-seq data-src#))
           ~bindings [data-vec#]]
       ~@body)))
