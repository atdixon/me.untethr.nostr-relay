(ns test.store-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [me.untethr.nostr.common.json-facade :as json-facade]
    [me.untethr.nostr.common.metrics :as metrics]
    [me.untethr.nostr.query :as query]
    [me.untethr.nostr.query.engine :as engine]
    [me.untethr.nostr.write-thread :as write-thread]
    [test.support :as support]
    [me.untethr.nostr.common.store :as store]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs])
  (:import (java.util.concurrent CountDownLatch TimeUnit)))

(deftest pragma-test
  (support/with-memory-db-new-schema [db]
    (is (= {:journal_size_limit 16777216
            :cache_size -2000
            :page_size 4096
            :auto_vacuum 2
            :wal_autocheckpoint 0
            :synchronous 0
            :foreign_keys 0}
          (select-keys
            (store/collect-pragmas! db)
            [:journal_size_limit
             :cache_size
             :page_size
             :auto_vacuum
             :wal_autocheckpoint
             :synchronous
             :foreign_keys]))))
  (support/with-memory-db-kv-schema [db-kv]
    (is (= {:journal_size_limit 16777216
            :cache_size -4000
            :page_size 16384
            :auto_vacuum 2
            :wal_autocheckpoint 0
            :synchronous 0
            :foreign_keys 0}
          (select-keys
            (store/collect-pragmas! db-kv)
            [:journal_size_limit
             :cache_size
             :page_size
             :auto_vacuum
             :wal_autocheckpoint
             :synchronous
             :foreign_keys])))))

(deftest idempotent-insert-test
  (support/with-memory-db [db]
    (store/insert-event! db "id0" "pk0" 0 1 "{}")
    (store/insert-event! db "id0" "xxx" -1 -1 "{}")
    (= 1 (count (jdbc/execute! db ["select * from n_events"])))))

(deftest singleton-kind-cleanup-test
  ;; verify our insert_singleton_kind trigger
  (support/with-memory-db [db]
    (store/insert-event! db "id0" "pk0" 0 1 "{}")
    (store/insert-event! db "id1" "pk0" 1 0 "{}")
    (store/insert-event! db "id2" "pk0" 2 0 "{}")
    (store/insert-event! db "id3" "pk0" 0 0 "{}")
    (store/insert-event! db "id4" "pk0" 1 1 "{}")
    (store/insert-event! db "id5" "pk0" 0 3 "{}")
    (store/insert-event! db "id6" "pk0" 5 3 "{}")
    (store/insert-event! db "id7" "pk1" 15 0 "{}")
    (store/insert-event! db "id8" "pk1" 15 1 "{}")
    (store/insert-event! db "id9" "pk1" 20 1 "{}")
    (store/insert-event! db "id10" "pk1" 20 3 "{}")
    (store/insert-event! db "id11" "pk1" 20 0 "{}")
    (store/insert-event! db "id12" "pk1" 15 3 "{}")
    ;; make sure idempotent insert doesn't mess with trigger:
    (store/insert-event! db "id5" "pk0" 1 3 "{}")
    (let [res (jdbc/execute! db ["select * from n_events where deleted_ = 0"]
                {:builder-fn rs/as-unqualified-lower-maps})
          g (group-by (juxt :pubkey :kind) res)]
      (is (= ["id0" "id4"] (mapv :id (get-in g [["pk0" 1]]))))
      (is (= ["id2"] (mapv :id (get-in g [["pk0" 0]]))))
      (is (= [["id11" 20]] (mapv (juxt :id :created_at) (get-in g [["pk1" 0]]))))
      (is (= [["id8" 15] ["id9" 20]] (mapv (juxt :id :created_at) (get-in g [["pk1" 1]]))))
      (is (= [["id10" 20]] (mapv (juxt :id :created_at) (get-in g [["pk1" 3]]))))
      (is (= [["id6" 5]] (mapv (juxt :id :created_at) (get-in g [["pk0" 3]])))))))

(defn- count-filter-results [db f]
  (count
    (jdbc/execute! db
      (engine/filter->query f
        :endcap-row-id Integer/MAX_VALUE
        :override-limit Integer/MAX_VALUE))))

(deftest simple-delete-test
  (support/with-memory-db-new-schema [db]
    ;; note for this test we can get away with unverified invalid raw data here.
    ;; [id pubkey created_at kind tags]
    (store/store-event-new-schema!- db
      {:id "A" :pubkey "x" :created_at 0 :kind 1
       :tags [["p" "p0"] ["p" "p1"] ["e" "e0"] ["t" "t0"]]})
    (store/store-event-new-schema!- db
      {:id "B" :pubkey "y" :created_at 0 :kind 1
       :tags [["p" "p0"]]})
    (store/store-event-new-schema!- db
      {:id "C" :pubkey "y" :created_at 0 :kind 1
       :tags [["p" "p0"] ["p" "p1"] ["e" "e0"] ["t" "t0"] ["t" "t1"]]})
    (is (= 1 (count-filter-results db {:ids ["A"]})))
    (is (= 1 (count-filter-results db {:ids ["B"]})))
    (is (= 1 (count-filter-results db {:ids ["C"]})))
    (is (= 3 (count-filter-results db {:#p ["p0"]})))
    (is (= 2 (count-filter-results db {:#e ["e0"]})))
    (is (= 2 (count-filter-results db {:#t ["t0"]})))
    (is (= 1 (count-filter-results db {:#t ["t1"]})))
    ;; we have a trigger for true deletion ... we want to make sure it works
    ;; ... b/c it will be nice just to purge n_events and get foreign refs deleted
    ;; too
    (is
      (= 1 (:next.jdbc/update-count
             (jdbc/execute-one! db ["delete from n_events where event_id = 'A';"]
               {:builder-fn rs/as-unqualified-lower-maps}))))
    (is (= 0 (count-filter-results db {:ids ["A"]})))
    (is (= 1 (count-filter-results db {:ids ["B"]})))
    (is (= 1 (count-filter-results db {:ids ["C"]})))
    (is (= 2 (count-filter-results db {:#p ["p0"]})))
    (is (= 1 (count-filter-results db {:#e ["e0"]})))
    (is (= 1 (count-filter-results db {:#t ["t0"]})))
    (is (= 1 (count-filter-results db {:#t ["t1"]})))
    ;; but usually we'll simple set deleted_ bit so let's test that too
    (is
      (= 1 (:next.jdbc/update-count
             (jdbc/execute-one! db ["update n_events set deleted_ = 1 where event_id = 'C';"]
               {:builder-fn rs/as-unqualified-lower-maps}))))
    (is (= 0 (count-filter-results db {:ids ["A"]})))
    (is (= 1 (count-filter-results db {:ids ["B"]})))
    (is (= 0 (count-filter-results db {:ids ["C"]})))
    (is (= 1 (count-filter-results db {:#p ["p0"]})))
    (is (= 0 (count-filter-results db {:#e ["e0"]})))
    (is (= 0 (count-filter-results db {:#t ["t0"]})))
    (is (= 0 (count-filter-results db {:#t ["t1"]})))))

(deftest simple-query-and-lookup-test
  (let [e-obj {:id "A" :pubkey "x" :created_at 0 :kind 1
               :tags [["p" "p0"] ["p" "p1"] ["e" "e0"] ["t" "t0"]]}
        raw-e (json-facade/write-str* e-obj)]
    (support/with-memory-db-kv-schema [db-kv]
      (support/with-memory-db-new-schema [db]
        (store/index-and-store-event! db db-kv e-obj raw-e)
        (let [q (engine/active-filters->query
                  [(engine/init-active-filter {:ids ["A"]})])
              r (jdbc/execute! db q
                  {:builder-fn rs/as-unqualified-lower-maps})
              ids (mapv :event_id r)
              fetched (transduce
                        ;; note: if observer throws exception we catch below and for
                        ;; now call it unexpected
                        (map :raw_event)
                        (completing
                          (fn [acc raw-event]
                            (conj acc raw-event)))
                        []
                        (jdbc/plan db-kv
                          (apply vector
                            (format
                              "select raw_event from n_kv_events where event_id in (%s)"
                              (str/join "," (repeat (count ids) "?")))
                            ids)))]
          (is (= [raw-e] fetched)))))))

(deftest many-followers-regression-test
  (support/with-regression-data [data-vec]
    (support/with-memory-db-new-schema [db]
      (support/with-memory-db-kv-schema [db-kv]
        (let [fake-metrics (metrics/create-metrics)
              [_ event-obj] (json-facade/parse (nth data-vec 4))
              latch (CountDownLatch. 1)
              _ (write-thread/submit-new-event!
                  fake-metrics db db-kv "chan0" event-obj "<raw...>"
                  (fn [])
                  (fn [])
                  (fn [] (.countDown latch)))]
          (is (time
                (.await latch 20000 TimeUnit/MILLISECONDS))))))))
