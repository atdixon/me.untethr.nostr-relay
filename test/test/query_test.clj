(ns test.query-test
  (:require
    [clojure.test :refer :all]
    [me.untethr.nostr.common.domain :as domain]
    [me.untethr.nostr.common.store :as store]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [me.untethr.nostr.app :as app]
    [me.untethr.nostr.query :as query]
    [me.untethr.nostr.query.engine :as engine]
    [test.support :as support]
    [test.test-data :as test-data])
  (:import (me.untethr.nostr.common.domain TableMaxRowIds)))

;; --

(defn- capture-table-max-ids
  [db]
  (domain/->TableMaxRowIds
    (or (store/max-event-db-id db) -1)
    (or (store/max-event-db-id-p-tags db) -1)
    (or (store/max-event-db-id-e-tags db) -1)
    (or (store/max-event-db-id-x-tags db) -1)))

(defn query-all-pages-lazy
  ([db filters page-size]
   (let [table-max-row-ids (capture-table-max-ids db)]
     (query-all-pages-lazy db filters page-size table-max-row-ids)))
  ([db filters page-size ^TableMaxRowIds table-max-row-ids]
   (let [active-filters (mapv
                          #(engine/init-active-filter %
                             :table-target-ids table-max-row-ids
                             :page-size page-size) filters)]
     (map ;; lazy! client can "consume" as many pages as they like
       (fn [[{:keys [_ active-filters]}
             {:keys [prev-results _]}]]
         {:active-filters active-filters
          :results prev-results})
       (partition ;; we don't want partition-all, b/c we want empty if coll is singleton or less
         2 1
         (take-while
           some?
           (take 10000 ;; prevent infinite iteration
             (iterate
               (fn [{:keys [prev-results curr-filters]}]
                 (when-not (empty? curr-filters)
                   (let [page (engine/execute-active-filters db curr-filters)
                         stats (engine/calculate-page-stats page)
                         next-active-filters (engine/next-active-filters curr-filters stats)]
                     {:prev-results page
                      :curr-filters next-active-filters})))
               {:prev-results nil
                :curr-filters active-filters}))))))))

;; --

(defn- query* [db filters & {:keys [target-row-id overall-limit]}]
  (mapv :rowid
    (jdbc/execute! db (query/filters->query filters
                        :target-row-id target-row-id
                        :overall-limit overall-limit)
      {:builder-fn rs/as-unqualified-lower-maps})))

(defn- row-id->id
  [db row-id]
  (:id
    (jdbc/execute-one! db ["select id from n_events where rowid = ?" row-id]
      {:builder-fn rs/as-unqualified-lower-maps})))

(deftest query-test
  (support/with-memory-db [db]
    (support/load-data db (:pool test-data/pool-with-filters))
    (doseq [[filters expected-results] (:filters->results test-data/pool-with-filters)
            :let [query-results (query* db filters)]]
      (is (= (set expected-results)
            (into #{} (map (partial row-id->id db)) query-results))
        (pr-str [filters expected-results]))
      (is (= (count expected-results) (count query-results))
        (pr-str [filters query-results])))

    ;; now, do same queries but leveraging overall-limit
    (doseq [[filters expected-results] (:filters->results test-data/pool-with-filters)]
      (dotimes [overall-limit (* 2 (count expected-results))]
        (let [query-results (query* db filters :overall-limit overall-limit)]
          (is (= (set (take overall-limit expected-results))
                (into #{} (map (partial row-id->id db)) query-results))
            (pr-str [filters overall-limit expected-results]))
          (is (= (min (count expected-results) overall-limit) (count query-results))
            (pr-str [filters overall-limit query-results])))))

    ;; with the well-known data set, let's test some w/ target-row-id..
    (is (= #{1 2 4} (-> (query* db
                          [{:ids (mapv test-data/hx ["100" "101"])}
                           {:#e (mapv test-data/hx ["100"])}
                           {:#e (mapv test-data/hx ["102" "103"])}]
                          :target-row-id 4) set)))
    (is (= #{4} (-> (query* db
                      [{:ids (mapv test-data/hx ["100" "101"])}
                       {:#e (mapv test-data/hx ["100"])}
                       {:#e (mapv test-data/hx ["102" "103"])}]
                      :target-row-id 4
                      :overall-limit 1) set)))))

(deftest regression-test
  (support/with-regression-data [data-vec]
    (support/with-memory-db [db]
      (let [[_req req-id & req-filters] (#'app/parse (nth data-vec 2))
            raw-evt (nth data-vec 3)
            [_ evt] (#'app/parse raw-evt)]
        (#'app/store-event! db evt raw-evt)
        (= 1 (query* db req-filters))))))

;; -- new engine

(defn- query-new-simulate-union [db filters & {:keys [?backup-limit]}]
  ;; each filter may produce overlapping ids
  ;; we rely on distinct keeping stable ordering in tact
  ;; note we're doing combining of filters here for legacy reason, to support
  ;; the test-data set that declares results for filter combos
  (vec
    (distinct
      (map :event_id
        (sort-by (comp - :created_at)
          (reduce
            (fn [acc one-filter]
              (into acc
                (jdbc/execute! db
                  (engine/filter->query one-filter
                    :endcap-row-id Integer/MAX_VALUE
                    :endcap-created-at (or (:until one-filter) Long/MAX_VALUE)
                    ;; filter->query requires an override limit, so for our
                    ;; testing we'll provide MAX_VAL if ?override-limit is nil
                    :override-limit (or (:limit one-filter) ?backup-limit Integer/MAX_VALUE))
                  {:builder-fn rs/as-unqualified-lower-maps})))
            []
            filters))))))

(deftest query-new-per-filter-test
  (let [test-data-pool (:pool test-data/pool-with-filters)
        test-data-filters (:filters->results test-data/pool-with-filters)]
    (support/with-memory-db-kv-schema [db-kv]
      (support/with-memory-db-new-schema [db]
        (support/load-data-new-schema db db-kv test-data-pool)
        (doseq [[filters expected-results] test-data-filters
                :let [query-results (query-new-simulate-union db filters)]]
          (is (= expected-results query-results)
            (pr-str [filters expected-results]))
          (is (= (count expected-results) (count query-results))
            (pr-str [filters query-results])))
        ;; now, do same queries but leveraging overall-limit
        (doseq [[filters expected-results] test-data-filters]
          (dotimes [overall-limit (* 2 (count expected-results))]
            (let [query-results (query-new-simulate-union db filters :?backup-limit overall-limit)]
              (is (= (set (take overall-limit expected-results))
                    (into #{} (take overall-limit query-results)))
                (pr-str [filters overall-limit expected-results])))))

        ;; note: we're not testing target-row-id yet as above b/c we need to
        ;; account for denormalized #tag table ids when we do.
        ))))


(defn- query-new* [db active-filters]
  (jdbc/execute! db (engine/active-filters->query active-filters)
    {:builder-fn rs/as-unqualified-lower-maps}))

(deftest query-new-test
  (let [test-data-pool (:pool test-data/pool-with-filters)
        test-data-filters (:filters->results test-data/pool-with-filters)]
    (support/with-memory-db-kv-schema [db-kv]
      (support/with-memory-db-new-schema [db]
        (support/load-data-new-schema db db-kv test-data-pool)
        (doseq [[filters expected-results] test-data-filters
                :let [query-results (query-new* db
                                      (mapv #(engine/init-active-filter %
                                               :page-size Integer/MAX_VALUE) filters))]]
          ;; note: we can have duplicates across filters -
          (is (= (set expected-results)
                (into #{} (map :event_id) query-results))
            (pr-str [filters expected-results]))
          (is (<= (count expected-results) (count query-results))
            (pr-str [filters query-results])))
        ;; with pagination... try all pages sizes...
        (dotimes [page-size 10]
          (doseq [[filters expected-results] test-data-filters
                  :let [pages (query-all-pages-lazy db filters (inc page-size))
                        query-results (vec
                                        (mapcat #(map :event_id (:results %)) pages))]]
            (is (= (set expected-results)
                  (set query-results))
              (pr-str [filters expected-results]))
            (is (<= (count expected-results) (count query-results))
              (pr-str [filters query-results]))))))))

(deftest row-id-management-regression-test
  (support/with-memory-db-kv-schema [db-kv]
    (support/with-memory-db-new-schema [db]
      (let [fake-clock-vol (volatile! 100)]
        ;; produce two events with lots of tags so that the row id of our tags
        ;; tables comes to far exceed the row id of our base table
        (dotimes [x 100]
          (store/index-and-store-event!
            db db-kv {:id (str "P" x)
                      :pubkey "pp"
                      :created_at (vswap! fake-clock-vol inc)
                      :kind 1
                      :tags (vec
                              (map
                                #(vector "p" (str "p" %))
                                (range 0 10)))} "<raw-event...>")
          (store/index-and-store-event!
            db db-kv {:id (str "E" x)
                      :pubkey "ee"
                      :created_at (vswap! fake-clock-vol inc)
                      :kind 1
                      :tags (vec
                              (map
                                #(vector "e" (str "e" %))
                                (range 0 10)))} "<raw-event...>")))
      ;; now run paginating queries as we would in prod...
      (let [table-max-ids (capture-table-max-ids db)
            _ (is (> (:p-tags-id table-max-ids)
                    (:n-events-id table-max-ids)))
            acc (query-all-pages-lazy db [{:#p ["p1" "p9" "p11"]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) acc))]
        (is (= (mapv #(str "P" %) (range 99 -1 -1)) results)))
      ;;
      (let [table-max-ids (capture-table-max-ids db)
            _ (is (> (:e-tags-id table-max-ids)
                    (:n-events-id table-max-ids)))
            acc (query-all-pages-lazy db [{:#e ["e1" "e9" "e11"]}] 1
                  table-max-ids)
            results (vec
                      (mapcat #(map :event_id (:results %)) acc))]
        (is (= (mapv #(str "E" %) (range 99 -1 -1)) results)))
      ;;
      (let [pages (query-all-pages-lazy db
                    [{:#p ["p1" "p9" "p11"]}
                     {:#e ["e1" "e9" "e11"]}]
                    1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        (is (= (vec
                 (interleave
                   (map #(str "P" %) (range 99 -1 -1))
                   (map #(str "E" %) (range 99 -1 -1))))
              results))))))

(deftest another-row-id-management-regression-test
  (support/with-memory-db-new-schema [cxn]
    (support/with-memory-db-kv-schema [kv-cxn]
      ;; the regression: if we have created_at that doesn't match order of rowids
      ;;   our pagination strategy ought to still work
      (store/index-and-store-event!
        cxn kv-cxn {:id "A" :pubkey "aa" :created_at 10 :kind 1
                    :tags [["p" "tp0"]
                           ["p" "tp1"]
                           ["e" "te0"]]} "<e...>")
      (store/index-and-store-event!
        cxn kv-cxn {:id "B0" :pubkey "bb" :created_at 30 :kind 1
                    :tags [["p" "tp0"]
                           ["e" "te0"]
                           ["p" "tp1"]]} "<e...>")
      (store/index-and-store-event!
        cxn kv-cxn {:id "B1" :pubkey "bbb" :created_at 30 :kind 1
                    :tags [["p" "tp0"]
                           ["e" "te0"]
                           ["p" "tp1"]]} "<e...>")
      (store/index-and-store-event!
        cxn kv-cxn {:id "B2" :pubkey "bbbb" :created_at 30 :kind 1
                    :tags [["p" "tp0"]
                           ["e" "te0"]
                           ["p" "tp1"]]} "<e...>")
      (store/index-and-store-event!
        cxn kv-cxn {:id "C0" :pubkey "cc" :created_at 20 :kind 1
                    :tags [["e" "te0"]
                           ["p" "tp0"]]} "<e...>")
      (store/index-and-store-event!
        cxn kv-cxn {:id "C1" :pubkey "ccc" :created_at 20 :kind 1
                    :tags [["e" "te0"]
                           ["p" "tp0"]]} "<e...>")
      (store/index-and-store-event!
        cxn kv-cxn {:id "C2" :pubkey "cccc" :created_at 20 :kind 1
                    :tags [["e" "te0"]
                           ["p" "tp0"]]} "<e...>")

      ;; now, test with all the optimized query stereotypes:

      (let [pages (query-all-pages-lazy cxn [{}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results)))
      (let [pages (query-all-pages-lazy cxn [{:kinds [1]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results)))
      (let [pages (query-all-pages-lazy cxn [{:authors ["aa" "bb" "cc"
                                                        "bbb" "bbbb"
                                                        "ccc" "cccc"]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results)))
      (let [pages (query-all-pages-lazy cxn [{:authors ["aa" "bb" "cc"
                                                        "bbb" "bbbb"
                                                        "ccc" "cccc"]
                                              :kinds [1]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results)))
      (let [pages (query-all-pages-lazy cxn [{:#p ["tp0"]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results)))
      (let [pages (query-all-pages-lazy cxn [{:#p ["tp0"] :kinds [1]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results)))
      (let [pages (query-all-pages-lazy cxn [{:#e ["te0"]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results)))
      (let [pages (query-all-pages-lazy cxn [{:#e ["te0"] :kinds [1]}] 1)
            results (vec
                      (mapcat #(map :event_id (:results %)) pages))]
        ;; note: in temporal order!
        (is (= ["B2" "B1" "B0" "C2" "C1" "C0" "A"] results))))))

(deftest contains-any-prefix-queries?-test
  []
  (is (not (#'engine/contains-any-prefix-queries? {})))
  (is (not (#'engine/contains-any-prefix-queries? {:ids []})))
  (is (not (#'engine/contains-any-prefix-queries? {:authors [(support/random-hex-str)]})))
  (is (not (#'engine/contains-any-prefix-queries? {:authors [(support/random-hex-str)
                                                             (support/random-hex-str)]
                                                   :ids [(support/random-hex-str)]})))
  (is (#'engine/contains-any-prefix-queries? {:authors [(support/random-hex-str)
                                                        (support/random-hex-str 63)]
                                              :ids [(support/random-hex-str)]}))
  (is (#'engine/contains-any-prefix-queries? {:authors [(support/random-hex-str)
                                                        (support/random-hex-str 1)]
                                              :ids [(support/random-hex-str)]}))
  (is (#'engine/contains-any-prefix-queries? {:authors [(support/random-hex-str)
                                                        (support/random-hex-str)]
                                              :ids [(support/random-hex-str 1)]})))

(deftest basic-prefix-query-test
  (binding [engine/*support-prefix-queries?* true]
    (support/with-memory-db-new-schema [cxn]
      (support/with-memory-db-kv-schema [kv-cxn]
        ;; the regression: if we have created_at that doesn't match order of rowids
        ;;   our pagination strategy ought to still work
        (store/index-and-store-event!
          cxn kv-cxn {:id "QAAA" :pubkey "qaa" :created_at 10 :kind 1
                      :tags [["p" "tp0"]
                             ["p" "tp1"]
                             ["e" "te0"]]} "<e...>")
        (store/index-and-store-event!
          cxn kv-cxn {:id "QBBB" :pubkey "qbb" :created_at 5 :kind 1
                      :tags [["p" "tp0"]
                             ["p" "tp1"]
                             ["e" "te0"]]} "<e...>")
        (let [pages (query-all-pages-lazy cxn [{:ids ["Q"]}] 1)
              results (vec
                        (mapcat #(map :event_id (:results %)) pages))]
          ;; note: in temporal order!
          (is (= ["QAAA" "QBBB"] results)))
        (let [pages (query-all-pages-lazy cxn [{:authors ["q"]}] 1)
              results (vec
                        (mapcat #(map :event_id (:results %)) pages))]
          ;; note: in temporal order!
          (is (= ["QAAA" "QBBB"] results)))
        ))))