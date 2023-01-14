(ns test.query-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [me.untethr.nostr.app :as app]
    [me.untethr.nostr.query :as query]
    [me.untethr.nostr.query.engine :as engine]
    [test.support :as support]
    [test.test-data :as test-data]))

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

(defn- query-new-iterate-pages* [db init-active-filters]
  (let [first-page-results (query-new* db init-active-filters)
        first-page-stats (engine/calculate-page-stats first-page-results)
        next-active-filters (engine/next-active-filters init-active-filters first-page-stats)]
    (if (empty? next-active-filters)
      {:results first-page-results
       :filter-log [init-active-filters next-active-filters]
       :page-stats-log [first-page-stats]
       :results-log [first-page-results]}
      (reduce
        (fn [{:keys [results filter-log page-stats-log results-log]} _iter]
          (let [curr-active-filters (last filter-log)
                curr-page-results (query-new* db curr-active-filters)
                curr-page-stats (engine/calculate-page-stats curr-page-results)
                next-active-filters
                (engine/next-active-filters curr-active-filters curr-page-stats)
                results' (into results (map #(dissoc % :filter_index)) curr-page-results)
                accumulator {:filter-log (conj filter-log next-active-filters)
                             :results-log (conj results-log curr-page-results)
                             :page-stats-log (conj page-stats-log curr-page-stats)
                             :results results'}]
            (if (empty? next-active-filters)
              (reduced accumulator) accumulator)))
        {:results (into [] (map #(dissoc % :filter_index)) first-page-results)
         :filter-log [init-active-filters next-active-filters]
         :page-stats-log [first-page-stats]
         :results-log [first-page-results]}
        (range)))))

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
        (dotimes [page-size 20]
          (doseq [[filters expected-results] test-data-filters
                  :let [query-results (query-new-iterate-pages* db
                                        (mapv #(engine/init-active-filter %
                                                 :page-size (inc page-size)) filters))
                        ;_ (println (str/join "\n" (map-indexed vector (:filter-log query-results))))
                        ;_ (println (str/join "\n" (map-indexed vector (:page-stats-log query-results))))
                        ;_ (println (str/join "\n" (map-indexed vector (:results-log query-results))))
                        query-results (:results query-results)]]
            (is (= (set expected-results)
                  (into #{} (map :event_id) query-results))
              (pr-str [filters expected-results]))
            (is (<= (count expected-results) (count query-results))
              (pr-str [filters query-results]))))))))
