(ns test.subscribe-test
  (:require
    [clojure.set :as set]
    [clojure.test :refer :all]
    [clojure.test.check :as tc]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.app :as app]
    [me.untethr.nostr.subscribe :as subscribe]
    [me.untethr.nostr.common.metrics :as metrics]
    [test.support :as support]
    [test.test-data :as test-data :refer [hx]])
  (:import (java.util List)))

(defn- throw-fn
  [& args] (throw (ex-info "unexpected" {:args args})))

(deftest simple-matches-test
  (doseq [{:keys [filter] :as spec} test-data/filter-matches]
    (doseq [[i m] (map-indexed vector (:known-matches spec))]
      (is
        (#'subscribe/compiled-filter-matches?
          (#'subscribe/compile-filter "chan-id:req-id" filter throw-fn)
          (:id m) (:pubkey m) (:created_at m) (:kind m) (:tags m))
        (pr-str i spec)))
    (doseq [[i n] (map-indexed vector (:known-non-matches spec))]
      (is
        (not
          (#'subscribe/compiled-filter-matches?
            (#'subscribe/compile-filter "chan-id:req-id" filter throw-fn)
            (:id n) (:pubkey n) (:created_at n) (:kind n) (:tags n)))
        (pr-str i spec)))))

(deftest subscribe-with-pool-data-test
  (let [metrics-fake (metrics/create-metrics)
        subs-atom (atom (subscribe/create-empty-subs))
        idx->results-atom (atom {})]
    ;; subscribe everything ...
    (doseq [[idx [filters _]] (map-indexed vector
                                (:filters->results test-data/pool-with-filters))]
      (subscribe/subscribe! subs-atom
        (format "chan%d" idx) (format "req%d" idx) filters
        (fn [fake-raw-event]
          (swap! idx->results-atom update idx (fnil conj []) fake-raw-event))))
    ;; notify everything...
    (doseq [{obj-id :id :as obj} (:pool test-data/pool-with-filters)]
      (subscribe/notify! metrics-fake subs-atom obj (format "<fake-raw-event:%s>" obj-id)))
    ;; verify that we got notified fully and correctly...
    (doseq [[idx [filters expected-ids]]
            (map-indexed vector
              (:filters->results test-data/pool-with-filters))
            :let [has-limits? (some :limit filters)
                  actual-raw-events (get @idx->results-atom idx [])
                  expected-raw-events (mapv (partial format "<fake-raw-event:%s>") expected-ids)]]
      (if has-limits?
        (is (= (set expected-raw-events)
              (set/intersection (set expected-raw-events) (set actual-raw-events)))
          filters)
        ;; note we reverse before making out comparison b/c pool lists results
        ;; in reverse temporal order
        (is (= expected-raw-events (reverse actual-raw-events)) filters)))))

(deftest candidate-filters-test
  (let [subs-atom
        (doto (atom (subscribe/create-empty-subs))
          (subscribe/subscribe! "chan0" "req0" [{:ids [(hx "ab0") (hx "ab1")] :authors [(hx "cd0") (hx "cd1")]}] throw-fn)
          (subscribe/subscribe! "chan0" "req1" [{:#e [(hx "ab0")]} {:#e [(hx "ab1")]}] throw-fn)
          ;; note: here we subscribe with a >1 filter, which means that our private
          ;; candidate-filters can produce two filters with the same sid (channel:req-id)
          (subscribe/subscribe! "chan0" "req2" [{:#p [(hx "cd0") (hx "cd1")]} {:#e [(hx "ab0") (hx "ab1")]} {:authors [(hx "cd0")]}] throw-fn)
          (subscribe/subscribe! "chan1" "req0" [{:#e [(hx "ab0") (hx "ab1")] :kinds [0 1]}] throw-fn)
          (subscribe/subscribe! "chan1" "req1" [{:since 50 :until 100}] throw-fn))]
    (is (= {"chan0:req0" 1
            "chan1:req1" 1
            "chan0:req2" 1}
          (frequencies (map :sid (#'subscribe/candidate-filters @subs-atom (hx "ab0") (hx "cd0") [])))))
    (is (= {"chan0:req0" 1
            "chan0:req1" 1
            "chan0:req2" 2 ;; !! candidate-filters can find two filters for the same subscription !!
            "chan1:req0" 1
            "chan1:req1" 1}
          (frequencies (map :sid (#'subscribe/candidate-filters @subs-atom (hx "ab0") (hx "cd0") [["e" (hx "ab0")]])))))))

(deftest basic-subscribe-test
  (let [subs-atom (atom (subscribe/create-empty-subs))]
    (is (= 0 (subscribe/num-subscriptions subs-atom "chan0")))
    (is (= 0 (subscribe/num-filters subs-atom "chan0")))
    (subscribe/subscribe! subs-atom "chan-id" "req-id" [{:since 120}]
      (fn [x] (throw (ex-info "unexpected" {:x x}))))
    (subscribe/unsubscribe! subs-atom "chan-id" "req-id")
    (is (= @subs-atom (subscribe/create-empty-subs))))
  (let [subs-atom
        (doto (atom (subscribe/create-empty-subs))
          (subscribe/subscribe! "chan0" "req0" [{:ids [(hx "ab0") (hx "ab1")]} {:authors [(hx "cd0") (hx "cd1")]}] throw-fn)
          (subscribe/subscribe! "chan0" "req1" [{:#e ["id0" "id1"]}] throw-fn)
          (subscribe/subscribe! "chan0" "req2" [{:#p [(hx "cd0") (hx "cd1")]}] throw-fn))]
    (is (= 3 (subscribe/num-subscriptions subs-atom "chan0")))
    (is (= 4 (subscribe/num-filters subs-atom "chan0")))
    (subscribe/unsubscribe! subs-atom "chan0" "req1")
    (is (= 2 (subscribe/num-subscriptions subs-atom "chan0")))
    (is (= 3 (subscribe/num-filters subs-atom "chan0")))
    (subscribe/unsubscribe-all! subs-atom "chan0")
    (is (= @subs-atom (subscribe/create-empty-subs)))))

(deftest basic-unsubscribe-prefix-test
  ;; the goal of this test is to ensure we don't leak any subscriptions after
  ;; unsubscribes, including subscriptions that contain nip-01 prefix queries
  (let [channel-pool (mapv #(str "chan" %) (range 5))
        req-pool (mapv #(str "req" %) (range 5))
        subs-atom (atom (subscribe/create-empty-subs))
        create-many-subscriptions!
        (fn []
          (letfn [(test-subscribe [filters]
                    (subscribe/subscribe! subs-atom
                      (rand-nth channel-pool)
                      (rand-nth req-pool)
                      filters #(throw (ex-info "unexpected" {:x %}))))]
            ;; fields that don't support prefix queries...
            (test-subscribe [{}])
            (test-subscribe [{:kinds [1 2]}])
            (test-subscribe [{:since 1234}])
            (test-subscribe [{:until 1234} {:since 1234}])
            (test-subscribe [{:limit 5}])
            (test-subscribe [{:#e ["abc"]}])
            (test-subscribe [{:#p ["def"]}])
            (test-subscribe [{:#t ["012"]}])
            ;; now, fields that support prefix queries...
            (test-subscribe [{:authors [(support/random-hex-str)]}])
            (test-subscribe [{:authors [(support/random-hex-str)]}])
            (test-subscribe [{:ids [(support/random-hex-str) (support/random-hex-str)]}])
            (dotimes [len 64]
              (test-subscribe [{:ids [(support/random-hex-str len)]}]))
            (dotimes [len 64]
              (test-subscribe [{:authors [(support/random-hex-str len)]}]))
            (dotimes [len 64]
              (test-subscribe [{:authors [(support/random-hex-str len) (support/random-hex-str len)]}
                               {:ids [(support/random-hex-str len) (support/random-hex-str len)]}]))))]
    (create-many-subscriptions!)
    (is (> (subscribe/num-filters-prefixes subs-atom) 64))
    (is (not= @subs-atom (subscribe/create-empty-subs)))
    (doseq [chan channel-pool]
      (doseq [req req-pool]
        (subscribe/unsubscribe! subs-atom chan req))
      (is (= 0 (subscribe/num-filters subs-atom chan)))
      (is (= 0 (subscribe/num-subscriptions subs-atom chan))))
    (is (= 0 (subscribe/num-subscriptions subs-atom)))
    (is (= 0 (subscribe/num-firehose-filters subs-atom)))
    (is (= 0 (subscribe/num-filters-prefixes subs-atom)))
    (is (= @subs-atom (subscribe/create-empty-subs)))
    (create-many-subscriptions!)
    (is (> (subscribe/num-filters-prefixes subs-atom) 64))
    (is (not= @subs-atom (subscribe/create-empty-subs)))
    (doseq [chan channel-pool]
      (subscribe/unsubscribe-all! subs-atom chan)
      (is (= 0 (subscribe/num-filters subs-atom chan))))
    (is (= 0 (subscribe/num-subscriptions subs-atom)))
    (is (= 0 (subscribe/num-firehose-filters subs-atom)))
    (is (= 0 (subscribe/num-filters-prefixes subs-atom)))
    (is (= @subs-atom (subscribe/create-empty-subs)))))

(deftest unsubscribe-regression-test
  []
  ;; this was not a cause of regression but discovered in analyzing an issue
  ;; that some users have spaces in their REQ ids, so keeping this test anyway
  ;; to have coverage on that case.
  (let [subs-atom (atom (subscribe/create-empty-subs))]
    (subscribe/subscribe! subs-atom
      "29566a63-cc6e-430b-837b-e772c18092d2"
      "adhoc 9658"
      [{:authors ["3e8ed898a0ae5402f6120ffa5a05e18b024b69209f17072fb89000c225b7109"]}]
      #(throw (ex-info "unexpected" {:x %})))
    (subscribe/unsubscribe! subs-atom
      "29566a63-cc6e-430b-837b-e772c18092d2" "adhoc 9658")
    (is (= @subs-atom (subscribe/create-empty-subs)))))

;; --

(def char-hex
  (gen/fmap clojure.core/char
    (gen/one-of [(gen/choose 48 57)
                 (gen/choose 97 102)])))

(def string-hex-64
  (gen/fmap clojure.string/join (gen/vector char-hex 64)))

(def filter-gen
  (gen/such-that
    #(or
       (nil? (:since %))
       (nil? (:until %))
       (>= (:until %) (:since %)))
    (gen/fmap
      #(zipmap [:ids :authors :kinds :#e :#p :since :until] %)
      (gen/tuple
        (gen/vector (gen/not-empty string-hex-64))
        (gen/vector (gen/not-empty string-hex-64))
        (gen/vector (gen/choose 0 10))
        (gen/vector (gen/not-empty gen/string-ascii))
        (gen/vector (gen/not-empty gen/string-ascii))
        (gen/one-of [gen/nat (gen/return nil)])
        (gen/one-of [gen/nat (gen/return nil)])))))

(def req-gen
  (gen/tuple
    (gen/elements ["chan0" "chan1" "chan2" "chan3"])
    (gen/elements ["req0" "req1" "req2" "req3"])))

(defn- subs-reflect-sids?
  [subs spec]
  (let [x (reduce
            (fn [acc [[chan-id req-id] _]]
              (update acc chan-id (fnil conj #{}) (str chan-id ":" req-id))) {} spec)
        y (reduce
            (fn [acc [[chan-id req-id] filters]]
              (assoc acc (str chan-id ":" req-id) filters)) {} spec)]
    (and (= x (:channel-id->sids subs)) (= y (:sid->filters subs)))))

(deftest subscribe-unsubscribe-test
  (let [prop
        (prop/for-all [spec (gen/vector
                              (gen/tuple req-gen
                                (gen/not-empty
                                  (gen/vector filter-gen))))]
          ;; note: spec purposefully includes duplicate subscription req ids
          (let [distinct-spec (map (comp last second) (group-by first spec))
                subs-atom (atom (subscribe/create-empty-subs))]
            (doseq [[[chan-id req-id] filters] spec]
              (subscribe/unsubscribe! subs-atom chan-id req-id)
              (subscribe/subscribe! subs-atom chan-id req-id filters throw-fn))
            (let [subs-snapshot @subs-atom
                  fresh-subs-atom (atom (subscribe/create-empty-subs))]
              ;; we apply just the latest subscriptions to the fresh-subs-atom
              (doseq [[[chan-id req-id] filters] distinct-spec]
                (subscribe/unsubscribe! fresh-subs-atom chan-id req-id)
                (subscribe/subscribe! fresh-subs-atom chan-id req-id filters throw-fn))
              (let [fresh-subs-snapshot @fresh-subs-atom]
                (doseq [[[chan-id req-id] _filters] spec]
                  (subscribe/unsubscribe! subs-atom chan-id req-id)
                  (subscribe/unsubscribe! fresh-subs-atom chan-id req-id))
                (and
                  (subs-reflect-sids? subs-snapshot distinct-spec)
                  (= subs-snapshot fresh-subs-snapshot)
                  (= @subs-atom (subscribe/create-empty-subs))
                  (= @fresh-subs-atom (subscribe/create-empty-subs)))))))]
    (let [res (tc/quick-check 40 prop)]
      (is (:pass? res) (pr-str res)))))

(defn- if-empty
  [coll x]
  (if (empty? coll) x coll))

(defn- filter->matching-event
  [seed {:keys [ids kinds since until authors] e# :#e p# :#p}]
  (let [gen (gen/fmap
              #(zipmap [:id :pubkey :created_at :kind :tags :content :sig] %)
              (gen/tuple
                (gen/elements (if-empty ids [(hx "abcd")]))
                (gen/elements (if-empty authors [(hx "ef01")]))
                (gen/choose (or since 0) (or until Long/MAX_VALUE))
                (gen/elements (if-empty kinds [-1]))
                (gen/let [es (gen/fmap (fn [lst] (map #(vector "e" %) lst))
                               (if (empty? e#)
                                 (gen/return [])
                                 (gen/not-empty
                                   (gen/list-distinct (gen/elements e#)))))
                          ps (gen/fmap (fn [lst] (map #(vector "p" %) lst))
                               (if (empty? p#)
                                 (gen/return [])
                                 (gen/not-empty
                                   (gen/list-distinct (gen/elements p#)))))]
                  (into [] (concat es ps)))
                gen/string
                (gen/not-empty gen/string-ascii)))]
    (gen/generate gen 30 seed)))

(deftest subscribe-notify-test
  (let [metrics-fake (metrics/create-metrics)
        seed (System/currentTimeMillis)
        prop
        (prop/for-all [filters (gen/not-empty (gen/vector filter-gen))]
          (let [subs-atom (atom (subscribe/create-empty-subs))
                result-vol (volatile! {})
                collect-fn #(vswap! result-vol update %1 (fnil conj []) %2)
                filter-tuples (for [[i filter] (map-indexed vector filters)
                                    :let [chan-id (str "c" (quot i 3))
                                          req-id (str "r" (rem i 3))]]
                                [[chan-id req-id] filter])]
            (doseq [[[chan-id req-id] f] filter-tuples]
              ;; note: here we happen to have just one filter per sid
              (subscribe/subscribe! subs-atom chan-id req-id [f]
                (partial collect-fn [chan-id req-id])))
            (every?
              (fn [[[chan-id req-id] f]]
                (let [matching-event (filter->matching-event seed f)
                      as-raw-event (#'app/write-str* matching-event)]
                  (subscribe/notify! metrics-fake subs-atom matching-event as-raw-event)
                  (let [^List r (get @result-vol [chan-id req-id])]
                    (when (not= 1 (count r))
                      ;; this can occur; would like to see case where it does
                      (log/warn "got more than one result" {:seed seed}))
                    (some-> ^List (get @result-vol [chan-id req-id]) (.contains as-raw-event)))))
              filter-tuples)))]
    (let [res (tc/quick-check 25 prop :seed seed)]
      (is (:pass? res) (pr-str res)))))

(deftest firehose-test
  (let [metrics-fake (metrics/create-metrics)
        subs-atom (atom (subscribe/create-empty-subs))
        result-0-atom (atom nil)
        result-1-atom (atom nil)]
    (subscribe/subscribe! subs-atom "scope-0" "main-channel"
      [{} {:ids ["abc"]}]
      #(swap! result-0-atom (fnil conj []) %))
    (subscribe/subscribe! subs-atom "scope-1" "main-channel"
      [{}]
      #(swap! result-1-atom (fnil conj []) %))
    (subscribe/notify! metrics-fake subs-atom {:id "abc"} "<raw-evt>")
    (is (= @result-0-atom ["<raw-evt>"]))
    (is (= @result-1-atom ["<raw-evt>"]))
    (is (= 2 (subscribe/num-subscriptions subs-atom)))
    (is (= 2 (subscribe/num-firehose-filters subs-atom)))
    (is (= 2 (subscribe/num-filters subs-atom "scope-0")))
    (is (= 1 (subscribe/num-filters subs-atom "scope-1")))))

(deftest regression-test
  (support/with-regression-data [data-vec]
    ;; regression 0
    (let [metrics-fake (metrics/create-metrics)
          subs-atom (atom (subscribe/create-empty-subs))
          [_req req-id & req-filters] (#'app/parse (nth data-vec 0))
          raw-evt (nth data-vec 1)
          [_ evt] (#'app/parse raw-evt)
          result-atom (atom nil)]
      (subscribe/subscribe! subs-atom "scope-0" "main-channel"
        req-filters #(swap! result-atom (fnil conj []) %))
      (subscribe/notify! metrics-fake subs-atom evt raw-evt)
      (is (= @result-atom [raw-evt]))
      (is (= 1 (subscribe/num-subscriptions subs-atom)))
      (is (= 0 (subscribe/num-firehose-filters subs-atom)))
      (is (= 3 (subscribe/num-filters subs-atom "scope-0"))))
    ;; regression 1
    (let [metrics-fake (metrics/create-metrics)
          subs-atom (atom (subscribe/create-empty-subs))
          [_req req-id & req-filters] (#'app/parse (nth data-vec 2))
          raw-evt (nth data-vec 3)
          [_ evt] (#'app/parse raw-evt)
          result-atom (atom nil)]
      (subscribe/subscribe! subs-atom "scope-0" "main-channel"
        req-filters #(swap! result-atom (fnil conj []) %))
      (subscribe/notify! metrics-fake subs-atom evt raw-evt)
      (is (= @result-atom [raw-evt]))
      (is (= 1 (subscribe/num-subscriptions subs-atom)))
      (is (= 0 (subscribe/num-firehose-filters subs-atom)))
      (is (= 1 (subscribe/num-filters subs-atom "scope-0"))))))
