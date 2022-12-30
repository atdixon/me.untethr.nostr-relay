(ns test.fulfill-test
  (:require [clojure.test :refer :all]
            [me.untethr.nostr.fulfill :as fulfill]
            [me.untethr.nostr.app :as app]
            [test.support :as support]
            [test.test-data :as test-data]
            [me.untethr.nostr.metrics :as metrics])
  (:import (java.util.concurrent Future TimeUnit Semaphore)))

(deftest fulfill-test
  (support/with-memory-db [db]
    (support/load-data db (:pool test-data/pool-with-filters))
    (let [results-atom (atom [])
          eose-atom (atom 0)
          fulfill-atom (atom (fulfill/create-empty-registry))
          ^Future f (fulfill/submit! (metrics/create-metrics)
                      db fulfill-atom "chan-id" "req-id" [{:since 110} {:since 120}] 5
                      (fn [res]
                        (swap! results-atom conj res))
                      #(swap! eose-atom inc))
          ;; cause test to swap! if inc the async work
          _ (.get f 1000 TimeUnit/MILLISECONDS)]
      (is (.isDone f))
      (is (= 0 (fulfill/num-active-fulfillments fulfill-atom)))
      (is (= 4 (count @results-atom)))
      (is (= 1 @eose-atom))
      ;; verify the results are json strings that can be parsed
      (is (= (into #{} (subvec (:pool test-data/pool-with-filters) 1 5))
            (into #{} (map #'app/parse) @results-atom)))
      ;; ensure cancellation -- a no-op, now that we're done -- leaves us
      ;; with an empty registry
      (fulfill/cancel! fulfill-atom "chan-id" "req-id")
      (is (= (fulfill/create-empty-registry) @fulfill-atom)))))

(deftest fulfill-with-batching-test
  (with-redefs [fulfill/batch-size 2]
    (support/with-memory-db [db]
      (support/load-data db (:pool test-data/pool-with-filters))
      (let [results-atom (atom [])
            eose-atom (atom 0)
            fulfill-atom (atom (fulfill/create-empty-registry))
            ^Future f (fulfill/submit-use-batching! (metrics/create-metrics)
                        db fulfill-atom "chan-id" "req-id" [{:since 110} {:since 120}] 5
                        (fn [res]
                          (swap! results-atom conj res))
                        #(swap! eose-atom inc))
            ;; cause test to swap! if inc the async work
            _ (.get f 1000 TimeUnit/MILLISECONDS)]
        (is (.isDone f))
        (is (= 0 (fulfill/num-active-fulfillments fulfill-atom)))
        (is (= 4 (count @results-atom)))
        (is (= 1 @eose-atom))
        ;; verify the results are json strings that can be parsed
        (is (= (into #{} (subvec (:pool test-data/pool-with-filters) 1 5))
              (into #{} (map #'app/parse) @results-atom)))
        ;; ensure cancellation -- a no-op, now that we're done -- leaves us
        ;; with an empty registry
        (fulfill/cancel! fulfill-atom "chan-id" "req-id")
        (is (= (fulfill/create-empty-registry) @fulfill-atom))))))

(defn- run-interruption-test
  [cancel-fn]
  (support/with-memory-db [db]
    (support/load-data db (:pool test-data/pool-with-filters))
    (let [semaphore (Semaphore. 0)
          results-atom (atom [])
          eose-atom (atom 0)
          fulfill-atom (atom (fulfill/create-empty-registry))
          ^Future f (fulfill/submit! (metrics/create-metrics)
                      db fulfill-atom "chan-id" "req-id" [{:since 110} {:since 120}] 5
                      (fn [res]
                        ;; block so our cancellation is guaranteed to cancel
                        ;; us in media res
                        (.acquire semaphore)
                        (swap! results-atom conj res))
                      #(swap! eose-atom inc))]
      (is (= 1 (fulfill/num-active-fulfillments fulfill-atom)))
      (Thread/sleep 100) ;; in most cases we want to exercise the true interruption path
      (is (= 1 (fulfill/num-active-fulfillments fulfill-atom)))
      (cancel-fn fulfill-atom "chan-id" "req-id")
      (.release semaphore)
      (is (.isCancelled f))
      (is (= 0 (fulfill/num-active-fulfillments fulfill-atom)))
      (is (= 1 (count @results-atom)))
      (is (= 0 @eose-atom))
      ;; ensure cancellation leaves us with an empty registry
      (is (= (fulfill/create-empty-registry) @fulfill-atom)))))

(deftest fulfill-interruption-test
  (run-interruption-test
    (fn [fulfill-atom chan-id req-id]
      (fulfill/cancel! fulfill-atom chan-id req-id)))
  (run-interruption-test
    (fn [fulfill-atom chan-id _req-id]
      (fulfill/cancel-all! fulfill-atom chan-id))))

(defn- run-interruption-with-batching-test
  [cancel-fn]
  (support/with-memory-db [db]
    (support/load-data db (:pool test-data/pool-with-filters))
    (let [semaphore (Semaphore. 0)
          results-atom (atom [])
          eose-atom (atom 0)
          fulfill-atom (atom (fulfill/create-empty-registry))
          ^Future f (fulfill/submit-use-batching! (metrics/create-metrics)
                      db fulfill-atom "chan-id" "req-id" [{:since 110} {:since 120}] 5
                      (fn [res]
                        ;; block so our cancellation is guaranteed to cancel
                        ;; us in media res
                        (.acquire semaphore)
                        (swap! results-atom conj res))
                      #(swap! eose-atom inc))]
      (is (= 1 (fulfill/num-active-fulfillments fulfill-atom)))
      (Thread/sleep 100) ;; in most cases we want to exercise the true interruption path
      (is (= 1 (fulfill/num-active-fulfillments fulfill-atom)))
      (cancel-fn fulfill-atom "chan-id" "req-id")
      (.release semaphore)
      (is (.isCancelled f))
      (is (= 0 (fulfill/num-active-fulfillments fulfill-atom)))
      (is (= 1 (count @results-atom)))
      (is (= 0 @eose-atom))
      ;; ensure cancellation leaves us with an empty registry
      (is (= (fulfill/create-empty-registry) @fulfill-atom)))))

(deftest fulfill-interruption-test
  (run-interruption-with-batching-test
    (fn [fulfill-atom chan-id req-id]
      (fulfill/cancel! fulfill-atom chan-id req-id)))
  (run-interruption-with-batching-test
    (fn [fulfill-atom chan-id _req-id]
      (fulfill/cancel-all! fulfill-atom chan-id))))
