(ns test.extra-test
  (:require [clojure.test :refer :all]
            [me.untethr.nostr.extra :as extra]
            [me.untethr.nostr.util :as util]))

(deftest query-params->filter
  (is (nil? (#'extra/query-params->filter nil)))
  (is (nil? (#'extra/query-params->filter {})))
  (is (nil? (#'extra/query-params->filter {"unsupported-key" 100})))
  (is (= {:limit 10} (#'extra/query-params->filter {"limit" "10"})))
  (is (= {:limit 10} (#'extra/query-params->filter {"limit" "10" "unsupported-key" 100})))
  (is (= {:limit 10 :authors ["xyz"]} (#'extra/query-params->filter {"limit" "10" "author" "xyz"})))
  (with-redefs [util/current-system-epoch-seconds (constantly 1234)]
    (is (= {:since 0 :until 1234
            :authors ["xyz"] :ids ["abc"]}
          (#'extra/query-params->filter
            {"since" 0 "until" "now" "author" "xyz" "id" "abc"})))))
