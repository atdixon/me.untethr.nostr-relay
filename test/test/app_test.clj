(ns test.app-test
  (:require
    [clojure.test :refer :all]
    [me.untethr.nostr.app :as app]
    [test.support :as support]))

(deftest prepare-req-filters*-test
  (is (= [] (#'app/prepare-req-filters* [])))
  (is (= [] (#'app/prepare-req-filters* [{:authors []}])))
  (is (= [{}] (#'app/prepare-req-filters* [{}])))
  (is (= [{}] (#'app/prepare-req-filters* [{} {}])))
  (is (= [{}] (#'app/prepare-req-filters* [{} {} {:authors []}])))
  (is (= [{} {:authors [support/fake-hex-str]}]
        (#'app/prepare-req-filters* [{} {} {:authors [support/fake-hex-str]}])))
  (is (= [{} {:authors [support/fake-hex-str]}]
        (#'app/prepare-req-filters* [{} {} {:authors [support/fake-hex-str]}
                                     {:authors [support/fake-hex-str]}]))))
