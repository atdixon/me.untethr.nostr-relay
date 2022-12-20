(ns test.app-test
  (:require
    [clojure.test :refer :all]
    [me.untethr.nostr.app :as app]
    [me.untethr.nostr.conf :as conf]
    [test.support :as support]))

(deftest prepare-req-filters*-test
  (let [conf (conf/->Conf nil 1234 "nx.db" nil)]
    (is (= [] (#'app/prepare-req-filters* conf [])))
    (is (= [] (#'app/prepare-req-filters* conf [{:authors []}])))
    (is (= [{}] (#'app/prepare-req-filters* conf [{}])))
    (is (= [{}] (#'app/prepare-req-filters* conf [{} {}])))
    (is (= [{}] (#'app/prepare-req-filters* conf [{} {} {:authors []}])))
    (is (= [{} {:authors [support/fake-hex-str]}]
          (#'app/prepare-req-filters* conf [{} {} {:authors [support/fake-hex-str]}])))
    (is (= [{} {:authors [support/fake-hex-str]}]
          (#'app/prepare-req-filters* conf [{} {} {:authors [support/fake-hex-str]}
                                            {:authors [support/fake-hex-str]}])))
    (is (= [{} {:authors [support/fake-hex-str]} {:authors [support/fake-hex-str] :kinds [1 2 3]}]
          (#'app/prepare-req-filters* conf [{} {} {:authors [support/fake-hex-str]}
                                            {:authors [support/fake-hex-str]
                                             :kinds [1 2 3]}]))))
  (let [conf (conf/->Conf nil 1234 "nx.db"
               (conf/parse-supported-kinds* {:supported-kinds ["1-2"]}))]
    (is (= [] (#'app/prepare-req-filters* conf [])))
    (is (= [] (#'app/prepare-req-filters* conf [{:authors []}])))
    (is (= [{}] (#'app/prepare-req-filters* conf [{}])))
    (is (= [{}] (#'app/prepare-req-filters* conf [{} {}])))
    (is (= [{}] (#'app/prepare-req-filters* conf [{} {} {:authors []}])))
    (is (= [{} {:authors [support/fake-hex-str]}]
          (#'app/prepare-req-filters* conf [{} {} {:authors [support/fake-hex-str]}])))
    (is (= [{} {:authors [support/fake-hex-str]}]
          (#'app/prepare-req-filters* conf [{} {} {:authors [support/fake-hex-str]}
                                            {:authors [support/fake-hex-str]}])))
    (is (= [{} {:authors [support/fake-hex-str]} {:authors [support/fake-hex-str] :kinds [1 2 3]}]
          (#'app/prepare-req-filters* conf [{} {} {:authors [support/fake-hex-str]}
                                            {:authors [support/fake-hex-str]
                                             :kinds [1 2 3]}])))
    ;; note: here if none of the kinds a filter references supports, the filter is removed:
    (is (= [{} {:authors [support/fake-hex-str]}]
          (#'app/prepare-req-filters* conf [{} {} {:authors [support/fake-hex-str]}
                                            {:authors [support/fake-hex-str]
                                             :kinds [3 4]}])))))
