(ns test.conf-test
  (:require
    [clojure.test :refer :all]
    [me.untethr.nostr.conf :as conf])
  (:import (java.io StringReader)))

(deftest conf-test
  (with-open [reader (StringReader.
                       (str
                         "http:\n"
                         "  port: 1234\n"
                         "sqlite:\n"
                         "  file: \"nx.db\"\n"))]
    (let [parsed (conf/parse-conf reader)]
      (is (= "nx.db" (:sqlite-file parsed)))
      (is (= 1234 (:http-port parsed)))
      (is (nil? (:optional-hostname parsed)))
      (is (nil? (:optional-supported-kinds-range-set parsed)))))

  (with-open [reader (StringReader.
                       (str
                         "http:\n"
                         "  port: 1234\n"
                         "sqlite:\n"
                         "  file: \"nx.db\"\n"
                         "supported-kinds: [\"0-3\", \"99\"]\n"
                         "hostname: test.test"))]
    (let [parsed (conf/parse-conf reader)]
      (is (= "nx.db" (:sqlite-file parsed)))
      (is (= 1234 (:http-port parsed)))
      (is (= "test.test" (:optional-hostname parsed)))
      (doseq [k [0 1 2 3 99]]
        (conf/supports-kind? parsed k)))))
