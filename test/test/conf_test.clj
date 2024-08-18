(ns test.conf-test
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [me.untethr.nostr.conf :as conf])
  (:import (java.io StringReader)))

(deftest default-config-test
  []
  ;; assume we're running tests from project root so we can load default conf
  ;; by relative path here:
  (let [parsed-conf (with-open [r (io/reader "conf/relay.yaml")]
                      (conf/parse-conf r))]
    ;; basic sniff testing
    (is (number? (:http-port parsed-conf)))
    (is (boolean? (:websockets-enable-batch-mode? parsed-conf)))
    (is (boolean? (:websockets-disable-permessage-deflate? parsed-conf)))))

(deftest conf-test
  (with-open [reader (StringReader.
                       (str
                         "http:\n"
                         "  host: 127.0.0.1\n"
                         "  port: 1234\n"
                         "sqlite:\n"
                         "  file: \"nx.db\"\n"))]
    (let [parsed (conf/parse-conf reader)]
      (is (= "nx.db" (:sqlite-file parsed)))
      (is (= 1234 (:http-port parsed)))
      (is (= "127.0.0.1" (:http-host parsed)))
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
        (conf/supports-kind? parsed k))))

  (with-open [reader (StringReader.
                       (str
                         "http:\n"
                         "  port: 1234\n"
                         "sqlite:\n"
                         "  file: \"nx.db\"\n"
                         "hostname: test.test\n"
                         "websockets:\n"
                         "   enable-batch-mode: xtrue"))]
    (is (thrown? AssertionError (conf/parse-conf reader)))))
