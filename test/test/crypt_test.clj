(ns test.crypt-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.string :as str]
            [me.untethr.nostr.crypt.crypt :as crypt]
            [clojure.tools.logging :as log]))

;; https://bips.xyz/340
;; https://bips.xyz/340#test-vectors-and-reference-code

(deftest verify-test
  []
  (with-open [reader
              (io/reader
                (io/resource "test/test-vectors.csv"))]
    (doseq [[index secret pubkey aux-rand msg sig verification comment]
            (drop 1 (csv/read-csv reader))]
      (when (not-empty secret)
        (is (= (str/lower-case pubkey)
              (-> secret
                crypt/hex-decode
                crypt/generate-pubkey
                crypt/hex-encode)))
        (doseq [sign-fn [crypt/sign crypt/slow-sign]]
          (is (=
                (some->
                  (sign-fn
                    (crypt/hex-decode secret)
                    (crypt/hex-decode msg)
                    (crypt/hex-decode aux-rand))
                  crypt/hex-encode)
                (str/lower-case sig))
            (str index ": " comment))))
      (doseq [verify-fn [crypt/verify crypt/slow-verify]]
        (is (=
              (verify-fn
                (crypt/hex-decode pubkey)
                (crypt/hex-decode msg)
                (crypt/hex-decode sig))
              (Boolean/parseBoolean verification))
          (str index ": " comment))))))
