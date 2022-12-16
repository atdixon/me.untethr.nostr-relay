(ns test.validation-test
  (:require
    [clojure.test :refer :all]
    [me.untethr.nostr.validation :as validation]))

(def ^:private hex-chars "abcdef0123456789")

(def ^:private fake-hex-str (apply str (take 32 (cycle hex-chars))))

(deftest conform-filter-lenient-test
  []
  (is (= {} (validation/conform-filter-lenient {})))
  ;ids kinds since until authors limit] e# :#e p# :#p
  (doseq [k [:ids :kinds :authors :#e :#p]]
    (is (= {k []} (validation/conform-filter-lenient {k []}))))
  (doseq [k [:since :until :limit]]
    (is (= {k 0} (validation/conform-filter-lenient {k 0}))))
  (is (= {:authors [validation/zero-hex-str]}
        (validation/conform-filter-lenient {:authors ["bad"]})))
  (is (= {:authors [validation/zero-hex-str validation/zero-hex-str]}
        (validation/conform-filter-lenient {:authors ["bad" "bad"]})))
  (is (= {:authors [validation/zero-hex-str fake-hex-str]}
        (validation/conform-filter-lenient
          {:authors ["bad" fake-hex-str]}))))
