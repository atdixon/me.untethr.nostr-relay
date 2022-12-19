(ns test.validation-test
  (:require
    [clojure.test :refer :all]
    [me.untethr.nostr.validation :as validation]
    [test.support :as support]))

(deftest conform-filter-lenient-test
  (is (= {} (validation/conform-filter-lenient {})))
  ;ids kinds since until authors limit] e# :#e p# :#p
  (doseq [k [:ids :kinds :authors :#e :#p]]
    (is (= {k []} (validation/conform-filter-lenient {k []}))))
  (doseq [k [:since :until :limit]]
    (is (= {k 0} (validation/conform-filter-lenient {k 0}))))
  (doseq [k [:since :until]]
    (is (= {k 0} (validation/conform-filter-lenient {k 0})))
    (is (= {k 1671315671} (validation/conform-filter-lenient {k 1671315671})))
    (is (= {k 1671315671} (validation/conform-filter-lenient {k 1.671315671052E9}))))
  (doseq [k [:ids :authors :#e :#p]]
    (is (= {k [validation/zero-hex-str]}
          (validation/conform-filter-lenient {k ["bad"]})))
    (is (= {k [validation/zero-hex-str validation/zero-hex-str]}
          (validation/conform-filter-lenient {k ["bad" "bad"]})))
    (is (= {k [validation/zero-hex-str support/fake-hex-str]}
          (validation/conform-filter-lenient
            {k ["bad" support/fake-hex-str]})))))

(deftest filter-has-empty-attr?-test
  (is (not (validation/filter-has-empty-attr? {})))
  (is (not (validation/filter-has-empty-attr? {:b ["x"] :a ["y"]})))
  (is (validation/filter-has-empty-attr? {:a []}))
  (is (validation/filter-has-empty-attr? {:b ["x"] :a []})))
