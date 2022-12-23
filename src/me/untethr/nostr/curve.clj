(ns me.untethr.nostr.curve
  (:refer-clojure :exclude [infinite?]))

(defrecord Point [^BigInteger x ^BigInteger y])

(def infinity (->Point nil nil))

(def ^BigInteger zero BigInteger/ZERO)
(def ^BigInteger one BigInteger/ONE)
(def ^BigInteger two BigInteger/TWO)
(def ^BigInteger three (BigInteger/valueOf 3))
(def ^BigInteger four (BigInteger/valueOf 4))
(def ^BigInteger seven (BigInteger/valueOf 7))

(def ^BigInteger p
  (biginteger 16rFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F))
(def ^BigInteger n
  (biginteger 16rFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141))
(def ^BigInteger e
  (.divide (.add p one) four))

(def G
  (->Point
    (biginteger 16r79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798)
    (biginteger 16r483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8)))

(def ^BigInteger p-minus-two
  (.subtract p two))

(defn- infinite?
  [p]
  (or (nil? (:x p)) (nil? (:y p))))

(defn add
  [p1 p2]
  (let [^BigInteger x1 (:x p1)
        ^BigInteger y1 (:y p1)
        ^BigInteger x2 (:x p2)
        ^BigInteger y2 (:y p2)
        infinite-p1? (infinite? p1)
        infinite-p2? (infinite? p2)]
    (cond
      (and infinite-p1? infinite-p2?) infinity
      infinite-p1? p2
      infinite-p2? p1
      (and (= x1 x2) (not= y1 y2)) infinity
      :else
      (let [lam (if (and (= x1 x2) (= y1 y2))
                  (.mod
                    (.multiply (.multiply (.multiply three x1) x1)
                      (.modPow (.multiply y2 two) p-minus-two p))
                    p)
                  (.mod
                    (.multiply (.subtract y2 y1)
                      (.modPow (.subtract x2 x1) p-minus-two p))
                    p))
            x3 (.mod (.subtract (.subtract (.multiply lam lam) x1) x2) p)]
        (->Point x3 (.mod (.subtract (.multiply lam (.subtract x1 x3)) y1) p))))))

(defn mul
  ^Point [p ^BigInteger n]
  (loop [i 0 R infinity P p]
    (if (= i 256)
      R
      (recur
        (inc i)
        (if (pos? (.compareTo (.and (.shiftRight n i) one) zero))
          (add R P) R)
        (add P P)))))
