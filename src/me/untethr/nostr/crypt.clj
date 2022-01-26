(ns me.untethr.nostr.crypt
  (:require [me.untethr.nostr.curve :as c])
  (:import (java.util Arrays)
           (java.security MessageDigest SecureRandom)
           (java.nio.charset StandardCharsets)
           (fr.acinq.secp256k1 Hex Secp256k1 Secp256k1Exception)))

(defn hex-encode
  ^String [^bytes data]
  (Hex/encode data))

(defn hex-decode
  ^bytes [^String data]
  (Hex/decode data))

(defn sha-256
  ^bytes [^bytes input]
  ;; MessageDigest/getInstance is cheap and instances aren't thread-safe
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (.digest digest input)))

(defn- biginteger*
  ^BigInteger [^bytes b]
  (BigInteger. 1 b))

(defn- bytes*
  ^bytes [^BigInteger num]
  (let [^bytes b (.toByteArray num) b-len (alength ^bytes b)]
    (case b-len
      32 b
      (if (> b-len 32)
        (Arrays/copyOfRange b (int (- b-len 32)) (int b-len))
        (let [rv (byte-array 32)]
          (System/arraycopy b 0 rv (- 32 b-len) b-len)
          rv)))))

(defn- bytes-xor
  [^bytes a ^bytes b]
  {:pre [(= (alength a) (alength b))]}
  (let [a-len (alength a)
        ^bytes rv (byte-array a-len)]
    (dotimes [i a-len]
      (aset rv i (byte (bit-xor (aget a i) (aget b i)))))
    rv))

(defn- ||
  ^bytes [& byte-arrays]
  (let [tuples (mapv vector
                 (concat byte-arrays [nil])
                 (reductions #(+ %1 (alength ^bytes %2)) 0 byte-arrays))
        rv (byte-array (second (peek tuples)))]
    (doseq [[arr offset] (butlast tuples)]
      (System/arraycopy arr 0 rv offset (alength ^bytes arr)))
    rv))

(defn- lift-x
  [^BigInteger x]
  (when (and (not (neg? (.signum x))) (neg? (.compareTo x c/p)))
    (let [^BigInteger c (.mod (.add (.modPow x c/three c/p) c/seven) c/p)
          ^BigInteger y (.modPow c c/e c/p)]
      (when (zero? (.compareTo c (.modPow y c/two c/p)))
        (c/->Point x (if (zero? (.mod y c/two)) y (.subtract c/p y)))))))


(def ^bytes tag-hash-challenge
  (sha-256 (.getBytes "BIP0340/challenge" StandardCharsets/UTF_8)))

(def ^bytes tag-hash-challenge-x2
  (|| tag-hash-challenge tag-hash-challenge))

(def ^bytes tag-hash-aux
  (sha-256 (.getBytes "BIP0340/aux" StandardCharsets/UTF_8)))

(def ^bytes tag-hash-aux-x2
  (|| tag-hash-aux tag-hash-aux))

(def ^bytes tag-hash-nonce
  (sha-256 (.getBytes "BIP0340/nonce" StandardCharsets/UTF_8)))

(def ^bytes tag-hash-nonce-x2
  (|| tag-hash-nonce tag-hash-nonce))

(defn slow-verify
  ;; @see https://bips.xyz/340#verification
  [^bytes public-key ^bytes message ^bytes signature]
  (or
    (when (and
            (= 32 (alength ^bytes public-key) (alength ^bytes message))
            (= 64 (alength ^bytes signature)))
      (when-let [P (lift-x (biginteger* public-key))]
        (let [r-bytes (Arrays/copyOfRange signature 0 32)
              r (biginteger* r-bytes)
              s (biginteger* (Arrays/copyOfRange signature 32 64))]
          (when (and
                  (< (.compareTo r c/p) 0)
                  (< (.compareTo s c/n) 0))
            (let [e (.mod
                      (biginteger*
                        (sha-256
                          (|| tag-hash-challenge-x2 r-bytes public-key message))) c/n)
                  R (c/add (c/mul c/G s) (c/mul P (.subtract c/n e)))]
              (when
                (and
                  (not= R c/infinity)
                  (zero? (.compareTo (.mod ^BigInteger (:y R) c/two) c/zero))
                  (zero? (.compareTo ^BigInteger (:x R) r)))
                true))))))
    false))

(defn slow-sign
  (^bytes [^bytes private-key ^bytes message ^bytes aux-rand]
   (slow-sign private-key message aux-rand true))
  (^bytes [^bytes private-key ^bytes message ^bytes aux-rand verify?]
   (let [d' (biginteger* private-key)]
     (when-not (or (zero? (.compareTo c/zero d')) (>= d' c/n))
       (let [P (c/mul c/G d')
             ^bytes bytes-P (bytes* (:x P))
             ^BigInteger d (if (zero? (.compareTo (.mod ^BigInteger (:y P) c/two) c/zero))
                             d'
                             (.subtract c/n d'))
             d-bytes (bytes* d)
             t-bytes (bytes-xor d-bytes (sha-256 (|| tag-hash-aux-x2 aux-rand)))
             rand (sha-256 (|| tag-hash-nonce-x2 t-bytes bytes-P message))
             k' (.mod (biginteger* rand) c/n)]
         (when-not (zero? (.compareTo k' c/zero))
           (let [R (c/mul c/G k')
                 bytes-R (bytes* (:x R))
                 ^BigInteger k (if (zero? (.compareTo (.mod ^BigInteger (:y R) c/two) c/zero))
                                 k'
                                 (.subtract c/n k'))
                 ^BigInteger e (.mod
                                 (biginteger*
                                   (sha-256 (|| tag-hash-challenge-x2 bytes-R bytes-P message)))
                                 c/n)
                 ^bytes sig (|| bytes-R (bytes* (.mod (.add k (.multiply e d)) c/n)))]
             (when (and verify? (not (slow-verify bytes-P message sig)))
               (throw (ex-info "computed signature didn't verify" {:sig sig})))
             sig)))))))

(defonce ^:private random (SecureRandom.)) ;; is thread-safe

(defn generate-pubkey
  ^bytes [^bytes private-key]
  (let [d' (biginteger* private-key)]
    (when-not (or (zero? (.compareTo c/zero d')) (>= d' c/n))
      (bytes* (:x (c/mul c/G d'))))))

(defn generate-keypair
  ^bytes []
  (let [private-key (byte-array 32)
        _ (.nextBytes random private-key)]
    (when-let [pubkey (generate-pubkey private-key)]
      [pubkey private-key])))

;; ---

;; sign and verify that are way faster than our bespoke impls above:

(defonce ^Secp256k1 secp256k1 (Secp256k1/get))

(defn verify
  [^bytes public-key ^bytes message ^bytes signature]
  (try
    (.verifySchnorr secp256k1 signature message public-key)
    (catch Secp256k1Exception e
      false)))

(defn sign
  ^bytes [^bytes private-key ^bytes message ^bytes aux-rand]
  (.signSchnorr secp256k1 message private-key aux-rand))

;; example:
;; (verify
;;  (hex-decode "aff9a9f017f32b2e8b60754a4102db9d9cf9ff2b967804b50e070780aa45c9a8")
;;  (hex-decode "2a3a85a53e99af51eb6b3303fb4c902594827f4c9da0a183f743054a9e3d3a33")
;;  (hex-decode "0e049520f7683ab9ca1c3f50243e09c11ace8fcabb0e2fcdd80861b9802d83180ca0bed00f42a032ac780152a3b3a5c01c136a271a7d360379a1f29e13eceb9d"))
