(ns test.test-data-gen
  (:require
    [clojure.test :refer :all]
    [jsonista.core :as json]
    [me.untethr.nostr.app :as app]
    [me.untethr.nostr.crypt :as crypt]
    [clojure.java.io :as io])
  (:import
    (java.security SecureRandom)
    (java.util Random)
    (java.io BufferedWriter BufferedOutputStream BufferedInputStream File)
    (com.fasterxml.jackson.databind ObjectMapper MappingJsonFactory)
    (com.fasterxml.jackson.core JsonGenerator$Feature)
    (clojure.lang Indexed)))

(defonce ^Random random (Random.))
(defonce ^SecureRandom secure-random (SecureRandom.))

(defn- fast-random-event
  [pk-hex sk-bytes aux-bytes content created-at]
  (let [event' (hash-map
                 :pubkey pk-hex
                 :created_at created-at
                 :kind 1
                 :tags [#_["p" (crypt/sha-256 (byte-array 32))]]
                 :content content)
        id (#'app/calc-event-id event')
        id-bytes (crypt/hex-decode id)
        sig-bytes (crypt/sign sk-bytes id-bytes aux-bytes)]
    (assoc event'
      :id id
      :sig (crypt/hex-encode sig-bytes))))

(defn- slow-random-event
  [content created-at]
  (let [[pk sk] (crypt/generate-keypair)
        aux-bytes (byte-array 32)
        _ (.nextBytes secure-random aux-bytes)]
    (#'app/as-verified-event
      (fast-random-event
        (crypt/hex-encode pk) sk aux-bytes content created-at))))

(defn- create-json-mapper
  []
  (json/object-mapper
    {:encode-key-fn name
     :factory
     (doto (MappingJsonFactory.)
       (.disable JsonGenerator$Feature/AUTO_CLOSE_TARGET))}))

(defn generate-key-pool!
  [n f]
  (with-open [^BufferedOutputStream o (io/output-stream f)]
    (doseq [[pk sk] (repeatedly n #(crypt/generate-keypair))]
      (.write o ^bytes pk)
      (.write o ^bytes sk))))

(defn load-key-pool!
  [f]
  (let [^File f (io/as-file f) f-len (.length f)]
    (assert (zero? (rem f-len 64)))
    (let [arr (make-array Object (quot (quot f-len 32) 2))]
      (with-open [^BufferedInputStream in (io/input-stream f)]
        (dotimes [idx (alength arr)]
          (let [pk (byte-array 32)
                _ (assert (= 32 (.read in pk)))
                sk (byte-array 32)
                _ (assert (= 32 (.read in sk)))]
            (aset arr idx [(crypt/hex-encode pk) pk sk]))))
      arr)))

(defn generate!
  [n-events keypool-f f]
  (let [^ObjectMapper json-mapper (create-json-mapper)
        pool (load-key-pool! keypool-f)
        aux-bytes (byte-array 32)
        _ (.nextBytes secure-random aux-bytes)]
    (with-open [^BufferedWriter w (io/writer f)]
      (dotimes [_ n-events]
        (let [[pk-hex _pk sk] (nth pool (.nextInt random (count pool)))
              e (fast-random-event pk-hex sk aux-bytes "hello" 0)]
          (json/write-value w ["EVENT" e] json-mapper)
          (.newLine w))))))

(comment
  (require '[clj-async-profiler.core :as prof])
  (prof/profile
    (generate! 1000 "./dump/keypool-10000" "./dump/events-1000"))
  (prof/serve-files 8080)

  (let [vfn @#'app/as-verified-event
        es (line-seq (io/reader "./dump/events-10000"))
        parsed-es (mapv (comp second @#'app/parse) es)
        n (count parsed-es)]
    (prof/profile
      (dotimes [_ 1000]
        (vfn (.nth ^Indexed parsed-es (rand-int n))))))

  (require '[criterium.core :as cc])
  (let [vfn @#'app/as-verified-event
        es (line-seq (io/reader "./dump/events-10000"))
        parsed-es (mapv (comp second @#'app/parse) es)
        n (count parsed-es)]
    (cc/quick-bench
      (vfn (.nth ^Indexed parsed-es (rand-int n)))))

  ;; FAST verify:

  ;Evaluation count : 9462 in 6 samples of 1577 calls.
  ;Execution time mean : 69.922071 µs
  ;Execution time std-deviation : 4.365818 µs
  ;Execution time lower quantile : 65.963976 µs ( 2.5%)
  ;Execution time upper quantile : 76.627744 µs (97.5%)
  ;Overhead used : 7.324508 ns

  ;; SLOW verify:

  ;Evaluation count : 24 in 6 samples of 4 calls.
  ;             Execution time mean : 22.353355 ms
  ;    Execution time std-deviation : 1.379761 ms
  ;   Execution time lower quantile : 21.150553 ms ( 2.5%)
  ;   Execution time upper quantile : 24.559130 ms (97.5%)
  ;                   Overhead used : 7.276868 ns
  ;
  ;Found 1 outliers in 6 samples (16.6667 %)
  ;	low-severe	 1 (16.6667 %)
  ; Variance from outliers : 14.7680 % Variance is moderately inflated by outliers

  (let [vfn @#'app/as-verified-event
        es (line-seq (io/reader "./dump/events-10000"))
        e (map (comp @#'app/as-verified-event second @#'app/parse) es)]
    (time
      (dorun (take 1 e)))))
