(ns me.untethr.nostr.conf
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str])
  (:import (com.google.common.collect ImmutableRangeSet ImmutableRangeSet$Builder Range RangeSet)
           (java.io Reader)))

;; known dependencies: test.config-test/make-test-conf
(defrecord Conf
  [^String optional-hostname
   ^String http-host
   ^Long http-port
   ^String sqlite-file
   ^String sqlite-kv-file
   ^RangeSet optional-supported-kinds-range-set
   ^RangeSet optional-unserved-kinds-range-set
   ^Long optional-max-content-length
   ^Long optional-max-created-at-delta
   ^Long websockets-max-outgoing-frames
   websockets-disable-permessage-deflate?
   websockets-enable-batch-mode?
   nip42-auth-enabled?])

(defn pretty* [{:keys [optional-hostname
                       http-host
                       http-port
                       sqlite-file
                       sqlite-kv-file
                       optional-supported-kinds-range-set
                       optional-unserved-kinds-range-set
                       optional-max-content-length
                       optional-max-created-at-delta
                       websockets-max-outgoing-frames
                       websockets-disable-permessage-deflate?
                       websockets-enable-batch-mode?
                       nip42-auth-enabled?] :as _conf}]
  (str/join
    "\n"
    [(format "hostname: %s" (or optional-hostname "none specified"))
     (format "bind address: %s" http-host)
     (format "port: %d" http-port)
     (format "database file: %s" sqlite-file)
     (format "database file (k/v store): %s" sqlite-kv-file)
     (format "max-content-length: %s" (or optional-max-content-length "<unlimited>"))
     (format "max-created-at-delta: %s" (or optional-max-created-at-delta "<unlimited>"))
     (format "websockets-max-outgoing-frames: %s" (or websockets-max-outgoing-frames "<unlimited>"))
     (format "websockets-disable-permessage-deflate: %s" (or websockets-disable-permessage-deflate? false))
     (format "websockets-enable-batch-mode: %s" (or websockets-enable-batch-mode? false))
     (format "supported nip-1 kinds: %s" (or (some-> optional-supported-kinds-range-set str) "all of them"))
     (format "unserved nip-1 kinds: %s" (or (some-> optional-unserved-kinds-range-set str) "none of them"))
     (format "nip42-auth-enabled: %s" (if nip42-auth-enabled? "yes" "no"))]))

(defn- parse-kinds-ranges-vec
  [kinds-ranges-vec]
  (->>
    kinds-ranges-vec
    (map
      (fn [part]
        (if-let [[_ a b] (re-matches #"^(\d+)-(\d+)$" part)]
          (Range/closed (Long/parseLong a) (Long/parseLong b))
          (Range/closed (Long/parseLong part) (Long/parseLong part)))))
    ^ImmutableRangeSet$Builder
    (reduce
      (fn [^ImmutableRangeSet$Builder acc ^Range r]
        (.add acc r))
      (ImmutableRangeSet/builder))
    .build))

(defn parse-supported-kinds*
  "Answers nil if there is no explicit :supported-kinds key in the provided
   conf. Downstream nil will be intepreted such that all kinds are stored."
  ^RangeSet [from-yaml]
  {:post [(or (nil? %) (instance? ImmutableRangeSet %))]}
  (some->> from-yaml
    :supported-kinds
    parse-kinds-ranges-vec))

(defn parse-unserved-kinds*
  "Answers nil if there is no explicit :unserved-kinds key in the provided
   conf. Downstream nil will be intepreted such that no kinds are unserved."
  ^RangeSet [from-yaml]
  {:post [(or (nil? %) (instance? ImmutableRangeSet %))]}
  (some->> from-yaml
    :unserved-kinds
    parse-kinds-ranges-vec))

(defn supports-kind?
  ;; i.e, do we *store* this kind of event
  [^Conf conf kind]
  (and
    (number? kind)
    (if-let [^RangeSet supported-kinds-set (:optional-supported-kinds-range-set conf)]
      (.contains supported-kinds-set (long kind))
      ;; when range set is nil, we will support every kind
      true)))

(defn serves-kind?
  [^Conf conf kind]
  (and
    (number? kind)
    (if-let [^RangeSet unserved-kinds-set (:optional-unserved-kinds-range-set conf)]
      ;; crucially we negate containment - ie serve only if kind is not in explicit
      ;; blacklist:
      (not (.contains unserved-kinds-set (long kind)))
      ;; when range set is nil, we will serve every kind!
      true)))

(defn ^Conf parse-conf
  [^Reader reader]
  ;; consider use of spec to validate parsed conf
  {:post [(or (nil? (:websockets-disable-permessage-deflate? %))
            (boolean? (:websockets-disable-permessage-deflate? %)))
          (or
            (nil? (:websockets-enable-batch-mode? %))
            (boolean? (:websockets-enable-batch-mode? %)))]}
  (let [from-yaml (yaml/parse-stream reader)]
    (->Conf
      (get-in from-yaml [:hostname])
      (get-in from-yaml [:http :host])
      (long (get-in from-yaml [:http :port]))
      (get-in from-yaml [:sqlite :file])
      (get-in from-yaml [:sqlite :file-kv])
      (parse-supported-kinds* from-yaml)
      (parse-unserved-kinds* from-yaml)
      (some-> (get from-yaml :max-content-length) long)
      (some-> (get from-yaml :max-created-at-delta) long)
      (get-in from-yaml [:websockets :max-outgoing-frames])
      (get-in from-yaml [:websockets :disable-permessage-deflate])
      (get-in from-yaml [:websockets :enable-batch-mode])
      (get-in from-yaml [:auth :enabled]))))
