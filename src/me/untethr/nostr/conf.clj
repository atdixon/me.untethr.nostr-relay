(ns me.untethr.nostr.conf
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str])
  (:import (com.google.common.collect ImmutableRangeSet ImmutableRangeSet$Builder Range RangeSet)
           (java.io Reader)))

(defrecord Conf
  [^String optional-hostname
   ^Integer http-port
   ^String sqlite-file
   ^RangeSet optional-supported-kinds-range-set])

(defn pretty* [{:keys [optional-hostname
                       http-port
                       sqlite-file
                       optional-supported-kinds-range-set] :as _conf}]
  (str/join
    "\n"
    [(format "hostname: %s" (or optional-hostname "none specified"))
     (format "port: %d" http-port)
     (format "database file: %s" sqlite-file)
     (format "supported nip-1 kinds: %s" (or (some-> optional-supported-kinds-range-set str) "all of them"))]))

(defn parse-supported-kinds*
  "Answers nil if there is no explicit :supported-kinds key in the provided
   conf. Downstream nil will be intepreted such that all kinds are stored."
  ^RangeSet [from-yaml]
  (some->> from-yaml
    :supported-kinds
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

(defn supports-kind?
  [^Conf conf kind]
  (and
    (number? kind)
    (if-let [^RangeSet supported-kinds-set (:optional-supported-kinds-range-set conf)]
      (.contains supported-kinds-set (long kind))
      ;; when range set is nil, we will support every kind
      true)))

(defn ^Conf parse-conf
  [^Reader reader]
  (let [from-yaml (yaml/parse-stream reader)]
    (->Conf
      (get-in from-yaml [:hostname])
      (long (get-in from-yaml [:http :port]))
      (get-in from-yaml [:sqlite :file])
      (parse-supported-kinds* from-yaml))))
