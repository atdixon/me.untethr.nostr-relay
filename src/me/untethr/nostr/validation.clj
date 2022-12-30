(ns me.untethr.nostr.validation
  (:require
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [me.untethr.nostr.json-facade :as json-facade]
    [clojure.string :as str]
    [clojure.set :as set])
  (:import
    (com.fasterxml.jackson.core JsonParseException)))

(defn- kind? [k]
  (nat-int? k))

(defn- hex-str? [s]
  (and (string? s) (re-matches #"[a-f0-9]{32,}" s)))

(defn hex-str-64? [s] ;; public
  (and (string? s) (re-matches #"[a-f0-9]{64}" s)))

(defn- hex-str-64-or-prefix? [s]
  (and (string? s) (re-matches #"[a-f0-9]{1,64}" s)))

(defn- timestamp? [t]
  ;; note: we support negative timestamps here (as some clients in the wild
  ;; have sent them)
  (integer? t))

(defn valid-username? [value]
  (or (nil? value)
    (and (string? value)
      (some? (re-matches #"\w[\w-]+\w" value)))))

(defn kind-0-event-err
  [{:keys [content]}]
  (cond
    (or (nil? content) (str/blank? content)) :err/no-content-in-metadata-event
    :else (if-let [parsed (try
                            (json-facade/parse content)
                            (catch JsonParseException e nil))]
            (when-not (valid-username? (:username parsed))
              :err/invalid-username-in-metadata-event)
            :err/bad-json-content-in-metadata-event)))

(defn is-valid-id-form?
  [id]
  (hex-str-64? id))

(defn event-err
  [{:keys [id pubkey created_at kind tags content sig] :as e}]
  (cond
    (not (is-valid-id-form? id)) :err/id
    (not (hex-str-64? pubkey)) :err/pubkey
    (not (hex-str? sig)) :err/sig
    (not (timestamp? created_at)) :err/created-at
    (not (kind? kind)) :err/kind
    (not (or
           (nil? tags)
           (vector? tags))) :err/tag
    (not (or (nil? content) (string? content))) :err/content
    (= kind 0) (kind-0-event-err e)))

(defn- every?*
  [pred x]
  (or (nil? x) (and (vector? x) (every? pred x))))

(def ^:private valid-filter-keys
  (into
    #{:ids :authors :kinds :since :until :limit}
    common/allowed-filter-tag-queries-set))

(defn invalid-filter-keys
  [the-filter]
  (set/difference (set (keys the-filter)) valid-filter-keys))

(defn filter-err
  [{:keys [ids kinds since until authors limit] e# :#e p# :#p :as the-filter}]
  (cond
    (not (every?* hex-str-64-or-prefix? ids)) :err/ids
    (not (every?* kind? kinds)) :err/kinds
    (not (every?* hex-str-64? e#)) :err/e#
    (not (every?* hex-str-64? p#)) :err/p#
    (not (or (nil? since) (timestamp? since))) :err/since
    (not (or (nil? until) (timestamp? until))) :err/until
    (not (every?* hex-str-64-or-prefix? authors)) :err/authors
    (not (or (nil? limit) (nat-int? limit))) :err/limit
    (not (empty? (invalid-filter-keys the-filter))) :err/invalid-filter-keys))

(defn req-err
  [req-id req-filters]
  (cond
    (not (string? req-id)) :err/req-id
    (nil? req-filters) nil
    :else (first (keep filter-err req-filters))))

(def zero-hex-str (apply str (repeat 64 "0")))

(defn filter-has-empty-attr?
  [the-filter]
  (some #(and (vector? %) (empty? %)) (vals the-filter)))

(defn conform-filter-lenient
  [the-filter]
  ;; based on what we see from clients in the wild, we'll forgive some mistakes
  ;; in requests that we can argue won't change the result semantics:
  ;; (1) we've seen non-hex authors, ids, #e and #p filters in queries; here
  ;;     we replace these with a valid hex str that should have no real reference
  ;;     in our db. in this way we'll do the right thing if there is one bad author
  ;;     ref in the query (i.e., we'll return no results). if there are good author
  ;;     refs in the query alongside bad, then we'll return matches if there are
  ;;     any for the good author refs
  (cond-> the-filter
    (not (empty? (:authors the-filter)))
    (update :authors #(mapv (fn [x] (if (hex-str-64-or-prefix? x) x zero-hex-str)) %))
    (not (empty? (:ids the-filter)))
    (update :ids #(mapv (fn [x] (if (hex-str-64-or-prefix? x) x zero-hex-str)) %))
    (not (empty? (:#e the-filter)))
    (update :#e #(mapv (fn [x] (if (hex-str-64? x) x zero-hex-str)) %))
    (not (empty? (:#p the-filter)))
    (update :#p #(mapv (fn [x] (if (hex-str-64? x) x zero-hex-str)) %))
    ;; (2) we've seen floats in the wild eg 1.671315671052E9
    (not (nil? (:since the-filter)))
    (update :since #(if (number? %) (long %) %))
    (not (nil? (:until the-filter)))
    (update :until #(if (number? %) (long %) %))))

(defn close-err
  [req-id]
  (cond
    (not (string? req-id)) :err/req-id))
