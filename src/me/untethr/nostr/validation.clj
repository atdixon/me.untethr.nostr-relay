(ns me.untethr.nostr.validation
  (:require [clojure.tools.logging :as log]))

(defn- kind? [k]
  (nat-int? k))

(defn- hex-str? [s]
  (and (string? s) (re-matches #"[a-f0-9]{32,}" s)))

(defn- timestamp? [t]
  (nat-int? t))

(defn event-err
  [{:keys [id pubkey created_at kind tags content sig]}]
  (cond
    (not (hex-str? id)) :err/id
    (not (hex-str? pubkey)) :err/pubkey
    (not (hex-str? sig)) :err/sig
    (not (timestamp? created_at)) :err/created-at
    (not (kind? kind)) :err/kind
    (not (or
           (nil? tags)
           (vector? tags))) :err/tag
    (not (or (nil? content) (string? content))) :err/content))

(defn- every?*
  [pred x]
  (or (nil? x) (and (vector? x) (every? pred x))))

(defn filter-err
  [{:keys [ids kinds since until authors limit] e# :#e p# :#p :as _filter}]
  (cond
    (not (every?* hex-str? ids)) :err/ids
    (not (every?* kind? kinds)) :err/kinds
    (not (every?* hex-str? e#)) :err/e#
    (not (every?* hex-str? p#)) :err/p#
    (not (or (nil? since) (timestamp? since))) :err/since
    (not (or (nil? until) (timestamp? until))) :err/until
    (not (every?* hex-str? authors)) :err/authors
    (not (or (nil? limit) (integer? limit))) :err/limit))

(defn req-err
  [req-id req-filters]
  (cond
    (not (string? req-id)) :err/req-id
    (nil? req-filters) nil
    :else (first (keep filter-err req-filters))))

(defn close-err
  [req-id]
  (cond
    (not (string? req-id)) :err/req-id))

(defn filter-empty?
  [{:keys [ids kinds since until authors] e# :#e p# :#p :as _filter}]
  (and (empty? ids) (empty? kinds) (nil? since) (nil? until) (empty? authors) (empty? e#) (empty? p#)))

(defn filters-empty?
  [filters]
  (or (empty? filters) (every? filter-empty? filters)))