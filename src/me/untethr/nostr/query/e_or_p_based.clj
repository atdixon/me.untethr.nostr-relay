(ns me.untethr.nostr.query.e-or-p-based
  (:require [clojure.string :as str]))

(defn e#-tags-only-single-query-str0
  [cols-str num-e#-tags use-since?]
  (format
    (str
      "select %s from e_tags"
      " where tagged_event_id in %s and source_event_deleted_ = 0"
      (when use-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at < ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-e#-tags "?")) ")")))

(defn e#-tags-only-single-query-str1
  [cols-str num-e#-tags use-since?]
  (format
    (str
      "select %s from e_tags"
      " where tagged_event_id in %s and source_event_deleted_ = 0"
      (when use-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at = ? and e_tags.id <= ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-e#-tags "?")) ")")))

(defn e#-tags-only-single-query
  [cols-str e#-tags endcap-created-at endcap-row-id local-limit & {:keys [since]}]
  {:pre [(some? endcap-created-at) (some? endcap-row-id) (not (empty? e#-tags))]}
  (let [use-since? (some? since)
        single-query-str
        (format
          (str
            "select * from (select * from (%s) union select * from (%s))"
            " order by created_at desc, id desc limit ?")
          (e#-tags-only-single-query-str0 cols-str (count e#-tags) use-since?)
          (e#-tags-only-single-query-str1 cols-str (count e#-tags) use-since?))
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-e#-tags (sort e#-tags)]
    (cond->
      [single-query-str]
      true (into sorted-e#-tags)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj local-limit)
      true (into sorted-e#-tags)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj endcap-row-id)
      true (conj local-limit)
      true (conj local-limit))))

(defn e#-tags-and-kinds-only-single-query-str0
  [cols-str num-e#-tags num-kinds use-since?]
  (format
    (str
      "select %s from e_tags"
      " where tagged_event_id in %s and source_event_kind in %s and source_event_deleted_ = 0"
      (when use-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at < ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-e#-tags "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn e#-tags-and-kinds-only-single-query-str1
  [cols-str num-e#-tags num-kinds use-since?]
  (format
    (str
      "select %s from e_tags"
      " where tagged_event_id in %s and source_event_kind in %s and source_event_deleted_ = 0"
      (when use-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at = ? and e_tags.id <= ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-e#-tags "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn e#-tags-and-kinds-only-single-query
  [cols-str e#-tags kinds endcap-created-at endcap-row-id local-limit & {:keys [since]}]
  {:pre [(some? endcap-created-at) (some? endcap-row-id) (not (empty? e#-tags)) (not (empty? kinds))]}
  (let [use-since? (some? since)
        single-query-str
        (format
          (str
            "select * from (select * from (%s) union select * from (%s))"
            " order by created_at desc, id desc limit ?")
          (e#-tags-and-kinds-only-single-query-str0 cols-str (count e#-tags) (count kinds) use-since?)
          (e#-tags-and-kinds-only-single-query-str1 cols-str (count e#-tags) (count kinds) use-since?))
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-e#-tags (sort e#-tags)
        sorted-kinds (sort kinds)]
    (cond->
      [single-query-str]
      true (into sorted-e#-tags)
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj local-limit)
      true (into sorted-e#-tags)
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj endcap-row-id)
      true (conj local-limit)
      true (conj local-limit))))

;; --

(defn p#-tags-only-single-query-str0
  [cols-str num-p#-tags has-since?]
  (format
    (str
      "select %s from p_tags"
      " where tagged_pubkey in %s and source_event_deleted_ = 0"
      (when has-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at < ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-p#-tags "?")) ")")))

(defn p#-tags-only-single-query-str1
  [cols-str num-p#-tags has-since?]
  (format
    (str
      "select %s from p_tags"
      " where tagged_pubkey in %s and source_event_deleted_ = 0"
      (when has-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at = ? and p_tags.id <= ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-p#-tags "?")) ")")))

(defn p#-tags-only-single-query
  [cols-str p#-tags endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  {:pre [(some? endcap-created-at) (some? endcap-row-id) (not (empty? p#-tags))]}
  (let [use-since? (some? since)
        single-query-str
        (format
          (str
            "select * from (select * from (%s) union select * from (%s))"
            " order by created_at desc, id desc limit ?")
          (p#-tags-only-single-query-str0 cols-str (count p#-tags) use-since?)
          (p#-tags-only-single-query-str1 cols-str (count p#-tags) use-since?))
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-p#-tags (sort p#-tags)]
    (cond->
      [single-query-str]
      true (into sorted-p#-tags)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj local-limit)
      true (into sorted-p#-tags)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj endcap-row-id)
      true (conj local-limit)
      true (conj local-limit))))

(defn p#-tags-and-kinds-only-single-query-str0
  [cols-str num-p#-tags num-kinds has-since?]
  (format
    (str
      "select %s from p_tags"
      " where tagged_pubkey in %s and source_event_kind in %s and source_event_deleted_ = 0"
      (when has-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at < ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-p#-tags "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn p#-tags-and-kinds-only-single-query-str1
  [cols-str num-p#-tags num-kinds has-since?]
  (format
    (str
      "select %s from p_tags"
      " where tagged_pubkey in %s and source_event_kind in %s and source_event_deleted_ = 0"
      (when has-since?
        " and source_event_created_at >= ?")
      " and source_event_created_at = ? and p_tags.id <= ?"
      " group by source_event_id, source_event_created_at"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-p#-tags "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn p#-tags-and-kinds-only-single-query
  [cols-str p#-tags kinds endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  {:pre [(some? endcap-created-at) (some? endcap-row-id) (not (empty? p#-tags)) (not (empty? kinds))]}
  (let [use-since? (some? since)
        single-query-str
        (format
          (str
            "select * from (select * from (%s) union select * from (%s))"
            " order by created_at desc, id desc limit ?")
          (p#-tags-and-kinds-only-single-query-str0 cols-str (count p#-tags) (count kinds) use-since?)
          (p#-tags-and-kinds-only-single-query-str1 cols-str (count p#-tags) (count kinds) use-since?))
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-p#-tags (sort p#-tags)
        sorted-kinds (sort kinds)]
    (cond->
      [single-query-str]
      true (into sorted-p#-tags)
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj local-limit)
      true (into sorted-p#-tags)
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj endcap-row-id)
      true (conj local-limit)
      true (conj local-limit))))
