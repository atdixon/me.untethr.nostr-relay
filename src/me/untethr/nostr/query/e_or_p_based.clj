(ns me.untethr.nostr.query.e-or-p-based
  (:require [clojure.string :as str]))

(defn e#-tags-only-single-query-str
  [cols-str num-e#-tags endcap-created-at & {:keys [since until]}]
  (format
    (str
      "select %s from e_tags"
      " where tagged_event_id in %s and source_event_deleted_ = 0"
      (when since
        " and source_event_created_at >= ?")
      (when (or endcap-created-at until)
        " and source_event_created_at <= ?")
      " group by source_event_id, source_event_created_at"
      " having (source_event_created_at < ? or id <= ?)"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-e#-tags "?")) ")")))

(defn e#-tags-only-single-query
  [cols-str e#-tags endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  (let [single-query-str
        (e#-tags-only-single-query-str cols-str (count e#-tags) endcap-created-at
          :since since :until until)
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-e#-tags (sort e#-tags)]
    (cond-> (apply vector single-query-str sorted-e#-tags)
      since (conj since)
      (or endcap-created-at until) (conj (or endcap-created-at until))
      true (into [endcap-created-at endcap-row-id local-limit]))))

(defn e#-tags-and-kinds-only-single-query-str
  [cols-str num-e#-tags num-kinds endcap-created-at & {:keys [since until]}]
  (format
    (str
      "select %s from e_tags"
      " where tagged_event_id in %s and source_event_kind in %s and source_event_deleted_ = 0"
      (when since
        " and source_event_created_at >= ?")
      (when (or endcap-created-at until)
        " and source_event_created_at <= ?")
      " group by source_event_id, source_event_created_at"
      " having (source_event_created_at < ? or id <= ?)"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-e#-tags "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn e#-tags-and-kinds-only-single-query
  [cols-str e#-tags kinds endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  (let [single-query-str
        (e#-tags-and-kinds-only-single-query-str cols-str (count e#-tags)
          (count kinds) endcap-created-at
          :since since :until until)
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-e#-tags (sort e#-tags)
        sorted-kinds (sort kinds)]
    (cond-> (apply vector single-query-str (concat sorted-e#-tags sorted-kinds))
      since (conj since)
      (or endcap-created-at until) (conj (or endcap-created-at until))
      true (into [endcap-created-at endcap-row-id local-limit]))))

;; --

(defn p#-tags-only-single-query-str
  [cols-str num-p#-tags endcap-created-at & {:keys [since until]}]
  (format
    (str
      "select %s from p_tags"
      " where tagged_pubkey in %s and source_event_deleted_ = 0"
      (when since
        " and source_event_created_at >= ?")
      (when (or endcap-created-at until)
        " and source_event_created_at <= ?")
      " group by source_event_id, source_event_created_at"
      " having (source_event_created_at < ? or id <= ?)"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-p#-tags "?")) ")")))

(defn p#-tags-only-single-query
  [cols-str p#-tags endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  (let [single-query-str
        (p#-tags-only-single-query-str cols-str (count p#-tags) endcap-created-at
          :since since :until until)
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-p#-tags (sort p#-tags)]
    (cond-> (apply vector single-query-str sorted-p#-tags)
      since (conj since)
      (or endcap-created-at until) (conj (or endcap-created-at until))
      true (into [endcap-created-at endcap-row-id local-limit]))))

(defn p#-tags-and-kinds-only-single-query-str
  [cols-str num-p#-tags num-kinds endcap-created-at & {:keys [since until]}]
  (format
    (str
      "select %s from p_tags"
      " where tagged_pubkey in %s and source_event_kind in %s and source_event_deleted_ = 0"
      (when since
        " and source_event_created_at >= ?")
      (when (or endcap-created-at until)
        " and source_event_created_at <= ?")
      " group by source_event_id, source_event_created_at"
      " having (source_event_created_at < ? or id <= ?)"
      " order by source_event_created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-p#-tags "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn p#-tags-and-kinds-only-single-query
  [cols-str p#-tags kinds endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  (let [single-query-str
        (p#-tags-and-kinds-only-single-query-str cols-str (count p#-tags)
          (count kinds) endcap-created-at
          :since since :until until)
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-p#-tags (sort p#-tags)
        sorted-kinds (sort kinds)]
    (cond-> (apply vector single-query-str (concat sorted-p#-tags sorted-kinds))
      since (conj since)
      (or endcap-created-at until) (conj (or endcap-created-at until))
      true (into [endcap-created-at endcap-row-id local-limit]))))
