(ns me.untethr.nostr.query.kind-based
  (:require [clojure.string :as str]))

(defn- kinds-only-single-query-str
  [cols-str num-kinds endcap-created-at & {:keys [since until]}]
  (format
    (str
      "select %s from n_events"
      " where kind in %s and deleted_ = 0"
      (when since
        " and created_at >= ?")
      (when (or endcap-created-at until)
        " and created_at <= ?")
      " and (created_at < ? or id <= ?)"
      " order by created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn kinds-only-single-query
  [cols-str kinds endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  (let [single-query-str
        (kinds-only-single-query-str cols-str (count kinds) endcap-created-at
          :since since :until until)
        sorted-kinds (sort kinds)]
    (cond-> (apply vector single-query-str sorted-kinds)
      since (conj since)
      (or endcap-created-at until) (conj (or endcap-created-at until))
      true (into [endcap-created-at endcap-row-id local-limit]))))

;; NOTE: experimental ..single non-union query over multiple kinds performs competitively
;;  so assuming that sqlite is able to walk the created_at indices cleverly backwards
;;  to avoid sorting the entirety of results before applying the limit. Should we see
;;  degraded perf over larger data volumes, we can try out union queries such as this one.
;(defn kinds-only-union-query
;  [cols-str kinds endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
;  ;; we read the explain query plan for this union showing sqlite leveraging
;  ;; index for each subquery using a *single* temp b-tree to order results from
;  ;; each union (see below for plan when two kinds are unioned).
;  ;; we use union all as we do not expect overlap between results of each query.
;  (let [union-query-str (str
;                          "select * from ("
;                          (str/join ") union all select * from ("
;                            (repeat (count kinds)
;                              (kinds-only-single-query-str cols-str 1 endcap-created-at
;                                :since since :until until)))
;                          ") order by created_at desc, id desc limit ?")
;        sorted-kinds (sort kinds)]
;    (if endcap-created-at
;      (vec (concat
;             [union-query-str]
;             (map #(vector % endcap-created-at endcap-row-id local-limit) sorted-kinds)
;             [local-limit]))
;      (vec (concat
;             [union-query-str]
;             (mapcat #(vector % endcap-row-id local-limit) sorted-kinds)
;             [local-limit])))))

;MERGE (UNION)
;LEFT
;CO-ROUTINE 1
;SEARCH TABLE n_events USING INDEX idx_event_kind_created_at (kind=? AND deleted_=? AND created_at<?)
;SCAN SUBQUERY 1
;USE TEMP B-TREE FOR ORDER BY
;RIGHT
;CO-ROUTINE 3
;SEARCH TABLE n_events USING INDEX idx_event_kind_created_at (kind=? AND deleted_=? AND created_at<?)
;SCAN SUBQUERY 3
;USE TEMP B-TREE FOR ORDER BY
