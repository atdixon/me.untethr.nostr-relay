(ns me.untethr.nostr.query.pubkey-based
  (:require [clojure.string :as str]))

(defn- pubkeys-only-single-query-str
  [cols-str num-pubkeys ?endcap-created-at & {:keys [since until]}]
  (format
    (str
      "select %s from n_events"
      " where pubkey in %s and deleted_ = 0"
      (when since
        " and created_at >= ?")
      (when (or ?endcap-created-at until)
        " and created_at <= ?")
      " and id <= ?"
      " order by created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-pubkeys "?")) ")")))

(defn pubkeys-only-single-query
  [cols-str pubkeys ?endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  (let [single-query-str
        (pubkeys-only-single-query-str cols-str (count pubkeys) ?endcap-created-at
          :since since :until until)
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-pubkeys (sort pubkeys)]
    (cond-> (apply vector single-query-str sorted-pubkeys)
      since (conj since)
      (or ?endcap-created-at until) (conj (or ?endcap-created-at until))
      true (into [endcap-row-id local-limit]))))

(defn pubkeys-and-kinds-only-single-query-str
  [cols-str num-pubkeys num-kinds ?endcap-created-at & {:keys [since until]}]
  (format
    (str
      "select %s from n_events"
      " where pubkey in %s and kind in %s and deleted_ = 0"
      (when since
        " and created_at >= ?")
      (when (or ?endcap-created-at until)
        " and created_at <= ?")
      " and id <= ?"
      " order by created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-pubkeys "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn pubkeys-and-kinds-only-single-query
  [cols-str pubkeys kinds ?endcap-created-at endcap-row-id local-limit & {:keys [since until]}]
  (let [single-query-str
        (pubkeys-and-kinds-only-single-query-str cols-str (count pubkeys)
          (count kinds) ?endcap-created-at :since since :until until)
        sorted-pubkeys (sort pubkeys)
        sorted-kinds (sort kinds)]
    (cond-> (apply vector single-query-str (concat sorted-pubkeys sorted-kinds))
      since (conj since)
      (or ?endcap-created-at until) (conj (or ?endcap-created-at until))
      true (into [endcap-row-id local-limit]))))
