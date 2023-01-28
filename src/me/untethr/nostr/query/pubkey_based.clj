(ns me.untethr.nostr.query.pubkey-based
  (:require [clojure.string :as str]))

(defn- pubkeys-only-single-query-str0
  [cols-str num-pubkeys use-since?]
  (format
    (str
      "select %s from n_events"
      " where pubkey in %s and deleted_ = 0"
      (when use-since?
        " and created_at >= ?")
      " and created_at < ?"
      " order by created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-pubkeys "?")) ")")))

(defn- pubkeys-only-single-query-str1
  [cols-str num-pubkeys use-since?]
  (format
    (str
      "select %s from n_events"
      " where pubkey in %s and deleted_ = 0"
      (when use-since?
        " and created_at >= ?")
      " and created_at = ? and id <= ?"
      " order by created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-pubkeys "?")) ")")))

(defn pubkeys-only-single-query
  [cols-str pubkeys endcap-created-at endcap-row-id local-limit & {:keys [since]}]
  {:pre [(some? endcap-created-at) (some? endcap-row-id)]}
  (let [use-since? (some? since)
        single-query-str
        (format
          (str
            "select * from (select * from (%s) union select * from (%s))"
            " order by created_at desc, id desc limit ?")
          (pubkeys-only-single-query-str0 cols-str (count pubkeys) use-since?)
          (pubkeys-only-single-query-str1 cols-str (count pubkeys) use-since?))
        ;; deterministic queries are nice for log processing and we may like to leverage
        ;; a query cache one day:
        sorted-pubkeys (sort pubkeys)]
    (cond->
      [single-query-str]
      true (into sorted-pubkeys)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj local-limit)
      true (into sorted-pubkeys)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj endcap-row-id)
      true (conj local-limit)
      true (conj local-limit))))

(defn pubkeys-and-kinds-only-single-query-str0
  [cols-str num-pubkeys num-kinds use-since?]
  (format
    (str
      "select %s from n_events"
      " where pubkey in %s and kind in %s and deleted_ = 0"
      (when use-since?
        " and created_at >= ?")
      " and created_at < ?"
      " order by created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-pubkeys "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn pubkeys-and-kinds-only-single-query-str1
  [cols-str num-pubkeys num-kinds use-since?]
  (format
    (str
      "select %s from n_events"
      " where pubkey in %s and kind in %s and deleted_ = 0"
      (when use-since?
        " and created_at >= ?")
      " and created_at = ? and id <= ?"
      " order by created_at desc, id desc limit ?")
    cols-str
    (str "(" (str/join "," (repeat num-pubkeys "?")) ")")
    (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn pubkeys-and-kinds-only-single-query
  [cols-str pubkeys kinds endcap-created-at endcap-row-id local-limit & {:keys [since]}]
  {:pre [(some? endcap-created-at) (some? endcap-row-id)]}
  (let [use-since? (some? since)
        single-query-str
        (format
          (str
            "select * from (select * from (%s) union select * from (%s))"
            " order by created_at desc, id desc limit ?")
          (pubkeys-and-kinds-only-single-query-str0 cols-str (count pubkeys) (count kinds) use-since?)
          (pubkeys-and-kinds-only-single-query-str1 cols-str (count pubkeys) (count kinds) use-since?))
        sorted-pubkeys (sort pubkeys)
        sorted-kinds (sort kinds)]
    (cond->
      [single-query-str]
      true (into sorted-pubkeys)
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj local-limit)
      true (into sorted-pubkeys)
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj endcap-row-id)
      true (conj local-limit)
      true (conj local-limit))))
