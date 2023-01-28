(ns me.untethr.nostr.query.kind-based
  (:require [clojure.string :as str]))

(defn- kinds-only-single-query-str0
  [cols-str num-kinds use-since?]
  (format
    (str
      "select %s from n_events"
      " where kind in %s and deleted_ = 0"
      (when use-since?
        " and created_at >= ?")
      " and created_at < ?"
      " order by created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn- kinds-only-single-query-str1
  [cols-str num-kinds use-since?]
  (format
    (str
      "select %s from n_events"
      " where kind in %s and deleted_ = 0"
      (when use-since?
        " and created_at >= ?")
      " and created_at = ? and id <= ?"
      " order by created_at desc, id desc limit ?")
    cols-str (str "(" (str/join "," (repeat num-kinds "?")) ")")))

(defn kinds-only-single-query
  [cols-str kinds endcap-created-at endcap-row-id local-limit & {:keys [since]}]
  {:pre [(some? endcap-created-at) (some? endcap-row-id)]}
  (let [use-since? (some? since)
        single-query-str
        (format
          (str
            "select * from (select * from (%s) union select * from (%s))"
            " order by created_at desc, id desc limit ?")
          (kinds-only-single-query-str0 cols-str (count kinds) use-since?)
          (kinds-only-single-query-str1 cols-str (count kinds) use-since?))
        sorted-kinds (sort kinds)]
    (cond->
      [single-query-str]
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj local-limit)
      true (into sorted-kinds)
      use-since? (conj since)
      true (conj endcap-created-at)
      true (conj endcap-row-id)
      true (conj local-limit)
      true (conj local-limit))))
