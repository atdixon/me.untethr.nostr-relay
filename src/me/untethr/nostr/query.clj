(ns me.untethr.nostr.query
  (:require [clojure.string :as str]))

(defn- filter->base*
  [ids kinds since until authors e# p#]
  {:post [(vector? (second %))]}
  (reduce
    (fn [[acc-clause acc-params] [clause params]]
      [(if (nil? acc-clause)
         clause
         (str acc-clause " and " clause)) (into acc-params params)])
    [nil []]
    (cond-> []
      ;; note: order here may govern query plan
      (not-empty authors)
      (conj [(format "pubkey in (%s)" (str/join "," (repeat (count authors) "?"))) authors])
      (some? since)
      (conj ["created_at >= ?" [since]])
      (not-empty ids)
      (conj [(format "id in (%s)" (str/join "," (repeat (count ids) "?"))) ids])
      (not-empty kinds)
      (conj [(format "kind in (%s)" (str/join "," (repeat (count kinds) "?"))) kinds])
      (some? until)
      (conj ["created_at <= ?" [until]])
      (not-empty e#)
      (conj [(format "e.tagged_event_id in (%s)" (str/join "," (repeat (count e#) "?"))) e#])
      (not-empty p#)
      (conj [(format "p.tagged_pubkey in (%s)" (str/join "," (repeat (count p#) "?"))) p#]))))

(def ^:private join-e "join e_tags e on e.source_event_id = v.id")
(def ^:private join-p "join p_tags p on p.source_event_id = v.id")

(defn filter->query
  "Note: no result ordering as of yet. Not req'd by nostr nips."
  [{:keys [ids kinds since until authors limit] e# :#e p# :#p :as _filter} target-row-id]
  (let [[base-clause base-params] (filter->base* ids kinds since until authors e# p#)
        q (cond
            (and (not-empty e#) (not-empty p#))
            (format "select v.rowid, v.raw_event from n_events v %s %s where %s" join-e join-p base-clause)
            (not-empty e#)
            (format "select v.rowid, v.raw_event from n_events v %s where %s" join-e base-clause)
            (not-empty p#)
            (format "select v.rowid, v.raw_event from n_events v %s where %s" join-p base-clause)
            :else
            (format "select v.rowid, v.raw_event from n_events v where %s" base-clause))
        q (if (some? target-row-id) (str q " and v.rowid <= " target-row-id) q)
        q (str q " and deleted_ = 0")]
    (if (some? limit)
      ;; note: can't do order by w/in union query unless you leverage sub-queries like so:
      (apply vector (str "select * from (" q " order by v.created_at desc limit ?)") (conj base-params limit))
      (apply vector q base-params))))

(defn filters->query
  ([filters] (filters->query filters nil))
  ([filters target-row-id]
   {:pre [(or (nil? target-row-id) (number? target-row-id))]}
   (vec
     (reduce
       (fn [[q & p] [q+ & p+]]
         (cons (if (nil? q) q+ (str q " union " q+)) (concat p p+)))
       [nil]
       (map #(filter->query % target-row-id) filters)))))
