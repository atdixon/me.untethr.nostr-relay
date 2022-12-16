(ns me.untethr.nostr.query
  (:require
    [clojure.string :as str]
    [me.untethr.nostr.common :as common]))

(defn- filter->base*
  [ids kinds since until authors e# p# generic-tags]
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
      (conj [(format "p.tagged_pubkey in (%s)" (str/join "," (repeat (count p#) "?"))) p#])
      (not-empty generic-tags)
      (into
        (map
          (fn [[generic-tag-as-keyword tag-values]]
            [(format "(x.generic_tag = '%s' and x.tagged_value in (%s))"
               (subs (name generic-tag-as-keyword) 1)
               (str/join "," (repeat (count tag-values) "?"))) tag-values])
          generic-tags)))))

(def ^:private join-e "join e_tags e on e.source_event_id = v.id")
(def ^:private join-p "join p_tags p on p.source_event_id = v.id")
(def ^:private join-x "join x_tags x on x.source_event_id = v.id")

(defn- ->non-e-or-p-generic-tags*
  [filter]
  (select-keys filter common/allow-filter-tag-queries-sans-e-and-p-set))

(defn filter->query
  "Note: no result ordering as yet. Not req'd by nostr nips."
  [{:keys [ids kinds since until authors limit] e# :#e p# :#p :as filter} target-row-id]
  (let [generic-tags (->non-e-or-p-generic-tags* filter)
        [base-clause base-params] (filter->base* ids kinds since until authors e# p# generic-tags)
        join-clause (str/join " "
                      (cond-> []
                        (not-empty e#) (conj join-e)
                        (not-empty p#) (conj join-p)
                        (not-empty generic-tags) (conj join-x)))
        q (if (empty? join-clause)
            (format "select v.rowid, v.raw_event from n_events v where %s" base-clause)
            (format "select v.rowid, v.raw_event from n_events v %s where %s" join-clause base-clause))
        q (if (some? target-row-id) (str q " and v.rowid <= " target-row-id) q)
        q (str q " and v.deleted_ = 0")]
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
