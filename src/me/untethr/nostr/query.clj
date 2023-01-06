(ns me.untethr.nostr.query
  (:require
    [clojure.string :as str]
    [me.untethr.nostr.common :as common]
    [me.untethr.nostr.validation :as validation]))

(defn- build-vals-and-prefixes-or-clause
  [column-name whole-vals prefixes]
  (let [whole-vals-clause (when (not-empty whole-vals)
                            (format "%s in (%s)"
                              column-name (str/join "," (repeat (count whole-vals) "?"))))
        prefixes-clause (when (not-empty prefixes)
                          (str/join " or "
                            ;; note: we should be validating incoming filters such that
                            ;; sql injection not possible here, where we must do a like
                            ;; query and not leverage "?" fill-ins.
                            (map #(format "%s like '%s%%'" column-name %) prefixes)))]
    [(cond
       (and whole-vals-clause prefixes-clause) (str whole-vals-clause " or " prefixes-clause)
       whole-vals-clause whole-vals-clause
       prefixes-clause prefixes-clause)
     whole-vals]))

(defn- filter->base*
  [ids kinds since until authors e# p# generic-tags]
  {:post [(vector? (second %))]}
  (let [{ids-prefixes false ids-whole true} (group-by (comp some? validation/hex-str-64?) ids)
        {authors-prefixes false authors-whole true} (group-by (comp some? validation/hex-str-64?) authors)]
    (reduce
      (fn [[acc-clause acc-params] [clause params]]
        [(if (nil? acc-clause)
           clause
           (str acc-clause " and " clause)) (into acc-params params)])
      [nil []]
      (cond-> []
        ;; note: order here may govern query plan
        (not-empty authors)
        (conj (build-vals-and-prefixes-or-clause "pubkey" authors-whole authors-prefixes))
        (some? since)
        (conj ["created_at >= ?" [since]])
        (not-empty ids)
        (conj (build-vals-and-prefixes-or-clause "id" ids-whole ids-prefixes))
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
            generic-tags))))))

(def ^:private join-e ["e_tags e" "e.source_event_id = v.id"])
(def ^:private join-p ["p_tags p" "p.source_event_id = v.id"])
(def ^:private join-x ["x_tags x" "x.source_event_id = v.id"])

(defn- ->non-e-or-p-generic-tags*
  [filter]
  (select-keys filter common/allow-filter-tag-queries-sans-e-and-p-set))

(defn- generic-filter->query
  [{:keys [ids kinds since until authors limit] e# :#e p# :#p :as filter} & {:keys [target-row-id]}]
  (let [generic-tags (->non-e-or-p-generic-tags* filter)
        [base-expr base-params] (filter->base* ids kinds since until authors e# p# generic-tags)
        joins (cond-> []
                (not-empty e#) (conj join-e)
                (not-empty p#) (conj join-p)
                (not-empty generic-tags) (conj join-x))
        join-clause (str/join " cross join "
                      (map #(nth % 0) joins))
        join-expr (str/join " and "
                    (map #(nth % 1) joins))
        q (cond
            (empty? base-expr)
            "select v.rowid from n_events v"
            (empty? join-clause)
            (format "select v.rowid from n_events v where %s" base-expr)
            :else
            (format "select v.rowid from %s cross join n_events v where %s and %s"
              join-clause join-expr base-expr))
        extra-clauses (cond-> []
                        (some? target-row-id) (conj (str "v.rowid <= " target-row-id))
                        true (conj "v.deleted_ = 0"))
        q (str q (if (empty? base-expr) " where " " and ") (str/join " and " extra-clauses))
        q (if (empty? join-clause) q (str q " group by v.rowid"))]
    (if (some? limit)
      ;; note: can't do order by w/in union query unless you leverage sub-queries like so
      ;; (ie, this will allow us to union *this* query with others in the same set of
      ;; filters):
      (apply vector (str "select rowid from (" q " order by v.created_at desc limit ?)") (conj base-params limit))
      (apply vector q base-params))))

(defn filters->query
  "Convert nostr filters to SQL query.

   Provided filters must be non-empty.

   However, [{}] is supported and produces all values.

   Callers that care about optimal efficiency should provide a de-duplicated list
   of filters; i.e., we won't do any filter de-duping here.

   Filter attributes that are empty colls are ignored.

   So upstream callers that want to return zero results in such cases are
   obligated to short-circuit before invoking this function.

   An overall limit will be applied if :overall-limit provided as a var-arg."
  ([filters] (filters->query filters nil))
  ([filters & {:keys [target-row-id overall-limit]}]
   {:pre [(not-empty filters) (or (nil? target-row-id) (number? target-row-id))]}
   (let [[compound-query-str & compound-query-params]
         (vec
           (reduce
             (fn [[q & p] [q+ & p+]]
               (cons (if (nil? q) q+ (str q " union " q+)) (concat p p+)))
             [nil]
             (map
               #(generic-filter->query %
                  :target-row-id target-row-id) filters)))
         compound-query-str (str "select rowid, raw_event from n_events where rowid in (" compound-query-str ")")]
     (if overall-limit
       (vec
         (cons
           (str compound-query-str " order by created_at desc limit ?")
           (concat compound-query-params [overall-limit])))
       (apply vector compound-query-str compound-query-params)))))
