(ns me.untethr.nostr.query.engine
  (:require
    [clojure.string :as str]
    [me.untethr.nostr.common.domain :as domain]
    [me.untethr.nostr.common.validation :as validation]
    [me.untethr.nostr.query :as legacy-query]
    [me.untethr.nostr.query.e-or-p-based :as e-or-p-based]
    [me.untethr.nostr.query.kind-based :as kind-based]
    [me.untethr.nostr.query.pubkey-based :as pubkey-based]
    [me.untethr.nostr.query :as query]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs])
  (:import
    (me.untethr.nostr.common.domain TableMaxRowIds)))

(defrecord ActiveFilterState
  ;; cursor-row-id is inclusive - ie for the next query issued for this
  ;; filter state, we will want to allow the row id to be *included*
  ;; in the query results.
  ;; cursor-created-at is inclusive as well (but of course query will remove
  ;; rows with matching created-ats with rowids that exceed cursor-row-id)
  [stereotype
   the-filter
   page-size
   remaining-quota
   cursor-row-id
   cursor-created-at])

(defrecord ActiveFilterPageStats
  [num-results min-seen-row-id min-seen-created-at])

(defn- contains-any-prefix-queries?
  [{:keys [ids authors] :as _the-filter}]
  (not
    (and
      (every? validation/hex-str-64? ids)
      (every? validation/hex-str-64? authors))))

;; by default - false ... we'll support prefix queries for reactive
;;  results but not for backfilling ... for now.  we should log typical
;;  usages and evaluate performance cost of supporting this for fulfillment:
(def ^:dynamic *support-prefix-queries?* false)

(defn- stereotype-filter
  [{:keys [ids kinds authors] _limit :limit e# :#e p# :#p :as the-filter}]
  ;; note: if we modify possible return values, we need to revisit other funcs
  ;; in this ns.
  {:post [(#{:kinds-only :pubkeys-only :e-tags-only :p-tags-only
             :pubkeys-and-kinds-only :e-tags-and-kinds-only :p-tags-and-kinds-only
             :generic-unoptimized} %)]}
  (if (and *support-prefix-queries?*
        (contains-any-prefix-queries? the-filter))
    ;; if there are any prefix queries present, we'll shortcut b/c denormalized
    ;;  tag tables are not (yet?) indexed to support prefix queries.
    :generic-unoptimized
    (let [num-ids (count ids)
          num-kinds (count kinds)
          num-authors (count authors)
          num-#e (count e#)
          num-#p (count p#)
          generic-tags (query/->non-e-or-p-generic-tags the-filter)
          num-generic-tags (transduce (comp (map second) (map count)) + 0 generic-tags)]
      (cond
        ;; -- single attr only queries --
        (and (pos? num-kinds)
          (every? zero? [num-ids num-authors num-#e num-#p num-generic-tags]))
        :kinds-only
        (and (pos? num-authors)
          (every? zero? [num-ids num-kinds num-#e num-#p num-generic-tags]))
        :pubkeys-only
        (and (pos? num-#e)
          (every? zero? [num-ids num-kinds num-authors num-#p num-generic-tags]))
        :e-tags-only
        (and (pos? num-#p)
          (every? zero? [num-ids num-kinds num-authors num-#e num-generic-tags]))
        :p-tags-only
        ;; -- common pair attr queries --
        (and (pos? num-kinds) (pos? num-authors)
          (every? zero? [num-ids num-#e num-#p num-generic-tags]))
        :pubkeys-and-kinds-only
        (and (pos? num-kinds) (pos? num-#e)
          (every? zero? [num-ids num-authors num-#p num-generic-tags]))
        :e-tags-and-kinds-only
        (and (pos? num-kinds) (pos? num-#p)
          (every? zero? [num-ids num-authors num-#e num-generic-tags]))
        :p-tags-and-kinds-only
        :else ;; non-optimized path
        :generic-unoptimized))))

(defn stereotyped-filter->query
  [stereotype
   {:keys [ids kinds since until authors] _limit :limit e# :#e p# :#p :as the-filter}
   & {:keys [endcap-row-id endcap-created-at override-limit]}]
  {:pre [(some? endcap-row-id) (some? endcap-created-at) (some? override-limit)]}
  (case stereotype
    ;; -- single attr only queries --
    :kinds-only
    (kind-based/kinds-only-single-query
      "id, event_id, created_at" kinds endcap-created-at endcap-row-id override-limit
      :since since :until until)
    :pubkeys-only
    (pubkey-based/pubkeys-only-single-query
      "id, event_id, created_at" authors endcap-created-at endcap-row-id override-limit
      :since since :until until)
    :e-tags-only
    (e-or-p-based/e#-tags-only-single-query
      ;; using min(id) as row id cursor for pagination purposes here -- our sql
      ;; query limit is applied *after* the group-by/min so that any min(id)
      ;; we obtain should truly be the min id for any given source_event_id
      ;; ASSUMING that the source_event_created_at is identical for every source_event_id.
      "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
      e# endcap-created-at endcap-row-id override-limit
      :since since :until until)
    :p-tags-only
    (e-or-p-based/p#-tags-only-single-query
      ;; using min(id) -- see min(id) note above.
      "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
      p# endcap-created-at endcap-row-id override-limit
      :since since :until until)
    ;; -- common pair attr queries --
    :pubkeys-and-kinds-only
    (pubkey-based/pubkeys-and-kinds-only-single-query
      "id, event_id, created_at" authors kinds endcap-created-at endcap-row-id override-limit
      :since since :until until)
    :e-tags-and-kinds-only
    (e-or-p-based/e#-tags-and-kinds-only-single-query
      ;; using min(id) -- see min(id) note above.
      "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
      e# kinds endcap-created-at endcap-row-id override-limit
      :since since :until until)
    :p-tags-and-kinds-only
    (e-or-p-based/p#-tags-and-kinds-only-single-query
      ;; using min(id) -- see min(id) note above.
      "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
      p# kinds endcap-created-at endcap-row-id override-limit
      :since since :until until)
    :generic-unoptimized
    (legacy-query/generic-filter->query-new "v.id, v.event_id, v.created_at" the-filter
      :override-limit override-limit
      :endcap-row-id endcap-row-id
      :endcap-created-at endcap-created-at)))

;; this version primarily for testing:
(defn filter->query [the-filter & more]
  (apply stereotyped-filter->query
    (stereotype-filter the-filter)
    the-filter more))

(defn- make-union-all-query
  [multiple-sql-params]
  (apply vector
    (str/join " union all " (map first multiple-sql-params))
    (mapcat rest multiple-sql-params)))

(defn active-filters->query
  [active-filters]
  {:pre [(not-empty active-filters)]}
  (make-union-all-query
    (map-indexed
      (fn [idx {:keys [stereotype the-filter page-size remaining-quota cursor-row-id cursor-created-at] :as _active-filter}]
        (let [[sql & sql-args] (stereotyped-filter->query stereotype the-filter
                                 :endcap-row-id cursor-row-id
                                 :endcap-created-at cursor-created-at
                                 :override-limit (min remaining-quota page-size))]
          (apply vector (format "select %d as filter_index, * from (%s)" idx sql) sql-args)))
      active-filters)))

(defn execute-active-filters
  [read-db active-filters]
  (let [q (active-filters->query active-filters)]
    (jdbc/execute! read-db q
      {:builder-fn rs/as-unqualified-lower-maps})))

(defn- pick-max-row-id
  [stereotype max-row-ids]
  {:pre [(instance? TableMaxRowIds max-row-ids)]
   :post [(some? %)]}
  (case stereotype
    :kinds-only (:n-events-id max-row-ids)
    :pubkeys-only (:n-events-id max-row-ids)
    :e-tags-only (:e-tags-id max-row-ids)
    :p-tags-only (:p-tags-id max-row-ids)
    :pubkeys-and-kinds-only (:n-events-id max-row-ids)
    :e-tags-and-kinds-only (:e-tags-id max-row-ids)
    :p-tags-and-kinds-only (:p-tags-id max-row-ids)
    :generic-unoptimized (:n-events-id max-row-ids)))

(defn init-active-filter
  [{:keys [until limit] :as f}
   & {:keys [page-size table-target-ids override-quota]
      :or {page-size 250
           table-target-ids (domain/->TableMaxRowIds
                              Integer/MAX_VALUE
                              Integer/MAX_VALUE
                              Integer/MAX_VALUE
                              Integer/MAX_VALUE)}}]
  (let [stereotype (stereotype-filter f)]
    (->ActiveFilterState stereotype f page-size
      (or override-quota limit Integer/MAX_VALUE)
      (pick-max-row-id stereotype table-target-ids)
      (or until Long/MAX_VALUE))))

(defn next-active-filters
  [active-filters prev-page-stats]
  (reduce
    (fn [acc [filter-idx active-filter]]
      (if-let [{:keys [num-results min-seen-row-id min-seen-created-at]} (get prev-page-stats filter-idx)]
        (let [new-active-filter (-> active-filter
                                  (update :remaining-quota - num-results)
                                  (assoc :cursor-row-id (dec min-seen-row-id))
                                  (assoc :cursor-created-at min-seen-created-at))]
          (if (pos? (:remaining-quota new-active-filter))
            (conj acc new-active-filter)
            ;; no more quota (i.e. we've reached the filter's limit or our own
            ;; restricted max limit of results to produce for this filter)
            acc))
        ;; else - we got no results for the active-filter in page, so
        ;; it we won't keep it in the actives list:
        acc))
    []
    (map-indexed vector active-filters)))

(defn calculate-page-stats
  [filters-query-results]
  ;; important that result never has an entry for a filter that didn't
  ;; produce results (i.e., we'll never see an page stats in our returned
  ;; map with a zero num-results or a nil min-seen-row-id...
  (reduce
    (fn [acc {:keys [filter_index id created_at]}]
      {:pre [(number? filter_index) (some? id) (some? created_at)]}
      (-> acc
        (update
          filter_index
          (fnil (fn [curr-stats]
                  (-> curr-stats
                    (update :num-results inc)
                    (update :min-seen-row-id min id)
                    (update :min-seen-created-at min created_at)))
            (->ActiveFilterPageStats 0 id created_at))))
      )
    {}
    filters-query-results))
