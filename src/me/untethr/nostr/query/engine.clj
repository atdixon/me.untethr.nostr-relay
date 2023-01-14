(ns me.untethr.nostr.query.engine
  (:require
    [clojure.string :as str]
    [me.untethr.nostr.query :as legacy-query]
    [me.untethr.nostr.query.e-or-p-based :as e-or-p-based]
    [me.untethr.nostr.query.kind-based :as kind-based]
    [me.untethr.nostr.query.pubkey-based :as pubkey-based]
    [me.untethr.nostr.query :as query]))


(defrecord ActiveFilterState
  ;; cursor-row-id is inclusive - ie for the next query issued for this
  ;; filter state, we will want to allow the row id to be *included*
  ;; in the query results.
  [the-filter page-size remaining-quota cursor-row-id])

(defrecord ActiveFilterPageStats
  [num-results min-seen-row-id])


(defn filter->query
  [{:keys [ids kinds since until authors] _limit :limit e# :#e p# :#p :as filter}
   & {:keys [endcap-row-id ?endcap-created-at override-limit]}]
  {:pre [(some? endcap-row-id) (some? override-limit)]}
  (let [num-ids (count ids)
        num-kinds (count kinds)
        num-authors (count authors)
        num-#e (count e#)
        num-#p (count p#)
        generic-tags (query/->non-e-or-p-generic-tags filter)
        num-generic-tags (transduce (comp (map second) (map count)) + 0 generic-tags)]
    (cond
      ;; -- single attr only queries --
      (and (pos? num-kinds)
        (every? zero? [num-ids num-authors num-#e num-#p num-generic-tags]))
      (kind-based/kinds-only-single-query
        "id, event_id, created_at" kinds ?endcap-created-at endcap-row-id override-limit
        :since since :until until)
      (and (pos? num-authors)
        (every? zero? [num-ids num-kinds num-#e num-#p num-generic-tags]))
      (pubkey-based/pubkeys-only-single-query
        "id, event_id, created_at" authors ?endcap-created-at endcap-row-id override-limit
        :since since :until until)
      (and (pos? num-#e)
        (every? zero? [num-ids num-kinds num-authors num-#p num-generic-tags]))
      (e-or-p-based/e#-tags-only-single-query
        ;; using min(id) as row id cursor for pagination purposes here -- but for this
        ;; overall strategy to work we are relying on the insertion of tags to produce
        ;; consecutive row ids in the denormalized tags table for the given source event
        ;; with subsequent insertions claiming later rowids.
        "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
        e# ?endcap-created-at endcap-row-id override-limit
        :since since :until until)
      (and (pos? num-#p)
        (every? zero? [num-ids num-kinds num-authors num-#e num-generic-tags]))
      (e-or-p-based/p#-tags-only-single-query
        ;; using min(id) -- see min(id) note above.
        "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
        p# ?endcap-created-at endcap-row-id override-limit
        :since since :until until)
      ;; -- common pair attr queries --
      (and (pos? num-kinds) (pos? num-authors)
        (every? zero? [num-ids num-#e num-#p num-generic-tags]))
      (pubkey-based/pubkeys-and-kinds-only-single-query
        "id, event_id, created_at" authors kinds ?endcap-created-at endcap-row-id override-limit
        :since since :until until)
      (and (pos? num-kinds) (pos? num-#e)
        (every? zero? [num-ids num-authors num-#p num-generic-tags]))
      (e-or-p-based/e#-tags-and-kinds-only-single-query
        ;; using min(id) -- see min(id) note above.
        "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
        e# kinds ?endcap-created-at endcap-row-id override-limit
        :since since :until until)
      (and (pos? num-kinds) (pos? num-#p)
        (every? zero? [num-ids num-authors num-#e num-generic-tags]))
      (e-or-p-based/p#-tags-and-kinds-only-single-query
        ;; using min(id) -- see min(id) note above.
        "min(id) as id, source_event_id as event_id, source_event_created_at as created_at"
        p# kinds ?endcap-created-at endcap-row-id override-limit
        :since since :until until)
      :else ;; non-optimized path
      (legacy-query/generic-filter->query-new "v.id, v.event_id, v.created_at" filter
        :override-limit override-limit
        :?endcap-row-id endcap-row-id
        :?endcap-created-at ?endcap-created-at))))

(defn- make-union-query
  [multiple-sql-params]
  (apply vector
    (str/join " union all " (map first multiple-sql-params))
    (mapcat rest multiple-sql-params)))

(defn active-filters->query
  [active-filters]
  {:pre [(not-empty active-filters)]}
  (make-union-query
    (map-indexed
      (fn [idx {:keys [the-filter page-size remaining-quota cursor-row-id] :as _active-filter}]
        (let [[sql & sql-args] (filter->query the-filter
                                 :endcap-row-id cursor-row-id
                                 :override-limit (min remaining-quota page-size))]
          (apply vector (format "select %d as filter_index, * from (%s)" idx sql) sql-args)))
      active-filters)))

(defn init-active-filter
  [{:keys [limit] :as f}
   & {:keys [page-size cursor-row-id override-quota]
      :or {page-size 250
           cursor-row-id Integer/MAX_VALUE}}]
  (->ActiveFilterState f page-size (or override-quota limit Integer/MAX_VALUE) cursor-row-id))

(defn next-active-filters
  [active-filters prev-page-stats]
  (reduce
    (fn [acc [filter-idx active-filter]]
      (if-let [{:keys [num-results min-seen-row-id]} (get prev-page-stats filter-idx)]
        (let [new-active-filter (-> active-filter
                                  (update :remaining-quota - num-results)
                                  (assoc :cursor-row-id (dec min-seen-row-id)))]
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
    (fn [acc {:keys [filter_index id]}]
      {:pre [(number? filter_index) (some? id)]}
      (-> acc
        (update
          filter_index
          (fnil (fn [curr-stats]
                  (-> curr-stats
                    (update :num-results inc)
                    (update :min-seen-row-id min id)))
            (->ActiveFilterPageStats 0 id))))
      )
    {}
    filters-query-results))
