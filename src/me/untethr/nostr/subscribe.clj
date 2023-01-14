(ns me.untethr.nostr.subscribe
  "Note: we're thread-safe here w/in subscribe! and unsubscribe! but upstream
   is expected to not concurrently perform these ops for the same channel-id / conn."
  (:require
    [clojure.string :as str]
    [me.untethr.nostr.common.metrics :as metrics]
    [me.untethr.nostr.util :as util]
    [me.untethr.nostr.common.validation :as validation]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [clojure.set :as set])
  (:import (com.google.common.collect Sets)
           (java.util Set Collection)))

(defrecord CompiledFilter
  ;; all-tags will be a map as in {"a" ["foo" "bar"] "p" ["..."] ...}
  [sid ids ids-prefixes kinds all-tags authors authors-prefixes since until observer])

(defrecord Subs
  [channel-id->sids
   sid->filters ;; not compiled filters
   author->filters
   author-prefix->filters
   p#->filters
   e#->filters
   id->filters
   id-prefix->filters
   firehose-filters])

(defn create-empty-subs
  []
  (->Subs {} {} {} {} {} {} {} {} {}))

(defn- compiled-filter-matches?
  [compiled-filter id pubkey created_at kind tags]
  (and
    (let [all-tags-filter (:all-tags compiled-filter)]
      (or (nil? all-tags-filter)
        (let [indexed-tags
              (reduce
                (fn [acc [tag-name tag-val]]
                  (update acc tag-name (fnil conj #{}) tag-val))
                {}
                tags)]
          (every?
            (fn [[filter-tag-name filter-tag-vals]]
              (and (contains? indexed-tags filter-tag-name)
                (not
                  (empty?
                    (set/intersection filter-tag-vals (get indexed-tags filter-tag-name))))))
            all-tags-filter))))
    (or
      (and (nil? (:ids compiled-filter)) (nil? (:ids-prefixes compiled-filter)))
      (and (nil? (:ids compiled-filter)) (some #(str/starts-with? id %) (:ids-prefixes compiled-filter)))
      (and (nil? (:ids-prefixes compiled-filter)) (contains? (:ids compiled-filter) id))
      (and (some #(str/starts-with? id %) (:ids-prefixes compiled-filter))
        (contains? (:ids compiled-filter) id)))
    (or (nil? (:kinds compiled-filter)) (contains? (:kinds compiled-filter) kind))
    (or
      (and (nil? (:authors compiled-filter)) (nil? (:authors-prefixes compiled-filter)))
      (and (nil? (:authors compiled-filter)) (some #(str/starts-with? pubkey %) (:authors-prefixes compiled-filter)))
      (and (nil? (:ids-prefixes compiled-filter)) (contains? (:authors compiled-filter) pubkey))
      (and (some #(str/starts-with? pubkey %) (:authors-prefixes compiled-filter))
        (contains? (:authors compiled-filter) pubkey)))
    (or (nil? (:since compiled-filter)) (>= created_at (:since compiled-filter)))
    (or (nil? (:until compiled-filter)) (<= created_at (:until compiled-filter)))))

(defn- candidate-filters
  ^Set [subs-snapshot id pubkey tags]
  (let [candidates (Sets/newIdentityHashSet)]
    (->> [:author->filters pubkey] (get-in subs-snapshot) vals flatten ^Collection (into []) (.addAll candidates))
    (->> [:id->filters id] (get-in subs-snapshot) vals flatten ^Collection (into []) (.addAll candidates))
    ;; note: that firehose filters presently include nip-12 generic tag queries
    (->> :firehose-filters (get subs-snapshot) vals flatten ^Collection (into []) (.addAll candidates))
    (when-let [author-prefix->filters (:author-prefix->filters subs-snapshot)]
      (dotimes [idx (dec (count pubkey))]
        (let [prefix (subs pubkey 0 idx)]
          (->> (get author-prefix->filters prefix) vals flatten ^Collection (into []) (.addAll candidates)))))
    (when-let [id-prefix->filters (:id-prefix->filters subs-snapshot)]
      (dotimes [idx (dec (count id))]
        (let [prefix (subs id 0 idx)]
          (->> (get id-prefix->filters prefix) vals flatten ^Collection (into []) (.addAll candidates)))))
    (doseq [[tag-kind arg0] tags]
      (condp = tag-kind
        "e" (->> [:e#->filters arg0] (get-in subs-snapshot) vals flatten ^Collection (into []) (.addAll candidates))
        "p" (->> [:p#->filters arg0] (get-in subs-snapshot) vals flatten ^Collection (into []) (.addAll candidates))
        :no-op))
    candidates))

(defn notify!
  [metrics subs-atom {:keys [id pubkey created_at kind tags] :as _e} raw-event]
  (let [^Set observers (Sets/newIdentityHashSet)
        ^Set candidates (candidate-filters @subs-atom id pubkey tags)]
    ;; todo note that if there are any candidate filters for specific kinds of queries, then we could skip filter-matches? check
    (metrics/notify-num-candidates! metrics (.size candidates))
    ;; we use a snapshot of subscriptions; if a cancellation arrives
    ;; as we're notifying here we may notify after cancellation. so be it.
    (doseq [candidate candidates
            :let [observer (:observer candidate)]
            :when (and
                    (compiled-filter-matches? candidate id pubkey created_at kind tags)
                    ;; do not notify same observer more than once:
                    (.add observers observer))]
      (try
        (observer raw-event)
        (catch Exception e
          (log/warn e "failed to notify observer; swallowing" {:sid (:sid candidate)}))))))

(defn- compile-filter
  [sid raw-filter observer]
  (let [{authors-whole true authors-prefixes false} (some->> raw-filter :authors (group-by (comp some? validation/hex-str-64?)))
        {ids-whole true ids-prefixes false} (some->> raw-filter :ids (group-by (comp some? validation/hex-str-64?)))]
    (->CompiledFilter
      sid
      (some-> ids-whole not-empty set)
      (some-> ids-prefixes not-empty set)
      (some-> raw-filter :kinds not-empty set)
      (as-> raw-filter x
        (select-keys x common/allowed-filter-tag-queries-set)
        (filter (comp not-empty second) x)
        (map (fn [[k v]] [(subs (name k) 1) (set v)]) x)
        (into {} x)
        (not-empty x))
      (some-> authors-whole not-empty set)
      (some-> authors-prefixes not-empty set)
      (some-> raw-filter :since)
      (some-> raw-filter :until)
      observer)))

(defn- subscribe!*
  [subs channel-id sid filters observer]
  (reduce
    (fn [subs' {:keys [ids authors] e# :#e p# :#p :as filter}]
      (let [compiled-filter (compile-filter sid filter observer) ;; singleton across registry (identity determines uniqueness)
            subs' (update-in subs' [:sid->filters sid] (fnil conj []) filter)] ;; not compiled filter!
        (if (every? empty? [authors p# ids e#])
          (update subs' :firehose-filters update sid (fnil conj []) compiled-filter)
          (as-> subs' s
            (reduce #(update-in %1 [:author->filters %2 sid] (fnil conj []) compiled-filter) s (:authors compiled-filter))
            (reduce #(update-in %1 [:author-prefix->filters %2 sid] (fnil conj []) compiled-filter) s (:authors-prefixes compiled-filter))
            (reduce #(update-in %1 [:p#->filters %2 sid] (fnil conj []) compiled-filter) s p#)
            (reduce #(update-in %1 [:id->filters %2 sid] (fnil conj []) compiled-filter) s (:ids compiled-filter))
            (reduce #(update-in %1 [:id-prefix->filters %2 sid] (fnil conj []) compiled-filter) s (:ids-prefixes compiled-filter))
            (reduce #(update-in %1 [:e#->filters %2 sid] (fnil conj []) compiled-filter) s e#)))))
    (update-in subs [:channel-id->sids channel-id] (fnil conj #{}) sid)
    filters))

(defn- unsubscribe!*
  [subs channel-id sid]
  (let [subs' (-> subs
                (update-in [:channel-id->sids channel-id] disj sid)
                (util/dissoc-in-if-empty [:channel-id->sids channel-id])
                (update :sid->filters dissoc sid))]
    (reduce
      (fn [subs' {:keys [ids authors] e# :#e p# :#p :as _filter}]
        (if (every? empty? [authors p# ids e#])
          (update subs' :firehose-filters dissoc sid)
          (let [{authors-whole true authors-prefixes false} (group-by (comp some? validation/hex-str-64?) authors)
                {ids-whole true ids-prefixes false} (group-by (comp some? validation/hex-str-64?) ids)]
            (as-> subs' s
              (reduce #(update-in %1 [:author->filters %2] dissoc sid) s authors-whole)
              (reduce #(util/dissoc-in-if-empty %1 [:author->filters %2]) s authors-whole)
              (reduce #(update-in %1 [:author-prefix->filters %2] dissoc sid) s authors-prefixes)
              (reduce #(util/dissoc-in-if-empty %1 [:author-prefix->filters %2]) s authors-prefixes)
              (reduce #(update-in %1 [:p#->filters %2] dissoc sid) s p#)
              (reduce #(util/dissoc-in-if-empty %1 [:p#->filters %2]) s p#)
              (reduce #(update-in %1 [:id->filters %2] dissoc sid) s ids-whole)
              (reduce #(util/dissoc-in-if-empty %1 [:id->filters %2]) s ids-whole)
              (reduce #(update-in %1 [:id-prefix->filters %2] dissoc sid) s ids-prefixes)
              (reduce #(util/dissoc-in-if-empty %1 [:id-prefix->filters %2]) s ids-prefixes)
              (reduce #(update-in %1 [:e#->filters %2] dissoc sid) s e#)
              (reduce #(util/dissoc-in-if-empty %1 [:e#->filters %2]) s e#)))))
      subs'
      (get-in subs [:sid->filters sid]))))

(defn num-subscriptions
  ([subs-atom]
   (count (get @subs-atom :channel-id->sids)))
  ([subs-atom channel-id]
   (count (get-in @subs-atom [:channel-id->sids channel-id]))))

(defn num-filters
  [subs-atom channel-id]
  (reduce +
    (map
      #(count (get-in @subs-atom [:sid->filters %]))
      (get-in @subs-atom [:channel-id->sids channel-id]))))

(defn num-firehose-filters
  [subs-atom]
  (count (get @subs-atom :firehose-filters)))

(defn num-filters-prefixes
  [subs-atom]
  ;; note: this is the number of distinct prefixes across filters, but will be
  ;; less than or equal to the number of filters including those prefixes.
  (+
    (count (get @subs-atom :id-prefix->filters))
    (count (get @subs-atom :author-prefix->filters))))

(defn subscribe!
  "Does not wipe out prior subscription with same req-id; upstream is expected
   to unsubscribe! priors before subscribe!."
  [subs-atom channel-id req-id filters observer]
  {:pre [(not (empty? filters))]}
  (let [sid (str channel-id ":" req-id)]
    (swap! subs-atom #(subscribe!* % channel-id sid filters observer))))

(defn unsubscribe!
  [subs-atom channel-id req-id]
  (let [sid (str channel-id ":" req-id)]
    (swap! subs-atom #(unsubscribe!* % channel-id sid))))

(defn unsubscribe-all!
  [subs-atom channel-id]
  (swap! subs-atom
    (fn [subs]
      (reduce
        #(unsubscribe!* %1 channel-id %2)
        subs
        (get-in subs [:channel-id->sids channel-id])))))
