(ns me.untethr.nostr.subscribe
  "Note: we're thread-safe here w/in subscribe! and unsubscribe! but upstream
   is expected to not concurrently perform these ops for the same channel-id / conn."
  (:require
    [me.untethr.nostr.metrics :as metrics]
    [me.untethr.nostr.util :as util]
    [clojure.tools.logging :as log])
  (:import (com.google.common.collect Sets)
           (java.util Set Collection)))

(defrecord CompiledFilter
  [sid ids kinds e# p# authors since until observer])

(defrecord Subs
  [channel-id->sids
   sid->filters ;; not compiled filters
   author->filters
   p#->filters
   e#->filters
   id->filters
   firehose-filters])

(defn create-empty-subs
  []
  (->Subs {} {} {} {} {} {} {}))

(defn- compiled-filter-matches?
  [compiled-filter id pubkey created_at kind tags]
  ;; CONSIDER: compiling efficient matches? as part of compiled filter
  ;; CONSIDER: splitting tag types upstream so we don't do two iters here
  (and
    (or (nil? (:e# compiled-filter)) (some (fn [[_ arg0]] (contains? (:e# compiled-filter) arg0)) (filter #(= "e" (nth % 0)) tags)))
    (or (nil? (:p# compiled-filter)) (some (fn [[_ arg0]] (contains? (:p# compiled-filter) arg0)) (filter #(= "p" (nth % 0)) tags)))
    (or (nil? (:ids compiled-filter)) (contains? (:ids compiled-filter) id))
    (or (nil? (:kinds compiled-filter)) (contains? (:kinds compiled-filter) kind))
    (or (nil? (:authors compiled-filter)) (contains? (:authors compiled-filter) pubkey))
    (or (nil? (:since compiled-filter)) (>= created_at (:since compiled-filter)))
    (or (nil? (:until compiled-filter)) (<= created_at (:until compiled-filter)))))

(defn- candidate-filters
  ^Set [subs-snapshot id pubkey tags]
  (let [candidates (Sets/newIdentityHashSet)]
    (->> [:author->filters pubkey] (get-in subs-snapshot) vals ^Collection (into []) (.addAll candidates))
    (->> [:id->filters id] (get-in subs-snapshot) vals ^Collection (into []) (.addAll candidates))
    (->> :firehose-filters (get subs-snapshot) vals ^Collection (into []) (.addAll candidates))
    (doseq [[tag-kind arg0] tags]
      (condp = tag-kind
        "e" (->> [:e#->filters arg0] (get-in subs-snapshot) vals ^Collection (into []) (.addAll candidates))
        "p" (->> [:p#->filters arg0] (get-in subs-snapshot) vals ^Collection (into []) (.addAll candidates))))
    candidates))

(defn notify!
  [metrics subs-atom {:keys [id pubkey created_at kind tags] :as _e} raw-event]
  (let [^Set observers (Sets/newIdentityHashSet)
        ^Set candidates (candidate-filters @subs-atom id pubkey tags)]
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
  [sid filter observer]
  (->CompiledFilter
    sid
    (some-> filter :ids not-empty set)
    (some-> filter :kinds not-empty set)
    (some-> filter :#e not-empty set)
    (some-> filter :#p not-empty set)
    (some-> filter :authors not-empty set)
    (some-> filter :since)
    (some-> filter :until)
    observer))

(defn- subscribe!*
  [subs channel-id sid filters observer]
  (reduce
    (fn [subs' {:keys [ids authors] e# :#e p# :#p :as filter}]
      (let [compiled-filter (compile-filter sid filter observer) ;; singleton across registry (identity determines uniqueness)
            subs' (update-in subs' [:sid->filters sid] (fnil conj []) filter)] ;; not compiled filter!
        (if (every? empty? [authors p# ids e#])
          (update subs' :firehose-filters assoc sid compiled-filter)
          (as-> subs' s
            (reduce #(assoc-in %1 [:author->filters %2 sid] compiled-filter) s authors)
            (reduce #(assoc-in %1 [:p#->filters %2 sid] compiled-filter) s p#)
            (reduce #(assoc-in %1 [:id->filters %2 sid] compiled-filter) s ids)
            (reduce #(assoc-in %1 [:e#->filters %2 sid] compiled-filter) s e#)))))
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
          (as-> subs' s
            (reduce #(update-in %1 [:author->filters %2] dissoc sid) s authors)
            (reduce #(util/dissoc-in-if-empty %1 [:author->filters %2]) s authors)
            (reduce #(update-in %1 [:p#->filters %2] dissoc sid) s p#)
            (reduce #(util/dissoc-in-if-empty %1 [:p#->filters %2]) s p#)
            (reduce #(update-in %1 [:id->filters %2] dissoc sid) s ids)
            (reduce #(util/dissoc-in-if-empty %1 [:id->filters %2]) s ids)
            (reduce #(update-in %1 [:e#->filters %2] dissoc sid) s e#)
            (reduce #(util/dissoc-in-if-empty %1 [:e#->filters %2]) s e#))))
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

(defn subscribe!
  "Does not wipe out prior subscription with same req-id; upstream is expected
   to unsubscribe! priors before subscribe!."
  [subs-atom channel-id req-id filters observer]
  {:pre [(not-empty filters)]}
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
