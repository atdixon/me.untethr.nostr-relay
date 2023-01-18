(ns me.untethr.nostr.page.metrics-porcelain
  (:require
    [hiccup.core :refer :all :exclude [html]]
    [me.untethr.nostr.common.metrics])
  (:import (com.codahale.metrics Counter Gauge Histogram Meter MetricFilter MetricRegistry Snapshot Timer)
           (com.codahale.metrics.jvm MemoryUsageGaugeSet)))

(defn- count-of
  ([metrics k]
   (count-of (k metrics)))
  ([obj]
   (cond
     (instance? Counter obj) (.getCount ^Counter obj)
     (instance? Histogram obj) (.getCount ^Histogram obj)
     (instance? Meter obj) (.getCount ^Meter obj)
     (instance? Timer obj) (.getCount ^Timer obj))))

(defn- value-of
  ([metrics k]
   (value-of (k metrics)))
  ([^Gauge gauge]
   (.getValue gauge)))

;; --

(defn- get-gauge
  [^MetricRegistry registry suffix]
  {:post [(some? %)]}
  (let [all-matches
        (vals (.getGauges registry (MetricFilter/endsWith suffix)))]
    (when (= 1 (count all-matches))
      (first all-matches))))

(defn- active-readonly-db-connections
  [metrics]
  (or
    (some->
      (get-gauge (:codahale-registry metrics) "db-kv-readonly.pool.ActiveConnections")
      (value-of)) "???"))

(defn- pending-readonly-db-connections
  [metrics]
  (or
    (some->
      (get-gauge (:codahale-registry metrics) "db-kv-readonly.pool.PendingConnections")
      (value-of)) "???"))

(defn- active-readonly-db-kv-connections
  [metrics]
  (or
    (some->
      (get-gauge (:codahale-registry metrics) "db-kv-readonly.pool.ActiveConnections")
      (value-of)) "???"))

(defn- pending-readonly-db-kv-connections
  [metrics]
  (or
    (some->
      (get-gauge (:codahale-registry metrics) "db-kv-readonly.pool.PendingConnections")
      (value-of)) "???"))

(defn- histo-summary-as-markup
  ([metrics k]
   (histo-summary-as-markup (k metrics)))
  ([obj]
   (let [divisor (if (instance? Timer obj) 1000000 1)
         ^Snapshot snap (cond
                          (instance? Histogram obj) (.getSnapshot ^Histogram obj)
                          (instance? Timer obj) (.getSnapshot ^Timer obj))]
     [:span (long (/ (.getMin snap) divisor)) "/" (long (/ (.getMean snap) divisor))
      "/" [:b (long (/ (.getMedian snap) divisor))] "/"
      (long (/ (.get999thPercentile snap) divisor))])))

(defn- meter-summary-as-markup
  ([metrics k]
   (meter-summary-as-markup (k metrics)))
  ([^Meter obj]
   (let [oneMinRate (.getOneMinuteRate obj)
         fiveMinRate (.getFiveMinuteRate obj)
         fifteenMinRate (.getFifteenMinuteRate obj)]
     [:span [:b (format "%.2f" oneMinRate)] "/"
      (format "%.2f/%.2f /s 1m/5m/15m" fiveMinRate fifteenMinRate)])))

(defn- jvm-memory-summary-as-markup
  [^MemoryUsageGaugeSet x]
  (let [m (.getMetrics x)
        used-bytes (value-of (.get m "total.used"))
        max-bytes (value-of (.get m "total.max"))
        percentage (long (* 100 (/ used-bytes max-bytes)))]
    [:span
     [:b (long (/ used-bytes 1000000))] "M / "
     [:b (long (/ max-bytes 1000000))] "M (" percentage "%)"]))

;; --

(defn- delegate
  [metrics]
  (tap> (.getNames (:codahale-registry metrics)))
  (hiccup.core/html
    [:body
     [:div
      [:h3 "Connections"]
      [:div [:b (count-of metrics :websocket-counter)]
       " active websocket channels (" [:b (value-of metrics :websocket-registry-size)]
       " in registry)."]
      [:div [:b (value-of metrics :active-subscriptions)] " active subscriptions"
       " ("
       [:b (value-of metrics :active-firehose-filters)] " firehose filters; "
       [:b (value-of metrics :active-filter-prefixes)] " prefix filters)."]
      [:div [:b (active-readonly-db-connections metrics)] " active readonly db connections ("
       [:b (pending-readonly-db-kv-connections metrics)] " pending!)"]
      [:div [:b (active-readonly-db-connections metrics)] " active readonly db kv connections ("
       [:b (pending-readonly-db-kv-connections metrics)] " pending!)"]
      [:br]
      [:div [:span "Among "
             [:b (count-of metrics :websocket-lifetime-secs)] " websocket channels"
             ", lifespan is " (histo-summary-as-markup metrics :websocket-lifetime-secs)
             " seconds."]]
      [:h3 "Fulfillment"]
      [:div (value-of metrics :active-fullfillments) " active fulfillments; "
       (count-of metrics :fulfillment-active-threads-counter) " threads working at them."]
      [:br]
      [:div (histo-summary-as-markup metrics :fulfillment-timer) " ms per batch."]
      [:div (histo-summary-as-markup metrics :fulfillment-overall-timer) " ms each overall"
       " (serving " (histo-summary-as-markup metrics :fulfillment-num-rows) " rows.)"]
      [:br]
      [:div [:b (count-of metrics :fulfillment-overall-timer)] " overall fulfillments completed"
       " (" (count-of metrics :fulfillment-timer) " pages completed)."]
      [:div [:b (count-of metrics :fulfillment-interrupt)] " cancellations."]
      [:div [:b (count-of metrics :fulfillment-error)] " errors."]
      [:h3 "DB"]
      [:div "Checkpoint Rate (full): " (meter-summary-as-markup metrics :db-checkpoint-full)]
      [:div "Checkpoint Rate (partial): " (meter-summary-as-markup metrics :db-checkpoint-partial)]
      [:div "Checkpoint Pages: " (histo-summary-as-markup metrics :db-checkpoint-pages)]
      [:div "Checkpoint Latency: " (histo-summary-as-markup metrics :db-checkpoint-timer) " ms"]
      [:div "Checkpoint Rate (kv; full): " (meter-summary-as-markup metrics :db-kv-checkpoint-full)]
      [:div "Checkpoint Rate (kv; partial): " (meter-summary-as-markup metrics :db-kv-checkpoint-partial)]
      [:div "Checkpoint Pages (kv): " (histo-summary-as-markup metrics :db-kv-checkpoint-pages)]
      [:div "Checkpoint Latency (kv): " (histo-summary-as-markup metrics :db-kv-checkpoint-timer) " ms"]
      [:br]
      [:div "Write Thread Backlog: " (value-of metrics :write-thread-backlog)]
      [:h3 "Events"]
      ;; latencies
      [:div "Verify: " (histo-summary-as-markup metrics :verify-timer) " ms"]
      [:div "Notify: " (histo-summary-as-markup metrics :notify-event-timer) " ms"
       " (" (histo-summary-as-markup metrics :notify-num-candidates) " filter candidates)"]
      [:div "Store: " (histo-summary-as-markup metrics :store-event-timer) " ms"]
      [:div "Continuation: " (histo-summary-as-markup metrics :exec-continuation-timer) " ms"]
      [:div "Purged: " (histo-summary-as-markup metrics :db-purge-deleted-timer) " ms"]
      [:br]
      ;; total counts and window throughputs
      [:div "Stored (or replaced): " (count-of metrics :stored-event)
       " (rate = " (meter-summary-as-markup metrics :stored-event) ")"]
      [:div "Continuation: " (count-of metrics :exec-continuation-timer)]
      [:div "Purged: " (count-of metrics :db-purge-deleted-timer) " (" (histo-summary-as-markup metrics :db-sweep-limit) " at a time)"]
      [:div "Duplicate: " (count-of metrics :duplicate-event)
       " (rate = " (meter-summary-as-markup metrics :duplicate-event) ")"]
      [:div "Rejected: " (count-of metrics :rejected-event)
       " (rate = " (meter-summary-as-markup metrics :rejected-event) ")"]
      [:div "Invalid: " (count-of metrics :invalid-event)]
      [:div "Problem: " (count-of metrics :problem-message)]
      [:h3 "Other"]
      [:div "JVM Heap Usage: " (jvm-memory-summary-as-markup (:jvm-memory-usage metrics))]
      [:br]
      [:div "Quick row count: " (value-of metrics :quick-row-count)]
      [:div "Quick row count (p-tags): " (value-of metrics :quick-row-count-p-tags)]
      [:div "Quick row count (e-tags): " (value-of metrics :quick-row-count-e-tags)]
      [:div "Quick row count (x-tags): " (value-of metrics :quick-row-count-x-tags)]
      [:br]
      [:div "Channel insert: " (histo-summary-as-markup metrics :insert-channel-timer) " ms"]
      ]]))

(defn html
  [metrics]
  (delegate metrics))
