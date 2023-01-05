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
     (instance? Meter obj) (.getCount ^Meter obj))))

(defn- value-of
  ([metrics k]
   (value-of (k metrics)))
  ([^Gauge gauge]
   (.getValue gauge)))

;; --

(defn- get-gauge
  [^MetricRegistry registry suffix]
  (let [all-matches
        (vals (.getGauges registry (MetricFilter/endsWith suffix)))]
    (when (= 1 (count all-matches))
      (first all-matches))))

(defn- active-db-connections
  [metrics]
  (value-of (get-gauge (:codahale-registry metrics) "ActiveConnections")))

(defn- pending-db-connections
  [metrics]
  (value-of (get-gauge (:codahale-registry metrics) "PendingConnections")))

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

(defn- jvm-memory-summary-as-markup
  [^MemoryUsageGaugeSet x]
  (let [m (.getMetrics x)
        used-bytes (value-of (.get m "total.used"))
        max-bytes (value-of (.get m "total.max"))
        percentage (* 100 (long (/ used-bytes max-bytes)))]
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
      [:div [:b (active-db-connections metrics)] " active db connections ("
       [:b (pending-db-connections metrics)] " pending!)"]
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
      [:div [:b (count-of metrics :fulfillment-interrupt)] " cancellations."]
      [:div [:b (count-of metrics :fulfillment-error)] " errors."]
      [:h3 "Events"]
      [:div "Verify: " (histo-summary-as-markup metrics :verify-timer) " ms"]
      [:div "Notify: " (histo-summary-as-markup metrics :notify-event-timer) " ms"
       " (" (histo-summary-as-markup metrics :notify-num-candidates) " filter candidates)"]
      [:div "Store: " (histo-summary-as-markup metrics :store-event-timer) " ms"]
      [:br]
      [:div "Duplicate: " (count-of metrics :duplicate-event)]
      [:div "Rejected: " (count-of metrics :rejected-event)]
      [:div "Invalid: " (count-of metrics :invalid-event)]
      [:div "Problem: " (count-of metrics :problem-message)]
      [:h3 "Other"]
      [:div "JVM Heap Usage: " (jvm-memory-summary-as-markup (:jvm-memory-usage metrics))]
      [:br]
      [:div "Channel insert: " (histo-summary-as-markup metrics :insert-channel-timer) " ms"]
      ]]))

(defn html
  [metrics]
  (delegate metrics))
