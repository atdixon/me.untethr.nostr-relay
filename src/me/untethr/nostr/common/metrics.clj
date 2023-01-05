(ns me.untethr.nostr.common.metrics
  (:require
    [metrics.core :as metrics-core :refer [new-registry]]
    [metrics.counters :refer [counter inc! dec!]]
    [metrics.gauges :refer [gauge gauge-fn]]
    [metrics.histograms :refer [histogram update!]]
    [metrics.meters :refer [meter mark!]]
    [metrics.timers :refer [timer time!]])
  (:import (com.codahale.metrics Gauge MetricRegistry Meter Histogram Timer Counter)
           (com.codahale.metrics.json MetricsModule)
           (java.util.concurrent TimeUnit)
           (com.codahale.metrics.jvm MemoryUsageGaugeSet)))

(defrecord Metrics
  [^MetricRegistry codahale-registry
   ^MemoryUsageGaugeSet jvm-memory-usage
   ^Gauge quick-row-count
   ^Gauge websocket-registry-size
   ^Gauge active-subscriptions
   ^Gauge active-filter-prefixes
   ^Gauge active-firehose-filters
   ^Gauge active-fullfillments
   ^Counter websocket-counter
   ^Meter websocket-created
   ^Histogram websocket-lifetime-secs
   ^Timer verify-timer
   ^Timer store-event-timer
   ^Timer insert-channel-timer
   ^Timer notify-event-timer
   ^Histogram notify-num-candidates
   ^Meter problem-message
   ^Meter rejected-event
   ^Meter invalid-event
   ^Meter duplicate-event
   ^Timer subscribe-timer
   ^Timer unsubscribe-timer
   ^Timer unsubscribe-all-timer
   ^Counter subscribe-excessive-filters-counter
   ^Timer fulfillment-timer
   ^Timer fulfillment-overall-timer
   ^Histogram fulfillment-num-rows
   ^Meter fulfillment-interrupt
   ^Meter fulfillment-error
   ^Counter fulfillment-active-threads-counter
   ])

(defn create-jackson-metrics-module
  []
  (MetricsModule. (TimeUnit/SECONDS) (TimeUnit/MILLISECONDS) false))

(defn memory-usage-gauge-set
  ^MemoryUsageGaugeSet []
  (proxy [MemoryUsageGaugeSet] []
    (getMetrics []
      (select-keys (proxy-super getMetrics) ["total.used" "total.max"]))))

(defn create-metrics
  ([] ;; arity for testing
   (create-metrics
     (constantly -1) (constantly -1) (constantly -1) (constantly -1) (constantly -1) (constantly -1)))
  ([quick-row-count-fn websocket-registry-size-fn num-subscriptions-fn num-filter-prefixes-fn
    num-firehose-filters-fn num-fulfillments-fn]
   (let [codahale (new-registry)]
     (->Metrics
       codahale
       (metrics-core/add-metric codahale ["jvm" "memory"] (memory-usage-gauge-set))
       (gauge-fn codahale ["app" "store" "quick-row-count"] quick-row-count-fn)
       (gauge-fn codahale ["app" "websocket-registry" "size-est"] websocket-registry-size-fn)
       (gauge-fn codahale ["app" "subscribe" "active-subscriptions"] num-subscriptions-fn)
       (gauge-fn codahale ["app" "subscribe" "active-filter-prefixes"] num-filter-prefixes-fn)
       (gauge-fn codahale ["app" "subscribe" "active-firehose-filters"] num-firehose-filters-fn)
       (gauge-fn codahale ["app" "subscribe" "fulfillment-active"] num-fulfillments-fn)
       (counter codahale ["app" "websocket" "active"])
       (meter codahale ["app" "websocket" "created"])
       (histogram codahale ["app" "websocket" "lifetime-secs"])
       (timer codahale ["app" "event" "verify"])
       (timer codahale ["app" "event" "store"])
       (timer codahale ["app" "channel" "insert"])
       (timer codahale ["app" "event" "notify"])
       (histogram codahale ["app" "subscribe" "notify-num-candidates"])
       (meter codahale ["app" "message" "problem"])
       (meter codahale ["app" "event" "rejected"])
       (meter codahale ["app" "event" "invalid"])
       (meter codahale ["app" "event" "duplicate"])
       (timer codahale ["app" "event" "subscribe"])
       (timer codahale ["app" "event" "unsubscribe"])
       (timer codahale ["app" "event" "unsubscribe-all"])
       (counter codahale ["app" "subscribe" "excessive-filter-count"])
       (timer codahale ["app" "subscribe" "fulfillment"])
       (timer codahale ["app" "subscribe" "fulfillment-overall"])
       (histogram codahale ["app" "subscribe" "fulfillment-num-rows"])
       (meter codahale ["app" "subscribe" "fulfillment-interrupt"])
       (meter codahale ["app" "subscribe" "fulfillment-error"])
       (counter codahale ["app" "subscribe" "fulfillment-active-threads"])))))

(defn websocket-open!
  [metrics]
  (mark! (:websocket-created metrics))
  (inc! (:websocket-counter metrics)))

(defn websocket-close!
  [metrics duration-ms]
  (dec! (:websocket-counter metrics))
  (update! (:websocket-lifetime-secs metrics) (quot duration-ms 1000)))

(defn duplicate-event!
  [metrics]
  (mark! (:duplicate-event metrics)))

(defn invalid-event!
  [metrics]
  (mark! (:invalid-event metrics)))

(defn problem-message!
  [metrics]
  (mark! (:problem-message metrics)))

(defn rejected-event!
  [metrics]
  (mark! (:rejected-event metrics)))

(defmacro time-verify!
  [metrics & body]
  `(time! (:verify-timer ~metrics) ~@body))

(defmacro time-store-event!
  [metrics & body]
  `(time! (:store-event-timer ~metrics) ~@body))

(defmacro time-insert-channel!
  [metrics & body]
  `(time! (:insert-channel-timer ~metrics) ~@body))

(defmacro time-notify-event!
  [metrics & body]
  `(time! (:notify-event-timer ~metrics) ~@body))

(defn notify-num-candidates!
  [metrics n]
  (update! (:notify-num-candidates metrics) n))

(defmacro time-subscribe!
  [metrics & body]
  `(time! (:subscribe-timer ~metrics) ~@body))

(defmacro time-unsubscribe!
  [metrics & body]
  `(time! (:unsubscribe-timer ~metrics) ~@body))

(defmacro time-unsubscribe-all!
  [metrics & body]
  `(time! (:unsubscribe-all-timer ~metrics) ~@body))

(defn inc-excessive-filters!
  [metrics]
  (inc! (:subscribe-excessive-filters-counter metrics)))

(defn update-overall-fullfillment-millis!
  [metrics millis]
  (.update ^Timer (:fulfillment-overall-timer metrics) millis TimeUnit/MILLISECONDS))

(defmacro time-fulfillment!
  [metrics & body]
  `(time!
     (:fulfillment-timer ~metrics)
     (inc! (:fulfillment-active-threads-counter ~metrics))
     (try
       ~@body
       (catch Throwable t#
         (throw t#))
       (finally
         (dec! (:fulfillment-active-threads-counter ~metrics))))))

(defn fulfillment-num-rows!
  [metrics n]
  (update! (:fulfillment-num-rows metrics) n))

(defn mark-fulfillment-interrupt!
  [metrics]
  (mark! (:fulfillment-interrupt metrics)))

(defn mark-fulfillment-error!
  [metrics]
  (mark! (:fulfillment-error metrics)))
