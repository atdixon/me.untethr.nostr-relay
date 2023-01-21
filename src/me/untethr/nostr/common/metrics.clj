(ns me.untethr.nostr.common.metrics
  (:require
    [me.untethr.nostr.common.ws-registry :as ws-registry]
    [me.untethr.nostr.util :as util]
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
   ^Gauge quick-row-count-p-tags
   ^Gauge quick-row-count-e-tags
   ^Gauge quick-row-count-x-tags
   ^Gauge websocket-registry-size
   ^Gauge active-subscriptions
   ^Gauge active-filter-prefixes
   ^Gauge active-firehose-filters
   ^Gauge active-fullfillments
   ^Gauge write-thread-backlog
   ^Counter websocket-counter
   ^Meter websocket-created
   ^Histogram websocket-lifetime-secs
   ^Histogram websocket-total-bytes-in
   ^Histogram websocket-total-bytes-out
   ^Histogram websocket-peak-1m-bytes-in
   ^Histogram websocket-peak-1m-bytes-out
   ^Timer verify-timer
   ^Timer store-event-timer
   ^Timer exec-continuation-timer
   ^Timer insert-channel-timer
   ^Timer notify-event-timer
   ^Histogram notify-num-candidates
   ^Meter problem-message
   ^Meter rejected-event
   ^Meter invalid-event
   ^Meter duplicate-event
   ^Meter stored-event
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
   ^Histogram db-sweep-limit
   ^Timer db-purge-deleted-timer
   ^Timer db-checkpoint-timer
   ^Histogram db-checkpoint-pages
   ^Meter db-checkpoint-partial
   ^Meter db-checkpoint-full
   ^Timer db-kv-checkpoint-timer
   ^Histogram db-kv-checkpoint-pages
   ^Meter db-kv-checkpoint-partial
   ^Meter db-kv-checkpoint-full
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
   (let [stub (constantly -1)]
     (create-metrics stub stub stub stub stub stub stub stub stub stub)))
  ([quick-n-events-row-count-fn
    quick-p-tags-row-count-fn
    quick-e-tags-row-count-fn
    quick-x-tags-row-count-fn
    websocket-registry-size-fn
    num-subscriptions-fn
    num-filter-prefixes-fn
    num-firehose-filters-fn
    num-fulfillments-fn
    write-thread-backlog-size-fn]
   (let [codahale (new-registry)]
     (->Metrics
       codahale
       (metrics-core/add-metric codahale ["jvm" "memory"] (memory-usage-gauge-set))
       (gauge-fn codahale ["app" "store" "quick-row-count"] quick-n-events-row-count-fn)
       (gauge-fn codahale ["app" "store" "quick-row-count-p-tags"] quick-p-tags-row-count-fn)
       (gauge-fn codahale ["app" "store" "quick-row-count-e-tags"] quick-e-tags-row-count-fn)
       (gauge-fn codahale ["app" "store" "quick-row-count-x-tags"] quick-x-tags-row-count-fn)
       (gauge-fn codahale ["app" "websocket-registry" "size-est"] websocket-registry-size-fn)
       (gauge-fn codahale ["app" "subscribe" "active-subscriptions"] num-subscriptions-fn)
       (gauge-fn codahale ["app" "subscribe" "active-filter-prefixes"] num-filter-prefixes-fn)
       (gauge-fn codahale ["app" "subscribe" "active-firehose-filters"] num-firehose-filters-fn)
       (gauge-fn codahale ["app" "subscribe" "fulfillment-active"] num-fulfillments-fn)
       (gauge-fn codahale ["app" "write-thread" "backlog"] write-thread-backlog-size-fn)
       (counter codahale ["app" "websocket" "active"])
       (meter codahale ["app" "websocket" "created"])
       (histogram codahale ["app" "websocket" "lifetime-secs"])
       (histogram codahale ["app" "websocket" "total-bytes-in"])
       (histogram codahale ["app" "websocket" "total-bytes-out"])
       (histogram codahale ["app" "websocket" "peak-1m-bytes-in"])
       (histogram codahale ["app" "websocket" "peak-1m-bytes-out"])
       (timer codahale ["app" "event" "verify"])
       (timer codahale ["app" "event" "store"])
       (timer codahale ["app" "event" "continuation"])
       (timer codahale ["app" "channel" "insert"])
       (timer codahale ["app" "event" "notify"])
       (histogram codahale ["app" "subscribe" "notify-num-candidates"])
       (meter codahale ["app" "message" "problem"])
       (meter codahale ["app" "event" "rejected"])
       (meter codahale ["app" "event" "invalid"])
       (meter codahale ["app" "event" "duplicate"])
       (meter codahale ["app" "event" "stored"])
       (timer codahale ["app" "event" "subscribe"])
       (timer codahale ["app" "event" "unsubscribe"])
       (timer codahale ["app" "event" "unsubscribe-all"])
       (counter codahale ["app" "subscribe" "excessive-filter-count"])
       (timer codahale ["app" "subscribe" "fulfillment"])
       (timer codahale ["app" "subscribe" "fulfillment-overall"])
       (histogram codahale ["app" "subscribe" "fulfillment-num-rows"])
       (meter codahale ["app" "subscribe" "fulfillment-interrupt"])
       (meter codahale ["app" "subscribe" "fulfillment-error"])
       (counter codahale ["app" "subscribe" "fulfillment-active-threads"])
       (histogram codahale ["db" "sweep" "limit"])
       (timer codahale ["db" "sweep" "latency"])
       (timer codahale ["db" "checkpoint" "latency"])
       (histogram codahale ["db" "checkpoint" "num-pages"])
       (meter codahale ["db" "checkpoint" "partial"])
       (meter codahale ["db" "checkpoint" "full"])
       (timer codahale ["db-kv" "checkpoint" "latency"])
       (histogram codahale ["db-kv" "checkpoint" "num-pages"])
       (meter codahale ["db-kv" "checkpoint" "partial"])
       (meter codahale ["db-kv" "checkpoint" "full"])))))

(defn websocket-open!
  [metrics]
  (mark! (:websocket-created metrics))
  (inc! (:websocket-counter metrics)))

(defn websocket-close!
  [metrics websocket-state]
  (let [duration-ms (util/nanos-to-millis
                      (- (System/nanoTime) (:start-ns websocket-state)))]
    (dec! (:websocket-counter metrics))
    (update! (:websocket-lifetime-secs metrics) (long (/ duration-ms 1000)))
    (let [total-bytes-in (.getCount ^Meter (:bytes-in websocket-state))
          total-bytes-out (.getCount ^Meter (:bytes-out websocket-state))]
      ;; we want any last bytes sent or received to get noted in our peak rates:
      (ws-registry/update-peak-1m-rates! websocket-state)
      (when (<= duration-ms 60000)
        ;; for websockets that didn't last a full minute we'll update w/ total bytes
        (ws-registry/update-peak-1m-rate-bytes-in! websocket-state total-bytes-in)
        (ws-registry/update-peak-1m-rate-bytes-out! websocket-state total-bytes-out))
      (update! (:websocket-total-bytes-in metrics) total-bytes-in)
      (update! (:websocket-total-bytes-out metrics) total-bytes-out)
      (update! (:websocket-peak-1m-bytes-in metrics) @(:peak-1m-rate-bytes-in-atom websocket-state))
      (update! (:websocket-peak-1m-bytes-out metrics) @(:peak-1m-rate-bytes-out-atom websocket-state)))))

(defn duplicate-event!
  [metrics]
  (mark! (:duplicate-event metrics)))

(defn stored-event!
  [metrics]
  (mark! (:stored-event metrics)))

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

(defmacro time-exec-continuation!
  [metrics & body]
  `(time! (:exec-continuation-timer ~metrics) ~@body))

(defmacro time-purge-deleted!
  [metrics & body]
  `(time! (:db-purge-deleted-timer ~metrics) ~@body))

(defn db-sweep-limit!
  [metrics n]
  (update! (:db-sweep-limit metrics) n))

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

(defmacro time-db-checkpoint!
  [metrics & body]
  `(time! (:db-checkpoint-timer ~metrics) ~@body))

(defn db-checkpoint-pages!
  [metrics n]
  (update! (:db-checkpoint-pages metrics) n))

(defn mark-db-checkpoint-partial!
  [metrics]
  (mark! (:db-checkpoint-partial metrics)))

(defn mark-db-checkpoint-full!
  [metrics]
  (mark! (:db-checkpoint-full metrics)))

(defmacro time-db-kv-checkpoint!
  [metrics & body]
  `(time! (:db-kv-checkpoint-timer ~metrics) ~@body))

(defn db-kv-checkpoint-pages!
  [metrics n]
  (update! (:db-kv-checkpoint-pages metrics) n))

(defn mark-db-kv-checkpoint-partial!
  [metrics]
  (mark! (:db-kv-checkpoint-partial metrics)))

(defn mark-db-kv-checkpoint-full!
  [metrics]
  (mark! (:db-kv-checkpoint-full metrics)))
