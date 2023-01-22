(ns me.untethr.nostr.common.domain
  (:import (javax.sql DataSource)))

(def track-bytes-in-out?
  ;; before enabling this, determine if it has impact on gc / young gen
  false)

(defrecord DatabaseCxns
  [^DataSource readonly-datasource
   ^DataSource writeable-datasource
   ^DataSource readonly-kv-datasource
   ^DataSource writeable-kv-datasource
   writeable-connection-singleton-atom
   writeable-kv-connection-singleton-atom])

(defn init-database-cxns
  [readonly-datasource writeable-datasource readonly-kv-datasource
   writeable-kv-datasource]
  (->DatabaseCxns readonly-datasource writeable-datasource
    readonly-kv-datasource writeable-kv-datasource (atom nil) (atom nil)))

(defrecord TableMaxRowIds
  [n-events-id p-tags-id e-tags-id x-tags-id])

(defrecord IndexEventContinuation
  [obo-row-id ;; row id in n_events
   p-tags-insert-batch
   e-tags-insert-batch
   x-tags-insert-batch])

(defn empty-continuation?
  [^IndexEventContinuation continuation]
  (and
    (empty? (:p-tags-insert-batch continuation))
    (empty? (:e-tags-insert-batch continuation))
    (empty? (:x-tags-insert-batch continuation))))

(defn continuation?
  [obj]
  (instance? IndexEventContinuation obj))
