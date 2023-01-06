(ns me.untethr.nostr.store
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (com.codahale.metrics MetricRegistry)
           (com.p6spy.engine.spy P6DataSource)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           (com.zaxxer.hikari.metrics.dropwizard CodahaleMetricsTrackerFactory)
           (java.sql Connection)
           (javax.sql DataSource)
           (org.sqlite SQLiteException)))

(defn- wrap-datasource-with-p6spy
  [^DataSource datasource]
  (P6DataSource. datasource))

(defn- create-readonly-hikari-datasource
  ^HikariDataSource [jdbc-url]
  (HikariDataSource.
    (doto (HikariConfig.)
      ;; this must be set on Hikari pool but will fail if the underlying jdbc
      ;; connection doesn't establish the connection as readonly (see below)
      (.setReadOnly true)
      (.setJdbcUrl jdbc-url)
      ;; note: jdbc.next with-transaction disables and re-enables auto-commit
      ;; before/after running the transaction.
      (.setAutoCommit true)
      ;; how long for a connection request to wait before throwing a SQLException
      ;; from DataSource/getConnection (See also maximumPoolSize below)
      (.setConnectionTimeout (* 15 1000)) ;; 15s.
      ;; Setting 0 here means we never remove idle connections from the pool.
      (.setIdleTimeout 0)
      ;; See docs @ https://github.com/brettwooldridge/HikariCP
      (.setKeepaliveTime (* 5 60 1000)) ;; 5 minutes.
      ;; And at some point read,
      ;;  https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
      ;; In any case, we're aiming for *more* than the number of threads we
      ;; allocate that can acquire a db connection, so that we're never unable
      ;; to acquire...
      (.setMaximumPoolSize 10)
      ;; note: we leave .setMinimumIdle alone per docs recommendation.
      ;; Setting 0 here means no maximum lifetime to a connection in the pool.
      (.setMaxLifetime 0)
      ;; note: we will leave .setValidationTimeout as default. it must be
      ;; less than conn timeout. when HikariCP takes a connection from pool
      ;; this is how long it's allowed to validate it before returning it
      ;; from getConnection.
      ;; Trying 25s for now for leakDetectionThreshold.
      (.setLeakDetectionThreshold (* 25 1000)))))

(defn- create-readonly-connection-pool
  "Create a connection pool. Our configuration here has all of our connections
   living forever in a fixed-sized pool. While the pool may or may not give us
   enormous benefit over a file-sys based db like sqlite, it's integration with
   metrics gives us a ton of observability with what's happening with the db."
  (^DataSource [jdbc-url]
   (create-readonly-connection-pool jdbc-url nil))
  (^DataSource [jdbc-url ^MetricRegistry metric-registry]
   (let [the-pool (create-readonly-hikari-datasource jdbc-url)]
     (when (some? metric-registry)
       (.setMetricsTrackerFactory the-pool
         (CodahaleMetricsTrackerFactory. metric-registry)))
     the-pool)))

(def get-unpooled-writeable-datasource*
  (memoize
    #(jdbc/get-datasource (str "jdbc:sqlite:" %))))

(def get-readonly-datasource*
  (memoize
    #(wrap-datasource-with-p6spy
       ;; note: open_mode=1 which creates readonly sqlite connections; with this
       ;; enabled we're free and ought to setReadOnly on the Hikari pool as well.
       (create-readonly-connection-pool (str "jdbc:sqlite:" %1 "?open_mode=1") %2))))

(defn- comment-line?
  [line]
  (str/starts-with? line "--"))

(defn parse-schema []
  (let [resource (io/resource "me/untethr/nostr/schema.sql")]
    (with-open [reader (io/reader resource)]
      (loop [lines (line-seq reader) acc []]
        (if (next lines)
          (let [[ddl more] (split-with (complement comment-line?) lines)]
            (if (not-empty ddl)
              (recur more (conj acc (str/join "\n" ddl)))
              (recur (drop-while comment-line? lines) acc)))
          acc)))))

(defn apply-schema! [writeable-db]
  {:pre [(some? writeable-db)]}
  (doseq [statement (parse-schema)]
    (try
      (jdbc/execute-one! writeable-db [statement])
      (catch SQLiteException e
        (when-not
          (and
            (re-matches #"(?is)^\s*alter table.*add column.*;\s*$" statement)
            (str/includes? (ex-message e) "duplicate column name"))
          (throw e))))))

(defn init!
  [path ^MetricRegistry metric-registry]
  {:readonly-datasource
   (get-readonly-datasource* path metric-registry)
   :writeable-datasource
   (doto (get-unpooled-writeable-datasource* path)
     apply-schema!)})

(comment
  (init! "./n.db" nil))

;; --

(defn checkpoint!
  [singleton-db-conn]
  {:pre [(instance? Connection singleton-db-conn)]}
  (jdbc/execute-one! singleton-db-conn ["PRAGMA wal_checkpoint(RESTART);"]))

;; --

(defn max-event-rowid
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(rowid) as res from n_events"])))

(defn insert-channel!
  [writeable-db channel-id ip-address]
  (jdbc/execute-one! writeable-db
    ["insert or ignore into channels (channel_id, ip_addr) values (?,?)"
     channel-id ip-address]))

(defn- insert-event!*
  [writeable-db id pubkey created-at kind raw channel-id]
  {:post [(or (nil? %) (contains? % :rowid))]}
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into n_events"
       " (id, pubkey, created_at, kind, raw_event, channel_id)"
       " values (?, ?, ?, ?, ?, ?) returning rowid")
     id pubkey created-at kind raw channel-id]
    {:builder-fn rs/as-unqualified-lower-maps}))

(defn insert-event!
  "Answers inserted sqlite rowid or nil if row already exists."
  ([writeable-db id pubkey created-at kind raw]
   (insert-event! writeable-db id pubkey created-at kind raw nil))
  ([writeable-db id pubkey created-at kind raw channel-id]
   (:rowid (insert-event!* writeable-db id pubkey created-at kind raw channel-id))))

(defn insert-e-tag!
  [writeable-db source-event-id tagged-event-id]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into e_tags"
       " (source_event_id, tagged_event_id)"
       " values (?, ?)")
     source-event-id tagged-event-id]))

(defn insert-p-tag!
  [writeable-db source-event-id tagged-pubkey]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into p_tags"
       " (source_event_id, tagged_pubkey)"
       " values (?, ?)")
     source-event-id tagged-pubkey]))

(defn insert-x-tag!
  [writeable-db source-event-id generic-tag tagged-value]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into x_tags"
       " (source_event_id, generic_tag, tagged_value)"
       " values (?, ?, ?)")
     ;; Note: we arbitrarily limit generic tags to 2056 characters, and
     ;; we'll query with the same restriction. That is, any values that
     ;; exceed 2056 characters will match any query value that exceeds
     ;; 2056 characters whenever their first 2056 characters match.
     source-event-id
     generic-tag
     (if (> (count tagged-value) 2056)
       (subs tagged-value 0 2056)
       tagged-value)]))
