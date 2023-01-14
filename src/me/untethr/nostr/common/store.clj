(ns me.untethr.nostr.common.store
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [me.untethr.nostr.common :as common]
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
  ^HikariDataSource [pool-name jdbc-url]
  (HikariDataSource.
    (doto (HikariConfig.)
      (.setPoolName pool-name)
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
  (^DataSource [pool-name jdbc-url]
   (create-readonly-connection-pool pool-name jdbc-url nil))
  (^DataSource [pool-name jdbc-url ^MetricRegistry metric-registry]
   (let [the-pool (create-readonly-hikari-datasource pool-name jdbc-url)]
     (when (some? metric-registry)
       (.setMetricsTrackerFactory the-pool
         (CodahaleMetricsTrackerFactory. metric-registry)))
     the-pool)))

(defn get-simplest-datasource
  [path]
  (jdbc/get-datasource (str "jdbc:sqlite:" path)))

(def get-unpooled-writeable-datasource*
  (memoize
    #(get-simplest-datasource %)))

(def get-readonly-datasource*
  (memoize
    (fn [pool-name sqlite-file metrics-registry]
      (wrap-datasource-with-p6spy
        ;; note: open_mode=1 which creates readonly sqlite connections; with this
        ;; enabled we're free and ought to setReadOnly on the Hikari pool as well.
        (create-readonly-connection-pool pool-name
          (str "jdbc:sqlite:" sqlite-file "?open_mode=1") metrics-registry)))))

(defn- comment-line?
  [line]
  (str/starts-with? line "--"))

(defn parse-schema [schema-simple-name]
  (let [resource (io/resource (format "me/untethr/nostr/%s" schema-simple-name))]
    (with-open [reader (io/reader resource)]
      (loop [lines (line-seq reader) acc []]
        (if (next lines)
          (let [[ddl more] (split-with (complement comment-line?) lines)]
            (if (not-empty ddl)
              (recur more (conj acc (str/join "\n" ddl)))
              (recur (drop-while comment-line? lines) acc)))
          acc)))))

(defn apply-schema! [writeable-db schema-simple-name]
  {:pre [(some? writeable-db)]}
  (doseq [statement (parse-schema schema-simple-name)]
    (try
      (jdbc/execute-one! writeable-db [statement])
      (catch SQLiteException e
        (when-not
          (and
            (re-matches #"(?is)^\s*alter table.*add column.*;\s*$" statement)
            (str/includes? (ex-message e) "duplicate column name"))
          (throw e))))))

(defn ^:deprecated init!
  [path ^MetricRegistry metric-registry]
  {:readonly-datasource
   (get-readonly-datasource* "legacy-readonly-pool" path metric-registry)
   :writeable-datasource
   (doto (get-unpooled-writeable-datasource* path)
     (apply-schema! "schema.sql"))})

(defn init-new!
  [path ^MetricRegistry metric-registry]
  ;; note: must create writeable first so that in the case of a brand-spanking
  ;; new db we create it (write it!) before trying to open it in readonly mode.
  ;; otherwise, we'll get a failure.
  (let [writeable-datasource
        (doto (get-unpooled-writeable-datasource* path)
          (apply-schema! "schema-new.sql"))]
    {:writeable-datasource writeable-datasource
     :readonly-datasource
     (get-readonly-datasource*
       ;; note metrics-porcelin known dependency on the pool name provided
       ;; here:
       "db-readonly" path metric-registry)}))

(defn init-new-kv!
  [path ^MetricRegistry metric-registry]
  ;; note: must create writeable first so that in the case of a brand-spanking
  ;; new db we create it (write it!) before trying to open it in readonly mode.
  ;; otherwise, we'll get a failure.
  (let [writeable-datasource
        (doto (get-unpooled-writeable-datasource* path)
          (apply-schema! "schema-kv.sql"))]
    {:writeable-datasource writeable-datasource
     :readonly-datasource
     (get-readonly-datasource*
       ;; note metrics-porcelin known dependency on the pool name provided
       ;; here:
       "db-kv-readonly" path metric-registry)}))


(comment
  (init-new! "./nn.db" nil))

;; --

(defn checkpoint!
  [singleton-db-conn]
  {:pre [(instance? Connection singleton-db-conn)]}
  (jdbc/execute-one! singleton-db-conn ["PRAGMA wal_checkpoint(RESTART);"]))

;; --

(defn ^:deprecated max-event-rowid
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(rowid) as res from n_events"])))

(defn max-event-db-id
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(id) as res from n_events"])))

(defn insert-channel!
  [writeable-db channel-id ip-address]
  (jdbc/execute-one! writeable-db
    ["insert or ignore into channels (channel_id, ip_addr) values (?,?)"
     channel-id ip-address]))

(defn- ^:deprecated insert-event!*
  [writeable-db id pubkey created-at kind raw channel-id]
  {:post [(or (nil? %) (contains? % :rowid))]}
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into n_events"
       " (id, pubkey, created_at, kind, raw_event, channel_id)"
       " values (?, ?, ?, ?, ?, ?) returning rowid")
     id pubkey created-at kind raw channel-id]
    {:builder-fn rs/as-unqualified-lower-maps}))

(defn ^:deprecated insert-event!
  "Answers inserted sqlite rowid or nil if row already exists."
  ([writeable-db id pubkey created-at kind raw]
   (insert-event! writeable-db id pubkey created-at kind raw nil))
  ([writeable-db id pubkey created-at kind raw channel-id]
   (:rowid (insert-event!* writeable-db id pubkey created-at kind raw channel-id))))

(defn- insert-event-new-schema!*
  [writeable-db id pubkey created-at kind channel-id]
  {:post [(or (nil? %) (contains? % :id))]}
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into n_events"
       " (event_id, pubkey, created_at, kind, channel_id)"
       " values (?, ?, ?, ?, ?) returning id")
     id pubkey created-at kind channel-id]
    {:builder-fn rs/as-unqualified-lower-maps}))

(defn insert-event-new-schema!
  "Answers inserted sqlite rowid or nil if row already exists."
  ([writeable-db id pubkey created-at kind]
   (insert-event-new-schema! writeable-db id pubkey created-at kind nil))
  ([writeable-db id pubkey created-at kind channel-id]
   (:id (insert-event-new-schema!* writeable-db id pubkey created-at kind channel-id))))

(defn ^:deprecated insert-e-tag!
  [writeable-db source-event-id tagged-event-id]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into e_tags"
       " (source_event_id, tagged_event_id)"
       " values (?, ?)")
     source-event-id tagged-event-id]))

(defn insert-e-tag-new-schema!
  [writeable-db source-event-id tagged-event-id source-kind source-created-at]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into e_tags"
       " (source_event_id, tagged_event_id, source_event_kind, source_event_created_at)"
       " values (?, ?, ?, ?)")
     source-event-id tagged-event-id source-kind source-created-at]))

(defn ^:deprecated insert-p-tag!
  [writeable-db source-event-id tagged-pubkey]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into p_tags"
       " (source_event_id, tagged_pubkey)"
       " values (?, ?)")
     source-event-id tagged-pubkey]))

(defn insert-p-tag-new-schema!
  [writeable-db source-event-id tagged-pubkey source-kind source-created-at source-pubkey]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into p_tags"
       " (source_event_id, tagged_pubkey, source_event_kind, source_event_created_at, source_event_pubkey)"
       " values (?, ?, ?, ?, ?)")
     source-event-id tagged-pubkey source-kind source-created-at source-pubkey]))

(defn ^:deprecated insert-x-tag!
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

(defn insert-x-tag-new-schema!
  [writeable-db source-event-id generic-tag tagged-value source-created-at]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into x_tags"
       " (source_event_id, generic_tag, tagged_value, source_event_created_at)"
       " values (?, ?, ?, ?)")
     ;; Note: we arbitrarily limit generic tags to 2056 characters, and
     ;; we'll query with the same restriction. That is, any values that
     ;; exceed 2056 characters will match any query value that exceeds
     ;; 2056 characters whenever their first 2056 characters match.
     source-event-id
     generic-tag
     (if (> (count tagged-value) 2056)
       (subs tagged-value 0 2056)
       tagged-value)
     source-created-at]))

;; -- facade --

(defn store-event-kv!-
  "Answers event_id or nil if row already exists."
  [writeable-conn-kv event-id raw-event]
  (:event_id
    (jdbc/execute-one! writeable-conn-kv
      [(str "insert or ignore into n_kv_events"
         " (event_id, raw_event)"
         "  values (?, ?) returning event_id") event-id raw-event]
      {:builder-fn rs/as-unqualified-lower-maps})))

(defn store-event-new-schema!-
  ([writeable-conn event-obj]
   (store-event-new-schema!- writeable-conn nil event-obj))
  ([writeable-conn channel-id {:keys [id pubkey created_at kind tags] :as _e}]
   ;; use a tx, for now; don't want to answer queries with events
   ;; that don't fully exist. could denormalize or some other strat
   ;; to avoid tx if needed. NOTE that here we have to have a transaction
   ;; because we don't get a rowid back on duplicates and we'd skip tag
   ;; inserts for any event that only got written to n_events... we could
   ;; alternatively always insert tags even if it existed and potentially move
   ;; toward dropping the tx.
   (jdbc/with-transaction [tx writeable-conn]
     (if-let [rowid (insert-event-new-schema! tx id pubkey created_at kind channel-id)]
       ;; !!! NOTE presently our query and pagination strategy depend on the tags for new
       ;; source events to be inserted altogether with each rowid consecutive
       (do
         (doseq [[tag-kind arg0] tags]
           (cond
             ;; we've seen empty tags in the wild (eg {... "tags": [[], ["p", "abc.."]] })
             ;;  so we'll just handle those gracefully.
             (or (nil? tag-kind) (nil? arg0)) :no-op
             (= tag-kind "e") (insert-e-tag-new-schema! tx id arg0 kind created_at)
             (= tag-kind "p") (insert-p-tag-new-schema! tx id arg0 kind created_at pubkey)
             (common/indexable-tag-str?* tag-kind) (insert-x-tag-new-schema! tx id tag-kind arg0 created_at)
             :else :no-op))
         rowid)
       :duplicate))))

(defn index-and-store-event!
  ([writeable-conn writeable-conn-kv event-obj raw-event]
   (index-and-store-event! writeable-conn writeable-conn-kv nil event-obj raw-event))
  ([writeable-conn writeable-conn-kv channel-id {:keys [id] :as event-obj} raw-event]
   ;; consider future where we index in a queue after durable write -- would this
   ;; be ok -- implication might be that a websocket that wrote, disconnected and
   ;; tried to read might not immediately see their write -- but would we care?
   (if-let [_kv-event-id (store-event-kv!- writeable-conn-kv id raw-event)]
     (let [_x (store-event-new-schema!- writeable-conn channel-id event-obj)]
       ;; note we ignore :dupliate from store-event-new-schema!-, if for some
       ;; reason we evere see a duplicate here it means we somehow only partially
       ;; wrote i.e. wrote it but didn't index it. so we want upstream to re-notify
       ;; or otherwise treat it as not a duplicate.
       :success)
     :duplicate)))
