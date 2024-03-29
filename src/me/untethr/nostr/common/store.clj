(ns me.untethr.nostr.common.store
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [me.untethr.nostr.common :as common]
            [me.untethr.nostr.common.domain :as domain]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as p]
            [next.jdbc.result-set :as rs])
  (:import (com.codahale.metrics MetricRegistry)
           (com.p6spy.engine.event JdbcEventListener)
           (com.p6spy.engine.spy JdbcEventListenerFactory P6DataSource)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           (com.zaxxer.hikari.metrics.dropwizard CodahaleMetricsTrackerFactory)
           (java.sql Connection PreparedStatement)
           (javax.sql DataSource)
           (org.sqlite SQLiteConnection SQLiteDataSource SQLiteException)))

(defrecord ParsedSchema
  [pragma-statements ddl-statements])

(defn- wrap-datasource-with-p6spy
  [^DataSource datasource]
  (P6DataSource. datasource))

(defn- create-readonly-hikari-datasource
  ^HikariDataSource [pool-name ro-sqlite-ds]
  (HikariDataSource.
    (doto (HikariConfig.)
      (.setPoolName pool-name)
      ;; note: connectionInitSql would be a clean way to initialize pragma stmts
      ;;  per connection creation, but this won't work -- init sql will only run
      ;;  first statement :( -- so we're using jdbc url params instead... (see
      ;;  a few setters down)
      ;;    (.setConnectionInitSql (str/join pragma-statements))
      ;; this must be set on Hikari pool but will fail if the underlying jdbc
      ;; connection doesn't establish the connection as readonly (see below)
      (.setReadOnly true)
      (.setDataSource ro-sqlite-ds)
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
      ;; We go with 900000/15m for now. If there's any slow mem leak etc w/ native
      ;; sqlite management or p6spy wrapper etc, we'll recycle connections
      ;; infrequently to reset any of that.
      ;; note that 30 seconds is min allowed lifetime here so we MUST set it
      ;; above that
      (.setMaxLifetime 900000) ;; 900000 = 15m
      ;; note: we will leave .setValidationTimeout as default. it must be
      ;; less than conn timeout. when HikariCP takes a connection from pool
      ;; this is how long it's allowed to validate it before returning it
      ;; from getConnection.
      ;; Trying 25s for now for leakDetectionThreshold.
      (.setLeakDetectionThreshold (* 25 1000)))))

(defn- create-write-enabled-hikari-datasource
  ^HikariDataSource [pool-name wr-sqlite-ds]
  (HikariDataSource.
    (doto (HikariConfig.)
      (.setPoolName pool-name)
      (.setReadOnly false)
      (.setDataSource wr-sqlite-ds)
      ;; no auto-commit! danger zone! we must take care to manage commits
      ;; explicitly.
      (.setAutoCommit false)
      (.setConnectionTimeout (* 15 1000))
      (.setIdleTimeout 0)
      (.setKeepaliveTime (* 5 60 1000))
      (.setMaximumPoolSize 1) ;; really consider if changing this from 1.
      ;; with our singleton write strategy where we take a connection and
      ;; never return it to the pool we would never see this max lifetime
      ;; take effect. however we'll ensure in our write thread management
      ;; that we return connection to the pool **crucially after ensuring
      ;; we commit outstanding work on the singleton thread** so that we
      ;; reset recycle cxns (see note above)
      ;; note that 30 seconds is min allowed lifetime here so we MUST set it
      ;; above that
      (.setMaxLifetime 900000) ;; 900000 = 15m
      ;; disable leak detection entirely - as we may take out connection
      ;; for writes forever.
      (.setLeakDetectionThreshold 0 #_zero!!!))))

(defn- create-readonly-connection-pool
  "Create a connection pool. Our configuration here has all of our connections
   living forever in a fixed-sized pool. While the pool may or may not give us
   enormous benefit over a file-sys based db like sqlite, it's integration with
   metrics gives us a ton of observability with what's happening with the db."
  (^DataSource [pool-name ro-sqlite-ds]
   (create-readonly-connection-pool pool-name ro-sqlite-ds nil))
  (^DataSource [pool-name ro-sqlite-ds ^MetricRegistry metric-registry]
   (let [the-pool (create-readonly-hikari-datasource pool-name ro-sqlite-ds)]
     (when (some? metric-registry)
       (.setMetricsTrackerFactory the-pool
         (CodahaleMetricsTrackerFactory. metric-registry)))
     the-pool)))

(defn- create-writeable-connection-pool
  (^DataSource [pool-name wr-sqlite-ds]
   (create-writeable-connection-pool pool-name wr-sqlite-ds nil))
  (^DataSource [pool-name wr-sqlite-ds ^MetricRegistry metric-registry]
   (let [the-pool (create-write-enabled-hikari-datasource pool-name wr-sqlite-ds)]
     (when (some? metric-registry)
       (.setMetricsTrackerFactory the-pool
         (CodahaleMetricsTrackerFactory. metric-registry)))
     the-pool)))

;(defn ^:deprecated get-simplest-datasource
;  [path]
;  (jdbc/get-datasource (str "jdbc:sqlite:" path)
;    ;; this is no good b/c not all pragmas (such as wal_autocheckpoint)
;    ;;   are supported by sqlite jdbc so some query path pragmas remain unapplied
;    ;;   and worse the filename of the db file gets the remaining query strings.
;    #_(append-query-string (str "jdbc:sqlite:" path)
;      (pragma-statements->query-string pragma-statements))))
;
;(def ^:deprecated get-unpooled-writeable-datasource*
;  (memoize
;    #(get-simplest-datasource %1)))

(def get-readonly-datasource*
  (memoize
    (fn [pool-name ro-sqlite-ds metrics-registry]
      ;; note for future: we do NOT care for "cache=shared"
      ;;   @see https://www.sqlite.org/sharedcache.html#use_of_shared_cache_is_discouraged
      (create-readonly-connection-pool
        pool-name
        (wrap-datasource-with-p6spy ro-sqlite-ds)
        metrics-registry))))

(def get-writeable-datasource*
  (memoize
    (fn [pool-name wr-sqlite-ds metrics-registry]
      (create-writeable-connection-pool
        pool-name
        (wrap-datasource-with-p6spy wr-sqlite-ds)
        metrics-registry))))

(defn- comment-line?
  [line]
  (str/starts-with? line "--"))

(defn- parse-schema-statements [schema-simple-name]
  (let [resource (io/resource (format "me/untethr/nostr/%s" schema-simple-name))]
    (with-open [reader (io/reader resource)]
      (loop [lines (line-seq reader) acc []]
        (if (next lines)
          (let [[ddl more] (split-with (complement comment-line?) lines)]
            (if (not-empty ddl)
              (recur more (conj acc (str/join "\n" ddl)))
              (recur (drop-while comment-line? lines) acc)))
          acc)))))

(defn parse-schema
  [schema-simple-name]
  {:post [(vector? (:pragma-statements %))
          (vector? (:ddl-statements %))]}
  (let [statements (parse-schema-statements schema-simple-name)
        grouped (group-by #(str/starts-with? (str/trim %) "pragma") statements)]
    (map->ParsedSchema
      (set/rename-keys grouped {true :pragma-statements false :ddl-statements}))))

(defn- apply-statements! [db-or-cxn statements]
  {:pre [(some? db-or-cxn)]}
  (jdbc/on-connection [cxn db-or-cxn]
    (doseq [statement statements]
      (try
        (jdbc/execute-one! cxn [statement])
        (catch SQLiteException e
          (when-not
            (and
              (re-matches #"(?is)^\s*alter table.*add column.*;\s*$" statement)
              (str/includes? (ex-message e) "duplicate column name"))
            (throw e)))))))

(defn apply-ddl-statements! [writeable-db {:keys [ddl-statements] :as _parsed-schema}]
  {:pre [(some? writeable-db)]}
  (apply-statements! writeable-db ddl-statements))

(defn apply-pragma-statements! [cxn {:keys [pragma-statements] :as _parsed-schema}]
  {:pre [(some? cxn)]}
  (apply-statements! cxn pragma-statements))

;; --

(defn create-sqlite-datasource
  [path parsed-schema
   & {:keys [read-only?] :or {read-only? false}}]
  (doto
    (proxy [SQLiteDataSource] []
      (getConnection
        (^SQLiteConnection []
         ;; this invokes our proxy's 2-arity getConnection, and is modelled
         ;; after known super-class's behavior:
         (.getConnection this nil nil))
        (^SQLiteConnection [username password]
         (let [^SQLiteConnection rv (proxy-super getConnection username password)]
           (apply-pragma-statements! rv parsed-schema)
           rv))))
    ;; this is what we'd get if added ?open_mode= to jdbc url query-string
    (.setReadOnly read-only?)
    ;; https://www.sqlite.org/sharedcache.html#read_uncommitted_isolation_mode
    ;;   sure, why not.
    (.setReadUncommited true)
    ;; notable we are NOT using query params in the jdbc url to specify pragmas
    ;; or anything else. if we do, we need to reconsider how tests call this
    ;; also make sure that the query params we add don't show up in the filename
    ;; itself -
    (.setUrl (format "jdbc:sqlite:%s" path))))

;; --

(defn ^:deprecated init!
  [path ^MetricRegistry metric-registry]
  (let [parsed-schema (parse-schema "schema-deprecated.sql")
        ro-sqlite-ds (create-sqlite-datasource path parsed-schema :read-only? true)
        wr-sqlite-ds (create-sqlite-datasource path parsed-schema :read-only? false)
        _ (apply-ddl-statements! wr-sqlite-ds parsed-schema)]
    {:writeable-datasource wr-sqlite-ds
     :readonly-datasource
     (get-readonly-datasource*
       "legacy-readonly-pool" ro-sqlite-ds metric-registry)}))

(defn init-new!
  [path ^MetricRegistry metric-registry]
  ;; note: must create writeable first so that in the case of a brand-spanking
  ;; new db we create it (write it!) before trying to open it in readonly mode.
  ;; otherwise, we'll get a failure.
  (let [parsed-schema (parse-schema "schema-new.sql")
        ro-sqlite-ds (create-sqlite-datasource path parsed-schema :read-only? true)
        wr-sqlite-ds (create-sqlite-datasource path parsed-schema :read-only? false)
        _ (apply-ddl-statements! wr-sqlite-ds parsed-schema)]
    {:writeable-datasource
     (get-writeable-datasource*
       "db-writeable" wr-sqlite-ds metric-registry)
     :readonly-datasource
     (get-readonly-datasource*
       ;; note metrics-porcelin known dependency on the pool name provided
       ;; here:
       "db-readonly" ro-sqlite-ds metric-registry)}))

(defn init-new-kv!
  [path ^MetricRegistry metric-registry]
  ;; note: must create writeable first so that in the case of a brand-spanking
  ;; new db we create it (write it!) before trying to open it in readonly mode.
  ;; otherwise, we'll get a failure.
  (let [parsed-schema (parse-schema "schema-kv.sql")
        ro-sqlite-ds (create-sqlite-datasource path parsed-schema :read-only? true)
        wr-sqlite-ds (create-sqlite-datasource path parsed-schema :read-only? false)
        _ (apply-ddl-statements! wr-sqlite-ds parsed-schema)]
    {:writeable-datasource
     (get-writeable-datasource*
       "db-kv-writeable" wr-sqlite-ds metric-registry)
     :readonly-datasource
     (get-readonly-datasource*
       ;; note metrics-porcelin known dependency on the pool name provided
       ;; here:
       "db-kv-readonly" ro-sqlite-ds metric-registry)}))

(defn collect-pragmas!
  [db]
  (reduce
    (fn [acc pragma]
      (into acc
        (jdbc/execute-one! db [(str "pragma " pragma ";")])))
    {}
    ["journal_mode"
     "journal_size_limit"
     "cache_size"
     "page_size"
     "auto_vacuum"
     "wal_autocheckpoint"
     "synchronous"
     "foreign_keys"]))

(comment
  (init-new! "./nn.db" nil))

;; --

(defn checkpoint!
  [singleton-db-conn]
  {:pre [(instance? Connection singleton-db-conn)]}
  ;; @see https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
  ;; pragma wal_checkpoint(FULL) -
  ;;      waits for all db writers to finish (which in our single-writer-thread
  ;;      case should be none. also waits for all readers to be reading from the
  ;;      latest DB snapshot.
  ;;
  ;;      what "wait" means for sqlite is to call the busy handler callback,
  ;;      which is configured via
  ;;        @see https://www.sqlite.org/c3ref/busy_handler.html
  ;;        @see also https://www.sqlite.org/pragma.html#pragma_busy_timeout
  ;;      by default, it seems we don't have any busy handler so in the case
  ;;      of our call here, we get a result that looks like:
  ;;          {:busy 0, :log 2, :checkpointed 2}
  ;;      :busy indicates whether the operation was skipped or partially done
  ;;      because of business. upstream callers tally app metrics on this --
  ;;      if we find that we encounter busy frequently (especially as we might
  ;;      increase reader threads, then we'll want to consider a busy wait handler
  ;;      or doing a busy wait loop ourselves in this operation or in the writer
  ;;      thread that calls this operation. Futher -> if such busy loop strategy
  ;;      is not successful, we may wish to halt readers to let a checkpoint go
  ;;      through at least so many times per write or period.
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

(defn max-event-db-id-p-tags
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(id) as res from p_tags"])))

(defn max-event-db-id-e-tags
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(id) as res from e_tags"])))

(defn max-event-db-id-x-tags
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(id) as res from x_tags"])))

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
  {:pre [(some? id) (some? pubkey) (some? created-at) (some? kind)]
   :post [(or (nil? %) (contains? % :id))]}
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into n_events"
       " (event_id, pubkey, created_at, kind, channel_id)"
       " values (?, ?, ?, ?, ?) returning id")
     id pubkey created-at kind channel-id]
    {:builder-fn rs/as-unqualified-lower-maps}))

(defn insert-event-new-schema!
  "Answers inserted sqlite rowid or nil if row already exists (assuming
   auto-commit; when auto-commit = false, even with read-uncommitted we'll
   always get back the `returning id`)."
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

(defn insert-e-tag-new-schema-prepared-stmt
  (^PreparedStatement [cxn]
   (jdbc/prepare cxn
     [(str
        "insert or ignore into e_tags"
        " (source_event_id, tagged_event_id, source_event_kind, source_event_created_at)"
        " values (?, ?, ?, ?)")]))
  (^PreparedStatement [cxn params-vec]
   ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.3.847/doc/getting-started/prepared-statements#prepared-statement-parameters
   (p/set-parameters (insert-e-tag-new-schema-prepared-stmt cxn) params-vec)))

(defn insert-e-tag-new-schema!
  [writeable-db source-event-id tagged-event-id source-kind source-created-at]
  (with-open [prepared-stmt
              (insert-e-tag-new-schema-prepared-stmt writeable-db
                [source-event-id tagged-event-id source-kind source-created-at])]
    (jdbc/execute-one! prepared-stmt)))

(defn ^:deprecated insert-p-tag!
  [writeable-db source-event-id tagged-pubkey]
  (jdbc/execute-one! writeable-db
    [(str
       "insert or ignore into p_tags"
       " (source_event_id, tagged_pubkey)"
       " values (?, ?)")
     source-event-id tagged-pubkey]))

(defn insert-p-tag-new-schema-prepared-stmt
  (^PreparedStatement [cxn]
   (jdbc/prepare cxn
     [(str
        "insert or ignore into p_tags"
        " (source_event_id, tagged_pubkey, source_event_kind, source_event_created_at, source_event_pubkey)"
        " values (?, ?, ?, ?, ?)")]))
  (^PreparedStatement [cxn params-vec]
   ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.3.847/doc/getting-started/prepared-statements#prepared-statement-parameters
   (p/set-parameters (insert-p-tag-new-schema-prepared-stmt cxn) params-vec)))

(defn insert-p-tag-new-schema!
  [writeable-db source-event-id tagged-pubkey source-kind source-created-at source-pubkey]
  (with-open [prepared-stmt
              (insert-p-tag-new-schema-prepared-stmt writeable-db
                [source-event-id tagged-pubkey source-kind source-created-at source-pubkey])]
    (jdbc/execute-one! prepared-stmt)))

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

(defn insert-x-tag-new-schema-prepared-stmt
  (^PreparedStatement [cxn]
   (jdbc/prepare cxn
     [(str
        "insert or ignore into x_tags"
        " (source_event_id, generic_tag, tagged_value, source_event_created_at)"
        " values (?, ?, ?, ?)")]))
  (^PreparedStatement [cxn params-vec]
   ;; @see https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.3.847/doc/getting-started/prepared-statements#prepared-statement-parameters
   (p/set-parameters (insert-x-tag-new-schema-prepared-stmt cxn) params-vec)))

(defn insert-x-tag-new-schema!
  [writeable-db source-event-id generic-tag tagged-value source-created-at]
  (with-open [prepared-stmt
              (insert-x-tag-new-schema-prepared-stmt writeable-db
                [source-event-id
                 generic-tag
                 ;; Note: we arbitrarily limit generic tags to 2056 characters, and
                 ;; we'll query with the same restriction. That is, any values that
                 ;; exceed 2056 characters will match any query value that exceeds
                 ;; 2056 characters whenever their first 2056 characters match.
                 (if (> (count tagged-value) 2056)
                   (subs tagged-value 0 2056)
                   tagged-value)
                 source-created-at])]
    (jdbc/execute-one! prepared-stmt)))

;; -- facade --

(defn store-event-kv!-
  "Answers event_id or nil if row already exists (note: if auto-commit is
   false even with read-uncommitted, we'll always get `returning event_id`
   even if record already exists)."
  [writeable-conn-kv event-id raw-event]
  (:event_id
    (jdbc/execute-one! writeable-conn-kv
      [(str "insert or ignore into n_kv_events"
         " (event_id, raw_event)"
         "  values (?, ?) returning event_id") event-id raw-event]
      {:builder-fn rs/as-unqualified-lower-maps})))

(def ^:private max-tags-per-store-op 50) ;; if each insert takes 10ms, say, then at 50 here we'll get 500ms total/batch
;; for now just simply quietly crop after 2500 -- we'll store the entire event obj but we won't index
;;  beyond 2500 for now:
(def ^:private max-tags-per-type 2500)

(defn- internal-execute-continuation!
  "Note! whoever calls this must use a transaction or auto-commit or do their own
  eventual .commit"
  [tx-or-cxn
   {:keys [obo-row-id
           p-tags-insert-batch
           e-tags-insert-batch
           x-tags-insert-batch] :as _prev-continuation}]
  (domain/->IndexEventContinuation
    obo-row-id
    (when-not (empty? p-tags-insert-batch)
      (let [[immediates leftovers] (split-at max-tags-per-store-op p-tags-insert-batch)]
        (with-open [prep (insert-p-tag-new-schema-prepared-stmt tx-or-cxn)]
          (jdbc/execute-batch! prep immediates))
        (take max-tags-per-type leftovers)))
    (when-not (empty? e-tags-insert-batch)
      (let [[immediates leftovers] (split-at max-tags-per-store-op e-tags-insert-batch)]
        (with-open [prep (insert-e-tag-new-schema-prepared-stmt tx-or-cxn)]
          (jdbc/execute-batch! prep immediates))
        (take max-tags-per-type leftovers)))
    (when-not (empty? x-tags-insert-batch)
      (let [[immediates leftovers] (split-at max-tags-per-store-op x-tags-insert-batch)]
        (with-open [prep (insert-x-tag-new-schema-prepared-stmt tx-or-cxn)]
          (jdbc/execute-batch! prep immediates))
        (take max-tags-per-type leftovers)))))

(defn internal-store-event-new-schema!-
  [tx-or-cxn channel-id {:keys [id pubkey created_at kind tags] :as _e}]
  (if-let [rowid (insert-event-new-schema! tx-or-cxn id pubkey created_at kind channel-id)]
    ;; !!! NOTE presently our query and pagination strategy depend on the tags for new
    ;; source events to be inserted altogether with each rowid consecutive
    (let [{:keys [p-tag-insert-batch
                  e-tag-insert-batch
                  x-tag-insert-batch]}
          (reduce
            (fn [acc [tag-kind arg0 :as _tag-vec]]
              (let [next-acc
                    (cond
                      ;; we've seen empty tags in the wild (eg {... "tags": [[], ["p", "abc.."]] })
                      ;;  so we'll just handle those gracefully.
                      (or (nil? tag-kind) (nil? arg0)) acc
                      (= tag-kind "e") (update acc :e-tag-insert-batch conj [id arg0 kind created_at])
                      (= tag-kind "p") (update acc :p-tag-insert-batch conj [id arg0 kind created_at pubkey])
                      (common/indexable-tag-str?* tag-kind) (update acc :x-tag-insert-batch conj [id tag-kind arg0 created_at])
                      :else acc)]
                next-acc))
            {:p-tag-insert-batch []
             :e-tag-insert-batch []
             :x-tag-insert-batch []}
            tags)]
      (internal-execute-continuation! tx-or-cxn
        (domain/->IndexEventContinuation
          rowid
          p-tag-insert-batch
          e-tag-insert-batch
          x-tag-insert-batch)))
    :index-duplicate))

(defn store-event-new-schema!-
  ([writeable-conn event-obj]
   (store-event-new-schema!- writeable-conn nil event-obj))
  ([writeable-conn channel-id event-obj & {:keys [transact?] :or {transact? true}}]
   ;; sqlite favors transactions - however we may split transactions that are
   ;; too big (for events with many tags) - if we encounter a situation
   ;; where we partially index an event, then so be it. we have a master repo of
   ;; events that we write to *before* indexing here - so we can always come back
   ;; and repair events that are partially indexed. we'll log errors and such for
   ;; record. consider strategy of efficiently tracking which k/v events are
   ;; not yet fully indexed, say.
   ;;
   ;; no matter what our datasource's auto-commit is, jdbc.next's with-transaction
   ;; will begin/commit.
   (if transact?
     (jdbc/with-transaction [tx writeable-conn]
       (internal-store-event-new-schema!- tx channel-id event-obj))
     (internal-store-event-new-schema!- writeable-conn channel-id event-obj))))

(defn index-and-store-event!
  ([writeable-conn writeable-conn-kv event-obj raw-event]
   (index-and-store-event! writeable-conn writeable-conn-kv nil event-obj raw-event))
  ([writeable-conn writeable-conn-kv channel-id {:keys [id] :as event-obj} raw-event
    & {:keys [transact?] :or {transact? true}}]
   ;; consider future where we index in a queue after durable write -- would this
   ;; be ok -- implication might be that a websocket that wrote, disconnected and
   ;; tried to read might not immediately see their write -- but would we care?
   (if-let [_kv-event-id (store-event-kv!- writeable-conn-kv id raw-event)]
     ;; if we've disabled auto-commit, we may arrive here with a kv-event-id
     ;; even if we've already inserted the event but have not yet committed.
     ;; we're fine with that. we're idempotent in our indexing here. it just
     ;; means we may not in certain race conditions where we receive the self-
     ;; same event twice in super-short succession we won't return :duplicate to
     ;; upstream callers (who may then do something like broadcast both short-
     ;; succession events twice; should be rare unless we're getting duplicate
     ;; events fired at us from some non-client in rapid succession)
     (let [duplicate-or-continuation
           (store-event-new-schema!- writeable-conn channel-id event-obj
             :transact? transact?)]
       (cond
         (domain/continuation? duplicate-or-continuation) duplicate-or-continuation
         :else
         (do ;; assume :index-duplicate
           ;; note we disregard :index-duplicate from store-event-new-schema!- and just call it
           ;; :success ... if for some reason we ever see a duplicate here it means we somehow only partially
           ;; wrote i.e. wrote it but didn't index it. so we want upstream to re-notify
           ;; or otherwise treat it as not a duplicate.
           :full-success)))
     :duplicate)))

(defn continuation!
  [writeable-tx-or-cxn continuation]
  (internal-execute-continuation! writeable-tx-or-cxn continuation))
