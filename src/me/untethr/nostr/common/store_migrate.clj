(ns me.untethr.nostr.common.store-migrate
  (:require
    [clojure.java.io :as io]
    [me.untethr.nostr.common.json-facade :as json-facade]
    [me.untethr.nostr.common.store :as store]
    [me.untethr.nostr.common.validation :as validation]
    [next.jdbc :as jdbc])
  (:import (java.sql Connection)
           (org.apache.commons.compress.compressors CompressorStreamFactory)))

(defmacro with-wal-autocheckpoint-disabled
  ;; note: value seems to go back to default after cxn closes but this macro still
  ;; useful for sanity or if you want to enable/disable w/in same connection.
  ;;
  ;; also note that when jdbc connection closes -- and presumably is the last
  ;; conn closing? -- it blocks in DB while protracted checkpoint-truncate
  ;; happens ... interesting!
  [db-cxn & body]
  `(let [start-val# (:wal_autocheckpoint
                      (jdbc/execute-one! ~db-cxn ["pragma wal_autocheckpoint;"]))]
     (when-not (number? start-val#)
       (throw (ex-info "unexpected current wal_autocheckpoint value"
                {:val start-val#})))
     (try
       (jdbc/execute-one! ~db-cxn
         ["pragma wal_autocheckpoint = -1;"])
       ~@body
       (finally
         (jdbc/execute-one! ~db-cxn
           [(format "pragma wal_autocheckpoint = %d;" start-val#)])
         (let [restored-val# (:wal_autocheckpoint
                               (jdbc/execute-one! ~db-cxn ["pragma wal_autocheckpoint;"]))]
           (when (not= start-val# restored-val#)
             (throw (ex-info "wal_autocheckpoint didn't seem to restore"
                      {:start-val start-val#
                       :restored-val restored-val#}))))))))

(comment
  (let [parsed-schema-old (store/parse-schema "schema-deprecated.sql")
        old-db (store/get-simplest-datasource
                 (str "./dump/n-prod.db?open_mode=1&"
                   (store/pragma-statements->query-string
                     (:pragma-statements parsed-schema-old))))
        parsed-schema (store/parse-schema "schema-new.sql")
        new-db (doto (store/get-simplest-datasource "nn.db"
                       (:pragma-statements parsed-schema))
                 (store/apply-pragma-statements! parsed-schema)
                 (store/apply-ddl-statements! parsed-schema))
        parsed-schema-kv (store/parse-schema "schema-kv.sql")
        new-db-kv (doto (store/get-simplest-datasource "nn-kv.db"
                          (:pragma-statements parsed-schema-kv))
                    (store/apply-pragma-statements! parsed-schema-kv)
                    (store/apply-ddl-statements! parsed-schema-kv))]
    (migrate! old-db new-db new-db-kv)))

(defn- etl
  [old-cxn new-cxn read-sql xf-fn load-sql]
  (with-open [prep-stmt (jdbc/prepare new-cxn [load-sql])]
    (let [old-plan (jdbc/plan old-cxn [read-sql])
          result (transduce
                   (map xf-fn)
                   (completing
                     (fn [{:keys [batch total-reads total-writes] :as acc} params]
                       (let [batch (conj batch params)]
                         (if (< (count batch) 50000)
                           (assoc acc :batch batch)
                           (let [result (jdbc/execute-batch! prep-stmt batch)]
                             (.commit ^Connection new-cxn)
                             {:batch []
                              :total-reads (+ total-reads (count batch))
                              :total-writes (+ total-writes (reduce + result))})))))
                   {:batch [] :total-reads 0 :total-writes 0}
                   old-plan)]
      (let [finale (jdbc/execute-batch! prep-stmt (:batch result))]
        (.commit ^Connection new-cxn)
        (-> result
          (update :total-reads + (count (:batch result)))
          (update :total-writes + (reduce + finale))
          (dissoc :batch))))))

(defn migrate!
  [old-db new-db new-db-kv]
  ;; if we're using hikari we'll have to ignore connection leaks btw
  ;; consider: insert without index and then add indices post facto
  (with-open [old-cxn (jdbc/get-connection old-db)
              new-cxn (jdbc/get-connection new-db)
              new-cxn-kv (jdbc/get-connection new-db-kv)]
    (.setAutoCommit ^Connection new-cxn false) ;; !!
    (.setAutoCommit ^Connection new-cxn-kv false) ;; !!
    (with-wal-autocheckpoint-disabled new-cxn-kv
      ;; -- kv --
      (->>
        (etl
          old-cxn
          new-cxn-kv
          "select id, raw_event from n_events"
          (fn [{:keys [id raw_event]}]
            [id raw_event])
          (str "insert or ignore into n_kv_events"
            " (event_id, raw_event)"
            "  values (?, ?)"))
        (println 'kv_events)))
    (with-wal-autocheckpoint-disabled new-cxn
      ;; -- n_events --
      (->>
        (etl
          old-cxn
          new-cxn
          "select id, pubkey, created_at, kind, deleted_, sys_ts, channel_id from n_events"
          (fn [{:keys [id pubkey created_at kind deleted_ sys_ts channel_id]}]
            [id pubkey kind created_at deleted_ sys_ts channel_id])
          (str "insert or ignore into n_events"
            " (event_id, pubkey, kind, created_at, deleted_, sys_ts, channel_id)"
            "  values (?, ?, ?, ?, ?, ?, ?)"))
        (println 'n_events))
      ;; -- e_tags --
      (->>
        (etl
          old-cxn
          new-cxn
          (str "select e.source_event_id, e.tagged_event_id, v.kind, v.created_at, v.deleted_"
            " from e_tags e join n_events v on e.source_event_id = v.id")
          (fn [{:keys [source_event_id tagged_event_id kind created_at deleted_]}]
            [source_event_id tagged_event_id kind created_at deleted_])
          (str "insert or ignore into e_tags"
            " (source_event_id, tagged_event_id, source_event_kind, source_event_created_at, source_event_deleted_)"
            "  values (?, ?, ?, ?, ?)"))
        (println 'e_tags))
      ;; -- p_tags --
      (->>
        (etl
          old-cxn
          new-cxn
          (str "select p.source_event_id, p.tagged_pubkey, v.pubkey, v.kind, v.created_at, v.deleted_"
            " from p_tags p join n_events v on p.source_event_id = v.id where v.deleted_ = 0")
          (fn [{:keys [source_event_id tagged_pubkey pubkey kind created_at deleted_]}]
            [source_event_id tagged_pubkey pubkey kind created_at deleted_])
          (str "insert or ignore into p_tags"
            " (source_event_id, tagged_pubkey, source_event_pubkey, source_event_kind, source_event_created_at, source_event_deleted_)"
            "  values (?, ?, ?, ?, ?, ?)"))
        (println 'p_tags)))))

;; --

(comment
  (let [file-to-load "./dump/nostr-wellorder-early-1m-v1.jsonl.bz2"
        parsed-schema (store/parse-schema "schema-new.sql")
        new-db (doto (store/get-simplest-datasource "./dump/n-load.db"
                       (:pragma-statements parsed-schema))
                 (store/apply-pragma-statements! parsed-schema)
                 (store/apply-ddl-statements! parsed-schema))
        parsed-schema-kv (store/parse-schema "schema-kv.sql")
        new-db-kv (doto (store/get-simplest-datasource "nn-kv.db"
                          (:pragma-statements parsed-schema-kv))
                    (store/apply-pragma-statements! parsed-schema-kv)
                    (store/apply-ddl-statements! parsed-schema-kv))]
    (load-data! file-to-load new-db new-db-kv)))

(defn load-data!
  [compressed-jsonl-file new-db new-db-kv]
  ;; NOTE !!! we are not verifying the sigs of incoming data here !!!
  ;;   ...but we *are* validating the form...
  (let [factory (CompressorStreamFactory.)] ;; will auto-detect compression type.
    (with-open [buffered-in (io/input-stream compressed-jsonl-file)
                compressor-in (.createCompressorInputStream factory buffered-in)
                compressor-reader (io/reader compressor-in)
                new-cxn (jdbc/get-connection new-db)]
      ;; not technically necessary given our current txn batch strategy here:
      (.setAutoCommit ^Connection new-cxn false)
      (with-wal-autocheckpoint-disabled new-cxn
        (doseq [[batch-idx batch] (map-indexed vector (partition-all 100000 (line-seq compressor-reader)))]
          (println 'batch batch-idx)
          ;; ultimately if this batching strategy is not efficient enough for larger
          ;; datasets we'll want to move toward prepared statements.
          ;; also consider: insert without index and then add indices post facto
          (jdbc/with-transaction [tx new-db]
            (doseq [raw-json-obj batch
                    :let [parsed-obj (json-facade/parse raw-json-obj)
                          _ (when-let [err (validation/event-err parsed-obj)]
                              (case err
                                :err/bad-json-content-in-metadata-event ::tolerate
                                (throw (ex-info "invalid obj" {:err err
                                                               :raw-json-obj raw-json-obj}))))]]
              (try
                (store/index-and-store-event! tx new-db-kv parsed-obj raw-json-obj)
                (catch Exception e
                  (throw (ex-info "failed to store" {:raw-json-obj raw-json-obj} e)))))))))))
