(ns me.untethr.nostr.store
  (:require [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc.result-set :as rs]))

(def get-datasource*
  (memoize
    #(jdbc/get-datasource (str "jdbc:sqlite:" %))))

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

(defn apply-schema! [db]
  (doseq [statement (parse-schema)]
    (jdbc/execute-one! db [statement])))

(defn init!
  [path]
  (doto (get-datasource* path)
    apply-schema!))

(comment
  (init! "./n.db"))

;; --

(defn max-event-rowid
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(rowid) as res from n_events"])))

(defn- insert-event!*
  [db id pubkey created-at kind raw]
  {:post [(or (nil? %) (contains? % :rowid))]}
  (jdbc/execute-one! db
    [(str
       "insert or ignore into n_events"
       " (id, pubkey, created_at, kind, raw_event)"
       " values (?, ?, ?, ?, ?) returning rowid")
     id pubkey created-at kind raw]
    {:builder-fn rs/as-unqualified-lower-maps}))

(defn insert-event!
  "Answers inserted sqlite rowid or nil if row already exists."
  [db id pubkey created-at kind raw]
  (:rowid (insert-event!* db id pubkey created-at kind raw)))

(defn insert-e-tag!
  [db source-event-id tagged-event-id]
  (jdbc/execute-one! db
    [(str
       "insert or ignore into e_tags"
       " (source_event_id, tagged_event_id)"
       " values (?, ?)")
     source-event-id tagged-event-id]))

(defn insert-p-tag!
  [db source-event-id tagged-pubkey]
  (jdbc/execute-one! db
    [(str
       "insert or ignore into p_tags"
       " (source_event_id, tagged_pubkey)"
       " values (?, ?)")
     source-event-id tagged-pubkey]))
