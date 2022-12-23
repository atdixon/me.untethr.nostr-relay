(ns me.untethr.nostr.store
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (javax.sql DataSource)
           (org.sqlite SQLiteException)))

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
    (try
      (jdbc/execute-one! db [statement])
      (catch SQLiteException e
        (when-not
          (and
            (re-matches #"(?is)^\s*alter table.*add column.*;\s*$" statement)
            (str/includes? (ex-message e) "duplicate column name"))
          (throw e))))))

(defn init!
  ^DataSource [path]
  (doto (get-datasource* path)
    apply-schema!))

(comment
  (init! "./n.db"))

;; --

(defn max-event-rowid
  [db]
  ;; we expect this simple select max(rowid) here to be fast over arbitrary volume
  (:res (jdbc/execute-one! db ["select max(rowid) as res from n_events"])))

(defn insert-channel!
  [db channel-id ip-address]
  (jdbc/execute-one! db
    ["insert or ignore into channels (channel_id, ip_addr) values (?,?)"
     channel-id ip-address]))

(defn- insert-event!*
  [db id pubkey created-at kind raw channel-id]
  {:post [(or (nil? %) (contains? % :rowid))]}
  (jdbc/execute-one! db
    [(str
       "insert or ignore into n_events"
       " (id, pubkey, created_at, kind, raw_event, channel_id)"
       " values (?, ?, ?, ?, ?, ?) returning rowid")
     id pubkey created-at kind raw channel-id]
    {:builder-fn rs/as-unqualified-lower-maps}))

(defn insert-event!
  "Answers inserted sqlite rowid or nil if row already exists."
  ([db id pubkey created-at kind raw]
   (insert-event! db id pubkey created-at kind raw nil))
  ([db id pubkey created-at kind raw channel-id]
   (:rowid (insert-event!* db id pubkey created-at kind raw channel-id))))

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

(defn insert-x-tag!
  [db source-event-id generic-tag tagged-value]
  (jdbc/execute-one! db
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
