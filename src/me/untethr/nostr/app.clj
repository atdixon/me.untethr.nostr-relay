(ns me.untethr.nostr.app
  (:require
    [clj-yaml.core :as yaml]
    [clojure.java.io :as io]
    [clojure.pprint :as pprint]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [me.untethr.nostr.crypt :as crypt]
    [me.untethr.nostr.fulfill :as fulfill]
    [me.untethr.nostr.json-facade :as json-facade]
    [me.untethr.nostr.metrics :as metrics]
    [me.untethr.nostr.store :as store]
    [me.untethr.nostr.subscribe :as subscribe]
    [me.untethr.nostr.validation :as validation]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [org.httpkit.server :as hk]
    [reitit.ring :as ring])
  (:import (java.nio.charset StandardCharsets)
           (java.util UUID)
           (java.io File)))

(def parse json-facade/parse)
(def write-str* json-facade/write-str*)

(defn- calc-event-id
  [{:keys [pubkey created_at kind tags content]}]
  (-> [0 pubkey created_at kind tags content]
    write-str*
    (.getBytes StandardCharsets/UTF_8)
    crypt/sha-256
    crypt/hex-encode))

(defn verify
  [public-key message signature]
  (crypt/verify
    (crypt/hex-decode public-key)
    (crypt/hex-decode message)
    (crypt/hex-decode signature)))

(defn- as-verified-event
  [{:keys [id sig pubkey] :as e}]
  (if-let [err-key (validation/event-err e)]
    {:event e :err "invalid event" :context (name err-key)}
    (let [calculated-id (calc-event-id e)]
      (if-not (= id calculated-id)
        {:event e :err "bad event id"}
        (if-not (verify pubkey id sig)
          {:event e :err "event did not verify"}
          e)))))


(defn- create-event-message
  [req-id raw-event]
  ;; careful here; we're stitching the json ourselves b/c we have the raw event
  (format "[\"EVENT\",%s,%s]" (write-str* req-id) raw-event))

(defn- create-eose-message
  [req-id]
  (format "[\"EOSE\",%s]" (write-str* req-id)))

(defn- create-notice-message
  [message]
  (write-str* ["NOTICE" message]))

(defn- create-ok-message
  [event-id bool-val message]
  ;; @see https://github.com/nostr-protocol/nips/blob/master/20.md
  (write-str* ["OK" event-id bool-val message]))

(defn- handle-duplicate-event!
  [metrics ch event ok-message-str]
  (metrics/duplicate-event! metrics)
  (hk/send! ch
    (create-ok-message
      (:id event)
      true
      (format "duplicate:%s" ok-message-str))))

(defn- handle-duplicate-event!
  [metrics ch event ok-message-str]
  (metrics/duplicate-event! metrics)
  (hk/send! ch
    (create-ok-message
      (:id event)
      true
      (format "duplicate:%s" ok-message-str))))

(defn- handle-stored-event!
  [metrics ch event ok-message-str]
  (metrics/duplicate-event! metrics)
  (hk/send! ch
    (create-ok-message (:id event) true ok-message-str)))

(defn- store-event!
  [db {:keys [id pubkey created_at kind tags] :as _e} raw-event]
  ;; use a tx, for now; don't want to answer queries with events
  ;; that don't fully exist. could denormalize or some other strat
  ;; to avoid tx if needed
  (jdbc/with-transaction [tx db]
    (if-let [rowid (store/insert-event! tx id pubkey created_at kind raw-event)]
      (do
        (doseq [[tag-kind arg0] tags]
          (cond
            (= tag-kind "e") (store/insert-e-tag! tx id arg0)
            (= tag-kind "p") (store/insert-p-tag! tx id arg0)
            (common/indexable-tag-str?* tag-kind) (store/insert-x-tag! tx id tag-kind arg0)
            :else :no-op))
        rowid)
      :duplicate)))

(defn- handle-invalid-event!
  [metrics ch event err-map]
  (log/debug "dropping invalid/unverified event" {:e event})
  (metrics/invalid-event! metrics)
  (let [event-id (:id event)]
    (if (validation/is-valid-id-form? event-id)
      (hk/send! ch
        (create-ok-message
          (:id event)
          false
          (format "invalid:%s%s" (:err err-map)
            (if (:context err-map)
              (str " (" (:context err-map) ")") ""))))
      (hk/send! ch
        (create-notice-message (str "Badly formed event id: " event-id))))))

(defn- receive-event
  [metrics db ch subs-atom [_ e] _raw-message]
  (let [verified-event-or-err-map (metrics/time-verify! metrics (as-verified-event e))]
    (if (identical? verified-event-or-err-map e)
      ;; for now re-render raw event into json; could be faster by stealing it
      ;; from raw message via state machine or regex instead of serializing again.
      (let [raw-event (write-str* verified-event-or-err-map)
            store-result (metrics/time-store-event! metrics
                           (store-event! db verified-event-or-err-map raw-event))]
        (if (identical? store-result :duplicate)
          (handle-duplicate-event! metrics ch e "ok")
          (handle-stored-event! metrics ch e "ok"))
        (metrics/time-notify-event! metrics
          (subscribe/notify! metrics subs-atom e raw-event)))
      (handle-invalid-event! metrics ch e verified-event-or-err-map))))

(def max-filters 10)

;; some clients may still send legacy filter format that permits singular id
;; in filter; so we'll support this for a while.
(defn- ^:deprecated interpret-legacy-filter
  [f]
  (cond-> f
    (and
      (contains? f :id)
      (not (contains? f :ids)))
    (-> (assoc :ids [(:id f)]) (dissoc :id))))

(defn- receive-req
  [metrics db subs-atom fulfill-atom channel-id ch [_ req-id & req-filters]]
  (let [use-req-filters (mapv (comp validation/conform-filter-lenient
                                interpret-legacy-filter) req-filters)]
    (if-let [err (validation/req-err req-id use-req-filters)]
      (log/warn "invalid req" {:err err :req [req-id (vec use-req-filters)]})
      (do
        ;; just in case we're still fulfilling prior subscription w/ same req-id
        (fulfill/cancel! fulfill-atom channel-id req-id)
        (subscribe/unsubscribe! subs-atom channel-id req-id)
        (when-not (validation/filters-empty? use-req-filters)
          (if (> (subscribe/num-filters subs-atom channel-id) max-filters)
            (do
              (metrics/inc-excessive-filters! metrics)
              (hk/send! ch
                (create-notice-message
                  (format
                    (str
                      "Too many subscription filters."
                      " Max allowed is %d, but you have %d.")
                    max-filters
                    (subscribe/num-filters subs-atom channel-id)))))
            (do
              ;; subscribe first so we are guaranteed to dispatch new arrivals
              (metrics/time-subscribe! metrics
                (subscribe/subscribe! subs-atom channel-id req-id use-req-filters
                  (fn [raw-event]
                    ;; "some" safety if we're notified and our channel has closed,
                    ;; but we've not yet unsubscribed in response; this isn't thread
                    ;; safe so could still see channel close before the send!;
                    ;; upstream observer invocation should catch and log.
                    (when (hk/open? ch)
                      (hk/send! ch (create-event-message req-id raw-event))))))
              ;; after subscription, capture fulfillment target rowid; in rare cases we
              ;; may double-deliver an event or few but we will never miss an event
              (if-let [target-row-id (store/max-event-rowid db)]
                (fulfill/submit! metrics db fulfill-atom channel-id req-id use-req-filters target-row-id
                  (fn [raw-event]
                    ;; see note above; we may see channel close before we cancel
                    ;; fulfillment
                    (when (hk/open? ch)
                      (hk/send! ch (create-event-message req-id raw-event))))
                  (fn []
                    (hk/send! ch (create-eose-message req-id))))
                ;; should only occur on epochal first event
                (log/warn "no max rowid; nothing yet to fulfill")))))))))

(defn- receive-close
  [metrics db subs-atom fulfill-atom channel-id ch [_ req-id]]
  (if-let [err (validation/close-err req-id)]
    (log/warn "invalid close" {:err err :req-id req-id})
    (do (metrics/time-unsubscribe! metrics
          (subscribe/unsubscribe! subs-atom channel-id req-id))
        (fulfill/cancel! fulfill-atom channel-id req-id))))

(defn- parse-raw-message*
  [raw-message]
  (try
    (parse raw-message)
    (catch Exception e
      e)))

(defn- handle-problem-message!
  [metrics ch raw-message notice-message-str]
  (log/debug "dropping problem message" {:raw-message raw-message})
  (metrics/problem-message! metrics)
  (hk/send! ch
    (create-notice-message notice-message-str)))

(defn- ws-receive
  [metrics db subs-atom fulfill-atom {:keys [uuid] :as _websocket-state} ch raw-message]
  ;; note: exceptions are caught, logged, and swallowed by http-kit
  (let [parsed-message-or-exc (parse-raw-message* raw-message)]
    (if (instance? Exception parsed-message-or-exc)
      (handle-problem-message! metrics ch raw-message (str "Parse failure on: " raw-message))
      (if (and (vector? parsed-message-or-exc) (not-empty parsed-message-or-exc))
        (condp = (nth parsed-message-or-exc 0)
          "EVENT" (receive-event metrics db ch subs-atom parsed-message-or-exc raw-message)
          "REQ" (receive-req metrics db subs-atom fulfill-atom uuid ch parsed-message-or-exc)
          "CLOSE" (receive-close metrics db subs-atom fulfill-atom uuid ch parsed-message-or-exc)
          (handle-problem-message! metrics ch raw-message (str "Unknown message type: " (nth parsed-message-or-exc 0))))
        (handle-problem-message! metrics ch raw-message (str "Expected a JSON array: " raw-message))))))

(defn- ws-open
  [metrics db subs-atom fulfill-atom {:keys [uuid] :as _websocket-state} ch]
  (log/debug 'ws-open uuid)
  (metrics/websocket-open! metrics))

(defn- ws-close
  [metrics db subs-atom fulfill-atom {:keys [uuid start-ns] :as _websocket-state} ch status]
  (log/debug 'ws-close uuid)
  (metrics/websocket-close! metrics (quot (- (System/nanoTime) start-ns) 1000000))
  (metrics/time-unsubscribe-all! metrics
    (subscribe/unsubscribe-all! subs-atom uuid))
  (fulfill/cancel-all! fulfill-atom uuid))

(defn- nip11-request?
  [req]
  ;; assume that httpkit always gives us headers lower-cased
  (= (some-> req :headers (get "accept") str/trim) "application/nostr+json"))

(defn- nip11-response
  [nip11-json]
  {:status 200
   :headers {"Content-Type" "application/nostr+json"}
   :body nip11-json})

(defn- handler [nip11-json metrics db subs-atom fulfill-atom req]
  ;; req contains :remote-addr, :headers {}, ...
  (cond
    (:websocket? req)
    (let [websocket-state {:uuid (str (UUID/randomUUID)) :start-ns (System/nanoTime)}]
      (hk/as-channel req
        {:on-open (partial ws-open metrics db subs-atom fulfill-atom websocket-state)
         :on-receive (partial ws-receive metrics db subs-atom fulfill-atom websocket-state)
         :on-close (partial ws-close metrics db subs-atom fulfill-atom websocket-state)}))
    (and (nip11-request? req) nip11-json)
    (nip11-response nip11-json)))

(defn- handler-q [db req]
  (let [rows (jdbc/execute! db
               ["select rowid, sys_ts, raw_event from n_events order by rowid desc limit 25"]
               {:builder-fn rs/as-unqualified-lower-maps})
        rows' (mapv
                (fn [row]
                  (let [parsed-event (-> row :raw_event parse)]
                    (-> row
                      (dissoc :raw_event)
                      (merge
                        (select-keys parsed-event [:kind :pubkey]))
                      (assoc :content
                             (str
                               (subs (:content parsed-event) 0
                                 (max 0 (min 75 (dec (count (:content parsed-event)))))) "..."))))) rows)]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (with-out-str
             (pprint/print-table
               [:rowid :sys_ts :kind :pubkey :content] rows'))}))

(defn- handler-metrics [metrics _req]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (write-str* (:codahale-registry metrics))})

(defn- handler-nip05 [nip05-json _req]
  (when nip05-json
    {:status 200
     :headers {"Content-Type" "application/json"
               "Access-Control-Allow-Origin" "*"}
     :body nip05-json}))

;; --

(defn- ring-handler [nip05-json nip11-json metrics db subs-atom fulfill-atom]
  (ring/ring-handler
    (ring/router
      [["/" {:get
             (partial handler nip11-json metrics db subs-atom fulfill-atom)}]
       ["/.well-known/nostr.json" {:get (partial handler-nip05 nip05-json)}]
       ["/metrics" {:get (partial handler-metrics metrics)}]
       ["/q" {:get (partial handler-q db)}]])))

;; --

;; ?: policy for bad actors to avoid repeated bad parsing/verification/&c
;; ?: policy for firehose-filters; who can have them, max on server, ...
;; ?: record origin/agent/etc
;; ?: ngnix + jvm config
;; ?: rate-limit subscription requests, events, etc.
;; ?: simple auth for metrics?

(defn go!
  [db-path server-port nip05-json nip11-json]
  (let [db (store/init! db-path)
        subs-atom (atom (subscribe/create-empty-subs))
        fulfill-atom (atom (fulfill/create-empty-registry))
        metrics (metrics/create-metrics
                  #(store/max-event-rowid db)
                  #(subscribe/num-subscriptions subs-atom)
                  #(subscribe/num-firehose-filters subs-atom))]
    (hk/run-server
      (ring-handler nip05-json nip11-json metrics db subs-atom fulfill-atom)
      {:port server-port :max-ws 4194304})
    (log/infof "server started on port %d; sleeping forever" server-port)
    (Thread/sleep Integer/MAX_VALUE)))

(defn- slurp-json*
  [f]
  (let [^File f (io/as-file f)]
    (when (.exists f)
      (doto (slurp f)
        ;; we keep json exactly as-is but send it through
        ;; read-write here simply as validation
        (-> parse write-str*)))))

(defn- parse-config
  []
  (with-open [r (io/reader "conf/relay.yaml")]
    (yaml/parse-stream r)))

(defn -main [& args]
  (let [conf (parse-config)
        nip05-json (slurp-json* "conf/nip05.json")
        nip11-json (slurp-json* "conf/nip11.json")]
    (go!
      (get-in conf [:sqlite :file])
      (get-in conf [:http :port])
      nip05-json
      nip11-json)))
