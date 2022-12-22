(ns me.untethr.nostr.app
  (:require
    [clojure.java.io :as io]
    [clojure.pprint :as pprint]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [me.untethr.nostr.conf :as conf]
    [me.untethr.nostr.crypt :as crypt]
    [me.untethr.nostr.fulfill :as fulfill]
    [me.untethr.nostr.json-facade :as json-facade]
    [me.untethr.nostr.metrics :as metrics]
    [me.untethr.nostr.query :as query]
    [me.untethr.nostr.store :as store]
    [me.untethr.nostr.subscribe :as subscribe]
    [me.untethr.nostr.validation :as validation]
    [me.untethr.nostr.version :as version]
    [me.untethr.nostr.write-thread :as write-thread]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [org.httpkit.server :as hk]
    [reitit.ring :as ring])
  (:import (java.nio.charset StandardCharsets)
           (java.util UUID)
           (java.io File)
           (me.untethr.nostr.conf Conf)))

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
  ;; important: req-id may be null
  ;; careful here! we're stitching the json ourselves b/c we have the raw event:
  (format "[\"EVENT\",%s,%s]" (write-str* req-id) raw-event))

(defn- create-eose-message
  [req-id]
  ;; important: req-id may be null
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

(defn- handle-stored-or-replaced-event!
  [_metrics ch event ok-message-str]
  (hk/send! ch
    (create-ok-message (:id event) true (format ":%s" ok-message-str))))

(defn- store-event!
  ([db event-obj raw-event]
   (store-event! db nil event-obj raw-event))
  ([db channel-id {:keys [id pubkey created_at kind tags] :as _e} raw-event]
   ;; use a tx, for now; don't want to answer queries with events
   ;; that don't fully exist. could denormalize or some other strat
   ;; to avoid tx if needed
   (jdbc/with-transaction [tx db]
     (if-let [rowid (store/insert-event! tx id pubkey created_at kind raw-event channel-id)]
       (do
         (doseq [[tag-kind arg0] tags]
           (cond
             (= tag-kind "e") (store/insert-e-tag! tx id arg0)
             (= tag-kind "p") (store/insert-p-tag! tx id arg0)
             (common/indexable-tag-str?* tag-kind) (store/insert-x-tag! tx id tag-kind arg0)
             :else :no-op))
         rowid)
       :duplicate))))

(defn- handle-invalid-event!
  [metrics ch event err-map]
  (log/debug "dropping invalid/unverified event" {:e event})
  (metrics/invalid-event! metrics)
  (let [event-id (:id event)]
    (if (validation/is-valid-id-form? event-id)
      (hk/send! ch
        (create-ok-message
          (:id event)
          false ;; failed!
          (format "invalid:%s%s" (:err err-map)
            (if (:context err-map)
              (str " (" (:context err-map) ")") ""))))
      (hk/send! ch
        (create-notice-message (str "Badly formed event id: " event-id))))))

(defn- reject-event-before-verify?
  "Before even verifying the event, can we determine that we'll reject it?
   This function returns nil for no rejection, otherwise it returns a short
   human-readable reason for the rejection."
  [conf event-obj]
  (cond
    ;; only validate kind if it's available as a number; the actual
    ;; event verification step will fail for non-numeric or missing kinds.
    (and
      (number? (:kind event-obj))
      (not (conf/supports-kind? conf (:kind event-obj))))
    (format "event \"kind\" '%s' not supported" (:kind event-obj))
    ;; only validate content if it's available as a string; the actual
    ;; event verification step will fail for bad content types.
    (and
      (:optional-max-content-length conf)
      (string? (get event-obj :content))
      (> (alength ^bytes (.getBytes ^String (get event-obj :content)))
        (:optional-max-content-length conf)))
    (format "event \"content\" too long; maximum content length is %d"
      (:optional-max-content-length conf))))

(defn- handle-rejected-event!
  [metrics ch event-obj rejection-message-str]
  (metrics/rejected-event! metrics)
  (hk/send! ch
    (create-ok-message
      (:id event-obj)
      false ;; failed
      (format "rejected:%s" rejection-message-str))))

(defn- replaceable-event?
  [event-obj]
  ;; https://github.com/nostr-protocol/nips/blob/master/16.md
  (some-> event-obj :kind (#(<= 10000 % (dec 20000)))))

(defn- ephemeral-event?
  [event-obj]
  ;; https://github.com/nostr-protocol/nips/blob/master/16.md
  (some-> event-obj :kind (#(<= 20000 % (dec 30000)))))

(defn- receive-accepted-event!
  [^Conf conf metrics db subs-atom channel-id ch event-obj _raw-message]
  (let [verified-event-or-err-map (metrics/time-verify! metrics (as-verified-event event-obj))]
    (if (identical? verified-event-or-err-map event-obj)
      ;; for now re-render raw event into json; could be faster by stealing it
      ;; from raw message via state machine or regex instead of serializing again.
      ;; we'll also order keys (as an unnecessary nicety) so that clients see
      ;; a predictable payload form.
      (let [raw-event (json-facade/write-str-order-keys* verified-event-or-err-map)]
        (cond
          (ephemeral-event? event-obj)
          (do
            ;; per https://github.com/nostr-protocol/nips/blob/master/20.md: "Ephemeral
            ;; events are not acknowledged with OK responses, unless there is a failure."
            :no-op)
          :else
          ;; note: we are not at this point handling nip-16 replaceable events in code;
          ;; currently a sqlite trigger is handling this for us, so we can go through the
          ;; standard storage handling here. incidentally this means we'll handle
          ;; duplicates the same for non-replaceable events (it should be noted that
          ;; this means we'll send a duplicate: response whenever someone sends a
          ;; replaceable event that has already been replaced, and we'll bear that cross)
          (write-thread/run-async!
            (fn []
              (metrics/time-store-event! metrics
                (store-event! db channel-id verified-event-or-err-map raw-event)))
            (fn [store-result]
              (if (identical? store-result :duplicate)
                (handle-duplicate-event! metrics ch event-obj "duplicate")
                (handle-stored-or-replaced-event! metrics ch event-obj "stored")))
            (fn [^Throwable t]
              (log/error t "while storing event" event-obj))))
        (metrics/time-notify-event! metrics
          (subscribe/notify! metrics subs-atom event-obj raw-event)))
      (handle-invalid-event! metrics ch event-obj verified-event-or-err-map))))

(defn- receive-event
  [^Conf conf metrics db subs-atom channel-id ch [_ e] raw-message]
  ;; here we'll only handle the incoming :kind if e has it and either we have
  ;; not configured supported-kinds or the kind is in the supported-kinds list.
  (if-let [rejection-reason (reject-event-before-verify? conf e)]
    (handle-rejected-event! metrics ch e rejection-reason)
    (receive-accepted-event! ^Conf conf metrics db subs-atom channel-id ch e raw-message)))

(def max-filters 15)

;; some clients may still send legacy filter format that permits singular id
;; in filter; so we'll support this for a while.
(defn- ^:deprecated interpret-legacy-filter
  [f]
  (cond-> f
    (and
      (contains? f :id)
      (not (contains? f :ids)))
    (-> (assoc :ids [(:id f)]) (dissoc :id))))

(defn- prepare-req-filters*
  [^Conf conf req-filters]
  ;; consider; another possible optimization would be to remove filters that
  ;; are a subset of other filters in the group.
  (->> req-filters
    ;; conform...
    (map (comp validation/conform-filter-lenient
           interpret-legacy-filter))
    ;; remove null filters (ie filters that could never match anything)...
    (filter (complement validation/filter-has-empty-attr?))
    ;; if :kind is present, at least one of the provided kinds is supported
    (filter #(or (not (contains? % :kinds))
               (some (partial conf/supports-kind? conf) (:kinds %))))
    ;; remove duplicates...
    distinct
    vec))

(defn- fulfill-synchronously?
  [req-filters]
  (and (= (count req-filters) 1)
    (some-> req-filters (nth 0) :limit (= 1))))

(defn- ->internal-req-id
  [req-id]
  (or req-id "<null>"))

(defn- receive-req
  [^Conf conf metrics db subs-atom fulfill-atom channel-id ch [_ req-id & req-filters]]
  (if-not (every? map? req-filters)
    (log/warn "invalid req" {:msg "expected objects"})
    ;; else -- req has basic form...
    (let [use-req-filters (prepare-req-filters* conf req-filters)
          ;; we've seen null subscription ids from clients in the wild, so we'll
          ;; begrudgingly support them by coercing to string here -- note that
          ;; we call it "internal" because any time we send the id back to the
          ;; client we'll want to use the original (possibly nil) id.
          internal-req-id (->internal-req-id req-id)
          req-err (validation/req-err internal-req-id use-req-filters)]
      (if req-err
        (log/warn "invalid req" {:req-err req-err :req [internal-req-id use-req-filters]})
        ;; else --
        (do
          ;; just in case we're still fulfilling prior subscription w/ same req-id
          (fulfill/cancel! fulfill-atom channel-id internal-req-id)
          (subscribe/unsubscribe! subs-atom channel-id internal-req-id)
          ;; from here on, we'll completely ignore empty filters -- that is,
          ;; filters that are empty after we've done our prepare step above.
          (when-not (empty? use-req-filters)
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
                  (subscribe/subscribe! subs-atom channel-id internal-req-id use-req-filters
                    (fn [raw-event]
                      ;; "some" safety if we're notified and our channel has closed,
                      ;; but we've not yet unsubscribed in response; this isn't thread
                      ;; safe so could still see channel close before the send!;
                      ;; upstream observer invocation should catch and log.
                      (when (hk/open? ch)
                        (hk/send! ch
                          ;; note: it's essential we use original possibly nil req-id here,
                          ;; note the internal one (see note above):
                          (create-event-message req-id raw-event))))))
                ;; after subscription, capture fulfillment target rowid; in rare cases we
                ;; may double-deliver an event or few, but we will never miss an event
                (if-let [target-row-id (store/max-event-rowid db)]
                  (letfn [(fulfillment-observer [raw-event]
                            ;; see note above; we may see channel close before we cancel
                            ;; fulfillment
                            (when (hk/open? ch)
                              (hk/send! ch (create-event-message req-id raw-event))))
                          (fulfillment-eose-callback []
                            (hk/send! ch (create-eose-message req-id)))]
                    (if (fulfill-synchronously? use-req-filters)
                      ;; note: some requests we'd like to *fulfill* asap w/in the
                      ;; channel request w/o giving up thre current thread. assumption
                      ;; here is that we're responding to client before we handle
                      ;; any other REQ or other event from the client (need to confirm
                      ;; httpkit handles channels this way so that other websocket
                      ;; channels are served..)
                      (fulfill/synchronous!!
                        metrics db channel-id internal-req-id use-req-filters target-row-id
                        fulfillment-observer fulfillment-eose-callback)
                      (fulfill/submit!
                        metrics db fulfill-atom channel-id internal-req-id use-req-filters target-row-id
                        fulfillment-observer fulfillment-eose-callback)))
                  ;; should only occur on epochal first event
                  (log/warn "no max rowid; nothing yet to fulfill"))))))))))

(defn- receive-close
  [metrics db subs-atom fulfill-atom channel-id ch [_ req-id]]
  (let [internal-req-id (->internal-req-id req-id)]
    (if-let [err (validation/close-err internal-req-id)]
      (log/warn "invalid close" {:err err :req-id internal-req-id})
      (do
        (metrics/time-unsubscribe! metrics
          (subscribe/unsubscribe! subs-atom channel-id internal-req-id))
        (fulfill/cancel! fulfill-atom channel-id internal-req-id)))))

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
  [^Conf conf metrics db subs-atom fulfill-atom {:keys [uuid] :as _websocket-state} ch raw-message]
  ;; note: exceptions are caught, logged, and swallowed by http-kit
  (let [parsed-message-or-exc (parse-raw-message* raw-message)]
    (if (instance? Exception parsed-message-or-exc)
      (handle-problem-message! metrics ch raw-message (str "Parse failure on: " raw-message))
      (if (and (vector? parsed-message-or-exc) (not-empty parsed-message-or-exc))
        (condp = (nth parsed-message-or-exc 0)
          "EVENT" (receive-event conf metrics db subs-atom uuid ch parsed-message-or-exc raw-message)
          "REQ" (receive-req conf metrics db subs-atom fulfill-atom uuid ch parsed-message-or-exc)
          "CLOSE" (receive-close metrics db subs-atom fulfill-atom uuid ch parsed-message-or-exc)
          (handle-problem-message! metrics ch raw-message (str "Unknown message type: " (nth parsed-message-or-exc 0))))
        (handle-problem-message! metrics ch raw-message (str "Expected a JSON array: " raw-message))))))

(defn- ws-open
  [metrics db subs-atom fulfill-atom {:keys [uuid ip-address] :as websocket-state} ch]
  (log/debug 'ws-open uuid)
  (write-thread/run-async!
    (fn [] (metrics/time-insert-channel! metrics
             (store/insert-channel! db uuid ip-address)))
    (fn [_] (log/debug "created channel" websocket-state))
    (fn [^Throwable t] (log/error t "while inserting new channel" websocket-state)))
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
   :headers {"Content-Type" "application/nostr+json"
             "Access-Control-Allow-Origin" "*"}
   :body (-> nip11-json
           (str/replace "${runtime.version}" version/version)
           ;; note: we replace the whole string including the runtime.nips
           ;; variable with a json array -- this is b/c we require the file,
           ;; even with variables, to be json-parseable
           (str/replace "\"${runtime.nips}\""
             (write-str* [1, 2, 4, 11, 12, 15, 16, 20])))})

(def ^:private nostr-url "https://github.com/nostr-protocol/nostr")
(def ^:private untethr-url "https://github.com/atdixon/me.untethr.nostr-relay")

(defn- req->ip-address*
  [req]
  ;; we expect any reverse proxy (eg ngnix) configured to pass header
  ;; X-Real-IP, httpkit will offer headers in lowercase:
  (get-in req [:headers "x-real-ip"] (:remote-addr req)))

(defn- create-websocket-channel*
  [^Conf conf metrics db subs-atom fulfill-atom req]
  (let [websocket-state {:uuid (str (UUID/randomUUID))
                         :start-ns (System/nanoTime)
                         :ip-address (req->ip-address* req)}]
    (try
      (hk/as-channel req
        {:on-open (partial ws-open metrics db subs-atom fulfill-atom websocket-state)
         :on-receive (partial ws-receive conf metrics db subs-atom fulfill-atom websocket-state)
         :on-close (partial ws-close metrics db subs-atom fulfill-atom websocket-state)})
      (catch Exception e
        (log/warn e "failed to create websockete channel" websocket-state)
        (throw e)))))

(defn- handler [^Conf conf nip11-json metrics db subs-atom fulfill-atom req]
  ;; req contains :remote-addr, :headers {}, ...
  (cond
    (:websocket? req)
    (create-websocket-channel* conf metrics db subs-atom fulfill-atom req)
    (and (nip11-request? req) nip11-json)
    (nip11-response nip11-json)
    (not (str/blank? (:optional-hostname conf)))
    (let [the-hostname (:optional-hostname conf)]
      {:status 200
       :headers {"Content-Type" "text/html"}
       :body (format
               (str
                 "<body>"
                 "<p>Hello!"
                 "<p>I am a fast <a target=\"_blank\" href=\"%s\">nostr</a> relay"
                 " of <a target=\"_blank\" href=\"%s\">this flavor</a>."
                 "<p>Add me to your nostr client using: <pre>wss://%s</pre>"
                 "<p>Or, get to know me better: <pre>curl -H 'Accept: application/nostr+json' https://%s</pre>"
                 "</body>")
               nostr-url untethr-url the-hostname the-hostname)})))

(defn- handler-q [^Conf conf db req]
  (let [parsed-body (or (some->> req :body slurp parse) [{}])
        _ (when-not (and
                      (every? map? parsed-body)
                      (nil? (validation/req-err "http:q" parsed-body)))
            (throw (ex-info "bad request" {:req req})))
        prepared-filters (prepare-req-filters* conf parsed-body)
        modified (mapv #(update % :limit (fn [a b] (min (or a b) 50)) 25) prepared-filters)
        as-query (query/filters->query modified)]
    (let [rows (jdbc/execute! db as-query
                 {:builder-fn rs/as-unqualified-lower-maps})
          rows' (mapv
                  (fn [row]
                    (let [parsed-event (-> row :raw_event parse)]
                      (-> row
                        (dissoc :raw_event)
                        (merge
                          (select-keys parsed-event [:kind :pubkey]))
                        (assoc :content
                               (let [max-summary-len 75
                                     the-content (:content parsed-event)
                                     the-content-len (count the-content)
                                     needs-summary? (> the-content-len max-summary-len)
                                     the-summary (if needs-summary?
                                                   (subs the-content 0 max-summary-len) the-content)
                                     suffix (if needs-summary? "..." "")]
                                 (str the-summary suffix)))))) rows)]
      {:status 200
       :headers {"Content-Type" "text/plain"}
       :body (with-out-str
               (pprint/print-table
                 [:rowid :kind :pubkey :content] rows'))})))

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

(defn- ring-handler [^Conf conf nip05-json nip11-json metrics db subs-atom fulfill-atom]
  (ring/ring-handler
    (ring/router
      [["/" {:get
             (partial handler conf nip11-json metrics db subs-atom fulfill-atom)}]
       ["/.well-known/nostr.json" {:get (partial handler-nip05 nip05-json)}]
       ["/metrics" {:get (partial handler-metrics metrics)}]
       ["/q" {:get (partial handler-q conf db)}]])))

;; --

;; ?: policy for bad actors to avoid repeated bad parsing/verification/&c
;; ?: policy for firehose-filters; who can have them, max on server, ...
;; ?: record origin/agent/etc
;; ?: rate-limit subscription requests, events, etc.

(defn go!
  [^Conf conf nip05-json nip11-json]
  (let [db (store/init! (:sqlite-file conf))
        subs-atom (atom (subscribe/create-empty-subs))
        fulfill-atom (atom (fulfill/create-empty-registry))
        metrics (metrics/create-metrics
                  #(store/max-event-rowid db)
                  #(subscribe/num-subscriptions subs-atom)
                  #(subscribe/num-firehose-filters subs-atom))]
    (hk/run-server
      (ring-handler conf nip05-json nip11-json metrics db subs-atom fulfill-atom)
      {:port (:http-port conf) :max-ws 4194304})
    (log/info (str "\n" (conf/pretty* conf)))
    (log/infof "server started on port %d; sleeping forever" (:http-port conf))
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
  ^Conf []
  (with-open [r (io/reader "conf/relay.yaml")]
    (conf/parse-conf r)))

(defn -main [& args]
  (let [conf (parse-config)
        nip05-json (slurp-json* "conf/nip05.json")
        nip11-json (slurp-json* "conf/nip11.json")]
    (go!
      conf
      nip05-json
      nip11-json)))
