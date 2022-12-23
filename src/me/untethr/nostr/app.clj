(ns me.untethr.nostr.app
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [me.untethr.nostr.conf :as conf]
    [me.untethr.nostr.crypt :as crypt]
    [me.untethr.nostr.extra :as extra]
    [me.untethr.nostr.fulfill :as fulfill]
    [me.untethr.nostr.json-facade :as json-facade]
    [me.untethr.nostr.metrics :as metrics]
    [me.untethr.nostr.store :as store]
    [me.untethr.nostr.subscribe :as subscribe]
    [me.untethr.nostr.util :as util]
    [me.untethr.nostr.validation :as validation]
    [me.untethr.nostr.version :as version]
    [me.untethr.nostr.write-thread :as write-thread]
    [next.jdbc :as jdbc]
    [org.httpkit.server :as hk]
    [ring.middleware.params]
    [reitit.ring :as ring])
  (:import (java.nio.charset StandardCharsets)
           (java.util UUID)
           (java.io File)
           (javax.sql DataSource)
           (me.untethr.nostr.conf Conf)))

(def parse json-facade/parse)
(def write-str* json-facade/write-str*)
(def current-system-epoch-seconds util/current-system-epoch-seconds)

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
  "Verify the event. Does it have all required properties with values of
   expected types? Is its event id calculated correctly? Is its signature
   verified? If everything looks good we will return the provided event
   itself. Otherwise, we return a map with an :err key indicating the error."
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
  ;; @see https://github.com/nostr-protocol/nips/blob/master/15.md
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
          event-id
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
  [^Conf conf event-obj]
  (cond
    ;; only validate created_at if it's available as a number; the actual
    ;; event verification step will fail for non-numeric or missing created_at.
    (and
      (:optional-max-created-at-delta conf)
      (number? (:created_at event-obj))
      (> (:created_at event-obj)
        (+ (util/current-system-epoch-seconds)
          (:optional-max-created-at-delta conf))))
    (format "event \"created_at\" too far in the future" (:created_at event-obj))
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
      ;; For now, we re-render the raw event into json; we could be faster by
      ;; stealing the json from the raw message via state machine or regex
      ;; instead of serializing it again here, but we'll also order keys (as an
      ;; unnecessary nicety) so that clients see a predictable payload form.
      ;; This raw-event is what we'll send to subscribers and we'll also write
      ;; it in the db when fulfilling queries of future subscribers.
      (let [raw-event (json-facade/write-str-order-keys* verified-event-or-err-map)]
        (cond
          (ephemeral-event? event-obj)
          (do
            ;; per https://github.com/nostr-protocol/nips/blob/master/20.md: "Ephemeral
            ;; events are not acknowledged with OK responses, unless there is a failure."
            :no-op)
          :else
          ;; note: we are not at this point handling nip-16 replaceable events *in code*;
          ;; see https://github.com/nostr-protocol/nips/blob/master/16.md
          ;; currently a sqlite trigger is handling this for us, so we can go through the
          ;; standard storage handling here. incidentally this means we'll handle
          ;; duplicates the same for non-replaceable events (it should be noted that
          ;; this means we'll send a "duplicate:" nip-20 response whenever someone sends a
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
        ;; Notify subscribers without waiting on the async db write.
        (metrics/time-notify-event! metrics
          (subscribe/notify! metrics subs-atom event-obj raw-event)))
      ;; event was invalid, per nip-20, we'll send make an indication that the
      ;; event did not get persisted (see
      ;; https://github.com/nostr-protocol/nips/blob/master/20.md)
      (handle-invalid-event! metrics ch event-obj verified-event-or-err-map))))

(defn- receive-event
  [^Conf conf metrics db subs-atom channel-id ch [_ e] raw-message]
  ;; Before we attempt to validate an event and its signature (which costs us
  ;; some compute), we'll determine if we're destined to reject the event anyway.
  ;; These are generally rules from configuration - for example, if the event
  ;; content is too long, its timestamp too far in the future, etc, then we can
  ;; reject it without trying to validate it.
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

(defn- prepare-req-filters
  "Prepare subscription \"REQ\" filters before processing them. This includes
   removing filters that could never match anything, removing filters referencing
   kinds none of which we support, removing duplicate filters..."
  [^Conf conf req-filters]
  ;; consider; another possible optimization would be to remove filters that
  ;; are a subset of other filtring.middleware.params/wrap-paramsers in the group.
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
  "This function defines how we handle any requests that come on an open websocket
   channel. We expect these to be valid nip-01 defined requests, but we don't
   assume all requests are valid and handle invalid requests in relevant ways."
  [^Conf conf metrics db subs-atom fulfill-atom channel-id ch [_ req-id & req-filters]]
  (if-not (every? map? req-filters)
    ;; Some filter in the request was not an object, so nothing we can do but
    ;; send back a NOTICE:
    (do
      (log/warn "invalid req" {:msg "expected filter objects"})
      (hk/send! ch
        (create-notice-message "\"REQ\" message had bad/non-object filter")))
    ;; else -- our req has at least the basic expected form.
    (let [use-req-filters (prepare-req-filters conf req-filters)
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
          ;; just in case we're still fulfilling prior subscription with the
          ;; same req-id, we need to cancel that fulfillment and remove any
          ;; subscriptions for the incoming req subscription id.
          (fulfill/cancel! fulfill-atom channel-id internal-req-id)
          (subscribe/unsubscribe! subs-atom channel-id internal-req-id)
          (if (empty? use-req-filters)
            (do
              ;; an empty set of filters at this point isn't going to produce
              ;; any results, so no need to do anything for it except send
              ;; a gratutious eose message to the sender to say they got
              ;; everything.
              (hk/send! ch (create-eose-message req-id)))
            (if (> (subscribe/num-filters subs-atom channel-id) max-filters)
              (do
                ;; The channel already has too many subscriptions, so we send
                ;; a NOTICE back to that effect and do nothing else.
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
                ;; We create the incoming subscription first, so we are guaranteed
                ;; to dispatch new event arrivals from this point forward...
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
                ;; After we create the subscription, we capture the current latest
                ;; rowid in the database. We will fullfill all matching messages up
                ;; to and including this row; in rare cases between the subscription
                ;; just above and the capturing of this rowid we may have recieved
                ;; some few number of messages in which case we may double-deliver
                ;; an event or few from both fulfillment and realtime notifications,
                ;; but, crucially, we will never miss an event:
                (if-let [target-row-id (store/max-event-rowid db)]
                  (letfn [
                          ;; this fullfillment observer function is a callback
                          ;; that will get invoked for every event in the db that
                          ;; matches the subscription.
                          (fulfillment-observer [raw-event]
                            ;; see note above; we may see channel close before we cancel
                            ;; fulfillment
                            (when (hk/open? ch)
                              (hk/send! ch (create-event-message req-id raw-event))))
                          ;; When the fulfillment completes successfully without
                          ;; getting cancelled, this callback will be invoked and
                          ;; we'll send an "eose" message (per nip-15)
                          (fulfillment-eose-callback []
                            (hk/send! ch (create-eose-message req-id)))]
                    (if (fulfill-synchronously? use-req-filters)
                      ;; note: some requests we'd like to *fulfill* asap w/in the
                      ;; channel request w/o giving up the current thread. assumption
                      ;; here is that we're responding to a client before we handle
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
  ;; First thing we do is parse any message on the wire - we expect every message
  ;; to be in json format:
  (let [parsed-message-or-exc (parse-raw-message* raw-message)]
    (if (instance? Exception parsed-message-or-exc)
      ;; If we fail to parse a websocket message, we handle it - we send a NOTICE
      ;; message in response.
      (handle-problem-message! metrics ch raw-message (str "Parse failure on: " raw-message))
      (if (and (vector? parsed-message-or-exc) (not-empty parsed-message-or-exc))
        (condp = (nth parsed-message-or-exc 0)
          ;; These are the three types of message that nostr defines:
          "EVENT" (receive-event conf metrics db subs-atom uuid ch parsed-message-or-exc raw-message)
          "REQ" (receive-req conf metrics db subs-atom fulfill-atom uuid ch parsed-message-or-exc)
          "CLOSE" (receive-close metrics db subs-atom fulfill-atom uuid ch parsed-message-or-exc)
          ;; If we do not recongize the message type, then we also do not process
          ;; and send a NOTICE response.
          (handle-problem-message! metrics ch raw-message (str "Unknown message type: " (nth parsed-message-or-exc 0))))
        ;; If event parsed, but it was not a json array, we do not process it and
        ;; send a NOTICE response.
        (handle-problem-message! metrics ch raw-message (str "Expected a JSON array: " raw-message))))))

(defn- ws-open
  [metrics db subs-atom fulfill-atom {:keys [uuid ip-address] :as websocket-state} ch]
  (log/debug 'ws-open uuid)
  ;; We keep track of created channels and the ip address that created the channel
  ;; in our database. This allows us to do some forensics if we see any bad behavior
  ;; and need to blacklist any ips, for example. Note that all of our db writes
  ;; are done on a singleton write thread - this is optimal write behavior for
  ;; sqlite3 using WAL-mode:
  (write-thread/run-async!
    (fn [] (metrics/time-insert-channel! metrics
             (store/insert-channel! db uuid ip-address)))
    (fn [_] (log/debug "created channel" websocket-state))
    (fn [^Throwable t] (log/error t "while inserting new channel" websocket-state)))
  ;; Without waiting fo the db write to occur, we can update our metrics and
  ;; return immedidately.
  (metrics/websocket-open! metrics))

(defn- ws-close
  [metrics db subs-atom fulfill-atom {:keys [uuid start-ns] :as _websocket-state} ch status]
  (log/debug 'ws-close uuid)
  ;; Update our metrics to close the websocket, recording also the duration of the
  ;; channel's lifespan.
  (metrics/websocket-close! metrics (quot (- (System/nanoTime) start-ns) 1000000))
  ;; We'll want to ensure that all subscriptions and associated state for the
  ;; websocket channel (uuid) are cleaned up and removed.
  (metrics/time-unsubscribe-all! metrics
    (subscribe/unsubscribe-all! subs-atom uuid))
  ;; And just in case we were fulfilling any subscriptions for the channel, we'll
  ;; cancel those as well.
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
             (write-str* [1, 2, 4, 11, 12, 15, 16, 20, 22])))})

(def ^:private nostr-url "https://github.com/nostr-protocol/nostr")
(def ^:private untethr-url "https://github.com/atdixon/me.untethr.nostr-relay")
(def ^:private untethr-new-issue-url "https://github.com/atdixon/me.untethr.nostr-relay/issues/new")

(defn- req->ip-address*
  [req]
  ;; we expect any reverse proxy (eg ngnix) configured to pass header
  ;; X-Real-IP, httpkit will offer headers in lowercase:
  (get-in req [:headers "x-real-ip"] (:remote-addr req)))

(defn- create-websocket-channel*
  [^Conf conf metrics db subs-atom fulfill-atom req]
  ;; Whenever we create a websocket channel, we'll hold on to some simple
  ;; state about the channel - a uuid that we can use to uniquely identify
  ;; the channel, the start time, and the originating ip address:
  (let [websocket-state {:uuid (str (UUID/randomUUID))
                         :start-ns (System/nanoTime)
                         :ip-address (req->ip-address* req)}]
    (try
      ;; To create the websocket channel via httpkit, we just have to say what
      ;; to do on opening and closing the channel, and how to handle any received
      ;; messages on the channel while it's open:
      (hk/as-channel req
        {:on-open (partial ws-open metrics db subs-atom fulfill-atom websocket-state)
         :on-receive (partial ws-receive conf metrics db subs-atom fulfill-atom websocket-state)
         :on-close (partial ws-close metrics db subs-atom fulfill-atom websocket-state)})
      (catch Exception e
        (log/warn e "failed to create websockete channel" websocket-state)
        (throw e)))))

(defn- handler
  "This is the primary relay handler, serving the http root '/' request path."
  [^Conf conf nip11-json metrics db subs-atom fulfill-atom req]
  ;; req contains :remote-addr, :headers {}, ...
  (cond
    ;; If the request is a websocket request, we'll need to create a channel
    ;; to handler further events sent on the channel:
    (:websocket? req)
    (create-websocket-channel* conf metrics db subs-atom fulfill-atom req)
    ;; If we're not a websocket request, then we'll handle the basic http GET.
    ;; See https://github.com/nostr-protocol/nips/blob/master/11.md
    (and (nip11-request? req) nip11-json)
    (nip11-response nip11-json)
    ;; If we're not a websocket or nip11 request and our configuration has
    ;; specified a hostname, we'll return html identifying ourselves with a
    ;; super-basic friendly home page.
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
                 "<p>Need a new feature? Found a bug? Tell us about it <a target=\"_blank\" href=\"%s\">here</a>."
                 "</body>")
               nostr-url untethr-url the-hostname the-hostname untethr-new-issue-url)})))

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
      [ ;; our primary path of access is root "/" this is the path at which websocket
       ;; connection requests arrive:
       ["/" {:get
             (partial handler conf nip11-json metrics db subs-atom fulfill-atom)}]
       ;; See https://github.com/nostr-protocol/nips/blob/master/05.md
       ["/.well-known/nostr.json" {:get (partial handler-nip05 nip05-json)}]
       ;; These are paths primarily for administrative use:
       ;; Metrics are available as json at /metrics:
       ["/metrics" {:get (partial handler-metrics metrics)}]
       ;; This is a simple query-able path to inspect the relay's data without
       ;; having to establish a websocket connection. For example:
       ;;    curl <relay-host>/q?until=now
       ;;    curl <relay-host>/q?author=...
       ["/q" {:get (partial extra/handler-q conf db prepare-req-filters)}]])
    nil
    ;; note wrap-params middleware will also parse request body if request
    ;; Content-type is "x-www-form-urlencoded" as is curl's default when
    ;; --data is provided:
    {:middleware [ring.middleware.params/wrap-params]}))

;; --

;; ?: policy for bad actors to avoid repeated bad parsing/verification/&c
;; ?: policy for firehose-filters; who can have them, max on server, ...
;; ?: rate-limit subscription requests, events, etc.

(defn go!
  "Start the server and sleep forever, blocking the main process thread."
  [^Conf conf nip05-json nip11-json]
  (let [^DataSource db (store/init! (:sqlite-file conf))
        ;; This is our subscription registry, stored in an atom for concurrent
        ;; access. All updates to this atom will be implementd in the subscribe
        ;; namespace.
        subs-atom (atom (subscribe/create-empty-subs))
        ;; This is our fulfillment registry, also in an atom. Whenever we
        ;; receive a nostr subscription (i.e., ["REQ", ...]) on a websocket
        ;; channel, we will register it in our subscription registry, but j
        ;; we also need to (what we call) "fulfill" the subscription by querying
        ;; and returning all historical data that matches the subscription.
        ;; Fulfillment may take time depending on how much historic data matches.
        ;; Fulfillment is handled in the fulfill namespace, and we keep state
        ;; in a registry so to support operations like cancelling fulfillments
        ;; (e.g., if a user sends a new subscription with the same id, we cancel
        ;; any corresponding ongoing fulfillment for that id; or if the websocket
        ;; is closed, we kill any associated fulfillments).
        fulfill-atom (atom (fulfill/create-empty-registry))
        ;; We report various server metrics and expose them via http for
        ;; observability (we use https://metrics.dropwizard.io/ for this):
        metrics (metrics/create-metrics
                  #(store/max-event-rowid db)
                  #(subscribe/num-subscriptions subs-atom)
                  #(subscribe/num-firehose-filters subs-atom))]
    ;; Run our ring-compatible httpkit server on the configured port. :max-ws
    ;; here is the max websocket message size.
    (hk/run-server
      (ring-handler conf nip05-json nip11-json metrics db subs-atom fulfill-atom)
      {:port (:http-port conf) :max-ws 4194304})
    ;; Print human-readable configuration information and that our server has
    ;; started, before sleeping and blocking main thread forever.
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
  ;; This is the main entry point. The process is expected to be executed
  ;; within a working directory that contains a ./conf subdirectory, from
  ;; which we load the `relay.yaml` config file, nip05.json and nip11.json.
  ;;
  ;; Logging is via logback-classic; using a system property on the command-line
  ;; used to start this process, you can specify the logging configuration file
  ;; like so: "-Dlogback.configurationFile=conf/logback.xml".
  (let [conf (parse-config)
        nip05-json (slurp-json* "conf/nip05.json")
        nip11-json (slurp-json* "conf/nip11.json")]
    (go!
      conf
      nip05-json
      nip11-json)))
