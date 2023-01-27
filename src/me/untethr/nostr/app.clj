(ns me.untethr.nostr.app
  (:require
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.common :as common]
    [me.untethr.nostr.conf :as conf]
    [me.untethr.nostr.crypt.crypt :as crypt]
    [me.untethr.nostr.extra :as extra]
    [me.untethr.nostr.fulfill :as fulfill]
    [me.untethr.nostr.page.home :as page-home]
    [me.untethr.nostr.page.metrics-porcelain :as metrics-porcelain]
    [me.untethr.nostr.page.nip11 :as page-nip11]
    [me.untethr.nostr.jetty :as jetty]
    [me.untethr.nostr.common.domain :as domain]
    [me.untethr.nostr.common.json-facade :as json-facade]
    [me.untethr.nostr.common.metrics :as metrics]
    [me.untethr.nostr.common.store :as store]
    [me.untethr.nostr.subscribe :as subscribe]
    [me.untethr.nostr.util :as util]
    [me.untethr.nostr.common.validation :as validation]
    [me.untethr.nostr.write-thread :as write-thread]
    [me.untethr.nostr.common.ws-registry :as ws-registry]
    [next.jdbc :as jdbc])
  (:import (com.codahale.metrics MetricRegistry)
           (jakarta.servlet.http HttpServletRequest)
           (java.nio.channels ClosedChannelException WritePendingException)
           (java.nio.charset StandardCharsets)
           (java.security SecureRandom)
           (java.io File)
           (java.util.concurrent.atomic AtomicInteger)
           (javax.sql DataSource)
           (me.untethr.nostr.common.domain DatabaseCxns)
           (me.untethr.nostr.conf Conf)
           (org.apache.commons.lang3 RandomStringUtils)
           (org.eclipse.jetty.io EofException)
           (org.eclipse.jetty.server Server)
           (org.eclipse.jetty.server.handler StatisticsHandler)
           (org.eclipse.jetty.util StaticException)
           (org.eclipse.jetty.websocket.api BatchMode ExtensionConfig Session)
           (org.eclipse.jetty.websocket.common WebSocketSession)
           (org.eclipse.jetty.websocket.server JettyServerUpgradeRequest JettyServerUpgradeResponse JettyWebSocketCreator)))

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
  ^String [req-id raw-event]
  ;; important: req-id may be null
  ;; careful here! we're stitching the json ourselves b/c we have the raw event:
  (format "[\"EVENT\",%s,%s]" (write-str* req-id) raw-event))

(defn- create-eose-message
  ^String [req-id]
  ;; important: req-id may be null
  ;; @see https://github.com/nostr-protocol/nips/blob/master/15.md
  (format "[\"EOSE\",%s]" (write-str* req-id)))

(defn- create-notice-message
  ^String [message]
  (write-str* ["NOTICE" message]))

(defn- create-ok-message
  ^String [event-id bool-val message]
  ;; @see https://github.com/nostr-protocol/nips/blob/master/20.md
  (write-str* ["OK" event-id bool-val message]))

(defn- update-outgoing-messages!
  [websocket-state op-keyword]
  (let [rv (case op-keyword
             :inc (.incrementAndGet ^AtomicInteger (:outgoing-messages websocket-state))
             :dec (.decrementAndGet ^AtomicInteger (:outgoing-messages websocket-state)))]
    (log/debugf "%s outgoing messages: %d" (:uuid websocket-state) rv)
    rv))

(defn- send!*
  ([websocket-state ch-sess data]
   (send!* websocket-state ch-sess data false))
  ([websocket-state ch-sess data flush?]
   (send!* websocket-state ch-sess data flush? nil))
  ([websocket-state ^Session ch-sess ^String data flush? context]
   (if-not (.isOpen ch-sess)
     (log/debug "refusing to send; channel was closed" {:context context})
     (do
       (update-outgoing-messages! websocket-state :inc)
       (try
         (jetty/send! ch-sess data
           (fn [] ;; success
             (when domain/track-bytes-in-out?
               (ws-registry/bytes-out! websocket-state (alength ^bytes (.getBytes data))))
             (update-outgoing-messages! websocket-state :dec))
           (fn [^Throwable t]
             (update-outgoing-messages! websocket-state :dec)
             (cond
               (instance? ClosedChannelException t)
               (log/debug "failed to send; channel was closed" {:context context})
               (and (instance? StaticException t)
                 (.equalsIgnoreCase "Closed" (.getMessage t)))
               ;; see FrameFlusher/CLOSED_CHANNEL
               (log/debug "failed to send; channel was closed" {:context context})
               (instance? WritePendingException t)
               ;; these are otherwise producing too many logs
               (log/debug
                 (str "exceeded max outgoing websocket frames"
                   " dropping messages but not closing channel")
                 (:uuid websocket-state))
               (instance? EofException t)
               (log/debug "failed to send; channel closed abruptly?" {:context context})
               :else
               (log/warn "failed to send"
                 {:exc-type (type t)
                  :exc-message (.getMessage t)
                  :context context})))
           flush?)
         (catch Throwable t
           ;; completely unexpected but here to protect outgoing-messages tally
           ;; we don't expect WriteCallback from jetty/send! to throw either,
           ;; but if they do, should be on separate jetty processing thread, so
           ;; tally should not double-decrement in completely worst cases.
           (log/error t "unexpected exception from jetty/send!")
           (update-outgoing-messages! websocket-state :dec)))))))

(defn- handle-duplicate-event!
  [metrics websocket-state ch-sess event ok-message-str]
  (metrics/duplicate-event! metrics)
  (send!* websocket-state ch-sess
    (create-ok-message
      (:id event)
      true
      (format "duplicate:%s" ok-message-str))
    true ;; flush?
    'handle-duplicate-event!))

(defn- handle-stored-or-replaced-event!
  [metrics websocket-state ch-sess event ok-message-str]
  (metrics/stored-event! metrics)
  (send!* websocket-state ch-sess
    (create-ok-message (:id event) true (format ":%s" ok-message-str))
    true ;; flush?
    'handle-stored-or-replaced-event!))

(defn- ^:deprecated store-event!
  ([singleton-writeable-connection event-obj raw-event]
   (store-event! singleton-writeable-connection nil event-obj raw-event))
  ([singleton-writeable-connection channel-id {:keys [id pubkey created_at kind tags] :as _e} raw-event]
   ;; use a tx, for now; don't want to answer queries with events
   ;; that don't fully exist. could denormalize or some other strat
   ;; to avoid tx if needed
   (jdbc/with-transaction [tx singleton-writeable-connection]
     (if-let [rowid (store/insert-event! tx id pubkey created_at kind raw-event channel-id)]
       (do
         (doseq [[tag-kind arg0] tags]
           (cond
             ;; we've seen empty tags in the wild (eg {... "tags": [[], ["p", "abc.."]] })
             ;;  so we'll just handle those gracefully.
             (or (nil? tag-kind) (nil? arg0)) :no-op
             (= tag-kind "e") (store/insert-e-tag! tx id arg0)
             (= tag-kind "p") (store/insert-p-tag! tx id arg0)
             (common/indexable-tag-str?* tag-kind) (store/insert-x-tag! tx id tag-kind arg0)
             :else :no-op))
         rowid)
       :duplicate))))

(defn- handle-invalid-event!
  [metrics websocket-state ch-sess event err-map]
  (log/debug "dropping invalid/unverified event" {:e event})
  (metrics/invalid-event! metrics)
  (let [event-id (:id event)]
    (if (validation/is-valid-id-form? event-id)
      (send!* websocket-state ch-sess
        (create-ok-message
          event-id
          false ;; failed!
          (format "invalid:%s%s" (:err err-map)
            (if (:context err-map)
              (str " (" (:context err-map) ")") "")))
        true ;; flush?
        'ok-false-invalid-event)
      (send!* websocket-state ch-sess
        (create-notice-message (str "Badly formed event id: " event-id))
        true ;; flush?
        'notice-invalid-event))))

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
  [metrics websocket-state ch-sess event-obj raw-message rejection-message-str]
  (log/log log/*logger-factory* "app.rejected" :info nil
    (str rejection-message-str " " raw-message))
  (metrics/rejected-event! metrics)
  (send!* websocket-state ch-sess
    (create-ok-message
      (:id event-obj)
      false ;; failed
      (format "rejected:%s" rejection-message-str))
    true ;; flush?
    'ok-false-rejected-event))

(defn- replaceable-event?
  [event-obj]
  ;; https://github.com/nostr-protocol/nips/blob/master/16.md
  (some-> event-obj :kind (#(<= 10000 % (dec 20000)))))

(defn- ephemeral-event?
  [event-obj]
  ;; https://github.com/nostr-protocol/nips/blob/master/16.md
  (some-> event-obj :kind (#(<= 20000 % (dec 30000)))))

(defn- receive-accepted-event!
  [^Conf conf metrics db-cxns subs-atom channel-id websocket-state ch-sess event-obj _raw-message]
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
            (metrics/time-notify-event! metrics
              (subscribe/notify! metrics subs-atom event-obj raw-event)))
          :else
          ;; note: we are not at this point handling nip-16 replaceable events *in code*;
          ;; see https://github.com/nostr-protocol/nips/blob/master/16.md
          ;; currently a sqlite trigger is handling this for us, so we can go through the
          ;; standard storage handling here. incidentally this means we'll handle
          ;; duplicates the same for non-replaceable events (it should be noted that
          ;; this means we'll send a "duplicate:" nip-20 response whenever someone sends a
          ;; replaceable event that has already been replaced, and we'll bear that cross)
          (write-thread/submit-new-event!
            metrics
            db-cxns
            channel-id
            verified-event-or-err-map
            raw-event
            (fn []
              (handle-duplicate-event! metrics websocket-state ch-sess event-obj "duplicate"))
            (fn []
              ;; for now this includes initial indexing - if there is any continuation to
              ;; indexing, we'll not get subsequent callbacks atm.
              (handle-stored-or-replaced-event! metrics websocket-state ch-sess event-obj "stored")
              ;; Notify subscribers only after we discover that the event is
              ;; NOT a duplicate.
              (metrics/time-notify-event! metrics
                (subscribe/notify! metrics subs-atom event-obj raw-event))))))
      ;; event was invalid, per nip-20, we'll send make an indication that the
      ;; event did not get persisted (see
      ;; https://github.com/nostr-protocol/nips/blob/master/20.md)
      (handle-invalid-event! metrics websocket-state ch-sess event-obj verified-event-or-err-map))))

(defn- receive-event
  [^Conf conf metrics db-cxns subs-atom channel-id websocket-state ch-sess [_ e] raw-message]
  ;; Before we attempt to validate an event and its signature (which costs us
  ;; some compute), we'll determine if we're destined to reject the event anyway.
  ;; These are generally rules from configuration - for example, if the event
  ;; content is too long, its timestamp too far in the future, etc, then we can
  ;; reject it without trying to validate it.
  (if-let [rejection-reason (reject-event-before-verify? conf e)]
    (handle-rejected-event! metrics websocket-state ch-sess e raw-message rejection-reason)
    (receive-accepted-event! ^Conf conf metrics db-cxns subs-atom channel-id
      websocket-state ch-sess e raw-message)))

(def max-filters 20)

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
    (keep ;; ... we're mapping and filtering here...
      (fn [one-filter]
        ;; ...some delicate logic; we won't touch the filter if it has no
        ;;  kinds array...
        (if (contains? one-filter :kinds)
          ;; ...otherwise we're going to remove all kinds that we refuse to
          ;;  serve...
          (let [modified-filter
                (update one-filter :kinds
                  (fn [kinds-vec]
                    (vec (remove #(not (conf/serves-kind? conf %)) kinds-vec))))]
            ;; ...and, finally, if the remaining kinds is empty then we're
            ;;  going to answer nil so this filter is effectively erased from
            ;;  the filters list...
            (when (not-empty (:kinds modified-filter))
              modified-filter))
          one-filter)))
    ;; if :kind is present, at least one of the provided kinds is supported
    (filter
      (fn [one-filter]
        (or (not (contains? one-filter :kinds))
          (some #(conf/supports-kind? conf %) (:kinds one-filter)))))
    ;; remove straight-up duplicate filters...
    distinct
    vec))

(defn- fulfill-synchronously?
  [req-filters]
  (and (= (count req-filters) 1)
    (some-> req-filters (nth 0) :limit (= 1))))

(defn- ->internal-req-id
  [req-id]
  (or req-id "<null>"))

(def ^:private max-req-id-character-length 2000)

(defn- receive-req
  "This function defines how we handle any requests that come on an open websocket
   channel. We expect these to be valid nip-01 defined requests, but we don't
   assume all requests are valid and handle invalid requests in relevant ways."
  [^Conf conf metrics db-cxns subs-atom fulfill-atom channel-id websocket-state
   ch-sess [_ req-id & req-filters]]
  (cond
    (> (count req-id) max-req-id-character-length)
    (do
      (log/warn "invalid req id length"
        {:req-id (str (subs req-id 0 250) "...")})
      (send!* websocket-state ch-sess
        (create-notice-message "\"REQ\"/subscription id is too long")
        true ;; flush?
        'notice-req-id-too-long))
    (not (every? map? req-filters))
    ;; Some filter in the request was not an object, so nothing we can do but
    ;; send back a NOTICE:
    (do
      (log/warn "invalid req" {:msg "expected filter objects"})
      (send!* websocket-state ch-sess
        (create-notice-message "\"REQ\" message had bad/non-object filter")
        true ;; flush?
        'notice-invalid-req))
    ;; else -- req looks good after basic checks
    :else
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
              (send!* websocket-state ch-sess (create-eose-message req-id)
                true ;; flush?
                'eose-short-circuit))
            (let [next-num-filters (+ (subscribe/num-filters subs-atom channel-id)
                                     (count use-req-filters))]
              (if (> next-num-filters max-filters)
                (do
                  ;; The channel already has too many subscriptions, so we send
                  ;; a NOTICE back to that effect and do nothing else.
                  (metrics/inc-excessive-filters! metrics)
                  (send!* websocket-state ch-sess
                    (create-notice-message
                      (format
                        (str
                          "Too many subscription filters."
                          " Max allowed is %d, but your latest subscription would"
                          " produce %d outstanding.")
                        max-filters
                        next-num-filters))
                    true ;; flush?
                    'notice-excessive-filters))
                (do
                  ;; We create the incoming subscription first, so we are guaranteed
                  ;; to dispatch new event arrivals from this point forward...
                  (metrics/time-subscribe! metrics
                    (subscribe/subscribe! subs-atom channel-id internal-req-id use-req-filters
                      (fn subscription-observer [raw-event]
                        (send!* websocket-state ch-sess
                          ;; note: it's essential we use original possibly nil req-id here,
                          ;; note the internal one (see note above):
                          (create-event-message req-id raw-event)
                          true ;; flush?
                          'notify-subscription))))
                  ;; After we create the subscription, we capture the current latest
                  ;; rowid in the database. We will fullfill all matching messages up
                  ;; to and including this row; in rare cases between the subscription
                  ;; just above and the capturing of this rowid we may have recieved
                  ;; some few number of messages in which case we may double-deliver
                  ;; an event or few from both fulfillment and realtime notifications,
                  ;; but, crucially, we will never miss an event:
                  (let [readonly-db (:readonly-datasource db-cxns)
                        table-max-ids (domain/->TableMaxRowIds
                                        ;; consider: are these disk seeks? or generally cached?
                                        ;;  we could consider only producing the ids needed for the
                                        ;;  given query filter/s we have.
                                        (or (store/max-event-db-id readonly-db) -1)
                                        (or (store/max-event-db-id-p-tags readonly-db) -1)
                                        (or (store/max-event-db-id-e-tags readonly-db) -1)
                                        (or (store/max-event-db-id-x-tags readonly-db) -1))]
                    (letfn [;; this fullfillment observer function is a callback
                            ;; that will get invoked for every event in the db that
                            ;; matches the subscription.
                            (fulfillment-observer [raw-event]
                              (send!* websocket-state ch-sess (create-event-message req-id raw-event)
                                ;; this is the case where don't want to aggressively
                                ;; flush if websocket batching is enabled:
                                false
                                'fulfill-event))
                            ;; When the fulfillment completes successfully without
                            ;; getting cancelled, this callback will be invoked and
                            ;; we'll send an "eose" message (per nip-15)
                            (fulfillment-eose-callback []
                              (send!* websocket-state ch-sess (create-eose-message req-id)
                                true ;; flush?
                                'eose-standard))]
                      (if (fulfill-synchronously? use-req-filters)
                        ;; note: some requests -- like point lookups -- we'd like to *fulfill* asap
                        ;; w/in the channel request w/o giving up the current thread. assumption
                        ;; here is that we're responding to a client before we handle
                        ;; any other REQ or other event from the client -- and we're not going
                        ;; into fulfillment queues.
                        (fulfill/synchronous!!
                          metrics db-cxns channel-id
                          internal-req-id use-req-filters table-max-ids
                          fulfillment-observer fulfillment-eose-callback)
                        (fulfill/submit-use-batching!
                          metrics db-cxns fulfill-atom channel-id
                          internal-req-id use-req-filters table-max-ids
                          fulfillment-observer fulfillment-eose-callback)))))))))))))

(defn- receive-close
  [metrics subs-atom fulfill-atom channel-id _websocket-state _ch-sess [_ req-id]]
  (let [internal-req-id (->internal-req-id req-id)]
    (if-let [err (validation/close-err internal-req-id)]
      (log/warn "invalid close" {:err err :req-id internal-req-id})
      (do
        (metrics/time-unsubscribe! metrics
          (subscribe/unsubscribe! subs-atom channel-id internal-req-id))
        (fulfill/cancel! fulfill-atom channel-id internal-req-id)))))

;; --

(defn- send-auth-challenge!
  "Creates a new auth challenge killing the old one (so be careful about
   outstandings) and sends it to client."
  [^Conf conf metrics websocket-state websocket-sess]
  (let [{:keys [auth-challenge-state-atom]} websocket-state
        new-challenge (RandomStringUtils/random 20 0 0 true true nil (SecureRandom.))
        _ (reset! auth-challenge-state-atom
            (ws-registry/create-auth-challenge-state
              new-challenge (System/nanoTime)))]
    (send!* websocket-state websocket-sess (write-str* ["AUTH" new-challenge]) true)))

(defn- inc-auth-failure-count!
  [{:keys [auth-challenge-state-atom] :as _websocket-state}]
  (swap! auth-challenge-state-atom update :challenge-failure-count (fnil inc 0)))

(defn- handle-auth-failure!
  [metrics websocket-state ch-sess notice-message-str]
  (inc-auth-failure-count! websocket-state)
  (send!* websocket-state ch-sess
    (create-notice-message notice-message-str)
    true ;; flush?
    'handle-auth-failure!))

(defn- get-first-tag-val
  [e tag-name]
  (when (vector? (:tags e))
    (let [candidate (some->
                      (first
                        (filter
                          (fn [[curr-tag-name _curr-tag-val]]
                            (and (string? curr-tag-name)
                              (= curr-tag-name tag-name)))
                          (filter vector? (:tags e))))
                      second)]
      (when (string? candidate)
        candidate))))


(defn- receive-auth
  [^Conf conf metrics uuid websocket-state ch-sess [_ e]]
  (when (:nip42-auth-enabled? conf) ;; otherwise we're simply silent/no-op when client sends AUTH messages
    ;; @see https://github.com/nostr-protocol/nips/blob/master/42.md
    (if (map? e)
      (let [verified-event-or-err-map (as-verified-event e)]
        (if (identical? verified-event-or-err-map e)
          (cond
            (not= (:kind e) 22242)
            ;; auth event not the right kind
            (handle-auth-failure! metrics websocket-state ch-sess
              "auth-error:auth event was not kind 22242")
            (or (not (vector? (:tags e)))
              (empty? (:tags e)))
            ;; no tags
            (handle-auth-failure! metrics websocket-state ch-sess
              "auth-error:no tags in auth event")
            :else
            (let [;; we won't validate relay atm - but we'll tolerate several
                  ;; failures ... in the future we may wish to ignore or count less/
                  ;; not count when the client was intending to auth another relay
                  _incoming-relay-tag-str (get-first-tag-val e "relay")
                  incoming-challenge-tag-str (get-first-tag-val e "challenge")
                  {:keys [most-recent-failure-type] :as _result}
                  (swap! (:auth-challenge-state-atom websocket-state)
                    (fn [{:keys [challenge challenge-satisfied-nanos] :as curr-auth-state}]
                      (cond
                        (= incoming-challenge-tag-str challenge)
                        (if
                          (some? challenge-satisfied-nanos)
                          curr-auth-state ;; don't update if they're authing successfully again
                          (-> curr-auth-state
                            (assoc :challenge-satisfied-pubkey (:pubkey e))
                            (assoc :challenge-satisfied-nanos (System/nanoTime))
                            (assoc :most-recent-failure-type nil)))
                        :else
                        (-> curr-auth-state
                          (assoc :most-recent-failure-type :challenge-mismatch)))))]
              (when most-recent-failure-type
                (case most-recent-failure-type
                  :challenge-mismatch
                  (handle-auth-failure! metrics websocket-state ch-sess
                    "auth-error:challenge didn't match")
                  ;; else - unexpected atm:
                  (handle-auth-failure! metrics websocket-state ch-sess
                    "auth-error:")))))
          ;; bad AUTH event didn't verify
          (handle-auth-failure! metrics websocket-state ch-sess
            (str "auth-error:auth event did not verify " verified-event-or-err-map))))
      ;; bad AUTH arg was not a map
      (handle-auth-failure! metrics websocket-state ch-sess
        "auth-error:auth event did not provide an object"))))

(defn- parse-raw-message*
  [raw-message]
  (try
    (parse raw-message)
    (catch Exception e
      e)))

(defn- handle-problem-message!
  [metrics websocket-state ch-sess raw-message notice-message-str]
  (log/debug "dropping problem message" {:raw-message raw-message})
  (metrics/problem-message! metrics)
  (send!* websocket-state ch-sess
    (create-notice-message notice-message-str)
    true ;; flush?
    'notice-problem-message))

(defn- ws-receive
  [^Conf conf metrics db-cxns subs-atom fulfill-atom
   {:keys [uuid] :as websocket-state} ch-sess raw-message]
  (try
    (when domain/track-bytes-in-out?
      (ws-registry/bytes-in! websocket-state (alength ^bytes (.getBytes raw-message))))
    ;; First thing we do is parse any message on the wire - we expect every message
    ;; to be in json format:
    (let [parsed-message-or-exc (parse-raw-message* raw-message)]
      (if (instance? Exception parsed-message-or-exc)
        ;; If we fail to parse a websocket message, we handle it - we send a NOTICE
        ;; message in response.
        (handle-problem-message! metrics websocket-state ch-sess raw-message
          (str "Parse failure on: " raw-message))
        (if (and (vector? parsed-message-or-exc) (not-empty parsed-message-or-exc))
          (condp = (nth parsed-message-or-exc 0)
            ;; These are the three types of message that nostr defines:
            "EVENT" (receive-event conf metrics db-cxns subs-atom uuid websocket-state
                      ch-sess parsed-message-or-exc raw-message)
            "REQ" (receive-req conf metrics db-cxns subs-atom fulfill-atom uuid
                    websocket-state ch-sess parsed-message-or-exc)
            "CLOSE" (receive-close metrics subs-atom fulfill-atom uuid websocket-state
                      ch-sess parsed-message-or-exc)
            "AUTH" (receive-auth conf metrics uuid websocket-state ch-sess parsed-message-or-exc)
            ;; If we do not recongize the message type, then we also do not process
            ;; and send a NOTICE response.
            (handle-problem-message! metrics websocket-state ch-sess raw-message
              (str "Unknown message type: " (nth parsed-message-or-exc 0))))
          ;; If event parsed, but it was not a json array, we do not process it and
          ;; send a NOTICE response.
          (handle-problem-message! metrics websocket-state ch-sess raw-message
            (str "Expected a JSON array: " raw-message)))))
    (catch Throwable t
      (log/error t "unexpected in ws-receive")
      ;; note: have verified that exceptions from here are caught, logged, and swallowed
      ;; by http-kit and the connection is closed *with* an error sent back to client
      ;; websocket.
      (throw t))))

(defn- ws-open
  [metrics db-cxns websocket-connections-registry _subs-atom _fulfill-atom
   {:keys [uuid ip-address] :as websocket-state}]
  (ws-registry/add! websocket-connections-registry websocket-state)
  ;; We keep track of created channels and the ip address that created the channel
  ;; in our database. This allows us to do some forensics if we see any bad behavior
  ;; and need to blacklist any ips, for example. Note that all of our db writes
  ;; are done on a singleton write thread - this is optimal write behavior for
  ;; sqlite3 using WAL-mode:
  (write-thread/run-async!
    metrics
    db-cxns
    (fn [db-cxn _db-kv-cxn]
      (metrics/time-insert-channel! metrics
        (store/insert-channel! db-cxn uuid ip-address)))
    (fn [_] (log/debug "inserted channel" (:uuid websocket-state)))
    (fn [^Throwable t] (log/error t "while inserting new channel" (:uuid websocket-state))))
  ;; Without waiting fo the db write to occur, we can update our metrics and
  ;; return immedidately.
  (metrics/websocket-open! metrics))

(defn- ws-close
  [metrics websocket-connections-registry subs-atom fulfill-atom
   {:keys [uuid] :as websocket-state} _ch-sess _status]
  (ws-registry/remove! websocket-connections-registry websocket-state)
  ;; Update our metrics to close the websocket, recording also the duration of the
  ;; channel's lifespan.
  (metrics/websocket-close! metrics websocket-state)
  ;; We'll want to ensure that all subscriptions and associated state for the
  ;; websocket channel (uuid) are cleaned up and removed.
  (metrics/time-unsubscribe-all! metrics
    (subscribe/unsubscribe-all! subs-atom uuid))
  ;; And just in case we were fulfilling any subscriptions for the channel, we'll
  ;; cancel those as well.
  (fulfill/cancel-all! fulfill-atom uuid))

;; --

(defn- populate-non-ws-handler!
  ^StatisticsHandler [^StatisticsHandler non-ws-handler
                      ^StatisticsHandler ws-handler
                      ^Conf conf
                      nip05-json
                      nip11-json
                      metrics
                      db-cxns]
  (doto non-ws-handler
    (.setHandler
      (jetty/create-handler-list
        ;; order matters here -- want to consider nip11 handler first,
        ;; before deciding to serve home page and, later, the websocket
        ;; upgrade.
        ;; -- nip11 --
        (jetty/create-simple-handler
          (every-pred (jetty/uri-req-pred "/")
            (jetty/header-eq-req-pred "Accept" "application/nostr+json"))
          (fn [_req] {:status 200
                      :content-type "application/nostr+json"
                      :headers {"Access-Control-Allow-Origin" "*"}
                      :body (page-nip11/json nip11-json)}))
        ;; -- home page --
        (jetty/create-simple-handler
          (every-pred (jetty/uri-req-pred "/")
            (some-fn
              (jetty/header-neq-req-pred "Connection" "upgrade")
              (jetty/header-missing-req-pred "Upgrade")))
          (fn [_req] {:status 200
                      :content-type "text/html"
                      :body (page-home/html conf)}))
        ;; -- nip05 --
        (jetty/create-simple-handler
          (jetty/uri-req-pred "/.well-known/nostr.json")
          (fn [_req] {:status 200
                      :content-type "application/json"
                      :body nip05-json}))
        ;; -- /q --
        (jetty/create-simple-handler
          (jetty/uri-req-pred "/q")
          (fn [^HttpServletRequest req]
            {:status 200
             :content-type "text/plain"
             :body (extra/execute-q conf db-cxns prepare-req-filters
                     (jetty/->query-params req)
                     (jetty/->body-str req))}))
        ;; -- /metrics --
        (jetty/create-simple-handler
          (jetty/uri-req-pred "/metrics")
          (fn [_req] {:status 200
                      :content-type "application/json"
                      ;; we can json-serialize the whole metrics registry here
                      ;; because we assume we have a jackson-metrics-module
                      ;; registered with the jackson object mapper.
                      :body (write-str* (:codahale-registry metrics))}))
        (jetty/create-simple-handler
          (jetty/uri-req-pred "/metrics-porcelain")
          (fn [_req] {:status 200
                      :content-type "text/html"
                      ;; we can json-serialize the whole metrics registry here
                      ;; because we assume we have a jackson-metrics-module
                      ;; registered with the jackson object mapper.
                      :body (metrics-porcelain/html metrics)}))
        ;; consider: moving these or subset to /metrics
        ;; -- /stats-jetty/not-ws --
        (jetty/create-simple-handler
          (jetty/uri-req-pred "/stats-jetty/not-ws")
          (fn [_req] {:status 200
                      :content-type "text/html"
                      :body (.toStatsHTML non-ws-handler)}))
        ;; -- /stats-jetty/ws --
        (jetty/create-simple-handler
          (jetty/uri-req-pred "/stats-jetty/ws")
          (fn [_req] {:status 200
                      :content-type "text/html"
                      :body (.toStatsHTML ws-handler)}))))))

(defn- create-jetty-websocket-creator
  ^JettyWebSocketCreator [^Conf conf metrics db-cxns websocket-connections-registry
                          subs-atom fulfill-atom]
  (jetty/create-jetty-websocket-creator
    {:on-create
     (fn [^JettyServerUpgradeRequest req ^JettyServerUpgradeResponse resp]
       (let [created-state (ws-registry/init-ws-connection-state
                             (jetty/upgrade-req->ip-address req))]
         (when (:websockets-disable-permessage-deflate? conf)
           ;; when deflate is disabled we save cpu at the network's expense
           ;; see https://github.com/eclipse/jetty.project/issues/1341
           (log/debug "disabling permessage-deflate" (:uuid created-state))
           (.setExtensions resp
             (filter #(not= "permessage-deflate"
                        (.getName ^ExtensionConfig %)) (.getExtensions req))))
         (log/debug 'ws-open (:uuid created-state) (:ip-address created-state))
         (ws-open metrics db-cxns websocket-connections-registry subs-atom fulfill-atom created-state)
         created-state))
     :on-connect
     (fn [created-state sess]
       (let [^WebSocketSession as-websocket-session (cast WebSocketSession sess)]
         ;; core session doesn't manage max outgoing frames in a thread-safe way
         ;; however, we assume we can set it here on connect and never again
         ;; so that subsequent threads observe updated value.
         ;; this is a safety measure to prevent a bad or slow client from
         ;; failing to consume events and causing us/jetty to accumuate
         ;; outgoing frames and consume memory.
         ;; per docs if this value is exceeded subsequent sends will fail
         ;; their WriteCallbacks with WritePendingException but the channel
         ;; won't otherwise close.
         ;; https://github.com/eclipse/jetty.project/issues/4824
         ;; practically, we expect this value to be higher than reasonable
         ;; for real-time notify events -- ie, clients should consume these
         ;; much faster than we'll produe them.
         ;; for fullfillment (historical), we'll need to take pains by using
         ;; WriteCallbacks so that we delay generating fulfillment events
         ;; until upstream client keeps up, or otherwise stop fulfilling for
         ;; client.
         (when (:websockets-max-outgoing-frames conf)
           (let [max-outgoing-frames (int (:websockets-max-outgoing-frames conf))]
             (log/debug "setting maxOutgoingFrames" max-outgoing-frames (:uuid created-state))
             (-> as-websocket-session
               .getCoreSession
               (.setMaxOutgoingFrames max-outgoing-frames))))
         (when (:websockets-enable-batch-mode? conf)
           (log/debug "enabling websockets batch mode" (:uuid created-state))
           (-> as-websocket-session
             .getRemote
             (.setBatchMode BatchMode/ON)))
         ;; for now we send auth challenge and let it exists for duration of cxn
         ;; but future we may want to have it have an expiry
         (when (:nip42-auth-enabled? conf)
           (send-auth-challenge! conf metrics created-state as-websocket-session))))
     :on-error
     (fn [created-state _sess ^Throwable t]
       ;; onError here should mean the websocket gets closed -- may see this
       ;; on abrupt client disconnects &c so log/debug and limit noise:
       (log/debug t "websocket on-error" created-state))
     :on-close
     ;; noting a jetty thing here: in case you see ClosedChannelExeptions logged at
     ;; debug in the logs, they are expected/no big deal.
     ;; see https://github.com/eclipse/jetty.project/issues/2699
     (fn [created-state sess status-code reason]
       (log/debug 'ws-close status-code reason (:uuid created-state))
       (ws-close metrics websocket-connections-registry subs-atom fulfill-atom created-state sess status-code))
     :on-text-message
     (fn [created-state sess message]
       (ws-receive conf metrics db-cxns subs-atom fulfill-atom created-state
         sess message))}))

;; --

(defn- init-db-cxns!
  ^DatabaseCxns [^Conf conf metrics]
  (let [datasources (store/init-new! (:sqlite-file conf) ^MetricRegistry (:codahale-registry metrics))
        datasources-kv (store/init-new-kv! (:sqlite-kv-file conf) ^MetricRegistry (:codahale-registry metrics))
        {:keys [^DataSource writeable-datasource ^DataSource readonly-datasource]} datasources
        {^DataSource writeable-datasource-kv :writeable-datasource
         ^DataSource readonly-datasource-kv :readonly-datasource} datasources-kv]
    (domain/init-database-cxns readonly-datasource writeable-datasource
      readonly-datasource-kv writeable-datasource-kv)))

;; --

;; ?: policy for bad actors to avoid repeated bad parsing/verification/&c
;; ?: policy for firehose-filters; who can have them, max on server, ...
;; ?: rate-limit subscription requests, events, etc.

(defn start-server!
  "Start the server and block forever the main process thread."
  ^Server [^Conf conf nip05-json nip11-json]
  (let [websocket-connections-registry (ws-registry/create)
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
        ;; We have a cyclic dependency between our db/datasource and metric
        ;; registry. (Mainly we want the metric registry to be able to query
        ;; the db for max-rowid. But we also want our datasource to be able
        ;; to report metrics.) So we use a holder volatile so the metrics
        ;; registry can use it after it's been instantiated.
        readonly-db-holder (volatile! nil)
        non-ws-handler-container (StatisticsHandler.)
        ws-handler-container (StatisticsHandler.)
        ;; We report various server metrics and expose them via http for
        ;; observability (we use https://metrics.dropwizard.io/ for this):
        metrics (metrics/create-metrics
                  (util/memoize-with-expiration
                    #(if @readonly-db-holder
                       (store/max-event-db-id @readonly-db-holder) -1)
                    ;; just in case some queries /metrics endpoint in fast cycle
                    ;; we don't hit database each time.
                    5000)
                  (util/memoize-with-expiration
                    #(if @readonly-db-holder
                       (store/max-event-db-id-p-tags @readonly-db-holder) -1)
                    ;; just in case some queries /metrics endpoint in fast cycle
                    ;; we don't hit database each time.
                    5100)
                  (util/memoize-with-expiration
                    #(if @readonly-db-holder
                       (store/max-event-db-id-e-tags @readonly-db-holder) -1)
                    ;; just in case some queries /metrics endpoint in fast cycle
                    ;; we don't hit database each time.
                    5200)
                  (util/memoize-with-expiration
                    #(if @readonly-db-holder
                       (store/max-event-db-id-x-tags @readonly-db-holder) -1)
                    ;; just in case some queries /metrics endpoint in fast cycle
                    ;; we don't hit database each time.
                    5300)
                  #(ws-registry/size-estimate websocket-connections-registry)
                  #(subscribe/num-subscriptions subs-atom)
                  #(subscribe/num-filters-prefixes subs-atom)
                  #(subscribe/num-firehose-filters subs-atom)
                  #(fulfill/num-active-fulfillments fulfill-atom)
                  #(write-thread/run-async!-backlog-size))
        db-cxns (init-db-cxns! conf metrics)
        ;; we'll want a perptual single connection for writes which we must
        ;; absolutely ensure we leverage from a single writer thread -- and
        ;; we must keep perpetual singleton connection with our current strategy
        ;; of enqueuing commits. HOWEVER we won't take the connection out here,
        ;; we will still give the write-thread the singleton pool data source.
        ;; this way the write thread can commit close and reopen connections at
        ;; strategic moments in order to ensure we don't accumulate any slow
        ;; off-heap memory that might grow with very long-lived connections
        _ (vreset! readonly-db-holder (:readonly-datasource db-cxns))]
    (write-thread/schedule-sweep-job! metrics db-cxns)
    (write-thread/schedule-connection-recycle! metrics db-cxns)
    (jetty/start-server!
      (populate-non-ws-handler!
        non-ws-handler-container
        ws-handler-container
        conf
        nip05-json
        nip11-json
        metrics
        db-cxns)
      ws-handler-container
      (create-jetty-websocket-creator
        conf
        metrics
        db-cxns
        websocket-connections-registry
        subs-atom
        fulfill-atom)
      {:port (:http-port conf)
       :host (:http-host conf)
       :max-ws 4194304})))

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

(defn- execute!
  ;; Repl note: The arity fn here is good for repl invocation and returns jetty-server
  ;; which can be .stop/.start-ed. Also note that we have a logback-test.xml in
  ;; test/ classpath - which is nice to have picked up when on the repl classpath.
  ([] (execute! nil nil))
  ([sqlite-file-override sqlite-kv-file-override]
   (let [conf (cond-> (parse-config)
                (some? sqlite-file-override) (assoc :sqlite-file sqlite-file-override)
                (some? sqlite-kv-file-override) (assoc :sqlite-kv-file sqlite-kv-file-override))
         nip05-json (slurp-json* "conf/nip05.json")
         nip11-json (slurp-json* "conf/nip11.json")]
     (let [jetty-server (start-server! conf nip05-json nip11-json)]
       ;; Print human-readable configuration information and that our server has
       ;; started, before sleeping and blocking main thread forever.
       (log/info (str "\n" (conf/pretty* conf)))
       (log/infof "server started on port %d" (:http-port conf))
       jetty-server))))

(defn -main [& args]
  ;; This is the main entry point. The process is expected to be executed
  ;; within a working directory that contains a ./conf subdirectory, from
  ;; which we load the `relay.yaml` config file, nip05.json and nip11.json.
  ;;
  ;; Logging is via logback-classic; using a system property on the command-line
  ;; used to start this process, you can specify the logging configuration file
  ;; like so: "-Dlogback.configurationFile=conf/logback.xml".
  (let [jetty-server (execute! nil nil)]
    (log/info "server is started; main thread blocking forever...")
    (.join jetty-server)))
