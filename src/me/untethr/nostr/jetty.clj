(ns me.untethr.nostr.jetty
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log])
  (:import (jakarta.servlet Servlet ServletContext)
           (jakarta.servlet.http HttpServlet HttpServletRequest HttpServletResponse)
           (java.net InetSocketAddress)
           (java.time Duration)
           (org.eclipse.jetty.server Handler Request Server ServerConnector)
           (org.eclipse.jetty.server.handler AbstractHandler HandlerList StatisticsHandler)
           (org.eclipse.jetty.servlet ServletContextHandler ServletHolder)
           (org.eclipse.jetty.util.thread QueuedThreadPool)
           (org.eclipse.jetty.websocket.api BatchMode ExtensionConfig RemoteEndpoint WebSocketAdapter WriteCallback)
           (org.eclipse.jetty.websocket.server JettyServerUpgradeRequest JettyServerUpgradeResponse JettyWebSocketCreator JettyWebSocketServerContainer)
           (org.eclipse.jetty.websocket.server.config JettyWebSocketServletContainerInitializer JettyWebSocketServletContainerInitializer$Configurator)))


(defn- create-handler-from-fn
  "Create org.eclipse.jetty.server.Handler from a fn.

   If the fn answers true/truthy the created handler will mark the request
   handled."
  ^Handler [handler-fn]
  {:pre [(fn? handler-fn)]}
  (proxy [AbstractHandler] []
    (handle [^String target ^Request jetty-request
             ^HttpServletRequest request ^HttpServletResponse response]
      (when (handler-fn target jetty-request request response)
        (.setHandled jetty-request true)))))

(defn set-headers!
  [^HttpServletResponse response headers]
  (doseq [[^String k ^String v] headers]
    (.setHeader response k v)))

(defn write-body!
  [^HttpServletResponse response ^String body]
  (doto (.getWriter response)
    (.print body)))

(defn update-response!
  [^HttpServletResponse response
   {:keys [status ^String content-type headers ^String character-encoding ^String body]
    :or {character-encoding "UTF-8"}}]
  {:post [(identical? % response)]}
  ;; cond-> does not short circuit and we use doto in each form so that the
  ;; modified response object threads all the way through -- and we return it
  ;; from our fn.
  (cond-> response
    status (doto (.setStatus (int status)))
    content-type (doto (.setContentType content-type))
    character-encoding (doto (.setCharacterEncoding character-encoding))
    (not-empty headers) (doto (set-headers! headers))
    body (doto (write-body! body))))

(defn upgrade-req->ip-address
  [^JettyServerUpgradeRequest upgrade-req]
  (let [http-servlet-req (.getHttpServletRequest upgrade-req)
        x-real-ip (.getHeader http-servlet-req "X-Real-IP")]
    (if-not (str/blank? x-real-ip)
      x-real-ip
      (when-let [remote-socket-address (some-> upgrade-req .getRemoteSocketAddress)]
        (when (instance? InetSocketAddress remote-socket-address)
          (some-> ^InetSocketAddress remote-socket-address
            .getAddress
            .getHostAddress))))))

(defn create-jetty-websocket-creator
  ^JettyWebSocketCreator [{:keys [on-create
                                  on-connect
                                  on-close
                                  on-error
                                  on-text-message
                                  on-binary-message]
                           :or {on-binary-message (fn [& _])}}]
  (reify JettyWebSocketCreator
    (^Object createWebSocket [_this
                              ^JettyServerUpgradeRequest req
                              ^JettyServerUpgradeResponse resp]
      (let [created-state (on-create req resp)]
        (proxy [WebSocketAdapter] []
          (onWebSocketConnect [^org.eclipse.jetty.websocket.api.Session sess]
            ;; note: essential to call proxy-super here so that the default
            ;; handling that sets session
            (proxy-super onWebSocketConnect sess)
            (on-connect created-state sess))
          (onWebSocketBinary [^bytes payload offset len]
            (on-binary-message created-state (.getSession ^WebSocketAdapter this) payload offset len))
          (onWebSocketClose [status-code ^String reason]
            ;; @see org.eclipse.jetty.websocket.api.StatusCode
            (on-close created-state (.getSession ^WebSocketAdapter this) status-code reason))
          (onWebSocketError [^Throwable t]
            (on-error created-state (.getSession ^WebSocketAdapter this) t))
          (onWebSocketText [^String message]
            (on-text-message created-state (.getSession ^WebSocketAdapter this) message)))))))

(defn websocket-request?
  [^HttpServletRequest req]
  (and
    (some-> req (.getHeader "Connection") (.equalsIgnoreCase "upgrade"))
    (some-> req (.getHeader "Upgrade") (.equalsIgnoreCase "websocket"))))

(defn create-websocket-upgrade-servlet [^JettyWebSocketCreator websocket-creator]
  ;; @see https://www.eclipse.org/jetty/documentation/jetty-11/programming-guide/index.html#pg-server-websocket-jetty-endpoints
  ;; ...specifically ProgrammaticWebSocketUpgradeServlet example.
  (proxy [HttpServlet] []
    (doGet [^HttpServletRequest req ^HttpServletResponse resp]
      ;; e.g. header names seen in wild: [Accept User-Agent Connection Host Upgrade]
      ;; shouldn't need to case-insensitive equals here but we do.
      (if (websocket-request? req)
        (let [^ServletContext servlet-context (.getServletContext this)
              ^JettyWebSocketServerContainer container
              (JettyWebSocketServerContainer/getContainer servlet-context)]
          (when-not (.upgrade container websocket-creator req resp)
            (log/error "failed to upgrade connection to websocket")))
        (do
          ;; for non-websocket requests, we expect an earlier handler to handle "/"
          ;; so this is unexpected...
          (log/warn "unexpected request"
            {:request-url (.getRequestURL req)
             :selected-headers {:connection
                                (.getHeader req "Connection")}})
          (update-response! resp {:status 404}))))))

(defn- disable-default-servlet!
  [^ServletContextHandler servlet-context-handler]
  (let [servlet-handler (.getServletHandler servlet-context-handler)]
    ;; we'll rely on server to configure its own default/404
    ;; handling config. and for no major reason really.
    (.setEnsureDefaultServlet servlet-handler false)
    servlet-context-handler))

(defn create-websocket-configurator
  ^JettyWebSocketServletContainerInitializer$Configurator [configurator-fn]
  (reify JettyWebSocketServletContainerInitializer$Configurator
    ;; void accept(ServletContext servletContext, JettyWebSocketServerContainer container);
    (^void accept [_this ^ServletContext servlet-context ^JettyWebSocketServerContainer container]
      (configurator-fn servlet-context container))))

(defn create-websocket-handler!
  [^Server server
   ^JettyWebSocketServletContainerInitializer$Configurator configurator
   ^Servlet websocket-upgrade-servlet]
  (doto (ServletContextHandler. server "/" false false) ;; no sessions, no security.
    ;; we'll rely on server to configure its own default/404
    ;; handling config. and for no major reason really.
    (disable-default-servlet!)
    (JettyWebSocketServletContainerInitializer/configure configurator)
    ;; using servlet pathspec "" here ensures we're only configuring
    ;; to handle requests at the root "/" path.
    (.addServlet (ServletHolder. websocket-upgrade-servlet) "")))

(defn create-handler-list
  ^HandlerList [& handlers]
  (let [rv (HandlerList.)]
    (doseq [handler handlers]
      (.addHandler rv handler))
    rv))

(defn create-simple-handler
  [req-pred-fn response-supplier-fn]
  (create-handler-from-fn
    (fn [^String _target ^Request _jetty-request
         ^HttpServletRequest req ^HttpServletResponse resp]
      (and (boolean (req-pred-fn req))
        (or (update-response! resp
              (response-supplier-fn req))
          true)))))

(defn uri-req-pred
  [subject]
  (fn [^HttpServletRequest req]
    (= subject (.getRequestURI req))))

(defn header-eq-req-pred
  [header-name header-val]
  {:pre [(some? header-val)]}
  (fn [^HttpServletRequest req]
    (some-> req (.getHeader header-name) (.equalsIgnoreCase header-val))))

(defn header-neq-req-pred
  [header-name header-val]
  {:pre [(some? header-val)]}
  (fn [^HttpServletRequest req]
    (let [actual-val (.getHeader req header-name)]
      (or (nil? actual-val)
        (not (.equalsIgnoreCase actual-val header-val))))))

;; --

(defn send!
  ([^org.eclipse.jetty.websocket.api.Session sess
    ^String message success-callback failure-callback]
   (send! sess message success-callback failure-callback false))
  ([^org.eclipse.jetty.websocket.api.Session sess
    ^String message success-callback failure-callback flush?]
   (let [^RemoteEndpoint remote-endpoint (.getRemote sess)]
     (.sendString remote-endpoint message
       ;; must provide WriteCallback so sendString is async
       ;; and doesn't block here.
       (reify WriteCallback
         (^void writeFailed [_this ^Throwable t]
           (failure-callback t))
         (^void writeSuccess [_this]
           (success-callback))))
     ;; only support explicit flushing if batching is not entirely off:
     (when (and flush?
             (not (identical? BatchMode/OFF
                    (.getBatchMode remote-endpoint))))
       (.flush remote-endpoint)))))

(defn ->query-params
  "Produces a map of {param-name values-vec} ({String vec<String>})."
  [^HttpServletRequest req]
  (into {} (map (fn [[k v]] [k (vec v)]) (.getParameterMap req))))

(defn ->body-str
  [^HttpServletRequest req]
  (let [content-type (.getContentType req)]
    ;; if content type is application/x-www-form-urlencoded then jetty will
    ;; have parsed body as parameters. we don't support this scenario at all
    ;; so better to throw an explicit exception. *for now*, in all other cases
    ;; we assume that the body can be slurped from req input stream -- really
    ;; our known upstream use case is for content-type "application/json" so
    ;; callers (ie curl requests) are best to be explicit with content type
    (if (= content-type "application/x-www-form-urlencoded")
      (throw (ex-info "content type not supported" {:content-type content-type}))
      (some-> req .getInputStream slurp))))

;; --

(defn start-server!
  ^Server [^StatisticsHandler non-ws-handler
           ^StatisticsHandler ws-handler-container
           ^JettyWebSocketCreator websocket-creator
           {:keys [port host max-ws idle-timeout]
            :or {port 9090
                 host "127.0.0.1"
                 max-ws 4194304
                 idle-timeout (Duration/ofHours 2)}}]
  {:pre [;; we will set ws-handler's handler, so ensure upstream doesn't
         (zero? (alength (.getHandlers ws-handler-container)))]}
  (let [thread-pool (doto (QueuedThreadPool.)
                      (.setName "server"))
        server (Server. thread-pool)
        ws-handler (create-websocket-handler!
                     server
                     (create-websocket-configurator
                       (fn [^ServletContext _servlet-context
                            ^JettyWebSocketServerContainer container]
                         (.setMaxTextMessageSize container max-ws)
                         (.setIdleTimeout container idle-timeout)
                         ;; consider more
                         ;; note ideally we'd set general setMaxOutgoingFrames
                         ;; here, but jetty needs some updates to support that.
                         ;; we presently set this for each session when its
                         ;; connected (see upstream comments).
                         ))
                     (create-websocket-upgrade-servlet websocket-creator))
        _ (.setHandler ws-handler-container ws-handler)
        server-handler (create-handler-list non-ws-handler ws-handler-container)]
    ;; note: there's a default ErrorHandler that Server installs, which will
    ;; render 404 error pages for non-"/" paths ("/" paths that 404 will get
    ;; html rendering done by the DefaultHandler that we install below).
    (let [connector (doto (ServerConnector. server)
                      (.setPort port)
                      (.setHost host))]
      (doto server
        (.addConnector connector)
        (.setHandler server-handler)
        (.start)))))
