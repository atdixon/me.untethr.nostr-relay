(ns me.untethr.nostr.extra
  (:require
    [clojure.pprint :as pprint]
    [clojure.set :as set]
    [clojure.walk :as walk]
    [me.untethr.nostr.conf]
    [me.untethr.nostr.json-facade :as json-facade]
    [me.untethr.nostr.query :as query]
    [me.untethr.nostr.util :as util]
    [me.untethr.nostr.validation :as validation]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs])
  (:import (me.untethr.nostr.conf Conf)))

(defn- coerce-num
  [x]
  (cond
    (= "now" x) (util/current-system-epoch-seconds)
    (string? x) (Long/parseLong x)
    :else (long x)))

(defn- query-params->filter
  [query-params]
  (let [prepared (some-> query-params
                   walk/keywordize-keys
                   (select-keys [:since :until :limit :kind :id :author])
                   not-empty
                   (set/rename-keys {:id :ids :author :authors :kind :kinds}))]
    (cond-> prepared
      (contains? prepared :authors) (update :authors vector)
      (contains? prepared :ids) (update :ids vector)
      (contains? prepared :kinds) (update :kinds (comp vector coerce-num))
      (contains? prepared :since) (update :since coerce-num)
      (contains? prepared :until) (update :until coerce-num)
      (contains? prepared :limit) (update :limit coerce-num))))

(defn- validate-filters!
  [filters]
  (when (> (count filters) 5)
    (throw (ex-info "too many filters" {:filters filters})))
  (when-not (every? map? filters)
    (throw (ex-info "bad filters" {:filters filters})))
  (when-let [req-err (validation/req-err "dummy-id" filters)]
    (throw (ex-info "bad request" {:err req-err :filters filters}))))

(defn handler-q
  "A ring hander that supports ad hoc queries over relay data. Primarily for
   admin purposes, i.e., should not be used by any clients.

   This handler supports querying using both URL parameters (especially
   useful from a browser) and the full query filter forms in the HTTP request
   body.

   Examples (as if using curl):

    curl https://<relay-host>/q
    curl https://<relay-host>/q?since=1671816629&until=now
    curl https://<relay-host>/q?author=<pubkey>
    curl https://<relay-host>/q?id=<sha256>

   When using filters in body request, you'll need to specify
   `Content-Type` to something other than \"x-www-form-urlencoded\"
   (otherwise ring middleware may consume the request :body before our handler
    here can):

     curl -H 'Content-Type: application/json' \\
       -XGET <your-relay-host>/q \\
       --data '[{\"authors\":[\"<pubkey>\"]}]'
   "
  [^Conf conf db prepare-req-filters-fn req]
  (let [query-params-as-filter (some-> req :query-params query-params->filter)
        body-as-filters (some->> req :body slurp not-empty json-facade/parse)
        use-filters (or (some-> query-params-as-filter vector) body-as-filters [{}])
        _ (validate-filters! use-filters)
        prepared-filters (prepare-req-filters-fn conf use-filters)
        ;; default limit to 25 if unspecified, but don't let limit exceed 100:
        modified-filters (mapv #(update % :limit (fn [a b] (min (or a b) 100)) 25) prepared-filters)
        as-query (query/filters->query modified-filters)]
    (let [rows (jdbc/execute! db as-query
                 {:builder-fn rs/as-unqualified-lower-maps})
          rows' (mapv
                  (fn [row]
                    (let [parsed-event (-> row :raw_event json-facade/parse)]
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
       :body (format "filters: %s%n%s"
               (json-facade/write-str* use-filters)
               (if (empty? rows')
                 "No results."
                 (with-out-str
                   (pprint/print-table
                     [:rowid :kind :pubkey :content] rows'))))})))
