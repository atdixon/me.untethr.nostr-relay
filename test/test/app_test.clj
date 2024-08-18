(ns test.app-test
  (:require
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [me.untethr.nostr.app :as app]
    [me.untethr.nostr.common.domain :as domain]
    [me.untethr.nostr.conf :as conf]
    [me.untethr.nostr.common.metrics :as metrics]
    [me.untethr.nostr.query.engine :as engine]
    [me.untethr.nostr.subscribe :as subscribe]
    [me.untethr.nostr.write-thread :as write-thread]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [test.support :as support]))

(defn make-test-conf
  ([]
   (make-test-conf nil nil))
  ([supported-kinds-vec]
   (make-test-conf supported-kinds-vec nil))
  ([supported-kinds-vec unserved-kinds-vec]
   (make-test-conf supported-kinds-vec unserved-kinds-vec nil))
  ([supported-kinds-vec unserved-kinds-vec max-content-length]
   (make-test-conf supported-kinds-vec unserved-kinds-vec max-content-length nil))
  ([supported-kinds-vec unserved-kinds-vec max-content-length max-created-at-delta]
   (conf/->Conf nil "127.0.0.1" 1234 "nx.db" "nx-kb.db"
     (some->> supported-kinds-vec (hash-map :supported-kinds) conf/parse-supported-kinds*)
     (some->> unserved-kinds-vec (hash-map :unserved-kinds) conf/parse-unserved-kinds*)
     max-content-length max-created-at-delta nil nil nil nil)))

(deftest fulfill-synchronously?-test
  (is (not (#'app/fulfill-synchronously? [])))
  (is (not (#'app/fulfill-synchronously? [{}])))
  (is (not (#'app/fulfill-synchronously? [{:authors []}])))
  (is (not (#'app/fulfill-synchronously? [{:authors [support/fake-hex-64-str]
                                           :kinds [1 2 3]}])))
  (is (not (#'app/fulfill-synchronously? [{:limit 2}])))
  (is (not (#'app/fulfill-synchronously? [{:authors [support/fake-hex-64-str]
                                           :kinds [1 2 3]
                                           :limit 2}])))
  (is (#'app/fulfill-synchronously? [{:limit 1}]))
  (is (#'app/fulfill-synchronously? [{:authors [] :limit 1}]))
  (is (#'app/fulfill-synchronously? [{:authors [support/fake-hex-64-str]
                                      :kinds [1 2 3]
                                      :limit 1}])))

(deftest prepare-req-filters*-test
  (let [conf (make-test-conf)]
    (is (= [] (#'app/prepare-req-filters conf [])))
    (is (= [] (#'app/prepare-req-filters conf [{:authors []}])))
    (is (= [{}] (#'app/prepare-req-filters conf [{}])))
    (is (= [{}] (#'app/prepare-req-filters conf [{} {}])))
    (is (= [{}] (#'app/prepare-req-filters conf [{} {} {:authors []}])))
    (is (= [{} {:authors [support/fake-hex-64-str]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}])))
    (is (= [{} {:authors [support/fake-hex-64-str]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                            {:authors [support/fake-hex-64-str]}])))
    (is (= [{} {:authors [support/fake-hex-64-str]} {:authors [support/fake-hex-64-str] :kinds [1 2 3]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                            {:authors [support/fake-hex-64-str]
                                             :kinds [1 2 3]}]))))
  (let [conf (make-test-conf ["1-2"])]
    (is (= [] (#'app/prepare-req-filters conf [])))
    (is (= [] (#'app/prepare-req-filters conf [{:authors []}])))
    (is (= [{}] (#'app/prepare-req-filters conf [{}])))
    (is (= [{}] (#'app/prepare-req-filters conf [{} {}])))
    (is (= [{}] (#'app/prepare-req-filters conf [{} {} {:authors []}])))
    (is (= [{} {:authors [support/fake-hex-64-str]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}])))
    (is (= [{} {:authors [support/fake-hex-64-str]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                            {:authors [support/fake-hex-64-str]}])))
    (is (= [{} {:authors [support/fake-hex-64-str]} {:authors [support/fake-hex-64-str] :kinds [1 2 3]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                            {:authors [support/fake-hex-64-str]
                                             :kinds [1 2 3]}])))
    ;; note: here if none of the kinds a filter references supports, the filter is removed:
    (is (= [{} {:authors [support/fake-hex-64-str]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                            {:authors [support/fake-hex-64-str]
                                             :kinds [3 4]}]))))
  ;; let's get unserved-kinds involved...
  (let [conf (make-test-conf ["1-2"] ["3"])]
    (is (= [{} {:authors [support/fake-hex-64-str]} {:authors [support/fake-hex-64-str] :kinds [1 2]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                           {:authors [support/fake-hex-64-str]
                                            :kinds [1 2 3]}])))
    ;; regression check here,
    (is (every? vector?
          (keep :kinds
            (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                           {:authors [support/fake-hex-64-str]
                                            :kinds [1 2 3]}]))))
    (is (= [{} {:authors [support/fake-hex-64-str]}]
          (#'app/prepare-req-filters conf [{} {} {:authors [support/fake-hex-64-str]}
                                           {:authors [support/fake-hex-64-str]
                                            ;; this filter gets erased b/c it only mentions unserved
                                            :kinds [3]}])))
    (is (= []
          (#'app/prepare-req-filters conf [{:authors [support/fake-hex-64-str]
                                            ;; this filter gets erased b/c it only mentions unserved
                                            ;; ...and since we're the only filter the entire list
                                            ;; of filters becomes empty...
                                            :kinds [3]}])))))

(deftest rejected-event-test
  ;; This test is a sort of \"integration\" test for the `app/receive-event`
  ;; handling. In the case of app code refactoring we may need to revisit
  ;; redefs here, etc.
  (let [result-atom (atom [])
        invoke-sut! #(do
                       (reset! result-atom [])
                       (#'app/receive-event
                         %1
                         ::stub-metrics
                         ::stub-db-cxns
                         ::stub-subs-atom
                         ::stub-channel-id
                         ::stub-websocket-state
                         ::stub-channel
                         ["EVENT" %2]
                         ::stub-raw-message)
                       @result-atom)]
    (with-redefs [app/receive-accepted-event! (fn [& _] (swap! result-atom conj :accept))
                  app/handle-rejected-event! (fn [& _] (swap! result-atom conj :reject))]
      ;; note: for rejection purposes we don't actually reject when :kind is missing
      ;; or invalid or when :content is invalid type, we only reject if :kind is
      ;; valid but we are configured not to support it, or if :content is valid
      ;; but too long
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"]) {})))
      (is (= [:reject] (invoke-sut! (make-test-conf ["1-2"] nil 3) {:content "0123"})))
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"] nil 3) {:content "012"})))
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"]) {:content "012"})))
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"]) {:kind 1})))
      (is (= [:reject] (invoke-sut! (make-test-conf ["1-2"]) {:kind 3})))
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"] nil nil 100)
                         {:created_at (#'app/current-system-epoch-seconds)})))
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"] nil nil 100)
                         {:created_at (#'app/current-system-epoch-seconds)})))
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"] nil nil 100)
                         {:created_at (+ (#'app/current-system-epoch-seconds) 50)})))
      (is (= [:reject] (invoke-sut! (make-test-conf ["1-2"] nil nil 100)
                         {:created_at (+ (#'app/current-system-epoch-seconds) 200)})))
      (is (= [:accept] (invoke-sut! (make-test-conf ["1-2"] nil nil 100)
                         {:tags (mapv (fn [i] ["p" (str i)]) (range 0 5000))})))
      (is (= [:reject] (invoke-sut! (make-test-conf ["1-2"] nil nil 100)
                         {:tags (mapv (fn [i] ["p" (str i)]) (range 0 5001))}))))))

(deftest receive-accepted-event-test
  ;; This test is an \"integration\"-ish test for the `app/receive-accepted-event!`
  ;; handling. In the case of app code refactoring we may need to revisit
  ;; redefs here, etc.
  (let [fake-metrics (metrics/create-metrics)
        result-atom (atom [])
        make-event (fn [id pk ts k & {:keys [p# e# z#]}]
                     {:id id :pubkey pk :created_at ts :kind k
                      :tags (vec
                              (concat
                                (map #(vector "p" %) p#)
                                (map #(vector "p" %) e#)
                                (map #(vector "z" %) z#)))})
        q* (fn
             ([db]
              (jdbc/execute! db ["select * from n_events where deleted_ = 0"]
                {:builder-fn rs/as-unqualified-lower-maps}))
             ([db id]
              (jdbc/execute-one! db ["select * from n_events where event_id = ? and deleted_ = 0" id]
                {:builder-fn rs/as-unqualified-lower-maps})))
        lookup-kv (fn [db-kv event-id]
                    (:raw_event
                      (jdbc/execute-one! db-kv
                        ["select raw_event from n_kv_events where event_id = ?" event-id]
                        {:builder-fn rs/as-unqualified-lower-maps})))]
    (support/with-memory-db-kv-schema [db-kv-cxn]
      (support/with-memory-db-new-schema [db-cxn]
        (let [invoke-sut! #(do
                             (reset! result-atom [])
                             (#'app/receive-accepted-event!
                               ::stub-conf
                               fake-metrics
                               ::stub-db-cxns
                               ::stub-subs-atom
                               ::stub-channel-id
                               ::stub-websocket-state
                               ::stub-channel
                               %1
                               ::stub-raw-message)
                             @result-atom)]
          (with-redefs [;; for our test we need run-async to actually be a blocking sync:
                        write-thread/run-async! (fn [_metrics db-cxns task-fn success-fn failure-fn]
                                                  (let [res (try
                                                              (task-fn
                                                                db-cxn
                                                                db-kv-cxn)
                                                              (catch Throwable t
                                                                t))]
                                                    (if (instance? Throwable res)
                                                      (failure-fn res)
                                                      (success-fn res))))
                        app/as-verified-event identity
                        app/handle-duplicate-event! (fn [& _] (swap! result-atom conj :duplicate))
                        app/handle-stored-or-replaced-event! (fn [& _] (swap! result-atom conj :stored*))
                        app/handle-invalid-event! (fn [& _] (swap! result-atom conj :invalid))
                        subscribe/notify! (fn [& _] (swap! result-atom conj :notified))]
            (is (= [:stored* :notified]
                  (invoke-sut! (make-event "10" "pk0" 10 1))))
            (is (= [:duplicate] ;; *not* :notified if duplicate
                  (invoke-sut! (make-event "10" "-pk0" -10 -1 :p# ["pkX"]))))
            (doseq [ephemeral-kind [29999 20000]
                    :let [event-obj (make-event "20" "pk0" 20 ephemeral-kind :p# ["pkX"])]]
              (is (#'app/ephemeral-event? event-obj))
              (is (= [:notified] ;; ephemeral events are notified, never stored.
                    (invoke-sut! event-obj))))
            ;; duplicate event did not replace original (this is sanity check;
            ;; in production, we should never invoke receive-accepted-event!
            ;; with same id but different payloads as at least one would not
            ;; verify.
            (is (= [{:event_id "10" :pubkey "pk0"}]
                  (mapv #(select-keys % [:event_id :pubkey]) (q* db-cxn))))
            (let [make-id (partial format "time-%s:kind-%s")]
              (doseq [replaceable-kind [19999 10000]]
                (let [created-at 10
                      event-id (make-id created-at replaceable-kind)
                      event-obj (make-event event-id "pk0" created-at replaceable-kind :p# ["pkX" "pkY"] :z# ["booya"])]
                  (is (#'app/replaceable-event? event-obj))
                  (is (= [:stored* :notified] (invoke-sut! event-obj))))
                (let [created-at 20
                      event-id (make-id created-at replaceable-kind)
                      event-obj (make-event event-id "pk0" created-at replaceable-kind :p# ["pkX"] :z# ["smasha"])]
                  (is (#'app/replaceable-event? event-obj))
                  (is (= [:stored* :notified] (invoke-sut! event-obj))))
                (let [created-at 15
                      event-id (make-id created-at replaceable-kind)
                      event-obj (make-event event-id "pk0" created-at replaceable-kind :p# ["pkX"])]
                  (is (#'app/replaceable-event? event-obj))
                  (is (= [:stored* :notified] (invoke-sut! event-obj)))))
              (doseq [[idx replaceable-kind] (map-indexed vector [19999 10000])]
                (is (nil? (q* db-cxn (make-id 10 idx replaceable-kind))))
                (is (nil? (q* db-cxn (make-id 15 idx replaceable-kind))))
                ;; the latest one is the not deleted one:
                (is (= {:pubkey "pk0" :created_at 20}
                      (->
                        (q* db-cxn (make-id 20 replaceable-kind))
                        (select-keys [:pubkey :created_at])))))
              ;; last check over all data written by this test -- we're passing
              ;; through a lot of integrative pieces here:
              (let [q (engine/active-filters->query [(engine/init-active-filter {})])]
                (is (= [(make-id 20 10000) (make-id 20 19999) "10"]
                      (mapv (comp :id #'app/parse (partial lookup-kv db-kv-cxn) :event_id)
                        (jdbc/execute! db-cxn q
                          {:builder-fn rs/as-unqualified-lower-maps})))))
              (let [q (engine/active-filters->query [(engine/init-active-filter {:#p ["pkX"]})])]
                (is (= [(make-id 20 10000) (make-id 20 19999)]
                      (mapv (comp :id #'app/parse (partial lookup-kv db-kv-cxn) :event_id)
                        (jdbc/execute! db-cxn q
                          {:builder-fn rs/as-unqualified-lower-maps})))))
              (let [q (engine/active-filters->query [(engine/init-active-filter {:#z ["booya"]})])]
                (is (= []
                      (mapv (comp :id #'app/parse (partial lookup-kv db-kv-cxn) :event_id)
                        (jdbc/execute! db-cxn q
                          {:builder-fn rs/as-unqualified-lower-maps})))))
              (let [q (engine/active-filters->query [(engine/init-active-filter {:#z ["smasha"]})])]
                (is (= [(make-id 20 10000) (make-id 20 19999)]
                      (mapv (comp :id #'app/parse (partial lookup-kv db-kv-cxn) :event_id)
                        (jdbc/execute! db-cxn q
                          {:builder-fn rs/as-unqualified-lower-maps}))))))))))))
