(ns test.store-test
  (:require [clojure.test :refer :all]
            [test.support :as support]
            [me.untethr.nostr.store :as store]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(deftest idempotent-insert-test
  (support/with-memory-db [db]
    (store/insert-event! db "id0" "pk0" 0 1 "{}")
    (store/insert-event! db "id0" "xxx" -1 -1 "{}")
    (= 1 (count (jdbc/execute! db ["select * from n_events"])))))

(deftest singleton-kind-cleanup-test
  ;; verify our insert_singleton_kind trigger
  (support/with-memory-db [db]
    (store/insert-event! db "id0" "pk0" 0 1 "{}")
    (store/insert-event! db "id1" "pk0" 1 0 "{}")
    (store/insert-event! db "id2" "pk0" 2 0 "{}")
    (store/insert-event! db "id3" "pk0" 0 0 "{}")
    (store/insert-event! db "id4" "pk0" 1 1 "{}")
    (store/insert-event! db "id5" "pk0" 0 3 "{}")
    (store/insert-event! db "id6" "pk0" 5 3 "{}")
    (store/insert-event! db "id7" "pk1" 15 0 "{}")
    (store/insert-event! db "id8" "pk1" 15 1 "{}")
    (store/insert-event! db "id9" "pk1" 20 1 "{}")
    (store/insert-event! db "id10" "pk1" 20 3 "{}")
    (store/insert-event! db "id11" "pk1" 20 0 "{}")
    (store/insert-event! db "id12" "pk1" 15 3 "{}")
    ;; make sure idempotent insert doesn't mess with trigger:
    (store/insert-event! db "id5" "pk0" 1 3 "{}")
    (let [res (jdbc/execute! db ["select * from n_events where deleted_ = 0"]
                {:builder-fn rs/as-unqualified-lower-maps})
          g (group-by (juxt :pubkey :kind) res)]
      (is (= ["id0" "id4"] (mapv :id (get-in g [["pk0" 1]]))))
      (is (= ["id2"] (mapv :id (get-in g [["pk0" 0]]))))
      (is (= [["id11" 20]] (mapv (juxt :id :created_at) (get-in g [["pk1" 0]]))))
      (is (= [["id8" 15] ["id9" 20]] (mapv (juxt :id :created_at) (get-in g [["pk1" 1]]))))
      (is (= [["id10" 20]] (mapv (juxt :id :created_at) (get-in g [["pk1" 3]]))))
      (is (= [["id6" 5]] (mapv (juxt :id :created_at) (get-in g [["pk0" 3]])))))))
