(ns test.test-data
  (:require [clojure.test :refer :all]))

(defn- ->hex-64*
  "Deterministically, make a 64-char hex string from a smaller string."
  [s]
  {:pre [(<= (count s) 64)]}
  (apply str
    (concat s
      (take (- 64 (count s)) (cycle "abcdef0123456789")))))

(def hx ->hex-64*) ;; public

(def pool-with-filters
  ;; note: no kind 0 or kind 3s here which are subject to triggered deletes
  {:pool [{:id (hx "100") :pubkey (hx "abe") :created_at 100 :kind 1 :tags []}
          {:id (hx "101") :pubkey (hx "bebe") :created_at 110 :kind 1 :tags []}
          {:id (hx "102") :pubkey (hx "abe") :created_at 120 :kind 1 :tags []}
          {:id (hx "103") :pubkey (hx "bebe") :created_at 130 :kind 4 :tags [["p" (hx "abe")] ["e" (hx "100")]]}
          {:id (hx "104") :pubkey (hx "cab") :created_at 140 :kind 4 :tags [["e" (hx "100")] ["e" (hx "101")]]}
          {:id (hx "105") :pubkey (hx "cab") :created_at 150 :kind 1 :tags [["p" (hx "bebe")] ["p" (hx "cab")]]}
          {:id (hx "106") :pubkey (hx "cab") :created_at 160 :kind 1 :tags [["t" "tag0"]]}
          {:id (hx "107") :pubkey "dog" :created_at 170 :kind 1 :tags [["p" (hx "cab")] ["t" "tag1"]]}]
   :filters->results
   ;; results here are ordered by created_at descending, which allows our tests
   ;; that use this pool to test outer limits...
   [[[{}] [(hx "107") (hx "106") (hx "105") (hx "104") (hx "103") (hx "102") (hx "101") (hx "100")]]
    [[{} {:limit 1}] [(hx "107") (hx "106") (hx "105") (hx "104") (hx "103") (hx "102") (hx "101") (hx "100")]]
    [[{:limit 2} {:limit 1}] [(hx "107") (hx "106")]]
    [[{:kinds [4]}] [(hx "104") (hx "103")]]
    [[{:ids [(hx "100") (hx "101")]}] [(hx "101") (hx "100")]]
    [[{:ids [(hx "100") (hx "101")] :limit 1}] [(hx "101")]]
    [[{:since 140}] [(hx "107") (hx "106") (hx "105") (hx "104")]]
    [[{:since 140 :limit 1}] [(hx "107")]]
    [[{:since 150 :limit 1} {:until 120 :limit 3}] [(hx "107") (hx "102") (hx "101") (hx "100")]]
    [[{:since 140 :until 140}] [(hx "104")]]
    [[{:until 140}] [(hx "104") (hx "103") (hx "102") (hx "101") (hx "100")]]
    [[{:until 140 :limit 3}] [(hx "104") (hx "103") (hx "102")]]
    [[{:ids [(hx "100") (hx "101")] :authors [(hx "abe")]}] [(hx "100")]]
    [[{:ids [(hx "100") (hx "101")]} {:#e [(hx "100")]} {:#e [(hx "101") (hx "102")]}] [(hx "104") (hx "103") (hx "101") (hx "100")]]
    [[{:authors [(hx "abe") (hx "bebe")] :kinds [4]}] [(hx "103")]]
    [[{:authors [(hx "abe") (hx "bebe")] :kinds [4 1]}] [(hx "103") (hx "102") (hx "101") (hx "100")]]
    [[{:authors [(hx "bebe")] :#p [(hx "abe")]}] [(hx "103")]]
    [[{:authors [(hx "bebe")] :#p [(hx "abe")] :#e [(hx "100")]}] [(hx "103")]]
    [[{:#e [(hx "100")]}] [(hx "104") (hx "103")]]
    [[{:#e [(hx "100")] :kinds [4]}] [(hx "104") (hx "103")]]
    [[{:#e [(hx "100")] :kinds [1]}] []]
    [[{:#p [(hx "abe") (hx "bebe") (hx "cab")]}] [(hx "107") (hx "105") (hx "103")]]
    [[{:#p [(hx "abe") (hx "bebe") (hx "cab")] :kinds [1]}] [(hx "107") (hx "105")]]
    [[{:#p [(hx "abe") (hx "bebe") (hx "cab")] :kinds [1]
       :since 160}] [(hx "107")]]
    [[{:#p [(hx "abe") (hx "bebe") (hx "cab")] :kinds [1]
       :until 160}] [(hx "105")]]
    [[{:ids [(hx "105")] :authors [(hx "abe") (hx "bebe") (hx "cab")]
       :kinds [1 2] :since 140 :until 150
       :#p [(hx "abe") (hx "bebe") (hx "cab")] :#e []}] [(hx "105")]]
    [[{:#e [(hx "102") (hx "103")]}] []]
    [[{:#t [(hx "100") (hx "101") (hx "abe") (hx "bebe") (hx "cab")]}] []]
    [[{:#t ["tag0" "tag1"]}] [(hx "107") (hx "106")]]
    [[{:ids [(hx "106")] :#t ["tag0" "tag1"]}] [(hx "106")]]
    [[{:#p [(hx "cab")] :#t ["tag0"]}] []]
    [[{:#p [(hx "cab")] :#t ["tag1"]}] [(hx "107")]]
    ;; prefix queries
    [[{:authors [(hx "bebe")]}] [(hx "103") (hx "101")]]
    [[{:ids ["107"]}] [(hx "107")]]
    [[{:ids ["10"]}] [(hx "107") (hx "106") (hx "105") (hx "104") (hx "103") (hx "102") (hx "101") (hx "100")]]
    ;; db union queries
    [[{:ids [(hx "100") (hx "101")]} {:#e [(hx "100")]} {:#e [(hx "102") (hx "103")]}] [(hx "104") (hx "103") (hx "101") (hx "100")]]
    [[{:ids [(hx "100") (hx "101")] :limit 1} {:ids [(hx "102") (hx "103") (hx "104")] :limit 2}] [(hx "104") (hx "103") (hx "101")]]
    [[{:#e [(hx "100")]} {:#p [(hx "abe") (hx "cab")]} {:#t ["tag0"]}] [(hx "107") (hx "106") (hx "105") (hx "104") (hx "103")]]]})

(def filter-matches
  [{:filter {}
    :known-matches [{:id (hx "ab") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}]
    :known-non-matches []}
   {:filter {:ids [(hx "ab0")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}]
    :known-non-matches [{:id (hx "ab1") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}]}
   {:filter {:kinds [0 1]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd") :created_at 100 :kind 0 :tags []}
                    {:id (hx "ab1") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}]
    :known-non-matches [{:id (hx "ab1") :pubkey (hx "cd") :created_at 100 :kind 2 :tags []}]}
   {:filter {:authors [(hx "cd0") (hx "cd1") (hx "cd2")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags []}
                    {:id (hx "ab1") :pubkey (hx "cd1") :created_at 100 :kind 1 :tags []}
                    {:id (hx "ab2") :pubkey (hx "cd2") :created_at 100 :kind 2 :tags []}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd3") :created_at 100 :kind 1 :tags []}]}
   {:filter {:until 100}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 0 :kind 0 :tags []}
                    {:id (hx "ab1") :pubkey (hx "cd1") :created_at 50 :kind 1 :tags []}
                    {:id (hx "ab2") :pubkey (hx "cd2") :created_at 100 :kind 2 :tags []}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd2") :created_at 101 :kind 1 :tags []}]}
   {:filter {:since 25 :until 100}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 25 :kind 0 :tags []}
                    {:id (hx "ab1") :pubkey (hx "cd1") :created_at 50 :kind 1 :tags []}
                    {:id (hx "ab2") :pubkey (hx "cd2") :created_at 100 :kind 2 :tags []}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 24 :kind 0 :tags []}
                        {:id (hx "ab1") :pubkey (hx "cd1") :created_at 101 :kind 1 :tags []}]}
   {:filter {:#p [(hx "cd0") (hx "cd1")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 25 :kind 0 :tags [["p" (hx "cd0")]]}
                    {:id (hx "ab1") :pubkey (hx "cd1") :created_at 50 :kind 1 :tags [["p" (hx "cd1")]]}
                    {:id (hx "ab2") :pubkey (hx "cd2") :created_at 100 :kind 2 :tags [["p" (hx "cd0")] ["e" (hx "ab0")] ["p" (hx "cd1")]]}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 24 :kind 0 :tags []}
                        {:id (hx "ab1") :pubkey (hx "cd1") :created_at 101 :kind 1 :tags []}
                        {:id (hx "ab2") :pubkey (hx "cd2") :created_at 101 :kind 1 :tags [["p" "p2"]]}
                        {:id (hx "ab2") :pubkey (hx "cd2") :created_at 101 :kind 1 :tags [["p" "p2"] ["p" "p3"]]}]}
   {:filter {:#e [(hx "ab0") (hx "ab1") (hx "ab2")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 25 :kind 0 :tags [["e" (hx "ab0")]]}
                    {:id (hx "ab1") :pubkey (hx "cd1") :created_at 50 :kind 1 :tags [["e" (hx "ab1")]]}
                    {:id (hx "ab2") :pubkey (hx "cd2") :created_at 100 :kind 2 :tags [["p" (hx "cd0")] ["e" (hx "ab0")] ["p" (hx "cd1")]]}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 24 :kind 0 :tags []}
                        {:id (hx "ab1") :pubkey (hx "cd1") :created_at 101 :kind 1 :tags []}
                        {:id (hx "ab2") :pubkey (hx "cd2") :created_at 101 :kind 1 :tags [["e" "id3"]]}
                        {:id (hx "ab2") :pubkey (hx "cd2") :created_at 101 :kind 1 :tags [["e" "id3"] ["p" "p0"] ["e" "id4"]]}]}
   {:filter {:ids [(hx "ab0")] :authors [(hx "cd0")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags []}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd1") :created_at 100 :kind 0 :tags []}
                        {:id (hx "ab1") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags [["e" "id3"] ["p" "p0"] ["e" "id4"]]}]}
   {:filter {:ids [(hx "ab0")] :authors [(hx "cd0")] :since 100}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags []}
                    {:id (hx "ab0") :pubkey (hx "cd0") :created_at 1000 :kind 1 :tags [["e" "id3"] ["p" "p0"] ["e" "id4"]]}]
    :known-non-matches [{:id (hx "ab1") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags []}
                        {:id (hx "ab0") :pubkey (hx "cd1") :created_at 100 :kind 0 :tags []}
                        {:id (hx "ab0") :pubkey (hx "cd0") :created_at 99 :kind 0 :tags []}]}
   {:filter {:kinds [0 2] :#p [(hx "cd0") (hx "cd2")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags [["p" (hx "cd0")]]}
                    {:id (hx "ab0") :pubkey (hx "cd1") :created_at 100 :kind 2 :tags [["p" (hx "cd0")] ["e" (hx "ab0")] ["p" (hx "cd2")]]}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 2 :tags [["p" (hx "cd1")]]}]}
   {:filter {:authors [(hx "cd0") (hx "cd1") (hx "cd2") (hx "cd3")] :kinds [0] :#p [(hx "cd0") (hx "cd2")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags [["p" (hx "cd0")]]}
                    {:id (hx "ab0") :pubkey (hx "cd3") :created_at 100 :kind 0 :tags [["p" (hx "cd0")] ["e" (hx "ab0")]]}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 1 :tags [["p" (hx "cd0")]]}]}
   ;; prefix filters...
   {:filter {:ids ["ab0"]} ;; this is a prefix filter!
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}
                    {:id (hx "ab000") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}]
    :known-non-matches [{:id (hx "ab") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}
                        {:id (hx "ab1") :pubkey (hx "cd") :created_at 100 :kind 1 :tags []}]}
   {:filter {:ids ["ab0"] :authors ["cd0"]} ;; prefix filters!!!
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags []}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd1") :created_at 100 :kind 0 :tags []}
                        {:id (hx "ab1") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags [["e" "id3"] ["p" "p0"] ["e" "id4"]]}]}
   {:filter {:authors [(hx "cd0") "cd1" #_prefix "cd2" #_prefix (hx "cd3")] :kinds [0] :#p [(hx "cd0") (hx "cd2")]}
    :known-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 0 :tags [["p" (hx "cd0")]]}
                    {:id (hx "ab0") :pubkey (hx "cd3") :created_at 100 :kind 0 :tags [["p" (hx "cd0")] ["e" (hx "ab0")]]}]
    :known-non-matches [{:id (hx "ab0") :pubkey (hx "cd0") :created_at 100 :kind 1 :tags [["p" (hx "cd0")]]}]}
   ])
