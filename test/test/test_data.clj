(ns test.test-data
  (:require [clojure.test :refer :all]))

(def pool-with-filters
  ;; note: no kind 0 or kind 3s here which are subject to triggered deletes
  {:pool [{:id "100" :pubkey "abe" :created_at 100 :kind 1 :tags []}
          {:id "101" :pubkey "bob" :created_at 110 :kind 1 :tags []}
          {:id "102" :pubkey "abe" :created_at 120 :kind 1 :tags []}
          {:id "103" :pubkey "bob" :created_at 130 :kind 4 :tags [["p" "abe"] ["e" "100"]]}
          {:id "104" :pubkey "cat" :created_at 140 :kind 4 :tags [["e" "100"] ["e" "101"]]}
          {:id "105" :pubkey "cat" :created_at 150 :kind 1 :tags [["p" "bob"] ["p" "cat"]]}
          {:id "106" :pubkey "cat" :created_at 160 :kind 1 :tags [["t" "tag0"]]}
          {:id "107" :pubkey "dog" :created_at 170 :kind 1 :tags [["p" "cat"] ["t" "tag1"]]}]
   :filters->results
   [[[{:ids ["100" "101"]}] ["100" "101"]]
    [[{:ids ["100" "101"] :limit 1}] ["101"]]
    [[{:since 140}] ["104" "105" "106" "107"]]
    [[{:since 140 :limit 1}] ["107"]]
    [[{:since 140 :until 140}] ["104"]]
    [[{:until 140}] ["100" "101" "102" "103" "104"]]
    [[{:until 140 :limit 3}] ["102" "103" "104"]]
    [[{:ids ["100" "101"] :authors ["abe"]}] ["100"]]
    [[{:authors ["bob"] :#p ["abe"]}] ["103"]]
    [[{:authors ["bob"] :#p ["abe"] :#e ["100"]}] ["103"]]
    [[{:#e ["100"]}] ["103" "104"]]
    [[{:#p ["abe" "bob" "cat"]}] ["103" "105" "107"]]
    [[{:ids ["105"] :authors ["abe " "bob" "cat"]
       :kinds [1 2] :since 140 :until 150
       :#p ["abe" "bob" "cat"] :#e []}] ["105"]]
    [[{:#e ["102" "103"]}] []]
    [[{:#t ["100" "101" "abe" "bob" "cat"]}] []]
    [[{:#t ["tag0" "tag1"]}] ["106" "107"]]
    [[{:ids ["106"] :#t ["tag0" "tag1"]}] ["106"]]
    [[{:#p ["cat"] :#t ["tag0"]}] []]
    [[{:#p ["cat"] :#t ["tag1"]}] ["107"]]
    ;; db union queries
    [[{:ids ["100" "101"]} {:#e ["100"]} {:#e ["102" "103"]}] ["100" "101" "103" "104"]]
    [[{:ids ["100" "101"] :limit 1} {:ids ["102" "103" "104"] :limit 2}] ["101" "103" "104"]]
    [[{:#e ["100"]} {:#p ["abe" "cat"]} {:#t ["tag0"]}] ["103" "104" "105" "106" "107"]]]})

(def filter-matches
  [;; we never expect empty filter but test the base case anyway
   {:filter {}
    :known-matches [{:id "id" :pubkey "pk" :created_at 100 :kind 1 :tags []}]
    :known-non-matches []}
   {:filter {:ids ["id0"]}
    :known-matches [{:id "id0" :pubkey "pk" :created_at 100 :kind 1 :tags []}]
    :known-non-matches [{:id "id1" :pubkey "pk" :created_at 100 :kind 1 :tags []}]}
   {:filter {:kinds [0 1]}
    :known-matches [{:id "id0" :pubkey "pk" :created_at 100 :kind 0 :tags []}
                    {:id "id1" :pubkey "pk" :created_at 100 :kind 1 :tags []}]
    :known-non-matches [{:id "id1" :pubkey "pk" :created_at 100 :kind 2 :tags []}]}
   {:filter {:authors ["pk0" "pk1" "pk2"]}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 100 :kind 0 :tags []}
                    {:id "id1" :pubkey "pk1" :created_at 100 :kind 1 :tags []}
                    {:id "id2" :pubkey "pk2" :created_at 100 :kind 2 :tags []}]
    :known-non-matches [{:id "id0" :pubkey "pk3" :created_at 100 :kind 1 :tags []}]}
   {:filter {:until 100}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 0 :kind 0 :tags []}
                    {:id "id1" :pubkey "pk1" :created_at 50 :kind 1 :tags []}
                    {:id "id2" :pubkey "pk2" :created_at 100 :kind 2 :tags []}]
    :known-non-matches [{:id "id0" :pubkey "pk2" :created_at 101 :kind 1 :tags []}]}
   {:filter {:since 25 :until 100}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 25 :kind 0 :tags []}
                    {:id "id1" :pubkey "pk1" :created_at 50 :kind 1 :tags []}
                    {:id "id2" :pubkey "pk2" :created_at 100 :kind 2 :tags []}]
    :known-non-matches [{:id "id0" :pubkey "pk0" :created_at 24 :kind 0 :tags []}
                        {:id "id1" :pubkey "pk1" :created_at 101 :kind 1 :tags []}]}
   {:filter {:#p ["pk0" "pk1"]}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 25 :kind 0 :tags [["p" "pk0"]]}
                    {:id "id1" :pubkey "pk1" :created_at 50 :kind 1 :tags [["p" "pk1"]]}
                    {:id "id2" :pubkey "pk2" :created_at 100 :kind 2 :tags [["p" "pk0"] ["e" "id0"] ["p" "pk1"]]}]
    :known-non-matches [{:id "id0" :pubkey "pk0" :created_at 24 :kind 0 :tags []}
                        {:id "id1" :pubkey "pk1" :created_at 101 :kind 1 :tags []}
                        {:id "id2" :pubkey "pk2" :created_at 101 :kind 1 :tags [["p" "p2"]]}
                        {:id "id2" :pubkey "pk2" :created_at 101 :kind 1 :tags [["p" "p2"] ["p" "p3"]]}]}
   {:filter {:#e ["id0" "id1" "id2"]}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 25 :kind 0 :tags [["e" "id0"]]}
                    {:id "id1" :pubkey "pk1" :created_at 50 :kind 1 :tags [["e" "id1"]]}
                    {:id "id2" :pubkey "pk2" :created_at 100 :kind 2 :tags [["p" "pk0"] ["e" "id0"] ["p" "pk1"]]}]
    :known-non-matches [{:id "id0" :pubkey "pk0" :created_at 24 :kind 0 :tags []}
                        {:id "id1" :pubkey "pk1" :created_at 101 :kind 1 :tags []}
                        {:id "id2" :pubkey "pk2" :created_at 101 :kind 1 :tags [["e" "id3"]]}
                        {:id "id2" :pubkey "pk2" :created_at 101 :kind 1 :tags [["e" "id3"] ["p" "p0"] ["e" "id4"]]}]}
   {:filter {:ids ["id0"] :authors ["pk0"]}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 100 :kind 0 :tags []}]
    :known-non-matches [{:id "id0" :pubkey "pk1" :created_at 100 :kind 0 :tags []}
                        {:id "id1" :pubkey "pk0" :created_at 100 :kind 0 :tags [["e" "id3"] ["p" "p0"] ["e" "id4"]]}]}
   {:filter {:ids ["id0"] :authors ["pk0"] :since 100}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 100 :kind 0 :tags []}
                    {:id "id0" :pubkey "pk0" :created_at 1000 :kind 1 :tags [["e" "id3"] ["p" "p0"] ["e" "id4"]]}]
    :known-non-matches [{:id "id1" :pubkey "pk0" :created_at 100 :kind 0 :tags []}
                        {:id "id0" :pubkey "pk1" :created_at 100 :kind 0 :tags []}
                        {:id "id0" :pubkey "pk0" :created_at 99 :kind 0 :tags []}]}
   {:filter {:kinds [0 2] :#p ["pk0" "pk2"]}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 100 :kind 0 :tags [["p" "pk0"]]}
                    {:id "id0" :pubkey "pk1" :created_at 100 :kind 2 :tags [["p" "pk0"] ["e" "id0"] ["p" "pk2"]]}]
    :known-non-matches [{:id "id0" :pubkey "pk0" :created_at 100 :kind 2 :tags [["p" "pk1"]]}]}
   {:filter {:authors ["pk0" "pk1" "pk2" "pk3"] :kinds [0] :#p ["pk0" "pk2"]}
    :known-matches [{:id "id0" :pubkey "pk0" :created_at 100 :kind 0 :tags [["p" "pk0"]]}
                    {:id "id0" :pubkey "pk3" :created_at 100 :kind 0 :tags [["p" "pk0"] ["e" "id0"]]}]
    :known-non-matches [{:id "id0" :pubkey "pk0" :created_at 100 :kind 1 :tags [["p" "pk0"]]}]}])
