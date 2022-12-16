(ns me.untethr.nostr.common
  (:require [clojure.set :as set]))

(defn indexable-tag-str?*
  [^String s]
  ;; for now, we'll only index single lowercase ascii character tags, though
  ;; nip-12 specifies ambiguous "single-letter key"; we'll also reject queries
  ;; that don't use lowercase ascii for generic tags.
  ;; @see https://github.com/nostr-protocol/nips/blob/master/12.md
  (.matches s "[a-z]"))

(def allowed-filter-tag-queries-set
  (into #{}
    (comp (map char) (map #(str "#" %)) (map keyword))
    (range (int \a) (inc (int \z)))))

(def allow-filter-tag-queries-sans-e-and-p-set
  (set/difference allowed-filter-tag-queries-set #{:#e :#p}))
