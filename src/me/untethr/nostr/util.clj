(ns me.untethr.nostr.util)

(defn current-system-epoch-seconds
  []
  (long (/ (System/currentTimeMillis) 1000)))

(defn dissoc-in-if-empty
  [m ks]
  (let [v (get-in m ks)]
    (if (or (nil? v) (and (coll? v) (empty? v)))
      (if (= 1 (count ks))
        (dissoc m (peek ks))
        (update-in m (butlast ks) dissoc (last ks)))
      m)))
