(ns me.untethr.nostr.util
  (:import (com.google.common.base Supplier Suppliers)
           (java.util.concurrent TimeUnit)))

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

(defn nanos-to-millis
  [nanos]
  (long (/ nanos 1000000)))

(defn memoize-with-expiration
  [f duration-millis]
  (let [memoized-supplier
        (Suppliers/memoizeWithExpiration
          (reify Supplier
            (get [_this] (f)))
          duration-millis
          TimeUnit/MILLISECONDS)]
    (fn []
      (.get memoized-supplier))))
