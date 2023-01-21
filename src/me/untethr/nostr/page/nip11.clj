(ns me.untethr.nostr.page.nip11
  (:require
    [clojure.string :as str]
    [me.untethr.nostr.conf]
    [me.untethr.nostr.common.json-facade :as json-facade]
    [me.untethr.nostr.common.version :as version]))

(def supported-nips [1, 2, 4, 11, 12, 15, 16, 20, 22, 42])

(defn json
  [nip11-json]
  (-> nip11-json
    (str/replace "${runtime.version}" version/version)
    ;; note: we replace the whole string including the runtime.nips
    ;; variable with a json array -- this is b/c we require the file,
    ;; even with variables, to be json-parseable
    (str/replace "\"${runtime.nips}\""
      (json-facade/write-str* supported-nips))))
