(ns me.untethr.nostr.page.home
  (:require [me.untethr.nostr.conf])
  (:import (me.untethr.nostr.conf Conf)))

(def ^:private nostr-url "https://github.com/nostr-protocol/nostr")
(def ^:private untethr-url "https://github.com/atdixon/me.untethr.nostr-relay")
(def ^:private untethr-new-issue-url "https://github.com/atdixon/me.untethr.nostr-relay/issues/new")

(defn html
  [^Conf conf]
  (let [the-hostname (:optional-hostname conf)]
    (format
      (str
        "<body>"
        "<p>Hello!"
        "<p>I am a fast <a target=\"_blank\" href=\"%s\">nostr</a> relay"
        " of <a target=\"_blank\" href=\"%s\">this flavor</a>."
        "<p>Add me to your nostr client using: <pre>wss://%s</pre>"
        "<p>Or, get to know me better: <pre>curl -H 'Accept: application/nostr+json' https://%s</pre>"
        "<p>Need a new feature? Found a bug? Tell us about it <a target=\"_blank\" href=\"%s\">here</a>."
        "</body>")
      nostr-url untethr-url the-hostname the-hostname untethr-new-issue-url)))
