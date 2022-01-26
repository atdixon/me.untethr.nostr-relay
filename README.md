## me.untethr.nostr-relay

A [nostr](https://github.com/fiatjaf/nostr/) relay, written in Clojure.

### Run

For a real deployment you'll want to deploy with ssl termination (eg [ngnix](https://www.nginx.com/))
but for local testing, simply:

```
$ brew install clojure/tools/clojure
$ make run
```

The relay will be reachable at `ws://localhost:9090` and data
in a git-ignored `./n.db` sqlite file.

With various app metrics available at `http://localhost:9090/metrics`.