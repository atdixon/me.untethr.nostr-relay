## me.untethr.nostr-relay

A [nostr](https://github.com/fiatjaf/nostr/) relay, written in Clojure.

### Run

For a [real deployment](#how-to-deploy), you'll want to deploy with ssl 
termination (e.g. using [ngnix](https://www.nginx.com/)) but for local testing, 
simply:

```
$ brew install clojure/tools/clojure
$ make run
```

The relay will be reachable at `ws://localhost:9090` and data
in a git-ignored `./n.db` sqlite file 

With various app metrics available at `http://localhost:9090/metrics`.

Edit `./conf/relay.yaml` to change port, file, etc.

### Known Deployments

* wss://nostr-relay.untethr.me
* wss://relay.kronkltd.net

### How to Deploy

Use a latest release from [releases](https://github.com/atdixon/me.untethr.nostr-relay/releases/).

A released `tar.gz` archive has these contents:

```
me.untethr.nostr-relay.jar
conf/relay.yaml
conf/nip05.json
conf/nip11.json
conf/logback.xml
```

Unpack on server, update config files to your personal liking (note: leave
the "supported_nips" and "version" as-is in the nip11.json file), and run 
(using java 11+):

```
$ java -Xms1g -Xmx1g \
    -Dlogback.configurationFile=conf/logback.xml \
    -cp me.untethr.nostr-relay.jar \
    clojure.main -m me.untethr.nostr.app
```

This runs the relay on the port specified in `conf/relay.yaml` (default 9090).

You'll want your users to hit a reverse proxy, configured to serve SSL traffic
(wss://...) and proxy to the relay.

If you're developing you can build a jar or deployment archive from latest 
source, like so:


```
$ make uberjar
```

or

```
$ make deploy-archive
```

See [Deploy](./doc/deploy.md) for more information on how to run a real 
deployment.
