## me.untethr.nostr-relay

A [nostr](https://github.com/fiatjaf/nostr/) relay, written in Clojure, backed by sqlite3.

**Supported NIPs**: [NIP-01](https://github.com/nostr-protocol/nips/blob/master/01.md),
[NIP-02](https://github.com/nostr-protocol/nips/blob/master/02.md),
[NIP-04](https://github.com/nostr-protocol/nips/blob/master/04.md),
[NIP-11](https://github.com/nostr-protocol/nips/blob/master/11.md),
[NIP-12](https://github.com/nostr-protocol/nips/blob/master/12.md),
[NIP-15](https://github.com/nostr-protocol/nips/blob/master/15.md),
[NIP-16](https://github.com/nostr-protocol/nips/blob/master/16.md),
[NIP-20](https://github.com/nostr-protocol/nips/blob/master/20.md),
[NIP-22](https://github.com/nostr-protocol/nips/blob/master/22.md),
[NIP-42](https://github.com/nostr-protocol/nips/blob/master/42.md)


**Coming Soon**: [NIP-09](https://github.com/nostr-protocol/nips/blob/master/09.md),
[NIP-26](https://github.com/nostr-protocol/nips/blob/master/26.md),
[NIP-33](https://github.com/nostr-protocol/nips/blob/master/33.md),
[NIP-40](https://github.com/nostr-protocol/nips/blob/master/40.md)

### Project Goals

* solid [nips](https://github.com/nostr-protocol/nips) coverage &amp; data completeness + accuracy
* speed and performance on modest hardware

### Run Locally

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
the "supported_nips" and "version" as-is in the `nip11.json` file), and run 
(using java 11+):

```
$ java -Xms1g -Xmx1g \
    -Dlogback.configurationFile=conf/logback.xml \
    -cp me.untethr.nostr-relay.jar \
    clojure.main -m me.untethr.nostr.app
```

This runs the relay on the port specified in `conf/relay.yaml` (default 9090).

You'll want your users to hit a reverse proxy, configured to serve SSL traffic
(wss://...) and proxy to the relay server.

See [Deploy](./doc/deploy.md) for more information on how to run a real 
deployment.

### Develop

The best place to start reading the code is from the `-main` method in the
well-documented [me.untethr.nostr.app](./src/me/untethr/nostr/app.clj) namespace.

If you're developing you can build a jar or deployment archive from latest
source, like so:


```
$ make uberjar
```

or

```
$ make deploy-archive
```
