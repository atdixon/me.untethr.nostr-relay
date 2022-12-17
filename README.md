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
in a git-ignored `./n.db` sqlite file 

With various app metrics available at `http://localhost:9090/metrics`.

Edit `./conf/relay.yaml` to change port, file, etc.

### Known Deployments

* wss://nostr-relay.untethr.me

### How to Deploy

```
$ make deploy-archive
```

This will produce a `tar.gz` archive with these contents:

```
me.untethr.nostr-relay.jar
conf/relay.yaml
conf/nip05.json
conf/nip11.json
conf/logback.xml
```

Unpack on server, update config files to your personal liking,
and run:

```
$ java -Xms512m -Xmx1g \
    -Dlogback.configurationFile=conf/logback.xml \
    -cp me.untethr.nostr-relay.jar \
    clojure.main -m me.untethr.nostr.app
```

There are many ways to complete a deployment.

I deploy on Debian and use [systemd](https://en.wikipedia.org/wiki/Systemd) to
launch a script with the above `java` command. 

`conf/relay.yml` controls which port the relay runs on. By
default this is `9090`; this is a fine default as you'll want
to run something else exposed on well-known SSL port.

I use ngnix + https://letsencrypt.org/ for SSL termination and
[DDoS mitigation](https://www.nginx.com/blog/mitigating-ddos-attacks-with-nginx-and-nginx-plus/)
and configure it to talk to the relay process on port `9090`.

Totally optionally but you may wish to limit access to `/metrics` and `/q`, eg, 
via 
[basic auth](https://docs.nginx.com/nginx/admin-guide/security-controls/configuring-http-basic-authentication/).
