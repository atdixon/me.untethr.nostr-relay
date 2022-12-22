### Deploying

I deploy on Debian and use [systemd](https://en.wikipedia.org/wiki/Systemd) to
launch a script with the above `java` command.

`conf/relay.yml` controls which port the relay runs on. By
default this is `9090`; this is a fine default as you'll want
to run a reverse proxy to server your relay on the well-known SSL port.

I use [ngnix](https://docs.nginx.com/) + [https://letsencrypt.org/](https://letsencrypt.org/) for [SSL termination](https://docs.nginx.com/nginx/admin-guide/security-controls/terminating-ssl-http/) and
[DDoS mitigation](https://www.nginx.com/blog/mitigating-ddos-attacks-with-nginx-and-nginx-plus/)
and configure it to talk to the relay process on port `9090`.

### nginx Notes

In `nginx.conf`, I set `worker_connections` high &mdash; e.g., `worker_connections 32768;`.

I also enforce https except make an exception for http requests with
`$http_accept = application/nostr+json`, because I've noticed that some clients
in the wild issue http [NIP-11](https://github.com/nostr-protocol/nips/blob/master/11.md)
requests.

Totally optionally but you may wish to limit access to `/metrics` and `/q`, eg,
via
[basic auth](https://docs.nginx.com/nginx/admin-guide/security-controls/configuring-http-basic-authentication/).

See [example nginx.conf](nginx.conf.example) &mdash; feel free to send PR for this
config file if you see any potential improvements.

### Show Me Code

This is a script on my server, `/home/aaron/run-relay.sh`:

```shell
#!/usr/bin/env bash

/usr/lib/jvm/jdk-17/bin/java \
    -Xms1g -Xmx1g \
    -Dlogback.configurationFile=conf/logback.xml \
    -cp me.untethr.nostr-relay.jar \
    clojure.main -m me.untethr.nostr.app
```

I made sure it had execute permissions:

```shell
$ chmod u+x /home/aaron/run-relay.sh
```

Then I created `/etc/systemd/system/nostr-relay.service`:

```
[Unit]
Description=me.untethr.nostr.app
[Service]
User=atdixon
WorkingDirectory=/home/aaron
ExecStart=/home/aaron/run-relay.sh
SuccessExitStatus=143
TimeoutStopSec=10
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal
[Install]
WantedBy=multi-user.target
```

And ran:

```
sudo systemctl daemon-reload
sudo systemctl enable nostr-relay.service
sudo systemctl start nostr-relay
sudo systemctl status nostr-relay
```

Now, 2 ways to look at logs.

* Relay server logs:

```
/home/aaron$ tail -f logs/nostr-relay-<latest>.log
```

* systemd service logs &mdash; this will show uncaught errors, due to bad requests or other issues, etc:

```
journalctl -u nostr-relay -f -o cat
```
