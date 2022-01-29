.PHONY: clean
clean:
	rm -rf target/

.PHONY: test
test:
	clj -X:test

.PHONY: run
run:
	clj -J-Dlogback.configurationFile=conf/logback.xml -M -m me.untethr.nostr.app

.PHONY: uberjar
uberjar:
	clj -M:uberdeps

.PHONY: run-uberjar
run-uberjar:
	java -Dlogback.configurationFile=conf/logback.xml \
		-cp target/me.untethr.nostr-relay.jar \
		clojure.main -m me.untethr.nostr.app

.PHONY: deploy-archive
deploy-archive: clean uberjar
	tar -czvf target/me.untethr.nostr-relay.tar.gz conf/* -C target me.untethr.nostr-relay.jar
