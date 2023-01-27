.PHONY: clean
clean:
	rm -rf target/

.PHONY: test
test:
	clj -X:test

.PHONE: test-store-only
test-store-only:
	clj -X:test :nses ["test.store-test"]

.PHONY: dependency-sources
dependency-sources:
	clj -X:deps mvn-pom
	mvn dependency:sources

.PHONY: run
run:
	clj \
		-J-Dlogback.configurationFile=conf/logback.xml \
		-J-Dspy.properties=conf/spy.properties \
		-J-Xms1g -J-Xmx1g \
 		-M -m me.untethr.nostr.app

.PHONY: uberjar
uberjar:
	clj -M:uberdeps

.PHONY: run-uberjar
run-uberjar:
	java \
		-Dlogback.configurationFile=conf/logback.xml \
		-Dspy.properties=conf/spy.properties \
		-Xms1g -Xmx1g \
		-cp target/me.untethr.nostr-relay.jar \
		clojure.main -m me.untethr.nostr.app

.PHONY: deploy-archive
deploy-archive: clean uberjar
	tar -czvf target/me.untethr.nostr-relay.tar.gz conf/* -C target me.untethr.nostr-relay.jar

.PHONY: release
release:
	scripts/release.sh
