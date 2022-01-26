.PHONY: clean
clean:
	rm -rf target/

.PHONY: test
test:
	clj -X:test

.PHONY: run
run:
	clj -M -m me.untethr.nostr.app

.PHONY: uberjar
uberjar:
	clj -M:uberdeps

.PHONY: run-uberjar
run-uberjar:
	java -cp target/go-social.jar clojure.main -m me.untethr.nostr.app
