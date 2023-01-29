#!/usr/bin/env bash

set -e

if [[ $(git status --porcelain) ]]; then
  echo "error: can't release if there are uncommitted changes"
  exit 1
fi

VERSION_CLJ_FILE="src/me/untethr/nostr/common/version.clj"

# safety - we want to fail (via set -e) if version.clj doesn't look like it
# restored from a previous release:
grep "\"SNAPSHOT\"" "${VERSION_CLJ_FILE}"

read -rp 'Version: ' VERSION

if [[ ! "${VERSION}" =~ ^[0-9]\.[0-9]\.[0-9]+$ ]]; then
  echo "error: version looks bad: ${VERSION}"
  exit 1
fi

echo "Will release at version: ${VERSION}"

read -rp 'Are you sure? (y/n) ' CONFIRM

if [[ "${CONFIRM}" != "y" ]]; then
  echo "okay quitting"
  exit 1
fi

git tag "v${VERSION}"
git push origin "v${VERSION}"

# probably non-os portable sed command (works on mac):
sed -i '' 's/"SNAPSHOT"/"'"${VERSION}"'"/g' "${VERSION_CLJ_FILE}"

make clean uberjar

mv target/me.untethr.nostr-relay.jar \
  "target/me.untethr.nostr-relay-${VERSION}.jar"

git checkout -- "${VERSION_CLJ_FILE}"

tar -czvf "target/me.untethr.nostr-relay-${VERSION}.tar.gz" conf/* -C target "me.untethr.nostr-relay-${VERSION}.jar"
