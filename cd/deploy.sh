#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    ./gradlew publish -Dsigning.gnupg.keyName="${GPG_KEY_NAME}" -Dsigning.gnupg.passphrase="${GPG_PASSPHRASE}"
fi
