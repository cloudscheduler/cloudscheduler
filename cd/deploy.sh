#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    ./gradlew publish -Psigning.gnupg.keyName="${GPG_KEY_NAME}" -Psigning.gnupg.passphrase="${GPG_PASSPHRASE}"
fi
