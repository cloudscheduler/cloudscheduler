#!/usr/bin/env bash
if [[ "$TRAVIS_PULL_REQUEST" == 'false' ]]; then
    ./gradlew publishToSonatype -Psigning.gnupg.keyName="${GPG_KEY_NAME}" -Psigning.gnupg.passphrase="${GPG_PASSPHRASE}" -Psigning.gnupg.useLegacyGpg=true
fi
