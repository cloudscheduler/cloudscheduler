#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    ./gradlew publish -Psigning.gnupg.keyName="03EB73BF" -Psigning.gnupg.useLegacyGpg=true
fi
