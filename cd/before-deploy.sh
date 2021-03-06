#!/usr/bin/env bash
if [[ "$TRAVIS_PULL_REQUEST" == 'false' && "$TRAVIS_BRANCH" == 'master' ]]; then
    openssl aes-256-cbc -K $encrypted_ede68231167e_key -iv $encrypted_ede68231167e_iv -in cd/codesigning.asc.enc -out cd/codesigning.asc -d
    gpg --batch --fast-import cd/codesigning.asc
fi
