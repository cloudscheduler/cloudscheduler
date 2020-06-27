#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    echo "Key: ${encrypted_ede68231167e_key}, IV: ${encrypted_ede68231167e_iv}, KeyName: ${GPG_KEY_NAME}, Passphrase: ${GPG_PASSPHRASE}, SONATYPE_USERNAME: ${SONATYPE_USERNAME}, SONATYPE_PASSWORD: ${SONATYPE_PASSWORD}"
    openssl aes-256-cbc -K $encrypted_ede68231167e_key -iv $encrypted_ede68231167e_iv -in cd/codesigning.asc.enc -out cd/codesigning.asc -d
    gpg --fast-import cd/codesigning.asc
fi
