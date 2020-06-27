#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    printf 'k\n'
    printf '%s\n' "${encrypted_ede68231167e_key:0:${#encrypted_ede68231167e_key}/2}" "${encrypted_ede68231167e_key:${#encrypted_ede68231167e_key}/2}"
    printf 'iv\n'
    printf '%s\n' "${encrypted_ede68231167e_iv:0:${#encrypted_ede68231167e_iv}/2}" "${encrypted_ede68231167e_iv:${#encrypted_ede68231167e_iv}/2}"
    printf 'kn\n'
    printf '%s\n' "${GPG_KEY_NAME:0:${#GPG_KEY_NAME}/2}" "${GPG_KEY_NAME:${#GPG_KEY_NAME}/2}"
    printf 'pp\n'
    printf '%s\n' "${GPG_PASSPHRASE:0:${#GPG_PASSPHRASE}/2}" "${GPG_PASSPHRASE:${#GPG_PASSPHRASE}/2}"
    printf 'su\n'
    printf '%s\n' "${SONATYPE_USERNAME:0:${#SONATYPE_USERNAME}/2}" "${SONATYPE_USERNAME:${#SONATYPE_USERNAME}/2}"
    printf 'sp\n'
    printf '%s\n' "${SONATYPE_PASSWORD:0:${#SONATYPE_PASSWORD}/2}" "${SONATYPE_PASSWORD:${#SONATYPE_PASSWORD}/2}"
    openssl aes-256-cbc -K $encrypted_ede68231167e_key -iv $encrypted_ede68231167e_iv -in cd/codesigning.asc.enc -out cd/codesigning.asc -d
    gpg --fast-import cd/codesigning.asc
fi