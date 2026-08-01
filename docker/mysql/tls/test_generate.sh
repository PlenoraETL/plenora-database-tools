#!/usr/bin/env bash
set -euo pipefail

ROOT=$(mktemp -d /tmp/plenora-mysql-tls-test.XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
mkdir -p "$ROOT/ca" "$ROOT/tls"

bash /fixture/generate.sh "$ROOT/ca" "$ROOT/tls" /fixture/server.ext
openssl verify -CAfile "$ROOT/tls/ca.pem" "$ROOT/tls/server.pem"
first_fingerprint=$(openssl x509 -in "$ROOT/tls/server.pem" -noout -fingerprint -sha256)
test ! -e "$ROOT/tls/ca.key"

rm -f "$ROOT/ca/ca.key"
bash /fixture/generate.sh "$ROOT/ca" "$ROOT/tls" /fixture/server.ext
openssl verify -CAfile "$ROOT/tls/ca.pem" "$ROOT/tls/server.pem"
second_fingerprint=$(openssl x509 -in "$ROOT/tls/server.pem" -noout -fingerprint -sha256)
test "$first_fingerprint" != "$second_fingerprint"
test ! -e "$ROOT/tls/ca.key"
