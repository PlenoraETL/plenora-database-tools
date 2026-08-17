#!/usr/bin/env bash
# Keep this fixture LF-only: it is executed inside Linux containers.
#
# Contratto della fixture TLS, in tre invocazioni:
#   1. generazione da zero, con le proprieta che i test live assumono;
#   2. seconda invocazione a materiale intatto -> stesso certificato, cioe
#      riproducibilita: un rerun del gate non invalida la CA gia distribuita;
#   3. invocazione dopo la perdita della chiave CA -> certificato nuovo.
set -euo pipefail

ROOT=$(mktemp -d /tmp/plenora-mysql-tls-test.XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
mkdir -p "$ROOT/ca" "$ROOT/tls"

fingerprint() {
  openssl x509 -in "$ROOT/tls/server.pem" -noout -fingerprint -sha256
}

bash /fixture/generate.sh "$ROOT/ca" "$ROOT/tls" /fixture/server.ext dataflow-mysql
openssl verify -CAfile "$ROOT/tls/ca.pem" "$ROOT/tls/server.pem"
first_fingerprint=$(fingerprint)
test ! -e "$ROOT/tls/ca.key"

# I due nomi su cui poggiano i test live: quello coperto e quello che deve
# restare scoperto. Senza la seconda meta, le prove di rifiuto TLS
# diventerebbero verdi per un motivo diverso da quello dichiarato.
openssl x509 -in "$ROOT/tls/server.pem" -noout -checkhost dataflow-mysql
openssl x509 -in "$ROOT/tls/server.pem" -noout -checkip 127.0.0.1
if openssl x509 -in "$ROOT/tls/server.pem" -noout -checkhost mysql-hostname-mismatch \
  >/dev/null 2>&1; then
  echo "il certificato copre mysql-hostname-mismatch: la prova TLS negativa non prova nulla" >&2
  exit 1
fi

bash /fixture/generate.sh "$ROOT/ca" "$ROOT/tls" /fixture/server.ext dataflow-mysql
openssl verify -CAfile "$ROOT/tls/ca.pem" "$ROOT/tls/server.pem"
test "$first_fingerprint" = "$(fingerprint)"
test ! -e "$ROOT/tls/ca.key"

rm -f "$ROOT/ca/ca.key"
bash /fixture/generate.sh "$ROOT/ca" "$ROOT/tls" /fixture/server.ext dataflow-mysql
openssl verify -CAfile "$ROOT/tls/ca.pem" "$ROOT/tls/server.pem"
second_fingerprint=$(fingerprint)
test "$first_fingerprint" != "$second_fingerprint"
test ! -e "$ROOT/tls/ca.key"
openssl x509 -in "$ROOT/tls/server.pem" -noout -checkhost dataflow-mysql
openssl x509 -in "$ROOT/tls/server.pem" -noout -checkip 127.0.0.1
