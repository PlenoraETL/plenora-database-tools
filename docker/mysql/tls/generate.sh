#!/usr/bin/env bash
# Keep this fixture LF-only: it is executed inside Linux containers.
set -euo pipefail

CA_DIR=${1:?directory CA privata obbligatoria}
TLS_DIR=${2:?directory TLS pubblica obbligatoria}
EXT_FILE=${3:?estensioni certificato obbligatorie}
# Il nome che il certificato deve coprire. Obbligatorio e non defaultato:
# questo generatore serve piu di una fixture — MySQL e MariaDB hanno host
# diversi — e un default silenzioso emetterebbe per l'altro riferimento un
# certificato valido, che il client rifiuterebbe per hostname mismatch. Un
# errore di verifica TLS non dice quale nome ci si aspettava.
SERVER_HOST=${4:?nome host del certificato obbligatorio}

mkdir -p "$CA_DIR" "$TLS_DIR"
rm -f "$TLS_DIR/ca.key"

certificates_are_consistent() {
  [[ -s "$CA_DIR/ca.key" && -s "$TLS_DIR/ca.pem" \
    && -s "$TLS_DIR/server.key" && -s "$TLS_DIR/server.pem" ]] || return 1
  openssl verify -CAfile "$TLS_DIR/ca.pem" "$TLS_DIR/server.pem" >/dev/null 2>&1 || return 1
  [[ "$(openssl pkey -in "$CA_DIR/ca.key" -pubout 2>/dev/null)" == \
    "$(openssl x509 -in "$TLS_DIR/ca.pem" -pubkey -noout 2>/dev/null)" ]] || return 1
  [[ "$(openssl pkey -in "$TLS_DIR/server.key" -pubout 2>/dev/null)" == \
    "$(openssl x509 -in "$TLS_DIR/server.pem" -pubkey -noout 2>/dev/null)" ]] || return 1
  openssl x509 -in "$TLS_DIR/server.pem" -noout -checkhost "$SERVER_HOST" >/dev/null 2>&1 || return 1
  openssl x509 -in "$TLS_DIR/server.pem" -noout -checkip 127.0.0.1 >/dev/null 2>&1 || return 1
}

if ! certificates_are_consistent; then
  work=$(mktemp -d "$TLS_DIR/.certgen.XXXXXX")
  ca_key=$(mktemp "$CA_DIR/.ca.key.XXXXXX")
  cleanup() {
    rm -rf "$work"
    rm -f "$ca_key"
  }
  trap cleanup EXIT

  openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
    -subj "/CN=Plenora $SERVER_HOST Test Root CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$ca_key" -out "$work/ca.pem"
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj "/CN=$SERVER_HOST" \
    -keyout "$work/server.key" -out "$work/server.csr"
  openssl x509 -req -sha256 -days 825 \
    -in "$work/server.csr" -CA "$work/ca.pem" -CAkey "$ca_key" \
    -CAcreateserial -extfile "$EXT_FILE" -out "$work/server.pem"

  openssl verify -CAfile "$work/ca.pem" "$work/server.pem"
  [[ "$(openssl pkey -in "$ca_key" -pubout)" == \
    "$(openssl x509 -in "$work/ca.pem" -pubkey -noout)" ]]
  [[ "$(openssl pkey -in "$work/server.key" -pubout)" == \
    "$(openssl x509 -in "$work/server.pem" -pubkey -noout)" ]]
  openssl x509 -in "$work/server.pem" -noout -checkhost "$SERVER_HOST"
  openssl x509 -in "$work/server.pem" -noout -checkip 127.0.0.1

  install -o 0 -g 0 -m 0444 "$work/ca.pem" "$TLS_DIR/ca.pem"
  install -o 999 -g 999 -m 0444 "$work/server.pem" "$TLS_DIR/server.pem"
  install -o 999 -g 999 -m 0400 "$work/server.key" "$TLS_DIR/server.key"
  install -o 0 -g 0 -m 0400 "$ca_key" "$CA_DIR/ca.key"
fi

chown 0:0 "$CA_DIR/ca.key" "$TLS_DIR/ca.pem"
chown 999:999 "$TLS_DIR/server.pem" "$TLS_DIR/server.key"
chmod 0400 "$CA_DIR/ca.key" "$TLS_DIR/server.key"
chmod 0444 "$TLS_DIR/ca.pem" "$TLS_DIR/server.pem"