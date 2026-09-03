#!/usr/bin/env bash
# Keep this fixture LF-only: it is executed inside Linux containers.
set -Eeuo pipefail

ca_dir=${1:?directory CA privata obbligatoria}
tls_dir=${2:?directory TLS obbligatoria}
pkcs12_password=${3:?password PKCS12 obbligatoria}

mkdir -p "${ca_dir}" "${tls_dir}"
work=$(mktemp -d "${tls_dir}/.certgen.XXXXXX")
cleanup() {
  rm -rf "${work}"
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -sha256 -days 30 -nodes \
  -subj '/CN=Plenora Oracle Test Root CA' \
  -addext 'basicConstraints=critical,CA:TRUE,pathlen:0' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -keyout "${ca_dir}/ca.key" -out "${work}/ca.pem"
openssl req -newkey rsa:2048 -sha256 -nodes \
  -subj '/CN=127.0.0.1' \
  -keyout "${work}/server.key" -out "${work}/server.csr"
printf '%s\n' \
  'subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:oracle' \
  'extendedKeyUsage=serverAuth' > "${work}/server.ext"
openssl x509 -req -sha256 -days 30 \
  -in "${work}/server.csr" \
  -CA "${work}/ca.pem" -CAkey "${ca_dir}/ca.key" -CAcreateserial \
  -extfile "${work}/server.ext" -out "${work}/server.pem"
openssl pkcs12 -export \
  -inkey "${work}/server.key" -in "${work}/server.pem" \
  -certfile "${work}/ca.pem" -name server \
  -passout "pass:${pkcs12_password}" -out "${work}/server.p12"

openssl verify -CAfile "${work}/ca.pem" "${work}/server.pem"
openssl x509 -in "${work}/server.pem" -noout -checkip 127.0.0.1
install -m 0444 "${work}/ca.pem" "${tls_dir}/ca.pem"
install -m 0444 "${work}/server.p12" "${tls_dir}/server.p12"
chmod 0400 "${ca_dir}/ca.key"
