#!/usr/bin/env bash
set -Eeuo pipefail

wallet_dir=/opt/oracle/oradata/plenora-tcps-wallet
certificate_dir=/opt/oracle/oradata/plenora-tcps-certificates
wallet_password=PlenoraWallet2026
pkcs12_password=PlenoraPkcs122026
network_admin="${ORACLE_BASE_HOME}/network/admin"

mkdir -p "${wallet_dir}" "${certificate_dir}"
if [[ ! -f "${wallet_dir}/cwallet.sso" ]]; then
  openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 30 \
    -subj '/CN=Plenora Oracle Test CA' \
    -keyout "${certificate_dir}/ca.key" \
    -out "${certificate_dir}/ca.pem"
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj '/CN=127.0.0.1' \
    -keyout "${certificate_dir}/server.key" \
    -out "${certificate_dir}/server.csr"
  openssl x509 -req -sha256 -days 30 \
    -in "${certificate_dir}/server.csr" \
    -CA "${certificate_dir}/ca.pem" \
    -CAkey "${certificate_dir}/ca.key" \
    -CAcreateserial \
    -extfile <(printf 'subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:oracle\nextendedKeyUsage=serverAuth\n') \
    -out "${certificate_dir}/server.pem"
  openssl pkcs12 -export \
    -inkey "${certificate_dir}/server.key" \
    -in "${certificate_dir}/server.pem" \
    -certfile "${certificate_dir}/ca.pem" \
    -passout "pass:${pkcs12_password}" \
    -out "${certificate_dir}/server.p12"
  orapki wallet create -wallet "${wallet_dir}" -pwd "${wallet_password}" -auto_login
  orapki wallet import_pkcs12 \
    -wallet "${wallet_dir}" \
    -pwd "${wallet_password}" \
    -pkcs12file "${certificate_dir}/server.p12" \
    -pkcs12pwd "${pkcs12_password}"
  chmod 600 "${certificate_dir}/ca.key" "${certificate_dir}/server.key"
fi

cat > "${network_admin}/listener.ora" <<EOF
WALLET_LOCATION=(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=${wallet_dir})))
TLS_CLIENT_AUTHENTICATION=FALSE
LISTENER=(DESCRIPTION_LIST=(DESCRIPTION=(ADDRESS=(PROTOCOL=IPC)(KEY=EXTPROC_FOR_FREE)))(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=0.0.0.0)(PORT=1521)))(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=0.0.0.0)(PORT=2484))))
DEDICATED_THROUGH_BROKER_LISTENER=ON
DIAG_ADR_ENABLED=off
EOF

lsnrctl stop
lsnrctl start
