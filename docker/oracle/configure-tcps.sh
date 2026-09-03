#!/usr/bin/env bash
set -Eeuo pipefail

wallet_dir=/opt/oracle/oradata/plenora-tcps-wallet
certificate_dir=/opt/oracle/tcps-certificates
wallet_password=PlenoraWallet2026
pkcs12_password=PlenoraPkcs122026
network_admin="${ORACLE_BASE_HOME}/network/admin"

mkdir -p "${wallet_dir}"
if [[ ! -f "${wallet_dir}/cwallet.sso" ]]; then
  orapki wallet create -wallet "${wallet_dir}" -pwd "${wallet_password}" -auto_login
  orapki wallet import_pkcs12 \
    -wallet "${wallet_dir}" \
    -pwd "${wallet_password}" \
    -pkcs12file "${certificate_dir}/server.p12" \
    -pkcs12pwd "${pkcs12_password}"
fi

cat > "${network_admin}/listener.ora" <<EOF
WALLET_LOCATION=(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=${wallet_dir})))
TLS_CLIENT_AUTHENTICATION=FALSE
USE_SNI_LISTENER=ON
LISTENER=(DESCRIPTION_LIST=(DESCRIPTION=(ADDRESS=(PROTOCOL=IPC)(KEY=EXTPROC_FOR_FREE)))(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=0.0.0.0)(PORT=1521)))(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=0.0.0.0)(PORT=2484))))
DEDICATED_THROUGH_BROKER_LISTENER=ON
DIAG_ADR_ENABLED=off
EOF

# Il listener termina il primo handshake, ma anche il processo server che
# riceve la sessione TCPS deve poter aprire lo stesso wallet auto-login.
cat >> "${network_admin}/sqlnet.ora" <<EOF
WALLET_LOCATION=(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=${wallet_dir})))
TLS_CLIENT_AUTHENTICATION=FALSE
EOF

lsnrctl stop
lsnrctl start
