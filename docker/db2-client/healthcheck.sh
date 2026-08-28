#!/bin/sh
# La salute segue lo stato persistente del fixture, non un marker in `/tmp` che
# sparisce al riavvio mentre Db2 conserva il database sul volume.
set -eu

test -n "${DB2INST1_PASSWORD:-}"

if ! fixture_tables="$(
    su - db2inst1 -c '. ~/sqllib/db2profile && db2 connect to plenora >/dev/null && db2 -x "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = '\''PLENORA_TEST'\'' AND TABNAME = '\''SPATIAL_PROBE'\''"' 2>/dev/null
)"; then
    echo "probe catalogo fixture Db2 fallita" >&2
    exit 1
fi
fixture_tables="$(printf '%s' "${fixture_tables}" | tr -d '[:space:]')"
if test "${fixture_tables}" != "1"; then
    echo "oggetto finale del fixture Db2 non disponibile" >&2
    exit 1
fi

if ! timeout 15s isql -b -k \
    "DRIVER={IBM DB2 ODBC DRIVER};DATABASE=plenora;HOSTNAME=127.0.0.1;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=${DB2INST1_PASSWORD};" \
    >/dev/null; then
    echo "probe ODBC fixture Db2 fallita" >&2
    exit 1
fi
