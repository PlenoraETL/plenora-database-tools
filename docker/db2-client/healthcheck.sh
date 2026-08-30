#!/bin/sh
# Il marker appartiene al solo startup corrente; l'entrypoint lo rimuove prima
# che il setup IBM riesegua gli script custom. L'inventario sotto verifica poi
# che il fixture persistente sia davvero completo.
set -eu

test -n "${DB2INST1_PASSWORD:-}"
test -f /run/plenora-fixture-ready

if ! fixture_objects="$(
    su - db2inst1 -c '. ~/sqllib/db2profile && db2 connect to plenora >/dev/null && db2 -x "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = '\''PLENORA_TEST'\'' AND ((TYPE = '\''T'\'' AND TABNAME IN ('\''CATALOG_PROBE'\'', '\''READ_PROBE'\'', '\''TX_PROBE'\'', '\''WRITE_PROBE'\'', '\''SPATIAL_PROBE'\'')) OR (TYPE = '\''V'\'' AND TABNAME = '\''CATALOG_PROBE_VIEW'\''))"' 2>/dev/null
)"; then
    echo "probe catalogo fixture Db2 fallita" >&2
    exit 1
fi
if ! schema_objects="$(
    su - db2inst1 -c '. ~/sqllib/db2profile && db2 connect to plenora >/dev/null && db2 -x "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = '\''PLENORA_TEST'\''"' 2>/dev/null
)"; then
    echo "probe inventario schema Db2 fallita" >&2
    exit 1
fi
fixture_objects="$(printf '%s' "${fixture_objects}" | tr -d '[:space:]')"
schema_objects="$(printf '%s' "${schema_objects}" | tr -d '[:space:]')"
if test "${fixture_objects}" != "6" || test "${schema_objects}" != "6"; then
    echo "inventario fixture Db2 incompleto" >&2
    exit 1
fi

if ! timeout 15s isql -b -k \
    "DRIVER={IBM DB2 ODBC DRIVER};DATABASE=plenora;HOSTNAME=127.0.0.1;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=${DB2INST1_PASSWORD};" \
    >/dev/null; then
    echo "probe ODBC fixture Db2 fallita" >&2
    exit 1
fi
