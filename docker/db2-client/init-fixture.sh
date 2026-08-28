#!/usr/bin/env bash
set -euo pipefail

rm -f /tmp/plenora-fixture-ready

schema_count="$(
    su - db2inst1 -c ". ~/sqllib/db2profile && db2 connect to plenora >/dev/null && db2 -x \"SELECT COUNT(*) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = 'PLENORA_TEST'\""
)"
schema_count="${schema_count//[[:space:]]/}"

if [[ "${schema_count}" == "1" ]]; then
    # L'inventario e intenzionalmente esplicito. `DROP SCHEMA ... RESTRICT`
    # rifiuta oggetti inattesi invece di cancellare dati che la fixture non ha
    # dichiarato; ADMIN_DROP_SCHEMA richiederebbe SYSTOOLSPACE, assente
    # nell'immagine Community di riferimento.
    su - db2inst1 -c ". ~/sqllib/db2profile && db2 connect to plenora >/dev/null && db2 -stvf /opt/plenora-fixture/drop-fixture.sql"
elif [[ "${schema_count}" != "0" ]]; then
    echo "conteggio schema PLENORA_TEST inatteso: ${schema_count}" >&2
    exit 1
fi

su - db2inst1 -c ". ~/sqllib/db2profile && db2 connect to plenora >/dev/null && db2 -stvf /opt/plenora-fixture/fixture.sql"
touch /tmp/plenora-fixture-ready
