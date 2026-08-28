#!/bin/sh
# Un solo parser costruisce la connection string: il comando inline nel YAML
# perdeva il nome driver con spazi attraversando Compose e CMD-SHELL.
set -eu

test -f /tmp/plenora-fixture-ready
test -n "${DB2INST1_PASSWORD:-}"

exec timeout 15s isql -b -k \
    "DRIVER={IBM DB2 ODBC DRIVER};DATABASE=plenora;HOSTNAME=127.0.0.1;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=${DB2INST1_PASSWORD};"
