#!/bin/sh
# Rende riavviabile la fixture con gli stessi volumi. L'entrypoint IBM esporta
# la configurazione come UID 1000, ma un volume ricreato da Docker puo lasciare
# `/database/config` a root:root e bloccare il setup prima dell'healthcheck.
set -eu

mkdir -p /database/config
chown -R 1000:1000 /database/config

exec /var/db2_setup/lib/setup_db2_instance.sh "$@"
