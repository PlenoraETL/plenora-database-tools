#!/usr/bin/env bash
# Dalla radice del checkout, con fixture isolati gia healthy e ambiente dei
# runner di riferimento (docs/operativo.md). Nessun prune, volume o worktree.
# AUDIT900_ENV_DIR puo contenere postgres.env, mysql.env, mariadb.env,
# sqlserver.env, oracle.env, db2.env: configurazioni locali, mai nei log.
set -euo pipefail
export CARGO_BUILD_JOBS=6
test -f Cargo.toml
mkdir -p target/sospetti-900/live
run_test() {
    local crate=$1 filter=$2 expected=$3
    local log="target/sospetti-900/live/${provider}-${filter}.log"
    cargo test --locked -p "$crate" --lib "$filter" -- \
        --include-ignored --show-output --test-threads=1 >"$log" 2>&1 || {
        cat "$log"; return 1;
    }
    cat "$log"
    grep -Eq "test result: ok\. ${expected} passed; 0 failed; 0 ignored;" "$log"
}
run_provider() (
    provider=$1
    if [[ -n ${AUDIT900_ENV_DIR:-} ]]; then
        # File di configurazione scelto esplicitamente dall'operatore VM.
        set -a
        source "$AUDIT900_ENV_DIR/$provider.env"
        set +a
    fi
    case "$provider" in
        postgres)
            : "${PLENORA_TEST_POSTGRES_DSN:?DSN fixture PostgreSQL richiesta}"
            export PLENORA_REQUIRE_LIVE_POSTGRES=1
            crate=plenora-db-postgres
            ;;
        mysql|mariadb)
            : "${PLENORA_MYSQL_HOST:?host fixture richiesto}"
            : "${PLENORA_MYSQL_CA:?CA fixture richiesta}"
            : "${PLENORA_MYSQL_PASSWORD:?password fixture richiesta}"
            export PLENORA_MYSQL_PRODUCT=$provider
            crate=plenora-db-mysql
            ;;
        sqlserver)
            : "${PLENORA_SQLSERVER_HOST:?host fixture richiesto}"
            : "${PLENORA_SQLSERVER_PASSWORD:?password fixture richiesta}"
            crate=plenora-db-sqlserver
            ;;
        oracle)
            : "${PLENORA_ORACLE_HOST:?host fixture richiesto}"
            : "${PLENORA_ORACLE_PASSWORD:?password fixture richiesta}"
            crate=plenora-db-oracle
            ;;
        db2)
            : "${PLENORA_DB2_HOST:?host fixture richiesto}"
            : "${PLENORA_DB2_PASSWORD:?password fixture richiesta}"
            # Eseguire dove il driver ODBC IBM della fixture e installato.
            crate=plenora-db-db2
            ;;
        *) printf 'Provider sconosciuto: %s\n' "$provider" >&2; exit 2 ;;
    esac
    run_test "$crate" live_parameter_values_bind_exactly_or_are_refused 1
    case "$provider" in
        postgres) run_test "$crate" live_audit900_ 3 ;;
        mysql|mariadb) run_test "$crate" live_audit900_ 2 ;;
    esac
)
if [[ ${1:-all} == all ]]; then
    failed=0
    for provider in postgres mysql mariadb sqlserver oracle db2; do
        # Non interrompe la raccolta degli altri provider dopo un fallimento.
        bash "$0" "$provider" || failed=1
    done
    exit "$failed"
fi
run_provider "$1"
