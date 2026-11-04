#!/usr/bin/env bash
# Dalla radice del checkout: bash scripts/check_audit900_vm.sh [all|postgres|mysql|mariadb|sqlserver|oracle|db2] [all|nul]
# Le fixture restano disponibili; MariaDB riallinea i server ai certificati TLS.
set -euo pipefail
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4}
if [[ ${1:-all} == all ]]; then
    failed=0
    for provider in postgres mysql mariadb sqlserver oracle db2; do
        bash "$0" "$provider" "${2:-all}" || failed=1
    done
    exit "$failed"
fi
python3 - "${1:-all}" "${2:-all}" <<'PY'
import os
import re
import subprocess
import sys
from pathlib import Path
from scripts import check_postgres_reference as postgres
from scripts import check_sqlserver_reference as sqlserver
from scripts import check_db2_reference as db2
from scripts import check_oracle_reference as oracle
from scripts import check_mariadb_driver as fleet
from scripts.compose_network import compose_network_arguments, compose_volume, container_variable

root = Path.cwd()
assert (root / 'Cargo.toml').is_file(), 'Eseguire dalla radice del checkout'
selected = sys.argv[1]
selection = sys.argv[2]
if selection not in {"all", "nul"}:
    raise SystemExit("Filtro sconosciuto")
if selected not in {'postgres', 'mysql', 'mariadb', 'sqlserver', 'oracle', 'db2'}:
    raise SystemExit('Provider sconosciuto')
logs = root / 'target/audit-900' / ('live-nul' if selection == 'nul' else 'live')
logs.mkdir(parents=True, exist_ok=True)
results = []
# Non lasciare che un log di una run precedente sembri una misura corrente.
for old in logs.glob(selected + '-*.log'):
    old.unlink()


def run(command, name, expected=None):
    # Nessun comando stampato: gli argomenti Docker possono contenere credenziali.
    print(name, flush=True)
    with (logs / (name + '.log')).open('w', encoding='utf-8') as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=os.environ.copy())
    text = (logs / (name + '.log')).read_text(encoding='utf-8', errors='replace')
    print(text, flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
    if expected is not None and re.findall(r'^test result: ok\. (\d+) passed; 0 failed; 0 ignored;', text, re.MULTILINE) != [str(expected)]:
        raise SystemExit('Inventario eseguito incompleto: ' + name)

def up(compose, name, build=False):
    command = ['docker', 'compose', '-f', compose, 'up', '-d', '--wait', '--wait-timeout', '1200']
    if build:
        command.append('--build')
    run(command, name + '-health')

def cargo_args(crate, test_filter):
    return ['cargo', 'test', '--locked', '-p', crate, '--lib', test_filter, '--', '--include-ignored', '--show-output', '--test-threads=1']

def run_case(command, name, expected):
    try:
        run(command, name, expected)
    except SystemExit:
        results.append((name, 'FAIL'))
    except Exception:
        results.append((name, 'ERROR'))
    else:
        results.append((name, 'PASS'))


def run_tests(command, crate, name, selected, selection):
    for test_filter, expected in test_cases(selected, selection):
        run_case(command + cargo_args(crate, test_filter), name + '-' + test_filter, expected)

def test_cases(selected, selection):
    if selection == 'nul':
        return [('live_audit900_nul', 1)]
    cases = [('live_parameter_values_bind_exactly_or_are_refused', 1), ('live_audit900_nul', 1)]
    if selected == 'postgres':
        cases.extend((name, 1) for name in [
            'live_audit900_l3_typed_wkb', 'live_audit900_l5_comment_search_path',
            'live_audit900_l6_name_bytes'])
    if selected in {'mysql', 'mariadb'}:
        cases.extend((name, 1) for name in [
            'live_audit900_l2_typed_json', 'live_audit900_l4_found_rows'])
    if selected in {'postgres', 'mysql', 'sqlserver'}:
        cases.append(('live_audit900_lb_srid_before_io', 1))
    if selected == 'sqlserver':
        cases.append(('live_audit900_typed_wkb_positive', 1))
    if selected == 'mariadb':
        cases.extend((name, 1) for name in [
            'live_audit900_la_json_read', 'live_audit900_lc_json_arrow_key',
            'live_audit900_ld_view_without_show_view'])
    return cases

try:
    if selected == 'postgres':
        up('docker-compose.postgres.yml', 'postgres')
        for test_filter, expected in test_cases(selected, selection):
            command = postgres.cargo(cargo_args('plenora-db-postgres', test_filter)[1:], postgres.DEFAULT_DSN)
            command[3:3] = ['-e', 'CARGO_BUILD_JOBS=' + os.environ['CARGO_BUILD_JOBS']]
            run_case(command, 'postgres-' + test_filter, expected)

    if selected == 'sqlserver':
        up('docker-compose.sqlserver.yml', 'sqlserver')
        run(['docker', 'wait', 'dataflow-sqlserver-init'], 'sqlserver-init')
        if (logs / 'sqlserver-init.log').read_text().strip() != '0':
            raise SystemExit('Inizializzazione SQL Server fallita')
        sqlserver.materialize_private_ca()
        sqlserver.wait_for_application_database()
        # Il runner VM usa sempre Cargo nel container, anche con un ambiente host preesistente.
        os.environ.pop('PLENORA_SQLSERVER_GATE_HOST_CARGO', None)
        for test_filter, expected in test_cases(selected, selection):
            command, _ = sqlserver.cargo(cargo_args('plenora-db-sqlserver', test_filter)[1:])
            command[3:3] = ['-e', 'CARGO_BUILD_JOBS=' + os.environ['CARGO_BUILD_JOBS']]
            run_case(command, 'sqlserver-' + test_filter, expected)

    if selected == 'db2':
        up('docker-compose.db2.yml', 'db2', True)
        command = ['docker', 'compose', '-f', 'docker-compose.db2.yml', 'exec', '-T']
        for key, value in {**db2.DB2_ENV, 'CARGO_BUILD_JOBS': os.environ['CARGO_BUILD_JOBS']}.items():
            command += ['-e', key + '=' + value]
        run_tests(command + ['db2'], 'plenora-db-db2', 'db2', selected, selection)

    if selected == 'oracle':
        up('docker-compose.oracle.yml', 'oracle')
        reference = oracle.reference_contract()
        identity = oracle.container_identity(reference)
        environment, _ = oracle.test_environments(reference, identity['container_id'])
        command = ['docker', 'run', '--rm', '--network', 'host', '-v', str(root) + ':/workspace',
                   '-v', 'audit900-oracle-target:/workspace/target', '-w', '/workspace',
                   '-e', 'CARGO_BUILD_JOBS=' + os.environ['CARGO_BUILD_JOBS']]
        for key, value in environment.items():
            command += ['-e', key + '=' + value]
        run_tests(command + [fleet.RUST_IMAGE], 'plenora-db-oracle', 'oracle', selected, selection)

    if selected in {'mysql', 'mariadb'}:
        if selected == 'mysql':
            up('docker-compose.mysql.yml', 'mysql')
        if selected == 'mariadb':
            # CA private isolate per riferimento; ricarica anche i certificati
            # eventualmente rigenerati, conservando tutti i volumi dati.
            run(['docker', 'compose', '-f', 'docker-compose.mariadb.yml',
                 'up', '-d', '--force-recreate', '--wait', '--wait-timeout', '1200'],
                'mariadb-health')
        for server in fleet.servers():
            product = 'mysql' if server.key == 'mysql' else 'mariadb'
            if selected != product:
                continue
            prefix = 'MYSQL' if product == 'mysql' else 'MARIADB'
            database = container_variable(server.container, prefix + '_DATABASE')
            environment = {
                'PLENORA_MYSQL_PRODUCT': product, 'PLENORA_MYSQL_HOST': server.container,
                'PLENORA_MYSQL_PORT': '3306', 'PLENORA_MYSQL_CA': '/tls/ca.pem',
                'PLENORA_MYSQL_DATABASE': database,
                'PLENORA_MYSQL_USER': container_variable(server.container, prefix + '_USER'),
                'PLENORA_MYSQL_PASSWORD': container_variable(server.container, server.password_variable),
                'CARGO_BUILD_JOBS': os.environ['CARGO_BUILD_JOBS'],
            }
            command = ['docker', 'run', '--rm', *compose_network_arguments(server.container),
                       '-v', str(root) + ':/workspace',
                       '-v', compose_volume(server.container, '/etc/mysql/tls') + ':/tls:ro',
                       '-v', 'audit900-mysql-target:/workspace/target', '-w', '/workspace']
            for key, value in environment.items():
                command += ['-e', key + '=' + value]
            run_tests(command + [fleet.RUST_IMAGE], 'plenora-db-mysql', server.key, selected, selection)
except Exception:
    raise SystemExit('Preparazione o esecuzione fixture fallita (dettagli sensibili omessi)') from None

finally:
    summary = '\n'.join(name + ': ' + outcome for name, outcome in results)
    (logs / (selected + '-summary.log')).write_text(summary + '\n', encoding='utf-8')
    print(summary, flush=True)
if not results or any(outcome != 'PASS' for _, outcome in results):
    raise SystemExit(1)

PY
