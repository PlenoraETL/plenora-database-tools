#!/usr/bin/env bash
# Dalla radice del checkout: bash scripts/check_sospetti_vm.sh [all|db2|oracle|mysql|mariadb]
# I fixture restano disponibili per la diagnosi; nessun prune o cancellazione volumi.
set -euo pipefail
export CARGO_BUILD_JOBS=4
python3 - "${1:-all}" <<'PY'
import os
import re
import subprocess
import sys
from pathlib import Path
from scripts import check_db2_reference as db2
from scripts import check_oracle_reference as oracle
from scripts import check_mariadb_driver as fleet
from scripts.compose_network import compose_network_arguments, compose_volume, container_variable

root = Path.cwd()
assert (root / 'Cargo.toml').is_file(), 'Eseguire dalla radice del checkout'
selected = sys.argv[1]
assert selected in {'all', 'db2', 'oracle', 'mysql', 'mariadb'}
logs = root / 'target/sospetti-900/live'
logs.mkdir(parents=True, exist_ok=True)

def run(command, name, expected=None):
    # Nessun comando stampato: gli argomenti Docker possono contenere credenziali.
    print(name, flush=True)
    with (logs / (name + '.log')).open('w', encoding='utf-8') as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=os.environ.copy())
    text = (logs / (name + '.log')).read_text(encoding='utf-8', errors='replace')
    print(text, flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
    if expected is not None and not re.search(rf'test result: ok\. {expected} passed; 0 failed; 0 ignored;', text):
        raise SystemExit('Inventario eseguito incompleto: ' + name)

def up(compose, name, build=False):
    command = ['docker', 'compose', '-f', compose, 'up', '-d', '--wait', '--wait-timeout', '1200']
    if build:
        command.append('--build')
    run(command, name + '-health')

def cargo_args(crate):
    return ['cargo', 'test', '--locked', '-p', crate, '--lib', 'live_sospetti_', '--', '--ignored', '--show-output', '--test-threads=1']

if selected in {'all', 'db2'}:
    up('docker-compose.db2.yml', 'db2', True)
    command = ['docker', 'compose', '-f', 'docker-compose.db2.yml', 'exec', '-T']
    for key, value in {**db2.DB2_ENV, 'CARGO_BUILD_JOBS': '4'}.items():
        command += ['-e', key + '=' + value]
    run(command + ['db2', *cargo_args('plenora-db-db2')], 'db2', 3)

if selected in {'all', 'oracle'}:
    up('docker-compose.oracle.yml', 'oracle')
    reference = oracle.reference_contract()
    identity = oracle.container_identity(reference)
    environment, _ = oracle.test_environments(reference, identity['container_id'])
    command = ['docker', 'run', '--rm', '--network', 'host', '-v', str(root) + ':/workspace',
               '-v', 'sospetti900-oracle-target:/workspace/target', '-w', '/workspace',
               '-e', 'CARGO_BUILD_JOBS=4']
    for key, value in environment.items():
        command += ['-e', key + '=' + value]
    run(command + [fleet.RUST_IMAGE, *cargo_args('plenora-db-oracle')], 'oracle', 1)

if selected in {'all', 'mysql', 'mariadb'}:
    up('docker-compose.mysql.yml', 'mysql')
    if selected in {'all', 'mariadb'}:
        # Certificati coerenti: il certgen riscrive il volume a ogni up mentre i server
        # restano accesi con quelli vecchi (BadSignature). Si ricrea solo questa fixture.
        run(['docker', 'compose', '-f', 'docker-compose.mariadb.yml', 'down', '--volumes'], 'mariadb-down')
        up('docker-compose.mariadb.yml', 'mariadb')
    for server in fleet.servers():
        product = 'mysql' if server.key == 'mysql' else 'mariadb'
        if selected not in {'all', product}:
            continue
        prefix = 'MYSQL' if product == 'mysql' else 'MARIADB'
        # Il login non prova i privilegi: verifica anche lo schema e le temporanee.
        # Su volumi gia inizializzati l'entrypoint non riesegue gli script init.
        client = 'mysql' if product == 'mysql' else 'mariadb'
        database = container_variable(server.container, prefix + '_DATABASE')
        if database != 'dataflow_test':
            raise SystemExit('Database fixture diverso da quello della GRANT minima')
        tls = ['--ssl-ca=/etc/mysql/tls/ca.pem',
               '--ssl-mode=VERIFY_IDENTITY' if product == 'mysql' else '--ssl-verify-server-cert']
        login = ['docker', 'exec', '-e', 'MYSQL_PWD=Minimal_Test_2026!', server.container,
                 client, *tls, '-uplenora_minimal', '--protocol=TCP', '-h' + server.container,
                 '--database=' + database, '-e',
                 'CREATE TEMPORARY TABLE pln_minimal_preflight(id INT); '
                 'DROP TEMPORARY TABLE pln_minimal_preflight']
        if subprocess.run(login, capture_output=True).returncode:
            output = subprocess.run(['docker', 'logs', server.container], capture_output=True,
                                    text=True, check=True)
            found = re.search(r'GENERATED ROOT PASSWORD: (\S+)', output.stdout + output.stderr)
            if not found:
                raise SystemExit('Credenziale root fixture non disponibile per account minimo')
            sql = (root / 'docker' / product / 'init/002-minimal-privileges.sql').read_text()
            setup = subprocess.run(['docker', 'exec', '-i', '-e', 'MYSQL_PWD=' + found[1],
                                    server.container, client, '-uroot'], input=sql,
                                   capture_output=True, text=True)
            if setup.returncode:
                raise SystemExit('Creazione account minimo fixture fallita')
            if subprocess.run(login, capture_output=True).returncode:
                raise SystemExit('Privilegi account minimo fixture insufficienti dopo la GRANT')

        environment = {
            'PLENORA_MYSQL_PRODUCT': product, 'PLENORA_MYSQL_HOST': server.container,
            'PLENORA_MYSQL_PORT': '3306', 'PLENORA_MYSQL_CA': '/tls/ca.pem',
            'PLENORA_MYSQL_DATABASE': database,
            'PLENORA_MYSQL_USER': container_variable(server.container, prefix + '_USER'),
            'PLENORA_MYSQL_PASSWORD': container_variable(server.container, server.password_variable),
            'CARGO_BUILD_JOBS': '4',
        }
        command = ['docker', 'run', '--rm', *compose_network_arguments(server.container),
                   '-v', str(root) + ':/workspace',
                   '-v', compose_volume(server.container, '/etc/mysql/tls') + ':/tls:ro',
                   '-v', 'sospetti900-mysql-target:/workspace/target', '-w', '/workspace']
        for key, value in environment.items():
            command += ['-e', key + '=' + value]
        run(command + [fleet.RUST_IMAGE, *cargo_args('plenora-db-mysql')], server.key, 10)
PY
