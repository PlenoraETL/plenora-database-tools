#!/usr/bin/env python3
"""Qualifica l'intera matrice MySQL su immagini fissate per digest.

Versioni e digest arrivano da `docker/mysql/references.json`, unica fonte di
verita: la baseline 9.x e i riferimenti di compatibilita 8.4 LTS e 8.0 sono
qualificati con la stessa fixture — TLS obbligatorio con CA privata,
`local_infile` disattivato e `sql_mode` stretta. Per ogni riferimento la
matrice esegue l'inventario live intero, serializzato, senza esclusioni: una
semantica che una versione non regge resta un blocco dichiarato, non un test
indebolito.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Il gate viene invocato sia come modulo del pacchetto sia come script: la
# radice del repository deve restare importabile in entrambi i casi.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_mysql_reference import (  # noqa: E402
    EXPECTED_LIVE_DEFAULT_TESTS,
    EXPECTED_LIVE_REFERENCE_TESTS,
    EXPECTED_UNIT_TESTS,
    validate_inventory,
)
from scripts.mysql_references import REFERENCES, MysqlReference  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CHECKER_SOURCE = Path(__file__).resolve()
NETWORK = "plenora-mysql-matrix"
RUST_IMAGE = "rust:1.92"
DATABASE = "dataflow_test"
USER = "dataflow"
TLS_FIXTURE = ROOT / "docker" / "mysql" / "tls"
INIT_FIXTURE = ROOT / "docker" / "mysql" / "init"
SQL_MODE = "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
PASSING_TEST = re.compile(r"^test live_tests::([^ ]+) \.\.\. ok$", re.MULTILINE)


MatrixEntry = MysqlReference
MATRIX = REFERENCES


def server_command(entry: MatrixEntry) -> list[str]:
    """Le stesse difese del riferimento fissato, per ogni versione."""
    del entry
    return [
        "--require-secure-transport=ON",
        "--local-infile=OFF",
        f"--sql-mode={SQL_MODE}",
        "--ssl-ca=/etc/mysql/tls/ca.pem",
        "--ssl-cert=/etc/mysql/tls/server.pem",
        "--ssl-key=/etc/mysql/tls/server.key",
    ]


def live_default_command() -> list[str]:
    """I test live non `#[ignore]`, un test alla volta."""
    return [
        "cargo",
        "test",
        "-p",
        "plenora-db-mysql",
        "live_",
        "--locked",
        "--",
        "--nocapture",
        "--test-threads=1",
    ]


def live_reference_command() -> list[str]:
    """L'inventario live `#[ignore]` intero: nessuna esclusione."""
    return [
        "cargo",
        "test",
        "-p",
        "plenora-db-mysql",
        "live_",
        "--locked",
        "--",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]


def live_inventory(output: str) -> set[str]:
    return {f"live_tests::{name}" for name in PASSING_TEST.findall(output)}


def verify_live_inventory(
    entry: MatrixEntry, family: str, expected: set[str], output: str
) -> None:
    executed = live_inventory(output)
    if executed != expected:
        missing = sorted(expected - executed)
        unexpected = sorted(executed - expected)
        raise RuntimeError(
            f"inventario {family} {entry.label} inatteso: "
            f"eseguiti {len(executed)}, attesi {len(expected)}, "
            f"mancanti={missing}, inattesi={unexpected}"
        )


def verify_candidate_unit() -> None:
    output = run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{ROOT}:/workspace",
            "-v",
            "plenora-cargo-registry:/usr/local/cargo/registry",
            "-v",
            "plenora-cargo-git:/usr/local/cargo/git",
            "-w",
            "/workspace",
            RUST_IMAGE,
            "cargo",
            "test",
            "-p",
            "plenora-db-mysql",
            "--locked",
            "--",
            "--skip",
            "live_",
        ],
        timeout=1800,
        capture=True,
    )
    executed = set(re.findall(r"^test ([^ ]+) \.\.\. ok$", output, re.MULTILINE))
    if executed != EXPECTED_UNIT_TESTS:
        missing = sorted(EXPECTED_UNIT_TESTS - executed)
        unexpected = sorted(executed - EXPECTED_UNIT_TESTS)
        raise RuntimeError(
            "inventario unit candidato MySQL inatteso: "
            f"mancanti={missing}, inattesi={unexpected}"
        )


def verify_hardening(entry: MatrixEntry, probe: dict[str, str]) -> None:
    version = probe.get("version", "")
    if version != entry.exact_version:
        raise RuntimeError(f"versione inattesa per {entry.label}: {version}")
    if probe.get("require_secure_transport") != "ON":
        raise RuntimeError(f"{entry.label} non impone il trasporto sicuro")
    if probe.get("local_infile") != "OFF":
        raise RuntimeError(f"{entry.label} espone LOCAL INFILE")
    tls_version = probe.get("tls_version", "")
    if not tls_version.startswith("TLSv1."):
        raise RuntimeError(f"{entry.label} senza sessione TLS osservabile")


def entry_report(entry: MatrixEntry, probe: dict[str, str]) -> dict[str, Any]:
    return {
        "label": entry.label,
        "role": entry.role,
        "expected_version": entry.exact_version,
        "image": entry.image,
        "product_version": probe["version"],
        "live_default_tests": {
            "expected": len(EXPECTED_LIVE_DEFAULT_TESTS),
            "passed": len(EXPECTED_LIVE_DEFAULT_TESTS),
        },
        "live_reference_tests": {
            "expected": len(EXPECTED_LIVE_REFERENCE_TESTS),
            "passed": len(EXPECTED_LIVE_REFERENCE_TESTS),
        },
        "hardening": {
            "require_secure_transport": probe["require_secure_transport"],
            "local_infile": probe["local_infile"],
            "tls_version": probe["tls_version"],
        },
    }


# --- orchestrazione Docker ------------------------------------------------


def run(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
    timeout: int = 900,
    capture: bool = False,
) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=False,
        text=True,
        capture_output=capture,
        timeout=timeout,
    )
    if capture:
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
    if completed.returncode:
        raise RuntimeError(f"comando fallito: {command[0]} {command[1:3]}")
    return completed.stdout if capture else ""


def quiet(command: list[str]) -> int:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=120,
    ).returncode


def ensure_network() -> None:
    if quiet(["docker", "network", "inspect", NETWORK]):
        run(["docker", "network", "create", NETWORK], timeout=60)


def generate_tls(entry: MatrixEntry) -> None:
    run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0:0",
            "--entrypoint",
            "/bin/bash",
            "-v",
            f"{entry.ca_volume}:/ca",
            "-v",
            f"{entry.tls_volume}:/tls",
            "-v",
            f"{TLS_FIXTURE}:/fixture:ro",
            entry.image,
            "/fixture/generate.sh",
            "/ca",
            "/tls",
            "/fixture/server.ext",
        ],
        timeout=300,
    )


def start(entry: MatrixEntry, environment: dict[str, str]) -> None:
    command = [
        "docker",
        "run",
        "-d",
        "--name",
        entry.container,
        "--network",
        NETWORK,
    ]
    for alias in entry.aliases:
        command.extend(["--network-alias", alias])
    command.extend(
        [
            "-e",
            "MYSQL_RANDOM_ROOT_PASSWORD=yes",
            "-e",
            f"MYSQL_DATABASE={DATABASE}",
            "-e",
            f"MYSQL_USER={USER}",
            # La password resta nell'ambiente del processo, mai negli argomenti.
            "-e",
            "MYSQL_PASSWORD",
            "-v",
            f"{entry.tls_volume}:/etc/mysql/tls:ro",
            "-v",
            f"{INIT_FIXTURE}:/docker-entrypoint-initdb.d:ro",
            entry.image,
            *server_command(entry),
        ]
    )
    run(command, environment=environment, timeout=300)


def mysql_value(entry: MatrixEntry, statement: str) -> tuple[int, str]:
    """Esegue una probe SQL nel container e riporta codice e stdout.

    Su errore lo stderr del client finisce sullo stderr del gate: una probe
    che fallisce deve dire perche, non solo che e fallita.
    """

    completed = subprocess.run(
        [
            "docker",
            "exec",
            entry.container,
            "/bin/sh",
            "-c",
            # TCP, non socket: durante il bootstrap l'entrypoint MySQL
            # avvia un server temporaneo raggiungibile solo dal socket. Una
            # probe sul socket puo quindi rispondere prima che il server
            # definitivo esista, e la verifica successiva trova il vuoto.
            'exec env MYSQL_PWD="$MYSQL_PASSWORD" mysql -Nse "$1" '
            f"-u {USER} -h 127.0.0.1 --protocol=TCP --ssl-mode=REQUIRED",
            "mysql-matrix-probe",
            statement,
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=60,
    )
    if completed.returncode:
        sys.stderr.write(completed.stderr)
    return completed.returncode, completed.stdout.strip()


def wait_ready(entry: MatrixEntry) -> None:
    deadline = time.monotonic() + 420
    while time.monotonic() < deadline:
        code, _ = mysql_value(entry, "SELECT 1")
        if not code:
            return
        time.sleep(3)
    raise RuntimeError(f"timeout avvio {entry.label}")


def probe_server(entry: MatrixEntry) -> dict[str, str]:
    code, combined = mysql_value(
        entry,
        "SELECT CONCAT_WS('|', VERSION(), "
        "IF(@@global.require_secure_transport, 'ON', 'OFF'), "
        "IF(@@global.local_infile, 'ON', 'OFF'))",
    )
    parts = combined.split("|")
    if code or len(parts) != 3:
        raise RuntimeError(f"probe indurimento {entry.label} fallita")
    code, status = mysql_value(entry, "SHOW SESSION STATUS LIKE 'Ssl_version'")
    if code:
        raise RuntimeError(f"probe TLS {entry.label} fallita")
    return {
        "version": parts[0],
        "require_secure_transport": parts[1],
        "local_infile": parts[2],
        "tls_version": status.split("\t")[-1].strip(),
    }


def run_suite(
    entry: MatrixEntry, environment: dict[str, str], suite: list[str]
) -> str:
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        NETWORK,
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        "plenora-cargo-registry:/usr/local/cargo/registry",
        "-v",
        "plenora-cargo-git:/usr/local/cargo/git",
        "-v",
        f"{entry.tls_volume}:/mysql-tls:ro",
        "-w",
        "/workspace",
        "-e",
        "PLENORA_MYSQL_HOST=dataflow-mysql",
        "-e",
        f"PLENORA_MYSQL_DATABASE={DATABASE}",
        "-e",
        f"PLENORA_MYSQL_USER={USER}",
        "-e",
        "PLENORA_MYSQL_PASSWORD",
        "-e",
        "PLENORA_MYSQL_CA=/mysql-tls/ca.pem",
        "-e",
        f"PLENORA_MYSQL_EXPECTED_VERSION={entry.version_prefix}",
        RUST_IMAGE,
        *suite,
    ]
    return run(command, environment=environment, timeout=3600, capture=True)


def discard(entry: MatrixEntry) -> None:
    quiet(["docker", "rm", "--force", "--volumes", entry.container])
    quiet(["docker", "volume", "rm", "--force", entry.tls_volume])
    quiet(["docker", "volume", "rm", "--force", entry.ca_volume])


def qualify(entry: MatrixEntry, environment: dict[str, str]) -> dict[str, Any]:
    discard(entry)
    try:
        generate_tls(entry)
        start(entry, environment)
        wait_ready(entry)
        probe = probe_server(entry)
        verify_hardening(entry, probe)
        verify_live_inventory(
            entry,
            "live default",
            EXPECTED_LIVE_DEFAULT_TESTS,
            run_suite(entry, environment, live_default_command()),
        )
        verify_live_inventory(
            entry,
            "live reference",
            EXPECTED_LIVE_REFERENCE_TESTS,
            run_suite(entry, environment, live_reference_command()),
        )
        return entry_report(entry, probe)
    finally:
        discard(entry)


def main() -> int:
    password = secrets.token_urlsafe(24)
    environment = os.environ.copy()
    environment["MYSQL_PASSWORD"] = password
    environment["PLENORA_MYSQL_PASSWORD"] = password
    results: list[dict[str, Any]] = []
    try:
        validate_inventory()
        verify_candidate_unit()
        ensure_network()
        for entry in MATRIX:
            results.append(qualify(entry, environment))
    except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
        print(f"mysql matrix gate: {error}", file=sys.stderr)
        return 1
    finally:
        quiet(["docker", "network", "rm", NETWORK])
    print(
        json.dumps(
            {
                "schema_version": 1,
                "gate": "mysql-version-matrix-v1",
                "status": "passed",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "live_tests_per_reference": (
                    len(EXPECTED_LIVE_DEFAULT_TESTS)
                    + len(EXPECTED_LIVE_REFERENCE_TESTS)
                ),
                "results": results,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
