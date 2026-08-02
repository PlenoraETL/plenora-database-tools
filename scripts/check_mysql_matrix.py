#!/usr/bin/env python3
"""Qualifica MySQL 8.0 e 8.4 su immagini fissate per digest.

Ogni riferimento viene avviato con la stessa fixture versionata del gate 8.4:
TLS obbligatorio con CA privata, `local_infile` disattivato e `sql_mode`
stretta. La matrice esegue l'inventario live completo, serializzato, senza
esclusioni: una semantica che un riferimento non regge resta un blocco
dichiarato, non un test indebolito.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Il gate viene invocato sia come modulo del pacchetto sia come script: la
# radice del repository deve restare importabile in entrambi i casi.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_mysql_reference import (  # noqa: E402
    EXPECTED_LIVE_TESTS,
    EXPECTED_OFFLINE_TESTS,
)

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


@dataclass(frozen=True)
class MatrixEntry:
    label: str
    version_prefix: str
    digest: str

    @property
    def image(self) -> str:
        return f"mysql@sha256:{self.digest}"

    @property
    def slug(self) -> str:
        return self.version_prefix.replace(".", "").strip()

    @property
    def container(self) -> str:
        return f"plenora-matrix-mysql-{self.slug}"

    @property
    def aliases(self) -> tuple[str, ...]:
        # Il certificato della fixture e emesso solo per dataflow-mysql.
        # L'alias risolvibile ma assente dai SAN rende deterministica la prova
        # TLS negativa senza introdurre un falso errore DNS.
        return ("dataflow-mysql", "mysql-hostname-mismatch")

    @property
    def ca_volume(self) -> str:
        return f"plenora-matrix-mysql-{self.slug}-ca"

    @property
    def tls_volume(self) -> str:
        return f"plenora-matrix-mysql-{self.slug}-tls"


MATRIX = (
    MatrixEntry(
        "MySQL 8.0",
        "8.0.",
        "7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b",
    ),
    MatrixEntry(
        "MySQL 8.4 LTS",
        "8.4.",
        "b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb",
    ),
)


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


def test_command(entry: MatrixEntry) -> list[str]:
    """L'inventario live intero, un test alla volta: nessuna esclusione."""
    del entry
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
    return set(PASSING_TEST.findall(output))


def verify_live_inventory(entry: MatrixEntry, output: str) -> None:
    executed = live_inventory(output)
    if executed != EXPECTED_LIVE_TESTS:
        missing = sorted(EXPECTED_LIVE_TESTS - executed)
        unexpected = sorted(executed - EXPECTED_LIVE_TESTS)
        raise RuntimeError(
            f"inventario live {entry.label} inatteso: "
            f"eseguiti {len(executed)}, attesi {len(EXPECTED_LIVE_TESTS)}, "
            f"mancanti={missing}, inattesi={unexpected}"
        )


def verify_candidate_offline() -> None:
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
        ],
        timeout=1800,
        capture=True,
    )
    executed = set(re.findall(r"^test ([^ ]+) \.\.\. ok$", output, re.MULTILINE))
    if executed != EXPECTED_OFFLINE_TESTS:
        missing = sorted(EXPECTED_OFFLINE_TESTS - executed)
        unexpected = sorted(executed - EXPECTED_OFFLINE_TESTS)
        raise RuntimeError(
            "inventario offline candidato MySQL inatteso: "
            f"mancanti={missing}, inattesi={unexpected}"
        )


def verify_hardening(entry: MatrixEntry, probe: dict[str, str]) -> None:
    version = probe.get("version", "")
    if not version.startswith(entry.version_prefix):
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
        "version_prefix": entry.version_prefix,
        "image": entry.image,
        "product_version": probe["version"],
        "live_tests": {
            "expected": len(EXPECTED_LIVE_TESTS),
            "passed": len(EXPECTED_LIVE_TESTS),
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
    completed = subprocess.run(
        [
            "docker",
            "exec",
            entry.container,
            "/bin/sh",
            "-c",
            'exec env MYSQL_PWD="$MYSQL_PASSWORD" mysql -Nse "$1" '
            f"-u {USER} --ssl-mode=REQUIRED",
            "mysql-matrix-probe",
            statement,
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=60,
    )
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


def run_suite(entry: MatrixEntry, environment: dict[str, str]) -> str:
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
        *test_command(entry),
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
        verify_live_inventory(entry, run_suite(entry, environment))
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
        verify_candidate_offline()
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
                "live_tests_per_reference": len(EXPECTED_LIVE_TESTS),
                "results": results,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
