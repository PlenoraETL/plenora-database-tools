#!/usr/bin/env python3
"""Suite del SDK Python, sempre su un'estensione appena costruita.

Il modulo nativo `plenora_database/_native.abi3.so` e gitignorato: non e un
artefatto del repository, e nessuno lo rigenera automaticamente. Chi cambia
il Rust e poi lancia `pytest` a mano esegue il binario precedente, e ottiene
un risultato che non riguarda il codice che ha scritto — rosso su codice
corretto, o verde su codice rotto. E successo due volte in una sola
sessione: la correzione sul `SessionContext` e quella sul limite di 52
caratteri sono state entrambe "smentite" da un `.so` vecchio.

Questo runner toglie il caso dalla mano di chi esegue:

1. costruisce il wheel con `maturin` dentro l'immagine Rust pinnata;
2. **cancella** il `.so` esistente prima di installare il nuovo, cosi un
   fallimento di build non puo lasciare in piedi quello di prima;
3. estrae il modulo dal wheel appena prodotto e lo mette al suo posto;
4. esegue `pytest` sulle reti Compose dei riferimenti attivi.

Uso:

    python scripts/check_sdk_tests.py             # Postgres + MySQL
    python scripts/check_sdk_tests.py --offline   # solo i test senza server

Le reti e i volumi non sono scritti a mano: si chiedono a Docker con gli
stessi helper dei tre gate di riferimento.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import (  # noqa: E402
    compose_network_arguments,
    compose_volume,
)

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "crates" / "plenora-database-py"
NATIVE = CRATE / "python" / "plenora_database" / "_native.abi3.so"
RUST_IMAGE = "rust:1.92"
PYTHON_IMAGE = "python:3.13-slim"
TEST_DEPENDENCIES = ("pytest", "pytest-asyncio", "pyarrow", "pandas")

POSTGRES_CONTAINER = "dataflow-postgres"
MYSQL_CONTAINER = "dataflow-mysql"
POSTGRES_DSN = (
    "host=dataflow-postgres port=5432 user=dataflow "
    "password=dataflow_test_2026 dbname=dataflow_test"
)
MYSQL_PASSWORD = "DataFlow_Test_2026!"


def run(command: list[str], *, capture: bool = False) -> str:
    completed = subprocess.run(
        command, cwd=ROOT, check=False, text=True, capture_output=capture
    )
    if completed.returncode:
        if capture:
            sys.stderr.write(completed.stdout)
            sys.stderr.write(completed.stderr)
        raise RuntimeError(f"comando fallito: {' '.join(command[:3])}")
    return completed.stdout if capture else ""


def build_extension() -> str:
    """Costruisce il wheel con maturin e installa il modulo nativo.

    # Returns

    Il nome del wheel prodotto, che finisce nel verdetto: e l'unico modo per
    verificare a posteriori quale artefatto ha girato.

    # Raises

    `RuntimeError` se la build fallisce. Il `.so` precedente e gia stato
    rimosso a quel punto: meglio nessuna estensione che una vecchia, perche
    la seconda produce risultati che sembrano validi.
    """

    if NATIVE.exists():
        NATIVE.unlink()

    script = (
        "set -e; "
        "apt-get update -qq >/dev/null 2>&1; "
        "apt-get install -y -qq python3 python3-venv unzip >/dev/null 2>&1; "
        "python3 -m venv /tmp/v; "
        "/tmp/v/bin/pip install -q maturin; "
        "cd /workspace/crates/plenora-database-py; "
        "/tmp/v/bin/maturin build --release --out /tmp/wheels; "
        "cd /tmp/wheels && ls *.whl; "
        "unzip -o -q *.whl -d /tmp/extracted; "
        "cp /tmp/extracted/plenora_database/_native.abi3.so "
        "/workspace/crates/plenora-database-py/python/plenora_database/"
    )
    output = run(
        [
            "docker", "run", "--rm",
            "-v", f"{ROOT}:/workspace",
            "-v", "plenora_cargo_registry:/usr/local/cargo/registry",
            "-v", "plenora_cargo_git:/usr/local/cargo/git",
            "-v", "pln_target_docker:/workspace/target-docker",
            "-w", "/workspace",
            "-e", "CARGO_TARGET_DIR=/workspace/target-docker",
            RUST_IMAGE, "sh", "-c", script,
        ],
        capture=True,
    )
    if not NATIVE.exists():
        raise RuntimeError("maturin non ha prodotto il modulo nativo")
    wheels = [line.strip() for line in output.splitlines() if line.strip().endswith(".whl")]
    return wheels[-1] if wheels else "sconosciuto"


def pytest_command(*, offline: bool) -> list[str]:
    command = ["docker", "run", "--rm"]
    environment: list[str] = []

    if not offline:
        command += compose_network_arguments(POSTGRES_CONTAINER, MYSQL_CONTAINER)
        tls_volume = compose_volume(MYSQL_CONTAINER, "/etc/mysql/tls")
        command += ["-v", f"{tls_volume}:/mysql-tls:ro"]
        environment = [
            "-e", f"PLENORA_TEST_POSTGRES_DSN={POSTGRES_DSN}",
            "-e", f"PLENORA_TEST_MYSQL_HOST={MYSQL_CONTAINER}",
            "-e", "PLENORA_TEST_MYSQL_DATABASE=dataflow_test",
            "-e", "PLENORA_TEST_MYSQL_USER=dataflow",
            "-e", f"PLENORA_TEST_MYSQL_PASSWORD={MYSQL_PASSWORD}",
            "-e", "PLENORA_TEST_MYSQL_CA=/mysql-tls/ca.pem",
            "-e", "PLENORA_BENCH_PARITY=1",
        ]

    installs = " ".join(TEST_DEPENDENCIES)
    script = (
        f"pip install -q {installs} >/dev/null 2>&1; "
        "PYTHONPATH=/workspace/crates/plenora-database-py/python "
        "python -m pytest python/tests -q -rs"
    )
    command += [
        "-v", f"{ROOT}:/workspace",
        "-w", "/workspace/crates/plenora-database-py",
        *environment,
        PYTHON_IMAGE, "sh", "-c", script,
    ]
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--offline",
        action="store_true",
        help="salta le reti dei riferimenti: girano solo i test senza server",
    )
    arguments = parser.parse_args()

    try:
        wheel = build_extension()
        output = run(pytest_command(offline=arguments.offline), capture=True)
    except RuntimeError as error:
        print(f"sdk gate: {error}", file=sys.stderr)
        return 1

    print(output)
    summary = next(
        (line for line in reversed(output.splitlines()) if " passed" in line),
        "",
    )
    print(
        json.dumps(
            {
                "schema_version": 1,
                "gate": "python-sdk-suite",
                "status": "passed",
                "scope": "offline" if arguments.offline else "live",
                "wheel": wheel,
                "pytest": summary.strip(),
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
