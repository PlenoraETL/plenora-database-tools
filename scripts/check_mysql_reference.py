#!/usr/bin/env python3
"""Gate riproducibile del riferimento MySQL baseline.

La versione e il digest del riferimento non sono scritti qui: arrivano da
`docker/mysql/references.json`, unica fonte di verita della matrice. Il gate
avvia la baseline dichiarata, ne verifica l'identita e poi esegue le tre
famiglie di test del provider, ciascuna con il proprio runner e il proprio
inventario fissato per nome.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Il gate viene invocato sia come modulo del pacchetto sia come script: la
# radice del repository deve restare importabile in entrambi i casi.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import (  # noqa: E402
    compose_network,
    compose_volume,
    container_variable,
)
from scripts.mysql_inventory import (  # noqa: E402
    collect as collect_inventory,
    difference as inventory_difference,
)
from scripts.mysql_references import (  # noqa: E402
    BASELINE,
    validate_compose_pins_the_baseline,
)


ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "dataflow-mysql"
RUST_IMAGE = "rust@sha256:462a9af3c54fb4718850d3c602fc0e54452c20b1c12a4e4080fdb001d4b9acbf"
EXPECTED_DIGEST = BASELINE.digest
EXPECTED_REFERENCE = BASELINE.image
EXPECTED_VERSION = BASELINE.exact_version
EXPECTED_VERSION_PREFIX = BASELINE.version_prefix

# Le famiglie derivano dai sorgenti, inclusi i target di integrazione.
_INVENTORY = collect_inventory()
EXPECTED_UNIT_TESTS = set(_INVENTORY["unit"])
EXPECTED_LIVE_DEFAULT_TESTS = set(_INVENTORY["live_default"])
EXPECTED_LIVE_REFERENCE_TESTS = set(_INVENTORY["live_reference"])


def run(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
    capture: bool = False,
    timeout: int = 15 * 60,
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
        raise RuntimeError(f"comando fallito ({completed.returncode}): {command[0]}")
    return f"{completed.stdout}{completed.stderr}" if capture else ""


def docker_value(arguments: list[str]) -> str:
    completed = subprocess.run(
        ["docker", *arguments],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        sys.stderr.write(completed.stderr)
        raise RuntimeError("interrogazione Docker MySQL fallita")
    return completed.stdout.strip()


def fixture_password() -> str:
    """La password della fixture, letta dal container che la porta.

    Una copia nel sorgente sarebbe una seconda fonte per un dato che ne ha
    una sola — il compose — e le due divergerebbero in silenzio.
    """

    return container_variable(CONTAINER, "MYSQL_PASSWORD")


def mysql_value(statement: str) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER,
            "/bin/sh",
            "-c",
            # TCP, non socket: durante il bootstrap l'entrypoint MySQL
            # avvia un server temporaneo raggiungibile solo dal socket. Una
            # probe sul socket puo quindi rispondere prima che il server
            # definitivo esista, e la verifica successiva trova il vuoto.
            'exec env MYSQL_PWD="$MYSQL_PASSWORD" mysql -Nse "$1" '
            "-u dataflow -h 127.0.0.1 --protocol=TCP --ssl-mode=REQUIRED",
            "mysql-reference-probe",
            statement,
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        sys.stderr.write(completed.stderr)
        raise RuntimeError("probe SQL MySQL fallita")
    return completed.stdout.strip()


def mysql_tls_volume() -> str:
    """Volume con la CA privata, chiesto a Docker come la rete."""

    return compose_volume(CONTAINER, "/etc/mysql/tls")


def mysql_network() -> str:
    """Rete Compose del riferimento, con l'alias della prova TLS negativa.

    La scoperta e condivisa con gli altri due gate; resta specifico di MySQL
    il secondo alias, che nessun altro riferimento usa.
    """

    return compose_network(
        CONTAINER,
        required_alias=CONTAINER,
        required_aliases={
            "mysql-hostname-mismatch": (
                "la prova TLS negativa diventerebbe un errore DNS invece di "
                "un rifiuto di identita"
            )
        },
    )


def host_ca_path() -> str:
    """CA privata esportata sull'host per l'esecuzione con cargo locale.

    # Raises

    `RuntimeError` quando la CA non e disponibile: senza di essa i test live
    girerebbero con verifica indebolita, cioe proprio la condizione che il
    gate esiste per escludere.
    """

    ca_path = os.environ.get("PLENORA_MYSQL_CA")
    if not ca_path:
        raise RuntimeError(
            "PLENORA_MYSQL_CA obbligatoria con cargo host: esportare la CA da "
            f"{CONTAINER}:/etc/mysql/tls/ca.pem prima di eseguire il gate"
        )
    return ca_path


def cargo(arguments: list[str]) -> tuple[list[str], dict[str, str] | None]:
    if os.environ.get("PLENORA_MYSQL_GATE_HOST_CARGO") == "1":
        environment = os.environ.copy()
        environment.setdefault("PLENORA_MYSQL_HOST", "127.0.0.1")
        environment.setdefault("PLENORA_MYSQL_DATABASE", "dataflow_test")
        environment.setdefault("PLENORA_MYSQL_USER", "dataflow")
        environment.setdefault("PLENORA_MYSQL_PASSWORD", fixture_password())
        environment["PLENORA_MYSQL_CA"] = host_ca_path()
        environment["PLENORA_MYSQL_EXPECTED_VERSION"] = EXPECTED_VERSION_PREFIX
        return [os.environ.get("CARGO", "cargo"), *arguments], environment

    environment = os.environ.copy()
    environment["PLENORA_MYSQL_PASSWORD"] = fixture_password()
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        mysql_network(),
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        "plenora-cargo-registry:/usr/local/cargo/registry",
        "-v",
        "plenora-cargo-git:/usr/local/cargo/git",
        "-v",
        f"{mysql_tls_volume()}:/mysql-tls:ro",
        "-w",
        "/workspace",
        "-e",
        f"PLENORA_MYSQL_HOST={CONTAINER}",
        "-e",
        "PLENORA_MYSQL_DATABASE=dataflow_test",
        "-e",
        "PLENORA_MYSQL_USER=dataflow",
        "-e",
        "PLENORA_MYSQL_PASSWORD",
        "-e",
        "PLENORA_MYSQL_CA=/mysql-tls/ca.pem",
        "-e",
        f"PLENORA_MYSQL_EXPECTED_VERSION={EXPECTED_VERSION_PREFIX}",
        RUST_IMAGE,
        "/usr/local/cargo/bin/cargo",
        *arguments,
    ]
    return command, environment


def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
    command, environment = cargo(arguments)
    return run(command, environment=environment, capture=capture)


def validate_inventory() -> None:
    """L'inventario dichiarato deve coincidere con la sorgente Rust.

    # Raises

    `RuntimeError` appena una famiglia diverge: un test aggiunto e mai
    eseguito, o rimosso e mai notato, resterebbe altrimenti invisibile.
    """

    observed = collect_inventory()
    declared = {
        "unit": frozenset(EXPECTED_UNIT_TESTS),
        "live_default": frozenset(EXPECTED_LIVE_DEFAULT_TESTS),
        "live_reference": frozenset(EXPECTED_LIVE_REFERENCE_TESTS),
    }
    for family, names in declared.items():
        if names != observed[family]:
            raise RuntimeError(
                f"inventario {family} MySQL stale rispetto alla sorgente: "
                f"{inventory_difference(names, observed[family])}"
            )


def validate_fixture() -> None:
    validate_compose_pins_the_baseline()
    run([sys.executable, str(ROOT / "scripts" / "test_check_mysql_reference.py")])
    run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{ROOT / 'docker' / 'mysql' / 'tls'}:/fixture:ro",
            EXPECTED_REFERENCE,
            "/bin/bash",
            "/fixture/test_generate.sh",
        ]
    )


def ensure_reference_running() -> None:
    compose = str(ROOT / "docker-compose.mysql.yml")
    run(["docker", "compose", "-f", compose, "config", "--quiet"])
    run(
        ["docker", "compose", "-f", compose, "up", "-d", "--wait", "mysql"],
        timeout=5 * 60,
    )


def validate_reference() -> dict[str, str]:
    configured = docker_value(["inspect", "--format", "{{.Config.Image}}", CONTAINER])
    image_id = docker_value(["inspect", "--format", "{{.Image}}", CONTAINER])
    if configured != EXPECTED_REFERENCE:
        raise RuntimeError("container MySQL diverso dal digest di riferimento")
    version = mysql_value("SELECT VERSION()")
    if version != EXPECTED_VERSION:
        raise RuntimeError(
            f"versione MySQL inattesa: {version}, attesa {EXPECTED_VERSION}"
        )
    return {"configured_reference": configured, "image_id": image_id, "version": version}


def executed_tests(output: str, pattern: str) -> set[str]:
    return set(re.findall(pattern, output, re.MULTILINE))


def verify_suite(family: str, declared: set[str], observed: set[str]) -> None:
    if declared != observed:
        raise RuntimeError(
            f"inventario test {family} MySQL inatteso: "
            f"eseguiti {len(observed)}, attesi {len(declared)}, "
            f"{inventory_difference(frozenset(declared), frozenset(observed))}"
        )


def run_unit_suite() -> None:
    """I test senza server: il filtro esclude ogni test live per nome."""

    output = run_cargo(
        ["test", "-p", "plenora-db-mysql", "--locked", "--", "--skip", "live_"],
        capture=True,
    )
    verify_suite(
        "unit",
        EXPECTED_UNIT_TESTS,
        executed_tests(output, r"^test ([^ ]+) \.\.\. ok$"),
    )


def run_live_default_suite() -> None:
    """I test live che il runner di default esegue senza `--ignored`."""

    output = run_cargo(
        [
            "test",
            "-p",
            "plenora-db-mysql",
            "live_",
            "--locked",
            "--",
            "--show-output",
            "--test-threads=1",
        ],
        capture=True,
    )
    verify_suite(
        "live default",
        EXPECTED_LIVE_DEFAULT_TESTS,
        executed_tests(output, r"^test (live_tests::[^ ]+) \.\.\. ok$"),
    )


def run_live_reference_suite() -> None:
    """I test live `#[ignore]`: l'inventario di qualifica del riferimento."""

    output = run_cargo(
        [
            "test",
            "-p",
            "plenora-db-mysql",
            "live_",
            "--locked",
            "--",
            "--ignored",
            "--show-output",
            "--test-threads=1",
        ],
        capture=True,
    )
    verify_suite(
        "live reference",
        EXPECTED_LIVE_REFERENCE_TESTS,
        executed_tests(output, r"^test (live_tests::[^ ]+) \.\.\. ok$"),
    )


def run_live_cli_probe() -> None:
    test_name = "live_database_probe_mysql_private_ca"
    output = run_cargo(
        [
            "test",
            "-p",
            "plenora-database-cli",
            # L'adapter MySQL della CLI e opt-in: senza la feature il binario
            # risponde `unsupported` e il probe proverebbe soltanto che il
            # provider non e stato compilato.
            "--features",
            "mysql",
            "--test",
            "live_probe",
            test_name,
            "--locked",
            "--",
            "--ignored",
            "--exact",
            "--show-output",
        ],
        capture=True,
    )
    if not re.search(rf"^test {re.escape(test_name)} \.\.\. ok$", output, re.MULTILINE):
        raise RuntimeError("probe CLI live MySQL non eseguito")
    if not re.search(
        r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output
    ):
        raise RuntimeError("risultato probe CLI live MySQL inatteso")


def run_static_checks() -> int:
    """I controlli del gate che non richiedono ne container ne toolchain Rust.

    Sono tre: la matrice dichiarata deve coincidere con cio che il compose
    avvia, l'inventario dichiarato con la sorgente Rust, e le due suite Python
    che presidiano gate e matrice devono passare. Girano in secondi su ogni PR
    perche intercettano subito la classe di errore che altrimenti si scopre
    mezz'ora dopo, in mezzo a una campagna live.
    """

    validate_compose_pins_the_baseline()
    validate_inventory()
    run([sys.executable, str(ROOT / "scripts" / "test_check_mysql_reference.py")])
    run([sys.executable, "-m", "unittest", "tests.test_mysql_matrix"])
    print(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed",
                "provider": "mysql",
                "scope": "static",
                "reference": BASELINE.label,
                "reference_version": EXPECTED_VERSION,
                "unit_tests": len(EXPECTED_UNIT_TESTS),
                "live_default_tests": len(EXPECTED_LIVE_DEFAULT_TESTS),
                "live_reference_tests": len(EXPECTED_LIVE_REFERENCE_TESTS),
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    validate_inventory()
    validate_fixture()
    ensure_reference_running()
    identity = validate_reference()
    run_cargo(["fmt", "--all", "--", "--check"])
    run_cargo(
        [
            "clippy",
            "-p",
            "plenora-db-mysql",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ]
    )
    run_unit_suite()
    run_live_default_suite()
    run_live_reference_suite()
    run_live_cli_probe()
    print(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed",
                "provider": "mysql",
                "reference": BASELINE.label,
                "reference_version": EXPECTED_VERSION,
                "unit_tests": len(EXPECTED_UNIT_TESTS),
                "live_default_tests": len(EXPECTED_LIVE_DEFAULT_TESTS),
                "live_reference_tests": len(EXPECTED_LIVE_REFERENCE_TESTS),
                "image": identity,
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def selected_entry_point(arguments: list[str]):
    """`--static` sceglie i soli controlli senza server; nient'altro e valido.

    # Raises

    `RuntimeError` per qualunque altro argomento: un flag scritto male non
    deve degradare in silenzio al gate completo, ne viceversa.
    """

    if not arguments:
        return main
    if arguments == ["--static"]:
        return run_static_checks
    raise RuntimeError(f"argomenti non riconosciuti: {arguments}")


if __name__ == "__main__":
    try:
        raise SystemExit(selected_entry_point(sys.argv[1:])())
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"MySQL reference gate FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
