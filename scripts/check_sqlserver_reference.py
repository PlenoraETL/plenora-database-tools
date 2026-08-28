#!/usr/bin/env python3
"""Gate live riproducibile del provider SQL Server 2022 di riferimento."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Il gate viene invocato sia come modulo del pacchetto sia come script: la
# radice del repository deve restare importabile in entrambi i casi.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import live_inventory  # noqa: E402
from scripts.compose_network import compose_network  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
RUST_IMAGE = "rust:1.98"
CONTAINER = "dataflow-sqlserver"
EXPECTED_IMAGE = (
    "sha256:e07b9699a2b749969f19d86563ceeea22bd3a69f7f1db85a8d1ac4bdaf0c6f56"
)
LIVE_TEST_SOURCE = ROOT / "crates" / "plenora-db-sqlserver" / "src" / "live_tests.rs"

# I due test che **questa** matrice non puo eseguire. Il motivo non e scritto
# qui: viene dal loro `#[ignore = "..."]`, cosi non puo divergere dal codice.
# Sono anche gli unici due `--skip` passati a cargo, derivati da questa lista:
# due elenchi separati sarebbero andati alla deriva.
SKIPPED_LIVE_TESTS = (
    "polybase_external_catalog_is_structural_and_not_implicit",
    "azure_sql_probe_uses_verified_tls_and_native_spatial_types",
)

# Test live che il gate non può dichiarare e poi non eseguire. Il conteggio da
# solo non basta: una matrice piena può comunque avere sostituito un test con
# un altro. Il nome va verificato.
REQUIRED_LIVE_TESTS = frozenset(
    {
        "live_native_query_policy_guards_every_transaction_entrypoint",
        "live_provider_execute_ddl_creates_and_drops_table",
        "live_provider_row_diagnostics_matches_confirmed_rollback_oracle",
    }
)


def live_test_inventory() -> set[str]:
    """I test live che `live_tests.rs` definisce, ora.

    Il gate contava soltanto **quanti** test erano passati — quarantacinque su
    quarantacinque — e un totale non distingue un test da un altro: sostituirne
    uno lasciava la matrice piena e il gate verde. E la stessa classe di
    difetto corretta nel gate PostgreSQL, e la regola 5 di AGENTS.md dice di
    cercarla altrove.
    """

    return live_inventory.source_inventory([LIVE_TEST_SOURCE])


def skipped_with_reasons() -> dict[str, str]:
    """I test saltati, con il motivo che portano nel codice."""

    reasons = live_inventory.ignore_reasons(
        LIVE_TEST_SOURCE.read_text(encoding="utf-8")
    )
    missing = [name for name in SKIPPED_LIVE_TESTS if name not in reasons]
    if missing:
        raise RuntimeError(
            f"test SQL Server dichiarati saltati ma senza `#[ignore = \"...\"]` "
            f"nei sorgenti: {missing}"
        )
    return {name: reasons[name] for name in SKIPPED_LIVE_TESTS}
DEFAULT_PASSWORD = "DataFlow_Test_2026!"
DOCKER_TIMEOUT_SECONDS = 30
CARGO_TIMEOUT_SECONDS = 15 * 60
ASSURANCE_RESULTS = ROOT / "assurance-results"
PRIVATE_CA = ASSURANCE_RESULTS / "sqlserver-private-ca.pem"


def run(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
    capture: bool = False,
    timeout_seconds: int,
) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=capture,
            env=environment,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"timeout dopo {timeout_seconds}s durante {command[0]}"
        ) from error
    if capture:
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
    if completed.returncode:
        raise RuntimeError(f"check fallito: {command[0]}")
    return f"{completed.stdout}{completed.stderr}" if capture else ""


def docker_value(arguments: list[str]) -> str:
    try:
        completed = subprocess.run(
            ["docker", *arguments],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=DOCKER_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("timeout interrogazione Docker SQL Server") from error
    if completed.returncode:
        sys.stderr.write(completed.stderr)
        raise RuntimeError("interrogazione Docker SQL Server fallita")
    return completed.stdout.strip()


def sqlserver_network() -> str:
    """Rete Compose osservata sul container di riferimento.

    Il nome dipende dal progetto Compose, cioè dalla directory del checkout: un
    valore cablato rende il gate ineseguibile in un worktree. La scoperta è
    fail-closed — senza label di progetto, senza la rete attesa o senza l'alias
    del container il gate fallisce, invece di ripiegare su una rete inventata
    che produrrebbe un errore di connessione travestito da difetto del provider.
    """

    return compose_network(CONTAINER, required_alias=CONTAINER)


def cargo(arguments: list[str]) -> tuple[list[str], dict[str, str] | None]:
    if os.environ.get("PLENORA_SQLSERVER_GATE_HOST_CARGO") == "1":
        environment = os.environ.copy()
        environment.setdefault("PLENORA_SQLSERVER_HOST", "127.0.0.1")
        environment.setdefault("PLENORA_SQLSERVER_DATABASE", "dataflow_test")
        environment.setdefault("PLENORA_SQLSERVER_USER", "dataflow")
        environment.setdefault("PLENORA_SQLSERVER_PASSWORD", DEFAULT_PASSWORD)
        environment["PLENORA_SQLSERVER_PRIVATE_CA"] = str(PRIVATE_CA)
        environment.setdefault("PLENORA_SQLSERVER_MISMATCH_HOST", "127.0.0.2")
        return ["cargo", *arguments], environment

    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        sqlserver_network(),
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-v",
        f"{ROOT.parent / 'plenora-rustup-cache'}:/usr/local/rustup",
        "-w",
        "/workspace",
        "-e",
        "PLENORA_SQLSERVER_HOST=sqlserver",
        "-e",
        "PLENORA_SQLSERVER_DATABASE=dataflow_test",
        "-e",
        "PLENORA_SQLSERVER_USER=dataflow",
        "-e",
        f"PLENORA_SQLSERVER_PASSWORD={DEFAULT_PASSWORD}",
        "-e",
        "PLENORA_SQLSERVER_PRIVATE_CA=/workspace/assurance-results/sqlserver-private-ca.pem",
        "-e",
        "PLENORA_SQLSERVER_MISMATCH_HOST=sqlserver-hostname-mismatch",
        RUST_IMAGE,
        "cargo",
        *arguments,
    ]
    return command, None


def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
    command, environment = cargo(arguments)
    return run(
        command,
        environment=environment,
        capture=capture,
        timeout_seconds=CARGO_TIMEOUT_SECONDS,
    )


def validate_image_pin() -> dict[str, str]:
    compose = (ROOT / "docker-compose.sqlserver.yml").read_text(encoding="utf-8")
    reference = f"mcr.microsoft.com/mssql/server@{EXPECTED_IMAGE}"
    if compose.count(reference) != 3:
        raise RuntimeError("digest SQL Server non fissato per tutti i servizi")
    configured_image = docker_value(
        ["inspect", "--format", "{{.Config.Image}}", CONTAINER]
    )
    image_id = docker_value(["inspect", "--format", "{{.Image}}", CONTAINER])
    if configured_image != reference and image_id != EXPECTED_IMAGE:
        raise RuntimeError("container SQL Server diverso dal digest di riferimento")
    return {
        "configured_reference": configured_image,
        "runtime_image_id": image_id,
        "expected_digest": EXPECTED_IMAGE,
    }


def materialize_private_ca() -> None:
    ASSURANCE_RESULTS.mkdir(parents=True, exist_ok=True)
    if PRIVATE_CA.exists():
        PRIVATE_CA.chmod(stat.S_IREAD | stat.S_IWRITE)
        PRIVATE_CA.unlink()
    run(
        [
            "docker",
            "cp",
            f"{CONTAINER}:/var/opt/mssql/tls/ca.pem",
            str(PRIVATE_CA),
        ],
        timeout_seconds=DOCKER_TIMEOUT_SECONDS,
    )
    if not PRIVATE_CA.is_file() or PRIVATE_CA.stat().st_size == 0:
        raise RuntimeError("CA privata SQL Server non materializzata")


def certificate_identity(path: str) -> str:
    return docker_value(
        [
            "exec",
            CONTAINER,
            "openssl",
            "x509",
            "-in",
            path,
            "-noout",
            "-fingerprint",
            "-sha256",
            "-serial",
        ]
    )


def rotate_server_certificate() -> dict[str, str]:
    server_before = certificate_identity("/var/opt/mssql/tls/server.pem")
    ca_before = certificate_identity("/var/opt/mssql/tls/ca.pem")
    compose = str(ROOT / "docker-compose.sqlserver.yml")
    run(
        [
            "docker",
            "compose",
            "-f",
            compose,
            "run",
            "--rm",
            "-e",
            "PLENORA_TLS_ROTATE=1",
            "sqlserver-certgen",
        ],
        timeout_seconds=2 * 60,
    )
    run(
        [
            "docker",
            "compose",
            "-f",
            compose,
            "up",
            "-d",
            "--force-recreate",
            "--wait",
            "sqlserver",
        ],
        timeout_seconds=3 * 60,
    )
    server_after = certificate_identity("/var/opt/mssql/tls/server.pem")
    ca_after = certificate_identity("/var/opt/mssql/tls/ca.pem")
    if server_before == server_after:
        raise RuntimeError("rotazione TLS non ha cambiato il certificato server")
    if ca_before != ca_after:
        raise RuntimeError("rotazione TLS ha cambiato inaspettatamente la CA privata")
    materialize_private_ca()
    run_cargo(
        [
            "test",
            "-p",
            "plenora-db-sqlserver",
            "live_tests::live_private_ca_tls_validates_chain_and_hostname",
            "--locked",
            "--",
            "--ignored",
            "--exact",
        ],
        capture=True,
    )
    return {
        "ca": ca_after,
        "server_before": server_before,
        "server_after": server_after,
    }


def server_identity() -> dict[str, str]:
    output = docker_value(
        [
            "exec",
            CONTAINER,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-P",
            DEFAULT_PASSWORD,
            "-d",
            "dataflow_test",
            "-C",
            "-b",
            "-h",
            "-1",
            "-W",
            "-s",
            "|",
            "-Q",
            (
                "SET NOCOUNT ON; "
                "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')), "
                "CONVERT(nvarchar(128), SERVERPROPERTY('Edition')), "
                "CONVERT(nvarchar(10), compatibility_level) "
                "FROM sys.databases WHERE name = DB_NAME();"
            ),
        ]
    )
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) != 1:
        raise RuntimeError("identita SQL Server non deterministica")
    parts = [part.strip() for part in lines[0].split("|")]
    if len(parts) != 3 or not all(parts):
        raise RuntimeError("identita SQL Server incompleta")
    return {
        "product_version": parts[0],
        "edition": parts[1],
        "compatibility_level": parts[2],
    }


def validate_live_result(output: str, listing: str) -> list[str]:
    """Ogni test live definito deve essere nella suite, ed essere passato.

    Le prove sono per nome: sorgenti contro suite compilata e suite contro
    esecuzione. Un totale non distinguerebbe la sostituzione di un test con un
    altro. Le esclusioni del riferimento sono dichiarate con la loro ragione.

    Restituisce i nomi eseguiti, che entrano nel verdetto.
    """

    declared = live_test_inventory()
    listed = live_inventory.listed_tests(
        listing, keep=lambda name: name.startswith("live_tests::")
    )
    absent = sorted(declared - {live_inventory.leaf(name) for name in listed})
    if absent:
        raise RuntimeError(
            f"test live SQL Server definiti nei sorgenti ma assenti dalla suite "
            f"compilata ({len(absent)} su {len(declared)}): {absent}"
        )
    skipped = set(skipped_with_reasons())
    unknown = sorted(skipped - declared)
    if unknown:
        raise RuntimeError(
            f"test SQL Server dichiarati saltati ma inesistenti: {unknown}"
        )
    expected = {name for name in listed if live_inventory.leaf(name) not in skipped}
    executed = live_inventory.executed_tests(output)
    missing = sorted(expected - executed)
    if missing:
        raise RuntimeError(
            f"test live SQL Server nella suite ma non eseguiti ({len(missing)} su "
            f"{len(expected)}): {missing}"
        )
    if not re.search(r"test result: ok\. \d+ passed; 0 failed;", output):
        raise RuntimeError("matrice live SQL Server con fallimenti")
    unmet = sorted(REQUIRED_LIVE_TESTS - {live_inventory.leaf(name) for name in executed})
    if unmet:
        raise RuntimeError(
            f"test live SQL Server dichiarati ma non eseguiti: {unmet}"
        )
    return sorted(expected)


def run_live_cli_probe() -> None:
    test_name = "live_database_probe_sqlserver_private_ca"
    output = run_cargo(
        [
            "test",
            "-p",
            "plenora-database-cli",
            # L'adapter SQL Server della CLI e opt-in: senza la feature il
            # binario risponde `unsupported` e il probe proverebbe soltanto
            # che il provider non e stato compilato.
            "--features",
            "sqlserver",
            "--test",
            "live_probe",
            test_name,
            "--locked",
            "--",
            "--ignored",
            "--exact",
            "--nocapture",
        ],
        capture=True,
    )
    if not re.search(rf"^test {re.escape(test_name)} \.\.\. ok$", output, re.MULTILINE):
        raise RuntimeError("probe CLI live SQL Server non eseguito")
    if not re.search(
        r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output
    ):
        raise RuntimeError("risultato probe CLI live SQL Server inatteso")


def main() -> int:
    # I passi che il gate ha **davvero** completato, registrati mentre
    # accadono. La lista tematica scritta a mano restava identica se un passo
    # veniva tolto: un artifact `passed` attestava verifiche di cui non
    # esisteva piu la prova.
    steps: list[str] = []
    try:
        state = docker_value(
            [
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.Health.Status}}",
                CONTAINER,
            ]
        )
        if state != "running|healthy":
            raise RuntimeError("container SQL Server non healthy")
        steps.append("container_health")
        image_identity = validate_image_pin()
        steps.append("immutable_image_digest")
        identity = server_identity()
        steps.append("server_identity")
        materialize_private_ca()
        steps.append("private_ca_materialized")
        run_cargo(
            [
                "clippy",
                "-p",
                "plenora-db-sqlserver",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ]
        )
        steps.append("clippy_deny_warnings")
        run_cargo(
            [
                "test",
                "-p",
                "plenora-db-sqlserver",
                "--lib",
                "--locked",
            ]
        )
        steps.append("offline_unit_tests")
        live_output = run_cargo(
            [
                "test",
                "-p",
                "plenora-db-sqlserver",
                "live_",
                "--locked",
                "--",
                "--ignored",
                "--test-threads=1",
                # Dalla dichiarazione, non da una seconda lista scritta a mano:
                # due elenchi separati sarebbero andati alla deriva, e il gate
                # avrebbe saltato un test che diceva di eseguire.
                *[argument for name in SKIPPED_LIVE_TESTS for argument in ("--skip", name)],
            ],
            capture=True,
        )
        listing = run_cargo(
            [
                "test",
                "-p",
                "plenora-db-sqlserver",
                "live_",
                "--locked",
                "--",
                "--list",
                "--ignored",
            ],
            capture=True,
        )
        executed_live_tests = validate_live_result(live_output, listing)
        steps.append("live_inventory_matches_sources_and_run")
        run_live_cli_probe()
        steps.append("public_cli_probe_against_private_ca")
        tls_rotation = rotate_server_certificate()
        steps.append("tls_certificate_rotation")
    except RuntimeError as error:
        print(f"sqlserver reference gate: {error}", file=sys.stderr)
        return 1

    report = {
        "schema_version": 1,
        "gate": "sqlserver-reference-v1",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": True,
        "secrets_persisted": False,
        "container_image": image_identity,
        "server": identity,
        "tls_rotation": tls_rotation,
        "live_tests": {
            "expected": len(executed_live_tests),
            "passed": len(executed_live_tests),
            "failed": 0,
        },
        "executed_live_tests": executed_live_tests,
        # Non "eseguiti": *non eseguibili qui*, con il motivo che portano nel
        # codice. Dichiararli non li fa girare: li rende visibili.
        "declared_not_executed": skipped_with_reasons(),
        "steps": steps,
        "open_non_blocking": [
            "azure_sql_live_qualification",
            "spatial_fullglobe_lossless_not_supported",
            "external_table_catalog_live_fixture",
        ],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
