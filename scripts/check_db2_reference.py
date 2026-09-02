#!/usr/bin/env python3
"""Gate live riproducibile del riferimento IBM Db2 LUW 12.1."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import live_inventory  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.db2.yml"
DOCKERFILE = ROOT / "docker" / "db2-client" / "Dockerfile"
ENTRYPOINT = ROOT / "docker" / "db2-client" / "entrypoint.sh"
HEALTHCHECK = ROOT / "docker" / "db2-client" / "healthcheck.sh"
INIT_FIXTURE = ROOT / "docker" / "db2-client" / "init-fixture.sh"
LIVE_SOURCE = ROOT / "crates" / "plenora-db-db2" / "src" / "live_tests.rs"
SPATIAL_LIVE_SOURCE = (
    ROOT / "crates" / "plenora-db-db2" / "src" / "spatial_live_tests.rs"
)
PYTHON_LIVE_SOURCE = (
    ROOT / "crates" / "plenora-database-py" / "python" / "tests" / "test_db2_session.py"
)
WHEEL_NAMER = ROOT / "scripts" / "name_db2_wheel.py"
RESULT = ROOT / "assurance-results" / "db2-reference.json"
TIMEOUT = 20 * 60
DB2_ENV = {
    "PLENORA_DB2_HOST": "127.0.0.1",
    "PLENORA_DB2_DATABASE": "plenora",
    "PLENORA_DB2_USER": "db2inst1",
    "PLENORA_DB2_PASSWORD": "plenora_test",
}
PYTHON_ENV = {
    "PLENORA_TEST_DB2_HOST": "127.0.0.1",
    "PLENORA_TEST_DB2_DATABASE": "plenora",
    "PLENORA_TEST_DB2_USER": "db2inst1",
    "PLENORA_TEST_DB2_PASSWORD": "plenora_test",
    "PLENORA_TEST_DB2_PORT": "50000",
    "PLENORA_TEST_DB2_TLS_MODE": "insecure_local",
}
REQUIRED_LIVE_TESTS = frozenset(
    {
        "live_reference_probe_catalog_and_capabilities",
        "live_errors_classify_security_and_missing_objects_without_payloads",
        "live_portable_sql_executes_insert_merge_select_and_rollback",
        "live_transaction_commit_rollback_savepoint_and_typed_query",
        "live_concurrent_users_keep_connections_and_rows_isolated",
        "live_arrow_write_modes_are_atomic_and_accounted",
        "live_parameter_array_binding_crosses_chunks_and_rolls_back_atomically",
        "live_provider_row_diagnostics_matches_confirmed_rollback_oracle",
        "live_keyset_checkpoint_persists_reopens_without_duplicates_or_gaps",
        "live_spatial_capabilities_and_streaming_wkb_are_evidence_backed",
        "live_spatial_write_round_trips_and_invalid_wkb_rolls_back",
        "live_spatial_portable_predicates_execute_with_bound_wkb_and_srid",
    }
)


def run(command: list[str], *, capture: bool = True) -> str:
    """Esegue un passo con timeout e rende visibile tutto il suo output."""

    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=capture,
            timeout=TIMEOUT,
            env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"timeout durante {command[0]}") from error
    output = f"{completed.stdout}{completed.stderr}" if capture else ""
    if output:
        print(output, end="")
    if completed.returncode:
        raise RuntimeError(f"passo fallito: {' '.join(command[:4])}")
    return output


def compose(arguments: list[str]) -> list[str]:
    return ["docker", "compose", "-f", str(COMPOSE_FILE), *arguments]


def compose_exec(arguments: list[str], environment: dict[str, str]) -> str:
    env_args = [
        item for pair in environment.items() for item in ("-e", f"{pair[0]}={pair[1]}")
    ]
    return run(compose(["exec", "-T", *env_args, "db2", *arguments]))


def live_test_inventory() -> set[str]:
    return live_inventory.source_inventory(
        [LIVE_SOURCE, SPATIAL_LIVE_SOURCE], keep=lambda name: name.startswith("live_")
    )


def validate_live_run(listing: str, output: str) -> list[str]:
    """Confronta sorgente, suite compilata ed esecuzione per nome."""

    source = live_test_inventory()
    listed = live_inventory.listed_tests(listing, keep=lambda name: "live_" in name)
    executed = live_inventory.executed_tests(output)
    listed_leaves = {live_inventory.leaf(name) for name in listed}
    if source != listed_leaves:
        raise RuntimeError(
            f"inventario live Db2 divergente: mancanti={sorted(source - listed_leaves)}, "
            f"inattesi={sorted(listed_leaves - source)}"
        )
    if listed != executed:
        raise RuntimeError(
            f"esecuzione live Db2 incompleta: mancanti={sorted(listed - executed)}, "
            f"inattesi={sorted(executed - listed)}"
        )
    missing_required = REQUIRED_LIVE_TESTS - listed_leaves
    if missing_required:
        raise RuntimeError(f"oracoli live Db2 mancanti: {sorted(missing_required)}")
    return sorted(listed)


def validate_build_contract() -> dict[str, str]:
    """Le immagini e il wheel DB2 devono avere input e confini espliciti."""

    source = DOCKERFILE.read_text(encoding="utf-8")
    images = re.findall(r"^FROM\s+(\S+@sha256:[0-9a-f]{64})", source, re.MULTILINE)
    if len(images) != 2:
        raise RuntimeError("Dockerfile Db2 senza due immagini fissate per digest")
    for required in (
        "--disablerepo=CRB",
        "python3.12 -m maturin build",
        "--features db2",
        "--auditwheel skip",
        "name_db2_wheel.py /opt/plenora-wheel",
        "*-1db2-cp310-abi3-linux_x86_64.whl",
        'ENTRYPOINT ["/usr/local/bin/plenora-db2-entrypoint"]',
    ):
        if required not in source:
            raise RuntimeError(f"contratto build Db2 assente: {required}")

    wheel_namer = WHEEL_NAMER.read_text(encoding="utf-8")
    for required in (
        "1db2",
        "cp310",
        "abi3",
        "linux_x86_64",
        "GITHUB_EVENT_NAME",
        "GITHUB_REF_NAME",
    ):
        if required not in wheel_namer:
            raise RuntimeError(f"contratto naming wheel Db2 assente: {required}")

    entrypoint = ENTRYPOINT.read_text(encoding="utf-8")
    healthcheck = HEALTHCHECK.read_text(encoding="utf-8")
    init_fixture = INIT_FIXTURE.read_text(encoding="utf-8")
    for required in (
        "SYSCAT.TABLES",
        "CATALOG_PROBE",
        "READ_PROBE",
        "TX_PROBE",
        "WRITE_PROBE",
        "SPATIAL_PROBE",
        "CATALOG_PROBE_VIEW",
        'test "${fixture_objects}" != "6"',
        'test "${schema_objects}" != "6"',
        "isql -b -k",
    ):
        if required not in healthcheck:
            raise RuntimeError(f"contratto healthcheck Db2 assente: {required}")
    marker = "/run/plenora-fixture-ready"
    if (
        marker not in entrypoint
        or marker not in healthcheck
        or marker not in init_fixture
    ):
        raise RuntimeError("sincronizzazione startup del fixture Db2 incompleta")
    return {"rust": images[0], "db2": images[1]}


def container_identity() -> dict[str, str]:
    container = run(compose(["ps", "-q", "db2"])).strip()
    if not container:
        raise RuntimeError("container Db2 Compose assente")
    state = run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}|{{.State.Health.Status}}",
            container,
        ]
    ).strip()
    if state != "running|healthy":
        raise RuntimeError(f"container Db2 non healthy: {state}")
    image = run(["docker", "inspect", "--format", "{{.Image}}", container]).strip()
    return {"container_id": container, "image_id": image}


def run_cli_probe() -> dict[str, object]:
    output = compose_exec(
        [
            "target/debug/plenora-database",
            "database-probe",
            "db2",
            "PLENORA_DB2_PASSWORD",
            "127.0.0.1",
            "plenora",
            "db2inst1",
            "50000",
            "--tls-mode",
            "disable",
        ],
        DB2_ENV,
    )
    for line in reversed(output.splitlines()):
        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue
        validate_cli_probe(document)
        return document
    raise RuntimeError("probe CLI Db2 senza documento JSON")


def validate_cli_probe(document: object) -> None:
    """Verifica entrambe le identita provider pubblicate dal probe."""

    if not isinstance(document, dict):
        raise RuntimeError("probe CLI Db2 con documento inatteso")
    connection = document.get("connection")
    capabilities = document.get("capabilities")
    if not isinstance(connection, dict) or connection.get("provider") != "db2":
        raise RuntimeError("probe CLI Db2 con connection.provider inatteso")
    if not isinstance(capabilities, dict) or capabilities.get("provider") != "db2":
        raise RuntimeError("probe CLI Db2 con capabilities.provider inatteso")


PYTHON_LIVE_TARGETS = (
    "db2_sdk_gate_tests/test_db2_session.py",
    "db2_sdk_gate_tests/test_orm.py::test_live_db2_generated_defaults_and_ddl",
    "db2_sdk_gate_tests/test_orm.py::test_live_db2_geometry_orm_qualification",
    "db2_sdk_gate_tests/test_orm.py::test_live_db2_migration_dag_is_idempotent_and_reversible",
    "db2_sdk_gate_tests/test_orm.py::test_live_db2_advanced_orm_qualification",
)
PYTHON_LIVE_EXPECTED = 9


def run_python_live() -> None:
    script = (
        "rm -rf /tmp/db2_sdk_gate_tests && "
        "mkdir /tmp/db2_sdk_gate_tests && "
        "cp -R /workspace/crates/plenora-database-py/python/tests/. /tmp/db2_sdk_gate_tests/ && "
        "cd /tmp && "
        "PLENORA_EXPECT_DB2_RUNTIME=1 "
        "python3.12 /workspace/.github/scripts/verify_wheel.py && "
        "python3.12 -m pytest -q -ra " + " ".join(PYTHON_LIVE_TARGETS)
    )
    output = compose_exec(["sh", "-lc", script], PYTHON_ENV)
    if not re.search(rf"\b{PYTHON_LIVE_EXPECTED} passed in ", output):
        raise RuntimeError("matrice Python Db2 non completa")


def main() -> int:
    steps: list[str] = []
    try:
        bases = validate_build_contract()
        steps.append("immutable_base_images_and_distinct_db2_wheel_contract")
        container = container_identity()
        steps.append("container_health")
        compose_exec(
            [
                "cargo",
                "clippy",
                "--locked",
                "-p",
                "plenora-db-db2",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
            DB2_ENV,
        )
        steps.append("clippy_deny_warnings")
        compose_exec(
            ["cargo", "test", "--locked", "-p", "plenora-db-db2", "--lib"], DB2_ENV
        )
        steps.append("offline_unit_tests")
        listing = compose_exec(
            [
                "cargo",
                "test",
                "--locked",
                "-p",
                "plenora-db-db2",
                "live_",
                "--",
                "--list",
                "--ignored",
            ],
            DB2_ENV,
        )
        live = compose_exec(
            [
                "cargo",
                "test",
                "--locked",
                "-p",
                "plenora-db-db2",
                "live_",
                "--",
                "--ignored",
                "--test-threads=1",
            ],
            DB2_ENV,
        )
        executed = validate_live_run(listing, live)
        steps.append("live_inventory_matches_sources_and_run")
        probe = run_cli_probe()
        steps.append("public_cli_probe")
        run_python_live()
        steps.append("installed_python_wheel_sync_async_live")
    except RuntimeError as error:
        print(f"db2 reference gate: {error}", file=sys.stderr)
        return 1

    report = {
        "schema_version": 1,
        "gate": "db2-reference-v1",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "platform": "linux-x86_64",
        "database_connections_opened": True,
        "base_images": bases,
        "container": container,
        "server_version": probe["connection"].get("server_version"),
        "live_tests": {"expected": len(executed), "passed": len(executed), "failed": 0},
        "python_live_tests": {
            "expected": PYTHON_LIVE_EXPECTED,
            "passed": PYTHON_LIVE_EXPECTED,
            "failed": 0,
        },
        "executed_live_tests": executed,
        "steps": steps,
        "platform_matrix": {
            "linux_x86_64": "live_qualified",
            "windows_x86_64": "build_only",
            "macos": "unsupported_by_ibm_client_matrix",
        },
    }
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
