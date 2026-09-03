#!/usr/bin/env python3
"""Gate riproducibile del riferimento Oracle Database Free 23ai."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import live_inventory
from scripts.compose_network import container_variable

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.oracle.yml"
REFERENCE_FILE = ROOT / "docker" / "oracle" / "references.json"
LIVE_SOURCE = ROOT / "crates" / "plenora-db-oracle" / "src" / "live_tests.rs"
PYTHON_TESTS = ROOT / "crates" / "plenora-database-py" / "python" / "tests"
RESULT = ROOT / "assurance-results" / "oracle-reference.json"
TIMEOUT = 30 * 60
REQUIRED_LIVE_TESTS = frozenset(
    {
        "live_thin_driver_crud_merge_stream_and_rollback",
        "live_driver_errors_are_redacted",
        "live_type_fidelity_includes_utc_timestamptz_and_lobs",
        "live_spatial_catalog_portable_predicates_and_arrow_wkb",
        "live_arrow_spatial_write_covers_create_append_update_upsert_replace_and_index",
        "live_large_wkb_temporary_blob_bind_is_lossless",
        "live_arrow_scalar_create_and_read_preserves_supported_types",
    }
)
PYTHON_LIVE_EXPECTED = 5
SERVER_VERSION_SQL = (
    "SELECT VERSION FROM PRODUCT_COMPONENT_VERSION "
    "WHERE PRODUCT LIKE 'Oracle%Database%' FETCH FIRST 1 ROW ONLY"
)


def run(command: list[str], *, environment: dict[str, str] | None = None) -> str:
    """Esegue un passo limitato nel tempo e ne rende visibile l'output."""

    merged_environment = os.environ.copy()
    if environment:
        merged_environment.update(environment)
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=TIMEOUT,
            env=merged_environment,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"timeout durante {command[0]}") from error
    output = f"{completed.stdout}{completed.stderr}"
    if output:
        print(output, end="")
    if completed.returncode:
        raise RuntimeError(f"passo fallito: {' '.join(command[:4])}")
    return output


def compose(arguments: list[str]) -> list[str]:
    return ["docker", "compose", "-f", str(COMPOSE_FILE), *arguments]


def reference_contract() -> dict[str, str]:
    document = json.loads(REFERENCE_FILE.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise RuntimeError("schema del riferimento Oracle non supportato")
    reference = document.get("reference")
    if not isinstance(reference, dict):
        raise TypeError("riferimento Oracle assente")
    required = {"server_major", "image", "digest", "platform", "service"}
    if not required.issubset(reference):
        raise RuntimeError("riferimento Oracle incompleto")
    digest = reference["digest"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise RuntimeError("digest Oracle non immutabile")
    compose_source = COMPOSE_FILE.read_text(encoding="utf-8")
    if f"{reference['image']}@{digest}" not in compose_source:
        raise RuntimeError("Compose Oracle diverge dal digest di riferimento")
    if f"platform: {reference['platform']}" not in compose_source:
        raise RuntimeError("Compose Oracle diverge dalla piattaforma qualificata")
    return reference


def live_source_inventory() -> set[str]:
    return live_inventory.source_inventory(
        [LIVE_SOURCE], keep=lambda name: name.startswith("live_")
    )


def validate_live_run(listing: str, output: str) -> list[str]:
    source = live_source_inventory()
    listed = live_inventory.listed_tests(listing, keep=lambda name: "live_" in name)
    executed = live_inventory.executed_tests(output)
    listed_leaves = {live_inventory.leaf(name) for name in listed}
    if source != listed_leaves:
        raise RuntimeError(
            f"inventario live Oracle divergente: mancanti={sorted(source - listed_leaves)}, "
            f"inattesi={sorted(listed_leaves - source)}"
        )
    if listed != executed:
        raise RuntimeError(
            f"esecuzione live Oracle incompleta: mancanti={sorted(listed - executed)}, "
            f"inattesi={sorted(executed - listed)}"
        )
    missing = REQUIRED_LIVE_TESTS - listed_leaves
    if missing:
        raise RuntimeError(f"oracoli live Oracle mancanti: {sorted(missing)}")
    return sorted(listed)


def container_identity(reference: dict[str, str]) -> dict[str, str]:
    container = run(compose(["ps", "-q", "oracle"])).strip()
    if not container:
        raise RuntimeError("container Oracle Compose assente")
    state = run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}|{{.State.Health.Status}}|{{.Image}}",
            container,
        ]
    ).strip()
    status, health, image_id = state.split("|", maxsplit=2)
    if (status, health) != ("running", "healthy"):
        raise RuntimeError(f"container Oracle non healthy: {status}|{health}")
    configured = run(
        ["docker", "inspect", "--format", "{{.Config.Image}}", container]
    ).strip()
    expected = f"{reference['image']}@{reference['digest']}"
    if configured != expected:
        raise RuntimeError("container Oracle avviato da un riferimento inatteso")
    return {"container_id": container, "image_id": image_id}


def test_environments(
    reference: dict[str, str], container: str
) -> tuple[dict[str, str], dict[str, str]]:
    """Deriva le credenziali dal fixture avviato, senza copiarle nel gate."""

    user = container_variable(container, "APP_USER")
    password = container_variable(container, "APP_USER_PASSWORD")
    rust = {
        "PLENORA_ORACLE_HOST": "127.0.0.1",
        "PLENORA_ORACLE_PORT": "1521",
        "PLENORA_ORACLE_SERVICE": reference["service"],
        "PLENORA_ORACLE_USER": user,
        "PLENORA_ORACLE_PASSWORD": password,
    }
    python = {
        "PLENORA_TEST_ORACLE_HOST": rust["PLENORA_ORACLE_HOST"],
        "PLENORA_TEST_ORACLE_PORT": rust["PLENORA_ORACLE_PORT"],
        "PLENORA_TEST_ORACLE_SERVICE": rust["PLENORA_ORACLE_SERVICE"],
        "PLENORA_TEST_ORACLE_USER": user,
        "PLENORA_TEST_ORACLE_PASSWORD": password,
        "PLENORA_TEST_ORACLE_TLS_MODE": "insecure_local",
    }
    return rust, python


def run_cli_probe(environment: dict[str, str]) -> dict[str, object]:
    environment = environment | {
        "PLENORA_ORACLE_CLI_SECRET": environment["PLENORA_ORACLE_PASSWORD"]
    }
    output = run(
        [
            str(ROOT / "target" / "debug" / "plenora-database"),
            "database-probe",
            "oracle",
            "PLENORA_ORACLE_CLI_SECRET",
            "127.0.0.1",
            environment["PLENORA_ORACLE_SERVICE"],
            environment["PLENORA_ORACLE_USER"],
            "1521",
            "--tls-mode",
            "disable",
        ],
        environment=environment,
    )
    for line in reversed(output.splitlines()):
        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue
        connection = document.get("connection", {})
        capabilities = document.get("capabilities", {})
        if (
            connection.get("provider") != "oracle"
            or capabilities.get("provider") != "oracle"
        ):
            raise RuntimeError("probe CLI Oracle con identita provider inattesa")
        transactions = capabilities.get("transactions", {})
        if (
            transactions.get("single_transaction") is not True
            or transactions.get("savepoints") is not True
            or transactions.get("scope") != "transaction"
            or transactions.get("transactional_ddl") is not False
        ):
            raise RuntimeError(
                "capability transazionali Oracle non coerenti con le prove live"
            )
        reads = capabilities.get("reads", {})
        if not all(
            reads.get(name) is True
            for name in (
                "streaming",
                "server_cursor",
                "pagination",
                "projection",
                "filter",
                "ordering",
            )
        ) or reads.get("resumable") is not False:
            raise RuntimeError("capability read Oracle non coerenti con le prove live")
        writes = capabilities.get("writes", {})
        if not all(
            writes.get(name) is True
            for name in (
                "create",
                "append",
                "update",
                "upsert",
                "replace",
                "delete_by_keys",
                "bulk",
                "rollback_on_failure",
            )
        ) or any(
            writes.get(name) is not False
            for name in ("truncate_insert", "array_binding", "returning")
        ):
            raise RuntimeError("capability write Oracle non coerenti con le prove live")
        spatial = capabilities.get("spatial", {})
        if not all(
            spatial.get(name) is True
            for name in (
                "read_wkb",
                "write_wkb",
                "geometry",
                "spatial_index",
                "mixed_geometry_types",
            )
        ) or spatial.get("geography") is not False:
            raise RuntimeError("capability Spatial Oracle non coerenti con le prove live")
        return document
    raise RuntimeError("probe CLI Oracle senza documento JSON")


def build_and_run_python_live(environment: dict[str, str]) -> str:
    wheel_dir = ROOT / "target" / "oracle-wheel"
    wheel_dir.mkdir(parents=True, exist_ok=True)
    run(
        [
            "maturin",
            "build",
            "--locked",
            "--release",
            "--out",
            str(wheel_dir),
            "--manifest-path",
            str(ROOT / "crates" / "plenora-database-py" / "Cargo.toml"),
        ]
    )
    wheels = list(wheel_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("build Oracle non ha prodotto esattamente un wheel")
    run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--force-reinstall",
            "--no-deps",
            str(wheels[0]),
        ]
    )
    with tempfile.TemporaryDirectory(prefix="plenora-oracle-tests-") as temporary:
        target = Path(temporary) / "tests"
        shutil.copytree(PYTHON_TESTS, target)
        output = run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                str(target / "test_oracle_session.py"),
            ],
            environment=environment,
        )
    if not re.search(rf"\b{PYTHON_LIVE_EXPECTED} passed in ", output):
        raise RuntimeError("matrice Python Oracle non completa")
    script = (
        "import os; import plenora_database as p; "
        "c=p.EngineConfig('oracle',host=os.environ['PLENORA_TEST_ORACLE_HOST'],"
        "database=os.environ['PLENORA_TEST_ORACLE_SERVICE'],"
        "user=os.environ['PLENORA_TEST_ORACLE_USER'],"
        "password=os.environ['PLENORA_TEST_ORACLE_PASSWORD'],"
        "port=int(os.environ['PLENORA_TEST_ORACLE_PORT']),"
        "tls_mode=os.environ['PLENORA_TEST_ORACLE_TLS_MODE']); "
        "s=p.engine_from_url(c).session(); "
        f"print(s.execute_scalar({SERVER_VERSION_SQL!r})); s.close()"
    )
    return (
        run([sys.executable, "-c", script], environment=environment)
        .strip()
        .splitlines()[-1]
    )


def main() -> int:
    steps: list[str] = []
    try:
        reference = reference_contract()
        steps.append("immutable_amd64_reference")
        container = container_identity(reference)
        steps.append("container_health_and_identity")
        rust_environment, python_environment = test_environments(
            reference, container["container_id"]
        )
        run(
            [
                "cargo",
                "clippy",
                "--locked",
                "-p",
                "plenora-db-oracle",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
            environment=rust_environment,
        )
        steps.append("clippy_deny_warnings")
        run(
            ["cargo", "test", "--locked", "-p", "plenora-db-oracle", "--lib"],
            environment=rust_environment,
        )
        steps.append("offline_unit_tests")
        listing = run(
            [
                "cargo",
                "test",
                "--locked",
                "-p",
                "plenora-db-oracle",
                "live_",
                "--",
                "--list",
                "--ignored",
            ],
            environment=rust_environment,
        )
        live = run(
            [
                "cargo",
                "test",
                "--locked",
                "-p",
                "plenora-db-oracle",
                "live_",
                "--",
                "--ignored",
                "--test-threads=1",
            ],
            environment=rust_environment,
        )
        executed = validate_live_run(listing, live)
        steps.append("live_inventory_matches_sources_and_run")
        run(
            [
                "cargo",
                "build",
                "--locked",
                "-p",
                "plenora-database-cli",
                "--no-default-features",
                "--features",
                "oracle",
            ]
        )
        probe = run_cli_probe(rust_environment)
        steps.append("public_cli_probe")
        server_version = build_and_run_python_live(python_environment)
        if not server_version.startswith(reference["server_major"] + "."):
            raise RuntimeError("major Oracle avviata diversa dal riferimento")
        steps.append("installed_python_wheel_sync_async_live")
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"oracle reference gate: {error}", file=sys.stderr)
        return 1

    report = {
        "schema_version": 1,
        "gate": "oracle-reference-v1",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "platform": reference["platform"],
        "database_connections_opened": True,
        "reference": reference,
        "container": container,
        "server_version": server_version,
        "live_tests": {"expected": len(executed), "passed": len(executed), "failed": 0},
        "python_live_tests": {
            "expected": PYTHON_LIVE_EXPECTED,
            "passed": PYTHON_LIVE_EXPECTED,
            "failed": 0,
        },
        "executed_live_tests": executed,
        "steps": steps,
        "cli_provider": probe["connection"]["provider"],
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
