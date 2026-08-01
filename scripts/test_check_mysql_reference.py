#!/usr/bin/env python3
"""Regressioni fail-closed della fixture MySQL reference."""

from __future__ import annotations

import unittest
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ROOT / "docker-compose.mysql.yml"
SERVER_EXT = ROOT / "docker" / "mysql" / "tls" / "server.ext"
GENERATOR = ROOT / "docker" / "mysql" / "tls" / "generate.sh"
GATE = ROOT / "scripts" / "check_mysql_reference.py"
WORKFLOW = ROOT / ".github" / "workflows" / "mysql-assurance.yml"
SPEC = importlib.util.spec_from_file_location("mysql_gate", GATE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("gate MySQL non importabile")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class MysqlReferenceFixtureTests(unittest.TestCase):
    def test_gate_rejects_offline_test_count_drift(self) -> None:
        offline_output = "\n".join(f"test offline::{index} ... ok" for index in range(33))
        cargo_calls: list[list[str]] = []

        def run_cargo(arguments: list[str], *, capture: bool = False) -> str:
            cargo_calls.append(arguments)
            if arguments[:3] == ["test", "-p", "plenora-db-mysql"]:
                return offline_output
            return ""

        with (
            patch.object(gate, "validate_fixture"),
            patch.object(gate, "ensure_reference_running"),
            patch.object(gate, "validate_reference", return_value={}),
            patch.object(gate, "run_cargo", side_effect=run_cargo),
        ):
            with self.assertRaisesRegex(RuntimeError, "33.*34"):
                gate.main()

        self.assertFalse(
            any("live_" in arguments for arguments in cargo_calls),
            "il gate deve fermarsi prima dei test live",
        )

    def test_gate_starts_reference_before_inspecting_it(self) -> None:
        calls: list[str] = []

        def stop_after_reference_probe() -> dict[str, str]:
            calls.append("validate_reference")
            raise RuntimeError("stop after reference probe")

        with (
            patch.object(gate, "validate_fixture", side_effect=lambda: calls.append("fixture")),
            patch.object(
                gate,
                "ensure_reference_running",
                side_effect=lambda: calls.append("ensure_running"),
                create=True,
            ),
            patch.object(gate, "validate_reference", side_effect=stop_after_reference_probe),
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after reference probe"):
                gate.main()

        self.assertEqual(calls, ["fixture", "ensure_running", "validate_reference"])

    def test_host_cargo_default_is_covered_by_the_server_certificate(self) -> None:
        gate = GATE.read_text(encoding="utf-8")
        server_ext = SERVER_EXT.read_text(encoding="utf-8")

        self.assertIn('setdefault("PLENORA_MYSQL_HOST", "127.0.0.1")', gate)
        self.assertIn("IP.1 = 127.0.0.1", server_ext)

    def test_version_probe_never_exposes_a_root_password_in_docker_arguments(self) -> None:
        completed = SimpleNamespace(returncode=0, stdout="8.4.11\n", stderr="")
        with (
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
            patch.object(gate.subprocess, "run", return_value=completed) as run,
        ):
            self.assertEqual(gate.mysql_value("SELECT VERSION()"), "8.4.11")

        arguments = run.call_args.args[0]
        self.assertNotIn("root", arguments)
        self.assertNotIn("fixture-secret", repr(arguments))
        self.assertNotIn("input", run.call_args.kwargs)
        self.assertIn('MYSQL_PWD="$MYSQL_PASSWORD"', repr(arguments))

    def test_ca_signing_key_is_not_mounted_into_the_mysql_service(self) -> None:
        compose = COMPOSE.read_text(encoding="utf-8")
        generator = GENERATOR.read_text(encoding="utf-8")
        certgen = compose.split("  mysql-certgen:\n", 1)[1].split("\n  mysql:\n", 1)[0]
        mysql = compose.split("\n  mysql:\n", 1)[1].split("\nvolumes:\n", 1)[0]

        self.assertIn("mysql_ca_private:/ca", certgen)
        self.assertNotIn("mysql_ca_private", mysql)
        self.assertNotIn("-keyout /tls/ca.key", compose)
        self.assertNotIn("-CAkey /tls/ca.key", compose)
        self.assertIn('rm -f "$TLS_DIR/ca.key"', generator)

    def test_root_credential_is_random_and_unused_by_the_healthcheck(self) -> None:
        compose = COMPOSE.read_text(encoding="utf-8")

        self.assertNotIn("MYSQL_ROOT_PASSWORD", compose)
        self.assertIn('MYSQL_RANDOM_ROOT_PASSWORD: "yes"', compose)
        self.assertIn("mysqladmin ping -h localhost -u dataflow", compose)

    def test_cargo_container_password_is_not_embedded_in_docker_arguments(self) -> None:
        with (
            patch.dict(
                gate.os.environ,
                {"PLENORA_MYSQL_GATE_HOST_CARGO": "0"},
                clear=False,
            ),
            patch.object(gate, "mysql_tls_volume", return_value="mysql_tls"),
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
        ):
            command, environment = gate.cargo(["test"])

        self.assertNotIn("fixture-secret", repr(command))
        self.assertIn("PLENORA_MYSQL_PASSWORD", command)
        self.assertIsNotNone(environment)
        self.assertEqual(environment["PLENORA_MYSQL_PASSWORD"], "fixture-secret")

    def test_host_cargo_password_is_only_passed_in_the_process_environment(self) -> None:
        with (
            patch.dict(
                gate.os.environ,
                {
                    "PLENORA_MYSQL_GATE_HOST_CARGO": "1",
                    "PLENORA_MYSQL_CA": "/tmp/mysql-ca.pem",
                },
                clear=True,
            ),
            patch.object(gate, "fixture_password", return_value="fixture-secret"),
        ):
            command, environment = gate.cargo(["test"])

        self.assertEqual(command, ["cargo", "test"])
        self.assertNotIn("fixture-secret", repr(command))
        self.assertIsNotNone(environment)
        self.assertEqual(environment["PLENORA_MYSQL_PASSWORD"], "fixture-secret")

    def test_gate_source_does_not_duplicate_the_fixture_password(self) -> None:
        self.assertNotIn("DataFlow_Test_2026!", GATE.read_text(encoding="utf-8"))

    def test_fixture_gate_runs_contract_and_partial_regeneration_tests(self) -> None:
        with patch.object(gate, "run") as run:
            gate.validate_fixture()

        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(
            commands[0],
            [sys.executable, str(ROOT / "scripts" / "test_check_mysql_reference.py")],
        )
        self.assertIn(gate.EXPECTED_REFERENCE, commands[1])
        self.assertIn("/fixture/test_generate.sh", commands[1])

    def test_mysql_workflow_watches_all_tls_assurance_inputs(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('      - "docker/mysql/tls/**"', workflow)
        self.assertIn('      - "scripts/test_check_mysql_reference.py"', workflow)

    def test_host_cargo_workflow_exports_the_generated_ca_path(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("docker cp dataflow-mysql:/etc/mysql/tls/ca.pem", workflow)
        self.assertIn("PLENORA_MYSQL_CA=", workflow)
        self.assertIn('>> "$GITHUB_ENV"', workflow)
        self.assertNotIn("PLENORA_MYSQL_PASSWORD:", workflow)

    def test_host_cargo_workflow_resolves_the_tls_mismatch_alias(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        alias = "127.0.0.1 mysql-hostname-mismatch"
        gate = "python3 scripts/check_mysql_reference.py"
        self.assertIn(alias, workflow)
        self.assertIn(gate, workflow)
        self.assertLess(
            workflow.index(alias),
            workflow.index(gate),
            "alias /etc/hosts deve essere registrato prima del reference gate",
        )


if __name__ == "__main__":
    unittest.main()
