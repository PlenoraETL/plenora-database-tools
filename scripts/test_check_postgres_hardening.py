#!/usr/bin/env python3
"""Regressioni del gate PostgreSQL hardening e dei probe CLI mTLS."""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
GATE = ROOT / "scripts" / "check_postgres_hardening.py"
SPEC = importlib.util.spec_from_file_location("postgres_hardening_gate", GATE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("gate PostgreSQL hardening non importabile")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class PostgresHardeningGateTests(unittest.TestCase):
    def test_compose_network_is_observed_and_requires_both_aliases(self) -> None:
        labels = json.dumps({"com.docker.compose.project": "plenora-review-42"})
        normal_network = json.dumps(
            {"plenora-review-42_default": {"Aliases": ["dataflow-postgres"]}}
        )
        tls_network = json.dumps(
            {"plenora-review-42_default": {"Aliases": ["dataflow-postgres-tls"]}}
        )
        with patch.object(
            gate,
            "docker_value",
            side_effect=[labels, normal_network, labels, tls_network],
        ):
            self.assertEqual(gate.postgres_network(), "plenora-review-42_default")
        self.assertFalse(hasattr(gate, "NETWORK"))

    def test_live_cli_runner_executes_both_postgres_routes(self) -> None:
        output = "\n".join(
            [
                "test live_database_probe_postgres_private_ca_mtls ... ok",
                "test live_legacy_postgres_probe_private_ca_mtls ... ok",
                "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out",
            ]
        )
        with (
            patch.object(gate, "cargo", return_value=["cargo-test"]) as cargo,
            patch.object(gate, "run", return_value=output) as run,
        ):
            gate.run_live_cli_probes("dsn")
        arguments = cargo.call_args.args[0]
        self.assertEqual(
            arguments[:5],
            ["test", "-p", "plenora-database-cli", "--test", "live_probe"],
        )
        self.assertEqual(arguments[5], "private_ca_mtls")
        self.assertIn("--ignored", arguments)
        self.assertIn("--test-threads=1", arguments)
        self.assertTrue(run.call_args.kwargs["capture"])


if __name__ == "__main__":
    unittest.main()
