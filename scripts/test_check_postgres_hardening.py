#!/usr/bin/env python3
"""Regressioni del gate PostgreSQL hardening e dei probe CLI mTLS."""

from __future__ import annotations

import importlib.util
import json
import re
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

import sys

sys.path.insert(0, str(ROOT))

from scripts import compose_network as network_helper  # noqa: E402


class PostgresHardeningGateTests(unittest.TestCase):
    def test_the_gate_discovers_both_networks_and_the_certificate_volume(
        self,
    ) -> None:
        """Rete e volume si chiedono a Docker, non si scrivono.

        I due riferimenti PostgreSQL vivono in progetti Compose distinti,
        quindi su reti distinte e con volumi che portano prefissi distinti. Il
        gate si attacca a entrambe le reti e monta il volume che il container
        TLS dichiara: un nome scritto a mano diventerebbe stale al primo
        rename, e il sintomo sarebbe un mount vuoto, non un errore leggibile.
        """

        plain_labels = json.dumps({"com.docker.compose.project": "plenora-postgres"})
        plain_networks = json.dumps(
            {"plenora-postgres_default": {"Aliases": ["dataflow-postgres"]}}
        )
        tls_labels = json.dumps({"com.docker.compose.project": "plenora-postgres-tls"})
        tls_networks = json.dumps(
            {"plenora-postgres-tls_default": {"Aliases": ["dataflow-postgres-tls"]}}
        )
        with patch.object(
            network_helper,
            "_inspect",
            side_effect=[plain_labels, plain_networks, tls_labels, tls_networks],
        ):
            self.assertEqual(
                gate.compose_network_arguments(*gate.POSTGRES_CONTAINERS),
                [
                    "--network",
                    "plenora-postgres_default",
                    "--network",
                    "plenora-postgres-tls_default",
                ],
            )

        mounts = json.dumps(
            [
                {"Destination": "/var/lib/postgresql/data", "Name": "altro"},
                {"Destination": "/tls", "Name": "plenora-postgres-tls_postgres_tls_certs"},
            ]
        )
        with patch.object(network_helper, "_inspect", side_effect=[mounts]):
            self.assertEqual(
                gate.compose_volume("dataflow-postgres-tls", gate.TLS_CERTS_DESTINATION),
                "plenora-postgres-tls_postgres_tls_certs",
            )

        # Nessun nome di rete o di volume resta scritto nel gate. Il match e
        # sui soli letterali interi: `startup_session_defaults` e il nome di
        # un check, non una rete.
        source = GATE.read_text(encoding="utf-8")
        self.assertFalse(hasattr(gate, "NETWORK"))
        self.assertFalse(hasattr(gate, "TLS_VOLUME"))
        for token in re.findall(r'"([^"]*)"', source):
            self.assertFalse(
                token.endswith("_default") or token.endswith("_postgres_tls_certs"),
                f"nome Compose scritto a mano nel gate: {token}",
            )

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
