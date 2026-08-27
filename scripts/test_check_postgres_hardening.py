#!/usr/bin/env python3
"""Regressioni del gate PostgreSQL hardening e dei probe CLI mTLS."""

from __future__ import annotations

import ast
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

    OUTPUT = "\n".join(
        [
            "test live_database_probe_postgres_private_ca_mtls ... ok",
            "test live_legacy_postgres_probe_private_ca_mtls ... ok",
            "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out",
        ]
    )

    def test_live_cli_probe_covers_both_postgres_routes(self) -> None:
        with patch.object(gate, "cargo", return_value=["cargo-test"]) as cargo:
            gate.live_cli_probe_command("dsn")
        arguments = cargo.call_args.args[0]
        self.assertEqual(
            arguments[:5],
            ["test", "-p", "plenora-database-cli", "--test", "live_probe"],
        )
        self.assertEqual(arguments[5], "private_ca_mtls")
        self.assertIn("--ignored", arguments)
        self.assertIn("--test-threads=1", arguments)
        self.assertEqual(cargo.call_args.kwargs["tls_dsn"], "dsn")

    def test_the_probe_validation_requires_exactly_those_two_routes(self) -> None:
        gate.validate_live_cli_probes(self.OUTPUT)
        with self.assertRaisesRegex(RuntimeError, "probe CLI PostgreSQL inattesi"):
            gate.validate_live_cli_probes(
                "test live_database_probe_postgres_private_ca_mtls ... ok\n"
                "test result: ok. 1 passed; 0 failed; 0 ignored;"
            )

    def test_the_report_publishes_the_steps_it_ran(self) -> None:
        """Nessuna lista tematica scritta a mano nel verdetto.

        Erano cinquantotto voci, nessuna legata al nome di un test: togliere il
        test che ne sosteneva una non toglieva la voce e non faceva fallire
        niente.
        """

        source = GATE.read_text(encoding="utf-8")
        self.assertIn('"steps": steps,', source)
        self.assertNotIn('"checks"', source)

    #: Ogni passo che il gate compie. Toglierne uno dal gate fa fallire qui.
    EXPECTED_STEPS = (
        "gate_self_test",
        "tls_reference_started",
        "container_health",
        "tls_container_health",
        "rustfmt",
        "clippy_deny_warnings",
        "core_and_sql_unit_tests",
        "provider_suite_against_plaintext_and_tls",
        "private_ca_mtls_test_observed",
        "public_cli_probes_against_private_ca",
    )

    def test_every_step_is_recorded_after_the_command_that_produces_it(self) -> None:
        source = GATE.read_text(encoding="utf-8")
        body = source[source.index("def main(") :]
        self.assertLess(
            body.index("steps: list[str] = []"),
            body.index("steps.append("),
            "i passi vanno dichiarati vuoti e riempiti mentre accadono",
        )
        # `step()` esegue e **poi** registra: e l'ordine a rendere vera la
        # dichiarazione, non il nome della funzione.
        helper = body[body.index("def step(") : body.index("    try:")]
        self.assertLess(helper.index("run(command"), helper.index("steps.append(name)"))
        # La prima voce non registrata da `step` arriva comunque dopo il
        # controllo che la produce.
        self.assertLess(
            body.index('raise RuntimeError("container PostgreSQL non healthy")'),
            body.index('steps.append("container_health")'),
        )

    def test_no_command_bypasses_the_step_recorder(self) -> None:
        """Un comando eseguito fuori da `step()` non comparirebbe nel verdetto.

        Il conteggio usa l'AST dell'intero modulo, non il solo corpo del `try`,
        così comprende anche le chiamate nascoste in helper.

        Le due eccezioni sono le letture di stato dei container, che non sono
        passi ma condizioni: il loro esito diventa `container_health` e
        `tls_container_health` subito dopo.
        """

        tree = ast.parse(GATE.read_text(encoding="utf-8"))

        def direct_calls(node: ast.AST) -> list[ast.Call]:
            """Le chiamate di questa funzione, non quelle delle sue annidate."""

            found: list[ast.Call] = []
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                if (
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id == "run"
                ):
                    found.append(child)
                found.extend(direct_calls(child))
            return found

        callers = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            for _ in direct_calls(node)
        ]
        self.assertEqual(
            sorted(callers),
            ["main", "main", "step"],
            "un comando del gate non passa da step(): non finirebbe nel verdetto",
        )

    def test_no_subprocess_escapes_the_single_executor(self) -> None:
        """`subprocess.run` sta in `run()` e in nessun altro posto.

        La guardia sulle chiamate a `run` non vedrebbe un helper che lancia un
        processo per conto proprio: contare i wrapper non basta se qualcuno
        salta il wrapper.

        L'unica funzione ammessa è l'esecutore. Un'eccezione senza chiamanti
        sarebbe un permesso preventivo per bypassare la registrazione.

        Sorvegliate tutte le primitive che creano un processo, non la sola
        grafia `subprocess.run`: `Popen`, `check_call` e `check_output` fanno
        la stessa cosa con un nome diverso.

        Resta fuori, e per scelta dichiarata, la scoperta di rete e volume
        (`scripts/compose_network`): sono letture che servono a costruire il
        comando, non passi del gate. Registrarle fra i `steps` direbbe che il
        gate ha verificato qualcosa che non ha verificato.
        """

        # Ogni primitiva del modulo che crea un processo. `getoutput` e
        # `getstatusoutput` lo fanno passando da una shell, e non
        # comparivano.
        spawning = {
            "run",
            "Popen",
            "call",
            "check_call",
            "check_output",
            "getoutput",
            "getstatusoutput",
        }
        tree = ast.parse(GATE.read_text(encoding="utf-8"))

        # Il modulo va importato con il suo nome: un alias (`import subprocess
        # as sp`) o un import diretto (`from subprocess import check_call`)
        # renderebbero invisibile la chiamata a una guardia che riconosce la
        # sola grafia `subprocess.<primitiva>`.
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "subprocess":
                        self.assertIsNone(
                            alias.asname,
                            "`subprocess` importato con un alias: la guardia non "
                            "lo vedrebbe",
                        )
            elif isinstance(node, ast.ImportFrom) and node.module == "subprocess":
                self.fail(
                    "import diretto da `subprocess`: la guardia riconosce la "
                    "sola forma `subprocess.<primitiva>`"
                )
            elif isinstance(node, ast.Assign) and any(
                isinstance(value, ast.Name) and value.id == "subprocess"
                for value in [node.value]
            ):
                self.fail(
                    "il modulo `subprocess` viene riassegnato a un altro nome: "
                    "vietare l'alias nell'import non basta se poi lo si rilega"
                )

        # Ogni chiamata del modulo, attribuita alla funzione piu vicina che la
        # contiene — `<module>` per quelle di livello superiore. Guardare le
        # sole `FunctionDef` lasciava fuori il livello di modulo e le `async
        # def`.
        owner: dict[int, str] = {}
        for node in ast.walk(tree):
            name = (
                node.name
                if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
                else None
            )
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call):
                    owner.setdefault(id(inner), name or "<module>")
                    if name is not None:
                        owner[id(inner)] = name

        owners: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = node.func
            if (
                isinstance(called, ast.Attribute)
                and called.attr in spawning
                and isinstance(called.value, ast.Name)
                and called.value.id == "subprocess"
            ):
                owners.append(owner.get(id(node), "<module>"))
        self.assertEqual(
            sorted(owners),
            ["run"],
            "un processo viene lanciato fuori dall'unico esecutore del gate",
        )

    def test_the_probe_helper_only_builds_its_command(self) -> None:
        """Chi costruisce non esegue: eseguire spetta all'unico esecutore."""

        tree = ast.parse(GATE.read_text(encoding="utf-8"))
        names = {
            node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
        }
        self.assertIn("live_cli_probe_command", names)
        self.assertIn("validate_live_cli_probes", names)
        self.assertNotIn("run_live_cli_probes", names)

    def test_the_report_names_every_step_the_gate_performs(self) -> None:
        source = GATE.read_text(encoding="utf-8")
        for name in self.EXPECTED_STEPS:
            self.assertIn(f'"{name}"', source, f"il gate non registra {name}")

    def test_the_private_ca_test_is_required_by_name(self) -> None:
        """L'unica prova mTLS del repository non puo passare per un `ok`.

        Il test ritorna subito quando mancano le quattro variabili TLS, e
        `cargo` lo conta comunque fra i passati: il gate del riferimento lo
        dichiara infatti non qualificante. Qui le variabili ci sono, quindi la
        prova vale — ed e questo gate a doverla pretendere per nome.
        """

        self.assertIn(
            "live_private_ca_mtls_and_cancellation_when_configured",
            gate.REQUIRED_LIVE_TESTS,
        )
        observed = (
            "test test_suite::tests::live_private_ca_mtls_and_cancellation_when_configured"
            " ... ok"
        )
        self.assertEqual(
            gate.validate_required_live_tests(observed),
            ["live_private_ca_mtls_and_cancellation_when_configured"],
        )

    def test_a_missing_private_ca_test_fails_the_gate(self) -> None:
        with self.assertRaises(RuntimeError) as raised:
            gate.validate_required_live_tests(
                "test test_suite::tests::qualcos_altro ... ok"
            )
        self.assertIn("live_private_ca_mtls", str(raised.exception))

    def test_a_failed_private_ca_test_is_not_counted_as_executed(self) -> None:
        with self.assertRaises(RuntimeError):
            gate.validate_required_live_tests(
                "test test_suite::tests::"
                "live_private_ca_mtls_and_cancellation_when_configured ... FAILED"
            )


if __name__ == "__main__":
    unittest.main()
