from __future__ import annotations

import pathlib
import unittest
from unittest.mock import patch

from scripts.check_mysql_matrix import (
    CHECKER_SOURCE,
    EXPECTED_LIVE_DEFAULT_TESTS,
    EXPECTED_LIVE_REFERENCE_TESTS,
    EXPECTED_UNIT_TESTS,
    INIT_FIXTURE,
    MATRIX,
    TLS_FIXTURE,
    discard,
    entry_report,
    live_default_command,
    live_inventory,
    live_reference_command,
    qualify,
    quiet,
    server_command,
    verify_candidate_unit,
    verify_hardening,
    verify_live_inventory,
)
from scripts.check_mysql_reference import (
    EXPECTED_DIGEST,
    EXPECTED_LIVE_REFERENCE_TESTS as REFERENCE_LIVE_TESTS,
    EXPECTED_UNIT_TESTS as REFERENCE_UNIT_TESTS,
)
from scripts.mysql_references import BASELINE, COMPATIBILITY


def cargo_output(names: set[str]) -> str:
    body = "\n".join(f"test {name} ... ok" for name in sorted(names))
    return f"{body}\ntest result: ok. {len(names)} passed; 0 failed; 0 ignored;\n"


class MysqlMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.entry = MATRIX[0]
        self.probe = {
            "version": self.entry.exact_version,
            "require_secure_transport": "ON",
            "local_infile": "OFF",
            "tls_version": "TLSv1.3",
        }

    # --- immagini fissate -------------------------------------------------

    def test_matrix_pins_the_declared_baseline_and_compatibility_by_digest(
        self,
    ) -> None:
        """La matrice e esattamente quella dichiarata: baseline 9.x piu 8.4 e 8.0.

        Il digest e la sola prova della versione avviata: un tag muta, un
        manifest index no. Il controllo sulla lunghezza impedisce che qualcuno
        rimpiazzi un digest con un tag mobile.
        """

        self.assertEqual(len(MATRIX), 3)
        self.assertEqual(
            [entry.version_prefix for entry in MATRIX], ["9.7.", "8.4.", "8.0."]
        )
        self.assertEqual(
            [entry.exact_version for entry in MATRIX], ["9.7.2", "8.4.11", "8.0.46"]
        )
        self.assertEqual([entry.role for entry in MATRIX][0], "baseline")
        self.assertEqual(
            {entry.role for entry in MATRIX[1:]}, {"compatibility"}
        )
        for entry in MATRIX:
            self.assertTrue(entry.image.startswith("mysql@sha256:"), entry.image)
            self.assertEqual(len(entry.digest), len("sha256:") + 64)
        self.assertEqual(MATRIX[0].image, f"mysql@{EXPECTED_DIGEST}")

    def test_the_reference_gate_qualifies_the_matrix_baseline(self) -> None:
        """Il gate reference e la matrice non possono divergere sulla baseline."""

        self.assertEqual(BASELINE, MATRIX[0])
        self.assertEqual(tuple(COMPATIBILITY), tuple(MATRIX[1:]))

    def test_matrix_containers_and_aliases_are_distinct_but_certificate_valid(
        self,
    ) -> None:
        self.assertEqual(len({entry.container for entry in MATRIX}), len(MATRIX))
        self.assertEqual(len({entry.tls_volume for entry in MATRIX}), len(MATRIX))
        self.assertEqual(len({entry.ca_volume for entry in MATRIX}), len(MATRIX))
        for entry in MATRIX:
            # Il certificato della fixture e emesso per dataflow-mysql: senza
            # l'alias la verifica TLS positiva non sarebbe provabile.
            self.assertIn("dataflow-mysql", entry.aliases)
            self.assertIn("mysql-hostname-mismatch", entry.aliases)

    # --- indurimento del server ------------------------------------------

    def test_server_command_keeps_tls_required_and_local_infile_off(self) -> None:
        command = server_command(self.entry)
        self.assertIn("--require-secure-transport=ON", command)
        self.assertIn("--local-infile=OFF", command)
        self.assertIn("--ssl-ca=/etc/mysql/tls/ca.pem", command)
        self.assertIn("--ssl-cert=/etc/mysql/tls/server.pem", command)
        self.assertIn("--ssl-key=/etc/mysql/tls/server.key", command)
        self.assertTrue(
            any(argument.startswith("--sql-mode=") for argument in command)
        )
        self.assertTrue(
            any("STRICT_TRANS_TABLES" in argument for argument in command)
        )

    def test_hardening_probe_is_fail_closed(self) -> None:
        verify_hardening(self.entry, self.probe)
        for key, value in (
            ("require_secure_transport", "OFF"),
            ("local_infile", "ON"),
            ("tls_version", ""),
            ("version", "5.7.44"),
            ("version", "8.0.99"),
        ):
            broken = dict(self.probe)
            broken[key] = value
            with self.assertRaises(RuntimeError):
                verify_hardening(self.entry, broken)

    # --- inventario live esplicito ---------------------------------------

    def test_matrix_reuses_the_shared_inventory_without_weakening_it(self) -> None:
        self.assertEqual(EXPECTED_LIVE_REFERENCE_TESTS, REFERENCE_LIVE_TESTS)
        self.assertEqual(EXPECTED_UNIT_TESTS, REFERENCE_UNIT_TESTS)
        self.assertTrue(EXPECTED_LIVE_DEFAULT_TESTS)
        self.assertFalse(
            EXPECTED_LIVE_DEFAULT_TESTS & EXPECTED_LIVE_REFERENCE_TESTS,
            "un test live non puo appartenere a due runner",
        )

    def test_the_matrix_never_skips_a_live_test(self) -> None:
        """L'unico `--skip` ammesso e quello che isola i test unit dal server."""

        source = CHECKER_SOURCE.read_text(encoding="utf-8")
        self.assertEqual(source.count('"--skip"'), 1)
        for command in (live_default_command(), live_reference_command()):
            self.assertNotIn("--skip", command)
            self.assertIn("--test-threads=1", command)
            self.assertIn("live_", command)
        self.assertNotIn("--ignored", live_default_command())
        self.assertIn("--ignored", live_reference_command())

    def test_live_inventory_parses_only_passing_tests(self) -> None:
        output = cargo_output(EXPECTED_LIVE_REFERENCE_TESTS)
        self.assertEqual(live_inventory(output), EXPECTED_LIVE_REFERENCE_TESTS)
        failing = output.replace(
            "test live_tests::live_append_commits_a_single_transaction_and_reads_back_exactly ... ok",
            "test live_tests::live_append_commits_a_single_transaction_and_reads_back_exactly ... FAILED",
        )
        self.assertNotIn(
            "live_tests::live_append_commits_a_single_transaction_and_reads_back_exactly",
            live_inventory(failing),
        )

    def test_inventory_verification_rejects_drift_in_both_directions(self) -> None:
        expected = set(EXPECTED_LIVE_REFERENCE_TESTS)
        verify_live_inventory(
            self.entry, "live reference", expected, cargo_output(expected)
        )
        missing = set(expected)
        missing.remove(
            "live_tests::live_append_spatial_xy_preserves_srid_and_coordinates"
        )
        with self.assertRaisesRegex(RuntimeError, "mancanti"):
            verify_live_inventory(
                self.entry, "live reference", expected, cargo_output(missing)
            )
        extra = set(expected)
        extra.add("live_tests::live_unexpected_new_probe")
        with self.assertRaisesRegex(RuntimeError, "inattesi"):
            verify_live_inventory(
                self.entry, "live reference", expected, cargo_output(extra)
            )
        substituted = set(expected)
        substituted.remove(
            "live_tests::live_append_batch_failure_rolls_back_without_partial_rows"
        )
        substituted.add("live_tests::live_same_count_replacement")
        with self.assertRaisesRegex(RuntimeError, "mancanti"):
            verify_live_inventory(
                self.entry, "live reference", expected, cargo_output(substituted)
            )

    # --- report -----------------------------------------------------------

    def test_entry_report_publishes_image_version_and_inventory(self) -> None:
        report = entry_report(self.entry, self.probe)
        self.assertEqual(report["image"], self.entry.image)
        self.assertEqual(report["label"], self.entry.label)
        self.assertEqual(report["role"], self.entry.role)
        self.assertEqual(report["product_version"], self.probe["version"])
        self.assertEqual(
            report["live_reference_tests"],
            {
                "expected": len(EXPECTED_LIVE_REFERENCE_TESTS),
                "passed": len(EXPECTED_LIVE_REFERENCE_TESTS),
            },
        )
        self.assertEqual(
            report["live_default_tests"],
            {
                "expected": len(EXPECTED_LIVE_DEFAULT_TESTS),
                "passed": len(EXPECTED_LIVE_DEFAULT_TESTS),
            },
        )
        self.assertEqual(report["hardening"]["require_secure_transport"], "ON")
        self.assertEqual(report["hardening"]["local_infile"], "OFF")

    def test_checker_removes_every_resource_it_creates(self) -> None:
        """La pulizia nomina container e volumi di ciascun riferimento.

        Il repository non ha piu workflow CI che ripuliscano al posto del
        gate: se `discard` dimentica una risorsa, la matrice successiva
        riparte da uno stato sporco.
        """

        for entry in MATRIX:
            with patch("scripts.check_mysql_matrix.quiet", return_value=0) as quiet:
                discard(entry)
            calls = [call.args[0] for call in quiet.call_args_list]
            self.assertIn(
                ["docker", "rm", "--force", "--volumes", entry.container], calls
            )
            self.assertIn(
                ["docker", "volume", "rm", "--force", entry.tls_volume], calls
            )
            self.assertIn(
                ["docker", "volume", "rm", "--force", entry.ca_volume], calls
            )

    def test_checker_cleans_up_when_start_fails(self) -> None:
        calls: list[str] = []
        with (
            patch(
                "scripts.check_mysql_matrix.discard",
                side_effect=lambda _entry: calls.append("discard"),
            ),
            patch("scripts.check_mysql_matrix.generate_tls"),
            patch(
                "scripts.check_mysql_matrix.start",
                side_effect=RuntimeError("start failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                qualify(self.entry, {})
        self.assertEqual(calls, ["discard", "discard"])

    def test_quiet_cleanup_tolerates_a_missing_docker_executable(self) -> None:
        with patch(
            "scripts.check_mysql_matrix.subprocess.run",
            side_effect=FileNotFoundError,
        ):
            self.assertEqual(quiet(["docker", "network", "rm", "fixture"]), 127)

    def test_inventory_and_unit_suite_run_before_matrix_network_creation(self) -> None:
        calls: list[str] = []
        with (
            patch(
                "scripts.check_mysql_matrix.validate_inventory",
                side_effect=lambda: calls.append("inventory"),
            ),
            patch(
                "scripts.check_mysql_matrix.verify_candidate_unit",
                side_effect=lambda: calls.append("unit"),
            ),
            patch(
                "scripts.check_mysql_matrix.ensure_network",
                side_effect=RuntimeError("stop after network"),
            ),
        ):
            from scripts.check_mysql_matrix import main

            self.assertEqual(main(), 1)
        self.assertEqual(calls, ["inventory", "unit"])

    def test_unit_runner_isolates_the_suite_from_the_server(self) -> None:
        captured: list[list[str]] = []

        def fake_run(command: list[str], **_kwargs: object) -> str:
            captured.append(command)
            return cargo_output(EXPECTED_UNIT_TESTS)

        with patch("scripts.check_mysql_matrix.run", side_effect=fake_run):
            verify_candidate_unit()

        self.assertEqual(len(captured), 1)
        self.assertIn("--skip", captured[0])
        self.assertIn("live_", captured[0])

    def test_checker_never_embeds_the_fixture_password(self) -> None:
        source = CHECKER_SOURCE.read_text(encoding="utf-8")
        self.assertNotIn("DataFlow_Test_2026!", source)

    def test_versioned_fixture_is_the_one_the_matrix_boots(self) -> None:
        self.assertEqual(
            TLS_FIXTURE.resolve(),
            pathlib.Path("docker/mysql/tls").resolve(),
        )
        self.assertEqual(
            INIT_FIXTURE.resolve(),
            pathlib.Path("docker/mysql/init").resolve(),
        )
        self.assertTrue((TLS_FIXTURE / "generate.sh").is_file())
        self.assertTrue((TLS_FIXTURE / "server.ext").is_file())
        self.assertTrue(any(INIT_FIXTURE.glob("*.sql")))


if __name__ == "__main__":
    unittest.main()
