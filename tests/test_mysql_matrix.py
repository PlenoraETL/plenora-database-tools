from __future__ import annotations

import pathlib
import unittest
from unittest.mock import patch

from scripts.check_mysql_matrix import (
    CHECKER_SOURCE,
    EXPECTED_LIVE_TESTS,
    INIT_FIXTURE,
    MATRIX,
    TLS_FIXTURE,
    MatrixEntry,
    discard,
    entry_report,
    live_inventory,
    qualify,
    server_command,
    test_command,
    verify_candidate_offline,
    verify_hardening,
    verify_live_inventory,
)
from scripts.check_mysql_reference import (
    EXPECTED_DIGEST,
    EXPECTED_LIVE_TESTS as REFERENCE_LIVE_TESTS,
)


def cargo_output(names: set[str]) -> str:
    body = "\n".join(f"test live_tests::{name} ... ok" for name in sorted(names))
    return f"{body}\ntest result: ok. {len(names)} passed; 0 failed; 0 ignored;\n"


class MysqlMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.entry = MATRIX[0]
        self.probe = {
            "version": f"{self.entry.version_prefix}46",
            "require_secure_transport": "ON",
            "local_infile": "OFF",
            "tls_version": "TLSv1.3",
        }

    # --- immagini fissate -------------------------------------------------

    def test_matrix_pins_exactly_eight_zero_and_eight_four_by_digest(self) -> None:
        self.assertEqual(len(MATRIX), 2)
        self.assertEqual(
            [entry.version_prefix for entry in MATRIX], ["8.0.", "8.4."]
        )
        for entry in MATRIX:
            self.assertTrue(entry.image.startswith("mysql@sha256:"), entry.image)
            self.assertEqual(len(entry.digest), 64)
        self.assertEqual(MATRIX[1].image, f"mysql@{EXPECTED_DIGEST}")

    def test_matrix_containers_and_aliases_are_distinct_but_certificate_valid(
        self,
    ) -> None:
        self.assertEqual(len({entry.container for entry in MATRIX}), len(MATRIX))
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
        ):
            broken = dict(self.probe)
            broken[key] = value
            with self.assertRaises(RuntimeError):
                verify_hardening(self.entry, broken)

    # --- inventario live esplicito ---------------------------------------

    def test_matrix_reuses_the_shared_live_inventory_without_weakening_it(
        self,
    ) -> None:
        self.assertEqual(EXPECTED_LIVE_TESTS, REFERENCE_LIVE_TESTS)
        self.assertEqual(len(EXPECTED_LIVE_TESTS), 22)

    def test_the_matrix_never_skips_a_live_test(self) -> None:
        source = CHECKER_SOURCE.read_text(encoding="utf-8")
        self.assertNotIn("--skip", source)
        command = test_command(self.entry)
        self.assertNotIn("--skip", command)
        self.assertIn("--test-threads=1", command)
        self.assertIn("--ignored", command)
        self.assertIn("live_", command)

    def test_live_inventory_parses_only_passing_tests(self) -> None:
        output = cargo_output(EXPECTED_LIVE_TESTS)
        self.assertEqual(live_inventory(output), EXPECTED_LIVE_TESTS)
        failing = output.replace(
            "test live_tests::live_append_commits_a_single_transaction_and_reads_back_exactly ... ok",
            "test live_tests::live_append_commits_a_single_transaction_and_reads_back_exactly ... FAILED",
        )
        self.assertNotIn(
            "live_append_commits_a_single_transaction_and_reads_back_exactly",
            live_inventory(failing),
        )

    def test_inventory_verification_rejects_drift_in_both_directions(self) -> None:
        verify_live_inventory(self.entry, cargo_output(EXPECTED_LIVE_TESTS))
        missing = set(EXPECTED_LIVE_TESTS)
        missing.remove("live_append_spatial_xy_preserves_srid_and_coordinates")
        with self.assertRaisesRegex(RuntimeError, "mancanti"):
            verify_live_inventory(self.entry, cargo_output(missing))
        extra = set(EXPECTED_LIVE_TESTS)
        extra.add("live_unexpected_new_probe")
        with self.assertRaisesRegex(RuntimeError, "inattesi"):
            verify_live_inventory(self.entry, cargo_output(extra))
        substituted = set(EXPECTED_LIVE_TESTS)
        substituted.remove("live_append_batch_failure_rolls_back_without_partial_rows")
        substituted.add("live_same_count_replacement")
        with self.assertRaisesRegex(RuntimeError, "mancanti"):
            verify_live_inventory(self.entry, cargo_output(substituted))

    # --- report -----------------------------------------------------------

    def test_entry_report_publishes_image_version_and_inventory(self) -> None:
        report = entry_report(self.entry, self.probe)
        self.assertEqual(report["image"], self.entry.image)
        self.assertEqual(report["label"], self.entry.label)
        self.assertEqual(report["product_version"], self.probe["version"])
        self.assertEqual(
            report["live_tests"],
            {"expected": len(EXPECTED_LIVE_TESTS), "passed": len(EXPECTED_LIVE_TESTS)},
        )
        self.assertEqual(report["hardening"]["require_secure_transport"], "ON")
        self.assertEqual(report["hardening"]["local_infile"], "OFF")

    def test_workflow_cleanup_names_every_resource_the_matrix_creates(self) -> None:
        workflow = pathlib.Path(
            ".github/workflows/mysql-version-matrix.yml"
        ).read_text(encoding="utf-8")
        for entry in MATRIX:
            self.assertIn(entry.container, workflow)
            self.assertIn(entry.tls_volume, workflow)
            self.assertIn(entry.ca_volume, workflow)
            self.assertIn(entry.digest, workflow)
        self.assertIn("docker network rm plenora-mysql-matrix", workflow)
        self.assertIn("docker rm --force --volumes", workflow)

    def test_checker_removes_the_anonymous_mysql_data_volume(self) -> None:
        with patch("scripts.check_mysql_matrix.quiet", return_value=0) as quiet:
            discard(self.entry)
        calls = [call.args[0] for call in quiet.call_args_list]
        self.assertIn(
            ["docker", "rm", "--force", "--volumes", self.entry.container],
            calls,
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

    def test_offline_inventory_is_verified_before_matrix_network_creation(self) -> None:
        calls: list[str] = []
        with (
            patch(
                "scripts.check_mysql_matrix.verify_candidate_offline",
                side_effect=lambda: calls.append("offline"),
            ),
            patch(
                "scripts.check_mysql_matrix.ensure_network",
                side_effect=RuntimeError("stop after network"),
            ),
        ):
            from scripts.check_mysql_matrix import main

            self.assertEqual(main(), 1)
        self.assertEqual(calls, ["offline"])

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
