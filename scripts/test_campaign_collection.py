"""Offline orchestration regressions: failures must not hide later evidence."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.campaign_collection import Collection, collect_all, complete_command, failure_message, observe
from scripts import check_mysql_matrix as matrix, check_mysql_reference as reference


class CollectionTests(unittest.TestCase):
    def test_failure_diagnostic_keeps_location_and_code_without_payload(self):
        output = "---- live_tests::spatial stdout ----\nthread panicked at crates/provider/src/live_tests.rs:6228:10\nsecret row; errore server (codice 3516)\n"
        message = failure_message("live_tests::spatial", output)
        self.assertIn("server_code=3516", message)
        self.assertIn("location=crates/provider/src/live_tests.rs:6228:10", message)
        self.assertNotIn("secret", message)
        self.assertNotIn("3516", failure_message("other_test", output))

    def test_failed_missing_ignored_and_timeout_results_are_not_success(self):
        collection = Collection("test")
        with collection.stage("8.0", "live", {"a", "b", "c", "d"}):
            observe("test a ... ok\ntest b ... FAILED\ntest c ... ignored\n")
            raise subprocess.TimeoutExpired("secret DSN", 1, output=b"test e ... ok\nsecret")
        with collection.stage("8.4", "live", {"later"}):
            observe("test later ... ok\n")
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(RuntimeError):
            collection.finish()
        rows = json.loads(output.getvalue())["collection"]
        self.assertEqual({r["test"]: r["outcome"] for r in rows if r["test"] != "live"},
                         dict(a="passed", b="failed", c="blocked", d="blocked", e="passed", later="passed"))
        self.assertNotIn("secret", output.getvalue())

    def test_nested_results_reach_the_final_ledger(self):
        parent = Collection("parent")
        with contextlib.redirect_stdout(io.StringIO()):
            with parent.stage("v", "child"):
                child = Collection("child")
                with child.stage("v", "live", {"test"}):
                    observe("test test ... FAILED\n")
                child.finish()
        self.assertTrue(parent.failed)
        self.assertTrue(any(r["test"] == "test" for r in parent.results))

    def test_collect_all_attempts_every_item_and_keeps_failure_red(self):
        calls = []
        def measure(item):
            calls.append(item)
            if item != "second":
                raise RuntimeError("secret password")
            return {}
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(RuntimeError):
            collect_all(["first", "second", "third"], measure, gate="fleet", key=str)
        self.assertEqual(calls, ["first", "second", "third"])
        self.assertNotIn("secret", output.getvalue())
        self.assertEqual(len(json.loads(output.getvalue())["collection"]), 3)

    def test_cargo_finishes_all_test_binaries_without_altering_libtest_arguments(self):
        command = ["docker", "run", "rust", "cargo", "test", "-p", "crate", "--", "--ignored"]
        result = complete_command(command)
        self.assertEqual(result[result.index("--"):], ["--", "--ignored"])
        self.assertLess(result.index("--no-fail-fast"), result.index("--"))
        self.assertEqual(complete_command(result), result)
        self.assertNotIn("--no-fail-fast", command)

    def test_pytest_parameter_payloads_are_redacted_and_skip_contract_stays_with_caller(self):
        collection = Collection("sdk")
        with collection.stage("fleet", "scope"):
            observe("PASSED tests/test_a.py::test_a[secret DSN]\n"
                    "FAILED tests/test_a.py::test_b[password] - sensitive assertion\n"
                    "SKIPPED tests/test_a.py::test_c - unavailable\n")
        self.assertTrue(collection.failed)
        rendered = json.dumps(collection.results)
        for secret in ("secret", "DSN", "password", "sensitive", "unavailable"):
            self.assertNotIn(secret, rendered)
        self.assertEqual([r["outcome"] for r in collection.results[:-1]], ["passed", "failed", "ignored"])


class MysqlCollectionTests(unittest.TestCase):
    def test_all_versions_and_both_families_after_multiple_failures(self):
        calls = []
        def suite(entry, environment, command):
            family = "reference" if "--ignored" in command else "default"
            calls.append((entry.exact_version, family))
            status = "FAILED" if family == "default" else "ok"
            output = f"test live_tests::{family} ... {status}\n"
            observe(output)
            if status == "FAILED":
                raise RuntimeError("redact this payload")
            return output
        output = io.StringIO()
        with contextlib.ExitStack() as stack:
            for name in ("validate_inventory", "verify_candidate_unit", "ensure_network", "discard", "generate_tls", "start", "wait_ready", "verify_hardening", "quiet"):
                stack.enter_context(patch.object(matrix, name))
            stack.enter_context(patch("scripts.check_sdk_tests.build_artifacts", return_value={"wheel": "test.whl"}))
            surfaces = stack.enter_context(patch.object(matrix, "run", side_effect=RuntimeError("surface payload")))
            stack.enter_context(patch.object(matrix, "probe_server", return_value={}))
            stack.enter_context(patch.object(matrix, "EXPECTED_UNIT_TESTS", set()))
            stack.enter_context(patch.object(matrix, "EXPECTED_LIVE_DEFAULT_TESTS", {"live_tests::default"}))
            stack.enter_context(patch.object(matrix, "EXPECTED_LIVE_REFERENCE_TESTS", {"live_tests::reference"}))
            stack.enter_context(patch.object(matrix, "run_suite", side_effect=suite))
            stack.enter_context(contextlib.redirect_stdout(output))
            self.assertEqual(matrix.main(), 1)
        self.assertEqual(calls, [(e.exact_version, f) for e in matrix.MATRIX for f in ("default", "reference")])
        self.assertEqual(surfaces.call_count, len(matrix.MATRIX))
        ledger = json.loads(output.getvalue().splitlines()[-1])
        for entry in matrix.MATRIX:
            self.assertTrue(any(r["version"] == entry.exact_version and r["stage"] == "mysql-surfaces" and r["outcome"] == "failed" for r in ledger["collection"]))
            self.assertTrue(any(r["version"] == entry.exact_version and r["test"] == "live_tests::default" and r["outcome"] == "failed" for r in ledger["collection"]))
        self.assertNotIn("redact this", output.getvalue())

    def test_baseline_runs_cli_and_both_families_after_unit_failure(self):
        calls = []
        def run(arguments, **kwargs):
            if arguments[0] != "test":
                return ""
            if "--skip" in arguments:
                family = "unit"
            elif "live_database_probe_mysql_private_ca" in arguments:
                family = "cli"
            else:
                family = "reference" if "--ignored" in arguments else "default"
            calls.append(family)
            observe(f"test {family} ... FAILED\n")
            raise RuntimeError("sensitive")
        with contextlib.ExitStack() as stack:
            for name in ("validate_inventory", "validate_fixture", "ensure_reference_running", "validate_reference"):
                stack.enter_context(patch.object(reference, name))
            stack.enter_context(patch.object(reference, "run_cargo", side_effect=run))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            with self.assertRaises(RuntimeError):
                reference.main()
        self.assertEqual(calls, ["unit", "default", "reference", "cli"])

    def test_failed_command_is_observed_before_the_runner_raises(self):
        for runner in (matrix, reference):
            collection = Collection("command")
            result = subprocess.CompletedProcess([], 1, "test first ... FAILED\ntest last ... ok\n", "payload")
            with patch.object(runner.subprocess, "run", return_value=result), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                with collection.stage("version", "suite"):
                    runner.run(["cargo", "test"], capture=True)
            self.assertTrue(collection.failed)
            self.assertEqual([r["test"] for r in collection.results], ["first", "last", "suite"])


class CleanCheckoutTests(unittest.TestCase):
    def test_inventory_self_tests_do_not_require_target(self):
        from scripts import check_campaign_inventories as inventories
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(inventories, "ROOT", root):
                suite = unittest.TestSuite(inventories.InventoryRegressions(name) for name in (
                    "test_python_parameter_cases_and_unknown_forms",
                    "test_integration_target_is_discovered",
                    "test_python_fixture_alias_and_parameter_growth"))
                result = unittest.TextTestRunner(stream=io.StringIO()).run(suite)
            self.assertTrue(result.wasSuccessful())
            self.assertFalse((root / "target").exists())


class OtherCampaignTests(unittest.TestCase):
    def test_postgres_and_sqlserver_matrices_continue_after_first_failure(self):
        from scripts import check_postgres_matrix as pg, check_sqlserver_matrix as sql
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(pg, "parse_args", return_value=unittest.mock.Mock(output=None)))
            stack.enter_context(patch.object(pg, "CACHE"))
            stack.enter_context(patch.object(pg, "ensure_network"))
            target = stack.enter_context(patch.object(pg, "test_target", side_effect=[RuntimeError("failed")] + [{}] * (len(pg.TARGETS) - 1)))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            self.assertEqual(pg.main(), 1)
            self.assertEqual(target.call_count, len(pg.TARGETS))
        with patch.object(sql, "ensure_network"), patch.object(sql, "qualify", side_effect=[RuntimeError("failed"), {}]) as target, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(sql.main(), 1)
            self.assertEqual(target.call_count, len(sql.MATRIX))

    def test_sdk_attempts_every_scope_after_pytest_errors(self):
        from scripts import check_sdk_tests as sdk
        with contextlib.ExitStack() as stack:
            for name in ("git", "worktree_state", "assert_artifacts_outside_repository", "assert_worktree_unchanged"):
                stack.enter_context(patch.object(sdk, name))
            stack.enter_context(patch.object(sdk, "build_artifacts", return_value={"wheel": "test.whl"}))
            stack.enter_context(patch.object(sdk, "pytest_command", side_effect=lambda **kw: ["pytest", kw["scope"]]))
            run = stack.enter_context(patch.object(sdk, "run", side_effect=RuntimeError("failed")))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            with self.assertRaises(RuntimeError):
                sdk.measure_scopes(["live", "benchmark", "parity"], dirty=[])
            self.assertEqual([c.args[0][1] for c in run.call_args_list], ["live", "benchmark", "parity"])

    def test_mariadb_and_session_attempt_every_server_after_measurement_errors(self):
        from scripts import check_mariadb_driver as mariadb, check_session_matrix as session
        for runner in (mariadb, session):
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch.object(runner, "image_identities", return_value=[]))
                stack.enter_context(patch.object(runner, "declares_image", return_value=True))
                measure = stack.enter_context(patch.object(runner, "measure", side_effect=RuntimeError("failed")))
                if runner is session:
                    stack.enter_context(patch.object(session, "preflight"))
                stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                with self.assertRaises(RuntimeError):
                    runner.verdict()
                self.assertEqual([c.args[0].key for c in measure.call_args_list], [s.key for s in runner.servers()])

    def test_db2_keeps_cli_and_python_after_clippy_and_live_errors(self):
        from scripts import check_db2_reference as db2
        with contextlib.ExitStack() as stack:
            for name in ("validate_build_contract", "container_identity"):
                stack.enter_context(patch.object(db2, name))
            cargo = stack.enter_context(patch.object(db2, "compose_exec", side_effect=RuntimeError("failed")))
            cli = stack.enter_context(patch.object(db2, "run_cli_probe", side_effect=RuntimeError("failed")))
            python = stack.enter_context(patch.object(db2, "run_python_live", side_effect=RuntimeError("failed")))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            self.assertEqual(db2.main(), 1)
            self.assertEqual(cargo.call_count, 3)
            cli.assert_called_once()
            python.assert_called_once()

    def test_postgres_runs_every_cli_suite_after_provider_failure(self):
        from scripts import check_postgres_reference as pg
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            if command[:2] == ["docker", "inspect"]:
                return "running|healthy"
            raise RuntimeError("failed")
        with patch.object(pg, "run", side_effect=run), patch.object(pg, "cargo", side_effect=lambda args, *a, **kw: args), patch.object(pg, "check_ipc_materialization") as ipc, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(pg.main(), 1)
        for suite in pg.CLI_REFERENCE_SUITES:
            self.assertTrue(any(suite in command for command in calls))
        ipc.assert_called_once()

    def test_sqlserver_runs_cli_and_rotation_after_provider_failure(self):
        from scripts import check_sqlserver_reference as sql
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(sql, "docker_value", return_value="running|healthy"))
            for name in ("validate_image_pin", "server_identity", "materialize_private_ca"):
                stack.enter_context(patch.object(sql, name))
            cargo = stack.enter_context(patch.object(sql, "run_cargo", side_effect=RuntimeError("failed")))
            cli = stack.enter_context(patch.object(sql, "run_live_cli_probe"))
            rotation = stack.enter_context(patch.object(sql, "rotate_server_certificate"))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            self.assertEqual(sql.main(), 1)
            self.assertEqual(cargo.call_count, 3)
            cli.assert_called_once()
            rotation.assert_called_once()

    def test_fixture_startup_failure_still_attempts_later_fixtures_and_measurement(self):
        from scripts import fixture_campaign as fixtures
        calls = []
        def compose(file, action, *args):
            calls.append((file, action))
            if file == "first.yml" and action == "up":
                raise RuntimeError("failed")
        with patch.object(fixtures, "compose", side_effect=compose), contextlib.redirect_stdout(io.StringIO()):
            with patch.object(fixtures, "stop_fixtures", return_value=[]) as cleanup:
                measure = unittest.mock.Mock(return_value={})
                with self.assertRaises(RuntimeError):
                    fixtures.campaign(compose_files=["first.yml", "second.yml"], preflight=lambda: "head", measure=measure, diagnostics_directory=None, keep_fixtures=False)
                measure.assert_called_once()
                cleanup.assert_called_once_with(["first.yml", "second.yml"])
        self.assertIn(("second.yml", "up"), calls)


if __name__ == "__main__":
    unittest.main()
