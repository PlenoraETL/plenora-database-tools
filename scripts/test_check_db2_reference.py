#!/usr/bin/env python3
"""Self-test del gate live Db2."""

from __future__ import annotations

import unittest
from unittest.mock import patch
from subprocess import CompletedProcess
from pathlib import Path

import check_db2_reference as gate


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "db2-assurance.yml"


FIDELITY_TESTS = {
    "live_parameter_values_bind_exactly_or_are_refused",
    "live_audit900_nul",
    "live_invalid_server_text_is_refused_by_catalog_transaction_and_arrow",
    "live_arrow_read_returns_floating_point_values_exactly",
    "live_sub_microsecond_timestamps_are_refused_on_read",
    "live_generated_values_bind_exactly_or_are_refused",
    "live_text_catalog_units_preserve_boundaries_without_false_refusals",
}


def module_of(name: str) -> str:
    if name in FIDELITY_TESTS:
        return "parameter_fidelity_live_tests"
    return "spatial_live_tests" if name.startswith("live_spatial_") else "live_tests"


def qualified(names: set[str]) -> set[str]:
    return {f"{module_of(name)}::{name}" for name in names}


class Db2ReferenceGateTests(unittest.TestCase):
    def test_result_preflight_precedes_execute_and_keeps_driver_errors_in_read(self) -> None:
        source = (gate.ROOT / "crates/plenora-db-db2/src/transaction.rs").read_text(encoding="utf-8")
        query = source.split("fn query_rows(", 1)[1].split("#[derive(Clone, Copy)]", 1)[0]
        self.assertLess(query.index("validate_result_type(data_type)?"), query.index("sent.mark_sent()"))
        self.assertLess(query.index("sent.mark_sent()"), query.index(".execute_strict("))
        self.assertNotIn("driver_error(&error, ErrorPhase::Prepare)", query)
        self.assertIn("driver_error(&error, ErrorPhase::Read)", query)
        decoder = source.split("pub fn decode_value(", 1)[1].split("pub fn canonical_timestamp", 1)[0]
        self.assertLess(decoder.index("validate_result_type(data_type)?"), decoder.index("let Some(value)"))

    def test_catalog_probe_binds_before_compilation_and_checks_execute_diagnostics(self) -> None:
        root = gate.ROOT
        connection = (root / "crates/plenora-db-db2/src/connection.rs").read_text(encoding="utf-8")
        query = connection.split("pub fn query_text(", 1)[1].split("pub fn catalog_text", 1)[0]
        self.assertIn(".preallocate()", query)
        self.assertNotIn(".prepare(", query)
        self.assertIn(".into_cursor_strict(sql, parameters.as_slice())", query)
        vendor = root / "vendor/odbc-api/src"
        preallocated = (vendor / "preallocated.rs").read_text(encoding="utf-8")
        direct = preallocated.split("pub fn into_cursor_strict(", 1)[1].split("\n    }", 1)[0]
        self.assertIn("execute_with_parameters_strict(self.statement, Some(&query), params)", direct)
        execute = (vendor / "execute.rs").read_text(encoding="utf-8")
        strict = execute.split("pub fn execute_with_parameters_strict", 1)[1].split("\n}", 1)[0]
        self.assertLess(strict.index("bind_parameters("), strict.index("execute_checked("))
        self.assertIn("execute_checked(statement, query, true)", strict)
        checked = execute.split("unsafe fn execute_checked", 1)[1].split("\n}", 1)[0]
        self.assertIn("statement.exec_direct(sql)", checked)
        self.assertLess(checked.index('result.into_result_strict('), checked.index("num_result_cols()"))
        self.assertIn('result.into_result_strict(&stmt, "SQLPutData")?', checked)
        self.assertIn("next_blob_param(&mut stmt, strict)?", checked)
        blob = execute.split("unsafe fn next_blob_param", 1)[1]
        self.assertIn('result.into_result_strict(stmt, "SQLParamData")?', blob)
        self.assertLess(blob.index('"SQLParamData"'), blob.index("if let Some(blob_ptr)"))
        transaction = (root / "crates/plenora-db-db2/src/transaction.rs").read_text(encoding="utf-8")
        self.assertNotIn(".execute(", transaction.split("fn execute_statement(", 1)[1].split("fn ", 1)[0])
        self.assertIn("inserter\n                    .execute_strict()", transaction)
        for source in [connection, transaction]:
            self.assertNotIn(".col_data_type(", source)
            self.assertIn(".col_data_type_strict(", source)
        metadata = (vendor / "result_set_metadata.rs").read_text(encoding="utf-8")
        metadata = metadata.split("fn col_data_type_strict", 1)[1].split("\n}", 1)[0]
        self.assertNotIn(".into_result(", metadata)
        self.assertNotIn("self.col_", metadata)
        self.assertIn('into_result_strict(&stmt, "SQLColAttribute")', metadata)
        statement = (vendor / "handles/statement.rs").read_text(encoding="utf-8")
        for name in ["col_name", "describe_col"]:
            body = statement.split(f"fn {name}_checked", 1)[1].split("\n    }", 1)[0]
            self.assertIn(".into_sql_result_strict(self,", body)
            self.assertNotIn("SuccessWithInfo", body)
            self.assertIn("if res.is_err()", body)
        for name in ["parameter_collection.rs", "parameter_collection/tuple.rs", "parameter/blob.rs"]:
            binding = (vendor / name).read_text(encoding="utf-8")
            self.assertNotIn(".into_result(stmt)", binding)
            self.assertIn('into_result_strict(stmt, "SQLBindParameter")', binding)

    def test_exact_rerun_continues_after_real_subprocess_nonzero_and_timeout(self) -> None:
        success = "test c ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;"
        outcomes = [
            CompletedProcess([], 101, "test a ... FAILED\n", ""),
            gate.subprocess.TimeoutExpired("docker", 1, output=b"test b started\n"),
            CompletedProcess([], 0, success, ""),
        ]
        with patch.object(gate.subprocess, "run", side_effect=outcomes) as run, patch("builtins.print") as output:
            self.assertEqual(gate.rerun_exact_tests(("a", "b", "c")), 1)
        self.assertEqual(run.call_count, 3)
        report = gate.json.loads(output.call_args.args[0])["exact_tests"]
        self.assertEqual([row["status"] for row in report], ["failed", "failed", "passed"])

    def test_exact_rerun_reports_every_result_after_failures_and_timeouts(self) -> None:
        names = ("a", "b", "c", "d", "e")
        success = "test e ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;"
        with patch.object(gate, "compose_exec", side_effect=[
            RuntimeError("exit nonzero"), RuntimeError("timeout"), OSError("docker"),
            "test result: ok. 0 passed; 0 failed;", success,
        ]) as execute, patch("builtins.print") as output:
            self.assertEqual(gate.rerun_exact_tests(names), 1)
        self.assertEqual(execute.call_count, len(names))
        report = gate.json.loads(output.call_args.args[0])["exact_tests"]
        self.assertEqual([row["test"] for row in report], list(names))
        self.assertEqual([row["status"] for row in report], ["failed", "failed", "failed", "incomplete", "passed"])
        for call in execute.call_args_list:
            self.assertEqual(call.args[1]["DB2CODEPAGE"], "1208")
            self.assertEqual(call.args[1]["CARGO_BUILD_JOBS"], gate.os.environ.get("CARGO_BUILD_JOBS", "4"))
            self.assertEqual(call.kwargs["timeout"], gate.LIVE_TIMEOUT)

    def test_exact_rerun_requires_each_named_test_and_no_ignored_tests(self) -> None:
        for output, expected in [
            ("test a ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;", 0),
            ("test other ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;", 1),
            ("test a ... ignored\ntest result: ok. 0 passed; 0 failed; 1 ignored;", 1),
        ]:
            with patch.object(gate, "compose_exec", return_value=output), patch("builtins.print"):
                self.assertEqual(gate.rerun_exact_tests(("a",)), expected)
        with patch("builtins.print"):
            self.assertEqual(gate.rerun_exact_tests(()), 1)

    def test_capture_preserves_progress_and_exact_results(self) -> None:
        source = Path(gate.__file__).read_text(encoding="utf-8")
        self.assertIn('"--show-output"', source)
        self.assertNotIn('"--nocapture"', source)
        names = sorted(qualified(gate.live_test_inventory()))
        listing = "\n".join(f"{name}: test" for name in names)
        output = "\n".join(f"test {name} ... ok" for name in names)
        output += "\n\nsuccesses:\ncensimento parametri: C_TS6 Native: fine\n"
        self.assertEqual(gate.validate_live_run(listing, output), names)
        for status in ("ignored", "FAILED", "ok censimento parametri: inizio\nok"):
            with self.assertRaises(RuntimeError):
                gate.validate_live_run(listing, output.replace(f"test {names[0]} ... ok", f"test {names[0]} ... {status}"))

    def test_live_timeout_reaches_docker_without_changing_other_steps(self) -> None:
        with patch.object(gate.subprocess, "run", return_value=CompletedProcess([], 0, "", "")) as run:
            gate.compose_exec(["cargo", "test"], gate.DB2_ENV, timeout=gate.LIVE_TIMEOUT)
            self.assertEqual(run.call_args.kwargs["timeout"], gate.LIVE_TIMEOUT)
            gate.compose_exec(["cargo", "test"], gate.DB2_ENV)
            self.assertEqual(run.call_args.kwargs["timeout"], gate.TIMEOUT)


    def test_python_live_inventory_includes_the_orm_gate(self) -> None:
        self.assertEqual(gate.PYTHON_LIVE_EXPECTED, 12)
        self.assertEqual(
            gate.PYTHON_LIVE_TARGETS,
            (
                "db2_sdk_gate_tests/test_db2_session.py",
                "db2_sdk_gate_tests/test_orm.py::test_live_db2_generated_defaults_and_ddl",
                "db2_sdk_gate_tests/test_orm.py::test_live_db2_geometry_orm_qualification",
                "db2_sdk_gate_tests/test_orm.py::test_live_db2_migration_dag_is_idempotent_and_reversible",
                "db2_sdk_gate_tests/test_orm.py::test_live_db2_advanced_orm_qualification",
            ),
        )

    def test_inventory_has_all_named_live_tests(self) -> None:
        inventory = gate.live_test_inventory()
        self.assertEqual(len(inventory), 41)
        self.assertIn("live_odbc_warnings_are_classified_before_next_call", inventory)
        self.assertLessEqual(gate.REQUIRED_LIVE_TESTS, inventory)

    def test_complete_named_run_passes(self) -> None:
        names = gate.live_test_inventory()
        listing = "\n".join(f"{name}: test" for name in sorted(qualified(names)))
        output = "\n".join(f"test {name} ... ok" for name in sorted(qualified(names)))
        self.assertEqual(gate.validate_live_run(listing, output), sorted(qualified(names)))

    def test_substitution_at_equal_count_fails(self) -> None:
        names = gate.live_test_inventory()
        compiled = qualified(names)
        executed = set(compiled)
        executed.remove(next(iter(executed)))
        executed.add("live_tests::live_inventato")
        listing = "\n".join(f"{name}: test" for name in sorted(compiled))
        output = "\n".join(f"test {name} ... ok" for name in sorted(executed))
        with self.assertRaisesRegex(RuntimeError, "incompleta"):
            gate.validate_live_run(listing, output)

    def test_every_live_test_carries_an_explicit_reason(self) -> None:
        reasons = {}
        for source in gate.LIVE_SOURCES:
            reasons.update(
                gate.live_inventory.ignore_reasons(source.read_text(encoding="utf-8"))
            )
        self.assertEqual(set(reasons), gate.live_test_inventory())
        self.assertTrue(all(reason.startswith("richiede Db2") for reason in reasons.values()))

    def test_build_contract_is_pinned_and_platform_explicit(self) -> None:
        bases = gate.validate_build_contract()
        self.assertEqual(set(bases), {"rust", "db2"})

    def test_cli_probe_requires_matching_connection_and_capability_provider(self) -> None:
        valid = {
            "connection": {"provider": "db2"},
            "capabilities": {"provider": "db2"},
        }
        gate.validate_cli_probe(valid)
        for surface in ("connection", "capabilities"):
            invalid = {key: dict(value) for key, value in valid.items()}
            invalid[surface]["provider"] = "postgres"
            with self.assertRaisesRegex(RuntimeError, surface):
                gate.validate_cli_probe(invalid)

    def test_workflow_executes_self_test_gate_and_cleanup(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        for expected in (
            "python3 scripts/test_check_db2_reference.py",
            "python3 scripts/test_name_db2_wheel.py",
            "python3 scripts/check_db2_reference.py",
            "docker-compose.db2.yml",
            "db2-windows-build:",
            "--features db2",
            "PLENORA_EXPECT_DB2_RUNTIME",
            '".github/scripts/verify_wheel.py"',
            '"rust-toolchain.toml"',
            '"vendor/odbc-api/**"',
            '"requirements-self-tests.txt"',
            "down --volumes",
        ):
            self.assertIn(expected, workflow)


if __name__ == "__main__":
    unittest.main()
