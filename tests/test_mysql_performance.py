from __future__ import annotations

import copy
import json
import os
import pathlib
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.check_mysql_performance import (
    APPEND_MODE,
    DEFAULT_BUDGET,
    DEFAULT_MANIFEST,
    EXPECTED_DIGEST,
    EXPECTED_REFERENCE,
    aggregate,
    baseline_comparison,
    compare_baseline,
    enforce_budget,
    environment_identity,
    mysql_network,
    validate_budget,
    validate_manifest,
)


def read_sample(total: int = 1000, rate: int = 100_000) -> dict[str, object]:
    return {"rows": 100, "total_micros": total, "rows_per_second": rate}


def write_sample(total: int = 2000, rate: int = 50_000) -> dict[str, object]:
    return {
        "mode": APPEND_MODE,
        "rows": 100,
        "prepare_micros": 500,
        "execute_micros": total - 500,
        "total_micros": total,
        "rows_per_second": rate,
        "differences": 0,
        "transactions": 1,
    }


class MysqlPerformanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = {
            "schema_version": 1,
            "campaign": "test",
            "rows": 100,
            "batch_rows": 25,
            "warmup": 1,
            "repeat": 2,
        }
        self.raw = {
            "schema_version": 1,
            "profile": "append-single-transaction",
            "rows": 100,
            "batch_rows": 25,
            "warmup": 1,
            "repeat": 2,
            "peak_rss_bytes": 1024,
            "reads": [read_sample(1000, 100_000), read_sample(1100, 90_909)],
            "writes": [write_sample(), write_sample(2100, 47_619)],
        }
        self.budget = {
            "schema_version": 1,
            "profile": "test",
            "limits": {
                "read_p95_micros_max": 10_000,
                "read_median_rows_per_second_min": 1_000,
                "append_p95_micros_max": 10_000,
                "append_median_rows_per_second_min": 1_000,
                "peak_rss_bytes_max": 4096,
            },
            "regression": {
                "p95_latency_multiplier_max": 1.5,
                "median_throughput_ratio_min": 0.6,
            },
        }
        self.environment = {
            "platform": "test",
            "machine": "x86_64",
            "cpu_count": 8,
            "mysql_reference": "mysql@sha256:test",
            "mysql_runtime_image": "sha256:test-runtime",
            "mysql_version": "8.4.0",
            "rust_image": "rust:test",
            "campaign": "test",
        }

    # --- manifest ---------------------------------------------------------

    def test_manifest_rejects_out_of_range_and_missing_fields(self) -> None:
        for key, value in (
            ("repeat", 0),
            ("rows", 0),
            ("rows", 1_000_001),
            ("batch_rows", 65_537),
            ("warmup", 101),
            ("warmup", -1),
        ):
            invalid = copy.deepcopy(self.manifest)
            invalid[key] = value
            with self.assertRaisesRegex(ValueError, key):
                validate_manifest(invalid)
        invalid = copy.deepcopy(self.manifest)
        del invalid["campaign"]
        with self.assertRaisesRegex(ValueError, "campaign"):
            validate_manifest(invalid)
        invalid = copy.deepcopy(self.manifest)
        invalid["schema_version"] = 2
        with self.assertRaisesRegex(ValueError, "schema_version"):
            validate_manifest(invalid)

    def test_manifest_rejects_a_boolean_masquerading_as_an_integer(self) -> None:
        invalid = copy.deepcopy(self.manifest)
        invalid["repeat"] = True
        with self.assertRaisesRegex(ValueError, "repeat"):
            validate_manifest(invalid)

    # --- aggregazione -----------------------------------------------------

    def test_aggregate_requires_zero_differential(self) -> None:
        invalid = copy.deepcopy(self.raw)
        invalid["writes"][1]["differences"] = 1
        with self.assertRaisesRegex(RuntimeError, "differenziale"):
            aggregate(invalid, self.manifest)

    def test_aggregate_rejects_a_write_mode_outside_the_qualified_append(self) -> None:
        invalid = copy.deepcopy(self.raw)
        invalid["writes"][0]["mode"] = "create"
        with self.assertRaisesRegex(RuntimeError, "append"):
            aggregate(invalid, self.manifest)

    def test_aggregate_rejects_sample_counts_that_do_not_match_repeat(self) -> None:
        invalid = copy.deepcopy(self.raw)
        invalid["writes"] = invalid["writes"][:1]
        with self.assertRaisesRegex(RuntimeError, "write"):
            aggregate(invalid, self.manifest)
        invalid = copy.deepcopy(self.raw)
        invalid["reads"] = [*invalid["reads"], read_sample()]
        with self.assertRaisesRegex(RuntimeError, "read"):
            aggregate(invalid, self.manifest)

    def test_aggregate_rejects_row_count_drift(self) -> None:
        invalid = copy.deepcopy(self.raw)
        invalid["reads"][0]["rows"] = 99
        with self.assertRaisesRegex(RuntimeError, "righe"):
            aggregate(invalid, self.manifest)

    def test_aggregate_rejects_a_prepare_execute_decomposition_that_does_not_add_up(
        self,
    ) -> None:
        invalid = copy.deepcopy(self.raw)
        invalid["writes"][0]["execute_micros"] = 1
        with self.assertRaisesRegex(RuntimeError, "prepare"):
            aggregate(invalid, self.manifest)

    def test_aggregate_requires_exactly_one_transaction_per_append(self) -> None:
        for value in (0, 2, None):
            invalid = copy.deepcopy(self.raw)
            invalid["writes"][0]["transactions"] = value
            with self.assertRaisesRegex(RuntimeError, "transazione|metrica"):
                aggregate(invalid, self.manifest)

    def test_aggregate_rejects_non_numeric_metrics_cleanly(self) -> None:
        for group, key in (
            ("reads", "total_micros"),
            ("reads", "rows_per_second"),
            ("writes", "rows_per_second"),
        ):
            invalid = copy.deepcopy(self.raw)
            invalid[group][0][key] = "100"
            with self.assertRaisesRegex(RuntimeError, "metrica"):
                aggregate(invalid, self.manifest)

    def test_aggregate_rejects_bool_and_float_contract_fields(self) -> None:
        for group, key, value in (
            ("reads", "rows", 100.0),
            ("writes", "rows", 100.0),
            ("writes", "differences", False),
            ("writes", "transactions", True),
        ):
            invalid = copy.deepcopy(self.raw)
            invalid[group][0][key] = value
            with self.assertRaisesRegex(RuntimeError, "metrica"):
                aggregate(invalid, self.manifest)

    def test_aggregate_publishes_read_and_append_statistics(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        self.assertEqual(
            sorted(summary),
            [
                "append_execute_micros",
                "append_prepare_micros",
                "append_rows_per_second",
                "append_total_micros",
                "read_rows_per_second",
                "read_total_micros",
            ],
        )
        self.assertEqual(summary["read_total_micros"]["p95"], 1100)
        self.assertEqual(summary["read_total_micros"]["min"], 1000)

    # --- budget assoluto --------------------------------------------------

    def test_budget_validation_is_fail_closed_on_missing_limits(self) -> None:
        invalid = copy.deepcopy(self.budget)
        del invalid["limits"]["append_p95_micros_max"]
        with self.assertRaisesRegex(ValueError, "append_p95_micros_max"):
            validate_budget(invalid)
        invalid = copy.deepcopy(self.budget)
        del invalid["regression"]
        with self.assertRaisesRegex(ValueError, "regression"):
            validate_budget(invalid)
        invalid = copy.deepcopy(self.budget)
        invalid["schema_version"] = 7
        with self.assertRaisesRegex(ValueError, "schema_version"):
            validate_budget(invalid)

    def test_budget_rejects_latency_throughput_and_memory_excursions(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        strict = copy.deepcopy(self.budget)
        strict["limits"]["append_p95_micros_max"] = 100
        with self.assertRaisesRegex(RuntimeError, "append p95"):
            enforce_budget(summary, self.raw, strict)
        strict = copy.deepcopy(self.budget)
        strict["limits"]["read_median_rows_per_second_min"] = 10_000_000
        with self.assertRaisesRegex(RuntimeError, "read throughput"):
            enforce_budget(summary, self.raw, strict)
        strict = copy.deepcopy(self.budget)
        strict["limits"]["peak_rss_bytes_max"] = 64
        with self.assertRaisesRegex(RuntimeError, "peak RSS"):
            enforce_budget(summary, self.raw, strict)

    def test_budget_passes_without_any_measured_baseline(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        enforce_budget(summary, self.raw, self.budget)

    def test_budget_rejects_missing_peak_rss_evidence(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        invalid = copy.deepcopy(self.raw)
        invalid["peak_rss_bytes"] = None
        with self.assertRaisesRegex(RuntimeError, "peak RSS"):
            enforce_budget(summary, invalid, self.budget)

    def test_environment_identity_accepts_a_distinct_runtime_id_with_pinned_repo_digest(
        self,
    ) -> None:
        runtime_id = "sha256:" + "a" * 64
        observations = (
            SimpleNamespace(
                returncode=0,
                stdout=f"{EXPECTED_REFERENCE}|{runtime_id}\n",
                stderr="",
            ),
            SimpleNamespace(
                returncode=0,
                stdout=json.dumps([EXPECTED_REFERENCE]) + "\n",
                stderr="",
            ),
            SimpleNamespace(returncode=0, stdout="8.4.11\n", stderr=""),
        )
        with (
            patch(
                "scripts.check_mysql_performance.subprocess.run",
                side_effect=observations,
            ),
            patch("scripts.check_mysql_performance.platform.system", return_value="Linux"),
            patch("scripts.check_mysql_performance.platform.machine", return_value="x86_64"),
            patch("scripts.check_mysql_performance.os.cpu_count", return_value=8),
        ):
            identity = environment_identity(self.manifest)
        self.assertEqual(identity["mysql_runtime_image"], runtime_id)

    def test_environment_identity_requires_requested_and_loaded_repo_digest(self) -> None:
        requested_mismatch = SimpleNamespace(
            returncode=0,
            stdout=f"mysql@sha256:other|{EXPECTED_DIGEST}\n",
            stderr="",
        )
        with patch(
            "scripts.check_mysql_performance.subprocess.run",
            return_value=requested_mismatch,
        ):
            with self.assertRaisesRegex(RuntimeError, "digest"):
                environment_identity(self.manifest)

        runtime_id = "sha256:" + "b" * 64
        observations = (
            SimpleNamespace(
                returncode=0,
                stdout=f"{EXPECTED_REFERENCE}|{runtime_id}\n",
                stderr="",
            ),
            SimpleNamespace(
                returncode=0,
                stdout=json.dumps(["mysql@sha256:other"]) + "\n",
                stderr="",
            ),
        )
        with patch(
            "scripts.check_mysql_performance.subprocess.run",
            side_effect=observations,
        ):
            with self.assertRaisesRegex(RuntimeError, "digest"):
                environment_identity(self.manifest)

    def test_performance_example_requires_private_ca_and_password(self) -> None:
        source = pathlib.Path(
            "crates/plenora-db-mysql/examples/mysql_performance.rs"
        ).read_text(encoding="utf-8")
        self.assertNotIn("TrustServerCertificate", source)
        self.assertNotIn("with_danger_accept_invalid_certs", source)
        self.assertNotIn("with_danger_skip_domain_validation", source)
        self.assertNotIn("DataFlow_Test_2026!", source)

    def test_network_is_derived_unambiguously_or_explicitly_overridden(self) -> None:
        completed = SimpleNamespace(
            returncode=0,
            stdout='{"project_default": {}}\n',
            stderr="",
        )
        with (
            patch.dict(os.environ, {}, clear=True),
            patch(
                "scripts.check_mysql_performance.subprocess.run",
                return_value=completed,
            ),
        ):
            self.assertEqual(mysql_network(), "project_default")

        ambiguous = SimpleNamespace(
            returncode=0,
            stdout='{"first": {}, "second": {}}\n',
            stderr="",
        )
        with (
            patch.dict(os.environ, {}, clear=True),
            patch(
                "scripts.check_mysql_performance.subprocess.run",
                return_value=ambiguous,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "ambigu"):
                mysql_network()

        with patch.dict(
            os.environ, {"PLENORA_MYSQL_PERF_NETWORK": "explicit"}, clear=True
        ):
            self.assertEqual(mysql_network(), "explicit")

    def test_performance_example_cleans_the_fixture_after_campaign_errors(self) -> None:
        source = pathlib.Path(
            "crates/plenora-db-mysql/examples/mysql_performance.rs"
        ).read_text(encoding="utf-8")
        self.assertIn("let campaign_result = run_campaign", source)
        self.assertIn("let cleanup_result = drop_fixture", source)

    # --- confronto baseline ----------------------------------------------

    def test_absent_baseline_is_reported_as_not_requested(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        comparison = baseline_comparison(
            None, summary, self.environment, self.budget
        )
        self.assertEqual(comparison, {"status": "not_requested"})

    def test_missing_baseline_file_is_reported_as_not_comparable(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        comparison = baseline_comparison(
            pathlib.Path("benchmarks/baseline/absent-mysql-baseline.json"),
            summary,
            self.environment,
            self.budget,
        )
        self.assertEqual(comparison["status"], "not_comparable")
        self.assertEqual(
            comparison["environment_mismatches"], ["baseline_file_absent"]
        )

    def test_identical_environment_and_summary_compare_as_passed(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        comparison = compare_baseline(
            summary,
            self.environment,
            {
                "environment": copy.deepcopy(self.environment),
                "summary": copy.deepcopy(summary),
            },
            self.budget,
        )
        self.assertEqual(comparison["status"], "passed")

    def test_a_different_environment_is_never_compared(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        for key, value in (
            ("cpu_count", 4),
            ("mysql_reference", "mysql@sha256:other"),
            ("mysql_version", "8.0.46"),
            ("rust_image", "rust:other"),
        ):
            baseline_environment = copy.deepcopy(self.environment)
            baseline_environment[key] = value
            comparison = compare_baseline(
                summary,
                self.environment,
                {"environment": baseline_environment, "summary": summary},
                self.budget,
            )
            self.assertEqual(comparison["status"], "not_comparable")
            self.assertEqual(comparison["environment_mismatches"], [key])

    def test_baseline_without_an_environment_is_not_comparable(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        comparison = compare_baseline(
            summary, self.environment, {"summary": summary}, self.budget
        )
        self.assertEqual(comparison["status"], "not_comparable")
        self.assertEqual(
            comparison["environment_mismatches"], ["baseline_environment_missing"]
        )

    def test_latency_and_throughput_regressions_fail_on_an_identical_environment(
        self,
    ) -> None:
        baseline = aggregate(self.raw, self.manifest)
        current = copy.deepcopy(baseline)
        current["append_total_micros"]["p95"] *= 4
        with self.assertRaisesRegex(RuntimeError, "latenza append"):
            compare_baseline(
                current,
                self.environment,
                {"environment": self.environment, "summary": baseline},
                self.budget,
            )
        current = copy.deepcopy(baseline)
        current["read_rows_per_second"]["median"] /= 4
        with self.assertRaisesRegex(RuntimeError, "throughput read"):
            compare_baseline(
                current,
                self.environment,
                {"environment": self.environment, "summary": baseline},
                self.budget,
            )

    # --- artefatti versionati --------------------------------------------

    def test_versioned_manifest_and_budget_are_valid_and_consistent(self) -> None:
        manifest = json.loads(DEFAULT_MANIFEST.read_text(encoding="utf-8"))
        budget = json.loads(DEFAULT_BUDGET.read_text(encoding="utf-8"))
        validate_manifest(manifest)
        validate_budget(budget)
        self.assertEqual(manifest["campaign"], budget["profile"])

    def test_no_measured_mysql_baseline_is_claimed_in_the_repository(self) -> None:
        candidates = sorted(
            pathlib.Path("benchmarks/baseline").glob("mysql*-performance-reference.json")
        )
        self.assertEqual(candidates, [])


if __name__ == "__main__":
    unittest.main()
