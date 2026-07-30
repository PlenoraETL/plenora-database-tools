from __future__ import annotations

import copy
import unittest

from scripts.check_sqlserver_performance import (
    aggregate,
    compare_baseline,
    validate_manifest,
)


class SqlServerPerformanceTests(unittest.TestCase):
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
            "peak_rss_bytes": 1024,
            "reads": [
                {"rows": 100, "total_micros": 1000, "rows_per_second": 100_000},
                {"rows": 100, "total_micros": 1100, "rows_per_second": 90_909},
            ],
            "writes": [
                {
                    "mode": mode,
                    "rows": 100,
                    "total_micros": 2000,
                    "rows_per_second": 50_000,
                    "differences": 0,
                }
                for _ in range(2)
                for mode in ("prepared", "tds_bulk")
            ],
            "lifecycle": [
                {
                    "mode": mode,
                    "rows": 100,
                    "total_micros": 3000,
                    "rows_per_second": 33_333,
                }
                for _ in range(2)
                for mode in ("create", "replace")
            ],
        }
        self.regression_budget = {
            "regression": {
                "p95_latency_multiplier_max": 1.5,
                "median_throughput_ratio_min": 0.6,
            }
        }
        self.environment = {
            "platform": "test",
            "machine": "x86_64",
            "cpu_count": 8,
            "sqlserver_reference": "mcr.microsoft.com/mssql/server@sha256:test",
            "sqlserver_runtime_image": "sha256:test-runtime",
            "rust_image": "rust:test",
            "campaign": "test",
        }

    def test_manifest_rejects_zero_repeat_and_oversized_batch(self) -> None:
        invalid = copy.deepcopy(self.manifest)
        invalid["repeat"] = 0
        with self.assertRaisesRegex(ValueError, "repeat"):
            validate_manifest(invalid)
        invalid = copy.deepcopy(self.manifest)
        invalid["batch_rows"] = 65_537
        with self.assertRaisesRegex(ValueError, "batch_rows"):
            validate_manifest(invalid)

    def test_aggregate_requires_zero_differential(self) -> None:
        invalid = copy.deepcopy(self.raw)
        invalid["writes"][0]["differences"] = 1
        with self.assertRaisesRegex(RuntimeError, "differenziale"):
            aggregate(invalid, self.manifest)

    def test_identical_baseline_passes(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        comparison = compare_baseline(
            summary,
            self.environment,
            {
                "environment": copy.deepcopy(self.environment),
                "summary": copy.deepcopy(summary),
            },
            self.regression_budget,
        )
        self.assertEqual(comparison["status"], "passed")

    def test_latency_and_throughput_regressions_fail(self) -> None:
        baseline = aggregate(self.raw, self.manifest)
        current = copy.deepcopy(baseline)
        current["replace_total_micros"]["p95"] *= 2
        with self.assertRaisesRegex(RuntimeError, "latenza replace"):
            compare_baseline(
                current,
                self.environment,
                {"environment": self.environment, "summary": baseline},
                self.regression_budget,
            )
        current = copy.deepcopy(baseline)
        current["read_rows_per_second"]["median"] /= 2
        with self.assertRaisesRegex(RuntimeError, "throughput read"):
            compare_baseline(
                current,
                self.environment,
                {"environment": self.environment, "summary": baseline},
                self.regression_budget,
            )

    def test_different_environment_is_not_compared(self) -> None:
        summary = aggregate(self.raw, self.manifest)
        baseline_environment = copy.deepcopy(self.environment)
        baseline_environment["cpu_count"] = 4
        comparison = compare_baseline(
            summary,
            self.environment,
            {"environment": baseline_environment, "summary": summary},
            self.regression_budget,
        )
        self.assertEqual(comparison["status"], "not_comparable")
        self.assertEqual(comparison["environment_mismatches"], ["cpu_count"])


if __name__ == "__main__":
    unittest.main()
