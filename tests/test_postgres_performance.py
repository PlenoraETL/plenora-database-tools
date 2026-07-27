from __future__ import annotations

import copy
import unittest

from scripts.check_postgres_performance import (
    compare_reports,
    percentile_nearest_rank,
    validate_manifest,
)


class PostgresPerformanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.budget = {
            "environment_must_match": [
                "postgres_major",
                "postgis_major_minor",
                "platform",
                "cpu_count",
            ],
            "maximum_regression_percent": {
                "read_total_median": 5.0,
                "read_total_p95": 10.0,
                "time_to_first_batch_median": 10.0,
                "write_execute_median": 5.0,
                "write_execute_p95": 10.0,
                "peak_rss_bytes": 10.0,
                "wal_bytes": 5.0,
            },
        }
        write = {
            mode: {
                "execute_micros": {"median": 100.0, "p95": 110.0},
                "wal_bytes": {"median": 1000.0},
            }
            for mode in ("copy_text", "copy_binary", "prepared")
        }
        self.report = {
            "environment": {
                "postgres_major": "16",
                "postgis_major_minor": "3.4",
                "platform": "test-x86_64",
                "cpu_count": 8,
            },
            "summaries": [
                {
                    "profile": "narrow",
                    "rows": 1000,
                    "batch_rows": 1000,
                    "target_batch_bytes": None,
                    "peak_rss_bytes": 1_000_000,
                    "read": {
                        "total_micros": {"median": 100.0, "p95": 110.0},
                        "first_batch_micros": {"median": 10.0},
                    },
                    "write": write,
                }
            ],
        }

    def test_nearest_rank_p95(self) -> None:
        self.assertEqual(percentile_nearest_rank([4, 1, 3, 2, 5], 0.95), 5)

    def test_manifest_rejects_duplicate_scenario(self) -> None:
        scenario = {"profile": "narrow", "rows": 1000, "batch_rows": 1000}
        manifest = {
            "schema_version": 1,
            "campaign": "test",
            "warmup": 1,
            "repeat": 5,
            "scenarios": [scenario, scenario.copy()],
        }
        with self.assertRaisesRegex(ValueError, "duplicato"):
            validate_manifest(manifest)

    def test_manifest_rejects_duplicate_mode(self) -> None:
        manifest = {
            "schema_version": 1,
            "campaign": "test",
            "warmup": 1,
            "repeat": 5,
            "scenarios": [
                {
                    "profile": "wide",
                    "rows": 1000,
                    "batch_rows": 1000,
                    "modes": ["copy_binary", "copy_binary"],
                }
            ],
        }
        with self.assertRaisesRegex(ValueError, "modes"):
            validate_manifest(manifest)

    def test_manifest_accepts_read_only_scenario(self) -> None:
        validate_manifest(
            {
                "schema_version": 1,
                "campaign": "read-only",
                "warmup": 2,
                "repeat": 20,
                "scenarios": [
                    {
                        "profile": "spatial",
                        "rows": 100_000,
                        "batch_rows": 8192,
                        "modes": [],
                    }
                ],
            }
        )

    def test_identical_report_passes(self) -> None:
        comparison = compare_reports(self.report, self.report, self.budget)
        self.assertEqual(comparison["status"], "passed")
        self.assertEqual(comparison["regressions"], [])

    def test_time_wal_and_rss_regressions_are_detected(self) -> None:
        current = copy.deepcopy(self.report)
        summary = current["summaries"][0]
        summary["read"]["total_micros"]["median"] = 120.0
        summary["write"]["copy_binary"]["wal_bytes"]["median"] = 1200.0
        summary["peak_rss_bytes"] = 1_200_000
        comparison = compare_reports(current, self.report, self.budget)
        self.assertEqual(comparison["status"], "failed")
        metrics = {item["metric"] for item in comparison["regressions"]}
        self.assertTrue(
            {"read_total_median", "wal_bytes", "peak_rss_bytes"} <= metrics
        )

    def test_environment_mismatch_is_not_comparable(self) -> None:
        current = copy.deepcopy(self.report)
        current["environment"]["postgres_major"] = "17"
        comparison = compare_reports(current, self.report, self.budget)
        self.assertEqual(comparison["status"], "not_comparable")
        self.assertEqual(comparison["environment_mismatches"], ["postgres_major"])

    def test_missing_baseline_scenario_is_not_comparable(self) -> None:
        current = copy.deepcopy(self.report)
        current["summaries"][0]["batch_rows"] = 8192
        comparison = compare_reports(current, self.report, self.budget)
        self.assertEqual(comparison["status"], "not_comparable")
        self.assertEqual(
            comparison["missing_baseline_scenarios"],
            [["narrow", 1000, 8192, None, 256, False, True, False]],
        )


if __name__ == "__main__":
    unittest.main()
