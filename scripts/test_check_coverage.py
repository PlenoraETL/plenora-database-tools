#!/usr/bin/env python3
"""Self-test del gate di coverage."""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import check_coverage


def report(*, covered: int = 34, count: int = 100, percent: float = 34.0) -> dict:
    totals = {
        metric: {"covered": covered, "count": count, "percent": percent}
        for metric in check_coverage.METRICS
    }
    return {
        "type": "llvm.coverage.json.export",
        "version": "3.1.0",
        "data": [{"totals": totals}],
    }


def budget(*, minimum: float = 34.0) -> dict:
    return {
        "schema_version": 1,
        "surfaces": {
            "unit": {
                "minimum_percent": {
                    metric: minimum for metric in check_coverage.METRICS
                }
            }
        },
    }


class CoverageGateTests(unittest.TestCase):
    def write(self, directory: Path, name: str, value: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def run_check(self, report_value: dict, budget_value: dict) -> bool:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                return check_coverage.check(
                    self.write(root, "summary.json", report_value),
                    self.write(root, "budget.json", budget_value),
                    "unit",
                )

    def test_equal_to_budget_passes(self) -> None:
        self.assertTrue(self.run_check(report(), budget()))

    def test_a_metric_below_budget_fails(self) -> None:
        value = report()
        value["data"][0]["totals"]["lines"] = {
            "covered": 33,
            "count": 100,
            "percent": 33.0,
        }
        self.assertFalse(self.run_check(value, budget()))

    def test_missing_surface_and_metric_are_rejected(self) -> None:
        with self.assertRaises(check_coverage.CoverageError):
            check_coverage.read_budget(budget(), "missing")
        incomplete = budget()
        del incomplete["surfaces"]["unit"]["minimum_percent"]["regions"]
        with self.assertRaises(check_coverage.CoverageError):
            check_coverage.read_budget(incomplete, "unit")

    def test_report_must_have_one_complete_data_block(self) -> None:
        for value in (
            {"type": "llvm.coverage.json.export", "data": []},
            {"type": "wrong", "data": report()["data"]},
        ):
            with self.subTest(value=value), self.assertRaises(
                check_coverage.CoverageError
            ):
                check_coverage.read_totals(value)

    def test_inconsistent_or_non_finite_numbers_are_rejected(self) -> None:
        inconsistent = report(percent=99.0)
        non_finite = report(percent=float("nan"))
        boolean = report()
        boolean["data"][0]["totals"]["lines"]["covered"] = True
        for value in (inconsistent, non_finite, boolean):
            with self.subTest(value=value), self.assertRaises(
                check_coverage.CoverageError
            ):
                check_coverage.read_totals(value)


if __name__ == "__main__":
    unittest.main()
