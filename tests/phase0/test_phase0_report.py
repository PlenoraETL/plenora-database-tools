from __future__ import annotations

import unittest

from scripts.phase0_report import ReportError, aggregate, nearest_rank, render_markdown


def sample(case_id: str, wall_ns: int, summary: object) -> dict[str, object]:
    return {
        "case_id": case_id,
        "provider": "test",
        "status": "passed",
        "metrics": {
            "wall_ns": wall_ns,
            "rss_before_bytes": 100,
            "rss_after_bytes": 120,
        },
        "summary": summary,
    }


class Phase0ReportTests(unittest.TestCase):
    def test_nearest_rank(self) -> None:
        self.assertEqual(nearest_rank([1, 2, 3, 4, 5], 0.95), 5)
        self.assertEqual(nearest_rank([5, 1, 3], 0.5), 3)
        with self.assertRaises(ReportError):
            nearest_rank([], 0.95)

    def test_aggregate_median_and_stability(self) -> None:
        report = aggregate(
            [
                sample("x", 30, {"rows": 10}),
                sample("x", 10, {"rows": 10}),
                sample("x", 20, {"rows": 10}),
            ]
        )
        case = report["cases"][0]
        self.assertEqual(case["wall_ns"]["median"], 20)
        self.assertEqual(case["wall_ns"]["p95_nearest_rank"], 30)
        self.assertTrue(case["stable_summary"])
        self.assertEqual(case["rss_delta_bytes"]["median"], 20)

    def test_detects_unstable_summary(self) -> None:
        report = aggregate(
            [sample("x", 10, {"rows": 1}), sample("x", 11, {"rows": 2})]
        )
        self.assertFalse(report["cases"][0]["stable_summary"])
        self.assertEqual(report["totals"]["unstable_summaries"], 1)

    def test_markdown_contains_case(self) -> None:
        report = aggregate([sample("case.a", 1_500_000, {"ok": True})])
        rendered = render_markdown(report)
        self.assertIn("case.a", rendered)
        self.assertIn("1.500", rendered)


if __name__ == "__main__":
    unittest.main()
