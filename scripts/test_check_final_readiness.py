"""Regressioni fail-closed della metadata candidate finale 1.0.0."""

from __future__ import annotations

import copy
import importlib.util
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GATE = ROOT / "scripts" / "check_final_readiness.py"
SPEC = importlib.util.spec_from_file_location("final_gate", GATE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("checker finale non importabile")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
FINAL_MANIFEST = gate.FINAL_MANIFEST
load_json = gate.load_json
validate_final_readiness = gate.validate_final_readiness


HISTORICAL_PATHS = (
    "release/rc1-readiness.json",
    "release/development.json",
    "docs/RC1-READINESS.md",
)


class FinalReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.document = load_json(FINAL_MANIFEST)

    def test_repository_final_candidate_is_consistent(self) -> None:
        self.assertEqual(validate_final_readiness(self.document), [])

    def test_rejects_premature_claims_and_evidence_drift(self) -> None:
        mutations = (
            ("component_version", "0.1.0-rc.1"),
            ("status", "component_final_tagged"),
            ("independent_review", True),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                document = copy.deepcopy(self.document)
                document[field] = value
                self.assertTrue(validate_final_readiness(document))

        document = copy.deepcopy(self.document)
        document["claims"]["system_rc"] = True
        self.assertTrue(validate_final_readiness(document))

        document = copy.deepcopy(self.document)
        document["evidence"][0]["revision"] = "0" * 40
        self.assertTrue(validate_final_readiness(document))

        document = copy.deepcopy(self.document)
        document["declared_scope_reductions"] = []
        self.assertTrue(validate_final_readiness(document))

        document = copy.deepcopy(self.document)
        document["release_action"]["tag_created"] = True
        self.assertTrue(validate_final_readiness(document))

    def test_historical_rc1_records_are_not_in_the_diff(self) -> None:
        completed = subprocess.run(
            ["git", "diff", "--name-only", "--", *HISTORICAL_PATHS],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "")


if __name__ == "__main__":
    unittest.main()
