"""Regressioni fail-closed dei record release storici e della candidate 1.1.0."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import shutil
import subprocess
import tempfile
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
RELEASE_MANIFEST = gate.RELEASE_MANIFEST
load_json = gate.load_json
validate_final_readiness = gate.validate_final_readiness
validate_release_readiness = gate.validate_release_readiness


HISTORICAL_FILE_HASHES = {
    "release/rc1-readiness.json": "e56d9600bd0d47c6156a26912321b8520eacf03077f0a9f4cf995cf4a57e00ee",
    "release/development.json": "d65e7fcef52e57c52a8bd62c7476b0345ade10051a89bd0d4dc53217af05d04d",
    "release/final-readiness.json": "32d399df116703f4328eb5ded469a76e87e6d0e61ab6242fcfd4b5e0e726994e",
    "docs/RC1-READINESS.md": "e04fb0cc9a3442174f0ea55bd1d5933589a3cc8263ce93f29a6129eb7a133c36",
    "docs/FINAL-1.0.0-READINESS.md": "235b4d073a04606f0c32bdde9b4c5b950ed1806a5eaffdc0dbc0a0a2751c49b7",
}


class FinalReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.document = load_json(FINAL_MANIFEST)

    def test_repository_final_candidate_is_consistent(self) -> None:
        self.assertEqual(
            validate_release_readiness(
                self.document, expected_version=gate.EXPECTED_VERSION
            ),
            [],
        )

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
                self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["claims"]["system_rc"] = True
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["evidence"][0]["revision"] = "0" * 40
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["declared_scope_reductions"] = []
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["release_action"]["tag_created"] = True
        self.assertTrue(validate_release_readiness(document))

    def test_historical_release_records_are_byte_immutable(self) -> None:
        for relative, expected in HISTORICAL_FILE_HASHES.items():
            with self.subTest(path=relative):
                completed = subprocess.run(
                    ["git", "show", f"HEAD:{relative}"],
                    cwd=ROOT,
                    check=True,
                    capture_output=True,
                )
                actual = hashlib.sha256(completed.stdout).hexdigest()
                self.assertEqual(actual, expected)


class Release110ReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.document = load_json(RELEASE_MANIFEST)

    def test_repository_1_1_0_candidate_is_consistent(self) -> None:
        self.assertEqual(
            validate_release_readiness(
                self.document, expected_version=gate.EXPECTED_RELEASE_VERSION
            ),
            [],
        )

    def test_rejects_authentic_1_0_0_record_as_1_1_0_manifest(self) -> None:
        historical = load_json(FINAL_MANIFEST)
        self.assertTrue(
            validate_release_readiness(
                historical, expected_version=gate.EXPECTED_RELEASE_VERSION
            )
        )

    def test_rejects_premature_or_weakened_1_1_0_claims(self) -> None:
        mutations = (
            ("component_version", "1.0.0"),
            ("status", "component_final_tagged"),
            ("independent_review", True),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                document = copy.deepcopy(self.document)
                document[field] = value
                self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["claims"]["system_rc"] = True
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["evidence"][0]["revision"] = "0" * 40
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["mysql_scope"]["dimensions"] = ["XY", "Z"]
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["declared_scope_reductions"] = []
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["external_dependencies"] = []
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["current_icd_baseline"]["revision"] = "0" * 40
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["verification_claim"] = "verified_independently"
        self.assertTrue(validate_release_readiness(document))

        document = copy.deepcopy(self.document)
        document["release_action"]["tag_created"] = True
        self.assertTrue(validate_release_readiness(document))

    def test_1_1_0_manifest_is_bound_to_1_1_0_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "fuzz").mkdir()
            for relative in ("Cargo.toml", "Cargo.lock", "fuzz/Cargo.lock"):
                shutil.copy2(ROOT / relative, root / relative)
            cargo = root / "Cargo.toml"
            cargo.write_text(
                cargo.read_text(encoding="utf-8").replace(
                    'version = "1.1.0"', 'version = "1.0.0"', 1
                ),
                encoding="utf-8",
            )
            self.assertTrue(
                validate_release_readiness(
                    self.document,
                    root,
                    expected_version=gate.EXPECTED_RELEASE_VERSION,
                )
            )


if __name__ == "__main__":
    unittest.main()
