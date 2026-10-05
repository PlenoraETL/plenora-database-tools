from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.render_adoption_manifest import SOURCE, manifest, validate_manifest
import json


class AdoptionManifestTests(unittest.TestCase):
    def test_records_real_digest_modes_pin_and_not_applicable_runtime(self) -> None:
        with TemporaryDirectory() as directory:
            wheel = Path(directory) / "plenora_database-4.0.0.whl"
            wheel.write_bytes(b"released wheel")
            document = manifest(
                "4.0.0",
                [f"plenora-database|python_sdk|{wheel}|sync,async"],
                ["python scripts/check_sdk_tests.py --scope contract"],
            )
        artifact = document["artifacts"][0]
        self.assertEqual(artifact["api_modes"], ["sync", "async"])
        self.assertRegex(artifact["digest"], r"^sha256:[0-9a-f]{64}$")
        self.assertRegex(
            document["contracts_source"]["revision"], r"^[0-9a-f]{40}$"
        )
        runtime = next(
            item
            for item in document["contracts"]
            if item["id"] == "plenora-runtime-binding-v1"
        )
        self.assertEqual(runtime["status"], "not_applicable")

    def test_declared_deviations_reach_the_manifest(self) -> None:
        # Il manifest scriveva `deviations: []` fisso mentre il codice ne
        # dichiarava una: la sorgente e l'unico posto in cui si dichiarano.
        declared = json.loads(SOURCE.read_text(encoding="utf-8"))["deviations"]
        self.assertTrue(declared)
        with TemporaryDirectory() as directory:
            cli = Path(directory) / "plenora-database"
            cli.write_bytes(b"released cli")
            document = manifest("7.0.0", [f"plenora-database|cli|{cli}"], ["gate"])
        self.assertEqual(document["deviations"], declared)

    def test_validation_fails_closed_against_the_supplied_schema(self) -> None:
        with TemporaryDirectory() as directory:
            schema = Path(directory) / "schema.json"
            schema.write_text(
                '{"$schema":"https://json-schema.org/draft/2020-12/schema",'
                '"type":"object","required":["schema_version"],'
                '"properties":{"schema_version":{"const":999}}}',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "manifest non valido"):
                validate_manifest({"schema_version": 4}, schema)


if __name__ == "__main__":
    unittest.main()
