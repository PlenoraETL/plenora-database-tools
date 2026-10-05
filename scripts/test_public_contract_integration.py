"""Regressioni del consumer contro il checkout fissato e un CLI costruito."""

import argparse
import copy
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import check_public_contracts as gate
from scripts import check_runtime_evidence as runtime_gate
from scripts.public_contract_semantics import load_semantics
from scripts.render_adoption_manifest import manifest, validate_manifest


class PublicContractIntegrationTests(unittest.TestCase):
    contracts: Path
    cli: Path
    evidence: Path

    def schema(self, name):
        return self.contracts / "schemas" / name

    def document(self):
        return manifest("6.0.0", [f"database-cli|cli|{self.cli}"], ["contract gate"])

    def assert_structural(self, document, schema):
        Draft202012Validator(gate.load(self.schema(schema))).validate(document)

    def check_capabilities(self, document):
        run_cli = gate.run_cli

        def invoke(cli, *arguments, **kwargs):
            result = run_cli(cli, *arguments, **kwargs)
            if arguments[0] == "capabilities":
                result["result"] = document
            return result

        self.assert_structural(document, "capabilities-v2.schema.json")
        with patch.object(gate, "run_cli", side_effect=invoke):
            with self.assertRaisesRegex(RuntimeError, "invarianti semantiche"):
                gate.check(self.contracts, self.cli)

    def test_real_cli_passes(self):
        result = gate.check(self.contracts, self.cli)
        self.assertGreater(result["operations"], 0)

    def test_duplicate_operation_is_rejected_before_dictionary_collapses_it(self):
        document = gate.run_cli(self.cli, "capabilities", "--format", "json", success=True)["result"]
        duplicate = copy.deepcopy(document["operations"][0])
        duplicate["status"] = "unavailable"
        duplicate["reason"] = "regression fixture"
        document["operations"].insert(0, duplicate)
        self.check_capabilities(document)

    def test_operation_cannot_claim_an_undeclared_interface(self):
        document = gate.run_cli(self.cli, "capabilities", "--format", "json", success=True)["result"]
        document["interfaces"][0]["kind"] = "rust"
        self.check_capabilities(document)

    def test_manifest_from_real_artifact_passes(self):
        validate_manifest(self.document(), self.schema("adoption-manifest-v4.schema.json"))

    def test_schema_valid_manifest_counterexamples_are_rejected(self):
        examples = self.contracts / "examples" / "invalid"
        for name in (
            "adoption-v4-duplicate-contract.json",
            "adoption-v4-undeclared-artifact.json",
            "adoption-v4-duplicate-artifact.json",
            "adoption-v4-conflicting-surface.json",
        ):
            with self.subTest(example=name):
                document = gate.load(examples / name)
                self.assert_structural(document, "adoption-manifest-v4.schema.json")
                with self.assertRaisesRegex(ValueError, "invarianti semantiche"):
                    validate_manifest(document, self.schema("adoption-manifest-v4.schema.json"))

    def test_consistent_repeated_evidence_remains_valid(self):
        document = self.document()
        for key in ("artifacts", "contracts"):
            duplicate = copy.deepcopy(document[key][0])
            duplicate["verification"] = ["additional evidence"]
            document[key].append(duplicate)
        validate_manifest(document, self.schema("adoption-manifest-v4.schema.json"))

    def test_manifest_command_writes_only_after_semantic_validation(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            first, second, output = root / "first.bin", root / "second.bin", root / "manifest.json"
            first.write_bytes(b"first artifact")
            second.write_bytes(b"different artifact")
            command = [
                sys.executable, str(ROOT / "scripts" / "render_adoption_manifest.py"),
                "--version", "6.0.0", "--verification", "contract gate",
                "--schema", str(self.schema("adoption-manifest-v4.schema.json")),
                "--output", str(output), "--artifact", f"database-cli|cli|{first}",
            ]
            failed = subprocess.run(
                [*command, "--artifact", f"database-cli|cli|{second}"], capture_output=True, text=True,
            )
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn("invarianti semantiche", failed.stderr)
            self.assertFalse(output.exists())
            subprocess.run(command, capture_output=True, check=True)
            validate_manifest(gate.load(output), self.schema("adoption-manifest-v4.schema.json"))

    def upstream_copy(self, directory):
        """Una copia di `contracts/upstream` da alterare senza toccare il repository."""

        import shutil

        target = Path(directory) / "upstream"
        shutil.copytree(gate.UPSTREAM, target)
        return target

    def test_upstream_copy_differing_from_the_pin_is_rejected(self):
        with TemporaryDirectory() as directory:
            upstream = self.upstream_copy(directory)
            vector = upstream / "arrow-v1" / "resolved-point.json"
            vector.write_bytes(vector.read_bytes().replace(b"4326", b"3857", 1))
            with patch.object(gate, "UPSTREAM", upstream):
                with self.assertRaisesRegex(RuntimeError, "copia diversa"):
                    gate.check(self.contracts, self.cli)

    def test_pinned_vector_without_an_executed_copy_is_rejected(self):
        with TemporaryDirectory() as directory:
            upstream = self.upstream_copy(directory)
            source = json.loads((upstream / "source.json").read_text(encoding="utf-8"))
            del source["files"]["arrow-v1/missing-crs.json"]
            (upstream / "source.json").write_text(json.dumps(source), encoding="utf-8")
            with patch.object(gate, "UPSTREAM", upstream):
                with self.assertRaisesRegex(RuntimeError, "senza copia eseguita"):
                    gate.check(self.contracts, self.cli)

    def test_manifest_with_declared_deviations_passes_the_pinned_validator(self):
        document = self.document()
        self.assertTrue(document["deviations"])
        validate_manifest(document, self.schema("adoption-manifest-v4.schema.json"))

    def runtime_evidence_copy(self, directory):
        import shutil

        target = Path(directory) / "evidence"
        shutil.copytree(self.evidence, target)
        return target

    def test_runtime_evidence_passes_the_pinned_schemas(self):
        result = runtime_gate.check(self.contracts, self.evidence)
        self.assertEqual(result["runtime_operations"], 8)

    def test_runtime_evidence_counterexamples_are_rejected(self):
        def missing(evidence):
            (evidence / "database.write.json").unlink()

        def unsafe_retry(evidence):
            path = evidence / "database-write-error.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            # ERR-006: un effetto ignoto non ammette un retry automatico.
            document["payload"]["retry"] = {"kind": "safe"}
            path.write_text(json.dumps(document), encoding="utf-8")

        def lost_correlation(evidence):
            path = evidence / "database-query-success.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            del document["metadata"]["plenora.trace.correlation_id"]
            path.write_text(json.dumps(document), encoding="utf-8")

        def foreign_result(evidence):
            path = evidence / "database.list_catalogs.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["payload"] = {"schemas": ["public"]}
            path.write_text(json.dumps(document), encoding="utf-8")

        def extra_operation(evidence):
            path = evidence / "capabilities.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["operations"] = document["operations"][:-1]
            path.write_text(json.dumps(document), encoding="utf-8")

        for mutation in (missing, unsafe_retry, lost_correlation, foreign_result, extra_operation):
            with self.subTest(mutation=mutation.__name__):
                with TemporaryDirectory() as directory:
                    evidence = self.runtime_evidence_copy(directory)
                    mutation(evidence)
                    with self.assertRaises(RuntimeError):
                        runtime_gate.check(self.contracts, evidence)

    def test_wrong_pin_fails_closed(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "source.json"
            source.write_text(json.dumps({"contracts_source": {"revision": "0" * 40}}), encoding="utf-8")
            with patch("scripts.public_contract_semantics.SOURCE", source):
                with self.assertRaisesRegex(RuntimeError, "diverso dal pin"):
                    load_semantics(self.contracts)

    def test_missing_semantic_module_fails_closed(self):
        with patch("scripts.public_contract_semantics.importlib.util.spec_from_file_location", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "non disponibili"):
                load_semantics(self.contracts)

    def test_schema_errors_do_not_expose_document_values(self):
        secret = "private-payload-regression-marker"
        with self.assertRaises(RuntimeError) as error:
            gate.validate(secret, {"type": "object"}, "capability document", gate.Registry())
        self.assertNotIn(secret, str(error.exception))
        document = self.document()
        document["schema_version"] = secret
        with self.assertRaises(ValueError) as error:
            validate_manifest(document, self.schema("adoption-manifest-v4.schema.json"))
        self.assertNotIn(secret, str(error.exception))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contracts", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    # Prodotte da `cargo test -p plenora-database-engine --test runtime_vectors`
    # con PLENORA_RUNTIME_EVIDENCE: obbligatorie, un gate saltato non e un gate.
    parser.add_argument("--runtime-evidence", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    PublicContractIntegrationTests.contracts = args.contracts.resolve()
    PublicContractIntegrationTests.cli = args.cli.resolve()
    PublicContractIntegrationTests.evidence = args.runtime_evidence.resolve()
    unittest.main(argv=[sys.argv[0], *remaining])
