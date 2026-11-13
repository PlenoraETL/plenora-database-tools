"""WriteOutcome v3 mantiene chiuso il documento e distingue l'assenza."""
import copy
import unittest

from scripts import phase0_validate as gate
from scripts.execution_loss_semantics import validate_outcome


class LossOutcomeContractTests(unittest.TestCase):
    def test_v3_requires_valid_report_and_rejects_unknown_members(self):
        schemas = gate.discover_schemas(gate.REPO_ROOT / "contracts")
        registry = gate.build_registry(schemas.values())
        schema = schemas[(gate.ACTIVE_CONTRACT_ROOT / "write-outcome-v3.schema.json").resolve()]
        valid = gate.load_json(gate.ACTIVE_CONTRACT_ROOT / "examples/outcome-v3-committed.json")
        validator = gate.Draft202012Validator(schema, registry=registry)
        validator.validate(valid)
        for mutation in ("absent", "null", "unknown", "unknown_report", "oversized"):
            value = copy.deepcopy(valid)
            if mutation == "absent":
                del value["loss_report"]
            elif mutation == "null":
                value["loss_report"] = None
            elif mutation == "unknown":
                value["extra"] = True
            elif mutation == "unknown_report":
                value["loss_report"]["extra"] = True
            else:
                value["loss_report"]["risk"][0]["reason"] = "x" * 1025
            with self.subTest(mutation=mutation):
                self.assertFalse(validator.is_valid(value))

    def test_shared_rust_schema_acceptance_matrix(self):
        schemas = gate.discover_schemas(gate.REPO_ROOT / "contracts")
        registry = gate.build_registry(schemas.values())
        schema = schemas[(gate.ACTIVE_CONTRACT_ROOT / "write-outcome-v3.schema.json").resolve()]
        validator = gate.Draft202012Validator(schema, registry=registry)
        cases = gate.load_json(gate.REPO_ROOT / "crates/plenora-database-core/tests/write_outcome_validation.json")
        self.assertEqual({case["document"]["status"] for case in cases},
                         {"committed", "rolled_back", "partially_committed", "outcome_unknown"})
        for case in cases:
            with self.subTest(name=case["name"]):
                structural = validator.is_valid(case["document"])
                self.assertEqual(structural, case.get("structural", case["valid"]))
                accepted = structural
                if accepted:
                    try:
                        validate_outcome(case["document"])
                    except ValueError:
                        accepted = False
                self.assertEqual(accepted, case["valid"])
