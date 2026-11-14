"""Versioned metadata loss examples and relational counter invariants."""
import copy
import json
import unittest

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from render_loss_registry import ROOT
from execution_loss_semantics import validate


class MetadataLossWireTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        directory = ROOT / "contracts/v2"
        schemas = [json.loads(p.read_text(encoding="utf-8")) for p in directory.glob("*.schema.json")]
        registry = Registry().with_resources((s["$id"], Resource.from_contents(s)) for s in schemas)
        schema = json.loads((directory / "execution-loss-report-v4.schema.json").read_text(encoding="utf-8"))
        cls.validator = Draft202012Validator(schema, registry=registry)
        cls.example = json.loads((directory / "examples/execution-loss-v4-metadata.json").read_text(encoding="utf-8"))

    def test_example_is_valid_in_schema_and_common_semantics(self):
        self.validator.validate(self.example)
        validate(self.example)

    def test_metadata_is_one_value_and_must_reference_its_subject(self):
        for mutation in ("duplicate", "counter", "reference", "version"):
            document = copy.deepcopy(self.example)
            if mutation == "duplicate":
                document["risk"].append(copy.deepcopy(document["risk"][0]))
            elif mutation == "counter":
                document["evaluation"]["entries"][0]["checked"] = 2
            elif mutation == "reference":
                document["effective"][0]["subject"] = 1
            else:
                document["schema_version"] = 3
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate(document)

    def test_present_subject_is_never_null_or_unlocalized(self):
        for mutation in ("null", "identity", "scope"):
            document = copy.deepcopy(self.example)
            if mutation == "null":
                document["risk"][0]["subject"] = None
            elif mutation == "identity":
                del document["risk"][0]["subject"]["field_identity"]
            else:
                document["risk"][0]["subject"]["scope"] = "schema"
            self.assertTrue(list(self.validator.iter_errors(document)))


if __name__ == "__main__":
    unittest.main()
