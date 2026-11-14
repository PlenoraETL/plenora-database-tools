"""B-13 closed boundary census and unique native lifecycle guard."""
import copy
import json
import unittest
from pathlib import Path
from scripts import python_test_inventory

ROOT = Path(__file__).resolve().parents[1]
SDK = ROOT / "crates/plenora-database-py"
MANIFEST = SDK / "python/tests/fixtures/sdk_stream_boundaries.json"


def validate(document):
    ids = [entry["id"] for entry in document["boundaries"]]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate boundary")
    required = {
        "postgres_read_sync", "family_read_sync", "postgres_read_async", "family_read_async",
        "next_sync", "next_async", "schema_sync", "schema_async", "close_sync", "close_async",
        "capsule_export", "lazy_callback", "table_materialize", "capsule_import", "imported_reader", "write_source",
        "async_batch_decode", "async_schema_decode", "async_table", "release_capsule", "cancel_decoder",
        "session_query", "family_query", "transaction_query", "async_transaction_query", "async_session_query", "async_family_query",
    }
    if set(ids) != required:
        raise ValueError("unreviewed boundary census")
    for entry in document["boundaries"]:
        source = (SDK / entry["owner"]).read_text(encoding="utf-8")
        if entry["entry"] not in source:
            raise ValueError("missing boundary implementation")
        filename, name = entry["proof"].split("::")
        path = SDK / filename if filename.startswith("src/") else SDK / "python/tests" / filename
        if "fn " + name not in path.read_text(encoding="utf-8") and "def " + name not in path.read_text(encoding="utf-8"):
            raise ValueError("missing executable proof")


class StreamBoundaries(unittest.TestCase):
    def test_closed_census_and_proofs(self):
        validate(json.loads(MANIFEST.read_text(encoding="utf-8")))

    def test_every_omission_is_rejected(self):
        document = json.loads(MANIFEST.read_text(encoding="utf-8"))
        for index in range(len(document["boundaries"])):
            broken = copy.deepcopy(document)
            del broken["boundaries"][index]
            with self.assertRaises(ValueError):
                validate(broken)

    def test_conditions_cross_every_c_and_async_boundary(self):
        document = json.loads(MANIFEST.read_text(encoding="utf-8"))
        cases = list(python_test_inventory.cases(SDK / "python/tests/test_stream_channel.py"))
        for name, kind in (("test_c_boundaries", "c_stream"), ("test_async_boundaries", "async")):
            actual = {(bindings["boundary"].value, bindings["condition"].value, bindings["position"].value)
                      for node, bindings in cases if node.name == name}
            boundaries = {entry["id"] for entry in document["boundaries"]
                          if entry["kind"] == kind and entry["proof"].endswith("::" + name)}
            expected = {(boundary, condition, position) for boundary in boundaries
                        for condition in document["conditions"] for position in document["callback_positions"]}
            self.assertEqual(actual, expected)

    def test_factories_and_consumers_use_the_unique_lifecycle(self):
        native = (SDK / "src/arrow_reader.rs").read_text(encoding="utf-8")
        self.assertEqual(native.count("crate::stream_reader::Reader::new("), 2)
        self.assertNotIn("next_batch(", native)
        self.assertNotIn("Mutex<", native)
        for filename, token in (("src/family_arrow_reader.rs", "BatchReader::new("),
                                ("src/async_session_family.rs", "AsyncBatchReader::new(")):
            self.assertIn(token, (SDK / filename).read_text(encoding="utf-8"))
        lifecycle = (SDK / "src/stream_reader.rs").read_text(encoding="utf-8")
        self.assertEqual(lifecycle.count("stream.next_batch("), 1)
        self.assertIn("self.state.publish(result)", lifecycle)
        self.assertIn("validated_batch(", lifecycle)
        for entry in json.loads(MANIFEST.read_text(encoding="utf-8"))["boundaries"]:
            if entry["kind"] == "eager":
                source = (SDK / entry["owner"]).read_text(encoding="utf-8")
                self.assertIn("to_py_err", source)
                self.assertNotIn("RecordBatchReader", source)

    def test_live_campaign_is_in_the_executed_suite(self):
        cases = python_test_inventory.sdk_counts(["test_stream_live.py"])
        self.assertEqual(set(cases), {"postgres"})
        self.assertEqual(cases["postgres"], 10)
        workflow = (ROOT / ".github/workflows/rust-ci.yml").read_text(encoding="utf-8")
        self.assertIn("scripts.test_sdk_stream_boundaries", workflow)


if __name__ == "__main__":
    unittest.main()
