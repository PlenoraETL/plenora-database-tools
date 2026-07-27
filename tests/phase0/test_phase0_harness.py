from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.phase0_harness import (
    DEFAULT_MANIFEST,
    HarnessError,
    Manifest,
    Recorder,
    stable_json_digest,
    write_jsonl_atomic,
)


class Phase0HarnessTests(unittest.TestCase):
    def test_manifest_has_unique_registered_cases(self) -> None:
        manifest = Manifest(DEFAULT_MANIFEST)
        self.assertGreaterEqual(len(manifest.cases), 10)
        self.assertTrue(manifest.cases["arcgis.write.apply_edits"].mutates)
        self.assertFalse(manifest.cases["postgres.read.events_stream"].mutates)

    def test_manifest_rejects_wrong_runner(self) -> None:
        manifest = Manifest(DEFAULT_MANIFEST)
        with self.assertRaises(HarnessError):
            manifest.require("arcgis.read.features", "postgres")

    def test_digest_is_order_stable_for_objects(self) -> None:
        self.assertEqual(
            stable_json_digest({"a": 1, "b": 2}),
            stable_json_digest({"b": 2, "a": 1}),
        )

    def test_recorder_redacts_exception_message(self) -> None:
        manifest = Manifest(DEFAULT_MANIFEST)
        recorder = Recorder(manifest)

        def fail() -> dict[str, object]:
            raise ValueError("password=secret-token")

        with self.assertRaises(HarnessError):
            recorder.run("backend.static_inventory", "inventory", fail)
        encoded = json.dumps(recorder.records)
        self.assertNotIn("secret-token", encoded)
        self.assertEqual(
            recorder.records[0]["error"]["message"], "case execution failed"
        )

    def test_recorder_warmup_and_repetition(self) -> None:
        manifest = Manifest(DEFAULT_MANIFEST)
        recorder = Recorder(manifest, repeat=3, warmup=2)
        calls = 0

        def count() -> dict[str, int]:
            nonlocal calls
            calls += 1
            return {"calls": calls}

        recorder.run("backend.static_inventory", "inventory", count)
        self.assertEqual(calls, 5)
        self.assertEqual(len(recorder.records), 3)
        self.assertEqual(
            [record["sample_index"] for record in recorder.records],
            [1, 2, 3],
        )
        self.assertTrue(
            all(record["sample_count"] == 3 for record in recorder.records)
        )

    def test_jsonl_publish_replaces_complete_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "result.jsonl"
            write_jsonl_atomic(target, [{"x": 1}, {"x": 2}])
            lines = target.read_text(encoding="utf-8").splitlines()
            self.assertEqual([json.loads(line) for line in lines], [{"x": 1}, {"x": 2}])


if __name__ == "__main__":
    unittest.main()
