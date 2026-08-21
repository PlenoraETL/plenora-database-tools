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
        self.assertGreaterEqual(len(manifest.cases), 5)
        self.assertEqual(len(manifest.cases), len(set(manifest.cases)))
        self.assertFalse(manifest.cases["postgres.read.events_stream"].mutates)

    def test_manifest_reads_the_mutating_flag(self) -> None:
        """La bandiera si legge da un manifest costruito qui.

        L'unico caso che mutava apparteneva a un dominio uscito da questo
        repository, e il test lo nominava. Ora nessun caso della suite muta:
        legare la prova a un caso reale la renderebbe di nuovo fragile alla
        prima suite che cambia. Quello che si verifica e il parsing.
        """
        document = {
            "schema_version": 1,
            "suite": "test",
            "cases": [
                {"id": "a.read", "provider": "p", "runner": "r", "mutates": False},
                {"id": "a.write", "provider": "p", "runner": "r", "mutates": True},
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            manifest = Manifest(path)
            self.assertFalse(manifest.cases["a.read"].mutates)
            self.assertTrue(manifest.cases["a.write"].mutates)

    def test_manifest_rejects_wrong_runner(self) -> None:
        manifest = Manifest(DEFAULT_MANIFEST)
        with self.assertRaises(HarnessError):
            manifest.require("postgres.read.events_stream", "altro-runner")

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
            recorder.run("postgres.read.events_stream", "postgres", fail)
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

        recorder.run("postgres.read.events_stream", "postgres", count)
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
