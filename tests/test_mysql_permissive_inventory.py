"""Mutazioni negative del dominio P1, senza server."""
import copy
import hashlib
import json
import unittest
from scripts.mysql_permissive_inventory import DIRECTORY, ROOT, generate, read, validate, verify_execution


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = read(ROOT / "tests/fixtures/ammissione_scrittura/mysql.json")
        self.declarations = read(DIRECTORY / "declarations.json")

    def test_generated_inventory(self):
        self.assertEqual(read(DIRECTORY / "inventory.json"), generate(self.fixture, self.declarations))

    def test_baseline_artifacts_match_capture_provenance(self):
        provenance = read(DIRECTORY / "baseline_provenance.json")
        self.assertEqual(provenance["base"], self.declarations["base"])
        for name, digest in provenance["snapshots"].items():
            self.assertEqual(hashlib.sha256((DIRECTORY / name).read_bytes()).hexdigest(), digest, name)

    def test_c9_adjustment_changes_only_nine_nonfinite_strict_cells(self):
        provenance = read(DIRECTORY / "baseline_provenance.json")
        adjustment, = provenance["adjustments"]
        baseline = read(DIRECTORY / adjustment["snapshot"])
        changed = set()
        for cell in baseline:
            if cell["key"]["nativo"] != adjustment["native"]:
                continue
            for row in cell["rows"]:
                if row["vector"] not in adjustment["vectors"]:
                    continue
                self.assertTrue(row["fixture_refused"])
                self.assertEqual(row["strict"]["category"], "data_mapping")
                self.assertEqual(row["strict"]["phase"], "write")
                self.assertEqual(row["strict"]["remote_effect"], "none")
                self.assertEqual(row["strict"]["retry"], {"kind": "never"})
                changed.add((cell["major"], row["vector"]))
                row["strict"] = {"ok": True}
        self.assertEqual(changed, {(major, vector) for major in adjustment["majors"]
                                  for vector in adjustment["vectors"]})
        self.assertEqual(len(changed), 9)
        # The original serde snapshot had no trailing newline. Restoring only
        # these cells must reproduce its exact digest, not just its shape.
        restored = json.dumps(baseline, ensure_ascii=False, indent=2).encode("utf-8")
        self.assertEqual(hashlib.sha256(restored).hexdigest(), adjustment["previous_sha256"])

    def test_negative_mutations(self):
        mutations = [
            lambda f, d: f["misure"][0]["chiavi"][0]["riscritture"].append({"classe": "unknown"}),
            lambda f, d: d["rules"][0].pop("predicate"),
            lambda f, d: d["rules"][0].update(predicate="unknown"),
            lambda f, d: d["predicates"].pop("non_null"),
            lambda f, d: d["predicates"].update(orphan={"expression": "true"}),
            lambda f, d: d["rules"].append({**copy.deepcopy(d["rules"][0]), "id": "mysql.orphan"}),
            lambda f, d: d["rules"][0]["proofs"].pop("8.0"),
            lambda f, d: d["rules"][0].update(basis="exact"),
            lambda f, d: d["rules"][0].update(aliases=["duplicate", "duplicate"]),
            lambda f, d: d["rules"][0]["proofs"]["8.0"].update(baseline="nonexistent"),
            lambda f, d: d["rules"][0]["targets"][0].update(native="float(8,3)"),
            lambda f, d: d["rules"].pop(),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                fixture, declarations = copy.deepcopy(self.fixture), copy.deepcopy(self.declarations)
                mutate(fixture, declarations)
                with self.assertRaises(ValueError):
                    validate(fixture, declarations)

    def test_manifest_requires_executed_not_filtered_or_ignored(self):
        names = self.declarations["baseline_tests"]
        passed = "\n".join(f"test {name} ... ok" for name in names)
        verify_execution(self.declarations, passed)
        for output in ["", passed.replace(" ... ok", " ... ignored", 1),
                       passed.replace(names[0], "nonexistent", 1)]:
            with self.assertRaises(ValueError):
                verify_execution(self.declarations, output)


if __name__ == "__main__":
    unittest.main()
