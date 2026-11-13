"""Mutazioni negative del dominio P1, senza server."""
import copy
import hashlib
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
