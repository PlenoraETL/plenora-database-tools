"""Offline architecture/enumeration guard for the real DB-b campaign."""
import ast
import itertools
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUITE = ROOT / "crates/plenora-database-py/python/tests"


class PostgresSurfaceMatrixTests(unittest.TestCase):
    def test_all_registered_properties_fixtures_and_public_adapters(self):
        source = json.loads((ROOT / "tests/fixtures/postgres_geometry_qualification.json").read_text(encoding="utf-8"))
        registry = (ROOT / "crates/plenora-db-postgres/src/geometry_properties.rs").read_text(encoding="utf-8")
        self.assertEqual(source["properties"], re.findall(r'name: "([a-z_]+)"', registry))
        tree = ast.parse((SUITE / "_postgres_surfaces.py").read_text(encoding="utf-8"))
        assigned = {target.id: node.value for node in tree.body if isinstance(node, ast.Assign)
                    for target in node.targets if isinstance(target, ast.Name)}
        surfaces = ast.literal_eval(assigned["SURFACES"])
        self.assertEqual(set(surfaces), {"rust", "cli", "sdk_sync", "sdk_async", "runtime"})
        for name in ("WRITERS", "READERS"):
            adapters = {ast.literal_eval(key) for key in assigned[name].keys}
            self.assertEqual(adapters, set(surfaces))
            for omitted in surfaces:
                self.assertNotEqual(adapters - {omitted}, set(surfaces))
        identities = {(f["name"], f["variant"]) for f in source["fixtures"]}
        expected = set(itertools.product(source["properties"], identities, surfaces))
        self.assertEqual(len(expected), 15 * 14 * 2 * 5)
        for cell in expected:
            self.assertNotEqual(expected - {cell}, expected)

    def test_adapters_only_forward_and_campaign_executes_them(self):
        example = ROOT / "crates/plenora-database-cli/examples"
        direct = (example / "postgres-surfaces.rs").read_text(encoding="utf-8")
        runtime = (example / "postgres_surfaces/host.rs").read_text(encoding="utf-8")
        adapters = (SUITE / "_postgres_surfaces.py").read_text(encoding="utf-8")
        for token in (".prepare_write(", ".write_with_consent(", ".read("):
            self.assertIn(token, direct)
        self.assertIn("RuntimeBinding::new", runtime)
        self.assertIn(".invoke(", runtime)
        for token in ("session.copy_from(", "await session.acopy_from(", "session.read(", "await session.aread(",
                      '"database-write-ipc"', '"database-read-ipc"', "subprocess.run("):
            self.assertIn(token, adapters)
        for text in (direct, runtime, adapters):
            for forbidden in ("GeometryPlan", "PROPERTY_REGISTRY", "classify_metadata", "insert_spatial_metadata"):
                self.assertNotIn(forbidden, text)
        gate = (ROOT / "scripts/check_sdk_tests.py").read_text(encoding="utf-8")
        self.assertIn('"--example", "postgres-surfaces"', gate)
        self.assertIn("PLENORA_PG_SURFACE_BIN", gate)
        self.assertIn("PLENORA_PG_GEOMETRY_FIXTURES", gate)
        workflow = (ROOT / ".github/workflows/sdk-assurance.yml").read_text(encoding="utf-8")
        self.assertIn('"tests/fixtures/postgres_geometry_qualification.json"', workflow)
        self.assertIn("python3 -m unittest discover -s scripts -p test_postgres_surface_matrix.py", workflow)
        self.assertIn("if: always()", workflow)
        from python_test_inventory import sdk_counts
        counts = sdk_counts(["test_postgres_geometry_surfaces.py"])
        self.assertEqual(counts, {"offline": 2, "postgres": 2})

    def test_evidence_rejects_missing_duplicate_or_unexecuted_cells(self):
        import copy
        from check_sdk_tests import postgres_matrix_evidence
        source = json.loads((ROOT / "tests/fixtures/postgres_geometry_qualification.json").read_text(encoding="utf-8"))
        surfaces = ("rust", "cli", "sdk_sync", "sdk_async", "runtime")
        policies = ("strict", "compatible", "lossy", "native")
        rows = [[f["name"], f["variant"], s, p, a, "preserved"]
                for f, s, p, a in itertools.product(source["fixtures"], surfaces, policies, (False, True))]
        document = {"records": rows, "properties": source["properties"], "expected": len(rows) * 15,
                    "asserted": len(rows) * 15, "live": True, "failed": [], "missing": []}
        losses = {"records": [[scope, empty, s, p, a, "reported" if p != "strict" and a else "refused"]
                  for scope, empty, s, p, a in itertools.product(("field", "schema", "aggregate"),
                      (False, True), surfaces, policies, (False, True))]}
        def output(doc, loss):
            return "PLENORA_DB_B_MATRIX " + json.dumps(doc) + "\nPLENORA_DB_B_LOSS_MATRIX " + json.dumps(loss)
        self.assertEqual(postgres_matrix_evidence(output(document, losses))["asserted"], 16800)
        for kind in ("missing", "duplicate", "unexecuted", "property", "loss"):
            doc, loss = copy.deepcopy(document), copy.deepcopy(losses)
            if kind == "missing": doc["records"].pop()
            elif kind == "duplicate": doc["records"].append(doc["records"][0])
            elif kind == "unexecuted": doc["records"][0][-1] = "not_executed"
            elif kind == "property": doc["properties"].pop()
            else: loss["records"].pop()
            with self.subTest(kind=kind), self.assertRaises(RuntimeError):
                postgres_matrix_evidence(output(doc, loss))


if __name__ == "__main__":
    unittest.main()
