#!/usr/bin/env python3
"""Gate offline degli inventari delle campagne, eseguito anche sulle PR."""

import argparse
import re
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import live_inventory, mysql_inventory, python_test_inventory


def check_probes(expected, source, pattern):
    declared = set(re.findall(pattern, source))
    if len(expected) != len(set(expected)) or set(expected) != declared:
        raise RuntimeError(f"inventario sonde divergente: {set(expected) ^ declared}")


def check():
    import tomllib
    import yaml
    from scripts import check_mysql_reference as mysql
    from scripts import check_mariadb_driver as mariadb
    from scripts import check_session_matrix as session
    from scripts import check_postgres_reference as postgres
    from scripts import check_sqlserver_reference as sqlserver
    from scripts import check_oracle_reference as oracle
    from scripts import check_db2_reference as db2

    mysql.validate_inventory()
    for runner, collect in ((postgres, postgres.live_test_inventory),
                            (sqlserver, sqlserver.live_test_inventory),
                            (oracle, oracle.live_source_inventory),
                            (db2, db2.live_test_inventory)):
        names = collect()
        if not runner.REQUIRED_LIVE_TESTS <= names:
            raise RuntimeError(f"requisiti senza test: {runner.__name__}")
    for expected, filename, pattern in (
        (mariadb.EXPECTED_PROBES, "mariadb_evidence.rs", r'"((?:raw|provider)\.[a-z_]+)"'),
        (session.EXPECTED_PROBES, "session_evidence.rs", r'"((?:bootstrap|transaction)\.[a-z_.]+)"'),
    ):
        check_probes(expected, (mysql_inventory.SOURCE_DIR / filename).read_text(encoding="utf-8"), pattern)
    counts = python_test_inventory.sdk_counts()
    total = sum(python_test_inventory.count([p.name]) for p in python_test_inventory.SUITE.glob("test_*.py"))
    if sum(counts.values()) != total:
        raise RuntimeError("classificazione SDK incompleta")
    fuzz = tomllib.loads((ROOT / "fuzz/Cargo.toml").read_text(encoding="utf-8"))
    workflow = yaml.safe_load((ROOT / ".github/workflows/fuzz.yml").read_text(encoding="utf-8"))
    targets = workflow["jobs"]["fuzz"]["strategy"]["matrix"]["target"]
    if len(targets) != len(set(targets)) or set(targets) != {entry["name"] for entry in fuzz["bin"]}:
        raise RuntimeError("matrice fuzz divergente dai target Cargo")


def compiled_mysql():
    """Confronta anche i target che Cargo compila, senza eseguire sonde."""
    output = subprocess.run(
        ["cargo", "test", "--locked", "-p", "plenora-db-mysql", "--", "--list"],
        cwd=ROOT, check=True, capture_output=True, encoding="utf-8",
    ).stdout
    listed = live_inventory.listed_tests(output)
    excluded = {"mariadb_evidence::mariadb_driver_evidence", "session_evidence::session_semantics_evidence"}
    expected = set().union(*mysql_inventory.collect().values())
    observed = listed - excluded
    if expected != observed:
        raise RuntimeError(f"Cargo/sorgenti divergenti: {sorted(expected ^ observed)}")


class InventoryRegressions(unittest.TestCase):
    def test_repository(self):
        check()

    def test_probe_addition_and_removal_fail(self):
        for expected in (("raw.a",), ("raw.a", "raw.b", "raw.c"), ("raw.a", "raw.a", "raw.b")):
            with self.assertRaises(RuntimeError):
                check_probes(expected, '"raw.a" "raw.b"', r'"(raw\.[a-z]+)"')

    def test_python_parameter_cases_and_unknown_forms(self):
        import tempfile
        with tempfile.TemporaryDirectory(dir=ROOT / "target") as temporary:
            path = Path(temporary) / "test_sample.py"
            path.write_text('@pytest.mark.parametrize("a", [1, 2])\n'
                            '@pytest.mark.parametrize("b", range(3))\n'
                            'def test_sample(a, b): pass\n', encoding="utf-8")
            self.assertEqual(len(list(python_test_inventory.cases(path))), 6)
            path.write_text('@pytest.mark.parametrize("a", build_cases())\n'
                            'def test_sample(a): pass\n', encoding="utf-8")
            with self.assertRaises(RuntimeError):
                list(python_test_inventory.cases(path))

    def test_integration_target_is_discovered(self):
        import tempfile
        with tempfile.TemporaryDirectory(dir=ROOT / "target") as temporary:
            crate = Path(temporary)
            (crate / "src").mkdir()
            (crate / "tests").mkdir()
            unit = crate / "src/unit.rs"
            live = crate / "src/live_tests.rs"
            unit.write_text("#[test]\nfn unit_case() {}", encoding="utf-8")
            live.write_text("#[test]\n#[ignore]\nfn live_case() {}", encoding="utf-8")
            (crate / "tests/new.rs").write_text("#[test]\nfn regression_added() {}", encoding="utf-8")
            with patch.object(mysql_inventory, "SOURCE_DIR", crate / "src"), patch.object(
                mysql_inventory, "external_test_modules", return_value={unit: "unit", live: "live_tests"}
            ):
                self.assertEqual(mysql_inventory.collect()["unit"], {"unit::unit_case", "regression_added"})
                (crate / "tests/new.rs").unlink()
                self.assertEqual(mysql_inventory.collect()["unit"], {"unit::unit_case"})

    def test_python_fixture_alias_and_parameter_growth(self):
        import tempfile
        with tempfile.TemporaryDirectory(dir=ROOT / "target") as temporary:
            suite = Path(temporary)
            (suite / "_harness.py").write_text("def postgres_dsn_or_skip(): pass", encoding="utf-8")
            test = suite / "test_fixture.py"
            source = ('from ._harness import postgres_dsn_or_skip\n'
                      '@pytest.fixture(name="session")\n'
                      'def fixture(): return postgres_dsn_or_skip()\n'
                      '@pytest.mark.parametrize("n", range(2))\n'
                      'def test_query(session, n): pass\n')
            test.write_text(source, encoding="utf-8")
            with patch.object(python_test_inventory, "SUITE", suite):
                self.assertEqual(python_test_inventory.sdk_counts(), {"postgres": 2})
                test.write_text(source.replace("range(2)", "range(3)"), encoding="utf-8")
                self.assertEqual(python_test_inventory.sdk_counts(), {"postgres": 3})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compiled-mysql", action="store_true")
    args = parser.parse_args()
    if args.compiled_mysql:
        compiled_mysql()
    else:
        unittest.main(argv=[sys.argv[0]])
