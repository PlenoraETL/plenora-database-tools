"""Regressions at each gate's real parse entrypoint, before schema validation."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import (check_runtime_evidence, check_public_contracts,
                     phase0_validate, phase0_harness, phase0_report,
                     render_adoption_manifest, public_contract_semantics,
                     check_coverage, code_size)
from scripts.strict_json import loads


class DuplicateJsonTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "input.json"

    def write(self, text):
        self.path.write_text(text, encoding="utf-8")
        return self.path

    def rejected(self, call):
        with self.assertRaises((ValueError, RuntimeError)) as caught:
            call()
        self.assertIn("chiave JSON ripetuta", str(caught.exception))
        self.assertNotIn("SECRET", str(caught.exception))

    def variants(self, clean):
        # Both values and keys are canaries; escaped-equivalent names count too.
        return [
            '{"SECRET_KEY":"SECRET_VALUE","SECRET_KEY":2,' + clean[1:],
            '{"extra":[{"SECRET_KEY":1,"SECRET_KEY":"SECRET_VALUE"}],' + clean[1:],
        ]

    def test_runtime_discovery_capabilities_results_and_errors(self):
        for name in ("discovery", "capabilities", "result", "error"):
            clean = json.dumps({"kind": name, "items": [{"a": 1}, {"a": 2}]})
            self.assertEqual(check_runtime_evidence.load(self.write(clean)), json.loads(clean))
            for text in self.variants(clean):
                with self.subTest(name=name, text=text):
                    self.rejected(lambda: check_runtime_evidence.load(self.write(text)))

    def test_public_documents_and_general_contract_validator(self):
        for loader in (check_public_contracts.load, phase0_validate.load_json):
            clean = '{"schema_version":4,"nested":{"a":1}}'
            self.assertEqual(loader(self.write(clean)), json.loads(clean))
            for text in self.variants(clean) + ['{"schema_version":3,"schema_version":4}']:
                self.rejected(lambda: loader(self.write(text)))

    def test_cli_envelopes(self):
        clean = '{"status":"ok","result":{"items":[{"a":1},{"a":2}]}}'
        for text in [clean, *self.variants(clean)]:
            result = subprocess.CompletedProcess([], 0, (text + "\n").encode(), b"")
            with patch.object(check_public_contracts.subprocess, "run", return_value=result):
                call = lambda: check_public_contracts.run_cli(Path("unused"), success=True)
                if text == clean:
                    self.assertEqual(call(), json.loads(clean))
                else:
                    self.rejected(call)

    def test_adoption_source_and_schema(self):
        source = render_adoption_manifest.SOURCE.read_text(encoding="utf-8")
        for text in [source, *self.variants(source), source.replace('"schema_version": 4', '"schema_version": 3,"schema_version": 4')]:
            with patch.object(render_adoption_manifest, "SOURCE", self.write(text)):
                if text == source:
                    self.assertEqual(render_adoption_manifest.manifest("1.0", [], ["unit"])["schema_version"], 4)
                else:
                    self.rejected(lambda: render_adoption_manifest.manifest("1.0", [], ["unit"]))
        for text in self.variants(source):
            with patch.object(public_contract_semantics, "SOURCE", self.write(text)):
                self.rejected(lambda: public_contract_semantics.load_semantics(ROOT))
        for text in self.variants('{"type":"object"}'):
            self.rejected(lambda: render_adoption_manifest.validate_manifest({}, self.write(text)))

    def test_campaign_manifest_and_failed_passed_report(self):
        clean = '{"schema_version":1,"cases":[{"id":"a","provider":"postgres","runner":"postgres"}]}'
        self.assertIn("a", phase0_harness.Manifest(self.write(clean)).cases)
        for text in self.variants(clean) + [clean.replace('"schema_version":1', '"schema_version":0,"schema_version":1')]:
            self.rejected(lambda: phase0_harness.Manifest(self.write(text)))
        clean = '{"case_id":"a","status":"passed","metrics":{"wall_ns":1}}'
        self.assertEqual(phase0_report.read_records([self.write(clean)])[0]["status"], "passed")
        for text in self.variants(clean) + [clean.replace('"status":"passed"', '"status":"failed","status":"passed"')]:
            self.rejected(lambda: phase0_report.read_records([self.write(text)]))

    def test_duplicate_budgets_cannot_turn_failure_into_success(self):
        clean = '{"max_physical_lines":10,"max_code_lines":10}'
        report = {"total": {"physical_lines":5, "code_lines":5}}
        self.assertEqual(code_size.check_budget(report, self.write(clean)), [])
        for text in self.variants(clean) + [clean.replace('"max_code_lines":10', '"max_code_lines":0,"max_code_lines":10')]:
            self.rejected(lambda: code_size.check_budget(report, self.write(text)))
        clean = '{"schema_version":1,"surfaces":{"rust":{"minimum_percent":{"lines":80}}}}'
        self.assertEqual(check_coverage.load_json(self.write(clean), "budget"), json.loads(clean))
        for text in self.variants(clean) + [clean.replace('"lines":80', '"lines":100,"lines":0')]:
            self.rejected(lambda: check_coverage.load_json(self.write(text), "budget"))

    def test_escaped_names_arrays_and_redacted_syntax_errors(self):
        self.rejected(lambda: loads(r'{"SECRET_KEY":1,"\u0053ECRET_KEY":2}'))
        self.rejected(lambda: loads('[{"SECRET_KEY":1,"SECRET_KEY":2}]'))
        with self.assertRaisesRegex(ValueError, "^JSON non valido$"):
            loads('{"SECRET_KEY": SECRET_VALUE}')
        self.assertEqual(loads('[{"a":1},{"a":2}]'), [{"a":1},{"a":2}])


class GatePrestazionaliTests(unittest.TestCase):
    def test_i_gate_prestazionali_leggono_tramite_strict_json(self):
        """Manifesti, budget e baseline dei gate prestazionali passano dal
        lettore che rifiuta i duplicati: il rilievo della revisione era
        proprio il json.load nudo in questi tre."""
        import check_mysql_performance
        import check_postgres_performance
        import check_sqlserver_performance
        import strict_json
        for modulo in (check_mysql_performance, check_postgres_performance,
                       check_sqlserver_performance):
            with self.subTest(modulo=modulo.__name__):
                self.assertIs(getattr(modulo, "strict_json"), strict_json)


if __name__ == "__main__":
    unittest.main()

