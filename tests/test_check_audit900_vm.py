"""Guardie offline del runner VM, raccolte dalla discovery di rust-ci."""
import ast
import contextlib
import io
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / 'scripts/check_audit900_vm.sh').read_text(encoding='utf-8')
BODY = SCRIPT.split("<<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]


class Audit900VmTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(BODY)
        functions = ast.Module(
            body=[node for node in tree.body if isinstance(node, ast.FunctionDef)],
            type_ignores=[],
        )
        self.namespace = dict(os=os, re=re, subprocess=subprocess)
        exec(compile(functions, '<audit900>', 'exec'), self.namespace)

    def test_provider_counts_and_filters(self):
        for provider, counts in {
            'postgres': [1, 3], 'mysql': [1, 2], 'mariadb': [1, 2],
            'sqlserver': [1], 'oracle': [1], 'db2': [1],
        }.items():
            self.namespace['selected'] = provider
            cases = self.namespace['test_cases']()
            self.assertEqual([count for _, count in cases], counts)
            self.assertEqual(cases[0][0], 'live_parameter_values_bind_exactly_or_are_refused')
            if len(cases) > 1:
                self.assertEqual(cases[1][0], 'live_audit900_')

    def test_exact_inventory_rejects_empty_wrong_or_duplicate_summary(self):
        good = 'test result: ok. 1 passed; 0 failed; 0 ignored; 7 filtered out\n'
        with tempfile.TemporaryDirectory() as directory:
            self.namespace['logs'] = Path(directory)
            for output, valid in [(good, True), ('', False),
                                  (good.replace('1 passed', '0 passed'), False),
                                  (good + good, False)]:
                def fake_run(command, **kwargs):
                    kwargs['stdout'].write(output)
                    return subprocess.CompletedProcess(command, 0)
                stdout = io.StringIO()
                with patch.object(subprocess, 'run', side_effect=fake_run), contextlib.redirect_stdout(stdout):
                    if valid:
                        self.namespace['run'](['secret-credential'], 'fixture', 1)
                    else:
                        with self.assertRaises(SystemExit):
                            self.namespace['run'](['secret-credential'], 'fixture', 1)
                self.assertNotIn('secret-credential', stdout.getvalue())

    def test_fixture_scope_and_helpers(self):
        self.assertNotIn('prune', BODY)
        self.assertEqual(BODY.count("'down', '--volumes'"), 1)
        self.assertIn("'docker-compose.mariadb.yml', 'down', '--volumes'", BODY)
        self.assertIn("'--wait'", BODY)
        self.assertIn('target/audit-900/live', BODY)
        for helper in ['postgres.cargo', 'sqlserver.cargo', 'db2.DB2_ENV',
                       'oracle.test_environments', 'fleet.servers',
                       'compose_network_arguments', 'compose_volume', 'container_variable']:
            self.assertIn(helper, BODY)
        self.assertIn('bash "$0" "$provider" || failed=1', SCRIPT)
        self.assertIn('--include-ignored', BODY)
        self.assertNotIn('AUDIT900_ENV_DIR', SCRIPT)


if __name__ == '__main__':
    unittest.main()
