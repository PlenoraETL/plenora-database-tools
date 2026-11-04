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
            'postgres': [1] * 6, 'mysql': [1] * 5, 'mariadb': [1] * 7,
            'sqlserver': [1] * 4, 'oracle': [1] * 2, 'db2': [1] * 2,
        }.items():
            cases = self.namespace['test_cases'](provider, 'all')
            self.assertEqual(self.namespace['test_cases'](provider, 'nul'),
                             [('live_audit900_nul', 1)])
            self.assertEqual([count for _, count in cases], counts)
            self.assertEqual(cases[0][0], 'live_parameter_values_bind_exactly_or_are_refused')
            crate = 'mysql' if provider == 'mariadb' else provider
            sources = '\n'.join(path.read_text(encoding='utf-8')
                                for path in (ROOT / f'crates/plenora-db-{crate}/src').glob('*.rs'))
            for name, _ in cases:
                self.assertEqual(len(re.findall(r'async fn ' + re.escape(name) + r'\(', sources)), 1)

            if len(cases) > 1:
                self.assertTrue(all(name.startswith('live_audit900_') for name, _ in cases[1:]))

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

    def test_failure_does_not_stop_later_cases(self):
        self.namespace['results'] = []
        calls = []
        def fake_run(command, name, expected):
            calls.append(name)
            if len(calls) == 1:
                raise SystemExit(101)
        self.namespace['run'] = fake_run
        self.namespace['run_tests']([], 'plenora-db-postgres', 'postgres', 'postgres', 'all')
        self.assertEqual(len(calls), 6)
        self.assertEqual([status for _, status in self.namespace['results']],
                         ['FAIL', 'PASS', 'PASS', 'PASS', 'PASS', 'PASS'])
        self.assertTrue(any('l3_' in name for name in calls))
        self.assertTrue(any('l5_' in name for name in calls))

    def test_log_cleanup_is_scoped_to_selected_provider(self):
        tree = ast.parse(BODY)
        cleanup = next(node for node in tree.body if isinstance(node, ast.For))
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory)
            stale = logs / 'postgres-live_audit900_.log'
            other = logs / 'oracle-live_parameter_values_bind_exactly_or_are_refused.log'
            stale.write_text('old failure', encoding='utf-8')
            other.write_text('other provider', encoding='utf-8')
            exec(compile(ast.Module(body=[cleanup], type_ignores=[]), '<cleanup>', 'exec'),
                 dict(logs=logs, selected='postgres'))
            self.assertFalse(stale.exists())
            self.assertTrue(other.exists())

    def test_log_paths_follow_selection(self):
        tree = ast.parse(BODY)
        assignment = next(node for node in tree.body if isinstance(node, ast.Assign)
                          and any(isinstance(t, ast.Name) and t.id == 'logs' for t in node.targets))
        for selection, folder in [('all', 'live'), ('nul', 'live-nul')]:
            namespace = dict(root=ROOT, selection=selection)
            exec(compile(ast.Module(body=[assignment], type_ignores=[]), '<logs>', 'exec'), namespace)
            self.assertEqual(namespace['logs'], ROOT / 'target/audit-900' / folder)

    def test_fixture_scope_and_helpers(self):
        self.assertNotIn('prune', BODY)
        self.assertNotIn("'down'", BODY)
        self.assertIn("'up', '-d', '--force-recreate'", BODY)
        self.assertIn('CARGO_BUILD_JOBS:-4', SCRIPT)
        self.assertIn("'--wait'", BODY)
        for helper in ['postgres.cargo', 'sqlserver.cargo', 'db2.DB2_ENV',
                       'oracle.test_environments', 'fleet.servers',
                       'compose_network_arguments', 'compose_volume', 'container_variable']:
            self.assertIn(helper, BODY)
        self.assertIn('bash "$0" "$provider" "${2:-all}" || failed=1', SCRIPT)
        self.assertIn('--include-ignored', BODY)
        self.assertNotIn('AUDIT900_ENV_DIR', SCRIPT)


if __name__ == '__main__':
    unittest.main()
