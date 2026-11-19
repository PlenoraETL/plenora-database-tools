#!/usr/bin/env python3
"""Regressioni fail-closed della qualifica e dei fork per repository."""
import copy
from pathlib import Path
import tempfile
import unittest

import manifesto_dipendenze as m


class QualificationTests(unittest.TestCase):
    def test_spdx_alternatives_and_obligations(self):
        allow = {'MIT', 'Apache-2.0', 'Apache-2.0 WITH LLVM-exception'}
        self.assertTrue(m.spdx_allowed('MIT OR GPL-3.0-only', allow))
        self.assertTrue(m.spdx_allowed('(MIT OR GPL-3.0-only) AND Apache-2.0', allow))
        self.assertTrue(m.spdx_allowed('Apache-2.0 WITH LLVM-exception', allow))
        self.assertFalse(m.spdx_allowed('MIT AND GPL-3.0-only', allow))
        self.assertFalse(m.spdx_allowed('LicenseRef-unknown', allow))
        for value in ('', 'MIT OR', '(MIT', 'MIT Apache-2.0', 'MIT WITH'):
            with self.assertRaises(ValueError):
                m.spdx_allowed(value, allow)

    def test_fork_is_scoped_and_digest_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'vendor/driver').mkdir(parents=True)
            manifest = root / 'vendor/driver/Cargo.toml'
            manifest.write_text('[package]\nname="driver"\nversion="1.0.0"\n')
            (root / 'Cargo.toml').write_text('[dependencies]\ndriver={path="vendor/driver"}\n')
            (root / 'Cargo.lock').write_text('[[package]]\nname="driver"\nversion="1.0.0"\n')
            policy = dict(baseline='test', interfaces=[dict(crate='driver', version='1.0.0', source=m.gate.REGISTRY)],
                          references=[dict(crate='driver', line='1', version='1.0.0', source=m.gate.REGISTRY)],
                          preferred={}, forks=[dict(repo='test', crate='driver', source='path:vendor/driver',
                                                  version='1.0.0', manifest_sha256=m.sha(manifest))])
            self.assertFalse(m.gate.collect(policy, {'test': root})[0])
            self.assertTrue(m.gate.collect(policy, {'other': root})[0])
            manifest.write_text(manifest.read_text() + '# drift\n')
            self.assertIn('fork_provenance', {f['rule'] for f in m.gate.collect(policy, {'test': root})[0]})

    def test_lock_coverage_is_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'fuzz').mkdir()
            (root / 'fuzz/Cargo.lock').write_text('[[package]]\nname="unreported"\nversion="1.0.0"\n')
            old = m.ROOT
            try:
                m.ROOT = root
                with self.assertRaisesRegex(ValueError, 'lock completo'):
                    m.qualify({'packages': [], 'resolve': {'nodes': []}}, 'fuzz/Cargo.lock', {'allow': ['MIT']})
            finally:
                m.ROOT = old

    def test_driver_cannot_disappear(self):
        with self.assertRaisesRegex(ValueError, 'inventario driver'):
            m.qualify({'packages': [], 'resolve': {'nodes': []}}, 'Cargo.lock', {'allow': ['MIT']})


if __name__ == '__main__':
    unittest.main()
