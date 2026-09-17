from copy import deepcopy
import tempfile
from pathlib import Path
import unittest
import zipfile

from scripts import render_release_sbom as sbom


class ReleaseSbomTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / 'fuzz').mkdir()
        (self.root / 'Cargo.toml').write_text('[workspace.package]\nversion = "5.0.1"\n')
        lock = '''version = 4
[[package]]
name = "app"
version = "5.0.1"
dependencies = ["driver"]
[[package]]
name = "driver"
version = "0.12.3"
dependencies = ["rustls"]
[[package]]
name = "rustls"
version = "0.23.45"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "''' + 'a' * 64 + '''"
'''
        (self.root / 'Cargo.lock').write_text(lock)
        (self.root / 'fuzz/Cargo.lock').write_text(lock)
        (self.root / 'requirements-sdk-tests.txt').write_text('-c requirements-transitive.txt\nPyArrow==25.0.1\n')
        (self.root / 'requirements-transitive.txt').write_text('tomli==2.4.1; python_version < "3.11"\n')
        self.dist = self.root / 'dist'
        self.dist.mkdir()
        self.wheel()
        self.native = {'bomFormat': 'CycloneDX', 'specVersion': '1.7', 'version': 1, 'components': []}

    def wheel(self, requires=''):
        with zipfile.ZipFile(self.dist / 'sdk.whl', 'w') as archive:
            archive.writestr('sdk.dist-info/METADATA', 'Name: plenora-database\nVersion: 5.0.1\n' + requires)

    def test_graph_covers_transitives_patches_python_pins_and_wheel(self):
        result = sbom.render(self.root, self.dist, self.native)
        sbom.validate(self.root, self.dist, result)
        components = {c['name']: c for c in result['components']}
        self.assertEqual(set(components), {'app', 'driver', 'rustls', 'pyarrow', 'tomli', 'sdk.whl'})
        self.assertNotIn('hashes', components['driver'])
        self.assertEqual(components['rustls']['hashes'][0]['content'], 'a' * 64)
        edges = {d['ref']: d['dependsOn'] for d in result['dependencies']}
        self.assertEqual(edges[components['driver']['bom-ref']], [components['rustls']['bom-ref']])

    def test_artifact_only_scan_cannot_pass_coverage(self):
        result = sbom.render(self.root, self.dist, self.native)
        result['components'] = [c for c in result['components'] if not c['bom-ref'].startswith('cargo:')]
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            sbom.validate(self.root, self.dist, result)

    def test_removed_transitive_edge_and_tampered_hash_are_detected(self):
        complete = sbom.render(self.root, self.dist, self.native)
        result = deepcopy(complete)
        next(d for d in result['dependencies'] if d['ref'].startswith('cargo:driver'))['dependsOn'] = []
        with self.assertRaisesRegex(ValueError, 'graph incomplete'):
            sbom.validate(self.root, self.dist, result)
        result = deepcopy(complete)
        next(c for c in result['components'] if c['name'] == 'rustls')['hashes'][0]['content'] = 'b' * 64
        with self.assertRaisesRegex(ValueError, 'stale'):
            sbom.validate(self.root, self.dist, result)

    def test_unresolved_python_runtime_or_qualification_dependency_blocks_release(self):
        self.wheel('Requires-Dist: new-runtime>=1\n')
        with self.assertRaisesRegex(ValueError, 'resolved SBOM graph'):
            sbom.render(self.root, self.dist, self.native)
        self.wheel()
        (self.root / 'requirements-sdk-tests.txt').write_text('new-package>=1\n')
        with self.assertRaisesRegex(ValueError, 'unresolved Python'):
            sbom.render(self.root, self.dist, self.native)

    def test_duplicate_and_dangling_references_are_rejected(self):
        result = sbom.render(self.root, self.dist, self.native)
        result['components'].append(result['components'][0])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            sbom.validate(self.root, self.dist, result)
        self.native['dependencies'] = [{'ref': 'missing', 'dependsOn': []}]
        with self.assertRaisesRegex(ValueError, 'dangling'):
            sbom.render(self.root, self.dist, self.native)


if __name__ == '__main__':
    unittest.main()
