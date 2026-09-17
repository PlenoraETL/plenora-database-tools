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

    def test_check_rejects_broken_aggregate_and_unknown_edges(self):
        complete = sbom.render(self.root, self.dist, self.native)
        for mutation in ('missing-root', 'missing-root-edge', 'unknown-source', 'unknown-target', 'duplicate-node', 'duplicate-edge'):
            with self.subTest(mutation=mutation):
                result = deepcopy(complete)
                aggregate = next(d for d in result['dependencies'] if d['ref'].startswith('plenora-release:'))
                if mutation == 'missing-root':
                    result['dependencies'].remove(aggregate)
                elif mutation == 'missing-root-edge':
                    aggregate['dependsOn'].pop()
                elif mutation == 'unknown-source':
                    result['dependencies'].append({'ref': 'missing', 'dependsOn': []})
                elif mutation == 'unknown-target':
                    aggregate['dependsOn'].append('missing')
                elif mutation == 'duplicate-node':
                    result['dependencies'].append(deepcopy(result['dependencies'][0]))
                else:
                    aggregate['dependsOn'].append(aggregate['dependsOn'][0])
                with self.assertRaises(ValueError):
                    sbom.validate(self.root, self.dist, result)

    def test_check_rejects_invalid_document_identity(self):
        complete = sbom.render(self.root, self.dist, self.native)
        for field, value in (('name', 'other'), ('bom-ref', 'other'), ('type', 'library')):
            with self.subTest(field=field):
                result = deepcopy(complete)
                result['metadata']['component'][field] = value
                with self.assertRaises(ValueError):
                    sbom.validate(self.root, self.dist, result)
        for field, value in (('specVersion', None), ('specVersion', 'unknown'), ('version', 0)):
            with self.subTest(field=field, value=value):
                result = deepcopy(complete)
                result[field] = value
                with self.assertRaises(ValueError):
                    sbom.validate(self.root, self.dist, result)

    def test_generation_rejects_duplicates_before_merging(self):
        for kind in ('components', 'dependencies'):
            with self.subTest(kind=kind):
                native = deepcopy(self.native)
                native['components'] = [{'bom-ref': 'native', 'type': 'library', 'name': 'native'}]
                native['dependencies'] = [{'ref': 'native', 'dependsOn': []}]
                native[kind].append(deepcopy(native[kind][0]))
                with self.assertRaisesRegex(ValueError, 'duplicate'):
                    sbom.render(self.root, self.dist, native)

    def test_native_graph_is_preserved_and_root_cannot_be_shadowed(self):
        native = deepcopy(self.native)
        native['components'] = [
            {'bom-ref': name, 'type': 'library', 'name': name}
            for name in ('native-a', 'native-b')
        ]
        native['dependencies'] = [{'ref': 'native-a', 'dependsOn': ['native-b']}]
        result = sbom.render(self.root, self.dist, native)
        sbom.validate(self.root, self.dist, result)
        self.assertIn(native['dependencies'][0], result['dependencies'])
        native['dependencies'][0]['dependsOn'].append('native-b')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            sbom.render(self.root, self.dist, native)
        collision = {'bom-ref': 'plenora-release:5.0.1', 'type': 'library', 'name': 'collision'}
        self.native['components'] = [collision]
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            sbom.render(self.root, self.dist, self.native)
        result['components'].append(collision)
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            sbom.validate(self.root, self.dist, result)

    def test_requirement_includes_are_recursive_relative_and_deduplicated(self):
        directory = self.root / 'pins'
        directory.mkdir()
        (directory / 'nested.txt').write_text('nested-package==1.2.3\n-c../requirements-transitive.txt\n')
        (directory / 'constraints.txt').write_text('constraint-package==4.5.6\n')
        path = self.root / 'requirements-sdk-tests.txt'
        for directive in ('-r pins/nested.txt', '-rpins/nested.txt', '--requirement=pins/nested.txt', '--requirement "pins/nested.txt"'):
            with self.subTest(directive=directive):
                path.write_text(directive + '\n--constraint pins/constraints.txt\n')
                inventory = sbom.python_inventory(self.root)
                self.assertEqual({c['name'] for c in inventory.values()}, {'nested-package', 'constraint-package', 'tomli'})
                nested = inventory['pkg:pypi/nested-package@1.2.3']
                self.assertIn(sbom.property_value('requirement', 'pins/nested.txt: nested-package==1.2.3'), nested['properties'])
                tomli = inventory['pkg:pypi/tomli@2.4.1']
                self.assertEqual(sum(p['name'] == 'plenora:requirement' for p in tomli['properties']), 1)

    def test_invalid_requirement_includes_fail_closed(self):
        path = self.root / 'requirements-sdk-tests.txt'
        for include in ('missing.txt', 'requirements-sdk-tests.txt', '../outside.txt', 'https://example.invalid/pins.txt'):
            with self.subTest(include=include):
                path.write_text('-r ' + include + '\n')
                with self.assertRaises(ValueError):
                    sbom.python_inventory(self.root)
        nested = self.root / 'nested.txt'
        path.write_text('-r nested.txt\n')
        nested.write_text('unpinned>=1\n')
        with self.assertRaisesRegex(ValueError, 'unresolved Python'):
            sbom.python_inventory(self.root)
        nested.write_text('-r requirements-sdk-tests.txt\n')
        with self.assertRaises(ValueError):
            sbom.python_inventory(self.root)


if __name__ == '__main__':
    unittest.main()
