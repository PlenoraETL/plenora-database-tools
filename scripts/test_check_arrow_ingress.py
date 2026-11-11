"""Mutation tests for the closed Arrow signature registry (no file edits)."""
import copy
import json
import unittest
import types
import ast
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from check_arrow_ingress import REGISTRY, audit, inventory, python_functions, runtime_python_functions, rust_functions, sources, form_errors, runtime_registry_errors
from check_arrow_ingress import python_review_dump


class ArrowInventoryTests(unittest.TestCase):
    def test_review_dump_preserves_legacy_empty_fields_and_type_parameters(self):
        node = ast.parse('def f(): pass').body[0]
        expected = "FunctionDef(name='f', args=arguments(posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]), body=[Pass()], decorator_list=[])"
        self.assertEqual(python_review_dump(node), expected)
        node._fields = (*[f for f in node._fields if f != 'type_params'], 'type_params')
        node.type_params = []
        self.assertEqual(python_review_dump(node), expected)
        node.type_params = [ast.Name(id='T', ctx=ast.Load())]
        self.assertNotEqual(python_review_dump(node), expected)
        node.body = [ast.Return(value=ast.Constant(value=1))]
        self.assertNotEqual(python_review_dump(node), expected)

    def test_catalog_path_order_is_platform_independent(self):
        import arrow_public_items
        with TemporaryDirectory() as directory:
            root = Path(directory) / 'registry/src/cache/arrow-test-1/src'
            root.mkdir(parents=True)
            for name in ('array.rs', 'ArrayData.rs'):
                (root / name).write_text('pub struct Value;', encoding='utf-8')
            with patch.dict('os.environ', CARGO_HOME=directory), \
                    patch.object(arrow_public_items, 'packages', return_value=[
                        {'name': 'arrow-test', 'version': '1', 'checksum': 'test'}]):
                items = arrow_public_items.generate()['items']
        self.assertEqual([item[1] for item in items], ['src/ArrayData.rs', 'src/array.rs'])

    @classmethod
    def setUpClass(cls):
        cls.sources = sources()
        cls.document = json.loads(REGISTRY.read_text(encoding='utf-8'))
        cls.registry = cls.document['functions']
        cls.found = inventory(cls.sources)

    def test_current_inventory_and_reviewed_chains(self):
        self.assertEqual(audit(self.found, self.registry,
                               self.document['support_sources'], self.sources), [])

    def test_new_python_ingest_is_rejected_in_memory(self):
        for declaration in ['def ingest(data):\n    return data.to_batches()\n',
                            'async def ingest(data):\n    return data.to_batches()\n',
                            'class New:\n    def ingest(self, data):\n        return data.to_batches()\n']:
            changed = dict(self.sources)
            changed['new.py'] = declaration
            self.assertTrue(any('unclassified: new.py::' in e
                                for e in audit(inventory(changed), self.registry)))

    def test_public_trait_default_method_exact_regression(self):
        source = 'pub trait NewSink { fn ingest(&self, data: &arrow_array::RecordBatch) {} }'
        self.assertIn('unclassified: new.rs::ingest#1', audit(inventory({'new.rs': source}), {}))

    def test_implicit_public_methods_declarations_and_implementations(self):
        for source in (
            'pub trait NewSink { fn ingest(&self, data: &arrow_buffer::Buffer); }',
            'pub trait NewSink { fn ingest(&self, data: &arrow_buffer::Buffer); } impl NewSink for Sink { fn ingest(&self, data: &arrow_buffer::Buffer) {} }',
            'impl upstream::NewSink for Sink { fn ingest(&self, data: &arrow_buffer::Buffer) {} }',
            'impl NewSink<{ 1 }> for Sink { fn ingest(&self, data: &arrow_buffer::Buffer) {} }',
        ):
            with self.subTest(source=source):
                errors = audit(inventory({'new.rs': source}), {})
                self.assertIn('unclassified: new.rs::ingest#1', errors)
                if 'pub trait' in source and 'impl NewSink' in source:
                    self.assertIn('unclassified: new.rs::ingest#2', errors)

    def test_aliases_in_public_traits_and_trait_impls(self):
        for source in (
            'use arrow_buffer::Buffer as Values; pub fn ingest(x: Values) {}',
            'pub type Values = arrow_buffer::Buffer;',
            'pub trait Sink { type Y = arrow_buffer::Buffer; }',
            'impl Sink for Concrete { type Y = arrow_buffer::Buffer; }',
        ):
            with self.subTest(source=source):
                self.assertTrue(form_errors({'new.rs': source}, {}))

    def test_arrow_bounds_in_every_position(self):
        for bound in ('arrow_array::RecordBatchReader + Send',
                      'Send + arrow_array::RecordBatchReader',
                      'Send + Sync + arrow_array::RecordBatchReader'):
            for source in (
                f'pub fn ingest<T: {bound}>(data: T) {{}}',
                f'pub fn ingest<T>(data: T) where T: {bound} {{}}',
                f'pub fn ingest(data: impl {bound}) {{}}',
                f'pub trait Sink<T: {bound}> {{ fn ingest(data: T); }}',
                f'impl<T> Sink for Concrete<T> where T: {bound} {{ fn ingest(data: T) {{}} }}',
            ):
                with self.subTest(source=source):
                    self.assertTrue(form_errors({'new.rs': source}, {}))

    def test_catalog_covers_every_recorded_type_without_name_heuristic(self):
        from arrow_public_items import load_catalog
        for name in load_catalog()['types']:
            with self.subTest(name=name):
                source = f'pub fn ingest(data: &{name}) {{}}'
                self.assertIn('new.rs::ingest#1', inventory({'new.rs': source}))
                self.assertTrue(form_errors({'new.rs': f'pub type Alias = {name};'}, {}))
                self.assertTrue(form_errors({'new.rs': f'pub fn ingest<T: Send + {name}>(data: T) {{}}'}, {}))

    def test_catalog_is_generated_from_each_locked_crate_offline(self):
        import os
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        import arrow_public_items as catalog
        packages = [{'name': name, 'version': '1.2.3', 'checksum': 'fixture'}
                    for name in ('arrow-new', 'arrow-schema', 'parquet')]
        with tempfile.TemporaryDirectory(dir=catalog.ROOT / 'target') as directory:
            for package in packages:
                root = Path(directory) / 'registry/src/index' / (package['name'] + '-1.2.3') / 'src'
                root.mkdir(parents=True)
                (root / 'lib.rs').write_text(
                    'pub struct UnusualName; pub trait NovelConstraint {} '
                    'pub type Alias = UnusualName; pub fn exported() {} '
                    '// pub struct CommentOnly;\n', encoding='utf-8')
            with patch.dict(os.environ, {'CARGO_HOME': directory}), patch.object(catalog, 'packages', return_value=packages):
                result = catalog.generate()
                self.assertEqual(result['types'], ['Alias', 'NovelConstraint', 'UnusualName'])
                self.assertEqual({row[0] for row in result['items']}, {p['name'] for p in packages})
                self.assertEqual(len(result['items']), 12)
                with patch.object(catalog, 'packages', return_value=packages + [dict(packages[0], version='9.9.9')]):
                    with self.assertRaises(ValueError):
                        catalog.generate()

    def test_nested_and_higher_ranked_arrow_bounds(self):
        for source in (
            "pub fn ingest(x: impl Iterator<Item: arrow_array::Array>) {}",
            "pub fn ingest<T>(x: T) where Wrapper<{ 1 }>: Send + arrow_array::Array {}",
            "pub fn ingest<T>(x: T) where T: Send + for<'a> arrow_array::Array {}",
            "pub fn ingest<T: Send + ?arrow_array::Array>(x: &T) {}",
        ):
            with self.subTest(source=source):
                self.assertTrue(form_errors({'new.rs': source}, {}))

    def test_catalog_rejects_lock_drift(self):
        from unittest.mock import patch
        import arrow_public_items
        with patch.object(arrow_public_items, 'packages', return_value=[]):
            with self.assertRaises(ValueError):
                arrow_public_items.load_catalog()

    def test_rust_unresolved_forms_fail_closed(self):
        for source in (
            'impl<T: Array> Thing<T> { pub fn ingest(&self, x: &T) {} }',
            'impl<T> Thing<T> where T: arrow_array::Array { pub fn ingest(&self, x: &T) {} }',
            'use arrow_array::Int64Array as Values; pub fn ingest(x: &Values) {}',
            'use arrow_array::{Int64Array as Values}; pub fn ingest(x: &Values) {}',
            'type Values = arrow_array::Int64Array; pub fn ingest(x: &Values) {}',
            'pub fn ingest<T: ArrayAccessor>(x: T) {}',
            'pub fn ingest(x: impl arrow_array::RecordBatchReader) {}',
            'pub use upstream::ingest;',
            'pub fn ingest<t: Array>(x: t) {}',
            'pub fn ingest<T: parquet::file::reader::FileReader>(x: T) {}',
            'use arrow_buffer::Buffer; type Values = Buffer;',
        ):
            with self.subTest(source=source):
                self.assertTrue(form_errors({'new.rs': source}, {}))

    def test_explicit_async_and_impl_methods_are_enumerated(self):
        for source in (
            'pub async fn ingest(x: &arrow_array::RecordBatch) {}',
            'impl Thing { pub fn ingest(&self, x: &arrow_array::Int64Array) {} }',
        ):
            with self.subTest(source=source):
                self.assertEqual(form_errors({'new.rs': source}, {}), [])
                self.assertIn('unclassified: new.rs::ingest#1',
                              audit(inventory({'new.rs': source}), {}))

    def test_forms_ignore_comments_literals_tests_and_vendor(self):
        source = 'use arrow_array::Int64Array as Values;'
        for path in ('vendor/lib.rs', 'crate/tests/ingest.rs', 'crate/src/ingest_tests.rs'):
            self.assertEqual(form_errors({path: source}, {}), [])
        self.assertEqual(form_errors({'new.rs': '// '+source+'\nconst X: &str = "'+source+'";'}, {}), [])

    def test_forms_and_runtime_links_are_reviewed(self):
        self.assertEqual(form_errors(self.sources, self.document['rust_forms']), [])
        self.assertEqual(runtime_registry_errors(self.document, self.found), [])
        document = copy.deepcopy(self.document)
        document['limits'] = {}
        self.assertTrue(runtime_registry_errors(document, self.found))
        document = copy.deepcopy(self.document)
        next(iter(document['python_runtime'].values()))['sources'] = ['missing']
        self.assertTrue(runtime_registry_errors(document, self.found))

    def test_runtime_native_builtins_and_reexports_are_not_filtered(self):
        module = types.ModuleType('native_example')
        module.ingest = len
        module.Reexport = type('Reexport', (), {'ingest': len})
        self.assertEqual(set(runtime_python_functions(module)), {'ingest', 'Reexport.ingest'})

    def test_native_function_without_arrow_names_is_unclassified(self):
        found = inventory({'crates/plenora-database-py/src/new.rs':
                           '#[pyfunction] fn ingest(data: &Bound<PyAny>) {}'})
        self.assertIn('unclassified: crates/plenora-database-py/src/new.rs::ingest#1',
                      audit(found, {}))

    def test_conditional_python_definition_is_enumerated(self):
        found = inventory({'new.py': 'if enabled:\n    async def ingest(data):\n        return data.to_batches()\n'})
        self.assertIn('unclassified: new.py::ingest', audit(found, {}))

    def test_new_rust_batch_entry_is_rejected_in_memory(self):
        changed = dict(self.sources)
        changed['new.rs'] = '''pub fn ingest<'a>(
            prefix: &[u8; 32],
            input: &'a RecordBatch,
        ) -> usize { input.num_rows() }'''
        self.assertIn('unclassified: new.rs::ingest#1', audit(inventory(changed), self.registry))

    def test_inspect_detects_real_new_sync_async_and_class_methods(self):
        module = types.ModuleType('new_sdk_module')
        exec('def ingest(data): pass\nasync def aingest(data): pass\n'
             'class Session:\n    def ingest(self, data): pass\n', module.__dict__)
        self.assertEqual(set(runtime_python_functions(module)), {'ingest', 'aingest', 'Session.ingest'})

    def test_generated_dataclass_constructor_is_classified_too(self):
        found = inventory({'new.py': '@dataclass\nclass New:\n    table: Table\n'})
        self.assertIn('unclassified: new.py::New.__init__', audit(found, self.registry))

    def test_rust_abi_and_const_generic_signature_cannot_hide_arrow(self):
        source = 'pub extern "C" fn ingest<const N: usize = { 1 }>(batch: &RecordBatch) {}'
        self.assertEqual([k for k, _ in rust_functions('new.rs', source)], ['new.rs::ingest#1'])

    def test_comments_strings_and_multiline_signatures(self):
        source = '''// pub fn fake(x: &RecordBatch) {}
        const EXAMPLE: &str = r#"pub fn fake(x: &RecordBatch) {}"#;
        pub async fn ingest(
            input: &RecordBatch,
        ) -> Result<()> { let s = "}"; Ok(()) }
        pub fn output() -> ArrayRef { todo!() }
        '''
        self.assertEqual([k for k, _ in rust_functions('new.rs', source)],
                         ['new.rs::ingest#1', 'new.rs::output#1'])

    def test_python_inspection_never_executes_annotations_or_bodies(self):
        source = '''def ingest(table: "pyarrow.Table" = dangerous()):
            raise RuntimeError("must not execute")
        '''
        found = dict(python_functions('new.py', source))
        self.assertIn('new.py::ingest', found)

    def test_all_guard_removals_fail_even_if_name_survives_in_comment(self):
        for key, rule in self.registry.items():
            if rule['classification'] != 'ingresso guardato':
                continue
            with self.subTest(key=key):
                changed = copy.deepcopy(self.found)
                changed[key]['body'] = ''
                self.assertTrue(audit(changed, self.registry))
                changed[key]['body'] = self.found[key]['body']
                changed[key]['review_sha256'] = 'changed control flow'
                self.assertTrue(audit(changed, self.registry))

    def test_non_ingress_cannot_silently_start_consuming_data(self):
        key = next(k for k, v in self.registry.items()
                   if v['classification'] == 'non riceve dati Arrow esterni')
        changed = copy.deepcopy(self.found)
        changed[key]['review_sha256'] = 'new data consumer'
        self.assertTrue(audit(changed, self.registry))

    def test_helper_precondition_must_be_in_public_documentation(self):
        for key, rule in self.registry.items():
            if rule['classification'] == 'helper con precondizione dichiarata':
                changed = copy.deepcopy(self.found)
                changed[key]['docs'] = ''
                self.assertTrue(audit(changed, self.registry))

    def test_missing_stale_and_invalid_classification(self):
        registry = copy.deepcopy(self.registry)
        key = next(iter(registry))
        registry[key]['classification'] = 'not_measured'
        self.assertTrue(audit(self.found, registry))
        self.assertTrue(audit({}, self.registry))
        self.assertTrue(audit(self.found, {}))

    def test_private_provider_runtime_and_helper_dependencies_are_pinned(self):
        for path in self.document['support_sources']:
            with self.subTest(path=path):
                changed = dict(self.sources)
                changed[path] += '\n// changed chain\n'
                self.assertTrue(audit(self.found, self.registry,
                                      self.document['support_sources'], changed))


if __name__ == '__main__':
    unittest.main()
