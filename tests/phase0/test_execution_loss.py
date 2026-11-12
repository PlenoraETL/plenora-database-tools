"""Gli stessi vettori vengono eseguiti dal reader Rust e dal gate contratti Python."""
import unittest
from scripts import phase0_validate as gate
from scripts import execution_loss_semantics, render_loss_registry

class ExecutionLossTests(unittest.TestCase):
    def test_generated_registry_schema_and_table(self):
        render_loss_registry.check()

    def test_shared_vectors(self):
        schemas = gate.discover_schemas(gate.REPO_ROOT / 'contracts')
        registry = gate.build_registry(schemas.values())
        schema = schemas[render_loss_registry.SCHEMA.resolve()]
        validator = gate.Draft202012Validator(schema, registry=registry)
        vectors = gate.load_json(gate.REPO_ROOT / 'crates/plenora-database-core/tests/execution_loss_validation.json')
        for case in vectors:
            with self.subTest(name=case['name']):
                structural = validator.is_valid(case['document'])
                self.assertEqual(structural, case['structural'])
                valid = structural
                if valid:
                    try:
                        execution_loss_semantics.validate(case['document'])
                    except ValueError:
                        valid = False
                self.assertEqual(valid, case['valid'])

    def test_collection_and_potential_counter_limits(self):
        import copy
        schemas = gate.discover_schemas(gate.REPO_ROOT / 'contracts')
        registry = gate.build_registry(schemas.values())
        validator = gate.Draft202012Validator(schemas[render_loss_registry.SCHEMA.resolve()], registry=registry)
        base = gate.load_json(gate.REPO_ROOT / 'contracts/v2/examples/execution-loss-v3.json')
        base['evaluation'] = {'complete': False, 'entries': []}
        base['effective'] = []
        risk = base['risk'][0]
        for count in (4096, 4097):
            doc = copy.deepcopy(base)
            doc['risk'] = [dict(risk, field_id=field) for field in range(count)]
            self.assertEqual(validator.is_valid(doc), count == 4096)
        for count in (2048, 2049):
            doc = copy.deepcopy(base)
            doc['risk'] = [dict(risk, field_id=field, rule_id='pg.numeric') for field in range(count)]
            validator.validate(doc)
            if count == 2048:
                execution_loss_semantics.validate(doc)
            else:
                with self.assertRaises(ValueError):
                    execution_loss_semantics.validate(doc)

    def test_all_migrated_writer_paths_share_ledger_finalization(self):
        # Eseguito dal job public-contract e da unittest discover in rust-ci.
        paths = {
            'plenora-db-sqlserver/src/write/mod.rs': (
                'execute_prepared_batch(', 'execute_bulk_batch(',
                'finalize_transaction_target(', 'diagnose_row_scoped_write('),
            'plenora-db-postgres/src/write.rs': (
                'copy_batch(', 'copy_binary_batch(', 'validate_prepared_bindings(',
                'row_diagnostics::execute(',),
        }
        for source, routes in paths.items():
            text = (gate.REPO_ROOT / 'crates' / source).read_text(encoding='utf-8')
            with self.subTest(source=source):
                self.assertEqual(text.count('.finalize(&result, evaluation.confirmed)?'), 1)
                self.assertNotIn('evaluation.ledger.uncertain()', text)
                self.assertNotIn('evaluation.ledger.rollback()', text)
                for route in routes:
                    self.assertIn(route, text)
