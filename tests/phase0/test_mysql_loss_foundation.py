"""La major segue il vocabolario chiuso; le attese 10.0.0 restano immutate."""
import copy
import unittest
from scripts import execution_loss_semantics, phase0_validate as gate, render_loss_registry

class MysqlLossFoundationTests(unittest.TestCase):
    def test_closed_previous_reader_rejects_new_rules_and_new_reader_accepts(self):
        schemas = gate.discover_schemas(gate.REPO_ROOT / 'contracts')
        registry = gate.build_registry(schemas.values())
        schema = schemas[render_loss_registry.SCHEMA.resolve()]
        baseline = gate.load_json(gate.REPO_ROOT / 'tests/fixtures/mysql_permissive/core_baseline.json')
        previous = copy.deepcopy(schema)
        previous['$defs']['rule']['enum'] = sorted({row['id'] for row in baseline})
        old_reader = gate.Draft202012Validator(previous, registry=registry)
        new_reader = gate.Draft202012Validator(schema, registry=registry)
        rules = [r for r in render_loss_registry.rules() if r[2] == 'Mysql']
        self.assertEqual(len(rules), 5)
        for rule in rules:
            with self.subTest(rule=rule[1]):
                category = {'NativeType': 'native_type'}.get(rule[3], rule[3].lower())
                document = {'schema_version': 3, 'policy': 'compatible',
                    'risk': [{'field_id': 0, 'rule_id': rule[1], 'category': category,
                              'severity': 'data_loss', 'reason': 'rischio pianificato'}],
                    'evaluation': {'complete': False, 'entries': []}, 'effective': []}
                self.assertFalse(old_reader.is_valid(document))
                new_reader.validate(document)
                execution_loss_semantics.validate(document)
