"""Genera vocabolario/schema v3 e tabella dal registro tipizzato Rust."""
from __future__ import annotations
import argparse
import copy
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'crates/plenora-database-core/src/loss_registry.rs'
SCHEMA = ROOT / 'contracts/v2/execution-loss-report-v3.schema.json'
TABLE = ROOT / 'docs/loss-registry.md'

def rules():
    source = SOURCE.read_text(encoding='utf-8-sig')
    body = source.split('rules! {', 1)[1].split('\n}\nimpl RuleId', 1)[0]
    # Whitespace-independent, including cargo fmt's multiline tuples.
    pattern = r'\(\s*(\w+),\s*"([a-z_.]+)",\s*(\w+),\s*(\w+),\s*(\w+),\s*(\w+),\s*(\w+),\s*(None|Some\(RuleId::\w+\)),\s*("(?:[^"\\]|\\.)*")\s*,?\s*\)'
    rows = [m.groups() for m in re.finditer(pattern, body, re.S)]
    if not rows or len(rows) != body.count('"mysql.') + body.count('"pg.') + body.count('"ss.') + body.count('"store.'):
        raise ValueError('registro non analizzabile integralmente')
    if len({r[1] for r in rows}) != len(rows):
        raise ValueError('ID duplicato')
    return rows

def object_schema(properties, required=None):
    return {'type': 'object', 'additionalProperties': False, 'required': list(properties) if required is None else required, 'properties': properties}

def render_schema():
    legacy = json.loads((ROOT / 'contracts/v2/loss-report.schema.json').read_text(encoding='utf-8'))
    risk = copy.deepcopy(legacy['properties']['losses']['items'])
    risk['required'].insert(1, 'rule_id')
    risk['properties']['rule_id'] = {'$ref': '#/$defs/rule'}
    field = {'$ref': 'common.schema.json#/$defs/field_id'}
    rule = {'$ref': '#/$defs/rule'}
    basis = {'$ref': '#/$defs/basis'}
    count = {'$ref': '#/$defs/count'}
    evaluation = object_schema({'field_id': field, 'rule_id': rule, 'basis': basis, 'checked': count, 'would_change': count})
    effective = object_schema({'field_id': field, 'rule_id': rule, 'category': risk['properties']['category'], 'basis': basis, 'committed': count, 'uncertain': count})
    effective['anyOf'] = [{'properties': {name: {'minimum': 1}}} for name in ('committed', 'uncertain')]
    def array(name):
        return {'type': 'array', 'maxItems': 4096, 'items': {'$ref': '#/$defs/' + name}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://plenora.local/database-tools/v2/' + SCHEMA.name}
    schema.update(object_schema({'schema_version': {'const': 3}, 'policy': legacy['properties']['policy'], 'risk': array('risk'), 'evaluation': object_schema({'complete': {'type': 'boolean'}, 'entries': array('evaluation_entry')}), 'effective': array('effective')}))
    schema['$defs'] = {'count': {'type': 'integer', 'minimum': 0, 'maximum': 18446744073709551615}, 'basis': {'enum': ['exact', 'conservative']}, 'rule': {'enum': [r[1] for r in rules()]}, 'risk': risk, 'evaluation_entry': evaluation, 'effective': effective}
    return json.dumps(schema, ensure_ascii=False, indent=2) + '\n'

def render_table():
    lines = ['# Registro delle perdite', '', 'Generato da `loss_registry.rs` con `scripts/render_loss_registry.py`.', '', '| ID | Applicabilità | Categoria | Severità | Predicato | Modalità | Alias PostgreSQL |', '| --- | --- | --- | --- | --- | --- | --- |']
    for row in rules():
        _, wire, provider, category, severity, predicate, mode, alias, _ = row
        lines.append(f'| {wire} | {provider} | {category} | {severity} | {predicate} | {mode} | {alias} |')
    return '\n'.join(lines) + '\n'

def check():
    for path, render in ((SCHEMA, render_schema), (TABLE, render_table)):
        if path.read_text(encoding='utf-8') != render():
            raise ValueError(f'rigenerare {path.name} con scripts/render_loss_registry.py')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.check:
        check()
    else:
        for path, render in ((SCHEMA, render_schema), (TABLE, render_table)):
            path.write_text(render(), encoding='utf-8', newline='\n')

if __name__ == '__main__':
    main()
