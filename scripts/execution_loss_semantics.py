"""Invarianti relazionali v3 non esprimibili dal solo JSON Schema."""
from scripts.render_loss_registry import rules

def validate(document):
    registry = {row[1]: row for row in rules()}
    risks = {}
    potential = 0
    for risk in document['risk']:
        key = risk['field_id'], risk['rule_id']
        if key in risks:
            raise ValueError('rischio duplicato')
        definition = registry[key[1]]
        category = {'NativeType': 'native_type', 'GeometryType': 'geometry_type'}.get(definition[3], definition[3].lower())
        if risk['category'] != category:
            raise ValueError('categoria incoerente')
        risks[key] = risk
        potential += 2 if definition[6] == 'Mixed' else 1
    if potential > 4096:
        raise ValueError('troppi contatori potenziali')
    evaluations = {}
    for entry in document['evaluation']['entries']:
        key = entry['field_id'], entry['rule_id'], entry['basis']
        mode = registry[key[1]][6]
        bases = ['exact', 'conservative'] if mode == 'Mixed' else ['conservative' if mode == 'Conservative' else 'exact']
        if key in evaluations or key[:2] not in risks or entry['would_change'] > entry['checked'] or key[2] not in bases:
            raise ValueError('valutazione incoerente')
        evaluations[key] = entry
    seen = set()
    for entry in document['effective']:
        key = entry['field_id'], entry['rule_id'], entry['basis']
        total = entry['committed'] + entry['uncertain']
        if key in seen or key not in evaluations or total == 0 or total > evaluations[key]['would_change'] or entry['category'] != risks[key[:2]]['category']:
            raise ValueError('perdita effettiva incoerente')
        seen.add(key)
