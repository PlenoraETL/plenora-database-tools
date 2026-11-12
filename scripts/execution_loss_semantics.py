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


def validate_outcome(document):
    """Parte semantica del contratto WriteOutcome v3, dopo JSON Schema."""
    rows = document['rows']
    status = document['status']
    if status in ('rolled_back', 'outcome_unknown') and rows['confirmed'] != 0:
        raise ValueError('stato incompatibile con righe confermate')
    if rows['confirmed'] + rows['failed'] + rows['skipped'] > rows['received']:
        raise ValueError('conteggi oltre righe ricevute')
    if sum(rows.get(key) or 0 for key in ('inserted', 'updated', 'deleted')) > rows['confirmed']:
        raise ValueError('mutazioni oltre righe confermate')
    if status == 'outcome_unknown' and document['recovery']['automatic_retry_allowed']:
        raise ValueError('esito ignoto richiede recovery')
    report = document['loss_report']
    if report['schema_version'] != 3:
        return
    validate(report)
    committed = {}
    for entry in report['effective']:
        if status == 'rolled_back' and (entry['committed'] or entry['uncertain']):
            raise ValueError('rollback con perdite effettive')
        if status == 'committed' and entry['uncertain']:
            raise ValueError('commit con perdite incerte')
        key = entry['field_id'], entry['rule_id']
        committed[key] = committed.get(key, 0) + entry['committed']
        if committed[key] > rows['confirmed']:
            raise ValueError('perdite oltre righe confermate')
