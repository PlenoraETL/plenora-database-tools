#!/usr/bin/env python3
"""Genera e verifica la qualifica completa: lock + cargo metadata --all-features.

La baseline di famiglia resta immutabile. --write e una proposta revisionabile,
mai un passo CI; il default confronta gli artefatti senza aggiornarli.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
HOME = ROOT / '.ci/plenora-dependencies'
sys.path.insert(0, str(HOME))
import controlla_dipendenze as gate

REPO = 'plenora-database-tools'
BASELINE_SHA = '09f47749e59fdf518e49386975b2f58fb8a61b9ade91da7db3136ab5f3bc78d0'
DRIVERS = {'postgres': 'plenora-db-postgres', 'sqlserver': 'plenora-db-sqlserver',
           'mysql': 'plenora-db-mysql', 'mariadb': 'plenora-db-mysql',
           'oracle': 'plenora-db-oracle', 'db2': 'plenora-db-db2'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def spdx_allowed(expression, allowed):
    """Fail closed; AND precede OR, WITH mantiene l'eccezione nell'identita."""
    tokens = re.findall(r'\(|\)|[^\s()]+', expression.replace('/', ' OR '))
    pos = 0

    def atom():
        nonlocal pos
        if pos >= len(tokens):
            raise ValueError('SPDX incompleto')
        token = tokens[pos]
        pos += 1
        if token == '(':
            value = disjunction()
            if pos >= len(tokens) or tokens[pos] != ')':
                raise ValueError('SPDX parentesi')
            pos += 1
            return value
        if token in ('AND', 'OR', ')', 'WITH'):
            raise ValueError('SPDX operatore')
        if pos < len(tokens) and tokens[pos] == 'WITH':
            pos += 1
            if pos >= len(tokens):
                raise ValueError('SPDX eccezione')
            token += ' WITH ' + tokens[pos]
            pos += 1
        return token in allowed

    def conjunction():
        nonlocal pos
        value = atom()
        while pos < len(tokens) and tokens[pos] == 'AND':
            pos += 1
            right = atom()
            value = value and right
        return value

    def disjunction():
        nonlocal pos
        value = conjunction()
        while pos < len(tokens) and tokens[pos] == 'OR':
            pos += 1
            right = conjunction()
            value = value or right
        return value

    value = disjunction()
    if pos != len(tokens):
        raise ValueError('SPDX non riconosciuto')
    return value


def toml_value(value):
    if isinstance(value, dict):
        return '{ ' + ', '.join(json.dumps(k) + ' = ' + toml_value(v) for k, v in value.items()) + ' }'
    if isinstance(value, list):
        return '[' + ', '.join(toml_value(v) for v in value) + ']'
    return json.dumps(value, ensure_ascii=False)


def render(policy):
    lines = ['# Generato da scripts/manifesto_dipendenze.py --write; revisionare il diff.']
    for key, value in policy.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            for item in value:
                lines += ['', '[[' + key + ']]']
                lines += [k + ' = ' + toml_value(v) for k, v in item.items()]
        else:
            # Dotted keys impediscono che scalari successivi entrino nell'ultima tabella.
            # Tutte le sezioni scalari precedono gli array di tabelle nella baseline.
            lines.append(key + ' = ' + toml_value(value))
    return '\n'.join(lines) + '\n'


def metadata(manifest):
    result = subprocess.run(['cargo', 'metadata', '--locked', '--all-features',
                             '--format-version', '1', '--manifest-path', str(ROOT / manifest)],
                            capture_output=True)
    if result.returncode:
        raise ValueError('cargo metadata fallito: ' + manifest)
    return json.loads(result.stdout)


def qualify(meta, lock, deny):
    nodes = {n['id']: n for n in meta['resolve']['nodes']}
    packages = {p['id']: p for p in meta['packages']}
    scopes = {}
    if lock == 'Cargo.lock':
        actual = {p['name'] for p in packages.values() if p['name'].startswith('plenora-db-')}
        if actual != set(DRIVERS.values()):
            raise ValueError('inventario driver non corrispondente al workspace')
    for driver, name in DRIVERS.items():
        starts = [p['id'] for p in packages.values() if p['name'] == name]
        if lock == 'Cargo.lock' and not starts:
            raise ValueError('driver assente: ' + driver)
        seen, todo = set(), starts
        while todo:
            ident = todo.pop()
            if ident in seen:
                continue
            seen.add(ident)
            todo.extend(nodes[ident]['dependencies'])
        scopes[driver] = seen
    locked = tomllib.loads((ROOT / lock).read_text(encoding='utf-8'))['package']
    identities = {(p['name'], p['version'], p.get('source') or 'path') for p in packages.values()}
    if identities != {(p['name'], p['version'], p.get('source', 'path')) for p in locked}:
        raise ValueError('metadata non copre il lock completo: ' + lock)
    rows = []
    own = tomllib.loads((ROOT / 'Cargo.toml').read_text(encoding='utf-8'))['workspace']['members']
    for ident, p in sorted(packages.items(), key=lambda pair: (pair[1]['name'], pair[1]['version'])):
        if ident in meta['workspace_members'] or (not p['source'] and
                Path(p['manifest_path']).parent.relative_to(ROOT).as_posix() in own):
            continue
        source = p['source'] or 'path:' + Path(p['manifest_path']).parent.relative_to(ROOT).as_posix()
        if p['source'] and p['source'] != gate.REGISTRY:
            raise ValueError('provenienza esterna non ammessa')
        expression = p['license'] or ''
        clarify = next((c for c in deny.get('clarify', []) if c['crate'] == p['name']), None)
        if clarify:
            expression = clarify['expression']
        allowed = set(deny['allow'])
        for exception in deny.get('exceptions', []):
            if exception['crate'] == p['name']:
                allowed.update(exception['allow'])
        drivers = sorted(d for d, ids in scopes.items() if ident in ids)
        parents = []
        for parent_id, node in nodes.items():
            for edge in node['deps']:
                if edge['pkg'] == ident:
                    parent = packages[parent_id]
                    requirements = sorted({d['req'] for d in parent['dependencies'] if d['name'] == p['name']})
                    parents.append(parent['name'] + '@' + parent['version'] + ': ' + ', '.join(requirements))
        lp = next(x for x in locked if (x['name'], x['version'], x.get('source', 'path')) ==
                  (p['name'], p['version'], p['source'] or 'path'))
        rows.append(dict(repo=REPO, file=lock, crate=p['name'], version=p['version'], source=source,
                         checksum=lp.get('checksum', ''), license_declared=p['license'] or '',
                         license=expression, features=sorted(nodes[ident]['features']),
                         license_allowed=bool(expression) and spdx_allowed(expression, allowed),
                         drivers=drivers, parents=sorted(parents),
                         reason=('Supporto driver ' + ', '.join(drivers) if drivers else
                                 'Supporto workspace/test/fuzz') + '; archi Cargo qualificati in parents'))
    return rows


def generate():
    baseline = HOME / 'family-baseline.toml'
    if sha(baseline) != BASELINE_SHA:
        raise ValueError('baseline famiglia modificata')
    policy = tomllib.loads(baseline.read_text(encoding='utf-8'))
    deny = tomllib.loads((ROOT / 'deny.toml').read_text(encoding='utf-8'))['licenses']
    policy['baseline'] += '-database-tutti-driver-2026-11-19'
    policy['decision_date'] = '2026-11-19'
    # Inseriti prima degli array di tabelle per mantenere lo scope TOML radice.
    policy = {'qualification_policy': {'license_policy': 'deny.toml', 'sha256': sha(ROOT / 'deny.toml'),
                                      'features': 'workspace --all-features; fuzz --all-features'}, **policy}
    qualifications = []
    for manifest, lock in [('Cargo.toml', 'Cargo.lock'), ('fuzz/Cargo.toml', 'fuzz/Cargo.lock')]:
        qualifications.extend(qualify(metadata(manifest), lock, deny))
    # Non si sovrascrivono versioni di famiglia: solo nuove linee.
    refs = {(r['crate'], r['line']) for r in policy['references']}
    for q in qualifications:
        if q['source'].startswith('path:'):
            path = q['source'].removeprefix('path:')
            known = next((f for f in policy.get('forks', []) if f['repo'] == REPO and f['crate'] == q['crate']), None)
            if known:
                if known['manifest_sha256'] != sha(ROOT / path / 'Cargo.toml'):
                    raise ValueError('fork esistente modificato: ' + q['crate'])
            else:
                if path not in ('vendor/mysql_async', 'vendor/mysql_common'):
                    raise ValueError('fork non deliberato')
                policy['forks'].append(dict(repo=REPO, crate=q['crate'], version=q['version'], source=q['source'],
                    manifest_sha256=sha(ROOT / path / 'Cargo.toml'),
                    provenance='Fork gia presente nel checkout database 33724c15; qualifica tutti driver 2026-11-19',
                    removal='Release upstream equivalente e riqualifica MySQL/MariaDB'))
        else:
            key = q['crate'], gate.line(q['version'])
            if key not in refs:
                policy['references'].append(dict(crate=q['crate'], line=key[1], version=q['version'],
                                                 source=q['source'], checksum=q['checksum']))
                refs.add(key)
    raw, _ = gate.collect(policy, {REPO: ROOT})
    raw_keys = {f['key'] for f in raw}
    # Trasporto solo delle deroghe gia deliberate, lungo archi espliciti cambiati.
    exceptions = []
    for old in policy['exceptions']:
        if old['repo'] != REPO or old['key'] in raw_keys:
            exceptions.append(old)
            continue
        updated = copy.deepcopy(old)
        if old.get('parent') == 'mysql_common@0.37.3':
            updated['parent'], updated['parent_source'] = 'mysql_common', 'path'
        elif old.get('parent') == 'zerocopy-derive@0.8.57':
            updated['parent'] = 'zerocopy-derive@0.8.59'
        else:
            # Deroga cessata: non se ne crea una sostitutiva.
            continue
        updated['key'] = gate.fingerprint(updated)
        updated['id'] = 'E-' + updated['key']
        updated['chain'] = updated['parent'] + ' -> ' + updated['crate'] + '@' + updated['version']
        updated['reason'] += ' Trasporto puntuale del genitore qualificato il 2026-11-19.'
        if updated['key'] in raw_keys:
            exceptions.append(updated)
    policy['exceptions'] = exceptions
    policy['alignments'] = [a for a in policy['alignments'] if a['repo'] != REPO or a['key'] in raw_keys]
    policy['qualifications'] = qualifications
    return policy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    os.environ.update(CARGO_BUILD_JOBS='4', CARGO_INCREMENTAL='0')
    try:
        policy = generate()
        content = render(policy)
        # Controllo del serializzatore e dello scope delle sezioni.
        if tomllib.loads(content) != policy:
            raise ValueError('serializzazione incoerente')
        path = HOME / 'plenora-dependencies.toml'
        if args.write:
            path.write_text(content, encoding='utf-8', newline='\n')
            (HOME / 'plenora-dependencies.sha256').write_text(sha(path) + '  ' + path.name + '\n', encoding='ascii', newline='\n')
            files = ['plenora-dependencies.toml', 'plenora-dependencies.sha256',
                     'controlla_dipendenze.py', 'family-baseline.toml']
            (HOME / 'snapshot.sha256').write_text(
                ''.join(sha(HOME / f) + '  ' + f + '\n' for f in files),
                encoding='ascii', newline='\n')
        elif path.read_text(encoding='utf-8') != content:
            raise ValueError('qualifica obsoleta: rigenerare e revisionare il diff')
        denied = [q for q in policy['qualifications'] if not q['license_allowed']]
        report = gate.evaluate(policy, {REPO: ROOT})
        print('qualifiche:', len(policy['qualifications']), 'licenze da decidere:', len(denied),
              'gate:', report['counts'])
        for q in denied:
            print('LICENZA DA DECIDERE:', q['crate'], q['version'], q['license'])
        return int(bool(denied or report['counts']['errors']))
    except (OSError, ValueError, KeyError, TypeError) as error:
        # Solo errori applicativi controllati, senza stderr Cargo o payload esterni.
        print('Qualifica non verificabile:', str(error) if isinstance(error, ValueError) else type(error).__name__)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
