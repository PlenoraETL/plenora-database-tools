"""Generate: python scripts/arrow_public_items.py --write (offline Cargo cache).

CI regenerates and compares after cargo fetch --locked. Scan every source file,
including cfg-disabled modules: a conservative superset of public declarations.
Macro expansions, re-exports and build-generated items have the registry limits.
"""
from pathlib import Path
import json
import os
import re
import sys
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / 'scripts/arrow_public_items.json'


def packages():
    lock = tomllib.loads((ROOT / 'Cargo.lock').read_text(encoding='utf-8'))
    return [{k: p[k] for k in ('name', 'version', 'checksum')} for p in lock['package']
            if p['name'].startswith('arrow') or p['name'] == 'parquet']


def generate():
    from check_arrow_ingress import rust_mask
    cache = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo')) / 'registry/src'
    result = {'packages': packages(), 'items': [], 'types': []}
    types = set()
    for package in result['packages']:
        roots = list(cache.glob(f"*/{package['name']}-{package['version']}"))
        if len(roots) != 1:
            raise ValueError(f"expected one cached source: {package['name']} {package['version']}")
        root = roots[0]
        for path in sorted((root / 'src').rglob('*.rs'), key=lambda path: path.relative_to(root).as_posix()):
            masked = rust_mask(path.read_text(encoding='utf-8'))
            for match in re.finditer(r'\bpub\s+(?:(?:unsafe|async|const|extern)\s+)*(struct|enum|union|trait|type|fn|mod|const|static)\s+(\w+)', masked):
                kind, name = match.groups()
                result['items'].append([package['name'], path.relative_to(root).as_posix(), kind, name])
                if kind in {'struct', 'enum', 'union', 'trait', 'type'}:
                    types.add(name)
    result['types'] = sorted(types)
    return result


def load_catalog():
    result = json.loads(CATALOG.read_text(encoding='utf-8'))
    if result['packages'] != packages():
        raise ValueError('Arrow public item catalog differs from Cargo.lock; regenerate')
    return result


if __name__ == '__main__':
    generated = generate()
    if '--write' in sys.argv:
        CATALOG.write_text(json.dumps(generated, indent=2) + '\n', encoding='utf-8')
    elif generated != load_catalog():
        raise SystemExit('Arrow public item catalog differs from cached locked sources')
    print(f"Arrow catalog: {len(generated['packages'])} packages, {len(generated['items'])} declarations")
