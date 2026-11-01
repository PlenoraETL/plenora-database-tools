#!/usr/bin/env python3
"""Censisce i confini ODBC e rifiuta conversioni fuori dal classificatore unico."""
from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOUNDARY = "vendor/odbc-api/src/handles/sql_result.rs"
# Eccezioni chiuse: SQLGetDiagRec non puo leggere se stesso; SQLFreeHandle
# invalida l'handle e accetta esclusivamente SUCCESS (ogni avviso fallisce).
EXCEPTIONS = {
    ("vendor/odbc-api/src/handles/diagnostics.rs", "sql_get_diag_rec"),
    ("vendor/odbc-api/src/handles.rs", "SQLFreeHandle"),
}


# Le eccezioni non autorizzano nuovi rami o nuove chiamate nello stesso file.
EXCEPTION_SOURCES = {'vendor/odbc-api/src/handles/diagnostics.rs': 'b65cb8ed01d39410bab81f593103baac03ec7526ec80ee0452f7bbf321a3a3df', 'vendor/odbc-api/src/handles.rs': '2f348d858f737af4404272e9c58b0b1a05bc0f573140a3760fbce4dc5cdcb9c5'}

def code_only(source: str) -> str:
    """Maschera commenti e stringhe conservando offset e righe del sorgente."""
    return re.sub(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"',
                  lambda m: re.sub(r'[^\n]', ' ', m[0]), source, flags=re.S)


def inspect(name: str, source: str) -> tuple[list[str], list[str]]:
    code = code_only(source)
    errors, inventory = [], []
    if name in EXCEPTION_SOURCES and hashlib.sha256(source.replace("\r\n", "\n").encode()).hexdigest() != EXCEPTION_SOURCES[name]:
        errors.append(f"{name}: eccezione modificata, richiede nuova revisione esplicita")
    def location(offset):
        return f"{name}:{source.count(chr(10), 0, offset) + 1}"
    test = name.endswith("_tests.rs") or "/tests/" in name
    for match in re.finditer(r'\.into_(?:sql_result|result|result_without_logging)\s*\(', code):
        errors.append(f"{location(match.start())}: conversione non strict")
    if not test and name not in (BOUNDARY, "vendor/odbc-api/src/error.rs"):
        for match in re.finditer(r'\bSuccessWithInfo\b|\bSUCCESS_WITH_INFO\b', code):
            # Il lettore dei diagnostici e l'unica eccezione ricorsiva.
            if name != "vendor/odbc-api/src/handles/diagnostics.rs":
                errors.append(f"{location(match.start())}: avviso fuori dal classificatore")
    aliases = set(re.findall(r'\bSQL\w+\s+as\s+(\w+)', code))
    calls = re.compile(r'\b(SQL[A-Z]\w*|sql_\w+' + ''.join('|'+re.escape(a) for a in aliases) + r')\s*\(')
    for match in calls.finditer(code):
        if re.search(r"\bfn\s*$", code[:match.start()]):
            continue
        function = match[1]
        start = match.end()
        depth, end = 1, start
        while depth and end < len(code):
            depth += (code[end] == '(') - (code[end] == ')')
            end += 1
        exception = (name, function) in EXCEPTIONS
        strict = re.match(r'\s*\}?\s*\.into_sql_result_strict\s*\(', code[end:])
        inventory.append(f"{location(match.start())}: {function} {'eccezione' if exception else 'strict'}")
        if not exception and not strict:
            errors.append(f"{location(match.start())}: {function} senza confine strict immediato")
    return errors, inventory


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path)
    args = parser.parse_args()
    errors, inventory = [], []
    for folder in ('vendor/odbc-api/src', 'crates/plenora-db-db2/src'):
        for path in sorted((ROOT / folder).rglob('*.rs')):
            found, calls = inspect(path.relative_to(ROOT).as_posix(), path.read_text(encoding='utf-8'))
            errors.extend(found)
            inventory.extend(calls)
    if args.inventory:
        args.inventory.parent.mkdir(parents=True, exist_ok=True)
        args.inventory.write_text('\n'.join(inventory) + '\n', encoding='utf-8')
    for error in errors:
        print(error)
    print(f"ODBC strict: {len(inventory)} chiamate, {len(errors)} violazioni")
    return int(bool(errors))


if __name__ == '__main__':
    raise SystemExit(main())
