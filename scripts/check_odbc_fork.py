#!/usr/bin/env python3
"""Verifica l'intero fork ODBC contro 29.0.0 e il solo delta dichiarato.

Le impronte normalizzano CRLF nei testi UTF-8, mai nei binari. La baseline contiene tutti i
file del pacchetto di registro: anche aggiunte e rimozioni sono controllate.
Se presente, la cache Cargo deve coincidere con la baseline versionata.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "assurance/registries/odbc-api-fork.json"


def fingerprint(path: Path) -> str:
    content = path.read_bytes()
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        pass
    else:
        if b"\0" not in content:
            content = content.replace(b"\r\n", b"\n")
    return hashlib.sha256(content).hexdigest()


def inventory(directory: Path) -> dict[str, str]:
    return {
        path.relative_to(directory).as_posix(): fingerprint(path)
        for path in directory.rglob("*") if path.is_file()
    }


def differences(actual: dict, expected: dict) -> list[str]:
    return sorted(name for name in actual.keys() | expected.keys()
                  if actual.get(name) != expected.get(name))


def verify(vendor: Path, manifest: dict, upstream: Path | None = None) -> list[str]:
    baseline = manifest["upstream_files"]
    errors = []
    if upstream is not None:
        errors += [f"upstream diverso: {name}"
                   for name in differences(inventory(upstream), baseline)]
    expected = dict(baseline)
    for name, change in manifest["changes"].items():
        if not change["reason"] or name not in baseline:
            errors.append(f"delta non dichiarato correttamente: {name}")
        if change["sha256"] is None:
            expected.pop(name, None)
        else:
            expected[name] = change["sha256"]
    errors += [f"fork diverso dal delta dichiarato: {name}"
               for name in differences(inventory(vendor), expected)]
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--upstream", type=Path)
    parser.add_argument("--require-upstream", action="store_true")
    args = parser.parse_args()
    sources = [args.upstream] if args.upstream else sorted(
        (Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "registry/src")
        .glob("*/odbc-api-29.0.0")
    )
    if args.require_upstream and not sources:
        print("ODBC: sorgente upstream 29.0.0 assente dalla cache")
        return 1
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    errors = []
    for source in sources or [None]:
        errors.extend(verify(ROOT / "vendor/odbc-api", manifest, source))
    for error in errors:
        print(error)
    print("ODBC: " + ("FAIL" if errors else "delta esatto rispetto alla baseline 29.0.0")
          + ("; cache verificata" if sources else "; cache non disponibile"))
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
