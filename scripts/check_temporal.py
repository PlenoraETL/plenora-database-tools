#!/usr/bin/env python3
"""Blocca nuove conversioni temporali infallibili nel codice di prodotto.

Riusa il riconoscimento dei moduli test e lo stripping lessicale del repo.
Le eccezioni sono espressioni esatte con cardinalita, mai interi file.
"""
from collections import Counter
from pathlib import Path
import json
import hashlib
import re
import sys
try:
    from scripts.code_size import rust_test_only_files
    from scripts.live_inventory import strip_noncode
except ModuleNotFoundError:
    from code_size import rust_test_only_files
    from live_inventory import strip_noncode

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "scripts/temporal_reviewed.json"
PATTERN = re.compile(
    r'\b(?:Duration|TimeDelta)::(?:seconds|minutes|hours|days|weeks|milliseconds)\s*\([^;\n]*'
    r'|\b(?:from_timestamp|from_ymd|from_hms|timestamp_nanos|from_secs_f64|from_secs_f32)\s*\([^;\n]*'
    r'|(?:from_timestamp_\w+|from_ymd_opt|and_hms_\w+|checked_add_signed|checked_sub_signed)\([^;]*?\)\s*\.(?:unwrap|expect)\('
)


def findings(root=ROOT):
    files = sorted((root / "crates").glob("*/src/**/*.rs"))
    tests = rust_test_only_files(files)
    result = Counter()
    for path in files:
        if path in tests:
            continue
        code = strip_noncode(path.read_text(encoding="utf-8"))
        for match in PATTERN.finditer(code):
            expression = " ".join(match.group().split())
            result[f"{path.relative_to(root).as_posix()}::{expression}"] += 1
    return result


def main():
    expected = Counter(json.loads(REGISTRY.read_text(encoding="utf-8"))["reviewed"])
    current = findings()
    extra = current - expected
    stale = expected - current
    forks = json.loads((ROOT / "assurance/registries/temporal-driver-forks.json").read_text(encoding="utf-8"))
    for path, digest in forks["reviewed_files"].items():
        if hashlib.sha256((ROOT / path).read_text(encoding="utf-8").encode("utf-8")).hexdigest() != digest:
            print(f"temporal: riesaminare il decoder vendorizzato: {path}")
            return 1
    for item in extra:
        print(f"temporal: conversione nuova da rendere checked: {item}")
    for item in stale:
        print(f"temporal: eccezione obsoleta da rimuovere: {item}")
    # Queste chiamate aggirerebbero la barriera che precede chrono.
    for driver, pattern in [("postgres", r'Option<chrono::NaiveTime>'),
                            ("sqlserver", r'(?:try_get::<chrono::|Option<(?:NaiveDate|NaiveTime|NaiveDateTime|DateTime<FixedOffset>)> = get)')]:
        for path in (ROOT / f"crates/plenora-db-{driver}/src").rglob("*.rs"):
            if path.name.endswith("tests.rs"):
                continue
            if re.search(pattern, strip_noncode(path.read_text(encoding="utf-8"))):
                print(f"temporal: decoder senza prevalidazione: {path.relative_to(ROOT)}")
                return 1
    if extra or stale:
        return 1
    print("temporal: conversioni infallibili e bypass decoder invariati")
    return 0


if __name__ == "__main__":
    sys.exit(main())
