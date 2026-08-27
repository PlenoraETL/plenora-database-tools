#!/usr/bin/env python3
"""Impedisce che suite Rust tornino dentro i file di prodotto.

I moduli ``cfg(test)`` possono essere file esterni oppure vivere inline in un
file gia dedicato ai test. Nei sorgenti di prodotto devono invece essere
esterni: restano moduli figli, quindi vedono gli elementi privati senza
allargare l'API del crate.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

try:
    from scripts.code_size import cfg_test_items, rust_test_only_files
    from scripts.live_inventory import TEST_ATTRIBUTE, annotated_tests, strip_noncode
except ModuleNotFoundError:  # esecuzione diretta: python scripts/check_test_layout.py
    from code_size import cfg_test_items, rust_test_only_files
    from live_inventory import TEST_ATTRIBUTE, annotated_tests, strip_noncode


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    reason: str


def is_dedicated_test(path: Path) -> bool:
    """Riconosce le directory che Cargo tratta come superfici di test."""

    return any(part in {"test", "tests"} for part in path.parts)


def scan(root: Path = ROOT) -> tuple[int, list[Violation]]:
    """Rende numero di sorgenti controllati e moduli test inline trovati."""

    checked = 0
    violations: list[Violation] = []
    files = sorted((root / "crates").glob("*/src/**/*.rs"))
    test_only = rust_test_only_files(files)
    for path in files:
        if path in test_only or is_dedicated_test(path):
            continue
        checked += 1
        source = path.read_text(encoding="utf-8")
        _, spans = cfg_test_items(source)
        violations.extend(
            Violation(
                path.relative_to(root),
                source.count("\n", 0, start) + 1,
                "spostare il modulo cfg(test) in un file dedicato",
            )
            for start, _ in spans
        )
        code = strip_noncode(source)
        if TEST_ATTRIBUTE.search(code) is None and "cfg_attr" not in code:
            continue
        for name in annotated_tests(source):
            marker = f"fn {name}"
            position = code.find(marker)
            if any(start <= position < end for start, end in spans):
                continue
            violations.append(
                Violation(
                    path.relative_to(root),
                    source.count("\n", 0, max(position, 0)) + 1,
                    "spostare la funzione #[test] in un modulo dedicato",
                )
            )
    return checked, violations


def main() -> int:
    checked, violations = scan()
    if violations:
        for violation in violations:
            print(
                f"{violation.path}:{violation.line}: test-layout: {violation.reason}"
            )
        return 1
    print(f"test-layout: {checked} sorgenti Rust controllati, nessuna violazione")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
