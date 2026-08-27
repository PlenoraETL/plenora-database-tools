#!/usr/bin/env python3
"""Misura riproducibile del solo codice di prodotto.

Test, evidence, esempi e gate non entrano nel denominatore: ridurli non e un
refactor del prodotto. I moduli Rust protetti da ``cfg(test)`` vengono esclusi
sia quando vivono in un file proprio sia quando sono inline.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

try:
    from scripts.live_inventory import strip_noncode
except ModuleNotFoundError:  # esecuzione diretta: python scripts/code_size.py
    from live_inventory import strip_noncode


REPO_ROOT = Path(__file__).resolve().parents[1]
BUDGET_PATH = REPO_ROOT / "scripts" / "code_size_budget.json"
CFG_TEST_MODULE = re.compile(
    r"#\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*"
    r"(?:#\[[^\]]*\]\s*)*"
    r"(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*([;{])",
    re.MULTILINE,
)


@dataclass(frozen=True)
class Count:
    physical: int = 0
    code: int = 0

    def __add__(self, other: "Count") -> "Count":
        return Count(self.physical + other.physical, self.code + other.code)


def cfg_test_items(source: str) -> tuple[set[str], list[tuple[int, int]]]:
    """Rende moduli esterni e intervalli dei moduli inline ``cfg(test)``."""

    code = strip_noncode(source)
    external: set[str] = set()
    spans: list[tuple[int, int]] = []
    for match in CFG_TEST_MODULE.finditer(code):
        if match.group(2) == ";":
            external.add(match.group(1))
            continue
        depth = 0
        end = match.end() - 1
        while end < len(code):
            if code[end] == "{":
                depth += 1
            elif code[end] == "}":
                depth -= 1
                if depth == 0:
                    end += 1
                    break
            end += 1
        if depth != 0:
            raise ValueError("modulo cfg(test) con parentesi non bilanciate")
        spans.append((match.start(), end))
    return external, spans


def without_spans(source: str, spans: list[tuple[int, int]]) -> str:
    for start, end in reversed(spans):
        source = source[:start] + source[end:]
    return source


def rust_test_only_files(files: Sequence[Path]) -> set[Path]:
    excluded = {
        path
        for path in files
        if any(part in {"test", "tests", "example", "examples"} for part in path.parts)
    }
    for path in files:
        source = path.read_text(encoding="utf-8")
        external, _ = cfg_test_items(source)
        for name in external:
            sibling = path.with_name(f"{name}.rs")
            nested = path.parent / name / "mod.rs"
            if sibling.exists():
                excluded.add(sibling)
            if nested.exists():
                excluded.add(nested)
    return excluded


def count_rust(source: str) -> Count:
    _, spans = cfg_test_items(source)
    product = without_spans(source, spans)
    physical = len(product.splitlines())
    code = sum(bool(line.strip()) for line in strip_noncode(product).splitlines())
    return Count(physical, code)


def count_python(source: str) -> Count:
    lines = source.splitlines()
    return Count(
        len(lines),
        sum(bool(line.strip()) and not line.lstrip().startswith("#") for line in lines),
    )


def measure(root: Path = REPO_ROOT) -> dict[str, object]:
    rust_files = sorted((root / "crates").glob("*/src/**/*.rs"))
    test_only = rust_test_only_files(rust_files)
    areas: dict[str, Count] = {}
    for path in rust_files:
        if path in test_only:
            continue
        crate = path.relative_to(root).parts[1]
        areas[crate] = areas.get(crate, Count()) + count_rust(
            path.read_text(encoding="utf-8")
        )

    package = root / "crates" / "plenora-database-py" / "python" / "plenora_database"
    python_count = Count()
    for path in sorted(package.glob("*.py")):
        python_count += count_python(path.read_text(encoding="utf-8"))
    areas["python-package"] = python_count

    total = Count()
    for count in areas.values():
        total += count
    return {
        "schema_version": 1,
        "areas": {
            name: {"physical_lines": count.physical, "code_lines": count.code}
            for name, count in sorted(areas.items())
        },
        "total": {
            "physical_lines": total.physical,
            "code_lines": total.code,
        },
    }


def check_budget(report: dict[str, object], budget_path: Path = BUDGET_PATH) -> list[str]:
    budget = json.loads(budget_path.read_text(encoding="utf-8"))
    total = report["total"]
    assert isinstance(total, dict)
    failures = []
    for metric in ("physical_lines", "code_lines"):
        maximum = budget[f"max_{metric}"]
        actual = total[metric]
        if actual > maximum:
            failures.append(f"{metric}: {actual} oltre il budget {maximum}")
    return failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    report = measure()
    print(json.dumps(report, indent=2, sort_keys=True))
    if not args.check:
        return 0
    failures = check_budget(report)
    if failures:
        for failure in failures:
            print(f"code-size: {failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
