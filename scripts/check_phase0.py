#!/usr/bin/env python3
"""Esegue l'intero gate offline della Fase 0 con un solo comando."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

try:
    from scripts.phase0_validate import ValidationError, run_gate
except ModuleNotFoundError:  # esecuzione diretta: python scripts\...
    from phase0_validate import ValidationError, run_gate


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    tests = subprocess.run(
        [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-t",
            ".",
            "-p",
            "test_*.py",
            "-v",
        ],
        cwd=REPO_ROOT,
        check=False,
    )
    if tests.returncode != 0:
        return tests.returncode
    try:
        report = run_gate()
    except ValidationError as exc:
        print(f"phase0 check: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
