#!/usr/bin/env python3
"""Gate opzionale per una fixture SQL Server PolyBase reale."""

from __future__ import annotations

import re
import sys

from check_sqlserver_reference import run_cargo


def main() -> int:
    try:
        output = run_cargo(
            [
                "test",
                "-p",
                "plenora-db-sqlserver",
                "polybase_external_catalog_is_structural_and_not_implicit",
                "--",
                "--ignored",
                "--nocapture",
            ],
            capture=True,
        )
    except RuntimeError as error:
        print(f"sqlserver PolyBase gate: {error}", file=sys.stderr)
        return 1
    if not re.search(r"test result: ok\. 1 passed; 0 failed;", output):
        print("sqlserver PolyBase gate: esito inatteso", file=sys.stderr)
        return 1
    print("sqlserver PolyBase gate: passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
