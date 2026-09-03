#!/usr/bin/env python3
"""Self-test statico del gate Oracle."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import scripts.check_oracle_reference as gate


def main() -> int:
    reference = gate.reference_contract()
    assert reference["server_major"] == "23"
    assert reference["platform"] == "linux/amd64"
    assert "PRODUCT_COMPONENT_VERSION" in gate.SERVER_VERSION_SQL
    assert "V$" not in gate.SERVER_VERSION_SQL
    assert gate.REQUIRED_LIVE_TESTS <= gate.live_source_inventory()
    workflow = (ROOT / ".github/workflows/oracle-assurance.yml").read_text(
        encoding="utf-8"
    )
    for required in (
        "scripts/test_check_oracle_reference.py",
        "scripts/check_oracle_reference.py",
        "docker-compose.oracle.yml up -d --wait",
        "docker-compose.oracle.yml down --volumes",
    ):
        assert required in workflow, required
    json.loads((ROOT / "docker/oracle/references.json").read_text(encoding="utf-8"))
    print("PASS oracle gate wiring")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
