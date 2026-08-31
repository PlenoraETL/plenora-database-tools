#!/usr/bin/env python3
"""Self-test statico del gate AGE."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import scripts.check_age_reference as gate  # noqa: E402


DIGEST = "sha256:e7de1717e487dac7c1be93a1cd5360a2cf07ff4170342c2af2ac4713c21baf00"


def main() -> int:
    compose = (ROOT / "docker-compose.age.yml").read_text(encoding="utf-8")
    workflow = (ROOT / ".github/workflows/age-assurance.yml").read_text(
        encoding="utf-8"
    )
    command = gate.cargo_command
    assert DIGEST in compose
    assert "apache/age@sha256:" in compose
    test_name = gate.LIVE_TEST.rsplit("::", 1)[-1]
    assert test_name in (
        ROOT / "crates/plenora-db-postgres/src/age_tests.rs"
    ).read_text(encoding="utf-8")
    assert "PLENORA_REQUIRE_LIVE_AGE=1" in (ROOT / "scripts/check_age_reference.py").read_text(
        encoding="utf-8"
    )
    assert "scripts/check_age_reference.py" in workflow
    assert callable(command)
    print("PASS age gate wiring")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
