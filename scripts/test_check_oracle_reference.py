#!/usr/bin/env python3
"""Self-test statico del gate Oracle."""

from __future__ import annotations

import json
import io
import subprocess
from contextlib import redirect_stdout
from unittest.mock import patch
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import strict_json
import scripts.check_oracle_reference as gate


def check_timeout_output() -> None:
    from scripts import check_db2_reference, check_sqlserver_reference

    for module, kwargs in [
        (gate, {}),
        (check_db2_reference, {}),
        (check_sqlserver_reference, {"capture": True, "timeout_seconds": 1}),
    ]:
        for stdout, stderr in [(b"test live_probe ...", b"diagnostic\n"), ("partial", None)]:
            output = io.StringIO()
            timeout = subprocess.TimeoutExpired(["cargo"], 1, output=stdout, stderr=stderr)
            with patch.object(module.subprocess, "run", side_effect=timeout), redirect_stdout(output):
                try:
                    module.run(["cargo"], **kwargs)
                except RuntimeError as error:
                    assert "timeout" in str(error)
                else:
                    raise AssertionError("un timeout deve fallire il gate")
            expected = "test live_probe ...diagnostic\n" if isinstance(stdout, bytes) else stdout
            assert output.getvalue() == expected


def check_captured_inventory() -> None:
    names = sorted(gate.live_source_inventory())
    qualified = [f"live_tests::parameter_fidelity::{name}" for name in names]
    listing = "\n".join(f"{name}: test" for name in qualified)
    output = "\n".join(f"test {name} ... ok" for name in qualified)
    output += "\n\nsuccesses:\n---- captured stdout ----\nmisura -0.0\n"
    assert gate.validate_live_run(listing, output) == qualified
    for status in ("ignored", "FAILED", "misura -0.0\nok"):
        broken = output.replace(f"test {qualified[0]} ... ok", f"test {qualified[0]} ... {status}")
        try:
            gate.validate_live_run(listing, broken)
        except RuntimeError:
            pass
        else:
            raise AssertionError("un test senza esito esatto deve fallire il gate")


def main() -> int:
    check_timeout_output()
    check_captured_inventory()
    reference = gate.reference_contract()
    assert reference["server_major"] == "23"
    assert reference["platform"] == "linux/amd64"
    assert "PRODUCT_COMPONENT_VERSION" in gate.SERVER_VERSION_SQL
    assert "V$" not in gate.SERVER_VERSION_SQL
    assert gate.REQUIRED_LIVE_TESTS <= gate.live_source_inventory()
    gate_source = (ROOT / "scripts/check_oracle_reference.py").read_text(
        encoding="utf-8"
    )
    assert 'TemporaryDirectory(prefix="plenora-oracle-wheel-")' in gate_source
    assert '"--show-output"' in gate_source
    assert '"--nocapture"' not in gate_source
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
    strict_json.loads((ROOT / "docker/oracle/references.json").read_text(encoding="utf-8"))
    print("PASS oracle gate wiring")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
