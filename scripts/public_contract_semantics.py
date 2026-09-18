"""Carica i controlli semantici dal checkout immutabile adottato."""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path
from types import ModuleType


SOURCE = Path(__file__).resolve().parents[1] / "contracts" / "adoption-source.json"


def load_semantics(contracts: Path) -> ModuleType:
    contracts = contracts.resolve()
    expected = json.loads(SOURCE.read_text(encoding="utf-8"))["contracts_source"]["revision"]
    command = ["git", "-c", f"safe.directory={contracts.as_posix()}"]
    revision = subprocess.run(
        [*command, "rev-parse", "HEAD"], cwd=contracts,
        capture_output=True, check=True, text=True,
    ).stdout.strip()
    if revision != expected:
        raise RuntimeError("checkout plenora-contracts diverso dal pin")
    subprocess.run(
        [*command, "diff", "--exit-code", "HEAD", "--", "schemas", "catalogs",
         "tools/conformance_checks.py"],
        cwd=contracts, capture_output=True, check=True,
    )
    spec = importlib.util.spec_from_file_location(
        "plenora_pinned_conformance", contracts / "tools" / "conformance_checks.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("controlli semantici del pin non disponibili")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
