"""Carica i controlli semantici dal checkout immutabile adottato."""

from __future__ import annotations

if __package__:
    from .strict_json import loads as strict_json_loads
else:  # esecuzione diretta da scripts/
    from strict_json import loads as strict_json_loads

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "contracts" / "adoption-source.json"


def validate_pins(root: Path) -> None:
    """Un solo pin per manifesti e checkout CI del repository."""
    canonical = strict_json_loads((root / "contracts/adoption-source.json").read_text(encoding="utf-8"))
    expected = canonical["contracts_source"]["revision"]
    repository = canonical["contracts_source"]["repository"]
    for path in (root / "contracts").rglob("*.json"):
        document = strict_json_loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            continue
        source = document.get("contracts_source", document)
        if isinstance(source, dict) and source.get("repository") == repository:
            if source.get("revision") != expected:
                raise ValueError("manifesto con un secondo pin dei contratti")
    for path in (root / ".github/workflows").glob("*.y*ml"):
        source = path.read_text(encoding="utf-8")
        for block in re.split(r"(?m)^      - ", source):
            if "repository: PlenoraETL/plenora-contracts" in block:
                refs = re.findall(r"(?m)^\s+ref: (\S+)\s*$", block)
                if refs != [expected]:
                    raise ValueError("checkout CI con un secondo pin dei contratti")


def load_semantics(contracts: Path) -> ModuleType:
    validate_pins(ROOT)
    contracts = contracts.resolve()
    expected = strict_json_loads(SOURCE.read_text(encoding="utf-8"))["contracts_source"]["revision"]
    command = ["git", "-c", f"safe.directory={contracts.as_posix()}"]
    revision = subprocess.run(
        [*command, "rev-parse", "HEAD"], cwd=contracts,
        capture_output=True, check=True, text=True,
    ).stdout.strip()
    if revision != expected:
        raise RuntimeError("checkout plenora-contracts diverso dal pin")
    subprocess.run(
        [*command, "diff", "--exit-code", "HEAD", "--", "schemas", "catalogs",
         "profiles", "specs", "vectors", "bindings", "tools"],
        cwd=contracts, capture_output=True, check=True,
    )
    spec = importlib.util.spec_from_file_location(
        "plenora_pinned_conformance", contracts / "tools" / "conformance_checks.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("controlli semantici del pin non disponibili")
    module = importlib.util.module_from_spec(spec)
    # Il validatore del pin importa json_values: anche la dipendenza deve
    # provenire da quel checkout, mai da un modulo omonimo gia in cache.
    dependency_spec = importlib.util.spec_from_file_location(
        "json_values", contracts / "tools" / "json_values.py"
    )
    if dependency_spec is None or dependency_spec.loader is None:
        raise RuntimeError("dipendenza semantica del pin non disponibile")
    dependency = importlib.util.module_from_spec(dependency_spec)
    previous = sys.modules.get("json_values")
    previous_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    sys.modules["json_values"] = dependency
    try:
        dependency_spec.loader.exec_module(dependency)
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous_bytecode
        if previous is None:
            sys.modules.pop("json_values", None)
        else:
            sys.modules["json_values"] = previous
    return module
