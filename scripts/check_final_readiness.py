#!/usr/bin/env python3
"""Gate fail-closed della metadata candidate finale 1.0.0."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FINAL_MANIFEST = ROOT / "release" / "final-readiness.json"
EXPECTED_VERSION = "1.0.0"
EXPECTED_EVIDENCE_BASE = "16248a904f19062403ea3f0215e5c7b620ed9b72"
EXPECTED_CONTRACTS_REVISION = "e81c3ce7941bacbdb0e083f03c512ae22a6b924a"
EXPECTED_EVIDENCE = {
    "mysql_reference": 30693284166,
    "postgres_reference": 30693284151,
    "sqlserver_reference": 30693284152,
    "workspace_coverage": 30693284173,
    "ewkb_fuzz": 30693284153,
    "postgres_matrix": 30693284154,
    "sqlserver_matrix": 30693284160,
    "release_manifest": 30693284177,
}
WORKSPACE_PACKAGES = {
    "plenora-database-cli",
    "plenora-database-core",
    "plenora-database-engine",
    "plenora-database-sql",
    "plenora-database-testkit",
    "plenora-db-mysql",
    "plenora-db-postgres",
    "plenora-db-sqlserver",
}
REQUIRED_MYSQL_REDUCTIONS = {
    "mysql_relational_write",
    "mysql_version_matrix",
    "mysql_spatial_dimensions",
}


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: oggetto JSON atteso")
    return value


def workspace_versions(root: Path) -> tuple[str | None, dict[str, str], str | None]:
    cargo = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    fuzz_lock = tomllib.loads((root / "fuzz" / "Cargo.lock").read_text(encoding="utf-8"))
    locked = {
        package["name"]: package["version"]
        for package in lock["package"]
        if package["name"] in WORKSPACE_PACKAGES
    }
    fuzz_core = next(
        (
            package["version"]
            for package in fuzz_lock["package"]
            if package["name"] == "plenora-database-core"
        ),
        None,
    )
    return cargo.get("workspace", {}).get("package", {}).get("version"), locked, fuzz_core


def validate_final_readiness(document: dict[str, Any], root: Path = ROOT) -> list[str]:
    errors: list[str] = []
    if document.get("manifest_version") != 1:
        errors.append("manifest_version deve essere 1")
    if document.get("component") != "plenora-database-tools":
        errors.append("component inatteso")
    if document.get("component_version") != EXPECTED_VERSION:
        errors.append("component_version deve essere 1.0.0")
    if document.get("status") != "metadata_candidate_pending_post_diff_ci":
        errors.append("stato candidate inatteso")
    if document.get("revision") != EXPECTED_EVIDENCE_BASE:
        errors.append("evidence base inattesa")
    if document.get("verification_claim") != "verified_internally":
        errors.append("claim di verifica inatteso")
    if document.get("independent_review") is not False:
        errors.append("review indipendente promossa")
    if document.get("claims") != {
        "component_rc": True,
        "system_rc": False,
        "avionic_certification": False,
    }:
        errors.append("claims finali inattesi")

    candidate = document.get("candidate", {})
    if candidate.get("evidence_base_revision") != EXPECTED_EVIDENCE_BASE:
        errors.append("candidate evidence base inattesa")
    if candidate.get("metadata_delta_requires_new_same_sha_ci") is not True:
        errors.append("nuova CI same-SHA non richiesta")
    if set(candidate.get("production_delta", [])) != {
        "Cargo.toml",
        "Cargo.lock",
        "fuzz/Cargo.lock",
    }:
        errors.append("delta produzione non e solo SemVer/lock")

    actual_evidence = {
        item.get("id"): item
        for item in document.get("evidence", [])
        if isinstance(item, dict)
    }
    if set(actual_evidence) != set(EXPECTED_EVIDENCE):
        errors.append("set evidenze finali inatteso")
    for evidence_id, run_id in EXPECTED_EVIDENCE.items():
        item = actual_evidence.get(evidence_id, {})
        if item.get("status") != "passed":
            errors.append(f"{evidence_id}: stato non passed")
        if item.get("revision") != EXPECTED_EVIDENCE_BASE:
            errors.append(f"{evidence_id}: revisione non same-SHA")
        if item.get("run_id") != run_id:
            errors.append(f"{evidence_id}: run_id inatteso")

    if document.get("release_criteria", {}).get("revision") != EXPECTED_CONTRACTS_REVISION:
        errors.append("release criteria Contracts non aggiornati")
    icd = document.get("current_icd_baseline", {})
    if icd.get("revision") != EXPECTED_CONTRACTS_REVISION or icd.get("tag") is not None:
        errors.append("baseline ICD deve usare revisione esatta senza tag inventato")
    reductions = {
        item.get("area")
        for item in document.get("declared_scope_reductions", [])
        if isinstance(item, dict)
    }
    if not REQUIRED_MYSQL_REDUCTIONS.issubset(reductions):
        errors.append("riduzioni di perimetro MySQL incomplete")

    release_action = document.get("release_action", {})
    if release_action.get("allowed") is not False:
        errors.append("release action autorizzata prematuramente")
    if release_action.get("intended_tag") != "v1.0.0":
        errors.append("tag finale previsto inatteso")
    if release_action.get("tag_created") is not False:
        errors.append("tag finale dichiarato prematuramente")
    if release_action.get("tag_target") is not None or release_action.get("tag_object") is not None:
        errors.append("identita tag finale inventata")

    workspace, locked, fuzz_core = workspace_versions(root)
    if workspace != EXPECTED_VERSION:
        errors.append("workspace non allineato a 1.0.0")
    if locked != {name: EXPECTED_VERSION for name in WORKSPACE_PACKAGES}:
        errors.append("Cargo.lock non allineato a 1.0.0")
    if fuzz_core != EXPECTED_VERSION:
        errors.append("fuzz/Cargo.lock non allineato a 1.0.0")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", nargs="?", type=Path, default=FINAL_MANIFEST)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--json-out", type=Path)
    arguments = parser.parse_args()
    try:
        document = load_json(arguments.manifest)
        errors = validate_final_readiness(document, arguments.repo.resolve())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors = [str(error)]
    result = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "component": "plenora-database-tools",
        "component_version": EXPECTED_VERSION,
        "manifest": str(arguments.manifest),
        "errors": errors,
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if arguments.json_out is not None:
        arguments.json_out.write_text(rendered, encoding="utf-8")
    stream = sys.stdout if not errors else sys.stderr
    stream.write(rendered)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
