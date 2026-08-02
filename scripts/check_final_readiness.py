#!/usr/bin/env python3
"""Gate fail-closed dei record di release stabili Database Tools."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FINAL_MANIFEST = ROOT / "release" / "final-readiness.json"
RELEASE_MANIFEST = ROOT / "release" / "1.1.0.json"
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
EXPECTED_RELEASE_VERSION = "1.1.0"
EXPECTED_RELEASE_BASE = "ee4fa7470d48f152aa40d86fb911e780bc75a908"
EXPECTED_RELEASE_CONTRACTS_REVISION = "ec55bf1379b8b26ca9b9e29e10b954e39c2258ed"
EXPECTED_PREVIOUS_RELEASE = "89c82c4c700550decc394bb1e43b22c8a32e44e1"
EXPECTED_RELEASE_EVIDENCE = {
    "release_manifest": 30744606166,
    "mysql_assurance": 30744606163,
    "mysql_version_matrix": 30746069178,
    "workspace_coverage": 30746112820,
    "ewkb_fuzz": 30746113685,
    "sqlserver_assurance": 30744606173,
    "postgres_assurance": 30744606189,
}
EXPECTED_RELEASE_PRODUCTION_DELTA = {
    "Cargo.toml",
    "Cargo.lock",
    "fuzz/Cargo.lock",
}
EXPECTED_RELEASE_ASSURANCE_DELTA = {
    ".github/workflows/release-manifest.yml",
    "README.md",
    "docs/FINAL-1.1.0-READINESS.md",
    "docs/PROVIDER-MATURITY-MATRIX.md",
    "release/1.1.0.json",
    "scripts/check_final_readiness.py",
    "scripts/check_mysql_matrix.py",
    "scripts/test_check_final_readiness.py",
    "tests/test_mysql_matrix.py",
}
EXPECTED_RELEASE_CRITERIA = {
    "repository": "plenora-contracts",
    "revision": EXPECTED_RELEASE_CONTRACTS_REVISION,
    "tag": None,
    "document": "conformance/releases/v1.0.0.json",
    "normative_status": "frozen_campaign_baseline",
}
EXPECTED_RELEASE_REDUCTIONS = [
    {
        "area": "mysql_spatial_dimensions",
        "status": "fail_closed",
        "runtime_policy": "XY_only; reject Z, M and ZM before network I/O",
    },
    {
        "area": "mysql_mariadb",
        "status": "not_qualified",
        "runtime_policy": "do not infer compatibility from MySQL evidence",
    },
    {
        "area": "mysql_geography",
        "status": "not_published",
        "runtime_policy": "capability remains false",
    },
    {
        "area": "mysql_spatial_index",
        "status": "not_published",
        "runtime_policy": "capability remains false",
    },
    {
        "area": "sqlserver_azure",
        "status": "not_qualified",
        "runtime_policy": "opt-in only; no Azure SQL qualification claim",
    },
]
EXPECTED_RELEASE_EXTERNAL_DEPENDENCIES = [
    {
        "id": "PLN-DB-CROSS-LIBRARY",
        "owner": "plenora-contracts/conformance",
        "status": "pending",
        "description": "Le catene PostgreSQL/MySQL con Data Tools e IO Tools devono passare sui nuovi artefatti prima del tag.",
    },
    {
        "id": "PLN-DB-SYSTEM",
        "owner": "plenora-contracts/conformance",
        "status": "pending",
        "description": "La comparativa Plenora resta separata e non autorizza claim system_rc.",
    },
]


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


def validate_final_readiness(
    document: dict[str, Any], root: Path = ROOT, *, validate_workspace: bool = True
) -> list[str]:
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

    if validate_workspace:
        workspace, locked, fuzz_core = workspace_versions(root)
        if workspace != EXPECTED_VERSION:
            errors.append("workspace non allineato a 1.0.0")
        if locked != {name: EXPECTED_VERSION for name in WORKSPACE_PACKAGES}:
            errors.append("Cargo.lock non allineato a 1.0.0")
        if fuzz_core != EXPECTED_VERSION:
            errors.append("fuzz/Cargo.lock non allineato a 1.0.0")
    return errors


def validate_release_readiness(
    document: dict[str, Any],
    root: Path = ROOT,
    *,
    expected_version: str | None = None,
) -> list[str]:
    """Valida il record corrente o quello storico senza reinterpretarne i claim."""
    actual_version = document.get("component_version")
    if expected_version is not None and actual_version != expected_version:
        return [
            f"manifest atteso per {expected_version}, component_version={actual_version!r}"
        ]
    selected_version = expected_version or actual_version
    if selected_version == EXPECTED_VERSION:
        return validate_final_readiness(document, root, validate_workspace=False)

    errors: list[str] = []
    if document.get("manifest_version") != 1:
        errors.append("manifest_version deve essere 1")
    if document.get("component") != "plenora-database-tools":
        errors.append("component inatteso")
    if document.get("component_version") != EXPECTED_RELEASE_VERSION:
        errors.append("component_version deve essere 1.1.0")
    if document.get("status") != "metadata_candidate_pending_same_sha_ci":
        errors.append("stato candidate 1.1.0 inatteso")
    if document.get("revision") != EXPECTED_RELEASE_BASE:
        errors.append("evidence base 1.1.0 inattesa")
    if document.get("verification_claim") != "verified_internally":
        errors.append("claim di verifica 1.1.0 inatteso")
    if document.get("independent_review") is not False:
        errors.append("review indipendente 1.1.0 promossa prematuramente")
    if document.get("claims") != {
        "component_rc": True,
        "system_rc": False,
        "avionic_certification": False,
    }:
        errors.append("claims 1.1.0 inattesi")

    supersedes = document.get("supersedes", {})
    if supersedes != {
        "tag": "v1.0.0",
        "revision": EXPECTED_PREVIOUS_RELEASE,
        "immutable": True,
    }:
        errors.append("identita release 1.0.0 precedente inattesa")

    candidate = document.get("candidate", {})
    if candidate.get("evidence_base_revision") != EXPECTED_RELEASE_BASE:
        errors.append("candidate evidence base 1.1.0 inattesa")
    if candidate.get("evidence_base_result") != "passed":
        errors.append("candidate evidence base 1.1.0 non passed")
    if candidate.get("metadata_delta_requires_new_same_sha_ci") is not True:
        errors.append("nuova CI same-SHA 1.1.0 non richiesta")
    if set(candidate.get("production_delta", [])) != EXPECTED_RELEASE_PRODUCTION_DELTA:
        errors.append("delta produzione 1.1.0 inatteso")
    if set(candidate.get("assurance_delta", [])) != EXPECTED_RELEASE_ASSURANCE_DELTA:
        errors.append("delta assurance 1.1.0 inatteso")

    actual_evidence = {
        item.get("id"): item
        for item in document.get("evidence", [])
        if isinstance(item, dict)
    }
    if set(actual_evidence) != set(EXPECTED_RELEASE_EVIDENCE):
        errors.append("set evidenze 1.1.0 inatteso")
    for evidence_id, run_id in EXPECTED_RELEASE_EVIDENCE.items():
        item = actual_evidence.get(evidence_id, {})
        if item.get("status") != "passed":
            errors.append(f"{evidence_id}: stato non passed")
        if item.get("revision") != EXPECTED_RELEASE_BASE:
            errors.append(f"{evidence_id}: revisione non same-SHA")
        if item.get("run_id") != run_id:
            errors.append(f"{evidence_id}: run_id inatteso")
        expected_url = (
            "https://github.com/PlenoraETL/plenora-database-tools/actions/runs/"
            f"{run_id}"
        )
        if item.get("url") != expected_url:
            errors.append(f"{evidence_id}: URL inatteso")

    criteria = document.get("release_criteria", {})
    if criteria != EXPECTED_RELEASE_CRITERIA:
        errors.append("release criteria Contracts 1.1.0 inattesi")
    if document.get("current_icd_baseline") != EXPECTED_RELEASE_CRITERIA:
        errors.append("baseline ICD 1.1.0 inattesa")

    mysql_scope = document.get("mysql_scope", {})
    if mysql_scope != {
        "references": ["8.0.46", "8.4.11"],
        "tls": "required_private_ca_and_hostname_verified",
        "relational_query": "qualified",
        "write_modes": ["Append", "SingleTransaction"],
        "transaction_semantics": "single_stream_transaction_rollback_or_quarantine",
        "spatial": "geometry_wkb_xy_srid",
        "dimensions": ["XY"],
        "higher_dimensions": "Z_M_ZM_fail_closed",
        "local_infile": "disabled",
        "max_placeholders": 65535,
        "mariadb_compatible": False,
        "geography_published": False,
        "spatial_index_published": False,
    }:
        errors.append("perimetro MySQL 1.1.0 inatteso")

    if document.get("declared_scope_reductions") != EXPECTED_RELEASE_REDUCTIONS:
        errors.append("riduzioni di perimetro 1.1.0 inattese")
    if document.get("external_dependencies") != EXPECTED_RELEASE_EXTERNAL_DEPENDENCIES:
        errors.append("dipendenze esterne 1.1.0 inattese")

    release_action = document.get("release_action", {})
    if release_action.get("allowed") is not False:
        errors.append("release action 1.1.0 autorizzata prematuramente")
    if release_action.get("intended_tag") != "v1.1.0":
        errors.append("tag 1.1.0 previsto inatteso")
    if release_action.get("tag_created") is not False:
        errors.append("tag 1.1.0 dichiarato prematuramente")
    if release_action.get("tag_target") is not None or release_action.get("tag_object") is not None:
        errors.append("identita tag 1.1.0 inventata")

    workspace, locked, fuzz_core = workspace_versions(root)
    if workspace != EXPECTED_RELEASE_VERSION:
        errors.append("workspace non allineato a 1.1.0")
    if locked != {name: EXPECTED_RELEASE_VERSION for name in WORKSPACE_PACKAGES}:
        errors.append("Cargo.lock non allineato a 1.1.0")
    if fuzz_core != EXPECTED_RELEASE_VERSION:
        errors.append("fuzz/Cargo.lock non allineato a 1.1.0")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", nargs="?", type=Path, default=RELEASE_MANIFEST)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--json-out", type=Path)
    arguments = parser.parse_args()
    document: dict[str, Any] | None = None
    try:
        document = load_json(arguments.manifest)
        expected_version = {
            FINAL_MANIFEST.name: EXPECTED_VERSION,
            RELEASE_MANIFEST.name: EXPECTED_RELEASE_VERSION,
        }.get(arguments.manifest.name)
        if expected_version is None:
            errors = [f"manifest readiness non supportato: {arguments.manifest.name}"]
        else:
            errors = validate_release_readiness(
                document,
                arguments.repo.resolve(),
                expected_version=expected_version,
            )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors = [str(error)]
    result = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "component": "plenora-database-tools",
        "component_version": document.get("component_version") if document is not None else None,
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
