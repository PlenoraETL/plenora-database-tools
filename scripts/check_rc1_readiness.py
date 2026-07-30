#!/usr/bin/env python3
"""Verifica il pacchetto di readiness della RC1 di componente.

Questo gate non sostituisce la revisione indipendente. Impedisce invece che un
candidato bloccato venga presentato come RC, che l'evidenza punti a revisioni
diverse dalla baseline, che una riduzione di scope sparisca dal manifesto o che
un requisito soltanto dichiarato venga presentato come verificato live.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tomllib
from pathlib import Path


REVISION = re.compile(r"^[0-9a-f]{40}$")
REQUIRED_EVIDENCE = {
    "postgres_reference",
    "sqlserver_reference",
    "workspace_coverage",
    "release_manifest",
}
REQUIRED_EXTERNAL_DEPENDENCIES = {
    "PLN-DB-R46",
    "PLN-DB-R153",
    "PLN-DB-SYSTEM",
}
REQUIRED_SCOPE_REDUCTIONS = {
    "sqlserver_version_matrix",
    "sqlserver_spatial_dimensions",
    "sqlserver_private_ca",
    "sqlserver_extended_catalog",
}
EXPECTED_ASSURANCE_STATUS = {
    "sqlserver_version_matrix": "partially_verified_live",
    "sqlserver_spatial_dimensions": "partially_verified_live",
    "sqlserver_private_ca": "partially_verified_live",
    "sqlserver_extended_catalog": "declared_only",
}
REQUIRED_UNVERIFIED_CLAIMS = {
    "sqlserver_version_matrix": {
        "sqlserver_2019",
        "sqlserver_2025",
        "azure_sql",
    },
    "sqlserver_spatial_dimensions": {
        "spatial_z_other_paths",
        "spatial_m",
        "spatial_zm",
        "spatial_fullglobe",
    },
    "sqlserver_private_ca": {
        "private_ca_positive_chain",
        "private_ca_hostname_cases",
        "private_ca_rotation",
    },
    "sqlserver_extended_catalog": {
        "temporal_catalog",
        "graph_catalog",
        "external_table_catalog",
        "partition_catalog",
    },
}
REQUIRED_VERIFIED_TESTS = {
    "sqlserver_version_matrix": {
        "live_reference_probe_and_catalog",
    },
    "sqlserver_spatial_dimensions": {
        "live_spatial_preflight_rejects_mixed_srid_and_z",
    },
    "sqlserver_private_ca": {
        "live_reference_probe_and_catalog",
        "live_self_signed_tls_is_rejected_by_default",
    },
    "sqlserver_extended_catalog": set(),
}
FROZEN_PATHS = (
    ".github/workflows",
    ":(exclude).github/workflows/release-manifest.yml",
    "benchmarks",
    "Cargo.toml",
    "Cargo.lock",
    "catalog",
    "contracts",
    "crates",
    "docker",
    "docker-compose.postgres-tls.yml",
    "docker-compose.postgres.yml",
    "docker-compose.sqlserver.yml",
    "fuzz",
    "golden",
    "requirements-phase0.txt",
    "rust-toolchain.toml",
    "scripts",
    ":(exclude)scripts/check_rc1_readiness.py",
    ":(exclude)scripts/test_check_rc1_readiness.py",
    "tests",
)
WORKSPACE_PACKAGES = {
    "plenora-database-cli",
    "plenora-database-core",
    "plenora-database-engine",
    "plenora-database-sql",
    "plenora-database-testkit",
    "plenora-db-postgres",
    "plenora-db-sqlserver",
}


def git_success(repository: Path, arguments: list[str]) -> bool:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


def production_tree_matches(repository: Path, revision: str) -> bool:
    """Confronta codice e input delle evidenze, incluso il worktree."""
    if not git_success(
        repository,
        ["diff", "--quiet", revision, "--", *FROZEN_PATHS],
    ):
        return False
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
                *FROZEN_PATHS,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0 and not completed.stdout.strip()


def workspace_versions_match(repository: Path) -> bool:
    """Verifica che manifesti e lockfile dichiarino la stessa RC."""
    try:
        cargo = tomllib.loads(
            (repository / "Cargo.toml").read_text(encoding="utf-8")
        )
        if cargo.get("workspace", {}).get("package", {}).get("version") != "0.1.0-rc.1":
            return False
        lock = tomllib.loads(
            (repository / "Cargo.lock").read_text(encoding="utf-8")
        )
    except (OSError, subprocess.SubprocessError, tomllib.TOMLDecodeError):
        return False
    versions = {
        package.get("name"): package.get("version")
        for package in lock.get("package", [])
        if package.get("name") in WORKSPACE_PACKAGES
    }
    return versions == {name: "0.1.0-rc.1" for name in WORKSPACE_PACKAGES}


def pending_packaging_delta_matches(repository: Path, revision: str) -> bool:
    """Ammette soltanto l'allineamento SemVer dei manifesti Cargo."""
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "diff",
                "--name-only",
                revision,
                "--",
                *FROZEN_PATHS,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if completed.returncode != 0:
        return False
    changed = {
        line.strip().replace("\\", "/")
        for line in completed.stdout.splitlines()
        if line.strip()
    }
    return changed == {"Cargo.toml", "Cargo.lock"} and workspace_versions_match(
        repository
    )


def keyed_items(
    document: dict,
    field: str,
    key: str,
    errors: list[str],
) -> dict[str, dict]:
    values = document.get(field)
    if not isinstance(values, list):
        errors.append(f"{field} deve essere una lista")
        return {}
    indexed: dict[str, dict] = {}
    for index, value in enumerate(values):
        if not isinstance(value, dict) or not isinstance(value.get(key), str):
            errors.append(f"{field}[{index}] deve dichiarare {key}")
            continue
        identity = value[key]
        if identity in indexed:
            errors.append(f"{field} contiene {key} duplicato: {identity}")
            continue
        indexed[identity] = value
    return indexed


def check(document: dict, repository: Path) -> list[str]:
    errors: list[str] = []
    candidate_value = document.get("candidate")
    decision = (
        candidate_value.get("decision")
        if isinstance(candidate_value, dict)
        else None
    )
    revision = document.get("revision")
    if not isinstance(revision, str) or not REVISION.fullmatch(revision):
        errors.append("revision deve contenere 40 cifre esadecimali")
        revision = None
    elif not git_success(repository, ["cat-file", "-e", f"{revision}^{{commit}}"]):
        errors.append("revision non esiste nella storia locale")
    elif not git_success(repository, ["merge-base", "--is-ancestor", revision, "HEAD"]):
        errors.append("revision non e un antenato del checkout corrente")
    elif decision == "rebaseline_pending":
        if not pending_packaging_delta_matches(repository, revision):
            errors.append(
                "il rebaseline pending deve contenere soltanto il delta SemVer Cargo"
            )
    elif not production_tree_matches(repository, revision):
        errors.append("i percorsi di produzione divergono dalla baseline congelata")

    if document.get("manifest_version") != 1:
        errors.append("manifest_version deve essere 1")
    if document.get("component") != "plenora-database-tools":
        errors.append("component deve essere plenora-database-tools")
    if document.get("component_version") != "0.1.0-rc.1":
        errors.append("component_version deve essere 0.1.0-rc.1")
    if not workspace_versions_match(repository):
        errors.append("tutti i crate e Cargo.lock devono dichiarare 0.1.0-rc.1")
    expected_status = {
        "rebaseline_pending": "rc1_rebaseline_pending",
        "ready": "rc1_candidate_ready",
        "tagged": "component_rc_tagged",
    }.get(decision)
    if document.get("status") != expected_status:
        errors.append(f"status deve essere {expected_status}")
    if document.get("verification_claim") != "verified_internally":
        errors.append("la RC senza review puo dichiarare solo verified_internally")
    if document.get("independent_review") is not False:
        errors.append("independent_review deve restare false fino alla review")

    claims = document.get("claims")
    if not isinstance(claims, dict):
        errors.append("claims deve essere un oggetto")
        claims = {}
    if claims.get("system_rc") is not False:
        errors.append("system_rc deve essere false")
    if claims.get("avionic_certification") is not False:
        errors.append("avionic_certification deve essere false")

    candidate = candidate_value
    if not isinstance(candidate, dict):
        errors.append("candidate deve essere un oggetto")
        candidate = {}
    decision = candidate.get("decision")
    if decision not in ("rebaseline_pending", "ready", "tagged"):
        errors.append(
            "candidate.decision deve essere rebaseline_pending, ready oppure tagged"
        )
    expected_freeze = decision != "rebaseline_pending"
    if candidate.get("code_freeze") is not expected_freeze:
        errors.append(f"candidate.code_freeze deve essere {expected_freeze}")

    blockers = keyed_items(document, "component_rc_blockers", "id", errors)
    if decision == "rebaseline_pending":
        if claims.get("component_rc") is not False:
            errors.append("component_rc deve essere false durante il rebaseline")
        if set(blockers) != {"PLN-DB-REBASELINE"}:
            errors.append(
                "il rebaseline pending deve avere soltanto PLN-DB-REBASELINE"
            )
    elif decision in ("ready", "tagged"):
        if blockers:
            errors.append("una RC pronta o taggata non puo avere component_rc_blockers")
        if claims.get("component_rc") is not True:
            errors.append("una RC pronta o taggata deve dichiarare component_rc true")

    assurance_attributes = keyed_items(
        document, "open_assurance_attributes", "id", errors
    )
    review = assurance_attributes.get("PLN-DB-REVIEW")
    if review is None:
        errors.append("PLN-DB-REVIEW deve restare un attributo di assurance aperto")
    else:
        if review.get("status") != "pending_eligible_reviewer":
            errors.append("PLN-DB-REVIEW deve restare pending_eligible_reviewer")
        if review.get("blocks_component_rc_release") is not False:
            errors.append("PLN-DB-REVIEW non deve bloccare la RC verified_internally")
        for field in ("description", "promotion_condition"):
            if not review.get(field):
                errors.append(f"PLN-DB-REVIEW deve dichiarare {field}")

    dependencies = keyed_items(document, "external_dependencies", "id", errors)
    missing_dependencies = REQUIRED_EXTERNAL_DEPENDENCIES - dependencies.keys()
    if missing_dependencies:
        errors.append(
            "external_dependencies mancanti: "
            + ", ".join(sorted(missing_dependencies))
        )
    for identity, dependency in dependencies.items():
        for field in ("owner", "description", "exit_condition", "component_rc_effect"):
            if not dependency.get(field):
                errors.append(f"dipendenza {identity} senza {field}")

    reductions = keyed_items(document, "declared_scope_reductions", "area", errors)
    missing_reductions = REQUIRED_SCOPE_REDUCTIONS - reductions.keys()
    if missing_reductions:
        errors.append(
            "declared_scope_reductions mancanti: "
            + ", ".join(sorted(missing_reductions))
        )
    for area, reduction in reductions.items():
        for field in ("scope", "runtime_policy", "exit_condition"):
            if not reduction.get(field):
                errors.append(f"riduzione {area} senza {field}")
        assurance = reduction.get("assurance")
        if not isinstance(assurance, dict):
            errors.append(f"riduzione {area} senza assurance")
            continue
        expected_status = EXPECTED_ASSURANCE_STATUS.get(area)
        status = assurance.get("status")
        if status != expected_status:
            errors.append(
                f"riduzione {area} con assurance.status {status!r}; "
                f"atteso {expected_status!r}"
            )

        verified = assurance.get("verified_live")
        if not isinstance(verified, list):
            errors.append(f"riduzione {area}: verified_live deve essere una lista")
            verified = []
        if status == "partially_verified_live" and not verified:
            errors.append(f"riduzione {area} senza prove verified_live")
        if status == "declared_only" and verified:
            errors.append(
                f"riduzione {area} declared_only non puo avere prove verified_live"
            )
        verified_tests: set[str] = set()
        for index, claim in enumerate(verified):
            if not isinstance(claim, dict):
                errors.append(
                    f"riduzione {area}: verified_live[{index}] deve essere un oggetto"
                )
                continue
            for field in ("claim", "evidence_id", "test"):
                if not claim.get(field):
                    errors.append(
                        f"riduzione {area}: verified_live[{index}] senza {field}"
                    )
            test = claim.get("test")
            if isinstance(test, str) and test:
                if test in verified_tests:
                    errors.append(
                        f"riduzione {area}: test verified_live duplicato {test}"
                    )
                verified_tests.add(test)
            if claim.get("evidence_id") != "sqlserver_reference":
                errors.append(
                    f"riduzione {area}: verified_live[{index}] deve riferire "
                    "sqlserver_reference"
                )
        missing_tests = REQUIRED_VERIFIED_TESTS.get(area, set()) - verified_tests
        unexpected_tests = verified_tests - REQUIRED_VERIFIED_TESTS.get(area, set())
        if missing_tests:
            errors.append(
                f"riduzione {area}: test verified_live mancanti: "
                + ", ".join(sorted(missing_tests))
            )
        if unexpected_tests:
            errors.append(
                f"riduzione {area}: test verified_live inattesi: "
                + ", ".join(sorted(unexpected_tests))
            )

        unverified = assurance.get("declared_not_verified_live")
        if not isinstance(unverified, list):
            errors.append(
                f"riduzione {area}: declared_not_verified_live deve essere una lista"
            )
            unverified = []
        claim_ids: set[str] = set()
        for index, claim in enumerate(unverified):
            if not isinstance(claim, dict):
                errors.append(
                    f"riduzione {area}: declared_not_verified_live[{index}] "
                    "deve essere un oggetto"
                )
                continue
            claim_id = claim.get("claim_id")
            if not isinstance(claim_id, str) or not claim_id:
                errors.append(
                    f"riduzione {area}: declared_not_verified_live[{index}] "
                    "senza claim_id"
                )
            elif claim_id in claim_ids:
                errors.append(f"riduzione {area}: claim_id duplicato {claim_id}")
            else:
                claim_ids.add(claim_id)
            if not claim.get("claim"):
                errors.append(
                    f"riduzione {area}: declared_not_verified_live[{index}] "
                    "senza claim"
                )
        missing_claims = REQUIRED_UNVERIFIED_CLAIMS.get(area, set()) - claim_ids
        if missing_claims:
            errors.append(
                f"riduzione {area}: claim non verificati mancanti: "
                + ", ".join(sorted(missing_claims))
            )

    evidence = keyed_items(document, "evidence", "id", errors)
    missing_evidence = REQUIRED_EVIDENCE - evidence.keys()
    if missing_evidence:
        errors.append("evidenze mancanti: " + ", ".join(sorted(missing_evidence)))
    for identity in REQUIRED_EVIDENCE & evidence.keys():
        item = evidence[identity]
        if item.get("status") != "passed":
            errors.append(f"evidenza {identity} non passed")
        if revision is not None and item.get("revision") != revision:
            errors.append(f"evidenza {identity} non fissata alla baseline")
        run_id = item.get("run_id")
        if not isinstance(run_id, int) or run_id <= 0:
            errors.append(f"evidenza {identity} senza run_id positivo")
            continue
        expected_url = (
            "https://github.com/PlenoraETL/plenora-database-tools/"
            f"actions/runs/{run_id}"
        )
        if item.get("url") != expected_url:
            errors.append(f"evidenza {identity} con URL incoerente")
    for area, reduction in reductions.items():
        assurance = reduction.get("assurance")
        if not isinstance(assurance, dict):
            continue
        verified = assurance.get("verified_live")
        if not isinstance(verified, list):
            continue
        for index, claim in enumerate(verified):
            if not isinstance(claim, dict):
                continue
            evidence_id = claim.get("evidence_id")
            if evidence_id not in evidence:
                errors.append(
                    f"riduzione {area}: verified_live[{index}] riferisce "
                    f"evidenza assente {evidence_id!r}"
                )
            elif evidence[evidence_id].get("status") != "passed":
                errors.append(
                    f"riduzione {area}: verified_live[{index}] riferisce "
                    f"evidenza non passed {evidence_id!r}"
                )

    release_action = document.get("release_action")
    if not isinstance(release_action, dict):
        errors.append("release_action deve essere un oggetto")
    else:
        expected_allowed = decision in ("ready", "tagged")
        if release_action.get("allowed") is not expected_allowed:
            errors.append(
                "release_action.allowed deve essere "
                f"{str(expected_allowed).lower()}"
            )
        expected_tag = (
            "v0.1.0-rc.1" if decision in ("ready", "tagged") else None
        )
        if release_action.get("tag") != expected_tag:
            errors.append(f"release_action.tag deve essere {expected_tag!r}")
        expected_created = decision == "tagged"
        if release_action.get("tag_created") is not expected_created:
            errors.append(
                "release_action.tag_created deve essere "
                f"{str(expected_created).lower()}"
            )
        if not release_action.get("reason"):
            errors.append("release_action.reason deve essere dichiarato")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--json-out", type=Path)
    arguments = parser.parse_args()
    try:
        document = json.loads(arguments.manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"rc1 readiness: manifesto non leggibile: {error}")
        return 1
    if not isinstance(document, dict):
        print("rc1 readiness: il manifesto deve essere un oggetto")
        return 1
    errors = check(document, arguments.repo.resolve())
    status = "pass" if not errors else "fail"
    for error in errors:
        print(f"ERRORE {error}")
    print(f"rc1 readiness: {status} ({len(errors)} errori)")
    if arguments.json_out:
        arguments.json_out.parent.mkdir(parents=True, exist_ok=True)
        arguments.json_out.write_text(
            json.dumps(
                {"status": status, "errors": errors},
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
