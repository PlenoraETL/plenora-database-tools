#!/usr/bin/env python3
"""Verifica il pacchetto di readiness della RC1 di componente.

Questo gate non sostituisce la revisione indipendente. Impedisce invece che un
candidato bloccato venga presentato come RC, che l'evidenza punti a revisioni
diverse dalla baseline o che una riduzione di scope sparisca dal manifesto.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
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
    revision = document.get("revision")
    if not isinstance(revision, str) or not REVISION.fullmatch(revision):
        errors.append("revision deve contenere 40 cifre esadecimali")
        revision = None
    elif not git_success(repository, ["cat-file", "-e", f"{revision}^{{commit}}"]):
        errors.append("revision non esiste nella storia locale")
    elif not git_success(repository, ["merge-base", "--is-ancestor", revision, "HEAD"]):
        errors.append("revision non e un antenato del checkout corrente")
    elif not production_tree_matches(repository, revision):
        errors.append("i percorsi di produzione divergono dalla baseline congelata")

    if document.get("manifest_version") != 1:
        errors.append("manifest_version deve essere 1")
    if document.get("component") != "plenora-database-tools":
        errors.append("component deve essere plenora-database-tools")
    if document.get("component_version") != "0.1.0-rc.1":
        errors.append("component_version deve essere 0.1.0-rc.1")
    if document.get("status") != "rc1_candidate_blocked":
        errors.append("status deve essere rc1_candidate_blocked")
    if document.get("verification_claim") != "verified_internally":
        errors.append("il candidato bloccato puo dichiarare solo verified_internally")
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

    candidate = document.get("candidate")
    if not isinstance(candidate, dict):
        errors.append("candidate deve essere un oggetto")
        candidate = {}
    decision = candidate.get("decision")
    if decision not in ("blocked", "ready"):
        errors.append("candidate.decision deve essere blocked oppure ready")
    if candidate.get("code_freeze") is not True:
        errors.append("candidate.code_freeze deve essere true")

    blockers = keyed_items(document, "component_rc_blockers", "id", errors)
    if decision == "blocked":
        if claims.get("component_rc") is not False:
            errors.append("component_rc deve essere false mentre il candidato e bloccato")
        if "PLN-DB-REVIEW" not in blockers:
            errors.append("il candidato bloccato deve dichiarare PLN-DB-REVIEW")
        else:
            review = blockers["PLN-DB-REVIEW"]
            for field in ("description", "exit_condition"):
                if not review.get(field):
                    errors.append(f"PLN-DB-REVIEW deve dichiarare {field}")
    elif decision == "ready":
        if blockers:
            errors.append("un candidato ready non puo avere component_rc_blockers")
        if claims.get("component_rc") is not True:
            errors.append("un candidato ready deve dichiarare component_rc true")
        errors.append(
            "la transizione ready richiede un manifesto separato con review "
            "indipendente registrata; questo file e deliberatamente bloccato"
        )

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

    release_action = document.get("release_action")
    if not isinstance(release_action, dict):
        errors.append("release_action deve essere un oggetto")
    else:
        if release_action.get("allowed") is not False:
            errors.append("release_action.allowed deve essere false")
        if release_action.get("tag") is not None:
            errors.append("release_action.tag deve essere null")
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
