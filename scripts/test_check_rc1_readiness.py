#!/usr/bin/env python3
"""Prova positiva e mutazioni negative del gate RC1 readiness."""

from __future__ import annotations

import importlib.util
import subprocess
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
GATE = ROOT / "scripts" / "check_rc1_readiness.py"
SPEC = importlib.util.spec_from_file_location("rc1_gate", GATE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("checker RC1 non importabile")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def head_revision() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def evidence(identity: str, revision: str, run_id: int) -> dict:
    return {
        "id": identity,
        "status": "passed",
        "revision": revision,
        "run_id": run_id,
        "url": (
            "https://github.com/PlenoraETL/plenora-database-tools/"
            f"actions/runs/{run_id}"
        ),
    }


def conforming(revision: str) -> dict:
    reductions = []
    for area in sorted(gate.REQUIRED_SCOPE_REDUCTIONS):
        status = gate.EXPECTED_ASSURANCE_STATUS[area]
        verified = [
            {
                "claim": "prova live limitata",
                "evidence_id": "sqlserver_reference",
                "test": test,
            }
            for test in sorted(gate.REQUIRED_VERIFIED_TESTS[area])
        ]
        reductions.append(
            {
                "area": area,
                "scope": "fuori perimetro",
                "runtime_policy": "fail_closed",
                "exit_condition": "campagna dedicata",
                "assurance": {
                    "status": status,
                    "verified_live": verified,
                    "declared_not_verified_live": [
                        {"claim_id": claim_id, "claim": "non provato live"}
                        for claim_id in sorted(
                            gate.REQUIRED_UNVERIFIED_CLAIMS[area]
                        )
                    ],
                },
            }
        )
    return {
        "manifest_version": 1,
        "component": "plenora-database-tools",
        "component_version": "0.1.0-rc.1",
        "status": "rc1_rebaseline_pending",
        "revision": revision,
        "verification_claim": "verified_internally",
        "independent_review": False,
        "claims": {
            "component_rc": False,
            "system_rc": False,
            "avionic_certification": False,
        },
        "candidate": {"decision": "rebaseline_pending", "code_freeze": False},
        "evidence": [
            evidence("postgres_reference", revision, 1),
            evidence("sqlserver_reference", revision, 2),
            evidence("workspace_coverage", revision, 3),
            evidence("release_manifest", revision, 4),
        ],
        "component_rc_blockers": [
            {
                "id": "PLN-DB-REBASELINE",
                "description": "rebaseline richiesto",
                "exit_condition": "nuova evidenza registrata",
            }
        ],
        "open_assurance_attributes": [
            {
                "id": "PLN-DB-REVIEW",
                "status": "pending_eligible_reviewer",
                "blocks_component_rc_release": False,
                "description": "review non eseguita",
                "promotion_condition": "review registrata",
            }
        ],
        "external_dependencies": [
            {
                "id": identity,
                "owner": "external",
                "description": "dipendenza",
                "exit_condition": "ratifica",
                "component_rc_effect": "non blocca il componente",
            }
            for identity in sorted(gate.REQUIRED_EXTERNAL_DEPENDENCIES)
        ],
        "declared_scope_reductions": reductions,
        "release_action": {
            "allowed": False,
            "tag": None,
            "tag_created": False,
            "reason": "rebaseline non completato",
        },
    }


def main() -> int:
    revision = head_revision()
    base = conforming(revision)
    cases: list[tuple[str, dict, str | None]] = [("conforme", base, None)]

    missing_evidence = deepcopy(base)
    missing_evidence["evidence"].pop()
    cases.append(("evidenza mancante", missing_evidence, "evidenze mancanti"))

    failed_evidence = deepcopy(base)
    failed_evidence["evidence"][0]["status"] = "failed"
    cases.append(("evidenza fallita", failed_evidence, "non passed"))

    wrong_revision = deepcopy(base)
    wrong_revision["evidence"][0]["revision"] = "0" * 40
    cases.append(("baseline divergente", wrong_revision, "non fissata"))

    wrong_url = deepcopy(base)
    wrong_url["evidence"][0]["url"] = "https://example.invalid/run"
    cases.append(("URL incoerente", wrong_url, "URL incoerente"))

    no_review_attribute = deepcopy(base)
    no_review_attribute["open_assurance_attributes"] = []
    cases.append(
        (
            "review omessa",
            no_review_attribute,
            "PLN-DB-REVIEW deve restare un attributo",
        )
    )

    premature_claim = deepcopy(base)
    premature_claim["claims"]["component_rc"] = True
    cases.append(
        (
            "claim prematuro",
            premature_claim,
            "component_rc deve essere false durante il rebaseline",
        )
    )

    system_claim = deepcopy(base)
    system_claim["claims"]["system_rc"] = True
    cases.append(("system claim", system_claim, "system_rc deve essere false"))

    missing_dependency = deepcopy(base)
    missing_dependency["external_dependencies"].pop()
    cases.append(
        ("dipendenza omessa", missing_dependency, "external_dependencies mancanti")
    )

    missing_reduction = deepcopy(base)
    missing_reduction["declared_scope_reductions"].pop()
    cases.append(
        ("scope omesso", missing_reduction, "declared_scope_reductions mancanti")
    )

    missing_assurance = deepcopy(base)
    del missing_assurance["declared_scope_reductions"][0]["assurance"]
    cases.append(("assurance omessa", missing_assurance, "senza assurance"))

    missing_unverified_claim = deepcopy(base)
    missing_unverified_claim["declared_scope_reductions"][0]["assurance"][
        "declared_not_verified_live"
    ].pop()
    cases.append(
        (
            "claim non verificato omesso",
            missing_unverified_claim,
            "claim non verificati mancanti",
        )
    )

    unknown_evidence = deepcopy(base)
    partial = next(
        reduction
        for reduction in unknown_evidence["declared_scope_reductions"]
        if reduction["assurance"]["status"] == "partially_verified_live"
    )
    partial["assurance"]["verified_live"][0]["evidence_id"] = "missing"
    cases.append(
        ("prova senza evidenza", unknown_evidence, "riferisce evidenza assente")
    )

    fictitious_test = deepcopy(base)
    partial = next(
        reduction
        for reduction in fictitious_test["declared_scope_reductions"]
        if reduction["assurance"]["status"] == "partially_verified_live"
    )
    partial["assurance"]["verified_live"][0]["test"] = "live_nonexistent"
    cases.append(
        ("test live fittizio", fictitious_test, "test verified_live mancanti")
    )

    release_allowed = deepcopy(base)
    release_allowed["release_action"]["allowed"] = True
    cases.append(
        ("release prematura", release_allowed, "release_action.allowed deve essere false")
    )

    failures: list[str] = []
    for label, document, expected in cases:
        errors = gate.check(document, ROOT)
        if expected is None and errors:
            failures.append(f"{label}: errori inattesi {errors}")
        elif expected is not None and not any(expected in error for error in errors):
            failures.append(f"{label}: atteso {expected!r}, ottenuto {errors}")
    with patch.object(gate, "pending_packaging_delta_matches", return_value=False):
        errors = gate.check(base, ROOT)
    if not any("soltanto il delta SemVer Cargo" in error for error in errors):
        failures.append(f"delta packaging divergente: errore non rilevato {errors}")
    total = len(cases) + 1
    print(f"{total - len(failures)}/{total} verifiche superate")
    for failure in failures:
        print(f"FALLITO {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
