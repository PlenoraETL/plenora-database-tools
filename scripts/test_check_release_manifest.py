#!/usr/bin/env python3
"""Dimostra che ogni controllo automatizzato del manifesto scatta davvero."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
GATE = ROOT / "check_release_manifest.py"
SPEC = importlib.util.spec_from_file_location("gate", GATE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("checker del manifesto non importabile")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)

CONFORMING = {
    "manifest_version": 1,
    "component": "plenora-example-tools",
    "component_version": "0.1.0-rc.1",
    "revision": None,
    "verification_claim": "verified_internally",
    "independent_review": False,
    "claims": {
        "component_rc": True,
        "system_rc": False,
        "avionic_certification": False,
    },
}


def without(document: dict, field: str) -> dict:
    return {key: value for key, value in document.items() if key != field}


def with_claims(document: dict, **changes) -> dict:
    return {**document, "claims": {**document["claims"], **changes}}


def build_cases(revision: str) -> list[tuple[str, dict, str | None]]:
    """Restituisce etichetta, manifesto e criterio atteso; None deve passare."""
    ok = {**CONFORMING, "revision": revision}
    return [
        ("conforme", ok, None),
        (
            "system_rc del componente",
            with_claims(ok, system_rc=True),
            "C3.1",
        ),
        (
            "conformita avionica",
            with_claims(ok, avionic_certification=True),
            "C4.4",
        ),
        (
            "claim assente",
            {
                **ok,
                "claims": {"component_rc": True, "system_rc": False},
            },
            "C1.3",
        ),
        (
            "claim non booleano",
            with_claims(ok, component_rc="si"),
            "C1.3",
        ),
        ("revisione inesistente", {**ok, "revision": "0" * 40}, "C2.2"),
        ("revisione abbreviata", {**ok, "revision": revision[:7]}, "C1.2"),
        (
            "claim verifica assente",
            without(ok, "verification_claim"),
            "C4.2",
        ),
        (
            "indipendente senza revisione",
            {**ok, "verification_claim": "verified_independently"},
            "C4.2",
        ),
        (
            "claim verifica invalido",
            {**ok, "verification_claim": "verificato_a_occhio"},
            "C4.2",
        ),
        (
            "campo obbligatorio assente",
            without(ok, "component_version"),
            "C1.2",
        ),
        (
            "manifest_version non intero",
            {**ok, "manifest_version": "1"},
            "C1.2",
        ),
        (
            "nessuna revisione",
            without(ok, "revision"),
            "C1.2",
        ),
    ]


def head_revision(repository: Path) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def main() -> int:
    repository = ROOT.parent
    revision = head_revision(repository)
    if revision is None:
        print("git non interrogabile: C2.2 non verificabile", file=sys.stderr)
        return 2

    failures: list[str] = []
    cases = build_cases(revision)
    with tempfile.TemporaryDirectory(prefix="plenora-release-gate-") as directory:
        workspace = Path(directory)
        for index, (label, manifest, expected) in enumerate(cases):
            path = workspace / f"{index:02d}.json"
            path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            errors, _ = gate.check({path.name: manifest}, repository)
            if expected is None:
                if errors:
                    failures.append(f"{label}: errori inattesi {errors}")
                continue
            if not any(error.startswith(expected) for error in errors):
                failures.append(
                    f"{label}: atteso {expected}, ottenuti {errors or 'nessuno'}"
                )

    print(f"{len(cases) - len(failures)}/{len(cases)} verifiche superate")
    for failure in failures:
        print(f"FALLITO {failure}")
    print(
        "non automatizzati per scelta: "
        "C2.1, C2.3, C3.2, C4.3, C5.1, C5.2"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
