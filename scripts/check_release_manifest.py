#!/usr/bin/env python3
"""Verifica i manifesti contro PLENORA-CRITERI-RC.md.

Copre C1, C2.2, C3.1, C4.2 e C4.4. I criteri che richiedono un
giudizio umano restano dichiarati come non automatizzati.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


CRITERIA_DOCUMENT = "PLENORA-CRITERI-RC.md"
REVISION = re.compile(r"^[0-9a-f]{40}$")
CLAIM_KEYS = ("component_rc", "system_rc", "avionic_certification")
VERIFICATION_CLAIMS = ("verified_internally", "verified_independently")
COMPONENT_PREFIX = "plenora-"
REVISION_KEYS = (
    "revision",
    "candidate_revision",
    "target_revision",
    "library_baseline_revision",
)
NOT_AUTOMATED = ("C2.1", "C2.3", "C3.2", "C4.3", "C5.1", "C5.2")


def find_revisions(
    document: object,
    component: str | None,
    path: str = "",
    foreign: bool = False,
) -> list[tuple[str, str, bool]]:
    """Restituisce revisione, percorso e appartenenza a un repository esterno."""
    found: list[tuple[str, str, bool]] = []
    if isinstance(document, dict):
        named = document.get("component") or document.get("repository")
        if (
            named is None
            and isinstance(document.get("name"), str)
            and document["name"].startswith(COMPONENT_PREFIX)
        ):
            named = document["name"]
        here_foreign = foreign or (
            isinstance(named, str)
            and component is not None
            and named != component
        )
        for key, value in document.items():
            here = f"{path}.{key}" if path else key
            key_foreign = here_foreign or key in ("icd", "external", "upstream")
            if key in REVISION_KEYS and isinstance(value, str):
                found.append((here, value, key_foreign))
            else:
                found.extend(
                    find_revisions(value, component, here, key_foreign)
                )
    elif isinstance(document, list):
        for index, value in enumerate(document):
            found.extend(
                find_revisions(value, component, f"{path}[{index}]", foreign)
            )
    return found


def revision_exists(repository: Path, revision: str) -> bool | None:
    """Verifica una revisione locale senza modificare il repository."""
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "cat-file",
                "-e",
                f"{revision}^{{commit}}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.returncode == 0


def check(
    manifests: dict[str, dict], repository: Path
) -> tuple[list[str], list[str]]:
    """Applica i controlli automatizzabili all'unione dei manifesti."""
    errors: list[str] = []
    warnings: list[str] = []

    def lookup(field: str):
        for name, document in manifests.items():
            if field in document:
                return document[field], name
        return None, None

    for field in ("manifest_version", "component", "component_version"):
        value, _ = lookup(field)
        if value is None:
            errors.append(
                f"C1.2: campo obbligatorio assente da tutti i manifesti: {field!r}"
            )
    version, source = lookup("manifest_version")
    if (
        version is not None
        and not isinstance(version, bool)
        and not isinstance(version, int)
    ):
        errors.append(
            f"C1.2: manifest_version in {source} deve essere un intero"
        )

    component, _ = lookup("component")
    component = component if isinstance(component, str) else None
    revisions: list[tuple[str, str, bool]] = []
    for name, document in manifests.items():
        revisions.extend(
            (f"{name}:{where}", value, foreign)
            for where, value, foreign in find_revisions(document, component)
        )

    own = [entry for entry in revisions if not entry[2]]
    if not own:
        errors.append(
            "C1.2: nessuna revisione propria dichiarata "
            f"(attese sotto una di {', '.join(REVISION_KEYS)})"
        )
    for where, value, _ in revisions:
        if not REVISION.fullmatch(value):
            errors.append(
                f"C1.2: {where} non e una revisione di 40 cifre esadecimali: "
                f"{value!r}"
            )

    claims, _ = lookup("claims")
    if claims is None:
        errors.append("C1.3: oggetto 'claims' assente da tutti i manifesti")
    elif not isinstance(claims, dict):
        errors.append("C1.3: 'claims' deve essere un oggetto")
    else:
        for key in CLAIM_KEYS:
            if key not in claims:
                errors.append(
                    f"C1.3: claims.{key} assente; l'assenza non vale false"
                )
            elif not isinstance(claims[key], bool):
                errors.append(f"C1.3: claims.{key} deve essere booleano")
        if claims.get("system_rc") is True:
            errors.append(
                "C3.1: system_rc non e dichiarabile da un componente"
            )
        if claims.get("avionic_certification") is True:
            errors.append(
                "C4.4: avionic_certification deve valere false"
            )

    claim, _ = lookup("verification_claim")
    independent, _ = lookup("independent_review")
    attributes, _ = lookup("assurance_attributes")
    if isinstance(attributes, dict):
        claim = attributes.get("verification_claim", claim)
        independent = attributes.get("independent_review", independent)
    if claim is None:
        errors.append("C4.2: verification_claim non dichiarato")
    elif claim not in VERIFICATION_CLAIMS:
        errors.append(
            f"C4.2: verification_claim {claim!r} fuori dai valori ammessi"
        )
    elif claim == "verified_independently" and independent is not True:
        errors.append(
            "C4.2: verified_independently senza independent_review true"
        )
    if claim is not None and independent is None:
        warnings.append(
            "C4.2: independent_review non dichiarato accanto al claim"
        )

    for where, value, foreign in revisions:
        if not REVISION.fullmatch(value):
            continue
        if foreign:
            warnings.append(
                f"C2.2: {where} appartiene a un altro repository; "
                "esistenza non verificabile da qui"
            )
            continue
        outcome = revision_exists(repository, value)
        if outcome is None:
            warnings.append(
                f"C2.2: git non interrogabile; {where} non verificata"
            )
        elif outcome is False:
            errors.append(
                f"C2.2: {where} = {value} non esiste nella storia locale"
            )
    return errors, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, nargs="+")
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--json-out", type=Path)
    arguments = parser.parse_args()

    manifests: dict[str, dict] = {}
    for path in arguments.manifest:
        if not path.is_file():
            print(f"manifesto non trovato: {path}", file=sys.stderr)
            return 1
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            print(f"C1.1: {path} non e JSON valido: {error}", file=sys.stderr)
            return 1
        if not isinstance(document, dict):
            print(f"C1.1: {path} deve essere un oggetto JSON", file=sys.stderr)
            return 1
        manifests[path.name] = document

    repository = arguments.repo
    if repository is None:
        repository = arguments.manifest[0].resolve().parent
        while (
            not (repository / ".git").exists()
            and repository != repository.parent
        ):
            repository = repository.parent

    errors, warnings = check(manifests, repository)
    status = "pass" if not errors else "fail"
    for warning in warnings:
        print(f"avviso   {warning}")
    for error in errors:
        print(f"ERRORE   {error}")
    print(
        f"\n{', '.join(manifests)}: {status} "
        f"({len(errors)} errori, {len(warnings)} avvisi) - "
        f"{CRITERIA_DOCUMENT}"
    )
    print(f"non automatizzati: {', '.join(NOT_AUTOMATED)}")

    if arguments.json_out:
        arguments.json_out.parent.mkdir(parents=True, exist_ok=True)
        arguments.json_out.write_text(
            json.dumps(
                {
                    "criteria_document": CRITERIA_DOCUMENT,
                    "manifests": [
                        str(path) for path in arguments.manifest
                    ],
                    "status": status,
                    "errors": errors,
                    "warnings": warnings,
                    "not_automated": list(NOT_AUTOMATED),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
