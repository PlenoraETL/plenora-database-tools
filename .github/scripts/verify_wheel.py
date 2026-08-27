#!/usr/bin/env python3
"""Il wheel appena costruito, verificato sulla piattaforma che lo ha prodotto.

Un wheel che si costruisce non e un wheel che funziona, e le tre cose che
possono rompersi non si vedono nell'output di `maturin build`:

* il modulo nativo puo non caricarsi — linking, ABI, target sbagliato — e
  l'unico modo di saperlo e importarlo **su quella piattaforma**, non su
  quella comoda;
* la versione puo essere due: `importlib.metadata` legge quella del pacchetto,
  `p.version()` quella compilata nel modulo nativo. Devono coincidere;
* il tag della release puo non essere quello del pacchetto, e allora gli
  asset allegati a `py-v0.10.0` contengono `0.9.2` senza che nessuno lo veda.

Gira dentro i job di build **prima** dell'upload degli artifact: un wheel
rotto non deve nemmeno diventare scaricabile.
"""

from __future__ import annotations

import importlib.metadata as metadata
import os
import sys
from pathlib import Path

PACKAGE = "plenora-database"
TAG_PREFIX = "py-v"


def fail(message: str) -> int:
    print(f"verify-wheel: {message}", file=sys.stderr)
    return 1


def main() -> int:
    # L'import e la prima cosa che il wheel deve saper fare, e la sola che
    # esercita davvero il caricamento del modulo nativo.
    import plenora_database as p

    origin = Path(p.__file__).resolve()
    workspace = os.environ.get("GITHUB_WORKSPACE")
    if workspace and origin.is_relative_to(Path(workspace).resolve()):
        return fail(
            f"il package importato viene dal checkout ({origin}), non dal "
            "wheel installato: la verifica non direbbe niente sull'artefatto"
        )

    declared = metadata.version(PACKAGE)
    native = p.version()
    if native != declared:
        return fail(
            f"il wheel dichiara {declared} e il modulo nativo risponde "
            f"{native}: sono la versione di pyproject.toml e quella del "
            "Cargo.toml del crate, e devono coincidere"
        )

    if os.environ.get("GITHUB_EVENT_NAME") == "release":
        expected = f"{TAG_PREFIX}{declared}"
        reference = os.environ.get("GITHUB_REF_NAME", "")
        if reference != expected:
            return fail(
                f"la release e taggata {reference!r} ma il wheel e {declared}: "
                f"il tag atteso e {expected!r}"
            )

    # L'import da solo non prova che i re-export pubblici siano completi.
    if not hasattr(p.Session, "execute"):
        return fail("Session senza execute: la superficie pubblica non e completa")
    if not hasattr(p, "PlenoraError"):
        return fail("PlenoraError assente dall'import top-level")

    print(
        f"verify-wheel: {PACKAGE} {declared} — import ok, "
        f"p.version() {native}, origine {origin}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
