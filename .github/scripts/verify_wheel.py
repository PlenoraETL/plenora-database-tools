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
DB2_RUNTIME_ENV = "PLENORA_EXPECT_DB2_RUNTIME"


def fail(message: str) -> int:
    print(f"verify-wheel: {message}", file=sys.stderr)
    return 1


def verify_db2_profile(package: object) -> str | None:
    """Distingue il wheel standard dallo specifico artefatto DB2.

    Un valore TLS invalido ferma la feature reale prima di aprire ODBC. Lo
    stub standard deve invece rispondere con l'errore Unsupported tipizzato.
    """

    expect_runtime = os.environ.get(DB2_RUNTIME_ENV) == "1"
    try:
        package.connect_db2(
            "localhost",
            "wheel_probe",
            "wheel_probe",
            "wheel_probe",
            tls_mode="wheel_probe_invalid",
        )
    except package.PlenoraUnsupportedError:
        if expect_runtime:
            return "feature DB2 richiesta ma il wheel contiene lo stub"
        return None
    except RuntimeError as error:
        if not expect_runtime:
            return "il wheel standard contiene il runtime DB2"
        if "tls_mode Db2 non riconosciuto" not in str(error):
            return "la feature DB2 non ha raggiunto la validazione nativa attesa"
        return None
    except Exception as error:  # pragma: no cover - diagnostica del gate
        return f"profilo DB2 inatteso: {type(error).__name__}"
    return "la probe DB2 ha tentato una connessione con TLS invalido"


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
    if db2_error := verify_db2_profile(p):
        return fail(db2_error)

    print(
        f"verify-wheel: {PACKAGE} {declared} — import ok, "
        f"p.version() {native}, origine {origin}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
