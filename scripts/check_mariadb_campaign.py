#!/usr/bin/env python3
"""Campagna live dell'evidenza MariaDB: fixture, misura, verdetto, uscita.

Il runner `check_mariadb_driver.py` sa misurare e sa giudicare, ma pretende
tre server gia accesi: in locale li ha, su un runner pulito no. Finche e stato
cosi, sessantadue sonde — e i contratti che le rendono prove — erano
presidiate solo da chi si ricordava di lanciarle.

Il punto 3 della fase 3 comincia ad aprire capability di **scrittura**. Ogni
prova nuova deve entrare subito in una campagna che gira su runner puliti, non
recuperare quella garanzia poco prima di esporre il provider: e la differenza
fra una misura che qualcuno rifa e una che qualcuno ricorda.

Il ciclo di vita delle fixture sta in `scripts/fixture_campaign.py`, condiviso
con la campagna di sessione. Qui restano le due cose che riguardano questa
misura: da dove deve partire, e cosa la fa fallire.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.check_mariadb_driver import (  # noqa: E402
    gate_violations,
    repository_state,
    verdict,
)
from scripts.fixture_campaign import campaign  # noqa: E402

# Gli stessi due compose della campagna di sessione: i riferimenti sono gli
# stessi tre, ed e l'unica cosa che le due misure hanno in comune oltre al
# ciclo di vita.
COMPOSE_FILES = ("docker-compose.mysql.yml", "docker-compose.mariadb.yml")


def preflight() -> str:
    """Pretende un albero pulito e restituisce il commit da cui si parte.

    Il verdetto registra il commit misurato, e con l'albero sporco quel commit
    non descrive il codice che ha prodotto i numeri. Il controllo sta prima di
    Docker: scoprirlo con tre server accesi e uno spreco, e renderebbe falsa
    l'affermazione che precede Docker.

    # Raises

    `RuntimeError` se l'albero ha modifiche non committate.
    """

    state = repository_state()
    if state["worktree_dirty"]:
        changes = state["worktree_changes"]
        raise RuntimeError(
            "albero con modifiche non committate: "
            + ", ".join(change.strip() for change in changes[:5])
            + (" ..." if len(changes) > 5 else "")
            + " — la misura deve partire da HEAD pulito"
        )
    return str(state["commit"])


def main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--keep-fixtures",
        action="store_true",
        help="lascia i container accesi (utile in locale fra due corse)",
    )
    parser.add_argument(
        "--diagnostics",
        type=Path,
        default=None,
        help="cartella in cui salvare log e stato dei container",
    )
    parsed = parser.parse_args(arguments)

    document = campaign(
        compose_files=COMPOSE_FILES,
        preflight=preflight,
        measure=verdict,
        diagnostics_directory=parsed.diagnostics,
        keep_fixtures=parsed.keep_fixtures,
    )

    print(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=1))

    # Il verdetto si stampa comunque — anche una corsa che fallisce e una
    # misura — ma l'uscita dice se cio che il profilo dichiara ha ancora una
    # prova sotto. E la stessa regola del runner, applicata dove la campagna
    # decide se il gate e verde.
    violations = gate_violations(document)
    if violations:
        print(
            "mariadb evidence campaign FAILED: l'inventario delle sonde o una prova "
            "necessaria non regge piu",
            file=sys.stderr,
        )
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except RuntimeError as error:
        print(f"mariadb evidence campaign FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
