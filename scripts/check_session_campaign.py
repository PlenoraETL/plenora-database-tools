#!/usr/bin/env python3
"""Campagna live della matrice di sessione: fixture, misura, verdetto.

La matrice e una prova permanente solo se qualcuno la riesegue. Il self-test
statico verifica il **giudizio** del runner su documenti costruiti a mano, e
questo e cio che serve su una pull request; ma se domani cambiasse
`SESSION_BOOTSTRAP_SQL` o `START TRANSACTION`, il documento resterebbe quello
di ieri e nessun controllo se ne accorgerebbe. Solo una corsa contro i tre
server reali lo scopre.

Il ciclo di vita non sta qui ma in `scripts/fixture_campaign.py`, che due
campagne condividono: le condizioni — quali fixture, in quale ordine, cosa
aspettare — sono decisioni scritte una volta sola, e ognuna di quelle righe e
stata aggiunta dopo che una corsa reale e morta senza. Cio che resta qui e
solo cio che riguarda **questa** misura.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.check_session_matrix import (  # noqa: E402
    EVIDENCE,
    markdown,
    preflight,
    verdict,
)
from scripts.fixture_campaign import campaign  # noqa: E402

# I due compose che accendono i tre riferimenti della matrice. L'ordine non
# conta — sono reti separate — ma l'elenco si: una campagna che ne avviasse
# uno solo misurerebbe due server su tre e il runner fallirebbe sul digest,
# che e il modo giusto di accorgersene ma non il piu chiaro.
COMPOSE_FILES = ("docker-compose.mysql.yml", "docker-compose.mariadb.yml")


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

    EVIDENCE.write_text(markdown(document), encoding="utf-8", newline="\n")
    print(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=1))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except RuntimeError as error:
        print(f"session campaign FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
