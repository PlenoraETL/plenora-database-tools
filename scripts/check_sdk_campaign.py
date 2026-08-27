#!/usr/bin/env python3
"""Campagna live del SDK Python: fixture, wheel, suite, verdetto.

`check_sdk_tests.py` costruisce wheel e CLI, li installa fuori dall'albero ed
esegue gli scope dichiarati. Questa campagna gli fornisce i quattro riferimenti
live e registra un verdetto riproducibile; la copertura e necessaria anche per
valutare l'eccezione pyo3 dichiarata in `deny.toml`.

Il ciclo di vita delle fixture sta in `scripts/fixture_campaign.py`, condiviso
con le campagne di sessione e dell'evidenza MariaDB. Qui restano le due cose
che riguardano questa misura: da dove deve partire, e cosa la fa fallire.

La campagna accende un riferimento per ciascuna factory pubblica:
PostgreSQL, MySQL, MariaDB e SQL Server. MariaDB usa la riga principale del
suo ciclo di evidenza; le righe di compatibilita restano responsabilita del
gate del provider.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.check_sdk_tests import measure_live_scopes, preflight  # noqa: E402
from scripts.fixture_campaign import campaign  # noqa: E402

# I quattro riferimenti che la suite interroga. L'ordine e quello in cui vengono
# accesi, e non conta: i compose dichiarano progetti distinti e non si
# toccano fra loro.
COMPOSE_FILES = (
    "docker-compose.postgres.yml",
    "docker-compose.mysql.yml",
    "docker-compose.mariadb.yml",
    "docker-compose.sqlserver.yml",
)


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
        measure=measure_live_scopes,
        diagnostics_directory=parsed.diagnostics,
        keep_fixtures=parsed.keep_fixtures,
    )

    print(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=1))
    return 0


if __name__ == "__main__":
    # Il verdetto di questa campagna non ha una parte "misurata ma rossa": il
    # contratto di ogni scope e verificato dentro `measure_live_scopes`, che
    # solleva invece di restituire un documento con dentro un fallimento. Chi
    # arriva alla stampa ha gia passato tutto, e l'unica uscita diversa da
    # zero e quella di un'eccezione.
    try:
        raise SystemExit(main(sys.argv[1:]))
    except RuntimeError as error:
        print(f"sdk campaign FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
