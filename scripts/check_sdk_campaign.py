#!/usr/bin/env python3
"""Campagna live del SDK Python: fixture, wheel, suite, verdetto.

`check_sdk_tests.py` sa costruire il wheel e il CLI, installarli fuori
dall'albero, eseguire la suite e confrontare la corsa con il contratto di
ciascuno scope. Pretende pero i due riferimenti gia accesi: in locale li ha,
su un runner pulito no. Finche e stato cosi, gli scope `live` e `benchmark`
non li eseguiva nessun workflow — e la sola prova automatica sul SDK era che
il wheel si importasse.

Non e un buco teorico. L'eccezione di `deny.toml` su pyo3 lo cita per nome:
la migrazione a 0.29 resta ferma perche una suite validata dal solo
compilatore non basta a giustificarla, e la copertura che servirebbe non
esisteva. Questa campagna e quella copertura.

Il ciclo di vita delle fixture sta in `scripts/fixture_campaign.py`, condiviso
con le campagne di sessione e dell'evidenza MariaDB. Qui restano le due cose
che riguardano questa misura: da dove deve partire, e cosa la fa fallire.

## Perche due riferimenti e non tre

Il SDK espone PostgreSQL e MySQL, e nient'altro: non dipende da
`plenora-db-sqlserver`, quindi accendere anche quel riferimento misurerebbe
un container che nessun test interroga. Quando il binding SQL Server
esistera, il compose si aggiunge qui e il contratto degli scope in
`check_sdk_tests.py` cresce di conseguenza.
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

# I due riferimenti che la suite interroga. L'ordine e quello in cui vengono
# accesi, e non conta: i due compose dichiarano progetti distinti e non si
# toccano fra loro.
COMPOSE_FILES = ("docker-compose.postgres.yml", "docker-compose.mysql.yml")


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
