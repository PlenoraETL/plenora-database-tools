#!/usr/bin/env python3
"""Un digest si fissa in un posto solo.

I riferimenti di questo repository sono fissati per digest, e la ragione e
scritta nei loro `references.json`: un digest non cambia mai contenuto, quindi
la riga e la **prova** della versione avviata invece di una promessa su quale
versione si otterra.

Quella prova vale finche c'e una fonte sola. Un digest ricopiato in un secondo
file e una fonte che invecchia da sola: il giorno in cui la riga originale
cambia — una patch nuova, una CVE — la copia resta indietro, e il gate che la
usa continua a misurare l'immagine vecchia dicendo di misurare quella
dichiarata.

# Il difetto che l'ha prodotta

`check_mysql_performance.py` portava scritto per esteso il digest di MySQL 8.4
LTS, che `docker/mysql/references.json` gia dichiara. I due coincidevano — la
copia era corretta — e questo e il punto: una copia sbagliata la si vede, una
copia giusta no, finche una delle due non si muove.

E' la stessa forma del registro dei test della matrice SQL Server, rimasto a
quarantaquattro mentre la sorgente arrivava a quarantotto, e la si e vista solo
il giorno in cui qualcuno ha eseguito quel gate.

# Cosa pretende

Che nessun digest compaia due volte. Se un file ne ha bisogno, lo legge dal
`references.json` che lo dichiara.

# Cosa **non** copre

I digest scritti senza il prefisso `sha256:`. `check_sqlserver_matrix.py` tiene
i suoi come esadecimale nudo e ricompone l'immagine con una `format!`: la
guardia non li vede, e allargare l'espressione a ogni stringa di
sessantaquattro esadecimali pescherebbe qualunque altro sha256 del repository.
E' un limite dichiarato, non una svista — e quei due riferimenti sono comunque
una fonte, non una copia.
"""

from __future__ import annotations

import re
import unittest
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


class EveryDigestHasOneHome(unittest.TestCase):
    #: I file che **dichiarano** i riferimenti: sono la fonte, e il digest ci
    #: sta per definizione.
    SOURCES = {
        "docker/mysql/references.json",
        "docker/mariadb/references.json",
    }

    #: Dove un digest puo comparire pur non essendo una fonte, e perche.
    DECLARED_COPIES = {
        "docker-compose.mysql.yml": "il Compose avvia l'immagine e il gate verifica che coincida con references.json",
        "docker-compose.mariadb.yml": "stessa ragione del Compose MySQL",
        "docker-compose.sqlserver.yml": "SQL Server non ha un references.json: il Compose e la fonte, e il gate lo legge da li",
        "docker-compose.postgres.yml": "PostgreSQL non ha un references.json: il Compose e la fonte",
        "docker-compose.postgres-tls.yml": "stessa fixture del Compose PostgreSQL",
        "scripts/check_postgres_matrix.py": "i cinque riferimenti PostGIS non hanno un references.json: la matrice e la loro dichiarazione",
        "scripts/check_sqlserver_reference.py": "verifica che l'immagine avviata sia quella del Compose, e per farlo deve nominarla",
    }

    def digests_by_file(self) -> dict[str, set[str]]:
        trovati: dict[str, set[str]] = defaultdict(set)
        for path in ROOT.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".json", ".yml", ".yaml", ".rs", ".toml"}:
                continue
            relative = path.relative_to(ROOT).as_posix()
            if relative.startswith(("target/", ".git/")):
                continue
            found = set(DIGEST.findall(path.read_text(encoding="utf-8", errors="replace")))
            if found:
                trovati[relative] = found
        return trovati

    def test_no_digest_is_copied_outside_its_source(self) -> None:
        per_file = self.digests_by_file()
        self.assertTrue(per_file, "nessun digest trovato: la guardia non sta guardando nulla")

        dichiarati: set[str] = set()
        for source in self.SOURCES:
            dichiarati |= per_file.get(source, set())
        self.assertTrue(dichiarati, "i references.json non dichiarano digest")

        copie: list[str] = []
        for relative, digests in sorted(per_file.items()):
            if relative in self.SOURCES or relative in self.DECLARED_COPIES:
                continue
            ripetuti = sorted(digests & dichiarati)
            if ripetuti:
                copie.append(f"{relative}: {[d[:19] + '...' for d in ripetuti]}")

        self.assertEqual(
            copie,
            [],
            "questi file ricopiano un digest che un references.json gia "
            f"dichiara: {copie}. O lo leggono da li, o dichiarano qui perche "
            "ne tengono una copia.",
        )

    def test_no_declared_copy_has_disappeared(self) -> None:
        """Una copia dichiarata e sparita e una dichiarazione scaduta."""

        per_file = self.digests_by_file()
        assenti = sorted(
            name for name in self.DECLARED_COPIES if name not in per_file
        )
        self.assertEqual(
            assenti,
            [],
            f"questi file sono dichiarati come copie ma non portano digest: {assenti}",
        )


if __name__ == "__main__":
    unittest.main()
