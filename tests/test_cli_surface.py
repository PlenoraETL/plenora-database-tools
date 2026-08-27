#!/usr/bin/env python3
"""Ogni comando del CLI dice a quale delle tre famiglie appartiene.

Le differenze tra superfici generiche, strumenti di prova e comandi specifici
di un prodotto devono essere dichiarate, non dedotte da un conteggio.

# Le tre famiglie

**Generica.** `database-*`: un comando, un argomento che nomina il provider,
tutti e quattro i motori. E' la direzione, ed e gia scritta accanto a
`database_inspect` nel sorgente: «gli adapter implementano lo stesso contratto,
e una quarta copia degli stessi comandi diverge alla prima correzione applicata
a una sola».

**Banco di prova.** `benchmark-*`, `test-*`, `doctor`, `diagnose`,
`pool-status`, `profile-*`: non sono superficie di prodotto, sono gli attrezzi
con cui questo repository misura se stesso. Che vivano su PostgreSQL soltanto
non e un'asimmetria da colmare — e dove il banco e stato costruito.

**Specifica di prodotto.** Comandi con il prefisso di un motore. Ognuno va
dichiarato qui con la ragione per cui non è generico.

# Perche una guardia

La guardia impedisce che un nuovo `<motore>-comando` allarghi per distrazione
la superficie specifica invece di riusare quella generica.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "crates" / "plenora-database-cli" / "src" / "main.rs"


class EveryCommandDeclaresItsFamily(unittest.TestCase):
    #: Gli attrezzi con cui il repository misura se stesso.
    #:
    #: Vivono su PostgreSQL perche li e stato costruito il banco. Renderli
    #: generici vorrebbe dire portare benchmark e doctor su quattro motori, che
    #: e lavoro vero senza un consumatore che lo chieda: chi misura questo
    #: repository lo misura da qui.
    BENCH = {
        "benchmark-oltp",
        "benchmark-read",
        "benchmark-spatial",
        "benchmark-write",
        "diagnose",
        "doctor",
        "pool-status",
        "profile-check",
        "profile-list",
        "session-context-test",
        "test-cancellation",
        "test-concurrency",
        "test-spatial",
        "test-streaming",
        "transaction-test",
    }

    #: comando -> perche non e (ancora) generico.
    PRODUCT_SPECIFIC = {
        "bulk-write": "scrive da un file Arrow IPC con opzioni proprie del path PostgreSQL; il contratto comune non ha ancora una forma che le esprima",
        "conditional-update": "update ottimistico con sonda della chiave: esiste sul contratto, e la forma degli argomenti non e ancora stata portata alla famiglia generica",
        "execute-ddl": "gemello di `database-execute-sql --allow-raw`, tenuto finche qualcuno dipende dal nome",
        "execute-scalar": "gemello di `database-execute-scalar`, stessa ragione",
        "execute-sql": "gemello di `database-execute-sql`, stessa ragione",
        "explain": "il piano di esecuzione non e nel contratto comune: ogni motore lo rende in una forma sua",
        "inspect-catalogs": "gemello di `database-inspect-catalogs`, tenuto finche qualcuno dipende dal nome",
        "inspect-database": "aggrega piu introspezioni in un documento solo, e quella forma non e nel contratto",
        "inspect-objects": "gemello di `database-inspect-objects`, stessa ragione",
        "inspect-schemas": "gemello di `database-inspect-schemas`, stessa ragione",
        "inspect-tables": "rende una forma piu ricca di `database-inspect-objects`, con dettagli che il contratto non porta",
        "portable-execute": "esegue un piano portabile contro un provider costruito esplicitamente per PostgreSQL; generico il giorno in cui il compilatore copre i quattro dialetti",
        "postgres-describe": "gemello di `database-describe`, tenuto finche qualcuno dipende dal nome",
        "postgres-probe": "gemello di `database-probe`, stessa ragione",
        "postgres-query": "query relazionale con opzioni proprie; la forma generica non esiste ancora",
        "postgres-read-ipc": "stream Arrow IPC su stdout, con un contratto di uscita proprio",
        "postgres-write-ipc": "gemello in scrittura del precedente",
        "postgres-read-summary": "riassunto di una lettura, forma propria",
        "mysql-conditional-update": "come `conditional-update`, sull'altro motore",
        "mysql-describe": "gemello di `database-describe`",
        "mysql-execute-ddl": "gemello di `database-execute-sql --allow-raw`",
        "mysql-execute-scalar": "gemello di `database-execute-scalar`",
        "mysql-execute-sql": "gemello di `database-execute-sql`",
        "mysql-inspect-schemas": "gemello di `database-inspect-schemas`",
        "mysql-inspect-tables": "come `inspect-tables`, sull'altro motore",
        "mysql-probe": "gemello di `database-probe`",
        "mysql-transaction-test": "banco di prova del motore MySQL, gemello di `transaction-test`",
    }

    def catalogue(self) -> dict[str, str | None]:
        source = MAIN.read_text(encoding="utf-8")
        match = re.search(
            r"const COMMAND_CATALOGUE: &\[\(&str, Option<&str>\)\] = &\[(.*?)\n\];",
            source,
            re.S,
        )
        self.assertIsNotNone(match, "catalogo dei comandi non riconosciuto")
        entries: dict[str, str | None] = {}
        for name, feature, _inner in re.findall(
            r'\("([\w-]+)",\s*(None|Some\("(\w+)"\))\)', match.group(1)
        ):
            entries[name] = None if feature == "None" else feature
        self.assertGreater(len(entries), 30, "catalogo troppo corto per essere quello vero")
        return entries

    def test_every_command_belongs_to_a_declared_family(self) -> None:
        entries = self.catalogue()
        generic = {name for name, feature in entries.items() if feature is None}
        # I generici si riconoscono da soli: nessuna feature li porta.
        self.assertTrue(generic, "nessun comando generico")

        senza_famiglia = sorted(
            name
            for name in entries
            if name not in generic
            and name not in self.BENCH
            and name not in self.PRODUCT_SPECIFIC
        )
        self.assertEqual(
            senza_famiglia,
            [],
            "questi comandi non dichiarano a quale famiglia appartengono: "
            f"{senza_famiglia}. Un comando nuovo e generico, oppure e banco di "
            "prova, oppure dice qui perche e legato a un prodotto.",
        )

    def test_no_declared_command_has_disappeared(self) -> None:
        """Una dichiarazione che nomina un comando inesistente e scaduta.

        E' la meta che si dimentica: quando un comando viene tolto o reso
        generico, la riga che lo spiegava resta, e da quel momento descrive un
        mondo che non c'e piu.
        """

        entries = self.catalogue()
        fantasmi = sorted(
            name for name in (self.BENCH | set(self.PRODUCT_SPECIFIC)) if name not in entries
        )
        self.assertEqual(
            fantasmi,
            [],
            f"questi comandi sono dichiarati ma non esistono piu: {fantasmi}",
        )

    def test_the_generic_family_is_reachable_by_every_provider(self) -> None:
        """Un comando generico non puo essere dietro la feature di un motore.

        La feature `None` rende il comando raggiungibile in ogni build e rende
        verificabile la parola «generico».
        """

        entries = self.catalogue()
        traditori = sorted(
            name
            for name, feature in entries.items()
            if name.startswith("database-") and feature is not None
        )
        self.assertEqual(
            traditori,
            [],
            f"questi comandi si chiamano generici e non lo sono: {traditori}",
        )


if __name__ == "__main__":
    unittest.main()
