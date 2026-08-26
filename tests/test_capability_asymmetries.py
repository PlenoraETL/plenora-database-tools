#!/usr/bin/env python3
"""Un'asimmetria fra prodotti va spiegata dove si trova.

Il repository ha gia due guardie sulle capability, e questa e la terza faccia
dello stesso problema.

`capability_surface`, nel motore, pretende che ogni campo sia consultato oppure
dichiarato **descrittivo** con la sua ragione: risponde a «questa bandiera
serve a qualcosa?». `test_capability_promises` pretende che cio che un provider
dichiara di saper fare esista nel suo codice: risponde a «questa bandiera e
mantenibile?».

Resta la terza domanda, che e quella che un lettore si fa davvero: **perche qui
si' e li no?**

# Perche solo le asimmetriche

Una bandiera chiusa su tutti e quattro ha una ragione sola, e sta nel contratto
dove il campo e definito — `array_binding` e descrittivo, `returning` vuole una
major. Ripeterla quattro volte le darebbe quattro posti in cui invecchiare
separatamente.

Una bandiera chiusa **qui** e aperta **li** e un'altra cosa: e una differenza
fra motori, e il posto giusto per spiegarla e il punto in cui si decide. Chi
legge `staged_swap: false` su MySQL e lo vede `true` su PostgreSQL vuole sapere
cosa cambia fra i due, e non lo trova nel contratto — li il campo e uno solo.

# Il difetto che l'ha prodotta

`savepoints: false` su SQL Server era chiusa senza una riga accanto. La ragione
esisteva, ma stava nel contratto: «non espone affatto uno scope transazionale».
Il giorno in cui quello scope e arrivato la ragione e caduta, e la bandiera e
rimasta chiusa senza piu niente a sostenerla — invisibile, perche nessuno
rilegge il contratto quando implementa un provider.

Insieme a quella, la scansione ne ha trovate altre quattro: `transactional_ddl`
e `staged_swap` su entrambi i profili della famiglia MySQL, e `geography` su
MySQL. Ragioni vere, tutte, e nessuna scritta dove serviva.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class AsymmetricFlagsCarryTheirReason(unittest.TestCase):
    #: dove ogni prodotto dichiara le proprie capability.
    DECLARATIONS = {
        "PostgreSQL": "crates/plenora-db-postgres/src/catalog/capabilities.rs",
        "SQL Server": "crates/plenora-db-sqlserver/src/provider.rs",
        "MySQL/MariaDB": "crates/plenora-db-mysql/src/profile.rs",
    }

    #: Le bandiere che **oggi** valgono diversamente fra i quattro prodotti.
    #:
    #: Scritte a mano, e non dedotte: dedurle vorrebbe dire leggere il valore
    #: di ogni campo su ogni prodotto, e i valori non sono tutti letterali —
    #: PostgreSQL li scopre interrogando il server. L'elenco e corto e cambia
    #: di rado; quando cambia, cambia insieme a una decisione che qualcuno ha
    #: preso apposta.
    ASYMMETRIC = (
        "truncate_insert",
        "transactional_ddl",
        "staged_swap",
        "geography",
        "requires_declared_crs",
    )

    def test_every_asymmetric_flag_closed_here_says_why(self) -> None:
        nudi: list[str] = []
        for product, relative in self.DECLARATIONS.items():
            lines = (ROOT / relative).read_text(encoding="utf-8").splitlines()
            for number, line in enumerate(lines):
                match = re.match(r"^\s+(\w+): false,\s*$", line)
                if not match or match.group(1) not in self.ASYMMETRIC:
                    continue
                # La ragione sta nelle righe subito sopra, e basta che la piu
                # vicina sia un commento: il blocco intero si legge da li.
                precedente = lines[number - 1].strip() if number else ""
                if not precedente.startswith("//"):
                    nudi.append(f"{product} {relative}:{number + 1} {match.group(1)}")

        self.assertEqual(
            nudi,
            [],
            "queste bandiere valgono diversamente fra i prodotti e sono chiuse "
            "senza una ragione accanto. Chi legge `false` qui la vede `true` "
            f"altrove e non sa cosa cambi fra i due motori: {nudi}",
        )

    def test_the_asymmetric_list_is_not_empty_and_names_real_fields(self) -> None:
        """L'elenco deve nominare campi che esistono nel contratto.

        Un nome sbagliato non farebbe fallire la prova sopra — semplicemente
        non troverebbe niente — e la guardia diventerebbe verde per il motivo
        peggiore: perche non sta guardando nulla.
        """

        contratto = (
            ROOT / "crates" / "plenora-database-core" / "src" / "capabilities.rs"
        ).read_text(encoding="utf-8")
        self.assertTrue(self.ASYMMETRIC, "nessuna bandiera asimmetrica dichiarata")
        for flag in self.ASYMMETRIC:
            self.assertRegex(
                contratto,
                rf"pub {flag}: ",
                f"{flag} non e un campo del contratto capability",
            )


if __name__ == "__main__":
    unittest.main()
