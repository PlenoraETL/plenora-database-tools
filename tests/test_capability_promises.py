#!/usr/bin/env python3
"""Le capability che promettono un percorso devono trovarlo implementato.

Una bandiera aperta e una promessa fatta a chi legge il documento capability
senza poterla verificare. Il repository ha gia una guardia per la meta opposta
del problema — `capability_surface`, nel motore, pretende che ogni campo sia
consultato oppure dichiarato descrittivo — ma nessuna per questa: che cio che
un provider **dichiara** di saper fare esista davvero nel suo codice.

# Perche una guardia statica

La coerenza fra bandiera e override si vede nel sorgente senza avviare un
server. La guardia copre ogni provider anche in assenza di una prova live
dedicata a quello specifico bordo.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ScopeTransactionIsImplemented(unittest.TestCase):
    """Chi pubblica `scope: Transaction` deve saper aprire una transazione."""

    #: I crate che implementano il contratto `Provider`.
    PROVIDER_CRATES = (
        "plenora-db-postgres",
        "plenora-db-mysql",
        "plenora-db-sqlserver",
        "plenora-db-oracle",
        "plenora-db-db2",
    )

    def sources(self, crate: str) -> list[Path]:
        return sorted((ROOT / "crates" / crate / "src").rglob("*.rs"))

    def test_every_declared_transaction_scope_has_an_implementation(self) -> None:
        promised: list[str] = []
        implemented: list[str] = []

        for crate in self.PROVIDER_CRATES:
            sources = self.sources(crate)
            self.assertTrue(sources, f"{crate}: nessun sorgente")
            text = "\n".join(
                source.read_text(encoding="utf-8") for source in sources
            )
            # La dichiarazione. Le prove che costruiscono capability finte non
            # contano: si escludono i file di sola prova, che non sono cio che
            # il provider pubblica.
            declares = any(
                re.search(r"scope:\s*TransactionScope::Transaction", source.read_text(encoding="utf-8"))
                for source in sources
                if source.name not in {"live_tests.rs", "test_suite.rs"}
            )
            if declares:
                promised.append(crate)
            # L'implementazione: una `fn begin_transaction` che non sia la
            # chiamata a quella di qualcun altro.
            if re.search(r"fn begin_transaction\s*<", text):
                implemented.append(crate)

        self.assertTrue(promised, "nessun provider dichiara scope Transaction")
        mancanti = sorted(set(promised) - set(implemented))
        self.assertEqual(
            mancanti,
            [],
            "questi provider pubblicano transactions.scope = Transaction e non "
            f"implementano begin_transaction: {mancanti}. Il default del "
            "contratto risponde Unsupported, quindi la capability e una "
            "promessa che nessuno puo mantenere.",
        )

    def test_the_guard_knows_every_provider_crate(self) -> None:
        """Un provider nuovo non deve poter sfuggire a questa guardia.

        L'elenco e scritto a mano — e la forma piu leggibile — ma un elenco
        copiato diverge dalla realta alla prima aggiunta. Questa seconda prova
        lo confronta con i crate che esistono, cosi il giorno in cui ne nasce
        uno la guardia lo dice invece di ignorarlo.
        """

        existing = {
            path.name
            for path in (ROOT / "crates").iterdir()
            if path.is_dir() and path.name.startswith("plenora-db-")
        }
        self.assertEqual(
            existing,
            set(self.PROVIDER_CRATES),
            "l'elenco dei crate provider non coincide con quelli presenti",
        )


if __name__ == "__main__":
    unittest.main()
