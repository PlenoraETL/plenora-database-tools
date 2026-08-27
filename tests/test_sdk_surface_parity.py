#!/usr/bin/env python3
"""Le due sessioni del SDK espongono la stessa superficie, o dicono perche no.

Il SDK ha due classi di sessione: `Session`, che serve PostgreSQL, e
`DatabaseSession`, che serve MySQL, MariaDB e SQL Server tenendo il provider
dietro `dyn Provider`.

I metodi comuni sono implementati in due classi; la guardia impedisce che una
modifica venga applicata a un solo percorso senza dichiararne la ragione.

# Perche una guardia e non una fusione

Fondere le due classi in una sarebbe la risposta pulita, e non si puo del tutto:
`metrics` poggia su `metrics_snapshot`, che e un metodo **inerente** di
`PostgresProvider` e non del trait, e `postgis_version` descrive un'estensione
che esiste su un prodotto solo. Una classe sola le perderebbe, o pretenderebbe
di allargare il trait per due metodi che riguardano un motore.

Cio che si puo pretendere e che ogni differenza sia **dichiarata**: le due
superfici coincidono, tranne cio che e elencato qui sotto con la sua ragione.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "crates" / "plenora-database-py" / "src"


def exposed(path: Path) -> set[str]:
    """I metodi che pyo3 pubblica, cioe quelli dentro `#[pymethods]`.

    Il blocco si chiude sulla graffa a colonna zero: gli helper privati che
    seguono nel file non ne fanno parte, e contarli direbbe che le due classi
    divergono dove invece condividono soltanto un'abitudine di layout.
    """

    text = path.read_text(encoding="utf-8")
    blocks = re.findall(r"#\[pymethods\]\n(?:impl[^\n]*\n)(.*?)\n\}\n", text, re.S)
    names: set[str] = set()
    for block in blocks:
        names |= set(re.findall(r"^    (?:pub )?fn (\w+)", block, re.M))
    return names


class TheTwoSessionsAgree(unittest.TestCase):
    #: metodo -> perche esiste su un solo lato.
    DECLARED_DIFFERENCES = {
        "metrics": (
            "poggia su `metrics_snapshot`, metodo inerente di PostgresProvider e "
            "non del trait Provider: la sessione di famiglia tiene un "
            "`dyn Provider` e non puo chiamarlo"
        ),
        "postgis_version": (
            "descrive un'estensione che esiste su un prodotto solo; sugli altri "
            "tre non c'e niente da riportare"
        ),
    }

    def test_the_surfaces_differ_only_where_declared(self) -> None:
        postgres = exposed(SRC / "session.rs")
        family = exposed(SRC / "session_family.rs")
        self.assertTrue(postgres, "nessun metodo trovato su Session")
        self.assertTrue(family, "nessun metodo trovato su DatabaseSession")

        differenze = (postgres - family) | (family - postgres)
        non_dichiarate = sorted(differenze - set(self.DECLARED_DIFFERENCES))
        self.assertEqual(
            non_dichiarate,
            [],
            "queste differenze fra le due sessioni del SDK non sono dichiarate: "
            f"{non_dichiarate}. O si colmano, o si scrive qui perche restano.",
        )

    def test_no_declared_difference_has_quietly_been_closed(self) -> None:
        """Una differenza dichiarata ma assente e documentazione scaduta."""

        postgres = exposed(SRC / "session.rs")
        family = exposed(SRC / "session_family.rs")
        differenze = (postgres - family) | (family - postgres)
        scadute = sorted(set(self.DECLARED_DIFFERENCES) - differenze)
        self.assertEqual(
            scadute,
            [],
            f"queste differenze sono dichiarate ma non esistono piu: {scadute}",
        )


if __name__ == "__main__":
    unittest.main()
