#!/usr/bin/env python3
"""Self-test del runner dei vettori arrow-data-v1: i verdetti dagli esiti.

Il runner decide che cosa vuol dire «conforme» per ogni esito osservato; un
errore qui renderebbe verde un gate che non lo e'. Si prova la decisione, non
il binario: gli esiti sono costruiti a mano, l'oracolo e' sostituito da uno
che dice soltanto se la classe e' di schema o di valore.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vettori_arrow_data as runner  # noqa: E402


class Oracolo:
    def __init__(self, di_schema: bool) -> None:
        self.di_schema = di_schema

    def schema_verdict(self, _vettore: dict) -> object:
        return object() if self.di_schema else None


def rifiuto(categoria: str, fase: str, effetto: str = "none", **altro: object) -> dict:
    return {"exit": 3, "category": categoria, "phase": fase,
            "remote_effect": effetto, "retry": "never", **altro}


VALIDO = {"expect": "valid"}
DI_VALORE = {"expect": "invalid", "category": "data_mapping"}
DI_SCHEMA = {"expect": "invalid", "category": "schema"}


class Verdetti(unittest.TestCase):
    def verdetto(self, vettore: dict, osservato: dict, *, di_schema: bool = False,
                 operazione: str = "write-postgres", limite: str | None = None) -> str:
        return runner.verdetto(operazione, vettore, osservato, Oracolo(di_schema), limite)

    def test_un_valido_accettato_e_conforme_solo_con_gli_srid_giusti(self) -> None:
        self.assertEqual(self.verdetto(VALIDO, {"exit": 0, "category": None}), "conforme")
        self.assertEqual(
            self.verdetto(VALIDO, {"exit": 0, "category": None, "srid_difformi": 1}),
            "accettazione-difforme",
        )

    def test_un_valido_rifiutato_senza_limite_dichiarato_e_indebito(self) -> None:
        self.assertEqual(self.verdetto(VALIDO, rifiuto("crs", "prepare")), "rifiuto-indebito")
        self.assertEqual(
            self.verdetto(VALIDO, rifiuto("crs", "prepare"), limite="crs"), "rifiuto-di-supporto"
        )
        # Il limite ha la sua categoria: un'altra non lo e'.
        self.assertEqual(
            self.verdetto(VALIDO, rifiuto("unsupported", "prepare"), limite="crs"),
            "rifiuto-indebito",
        )

    def test_un_invalido_accettato_e_indebito(self) -> None:
        self.assertEqual(self.verdetto(DI_VALORE, {"exit": 0, "category": None}),
                         "accettazione-indebita")

    def test_la_categoria_viene_prima_degli_assi(self) -> None:
        self.assertEqual(self.verdetto(DI_VALORE, rifiuto("invalid_plan", "validate")),
                         "categoria-sbagliata")

    def test_gli_assi_di_rej_003(self) -> None:
        # Classe di valore: la fase in cui il valore si incontra.
        self.assertEqual(self.verdetto(DI_VALORE, rifiuto("data_mapping", "write", "rolled_back")),
                         "conforme")
        self.assertEqual(self.verdetto(DI_VALORE, rifiuto("data_mapping", "validate")),
                         "assi-del-rifiuto")
        self.assertEqual(
            self.verdetto(DI_VALORE, rifiuto("data_mapping", "read"), operazione="inspect-dataset"),
            "conforme",
        )
        # Classe di schema: niente effetto remoto, mai `rolled_back`.
        self.assertEqual(self.verdetto(DI_SCHEMA, rifiuto("schema", "validate"), di_schema=True),
                         "conforme")
        self.assertEqual(
            self.verdetto(DI_SCHEMA, rifiuto("schema", "validate", "rolled_back"), di_schema=True),
            "assi-del-rifiuto",
        )
        # Un rifiuto che lascia la tabella ha avuto un effetto.
        self.assertEqual(
            self.verdetto(DI_SCHEMA, rifiuto("schema", "validate", tabella_rimasta=True),
                          di_schema=True),
            "assi-del-rifiuto",
        )
        # Exit e retry fanno parte degli assi.
        self.assertEqual(
            self.verdetto(DI_SCHEMA, {**rifiuto("schema", "validate"), "exit": 2}, di_schema=True),
            "assi-del-rifiuto",
        )
        self.assertEqual(
            self.verdetto(DI_SCHEMA, {**rifiuto("schema", "validate"), "retry": "after"},
                          di_schema=True),
            "assi-del-rifiuto",
        )

    def test_un_limite_dichiarato_precede_un_valore_invalido(self) -> None:
        self.assertEqual(self.verdetto(DI_VALORE, rifiuto("crs", "prepare"), limite="crs"),
                         "rifiuto-di-supporto")


if __name__ == "__main__":
    unittest.main()
