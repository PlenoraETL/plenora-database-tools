#!/usr/bin/env python3
"""Le decisioni di `sonde_scritture_server.py` e i fixture della regola A, senza server.

Il gate live `write-admission-probes` misura le chiavi d'ammissione delle
scritture e confronta le misure con `tests/fixtures/scrittura_server_*.json`;
la libreria ammette soltanto cio che quei fixture misurano
(`plenora_database_core::write_admission`). Qui si prova, offline, che ogni
prodotto dell'harness abbia il suo fixture, che il workflow li misuri tutti,
che ogni fixture abbia la forma che la libreria accetta (le stesse condizioni
di `AdmissionTable::parse`) e che `--aggiorna` sostituisca soltanto la major
misurata.
"""
from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sonde_scritture_server as sonde  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "write-admission-probes.yml"
ESITI = {"esatto", "riscrive", "rifiuta", "illeggibile"}
CLASSI = {"vuoto_a_null", "riempie_testo", "riempie_binario", "toglie_finali", "spazi_oltre",
          "underflow", "zero_negativo", "frazione_secondi", "non_ascii", "json_numeri", "ogni"}
CHIAVE = {"arrow", "nativo", "dimensione", "esito", "riscritture", "riscritti", "rifiutati",
          "per_eccesso"}


def coerente(documento: dict, prodotto: str) -> list[str]:
    """Le violazioni delle condizioni di `AdmissionTable::parse`."""
    errori = []
    if documento.get("schema_version") != 1 or documento.get("provider") != prodotto:
        errori.append("schema_version o provider")
    majors = [misura["server_major"] for misura in documento["misure"]]
    if len(majors) != len(set(majors)):
        errori.append("major ripetuta")
    for misura in documento["misure"]:
        if set(misura) != {"server_major", "server_version", "immagine", "batteria", "chiavi"}:
            errori.append(f"campi della misura {misura.get('server_major')}")
        if misura["batteria"] != "1":
            errori.append("batteria")
        if "@sha256:" not in misura["immagine"]:
            errori.append(f"immagine non per digest: {misura['immagine']}")
        viste = set()
        for chiave in misura["chiavi"]:
            if not set(chiave) <= CHIAVE:
                errori.append(f"campi della chiave {chiave.get('nativo')}")
            identita = (chiave["arrow"], chiave["nativo"], chiave.get("dimensione"))
            if identita in viste:
                errori.append(f"chiave ripetuta {identita}")
            viste.add(identita)
            if chiave["esito"] not in ESITI:
                errori.append(f"esito {chiave['esito']}")
            if any(classe.get("classe") not in CLASSI for classe in chiave["riscritture"]):
                errori.append(f"classe sconosciuta in {identita}")
            if chiave["esito"] == "esatto" and (chiave["riscritture"] or chiave["riscritti"]):
                errori.append(f"esatta con riscritture {identita}")
            if chiave["esito"] == "riscrive" and not (chiave["riscritture"]
                                                      and chiave["riscritti"]):
                errori.append(f"riscrive senza classi {identita}")
            if chiave["esito"] in {"rifiuta", "illeggibile"} and chiave["riscritture"]:
                errori.append(f"rifiutata con classi {identita}")
    return errori


class Fixture(unittest.TestCase):
    def test_ogni_prodotto_ha_un_fixture_coerente(self) -> None:
        for prodotto in sonde.PRODOTTI:
            with self.subTest(prodotto=prodotto):
                percorso = sonde.fixture_path(prodotto)
                self.assertTrue(percorso.exists(), percorso)
                documento = json.loads(percorso.read_text(encoding="utf-8"))
                self.assertEqual(coerente(documento, prodotto), [])

    def test_ogni_immagine_misurata_viene_dalle_fonti(self) -> None:
        # Db2 si costruisce dal Dockerfile del gate: la sua immagine non ha un
        # digest di registro da confrontare.
        for prodotto, voce in sonde.PRODOTTI.items():
            if prodotto == "db2":
                continue
            with self.subTest(prodotto=prodotto):
                fonti = set(voce.immagini())
                documento = json.loads(sonde.fixture_path(prodotto).read_text(encoding="utf-8"))
                for misura in documento["misure"]:
                    self.assertIn(misura["immagine"], fonti, misura["server_major"])

    def test_il_workflow_misura_ogni_prodotto(self) -> None:
        testo = WORKFLOW.read_text(encoding="utf-8")
        trovata = re.search(r"server:\s*\[([^\]]*)\]", testo)
        self.assertIsNotNone(trovata)
        matrice = {voce.strip() for voce in trovata.group(1).split(",")}
        self.assertEqual(matrice, set(sonde.PRODOTTI))

    def test_ogni_crate_incorpora_il_proprio_fixture(self) -> None:
        for prodotto, voce in sonde.PRODOTTI.items():
            with self.subTest(prodotto=prodotto):
                sorgenti = (ROOT / "crates" / voce.crate / "src").rglob("*.rs")
                incorporato = any(
                    f"scrittura_server_{prodotto}.json" in sorgente.read_text(encoding="utf-8")
                    for sorgente in sorgenti)
                self.assertTrue(incorporato, f"{voce.crate} non incorpora il fixture")


class Coerenza(unittest.TestCase):
    def documento(self, chiave: dict) -> dict:
        return {"schema_version": 1, "provider": "oracle", "misure": [{
            "server_major": "23", "server_version": "23.0", "immagine": "x@sha256:0",
            "batteria": "1", "chiavi": [chiave]}]}

    def test_le_incoerenze_si_vedono(self) -> None:
        base = {"arrow": "utf8", "nativo": "X", "esito": "esatto", "riscritture": [],
                "riscritti": [], "rifiutati": [], "per_eccesso": []}
        self.assertEqual(coerente(self.documento(base), "oracle"), [])
        for guasto in ({"esito": "riscrive"}, {"riscritti": ["a"]}, {"esito": "boh"},
                       {"esito": "rifiuta", "riscritture": [{"classe": "ogni"}]},
                       {"esito": "riscrive", "riscritti": ["a"],
                        "riscritture": [{"classe": "inventata"}]}):
            self.assertNotEqual(coerente(self.documento({**base, **guasto}), "oracle"), [],
                                guasto)


class Aggiorna(unittest.TestCase):
    def test_si_sostituisce_soltanto_la_major_misurata(self) -> None:
        with tempfile.TemporaryDirectory() as cartella:
            percorso = Path(cartella) / "f.json"
            percorso.write_text(json.dumps({"schema_version": 1, "provider": "oracle", "misure": [
                {"server_major": "19", "chiavi": ["vecchia"]},
                {"server_major": "23", "chiavi": ["vecchia"]}]}), encoding="utf-8")
            with mock.patch.object(sonde, "fixture_path", return_value=percorso):
                sonde.aggiorna("oracle", [{"server_major": "23", "chiavi": ["nuova"]}])
            documento = json.loads(percorso.read_text(encoding="utf-8"))
            self.assertEqual([(m["server_major"], m["chiavi"]) for m in documento["misure"]],
                             [("19", ["vecchia"]), ("23", ["nuova"])])


if __name__ == "__main__":
    unittest.main()
