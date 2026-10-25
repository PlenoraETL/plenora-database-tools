#!/usr/bin/env python3
"""Le decisioni di `sonde_srid_server.py`, senza server.

Il gate live misura i fatti su cui si regge la tabella delle guardie della
regola delle sonde (`spatial_probe::server_guard`). Qui si prova, offline, che
lo script legga gli esiti dei quattro client nello stesso modo, che le
immagini vengano dalle loro fonti per digest, e che la tabella del core e le
sonde dello script dicano la stessa cosa: un prodotto ha una guardia nel core
se e soltanto se lo script ne misura una.
"""
from __future__ import annotations

import re
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sonde_srid_server as sonde  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def completato(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


class Esito(unittest.TestCase):
    def test_un_errore_e_il_codice_del_server(self) -> None:
        casi = {
            "mysql": "ERROR 3033 (HY000) at line 1: Binary geometry function st_intersects",
            "mariadb": "ERROR 1064 (42000) at line 1: You have an error",
            "postgres": "ERROR:  XX000: ST_Equals: Operation on mixed SRID geometries",
            "sqlserver": "Msg 6522, Level 16, State 1, Server x, Line 1",
        }
        attesi = {"mysql": "3033", "mariadb": "1064", "postgres": "XX000", "sqlserver": "6522"}
        for server, uscita in casi.items():
            self.assertEqual(sonde.esito(server, completato(1, stderr=uscita)),
                             {"errore": attesi[server]}, server)

    def test_un_valore_e_l_ultima_riga_non_vuota(self) -> None:
        self.assertEqual(sonde.esito("sqlserver", completato(0, "\n0\n\n")), {"valore": "0"})
        self.assertEqual(sonde.esito("postgres", completato(0, "SET\n1\n")), {"valore": "1"})

    def test_un_codice_nell_uscita_conta_anche_con_exit_zero(self) -> None:
        # `psql` con piu statement, o un client che non propaga l'errore:
        # un codice nell'uscita e un errore, non un valore.
        self.assertIn("errore", sonde.esito("postgres", completato(0, stderr="ERROR:  22023: x")))


class Verdetto(unittest.TestCase):
    def test_errore_righe_e_ok(self) -> None:
        self.assertTrue(sonde.verdetto(("errore", {"3033", "3643"}), {"errore": "3643"}))
        self.assertFalse(sonde.verdetto(("errore", {"3033"}), {"valore": "1"}))
        self.assertTrue(sonde.verdetto(("righe", 0), {"valore": "0"}))
        self.assertFalse(sonde.verdetto(("righe", 0), {"errore": "6522"}))
        self.assertFalse(sonde.verdetto(("righe", 1), {"valore": "0"}))


class Riferimenti(unittest.TestCase):
    def test_ogni_immagine_e_per_digest_e_viene_dalla_sua_fonte(self) -> None:
        for server in sonde.SERVER:
            immagini = sonde.riferimenti(server)
            self.assertTrue(immagini, server)
            for immagine in immagini:
                self.assertRegex(immagine, r"@sha256:[0-9a-f]{64}$", server)
        self.assertEqual(len(sonde.riferimenti("mysql")), 3)
        self.assertEqual(len(sonde.riferimenti("mariadb")), 3)


class TabellaDelleGuardie(unittest.TestCase):
    """La tabella `server_guard` del core e le sonde dello script."""

    @staticmethod
    def guardie_del_core() -> set[str]:
        sorgente = (ROOT / "crates" / "plenora-database-core" / "src" / "spatial_probe.rs"
                    ).read_text(encoding="utf-8")
        corpo = sorgente.split("pub const fn server_guard", 1)[1].split("\n}\n", 1)[0]
        return {nome.lower() for nome in re.findall(r"ProviderKind::(\w+)", corpo)}

    def test_un_prodotto_ha_una_guardia_se_e_solo_se_lo_script_la_misura(self) -> None:
        misurate = {server for server, definizione in sonde.SERVER.items()
                    if any(sonda.prova == "guardia" for sonda in definizione.sonde)}
        self.assertEqual(self.guardie_del_core(), misurate)

    def test_i_prodotti_senza_guardia_sono_misurati_senza(self) -> None:
        for server, definizione in sonde.SERVER.items():
            prove = {sonda.prova for sonda in definizione.sonde}
            self.assertTrue(prove & {"guardia", "senza-guardia"}, server)
            if server not in self.guardie_del_core():
                self.assertNotIn("guardia", prove, server)
                self.assertIn("senza-guardia", prove, server)

    def test_ogni_sonda_ha_un_atteso_leggibile(self) -> None:
        for server, definizione in sonde.SERVER.items():
            nomi = [sonda.nome for sonda in definizione.sonde]
            self.assertEqual(len(nomi), len(set(nomi)), server)
            for sonda in definizione.sonde:
                self.assertIn(sonda.atteso[0], {"errore", "righe", "ok"}, sonda.nome)
                self.assertIn(sonda.prova, {"guardia", "senza-guardia", "controllo", "catalogo"})


if __name__ == "__main__":
    unittest.main()
