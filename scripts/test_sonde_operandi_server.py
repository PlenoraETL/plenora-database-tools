#!/usr/bin/env python3
"""Le decisioni di `sonde_operandi_server.py`, senza server.

Il gate live misura la guardia del server sugli operandi scalari geometrici
(`spatial_operand::operand_server_guard`). Qui si prova, offline, che lo
script legga gli esiti dei due client nello stesso modo, che apra lo SQL reso
con bind veri, che l'immagine venga dalla sua fonte per digest, e che la
tabella del core e le misure del fixture dicano la stessa cosa: un prodotto ha
la guardia nel core se e soltanto se ogni forma misurata che la regola non
ammette e rifiutata dal server.
"""
from __future__ import annotations

import re
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sonde_operandi_server as sonde  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def completato(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


class Esito(unittest.TestCase):
    def test_un_errore_e_il_primo_codice_del_server(self) -> None:
        oracle = ("DECLARE c SYS_REFCURSOR; ...\nERROR at line 1:\n"
                  "ORA-22901: cannot compare VARRAY or LOB attributes\nORA-06512: at line 1\n")
        # sqlplus senza WHENEVER SQLERROR EXIT: exit 0 anche con un errore.
        self.assertEqual(sonde.esito("oracle", completato(0, oracle)), {"errore": "ORA-22901"})
        db2 = "SQL0134N  Improper use of a string column, host variable, constant\n"
        self.assertEqual(sonde.esito("db2", completato(4, db2)), {"errore": "SQL0134N"})

    def test_un_exit_d_errore_senza_codice_e_un_errore(self) -> None:
        self.assertEqual(sonde.esito("db2", completato(8, "")), {"errore": None})
        self.assertEqual(sonde.esito("oracle", completato(1, "")), {"errore": None})

    def test_db2_nessuna_riga_e_avviso_non_sono_errori(self) -> None:
        self.assertEqual(sonde.esito("db2", completato(1, "\n")), {"valore": None})
        self.assertEqual(sonde.esito("db2", completato(0, "SYSIBM.ST_GEOMETRY  \n")),
                         {"valore": "SYSIBM.ST_GEOMETRY"})

    def test_verdetto(self) -> None:
        self.assertTrue(sonde.verdetto({"errore": ["ORA-00932"]}, {"errore": "ORA-00932"}))
        self.assertFalse(sonde.verdetto({"errore": ["ORA-00932"]}, {"valore": None}))
        self.assertFalse(sonde.verdetto({"errore": ["ORA-00932"]}, {"errore": None}))
        self.assertTrue(sonde.verdetto({"ok": True}, {"valore": None}))
        self.assertFalse(sonde.verdetto({"ok": True}, {"errore": "SQL0134N"}))


class Blocco(unittest.TestCase):
    def test_i_bind_sono_veri_bind_tipati(self) -> None:
        sql = "SELECT 1 FROM T WHERE G = CAST(:1 AS RAW(2000)) AND N = CAST(:2 AS VARCHAR2(4000))"
        tipi = {"p": "binary", "s": "string"}
        oracle = sonde.blocco("oracle", sql, ["p", "s"], tipi)
        self.assertIn("v1 RAW(16) := HEXTORAW('01');", oracle)
        self.assertIn("v2 VARCHAR2(20) := 'a';", oracle)
        self.assertIn("USING v1, v2;", oracle)
        db2 = sonde.blocco("db2", sql.replace(":1", "?").replace(":2", "?"), ["p", "s"], tipi)
        self.assertIn("DECLARE v1 VARCHAR(16) FOR BIT DATA DEFAULT BX'01';", db2)
        self.assertIn("OPEN c USING v1, v2;", db2)

    def test_gli_apici_dello_sql_si_raddoppiano(self) -> None:
        self.assertIn("'SELECT ''a'' FROM DUAL'", sonde.blocco("oracle", "SELECT 'a' FROM DUAL",
                                                               [], {}))

    def test_un_bind_si_dichiara_dal_tipo_del_piano(self) -> None:
        piano = {"filter": {"kind": "between", "lower": {"kind": "typed_parameter", "name": "p",
                                                         "parameter_type": "binary"},
                            "upper": {"kind": "typed_parameter", "name": "q",
                                      "parameter_type": "binary"}}}
        self.assertEqual(sonde.bind_tipati(piano), {"p": "binary", "q": "binary"})

    def test_il_controllo_cambia_soltanto_la_colonna_geometrica(self) -> None:
        sql = 'SELECT "T"."ID" AS "ID", "T"."G" AS "G" FROM "PLENORA"."SONDE_OPERANDI" "T"'
        self.assertEqual(sonde.sql_di_controllo(sql, "B"),
                         'SELECT "T"."ID" AS "ID", "T"."B" AS "B" FROM "PLENORA"."SONDE_OPERANDI" "T"')


class Riferimenti(unittest.TestCase):
    def test_l_immagine_e_per_digest_e_viene_dalla_sua_fonte(self) -> None:
        for server in sonde.SERVER:
            self.assertRegex(sonde.riferimento(server), r"@sha256:[0-9a-f]{64}$", server)
        self.assertTrue(sonde.riferimento("oracle").startswith("ghcr.io/gvenzl/oracle-free@"))
        self.assertTrue(sonde.riferimento("db2").startswith("icr.io/db2_community/db2@"))


class Fixture(unittest.TestCase):
    def test_ogni_forma_ha_sql_bind_ed_esito_per_ogni_server(self) -> None:
        forme = sonde.carica()["forme"]
        nomi = [forma["forma"] for forma in forme]
        self.assertEqual(len(nomi), len(set(nomi)))
        for forma in forme:
            tipi = sonde.bind_tipati(forma["piano"])
            for server in sonde.SERVER:
                resa = forma["sql"][server]
                self.assertTrue(resa["sql"].startswith(("SELECT ", "WITH ")), forma["forma"])
                self.assertEqual(set(resa["binds"]), set(tipi), forma["forma"])
                esito = forma["esito"][server]
                self.assertTrue(esito == {"ok": True} or esito.get("errore"), forma["forma"])
                for codice in esito.get("errore", []):
                    self.assertRegex(codice, sonde.CODICE[server], forma["forma"])
            if forma["controllo"] is not None:
                self.assertIn('"G"', forma["sql"]["oracle"]["sql"], forma["forma"])


class TabellaDelleGuardie(unittest.TestCase):
    """La tabella `operand_server_guard` del core e le misure del fixture."""

    @staticmethod
    def guardie_del_core() -> set[str]:
        sorgente = (ROOT / "crates" / "plenora-database-core" / "src" / "spatial_operand.rs"
                    ).read_text(encoding="utf-8")
        corpo = sorgente.split("pub const fn operand_server_guard", 1)[1].split("\n}\n", 1)[0]
        return {nome.lower() for nome in re.findall(r"ProviderKind::(\w+)", corpo)}

    def test_un_prodotto_ha_la_guardia_se_e_solo_se_ogni_forma_e_rifiutata(self) -> None:
        forme = sonde.carica()["forme"]
        misurate = set()
        for server in sonde.SERVER:
            prove = {sonde.prova(forma, server) for forma in forme}
            if "guardia" in prove and "senza-guardia" not in prove:
                misurate.add(server)
        self.assertEqual(self.guardie_del_core(), misurate)
        self.assertEqual(misurate, {"oracle"})

    def test_db2_e_misurato_senza_guardia(self) -> None:
        forme = sonde.carica()["forme"]
        senza = [forma["forma"] for forma in forme if sonde.prova(forma, "db2") == "senza-guardia"]
        self.assertIn("= con bind binario", senza)
        self.assertIn("= fra due colonne geometriche", senza)

    def test_le_forme_ammesse_dalla_regola_passano_ovunque(self) -> None:
        for forma in sonde.carica()["forme"]:
            if forma.get("ammessa"):
                for server in sonde.SERVER:
                    self.assertEqual(forma["esito"][server], {"ok": True}, forma["forma"])


if __name__ == "__main__":
    unittest.main()
