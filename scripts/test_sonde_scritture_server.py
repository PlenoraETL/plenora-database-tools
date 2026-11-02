#!/usr/bin/env python3
"""Le decisioni di `sonde_scritture_server.py` e i fixture della regola A, senza server.

Il gate live `write-admission-probes` misura le chiavi d'ammissione delle
scritture e confronta le misure con `tests/fixtures/scrittura_server_*.json`;
la libreria conserva separatamente i profili qualificati di ammissione
(`plenora_database_core::write_admission`). Qui si prova, offline, che ogni
prodotto dell'harness abbia il suo fixture, che il workflow li misuri tutti,
che ogni fixture abbia la forma che la libreria accetta (le stesse condizioni
di `AdmissionTable::parse`) e che `--aggiorna` sostituisca soltanto la major
misurata.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sonde_scritture_server as sonde  # noqa: E402
from import_write_probe_log import identity, log_changes  # noqa: E402

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
    def test_ammissione_ristretta_dal_writer_e_misure_separate(self) -> None:
        directory = ROOT / 'tests/fixtures/ammissione_scrittura'
        baseline = json.loads((directory / 'baseline.json').read_text(encoding='utf-8'))
        self.assertEqual(baseline['baseline'], '3af0fd3011047189f7340bc2998e6894ca15744c')
        for provider, pin in baseline['providers'].items():
            with self.subTest(provider=provider):
                qualified = json.loads((directory / f'{provider}.json').read_text(encoding='utf-8'))
                self.assertEqual(coerente(qualified, provider), [])
                restrictions = json.loads((directory / 'writer_restrictions.json').read_text(encoding='utf-8'))
                current = {(m['server_major'], identity(k)): k for m in qualified['misure'] for k in m['chiavi']}
                seen = set()
                for change in restrictions['changes']:
                    if change['provider'] != provider:
                        continue
                    key = change['major'], identity(change['before'])
                    self.assertNotIn(key, seen)
                    seen.add(key)
                    old, new = change['before'], change['after']
                    self.assertEqual(identity(old), identity(new))
                    self.assertEqual(current[key], new)
                    self.assertLessEqual(set(old['rifiutati']), set(new['rifiutati']))
                    self.assertFalse(set(new['rifiutati']) & set(new['riscritti']))
                    names = set(change['battery'])
                    self.assertLessEqual(set(new['rifiutati']) | set(new['riscritti']), names)
                    # Decisioni calcolate per ogni valore e politica: nessun successo nuovo.
                    for strict in (False, True):
                        def admitted(k):
                            if k['esito'] in ('rifiuta', 'illeggibile'):
                                return set()
                            refused = set(k['rifiutati'])
                            if strict:
                                refused |= set(k['riscritti']) | set(k['per_eccesso'])
                            return names - refused
                        self.assertLessEqual(admitted(new), admitted(old), (provider, key, strict))
                    current[key].clear()
                    current[key].update(old)
                # Ricostruisce il baseline immutato: non si ripinna la storia.
                canonical = json.dumps(qualified, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()
                self.assertEqual(hashlib.sha256(canonical).hexdigest(), pin['sha256'])
                measured = json.loads(sonde.fixture_path(provider).read_text(encoding='utf-8'))
                evidence = json.loads((ROOT / f'tests/fixtures/write_probe_evidence/{provider}.json').read_text(encoding='utf-8'))
                before = {(m['server_major'], identity(k)): k for m in qualified['misure'] for k in m['chiavi']}
                after = {(m['server_major'], identity(k)): k for m in measured['misure'] for k in m['chiavi']}
                self.assertEqual(len(before), pin['keys'])
                self.assertEqual(before.keys(), after.keys())
                for change in evidence['changes']:
                    if provider != 'db2':
                        self.assertIn(change['image'], set(sonde.PRODOTTI[provider].immagini()))
                    key = change['major'], identity(change['before'])
                    self.assertEqual(before[key], change['before'])
                    self.assertEqual(after[key], change['after'])
                for change in evidence['changes']:
                    after[change['major'], identity(change['before'])] = change['before']
                self.assertEqual(before, after)
                # La separazione e obbligatoria anche nel consumatore di produzione.
                crate = sonde.PRODOTTI[provider].crate
                includes = [p for p in (ROOT / 'crates' / crate / 'src').rglob('*.rs')
                            if f'ammissione_scrittura/{provider}.json' in p.read_text(encoding='utf-8')]
                self.assertTrue(any(p.name in ('admission.rs', 'write_admission.rs') for p in includes))

    def test_import_rifiuta_log_senza_riepilogo(self) -> None:
        with self.assertRaises(ValueError):
            log_changes('la misura del server (non l\'ammissione della libreria) non coincide con il fixture:', 'mysql')

    def test_db2_client_uses_the_qualified_utf8_codepage(self) -> None:
        from check_db2_reference import DB2_CODEPAGE
        self.assertEqual(
            sonde.PRODOTTI["db2"].test_env["DB2CODEPAGE"],
            DB2_CODEPAGE["DB2CODEPAGE"],
        )


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


class PrivilegiMinimi(unittest.TestCase):
    def test_grant_temporanee_e_mount_su_tutta_la_flotta(self):
        import re
        import yaml
        for product in ("mysql", "mariadb"):
            with self.subTest(product=product):
                directory = ROOT / "docker" / product / "init"
                sql = (directory / "002-minimal-privileges.sql").read_text()
                statements = re.sub(r"--[^\n]*", "", sql)
                self.assertRegex(statements, r"USE dataflow_test;\s*CREATE USER IF NOT EXISTS")
                grant = re.search(r"GRANT (.*?)\s+ON dataflow_test\.\* TO 'plenora_minimal'@'%';",
                                  statements, re.S)
                self.assertIsNotNone(grant)
                self.assertEqual({p.strip() for p in grant[1].split(',')},
                                 {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
                                  "ALTER", "INDEX", "CREATE TEMPORARY TABLES"})
                compose = yaml.safe_load((ROOT / f"docker-compose.{product}.yml").read_text())
                servers = [s for s in compose["services"].values()
                           if f"{product.upper()}_DATABASE" in s.get("environment", {})]
                self.assertEqual(len(servers), 3 if product == "mariadb" else 1)
                for server in servers:
                    self.assertEqual(server["environment"][f"{product.upper()}_DATABASE"],
                                     "dataflow_test")
                    self.assertIn(f"./docker/{product}/init:/docker-entrypoint-initdb.d:ro",
                                  server["volumes"])

    def test_fixture_e_gate_esercitano_account_senza_privilegi_globali(self):
        from dataclasses import replace
        import subprocess
        import shlex
        sql = (ROOT / "docker/mysql/init/002-minimal-privileges.sql").read_text()
        self.assertEqual(sql, (ROOT / "docker/mariadb/init/002-minimal-privileges.sql").read_text())
        self.assertIn("SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX", sql)
        self.assertNotIn("ON *.*", sql)
        for product in ("mysql", "mariadb"):
            definition = sonde.PRODOTTI[product]
            self.assertIn(sql, shlex.split(definition.argomenti[-1]))
            minimal = "live_tests::live_sospetti_minimal_privileges"
            with mock.patch.object(sonde.subprocess, "run") as run:
                run.side_effect = [subprocess.CompletedProcess([], 0, "1 passed; 0 failed", ""),
                                   subprocess.CompletedProcess([], 0, "misura", "")]
                code, _, _ = sonde.sonda(mock.Mock(), "fixture", replace(definition, ca_nel_container=None),
                                         3306, "image", False)
                self.assertEqual(code, 0)
                self.assertIn(minimal, run.call_args_list[0].args[0])
                self.assertIn("live_tests::" + sonde.TEST, run.call_args_list[1].args[0])
            for output in ("0 passed; 0 failed", "1 failed"):
                with mock.patch.object(sonde.subprocess, "run", return_value=
                                       subprocess.CompletedProcess([], 0, output, "")) as run:
                    code, _, _ = sonde.sonda(mock.Mock(), "fixture", replace(definition, ca_nel_container=None),
                                             3306, "image", False)
                    self.assertNotEqual(code, 0)
                    self.assertEqual(run.call_count, 1)


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
