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
    def test_real_cli_envelope_and_raw_outcome_have_identical_report(self):
        import json
        from subprocess import CompletedProcess
        from unittest.mock import patch
        report = {"effective": [{"rule_id": "pg.geometry.representation", "committed": 1, "uncertain": 0}]}
        outcome = {"status": "committed", "loss_report": report}
        normalized = []
        for document in (outcome, {"status": "ok", "protocol_version": 2,
                                  "command": "write", "result": outcome}):
            with patch.object(runner.subprocess, "run", return_value=CompletedProcess([], 0, json.dumps(document), "")):
                result = runner.busta(["cli"])
                self.assertEqual(result["loss_report"], report)
                self.assertEqual(result["remote_effect"], "committed")
                normalized.append(result)
        self.assertEqual(*normalized)

    def test_raw_inspection_is_not_a_canonical_envelope_and_bad_envelopes_fail(self):
        import json
        from subprocess import CompletedProcess
        from unittest.mock import patch
        documents = [("inspect-dataset", {"status": "ok", "fields": []}, "conforme"),
                     ("write-postgres", {"status": "ok", "protocol_version": 2}, "busta-illeggibile"),
                     ("write-postgres", {"status": "ok", "protocol_version": 99, "result": {}}, "busta-illeggibile"),
                     ("write-postgres", [], "busta-illeggibile"),
                     ("write-postgres", {"protocol_version": 2, "status": "error", "error": None}, "busta-illeggibile"),
                     ("write-postgres", {"protocol_version": 2, "status": "error", "error": {}}, "busta-illeggibile"),
                     ("write-postgres", {"protocol_version": 99, "status": "committed"}, "busta-illeggibile"),
                     ("write-postgres", {"protocol_version": 2, "status": "ok", "result": {}}, "busta-illeggibile"),
                     ("write-postgres", {"protocol_version": 2, "status": "other", "result": {}}, "busta-illeggibile")]
        for operation, document, expected in documents:
            with patch.object(runner.subprocess, "run", return_value=CompletedProcess([], 0, json.dumps(document), "")):
                self.assertEqual(self.verdetto(VALIDO, runner.busta(["cli"]), operazione=operation), expected)
        valid_dry = {"exit": 0, "category": None, "catalog": "performed", "row_security": "not_applicable"}
        malformed = {**valid_dry, "busta": "illeggibile"}
        for dry, write in ((malformed, valid_dry), (valid_dry, malformed)):
            self.assertEqual(runner.accordo_dry_run(dry, write), "busta-illeggibile")


    def test_normalization_requires_strict_axes_report_and_readback(self):
        import copy
        good = rifiuto("data_mapping", "write", "rolled_back", tabella_rimasta=False,
            loss_report={"policy": "strict", "effective": [], "evaluation": {"entries": [
                {"rule_id": "pg.geometry.representation", "would_change": 1}]}},
            normalizzazione_consentita=True)
        self.assertEqual(self.verdetto(VALIDO, good), "rifiuto-di-supporto")
        for key, value in [("category", "schema"), ("phase", "read"), ("exit", 5),
                           ("remote_effect", "unknown"), ("retry", "after"),
                           ("tabella_rimasta", True), ("normalizzazione_consentita", False)]:
            self.assertNotEqual(self.verdetto(VALIDO, {**good, key: value}), "rifiuto-di-supporto")
        for report in ({}, {"policy": "compatible"}, {"effective": [{"committed": 1}]},
                       {"evaluation": {"entries": []}},
                       {"evaluation": {"entries": [{"rule_id": "other", "would_change": 1}]}},
                       {"evaluation": {"entries": [{"rule_id": "pg.geometry.representation", "would_change": 0}]}}):
            changed = copy.deepcopy(good)
            changed["loss_report"] = {**changed["loss_report"], **report} if report else {}
            self.assertNotEqual(self.verdetto(VALIDO, changed), "rifiuto-di-supporto")

    def test_normalization_runs_independent_readback_checks(self):
        from tempfile import TemporaryDirectory
        from unittest.mock import patch
        strict = rifiuto("data_mapping", "write", "rolled_back", loss_report={
            "policy": "strict", "effective": [], "evaluation": {"entries": [
                {"rule_id": "pg.geometry.representation", "would_change": 1}]}})
        success = {"exit": 0, "category": None, "loss_report": {"effective": [
            {"rule_id": "pg.geometry.representation", "committed": 1, "uncertain": 0}]}}
        for srid, coordinate, binary, allowed in [(0, 0, 1, True), (1, 0, 1, False),
                                                (0, 1, 1, False), (0, 0, 0, False)]:
            with TemporaryDirectory() as directory:
                pg = runner.Postgres.__new__(runner.Postgres)
                pg.lavoro = Path(directory)
                pg.cli, pg.variabile = Path("cli"), "DSN"
                calls = []
                def cli(argv):
                    import json
                    self.assertNotIn("--accept-loss", argv)
                    request = json.loads(Path(argv[argv.index("--input") + 1]).read_text(encoding="utf-8"))
                    op = json.loads(Path(request["operation_path"]).read_text(encoding="utf-8"))
                    calls.append(op["mapping_policy"])
                    if op["mapping_policy"] == "strict":
                        self.assertNotIn("accept_loss", request)
                        return strict.copy()
                    self.assertIs(request["accept_loss"], True)
                    return success
                with patch.object(pg, "cancella"), patch.object(pg, "esiste", return_value=False), \
                     patch.object(pg, "srid_difformi", return_value=srid), \
                     patch.object(pg, "coordinate_difformi", return_value=coordinate), \
                     patch.object(pg, "byte_difformi", return_value=binary), \
                     patch.object(runner, "busta", side_effect=cli):
                    result = pg.scrivi("fixture", Path("input"), VALIDO)
                    self.assertEqual(result["normalizzazione_consentita"], allowed)
                    self.assertEqual(calls, ["strict", "compatible"])

    def test_pg14_view_keeps_rls_under_the_restricted_owner(self) -> None:
        sql = next(row[1] for row in runner.SCENARI if row[0] == "regola-r-vista-rls-delete")
        old = runner.prepara_vista_rls(sql, 140_000)
        self.assertNotIn("security_invoker", old)
        self.assertIn("ALTER VIEW {t} OWNER TO {r}", old)
        self.assertIn("ENABLE ROW LEVEL SECURITY", old)
        for version in (150_000, 180_000):
            self.assertEqual(runner.prepara_vista_rls(sql, version), sql)


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

    def test_piu_limiti_dichiarati_ammettono_ciascuna_categoria(self) -> None:
        for categoria in ("crs", "unsupported"):
            self.assertEqual(
                self.verdetto(VALIDO, rifiuto(categoria, "validate"),
                              limite={"crs", "unsupported"}),
                "rifiuto-di-supporto",
            )
        self.assertEqual(
            self.verdetto(VALIDO, rifiuto("schema", "validate"), limite={"crs", "unsupported"}),
            "rifiuto-indebito",
        )

    def test_coordinate_scambiate_sono_un_accettazione_difforme(self) -> None:
        # GEO-001: lo SRID giusto non basta, servono le ordinate scritte.
        self.assertEqual(
            self.verdetto(VALIDO, {"exit": 0, "category": None, "srid_difformi": 0,
                                   "coordinate_difformi": 1}),
            "accettazione-difforme",
        )
        # Un campo `lat_lon` accettato da PostGIS ha gli assi scambiati anche
        # con le ordinate rilette identiche.
        self.assertEqual(
            self.verdetto(VALIDO, {"exit": 0, "category": None, "assi_scambiati": 1}),
            "accettazione-difforme",
        )


class Coordinate(unittest.TestCase):
    def test_byte_order_e_forma_del_codice_non_contano(self) -> None:
        # POINT Z(1 2 3): ISO little endian, esteso big endian con SRID 4326.
        iso = bytes.fromhex("01e9030000" "000000000000f03f" "0000000000000040"
                            "0000000000000840")
        esteso = bytes.fromhex("00a0000001" "000010e6" "3ff0000000000000"
                               "4000000000000000" "4008000000000000")
        self.assertEqual(runner.coordinate(iso), runner.coordinate(esteso))

    def test_gli_assi_scambiati_si_vedono(self) -> None:
        punto = bytes.fromhex("0101000000" "0000000000002940" "0000000000004e40")
        scambiato = bytes.fromhex("0101000000" "0000000000004e40" "0000000000002940")
        self.assertNotEqual(runner.coordinate(punto), runner.coordinate(scambiato))

    def test_un_punto_vuoto_e_uguale_a_se_stesso(self) -> None:
        # POINT EMPTY: ordinate NaN, con due rappresentazioni diverse del NaN.
        vuoto = bytes.fromhex("0101000000" "000000000000f87f" "000000000000f87f")
        altro_nan = bytes.fromhex("0101000000" "010000000000f87f" "000000000000f8ff")
        self.assertEqual(runner.coordinate(vuoto), runner.coordinate(vuoto))
        self.assertEqual(runner.coordinate(vuoto), runner.coordinate(altro_nan))

    def test_le_collezioni_si_leggono_per_membro(self) -> None:
        multi = bytes.fromhex("010400000002000000" "0101000000" + "00" * 16
                              + "0101000000" + "000000000000f03f" * 2)
        self.assertEqual(runner.coordinate(multi)[0], 4)
        self.assertEqual(len(runner.coordinate(multi)), 5)


class AccordoDryRun(unittest.TestCase):
    ACCETTATO = {"exit": 0, "category": None, "catalog": "performed",
                 "row_security": "not_applicable"}

    def test_rifiuta_cio_che_la_scrittura_rifiuta_in_ogni_fase(self) -> None:
        for scrittura in (rifiuto("crs", "validate"), rifiuto("crs", "prepare"),
                          rifiuto("data_mapping", "write", "rolled_back")):
            stesso = dict(scrittura, remote_effect="none")
            self.assertEqual(runner.accordo_dry_run(stesso, scrittura), "conforme")
            self.assertEqual(runner.accordo_dry_run(self.ACCETTATO, scrittura),
                             "dry-run-accetta")
            self.assertEqual(runner.accordo_dry_run(dict(stesso, category="schema"), scrittura),
                             "dry-run-categoria")

    def test_la_fase_e_gli_assi_devono_coincidere(self) -> None:
        scrittura = rifiuto("data_mapping", "write", "rolled_back")
        self.assertEqual(runner.accordo_dry_run(rifiuto("data_mapping", "read"), scrittura),
                         "dry-run-fase")
        # Un dry run non ha effetti: `rolled_back` direbbe che ha scritto.
        self.assertEqual(runner.accordo_dry_run(scrittura, scrittura), "dry-run-assi")
        self.assertEqual(runner.accordo_dry_run(dict(rifiuto("data_mapping", "write"), exit=1),
                                                scrittura), "dry-run-exit")

    def test_accetta_solo_cio_che_la_scrittura_accetta_con_il_catalogo(self) -> None:
        scrittura = {"exit": 0, "category": None}
        self.assertEqual(runner.accordo_dry_run(self.ACCETTATO, scrittura), "conforme")
        self.assertEqual(runner.accordo_dry_run(rifiuto("crs", "validate"), scrittura),
                         "dry-run-rifiuta")
        for catalogo in ("not_performed", None):
            self.assertEqual(runner.accordo_dry_run(dict(self.ACCETTATO, catalog=catalogo),
                                                    scrittura), "dry-run-senza-catalogo")

    def test_row_security_si_dichiara_solo_dove_si_applica(self) -> None:
        scrittura = {"exit": 0, "category": None}
        dichiarata = dict(self.ACCETTATO, row_security="not_performed")
        self.assertEqual(runner.accordo_dry_run(dichiarata, scrittura, True), "conforme")
        self.assertEqual(runner.accordo_dry_run(self.ACCETTATO, scrittura, True), "dry-run-rls")
        self.assertEqual(runner.accordo_dry_run(dichiarata, scrittura), "dry-run-rls")
        self.assertEqual(runner.accordo_dry_run(dict(self.ACCETTATO, row_security=None),
                                                scrittura), "dry-run-rls")


class ScenariDelTarget(unittest.TestCase):
    """Gli scenari del target si giudicano con `accordo_dry_run`; qui si
    prova che siano ben formati e che la DSN del ruolo ristretto cambi solo
    l'utente."""

    MODE = {"append", "append-altrove", "create", "create-altrove", "replace",
            "truncate_insert", "update", "delete_by_keys", "upsert"}

    def test_ogni_scenario_e_ben_formato(self) -> None:
        nomi = [scenario[0] for scenario in runner.SCENARI]
        self.assertEqual(len(nomi), len(set(nomi)))
        for nome, preparazione, mode, campi, chiavi, policy, utente in runner.SCENARI:
            self.assertIn(mode, self.MODE, nome)
            self.assertTrue(set(chiavi) <= {campo.removesuffix("+field_id") for campo in campi})
            self.assertIn(policy, {"strict", "lossy"}, nome)
            self.assertIn(utente, {"lettore", "proprietario"}, nome)
            # Il ruolo ristretto ha i privilegi che lo scenario gli concede.
            self.assertEqual(utente == "lettore", "{r}" in preparazione or mode == "create", nome)

    def test_le_classi_della_rilettura_ci_sono(self) -> None:
        nomi = {scenario[0] for scenario in runner.SCENARI}
        self.assertTrue({"privilegi-append", "privilegi-create", "schema-assente",
                         "upsert-senza-unique", "lossy-colonna-assente"} <= nomi)
        # La quarta rilettura: ogni classe del dry run chiuso ha uno scenario.
        self.assertTrue({"rls-senza-policy-append", "rls-senza-policy-truncate",
                         "rls-forzata-al-proprietario", "rls-con-policy-append",
                         "generata-append", "generata-truncate", "identita-always-append",
                         "upsert-senza-select", "upsert-select-solo-chiavi",
                         "vista-non-aggiornabile-append", "vista-aggiornabile-replace",
                         "not-null-assente-append", "not-null-assente-truncate",
                         "schema-senza-usage", "replace-senza-insert"} <= nomi)
        self.assertTrue(runner.SCENARI_RLS_DICHIARATA <= nomi)
        self.assertTrue(runner.SCENARI_DEL_LOCK <= nomi)

    def test_la_dsn_del_ruolo_cambia_solo_l_utente(self) -> None:
        self.assertEqual(
            runner.dsn_con_utente("host=h port=5432 user=a password=b dbname=d", "u", "p"),
            "host=h port=5432   dbname=d user=u password=p")
        self.assertEqual(
            runner.dsn_con_utente("host=h user='a b' password='x\\'y' dbname=d", "u", "p"),
            "host=h   dbname=d user=u password=p")
        self.assertEqual(
            runner.dsn_con_utente("postgresql://a:b@h:5432/d?sslmode=disable", "u", "p"),
            "postgresql://u:p@h:5432/d?sslmode=disable")


if __name__ == "__main__":
    unittest.main()
