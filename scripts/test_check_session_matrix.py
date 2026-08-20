#!/usr/bin/env python3
"""Self-test della matrice della semantica di sessione.

La matrice giustifica una decisione architetturale: il codice di sessione
resta condiviso perche i tre riferimenti si comportano allo stesso modo. Una
matrice che smettesse di accorgersi di una divergenza — o che chiamasse
"accordo" un fallimento comune — lascerebbe quella decisione in piedi senza
la prova che la regge.

Non serve un server: qui si verifica il **giudizio** del runner su documenti
costruiti a mano, che e la parte che puo rompersi in silenzio.
"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load():
    specification = importlib.util.spec_from_file_location(
        "session_matrix", ROOT / "scripts" / "check_session_matrix.py"
    )
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


MATRIX = load()


def load_campaign():
    specification = importlib.util.spec_from_file_location(
        "session_campaign", ROOT / "scripts" / "check_session_campaign.py"
    )
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


CAMPAIGN = load_campaign()


def observations(probes, outcome: str = "accepted", detail: str = "uguale"):
    return [
        {
            "probe": probe,
            "family": "session",
            "surface": "bootstrap",
            "question": "domanda",
            "outcome": outcome,
            "detail": detail,
            "server_code": None,
        }
        for probe in probes
    ]


def documents(fleet, probes=None, **override):
    """Tre documenti identici, salvo cio che il caso vuole cambiare."""

    probes = MATRIX.EXPECTED_PROBES if probes is None else probes
    built = {}
    for index, server in enumerate(fleet):
        outcome = override.get("outcome") if index and "outcome" in override else "accepted"
        detail = override.get("detail") if index and "detail" in override else "uguale"
        if override.get("everywhere"):
            outcome = override.get("outcome", "accepted")
        built[server.key] = {
            "server": {"product_version": "x", "version_comment": "y"},
            "bootstrap_sql": "SET SESSION autocommit = 1",
            "observations": observations(probes, outcome, detail),
        }
    return built


class SessionMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fleet = MATRIX.servers()

    def judge(self, built):
        results = MATRIX.compare(built, self.fleet, MATRIX.OUTCOME_ONLY)
        MATRIX.validate(built, results)

    def test_the_matrix_measures_the_three_declared_references(self) -> None:
        keys = [server.key for server in self.fleet]
        self.assertEqual(keys, ["mysql", "mariadb-12", "mariadb-11"])
        for server in self.fleet:
            self.assertTrue(
                server.digest.startswith("sha256:"),
                f"{server.label}: il riferimento deve essere fissato per digest",
            )

    def test_the_runner_asks_for_the_session_measure(self) -> None:
        self.assertIn("session_semantics_evidence", MATRIX.TEST_COMMAND)
        self.assertIn("--ignored", MATRIX.TEST_COMMAND)
        self.assertIn("--nocapture", MATRIX.TEST_COMMAND)
        self.assertEqual(MATRIX.MARKER, "PLENORA_SESSION_EVIDENCE ")

    def test_no_probe_is_compared_by_outcome_alone(self) -> None:
        """Nessuna sonda di sessione ha un dettaglio server-dipendente.

        Se una entrasse in `OUTCOME_ONLY` senza una ragione scritta, il suo
        testo smetterebbe di essere confrontato e una divergenza vera
        passerebbe per accordo.
        """

        self.assertEqual(MATRIX.OUTCOME_ONLY, frozenset())

    def test_a_complete_and_agreeing_matrix_is_accepted(self) -> None:
        self.judge(documents(self.fleet))

    def test_a_diverging_probe_is_refused(self) -> None:
        with self.assertRaises(RuntimeError) as raised:
            self.judge(documents(self.fleet, detail="diverso"))
        self.assertIn("divergenti", str(raised.exception))

    def test_a_shared_failure_is_refused(self) -> None:
        """Il verde falso che questa misura esiste per escludere."""

        with self.assertRaises(RuntimeError) as raised:
            self.judge(documents(self.fleet, outcome="rejected", everywhere=True))
        self.assertIn("non accettate", str(raised.exception))

    def test_a_shrinking_inventory_is_refused(self) -> None:
        """Dodici sonde su tredici non sono la matrice che decide."""

        with self.assertRaises(RuntimeError) as raised:
            self.judge(documents(self.fleet, probes=MATRIX.EXPECTED_PROBES[:-1]))
        self.assertIn("inventario", str(raised.exception))

    def test_a_reordered_inventory_is_refused(self) -> None:
        reordered = (MATRIX.EXPECTED_PROBES[-1],) + MATRIX.EXPECTED_PROBES[:-1]
        with self.assertRaises(RuntimeError) as raised:
            self.judge(documents(self.fleet, probes=reordered))
        self.assertIn("inventario", str(raised.exception))

    def test_a_duplicated_probe_is_refused(self) -> None:
        duplicated = MATRIX.EXPECTED_PROBES[:-1] + (MATRIX.EXPECTED_PROBES[0],)
        with self.assertRaises(RuntimeError) as raised:
            self.judge(documents(self.fleet, probes=duplicated))
        self.assertIn("duplicate", str(raised.exception))

    def test_a_probe_present_on_one_server_only_is_refused(self) -> None:
        built = documents(self.fleet)
        built["mariadb-11"]["observations"] = observations(MATRIX.EXPECTED_PROBES[:-1])
        with self.assertRaises(RuntimeError):
            self.judge(built)

    def recorded_matrix(self) -> str:
        """Il documento generato, se esiste.

        E un artefatto: prima della prima esecuzione non c'e, e pretenderlo
        renderebbe rosso il commit che introduce il runner. Dopo, il runner
        pretende un albero pulito, quindi cio che si legge qui descrive un
        commit preciso. Il salto vale per l'assenza, non per un documento
        vecchio: quello fa fallire le asserzioni sotto, ed e giusto cosi.
        """

        if not MATRIX.EVIDENCE.exists():
            self.skipTest(f"{MATRIX.EVIDENCE} non ancora generato")
        return MATRIX.EVIDENCE.read_text(encoding="utf-8")

    def run_verdict(self, changes, heads, spy):
        """Esegue `verdict()` sostituendo cio che tocca l'host.

        Il documento gia generato non dice se i preflight esistono ancora:
        toglierli dal runner lo lascerebbe identico. Qui si chiama la funzione
        vera, con le sole letture di git e Docker sostituite, e si guarda se
        rifiuta — e se rifiuta **prima** di misurare.
        """

        original = {
            name: getattr(MATRIX, name)
            for name in ("worktree_changes", "head", "servers", "running_digest", "measure")
        }
        changes = list(changes)
        heads = list(heads)
        try:
            MATRIX.worktree_changes = lambda: changes.pop(0) if changes else []
            MATRIX.head = lambda: heads.pop(0) if heads else "commit"
            MATRIX.servers = spy.servers
            MATRIX.running_digest = spy.running_digest
            MATRIX.measure = spy.measure
            return MATRIX.verdict()
        finally:
            for name, value in original.items():
                setattr(MATRIX, name, value)

    def spy(self):
        fleet = MATRIX.servers()
        outer = self

        class Spy:
            def __init__(self) -> None:
                self.measured = 0

            def servers(self):
                return fleet

            def running_digest(self, container):
                for server in fleet:
                    if server.container == container:
                        return server.digest
                raise AssertionError(container)

            def measure(self, server, marker, command):
                self.measured += 1
                return outer_documents(fleet)[server.key]

        def outer_documents(fleet):
            return documents(fleet)

        _ = outer
        return Spy()

    def test_a_dirty_tree_is_refused_before_any_measure(self) -> None:
        spy = self.spy()
        with self.assertRaises(RuntimeError) as raised:
            self.run_verdict([[" M crates/x.rs"]], ["commit"], spy)
        self.assertIn("albero con modifiche non committate", str(raised.exception))
        self.assertEqual(spy.measured, 0, "il rifiuto deve precedere Docker")

    def test_a_head_that_moves_during_the_run_is_refused(self) -> None:
        spy = self.spy()
        with self.assertRaises(RuntimeError) as raised:
            self.run_verdict([[], []], ["prima", "dopo", "dopo"], spy)
        self.assertIn("HEAD e cambiato", str(raised.exception))
        self.assertEqual(spy.measured, 3, "le misure erano gia partite")

    def test_a_tree_touched_during_the_run_is_refused(self) -> None:
        spy = self.spy()
        with self.assertRaises(RuntimeError) as raised:
            self.run_verdict([[], [" M crates/x.rs"]], ["commit"], spy)
        self.assertIn("albero e cambiato durante la misura", str(raised.exception))

    def test_a_live_campaign_exists_and_runs_the_matrix(self) -> None:
        """Il self-test statico non puo accorgersi di un comportamento cambiato.

        Verifica il giudizio del runner su documenti costruiti a mano, che e
        cio che serve su una PR; se domani cambiasse `SESSION_BOOTSTRAP_SQL`
        il documento resterebbe quello di ieri. La campagna live e cio che
        chiude il buco, e deve esistere, essere schedulata e invocabile a
        mano, e rieseguire davvero la misura.
        """

        workflow = (ROOT / ".github" / "workflows" / "session-matrix.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("schedule:", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("scripts/check_session_campaign.py", workflow)
        # E deve accorgersi se la matrice registrata non e piu quella
        # osservata: senza questo, la campagna girerebbe e non direbbe nulla.
        self.assertIn("git diff --exit-code -- docs/mariadb/SESSION-MATRIX.md", workflow)

        # Log e diagnostica **fuori** dal repository. Scritti dentro,
        # sporcherebbero l'albero e il preflight della campagna rifiuterebbe
        # proprio quel file: un gate che non puo passare per costruzione, ed e
        # esattamente com'era scritto la prima volta.
        self.assertIn("$RUNNER_TEMP/session-matrix", workflow)
        self.assertIn("runner.temp", workflow)
        for line in workflow.splitlines():
            stripped = line.strip()
            if stripped.startswith(("mkdir", "| tee", "--diagnostics")):
                self.assertIn(
                    "RUNNER_TEMP",
                    stripped,
                    f"scrive nell'albero di lavoro: {stripped}",
                )
        # Il ciclo di vita delle fixture sta nello script, non nello YAML.
        self.assertNotIn("docker compose", workflow)

        campaign = (ROOT / "scripts" / "check_session_campaign.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("docker-compose.mysql.yml", campaign)
        self.assertIn("docker-compose.mariadb.yml", campaign)
        self.assertIn("--wait", campaign)
        # Mai `--remove-orphans`: cancellerebbe i container degli altri
        # provider su una macchina con piu riferimenti accesi. L'ago cerca
        # l'argomento quotato, non la parola: il commento che spiega perche
        # non si usa la contiene, ed e giusto che la contenga. Ed e composto a
        # pezzi, perche scritto per intero questo file violerebbe la guardia
        # che vieta il flag in tutti i runner del repository.
        self.assertNotIn(chr(34) + "--remove" + "-orphans" + chr(34), campaign)

    def run_campaign(self, tmp, calls, *, fail_on=None, arguments=None):
        """Esegue `main` della campagna con Docker e misura sostituiti.

        Cio che si verifica e l'ordine e il ciclo di vita: preflight prima di
        Docker, avvio dentro il tentativo, pulizia di cio che era partito, e
        l'errore originale che arriva al chiamante invece di essere sostituito
        da quello della pulizia.

        `calls` e del chiamante perche serve anche quando la campagna
        fallisce, ed e proprio allora che dice la cosa interessante.
        """

        original = {
            name: getattr(CAMPAIGN, name)
            for name in ("compose", "run", "preflight", "verdict", "markdown", "EVIDENCE")
        }

        def compose(file, *parameters, capture=False):
            calls.append((file, parameters[0]))
            if fail_on is not None and file == fail_on and parameters[0] == "up":
                raise RuntimeError(f"{file}: avvio fallito")
            return ""

        def preflight():
            calls.append(("git", "preflight"))
            return "commit"

        try:
            CAMPAIGN.compose = compose
            CAMPAIGN.run = lambda command, capture=False: ""
            CAMPAIGN.preflight = preflight
            CAMPAIGN.verdict = lambda: {"totals": {"probes": 13}}
            CAMPAIGN.markdown = lambda document: "matrice" + chr(10)
            CAMPAIGN.EVIDENCE = tmp / "SESSION-MATRIX.md"
            return CAMPAIGN.main(
                arguments if arguments is not None else ["--diagnostics", str(tmp / "diag")]
            )
        finally:
            for name, value in original.items():
                setattr(CAMPAIGN, name, value)

    def test_the_campaign_starts_measures_and_cleans_up(self) -> None:
        import tempfile

        calls: list[tuple[str, str]] = []
        with tempfile.TemporaryDirectory() as directory:
            tmp = Path(directory)
            code = self.run_campaign(tmp, calls)
            self.assertEqual(code, 0)
            self.assertTrue((tmp / "SESSION-MATRIX.md").exists(), "documento non scritto")

        # Il preflight sull'albero precede qualunque comando Docker: farlo dopo
        # significava scoprire l'albero sporco con tre server gia accesi.
        self.assertEqual(calls[0], ("git", "preflight"))
        self.assertEqual(
            [file for file, action in calls if action == "up"],
            list(CAMPAIGN.COMPOSE_FILES),
        )
        # Ogni riferimento viene spento due volte: una prima di accenderlo,
        # per partire da uno stato noto — il compose genera il materiale TLS
        # con un container one-shot, e un `up` su uno stack gia acceso lo
        # rigenera sotto i server che lo usano — e una alla fine, nella
        # pulizia. Quello che conta e che alla fine siano spenti entrambi.
        stopped = [file for file, action in calls if action == "down"]
        for file in CAMPAIGN.COMPOSE_FILES:
            self.assertIn(file, stopped[-len(CAMPAIGN.COMPOSE_FILES) :])
        # E che il primo comando su ogni riferimento sia lo spegnimento, non
        # l'accensione.
        for file in CAMPAIGN.COMPOSE_FILES:
            actions = [action for target, action in calls if target == file]
            self.assertEqual(actions[:3], ["config", "down", "up"], file)

    def test_a_failure_starting_the_second_compose_cleans_up_the_first(self) -> None:
        """Il caso che lasciava container accesi senza nemmeno i log.

        L'avvio stava fuori dal tentativo: un fallimento a meta lasciava su il
        primo compose, senza diagnostica ne pulizia.
        """

        import tempfile

        calls: list[tuple[str, str]] = []
        second = CAMPAIGN.COMPOSE_FILES[1]
        with tempfile.TemporaryDirectory() as directory:
            tmp = Path(directory)
            with self.assertRaises(RuntimeError) as raised:
                self.run_campaign(tmp, calls, fail_on=second)
            self.assertTrue((tmp / "diag").exists(), "diagnostica non raccolta")

        # L'errore che arriva e quello vero, non quello della pulizia.
        self.assertIn("avvio fallito", str(raised.exception))
        # E il primo compose, che era davvero acceso, e stato spento.
        self.assertIn(
            CAMPAIGN.COMPOSE_FILES[0],
            [file for file, action in calls if action == "down"],
            "il primo riferimento e rimasto acceso",
        )

    def test_the_generated_document_declares_it_is_generated(self) -> None:
        text = self.recorded_matrix()
        self.assertIn("non modificare a mano", text)
        self.assertIn("check_session_matrix.py", text)
        for server in self.fleet:
            self.assertIn(server.label, text)

    def test_the_recorded_matrix_was_measured_on_a_clean_tree(self) -> None:
        """Un documento generato da un albero sporco non e autorevole.

        Il commit che dichiara non descriverebbe il codice misurato, e la
        matrice smetterebbe di essere una prova su qualcosa di identificabile.
        """

        text = self.recorded_matrix()
        self.assertIn("albero pulito", text)
        self.assertNotIn("modifiche non committate", text)
        # Il documento non nomina un commit: cambierebbe a ogni corsa, e la
        # campagna che lo rigenera per confrontarlo vedrebbe una differenza
        # sempre, cioe mai una vera.
        self.assertNotIn("Misurata su `", text)
        self.assertIn("0 divergono", text)
        self.assertNotIn("| differs |", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
