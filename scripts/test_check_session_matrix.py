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


def document(outcome: str, detail: str) -> dict[str, object]:
    return {
        "server": {"product_version": "x", "version_comment": "y"},
        "bootstrap_sql": "SET SESSION autocommit = 1",
        "observations": [
            {
                "probe": "bootstrap.statement",
                "family": "session",
                "surface": "bootstrap",
                "question": "domanda",
                "outcome": outcome,
                "detail": detail,
                "server_code": None,
            }
        ],
    }


class SessionMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fleet = MATRIX.servers()

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

    def test_a_diverging_probe_is_named(self) -> None:
        documents = {
            server.key: document(
                "accepted", "uguale" if server.key == "mysql" else "diverso"
            )
            for server in self.fleet
        }
        results = MATRIX.compare(documents, self.fleet, MATRIX.OUTCOME_ONLY)
        self.assertEqual(results[0]["verdict"], "differs")
        self.assertEqual(sorted(results[0]["divergent"]), ["mariadb-11", "mariadb-12"])

    def test_a_shared_failure_is_not_agreement(self) -> None:
        """Il verde falso che questa misura esiste per escludere."""

        documents = {
            server.key: document("rejected", "uguale") for server in self.fleet
        }
        results = MATRIX.compare(documents, self.fleet, MATRIX.OUTCOME_ONLY)
        self.assertEqual(results[0]["verdict"], "same")
        unaccepted = [
            key
            for entry in results
            for key, observation in entry["observations"].items()
            if observation["outcome"] != "accepted"
        ]
        self.assertEqual(len(unaccepted), 3, "il runner deve poterle contare")

    def test_the_generated_document_declares_it_is_generated(self) -> None:
        evidence = MATRIX.EVIDENCE
        self.assertTrue(evidence.exists(), f"{evidence} deve esistere")
        text = evidence.read_text(encoding="utf-8")
        self.assertIn("non modificare a mano", text)
        self.assertIn("check_session_matrix.py", text)
        for server in self.fleet:
            self.assertIn(server.label, text)

    def test_the_recorded_matrix_still_justifies_the_decision(self) -> None:
        """Il documento committato deve dire cio su cui la decisione poggia.

        Se una futura esecuzione registrasse una divergenza, il runner
        fallirebbe prima di scriverla; ma il documento resta nel repository, e
        va letto anche da chi non riesegue la misura.
        """

        text = MATRIX.EVIDENCE.read_text(encoding="utf-8")
        self.assertIn("0 divergono", text)
        self.assertNotIn("| differs |", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
