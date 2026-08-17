#!/usr/bin/env python3
"""Guardie della fixture di evidenza MariaDB.

Il ciclo MariaDB apre con una fixture che **non** e un riferimento
qualificato: il provider `mysql` fa fail-close alla probe quando la
riconosce, e questa fixture serve a produrre l'evidenza che ADR 0014 chiede
prima di decidere se qualificarla. Le guardie qui sotto tengono ferme le due
cose che quella distinzione richiede — l'immagine e fissata per digest, e
niente in giro dichiara MariaDB supportata.

Nessun server: si legge cio che i documenti e i compose affermano. La fixture
viva la esercita il ciclo, non questa suite.
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.mariadb_references import (  # noqa: E402
    COMPOSE_FILE,
    CONTAINER,
    EVIDENCE,
    EVIDENCE_ROLE,
    MISMATCH_ALIAS,
    REFERENCES_FILE,
    validate_compose_pins_the_reference,
)

ADR = ROOT / "docs" / "adr" / "0014-mariadb-evidence-first.md"
CATALOG = ROOT / "crates" / "plenora-db-mysql" / "src" / "catalog.rs"
GENERATOR = ROOT / "docker" / "mysql" / "tls" / "generate.sh"
SERVER_EXT = ROOT / "docker" / "mariadb" / "tls" / "server.ext"


class MariadbEvidenceFixtureTests(unittest.TestCase):
    def test_the_reference_document_is_the_only_place_with_the_digest(self) -> None:
        """Versione e digest stanno in un posto solo.

        Ricopiati nel compose o in uno script diventano una seconda fonte: al
        primo aggiornamento le due divergono, e il documento che dice "questa
        e l'immagine avviata" smette di essere vero senza che nulla fallisca.
        Il compose li **usa**, e la validazione confronta le due cose invece
        di fidarsi.
        """

        validate_compose_pins_the_reference()
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        self.assertEqual(compose.count(EVIDENCE.digest), 2, "certgen e server")
        self.assertNotIn(f"mariadb:{EVIDENCE.exact_version}", compose)

        for script in sorted((ROOT / "scripts").glob("*.py")):
            if script.name in {"mariadb_references.py", Path(__file__).name}:
                continue
            source = script.read_text(encoding="utf-8")
            self.assertNotIn(
                EVIDENCE.digest,
                source,
                f"{script.name} ricopia il digest invece di chiederlo",
            )

    def test_the_reference_is_pinned_by_an_immutable_digest(self) -> None:
        document = REFERENCES_FILE.read_text(encoding="utf-8")
        self.assertIn(EVIDENCE.digest, document)
        self.assertTrue(EVIDENCE.digest.startswith("sha256:"))
        self.assertEqual(len(EVIDENCE.digest), 71)
        self.assertEqual(EVIDENCE.exact_version.count("."), 2)

    def test_the_fixture_declares_itself_evidence_and_not_a_baseline(self) -> None:
        """Il ruolo e `evidence`, e non e una sfumatura lessicale.

        `baseline` in questo repository significa "riferimento qualificato":
        e la riga di testa di una matrice che i gate eseguono e che i
        documenti dichiarano supportata. MariaDB non lo e — il provider la
        rifiuta alla probe — e un ruolo che suggerisce il contrario
        rimetterebbe in circolo proprio l'equivoco che il fail-close esiste
        per impedire.
        """

        self.assertEqual(EVIDENCE.role, EVIDENCE_ROLE)
        self.assertEqual(EVIDENCE_ROLE, "evidence")
        for role in ("baseline", "compatibility"):
            self.assertNotIn(
                f'"role": "{role}"', REFERENCES_FILE.read_text(encoding="utf-8")
            )

    def test_the_provider_still_fails_closed_on_mariadb(self) -> None:
        """Il fail-close resta finche una capability non ha una prova.

        E la decisione di ADR 0014, e questa guardia e cio che impedisce di
        allentarla per comodita mentre si raccoglie l'evidenza: un provider
        che accetta e poi diverge in silenzio e peggio di uno che rifiuta.
        """

        catalog = CATALOG.read_text(encoding="utf-8")
        self.assertIn("looks_like_mariadb", catalog)
        self.assertIn("ErrorCategory::Unsupported", catalog)
        self.assertIn("RemoteEffect::None", catalog)
        # Riconosciuta da entrambe le stringhe che il server pubblica: una
        # sola basterebbe a farla passare se il fork cambiasse l'altra.
        self.assertIn("product_version.to_ascii_lowercase().contains(\"mariadb\")", catalog)
        self.assertIn("version_comment.to_ascii_lowercase().contains(\"mariadb\")", catalog)

        adr = ADR.read_text(encoding="utf-8")
        self.assertIn("Il **fail-close resta**", adr)
        self.assertIn(EVIDENCE.exact_version, adr)
        self.assertIn(EVIDENCE.digest, adr)

    def test_no_surface_declares_mariadb_qualified(self) -> None:
        """Nessun documento corrente puo dire che MariaDB e supportata.

        La fixture rende MariaDB avviabile, ed e proprio quando una cosa
        diventa avviabile che qualcuno la descrive come disponibile.
        """

        documents = [ROOT / "README.md"]
        documents += sorted((ROOT / "docs").rglob("*.md"))
        documents += sorted((ROOT / "crates").rglob("README.md"))
        claims = (
            "MariaDB e qualificata",
            "MariaDB è qualificata",
            "MariaDB supportata",
            "provider MariaDB disponibile",
        )
        for document in documents:
            if "/docs/history/" in document.as_posix():
                continue
            text = document.read_text(encoding="utf-8")
            for claim in claims:
                self.assertNotIn(
                    claim,
                    text,
                    f"{document.relative_to(ROOT).as_posix()} dichiara "
                    f"'{claim}' mentre il provider la rifiuta",
                )

    def test_the_two_fixtures_can_run_side_by_side(self) -> None:
        """L'evidenza e un confronto, quindi le due fixture devono convivere.

        Progetti Compose distinti — altrimenti un `down` su una cancella i
        container dell'altra — e porte distinte sul loopback, altrimenti la
        seconda non parte e il confronto si riduce a una descrizione.
        """

        mariadb = COMPOSE_FILE.read_text(encoding="utf-8")
        mysql = (ROOT / "docker-compose.mysql.yml").read_text(encoding="utf-8")
        self.assertIn("name: plenora-mariadb", mariadb)
        self.assertIn("name: plenora-mysql", mysql)

        def published(compose: str) -> set[str]:
            return set(re.findall(r'"127\.0\.0\.1:(\d+):\d+"', compose))

        self.assertTrue(published(mariadb))
        self.assertFalse(
            published(mariadb) & published(mysql),
            "le due fixture pubblicano la stessa porta sul loopback",
        )
        self.assertIn(f"container_name: {CONTAINER}", mariadb)

    def test_the_tls_generator_is_shared_and_takes_the_host_as_an_argument(
        self,
    ) -> None:
        """Un generatore solo, parametrizzato — non due copie.

        Due copie della stessa fixture TLS divergono alla prima correzione
        applicata a una sola, e la seconda continua a emettere certificati
        con il difetto che la prima ha gia risolto. Il nome host e
        obbligatorio: un default silenzioso emetterebbe per MariaDB un
        certificato valido per l'altro riferimento, e il client lo
        rifiuterebbe per hostname mismatch — un errore che non dice quale
        nome ci si aspettava.
        """

        generator = GENERATOR.read_text(encoding="utf-8")
        self.assertIn("SERVER_HOST=${4:?", generator)
        self.assertNotIn("dataflow-mysql", generator)
        self.assertNotIn("dataflow-mariadb", generator)

        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        self.assertIn("./docker/mysql/tls:/fixture:ro", compose)
        self.assertIn(f'"/fixture-host/server.ext", "{CONTAINER}"', compose)
        self.assertFalse(
            (ROOT / "docker" / "mariadb" / "tls" / "generate.sh").exists(),
            "una seconda copia del generatore",
        )

        extension = SERVER_EXT.read_text(encoding="utf-8")
        self.assertIn(f"DNS.1 = {CONTAINER}", extension)
        self.assertIn("IP.1 = 127.0.0.1", extension)
        # L'alias della prova TLS negativa non deve stare nei SAN, altrimenti
        # il rifiuto per identita diventerebbe un successo.
        self.assertNotIn(MISMATCH_ALIAS, extension)
        self.assertIn(MISMATCH_ALIAS, compose)


if __name__ == "__main__":
    unittest.main()
