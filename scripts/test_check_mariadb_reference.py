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
    COMPATIBILITY,
    COMPOSE_FILE,
    EVIDENCE,
    EVIDENCE_ROLE,
    MISMATCH_ALIAS,
    REFERENCES,
    REFERENCES_FILE,
    validate_compose_pins_the_references,
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

        validate_compose_pins_the_references()
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        for reference in REFERENCES:
            self.assertEqual(
                compose.count(reference.digest), 2, f"{reference.label}: certgen e server"
            )
            self.assertNotIn(f"mariadb:{reference.exact_version}", compose)

        for script in sorted((ROOT / "scripts").glob("*.py")):
            if script.name in {"mariadb_references.py", Path(__file__).name}:
                continue
            source = script.read_text(encoding="utf-8")
            for reference in REFERENCES:
                self.assertNotIn(
                    reference.digest,
                    source,
                    f"{script.name} ricopia il digest invece di chiederlo",
                )

    def test_every_reference_is_pinned_by_an_immutable_digest(self) -> None:
        document = REFERENCES_FILE.read_text(encoding="utf-8")
        self.assertGreaterEqual(len(REFERENCES), 2, "evidence piu la LTS precedente")
        for reference in REFERENCES:
            self.assertIn(reference.digest, document)
            self.assertTrue(reference.digest.startswith("sha256:"))
            self.assertEqual(len(reference.digest), 71)
            self.assertEqual(reference.exact_version.count("."), 2)

    def test_the_previous_lts_is_kept_as_a_compatibility_row(self) -> None:
        """L'evidenza gia raccolta non si butta quando esce una versione.

        Per un fork il confronto fra due versioni **e** evidenza: dice se una
        divergenza dal comportamento MySQL appartiene a MariaDB o a una sua
        release. Toglierla lascerebbe una sola osservazione e nessun modo di
        attribuirla.
        """

        self.assertEqual(len(COMPATIBILITY), 1)
        previous = COMPATIBILITY[0]
        self.assertEqual(previous.exact_version, "11.8.8")
        self.assertLess(previous.major, EVIDENCE.major)
        self.assertNotEqual(previous.tls_volume, EVIDENCE.tls_volume)

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
        self.assertNotIn(
            '"role": "baseline"', REFERENCES_FILE.read_text(encoding="utf-8")
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
        for reference in REFERENCES:
            self.assertIn(reference.exact_version, adr)
            self.assertIn(reference.digest, adr)

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

    def test_every_fixture_can_run_beside_the_others(self) -> None:
        """L'evidenza e un confronto, quindi le fixture devono convivere.

        Tre server insieme: le due MariaDB e MySQL. Progetti Compose distinti
        — altrimenti un `down` su uno cancella i container degli altri — e
        porte distinte sul loopback, altrimenti il secondo non parte e il
        confronto si riduce a una descrizione.
        """

        mariadb = COMPOSE_FILE.read_text(encoding="utf-8")
        mysql = (ROOT / "docker-compose.mysql.yml").read_text(encoding="utf-8")
        self.assertIn("name: plenora-mariadb", mariadb)
        self.assertIn("name: plenora-mysql", mysql)

        def published(compose: str) -> set[str]:
            return set(re.findall(r'"127\.0\.0\.1:(\d+):\d+"', compose))

        mariadb_ports = published(mariadb)
        self.assertEqual(
            mariadb_ports,
            {str(reference.port) for reference in REFERENCES},
            "le porte pubblicate non sono quelle dichiarate",
        )
        self.assertFalse(
            mariadb_ports & published(mysql),
            "una fixture MariaDB pubblica la porta della fixture MySQL",
        )

        containers = set(re.findall(r"container_name: (\S+)", mariadb))
        for reference in REFERENCES:
            self.assertIn(reference.container, containers)
            # Ogni server ha il proprio volume TLS: condividerlo darebbe a uno
            # il certificato emesso per l'altro, e il client lo rifiuterebbe
            # per hostname mismatch — un errore che somiglia a un difetto del
            # provider e non lo e.
            self.assertIn(f"{reference.tls_volume}:/etc/mysql/tls:ro", mariadb)

    def test_the_tls_generator_is_shared_and_takes_the_host_as_an_argument(
        self,
    ) -> None:
        """Un generatore solo, parametrizzato — non una copia per fixture.

        Due copie della stessa fixture TLS divergono alla prima correzione
        applicata a una sola, e la seconda continua a emettere certificati
        con il difetto che la prima ha gia risolto. Il nome host e
        obbligatorio: un default silenzioso emetterebbe per un riferimento il
        certificato di un altro, e il client lo rifiuterebbe per hostname
        mismatch — un errore che non dice quale nome ci si aspettava.
        """

        generator = GENERATOR.read_text(encoding="utf-8")
        self.assertIn("SERVER_HOST=${4:?", generator)
        self.assertNotIn("dataflow-mysql", generator)
        self.assertNotIn("dataflow-mariadb", generator)
        self.assertFalse(
            (ROOT / "docker" / "mariadb" / "tls" / "generate.sh").exists(),
            "una seconda copia del generatore",
        )

        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        self.assertIn("./docker/mysql/tls:/fixture:ro", compose)
        for reference in REFERENCES:
            extensions = sorted(
                (ROOT / "docker" / "mariadb" / "tls").glob("*.ext")
            )
            wanted = f"DNS.1 = {reference.container}"
            extension = next(
                path
                for path in extensions
                if wanted in path.read_text(encoding="utf-8").splitlines()
            )
            self.assertIn(
                f'"/fixture-host/{extension.name}", "{reference.container}"',
                compose,
                f"{reference.label}: certificato emesso per un altro nome",
            )
            self.assertIn("IP.1 = 127.0.0.1", extension.read_text(encoding="utf-8"))
            # L'alias della prova TLS negativa non deve stare nei SAN,
            # altrimenti il rifiuto per identita diventerebbe un successo.
            self.assertNotIn(MISMATCH_ALIAS, extension.read_text(encoding="utf-8"))
        self.assertIn(MISMATCH_ALIAS, compose)


if __name__ == "__main__":
    unittest.main()
