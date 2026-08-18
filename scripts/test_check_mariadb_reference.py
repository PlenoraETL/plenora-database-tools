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

    def test_the_decision_does_not_promise_what_was_not_measured(self) -> None:
        """La scelta del crate non e una qualifica, e l'ADR deve dirlo.

        E il passo dove la confusione costa di piu: "un provider MariaDB
        pubblico" e "MariaDB supportata" si somigliano abbastanza da essere
        letti come la stessa cosa, e non lo sono. La decisione riguarda dove
        vivra il codice; cosa e stato dimostrato lo dice l'evidenza, e tre
        superfici non lo sono ancora.
        """

        adr = ADR.read_text(encoding="utf-8")
        self.assertIn("Decisione architetturale", adr)
        self.assertIn("MariaDB **non e qualificata** da questa decisione", adr)
        self.assertIn("Cosa resta fail-closed", adr)
        # In grassetto e come voce di elenco: e la forma con cui la sezione
        # dichiara una superficie ancora chiusa. Cercare la sola frase
        # lascerebbe passare "commit ambiguo, risolto", che dice il contrario.
        closed = ADR.read_text(encoding="utf-8").split("Cosa resta fail-closed", 1)[1]
        for surface in ("**spatial su MariaDB**", "**commit ambiguo**", "**lettura via catalogo**"):
            self.assertIn(
                f"* {surface} —",
                closed,
                f"superficie non piu dichiarata fail-closed: {surface}",
            )

        # Nessuna selezione automatica: e la proprieta che impedisce a un
        # consumer di finire sull'altro motore senza accorgersene.
        self.assertIn("Nessuna selezione automatica", adr)
        self.assertIn("`MariadbProvider` rifiuta MySQL", adr)

        # E ogni riga del profilo deve poggiare su una misura, non su una
        # previsione: i codici osservati sono la prova che c'e stata.
        for evidence in ("1193", "1054", "native_type=json", "SRS_ID"):
            self.assertIn(evidence, adr, f"riga del profilo senza misura: {evidence}")

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


class MariadbDivergenceMatrixTests(unittest.TestCase):
    """Il documento dell'evidenza segue il catalogo che la produce.

    Una matrice trascritta a mano invecchia alla prima sonda aggiunta, e chi
    la legge non ha modo di distinguere una riga misurata da una rimasta
    indietro. Qui il catalogo e il documento si confrontano per nome.
    """

    HARNESS = ROOT / "scripts" / "check_mariadb_divergence.py"
    DOCUMENT = ROOT / "docs" / "mariadb" / "EVIDENCE.md"

    def probe_identifiers(self) -> set[str]:
        source = self.HARNESS.read_text(encoding="utf-8")
        # Il primo campo di ogni `Probe(...)` e l'identificatore, e sta
        # sulla riga successiva alla parentesi.
        pattern = "".join([r"Probe\(", r"\s+", r'"([a-z_]+\.[a-z_]+)"'])
        return set(re.findall(pattern, source))

    def test_every_probe_is_recorded_in_the_document(self) -> None:
        probes = self.probe_identifiers()
        self.assertGreaterEqual(len(probes), 15, "catalogo non riconosciuto")
        document = self.DOCUMENT.read_text(encoding="utf-8")
        documented = set(re.findall(r"`([a-z_]+\.[a-z_]+)`", document))
        self.assertEqual(
            probes - documented,
            set(),
            "sonde misurate ma non registrate nel documento",
        )
        # Il documento nomina anche tabelle di sistema — `information_schema.
        # statistics`, `performance_schema.prepared_statements_instances` —
        # che hanno la stessa forma di un identificatore di sonda. Si
        # confronta solo cio che porta un prefisso del catalogo, altrimenti
        # la guardia chiederebbe di registrare come sonda ogni tabella
        # citata.
        surfaces = {name.split(".", 1)[0] for name in probes}
        self.assertEqual(
            {name for name in documented if name.split(".", 1)[0] in surfaces}
            - probes,
            set(),
            "il documento riporta sonde che il catalogo non contiene piu",
        )

    def test_the_document_says_how_to_reproduce_the_measure(self) -> None:
        """Una misura senza il comando che la rifa e un'affermazione."""

        document = self.DOCUMENT.read_text(encoding="utf-8")
        self.assertIn("python scripts/check_mariadb_divergence.py", document)
        self.assertIn("docker-compose.mariadb.yml", document)
        self.assertIn("docker-compose.mysql.yml", document)

    def test_the_document_does_not_turn_evidence_into_a_decision(self) -> None:
        """Misurare non e decidere, e il documento deve dirlo.

        E la riga che impedisce alla matrice di essere letta come un via
        libera: tre divergenze dichiarate si sono rivelate false, e da li a
        concludere "allora si puo qualificare" il passo e corto — mentre due
        divergenze nuove, che nessuno aveva nominato, romperebbero il provider
        in produzione.
        """

        document = self.DOCUMENT.read_text(encoding="utf-8")
        self.assertIn("Non e una decisione", document)
        self.assertIn("il fail-close resta", document.lower())
        self.assertIn("Cosa resta aperto", document)

    def test_the_harness_measures_the_surfaces_the_provider_uses(self) -> None:
        """Le sonde devono nominare cio che il provider emette davvero.

        Una matrice di curiosita sul motore non serve a decidere: le due
        divergenze che contano — `MAX_EXECUTION_TIME` e la colonna
        `EXPRESSION` — si sono viste solo perche il catalogo parte da cio che
        il provider esegue, non da cio che i due motori hanno di diverso.
        """

        harness = self.HARNESS.read_text(encoding="utf-8")
        transaction = (
            ROOT / "crates" / "plenora-db-mysql" / "src" / "transaction.rs"
        ).read_text(encoding="utf-8")
        catalog = CATALOG.read_text(encoding="utf-8")

        self.assertIn("MAX_EXECUTION_TIME", harness)
        self.assertIn("MAX_EXECUTION_TIME", transaction)
        self.assertIn("EXPRESSION", harness)
        self.assertIn("expression", catalog)
        self.assertIn("SET SESSION TRANSACTION ISOLATION LEVEL", harness)
        self.assertIn("SET SESSION TRANSACTION ISOLATION LEVEL", transaction)
        self.assertIn("plenora_ctx_", harness)
        self.assertIn("plenora_ctx_", transaction)


class MariadbDriverEvidenceTests(unittest.TestCase):
    """Il bypass resta di solo test, e la misura resta una misura.

    E la guardia che tiene separate due cose che si somigliano: attraversare
    il rifiuto per **misurare** e attraversarlo per **supportare**. La prima
    e cio che ADR 0014 chiede, la seconda e la decisione che l'ADR rimanda.
    """

    CATALOG = ROOT / "crates" / "plenora-db-mysql" / "src" / "catalog.rs"
    EVIDENCE = ROOT / "crates" / "plenora-db-mysql" / "src" / "mariadb_evidence.rs"
    LIB = ROOT / "crates" / "plenora-db-mysql" / "src" / "lib.rs"
    RUNNER = ROOT / "scripts" / "check_mariadb_driver.py"
    DOCUMENT = ROOT / "docs" / "mariadb" / "EVIDENCE.md"

    def test_the_bypass_exists_only_in_test_builds(self) -> None:
        """Ogni pezzo del bypass e dietro `cfg(test)`, e il modulo pure.

        Una feature, una variabile d'ambiente o un parametro pubblico
        renderebbero il rifiuto disattivabile da fuori: sarebbe supporto
        silenzioso, non misura.
        """

        catalog = self.CATALOG.read_text(encoding="utf-8")
        # Le quattro dichiarazioni del bypass, ognuna con la sua `cfg`.
        newline = chr(10)
        for declaration in (
            "static MARIADB_REJECTION_BYPASS",
            "pub(crate) struct MariadbRejectionBypass",
            "impl Drop for MariadbRejectionBypass",
            "fn mariadb_rejection_bypassed",
        ):
            position = catalog.find(declaration)
            self.assertGreater(position, 0, f"dichiarazione assente: {declaration}")
            preceding = catalog[:position].rstrip().rsplit(newline, 1)[-1].strip()
            self.assertIn(
                "cfg(",
                preceding,
                f"{declaration} non e dietro una cfg: {preceding}",
            )
        self.assertIn("#[cfg(not(test))]", catalog)
        # Ripristinabile: un interruttore che si accende e basta lascerebbe il
        # rifiuto disattivato per il resto del processo di test.
        self.assertIn("fn drop(&mut self)", catalog)
        self.assertIn("MARIADB_REJECTION_BYPASS.store(false", catalog)
        evidence = self.EVIDENCE.read_text(encoding="utf-8")
        self.assertIn("MariadbRejectionBypass::engage()", evidence)
        # Fuori dai test la funzione e `false` costante, quindi la condizione
        # del rifiuto resta quella di prima.
        self.assertIn("    false" + chr(10) + "}", catalog)
        self.assertIn(
            "if looks_like_mariadb && !mariadb_rejection_bypassed() {", catalog
        )

        # Nessuna superficie pubblica lo espone.
        self.assertNotIn("pub fn bypass", catalog)
        self.assertNotIn("PLENORA_MARIADB", catalog)
        manifest = (
            ROOT / "crates" / "plenora-db-mysql" / "Cargo.toml"
        ).read_text(encoding="utf-8")
        self.assertNotIn("mariadb", manifest.lower(), "una feature per MariaDB")

        # E il modulo della misura non entra nel binario pubblico.
        library = self.LIB.read_text(encoding="utf-8")
        self.assertIn("#[cfg(test)]" + chr(10) + "mod mariadb_evidence;", library)

    def test_the_measure_does_not_branch_on_the_engine(self) -> None:
        """La misura non aggira cio che sta misurando.

        Un ramo che scegliesse un'altra istruzione quando il server e MariaDB
        — `max_statement_time` invece di `MAX_EXECUTION_TIME`, o una query
        senza `EXPRESSION` — trasformerebbe l'evidenza in una dimostrazione
        che si puo fare, che e una risposta alla domanda ancora aperta.
        """

        evidence = self.EVIDENCE.read_text(encoding="utf-8")
        executable = chr(10).join(
            line
            for line in evidence.splitlines()
            if not line.lstrip().startswith("//")
        )
        for forbidden in ("max_statement_time", "is_mariadb", "if mariadb"):
            self.assertNotIn(
                forbidden,
                executable,
                f"la misura si adatta al motore invece di misurarlo: {forbidden}",
            )

    def test_the_two_families_stay_separate(self) -> None:
        """`raw` e `provider` rispondono a due domande, e restano distinte."""

        evidence = self.EVIDENCE.read_text(encoding="utf-8")
        self.assertIn('"raw"', evidence)
        self.assertIn('"provider"', evidence)
        runner = self.RUNNER.read_text(encoding="utf-8")
        self.assertIn('"families"', runner)
        document = self.DOCUMENT.read_text(encoding="utf-8")
        self.assertIn("| raw |", document)
        self.assertIn("| provider |", document)

    def test_the_ambiguous_commit_stays_not_measured(self) -> None:
        """Senza fault injection deterministica non si conclude niente.

        E la sonda dove la tentazione di dedurre e piu forte: il commit e
        andato, quindi "probabilmente" il provider lo classifica bene. Un
        verdetto che confondesse un esito assente con uno negativo — o
        positivo — porterebbe a decidere su una prova mai fatta.
        """

        evidence = self.EVIDENCE.read_text(encoding="utf-8")
        self.assertIn('"provider.ambiguous_commit"', evidence)
        self.assertIn("not_measured", evidence)
        self.assertIn("fault injection deterministica", evidence)
        self.assertIn("not_measured", self.DOCUMENT.read_text(encoding="utf-8"))

    def test_every_driver_probe_is_recorded_in_the_document(self) -> None:
        """La matrice del documento segue le sonde che la producono."""

        evidence = self.EVIDENCE.read_text(encoding="utf-8")
        probes = set(re.findall(r'"((?:raw|provider)\.[a-z_]+)"', evidence))
        self.assertGreaterEqual(len(probes), 15, "catalogo driver non riconosciuto")
        documented = set(
            re.findall(r"`((?:raw|provider)\.[a-z_]+)`", self.DOCUMENT.read_text(encoding="utf-8"))
        )
        self.assertEqual(
            probes - documented, set(), "sonde misurate e non registrate"
        )
        self.assertEqual(
            documented - probes, set(), "il documento cita sonde inesistenti"
        )


class MariadbDriverRunnerTests(unittest.TestCase):
    """Il runner della misura: cosa dichiara di aver misurato, e su cosa.

    Il runner e l'unico modo di produrre il verdetto, quindi cio che il
    verdetto afferma dipende da lui. Queste guardie tengono ferme le tre
    affermazioni che un lettore da per vere senza poterle controllare:
    l'immagine su cui la misura e girata, il commit del codice misurato, e il
    fatto che le sonde siano le stesse sui tre server.
    """

    RUNNER = ROOT / "scripts" / "check_mariadb_driver.py"

    def test_the_runner_verifies_the_image_it_measured(self) -> None:
        """Digest dichiarato e digest in esecuzione devono coincidere.

        Il documento dice quale immagine dovrebbe girare; `docker inspect`
        dice quale gira. Registrare solo il primo farebbe passare per misurata
        su una versione una corsa fatta su un'immagine sostituita sotto lo
        stesso nome — il caso che il pin per digest esiste per escludere.
        """

        source = self.RUNNER.read_text(encoding="utf-8")
        self.assertIn("def running_digest(", source)
        self.assertIn('"{{.Image}}"', source)
        self.assertIn("declared_digest", source)
        self.assertIn("running_digest", source)
        self.assertIn("la misura non riguarderebbe", source)

    def test_the_runner_records_the_code_it_measured(self) -> None:
        """Commit e stato dell'albero, come nel gate del SDK."""

        source = self.RUNNER.read_text(encoding="utf-8")
        self.assertIn("def repository_state(", source)
        self.assertIn('"status", "--porcelain", "-uall"', source)
        self.assertIn('"worktree_dirty"', source)
        self.assertIn('"commit"', source)

    def test_the_runner_refuses_a_probe_set_that_differs(self) -> None:
        """Le sonde devono essere le stesse sui tre server.

        Un confronto fra insiemi diversi non e un confronto: se un server
        producesse una sonda in meno, la riga corrispondente sparirebbe dalla
        matrice invece di comparire come divergenza.
        """

        source = self.RUNNER.read_text(encoding="utf-8")
        self.assertIn("sonda {probe} assente", source)
        self.assertIn("devono", source)

    def test_the_runner_reads_versions_and_digests_from_the_documents(self) -> None:
        """Nessuna versione o digest ricopiato nel runner."""

        source = self.RUNNER.read_text(encoding="utf-8")
        self.assertIn("from scripts.mariadb_references import", source)
        self.assertIn("from scripts.mysql_references import", source)
        # Solo le righe eseguibili: la docstring nomina le versioni per dire
        # su cosa gira la misura, e vietarlo obbligherebbe a togliere la
        # spiegazione insieme al valore.
        executable = chr(10).join(
            line
            for line in source.splitlines()
            if not line.lstrip().startswith(("#", '"""', "*", "`"))
        )
        for reference in REFERENCES:
            self.assertNotIn(reference.digest, executable)

    def test_the_evidence_test_is_not_in_the_mysql_inventory(self) -> None:
        """La misura non entra negli inventari della qualifica MySQL.

        I tre runner del gate filtrano il prefisso `live_`, e i loro inventari
        dichiarano cosa il provider **MySQL** ha dimostrato. Un test che misura
        MariaDB non puo sostenere quell'affermazione, e finirebbe per essere
        eseguito contro il riferimento sbagliato.
        """

        from scripts.mysql_inventory import EXCLUDED_SOURCES, collect

        self.assertIn("mariadb_evidence.rs", EXCLUDED_SOURCES)
        evidence = (
            ROOT / "crates" / "plenora-db-mysql" / "src" / "mariadb_evidence.rs"
        ).read_text(encoding="utf-8")
        self.assertIn("async fn mariadb_driver_evidence()", evidence)
        self.assertNotIn("async fn live_mariadb_driver_evidence", evidence)
        self.assertIn("#[ignore", evidence)

        inventory = collect()
        for family in inventory.values():
            self.assertFalse(
                [name for name in family if "evidence" in name],
                "la misura e finita nell'inventario MySQL",
            )


if __name__ == "__main__":
    unittest.main()
