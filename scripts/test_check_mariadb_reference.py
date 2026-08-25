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

import itertools
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

# La decisione che governa questa campagna sta con la campagna, non
# nell'archivio: le altre ADR registrano scelte concluse, questa e
# ancora in corso di esecuzione.
ADR = ROOT / "docs" / "mariadb" / "ADR-0014-evidence-first.md"
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

        # Il riconoscimento e la sua motivazione vivono ora nel profilo; il
        # punto in cui il rifiuto scatta, e con esso il bypass, e rimasto nel
        # catalogo. La guardia verifica entrambi, perche il fail-close regge
        # solo se stanno insieme.
        catalog = CATALOG.read_text(encoding="utf-8")
        profile = (
            ROOT / "crates" / "plenora-db-mysql" / "src" / "profile.rs"
        ).read_text(encoding="utf-8")
        self.assertIn("looks_like_mariadb", profile)
        self.assertIn("ErrorCategory::Unsupported", profile)
        self.assertIn("RemoteEffect::None", profile)
        # Riconosciuta da entrambe le stringhe che il server pubblica: una
        # sola basterebbe a farla passare se il fork cambiasse l'altra.
        self.assertIn("product_version.to_ascii_lowercase().contains(\"mariadb\")", profile)
        self.assertIn("version_comment.to_ascii_lowercase().contains(\"mariadb\")", profile)
        self.assertIn("foreign_product_rejection", catalog)

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
        for surface in ("**spatial su MariaDB**", "**commit ambiguo**"):
            self.assertIn(
                f"* {surface} —",
                closed,
                f"superficie non piu dichiarata fail-closed: {surface}",
            )

        # La lettura via catalogo era la terza, ed e stata misurata: l'elenco
        # non puo continuare a dichiararla chiusa, e l'ADR non puo tacerlo.
        # La guardia pretende entrambe le cose, perche togliere la voce senza
        # spiegare dove sia finita e il modo in cui un documento smette di
        # essere leggibile.
        self.assertNotIn("**lettura via catalogo** —", closed)
        self.assertIn("la lettura via catalogo e stata misurata", adr)
        self.assertIn("nessun provider seleziona quel profilo", adr)

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
        #
        # Restano pero i **campi di capability**, che con `spatial` collidono
        # davvero: `spatial.read_wkb` e `spatial.functions` hanno la forma di
        # una sonda della superficie `spatial` e non lo sono. La convenzione
        # del documento e scriverli per intero — `SpatialCapabilities::functions`
        # — e questa guardia e cio che la fa rispettare. Tre tranche di fila ci
        # sono inciampate: se ci inciampa una quarta, il messaggio qui sotto e
        # quello che deve dirle cosa fare.
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
        # L'estrazione del profilo ha spostato la decisione su *quale*
        # variabile scrivere: la transazione emette il timeout, ma il nome e
        # l'unita li decide il profilo. L'asserzione segue il codice, non il
        # file in cui stava.
        profile = (
            ROOT / "crates" / "plenora-db-mysql" / "src" / "profile.rs"
        ).read_text(encoding="utf-8")

        self.assertIn("MAX_EXECUTION_TIME", harness)
        self.assertIn("MAX_EXECUTION_TIME", profile)
        self.assertIn("statement_timeout_statement", transaction)
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
        # La forma della condizione e cambiata con l'estrazione del profilo —
        # il bypass ora avvolge il rifiuto invece di essere una congiunzione —
        # ma cio che deve restare vero e lo stesso: il rifiuto passa di li, e
        # senza bypass scatta.
        self.assertIn("if !mariadb_rejection_bypassed() {", catalog)
        self.assertIn("return Err(rejection);", catalog)

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

    def test_the_ambiguous_commit_is_measured_and_verified_from_outside(self) -> None:
        """La sonda del commit ignoto verifica **due** cose, non una.

        Questa guardia diceva un'altra cosa, ed e stata riscritta il giorno in
        cui ha smesso di valere — che era il suo scopo. Diceva: senza fault
        injection deterministica non si conclude niente, ed era vero finche
        l'unico modo immaginato era uccidere la connessione a meta commit, che
        e una corsa. La forma deterministica c'era gia nel provider SQL Server
        di questo repository: far atterrare il commit e **poi** trattenere la
        risposta.

        Cio che va presidiato ora e piu stretto della misura stessa. Che il
        provider dichiari `OutcomeUnknown` e meta della prova; l'altra meta e
        che quella dichiarazione sia onesta, e si vede solo rileggendo il
        server da un'altra connessione. Una sonda che si fermasse alla prima
        meta chiamerebbe prova un'alzata di spalle, e `Unknown` verrebbe
        accettato anche dove il commit non e mai atterrato — cioe dove la
        misura riguarda un altro caso.
        """

        evidence = self.EVIDENCE.read_text(encoding="utf-8")
        self.assertIn('"provider.ambiguous_commit"', evidence)
        # Il seam e deterministico e vive nel percorso di produzione: e
        # l'interruttore a cambiare il testo dello statement, non una copia
        # della logica che ne classifica l'esito.
        self.assertIn("DelayedCommitResponse", evidence)
        # E la rilettura da fuori, che e la meta che distingue una prova da
        # una citazione.
        self.assertIn("commit_contents", evidence)
        self.assertIn("OutcomeUnknown", evidence)

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

    def test_a_probe_that_supports_a_capability_makes_the_gate_red(self) -> None:
        """Una prova che viene a mancare non e piu solo un'osservazione.

        E la distanza fra le due cose che il runner faceva coincidere: la
        matrice **osserva** due motori diversi, e un rifiuto identico su tutti
        e tre e `same`, che usciva con zero. Ma se quel rifiuto riguarda una
        sonda su cui poggia una capability gia pubblicata, la promessa resta e
        la prova no.
        """

        from scripts.check_mariadb_driver import (
            QUALIFICATION_PROBES,
            REQUIRED_ACCEPTED_PROBES,
            REQUIRED_REJECTED_PROBES,
            capability_violations,
        )

        def document(outcomes: dict[str, str]) -> dict[str, object]:
            return {
                "results": [
                    {
                        "probe": probe,
                        "observations": {
                            server: {"outcome": outcome}
                            for server in ("mysql", "mariadb-12", "mariadb-11")
                        },
                    }
                    for probe, outcome in outcomes.items()
                ]
            }

        healthy = {probe: "accepted" for probe in REQUIRED_ACCEPTED_PROBES}
        healthy.update({probe: "rejected" for probe in REQUIRED_REJECTED_PROBES})
        # La terza famiglia: qualifica una superficie non ancora pubblicata, e
        # ciascuna sonda dichiara da se cosa deve rendere.
        healthy.update(
            {probe: expected for probe, (_, expected) in QUALIFICATION_PROBES.items()}
        )
        self.assertEqual(capability_violations(document(healthy)), [])

        # Una prova positiva che diventa un rifiuto, su un server solo.
        regressed = dict(healthy)
        first = sorted(REQUIRED_ACCEPTED_PROBES)[0]
        broken = document(regressed)
        entry = next(item for item in broken["results"] if item["probe"] == first)
        entry["observations"]["mariadb-11"]["outcome"] = "rejected"
        violations = capability_violations(broken)
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("mariadb-11", violations[0])
        self.assertIn("atteso accepted", violations[0])

        # Un fail-close che smette di chiudere.
        opened = dict(healthy)
        closed_probe = sorted(REQUIRED_REJECTED_PROBES)[0]
        opened[closed_probe] = "accepted"
        violations = capability_violations(document(opened))
        self.assertEqual(len(violations), 3, violations)
        self.assertTrue(all("atteso rejected" in entry for entry in violations))

        # Un fail-close rifiutato per un'altra ragione: la sonda lo registra
        # come `not_measured`, e un fail-close non verificato non e verificato.
        wrong_reason = dict(healthy)
        wrong_reason[closed_probe] = "not_measured"
        violations = capability_violations(document(wrong_reason))
        self.assertEqual(len(violations), 3, violations)
        self.assertTrue(all("not_measured" in entry for entry in violations))

        # E una sonda sparita non e un problema in meno.
        missing = {
            probe: outcome for probe, outcome in healthy.items() if probe != first
        }
        violations = capability_violations(document(missing))
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("sonda assente", violations[0])

    def expected_probes(self) -> tuple[str, ...]:
        from scripts.check_mariadb_driver import EXPECTED_PROBES

        return EXPECTED_PROBES

    def document(self, names: list[str]) -> dict[str, object]:
        return {
            "observations": [
                {
                    "probe": name,
                    "outcome": "accepted",
                    "detail": "d",
                    "server_code": None,
                    "family": "provider",
                    "surface": "profilo",
                    "question": "q",
                }
                for name in names
            ]
        }

    def test_the_inventory_is_checked_on_every_server_not_only_the_baseline(self) -> None:
        """Dopo l'allineamento l'informazione non c'e piu.

        L'elenco delle sonde viene dal solo server di riferimento, e gli altri
        passano da un dizionario: una sonda in piu — o spostata — su MariaDB
        sparirebbe dall'allineamento, e il documento finale conserverebbe le
        sonde di MySQL intatte. Il controllo va fatto su ogni documento grezzo,
        prima che l'allineamento le perda.
        """

        from scripts.check_mariadb_driver import OUTCOME_ONLY, compare, servers

        fleet = servers()
        healthy = list(self.expected_probes())

        # Una sonda in piu su un server che **non** e il riferimento.
        extra = {server.key: self.document(healthy) for server in fleet}
        extra[fleet[2].key] = self.document(healthy + ["raw.inventata"])
        with self.assertRaises(RuntimeError) as raised:
            compare(extra, fleet, OUTCOME_ONLY, self.expected_probes())
        self.assertIn(fleet[2].key, str(raised.exception))
        self.assertIn("raw.inventata", str(raised.exception))

        # Lo stesso insieme, in un altro ordine, sempre fuori dal riferimento:
        # senza il confronto ordinato, l'allineamento per nome non se ne
        # accorgerebbe mai.
        reordered = list(healthy)
        reordered[0], reordered[1] = reordered[1], reordered[0]
        shuffled = {server.key: self.document(healthy) for server in fleet}
        shuffled[fleet[1].key] = self.document(reordered)
        with self.assertRaises(RuntimeError) as raised:
            compare(shuffled, fleet, OUTCOME_ONLY, self.expected_probes())
        self.assertIn(fleet[1].key, str(raised.exception))
        self.assertIn("altro ordine", str(raised.exception))

    def test_every_read_probe_is_classified_by_the_runner(self) -> None:
        """Una sonda di lettura nuova deve dire cosa sostiene.

        Senza questa guardia si potrebbe aggiungere una sonda che nessun
        inventario nomina: girerebbe, comparirebbe nella matrice, e non
        renderebbe rosso niente. La classificazione e una decisione, e va
        presa quando la sonda si scrive.
        """

        from scripts.check_mariadb_driver import (
            OBSERVATION_ONLY_PROBES,
            QUALIFICATION_PROBES,
            REQUIRED_ACCEPTED_PROBES,
            REQUIRED_REJECTED_PROBES,
        )

        source = (
            ROOT / "crates" / "plenora-db-mysql" / "src" / "mariadb_evidence.rs"
        ).read_text(encoding="utf-8")
        declared = set(re.findall(r'"(provider\.profile_[a-z_]+)"', source))
        inventories = {
            "accettate": set(REQUIRED_ACCEPTED_PROBES),
            "rifiutate": set(REQUIRED_REJECTED_PROBES),
            "osservative": set(OBSERVATION_ONLY_PROBES),
            "di qualifica": set(QUALIFICATION_PROBES),
        }
        classified = set().union(*inventories.values())
        # Le due direzioni non hanno lo stesso perimetro, e l'asimmetria e
        # voluta.
        #
        # In avanti: ogni sonda **del profilo** deve essere classificata. E'
        # quella la famiglia che nasce insieme a una capability, e per cui
        # «non e in nessun inventario» sarebbe indistinguibile da una
        # dimenticanza.
        self.assertEqual(
            declared - classified,
            set(),
            "sonde del profilo che nessun inventario del runner classifica",
        )
        # All'indietro: ogni sonda classificata deve **esistere**, e qui il
        # perimetro e piu largo del profilo. Gli inventari possono nominare
        # qualunque sonda — `provider.ambiguous_commit` e stata la prima a
        # valersene, `raw.returning_forms` la prima a portarlo fuori dalla
        # famiglia `provider` — e una dichiarazione su una sonda cancellata non
        # dichiara nulla.
        #
        # Il perimetro include le `raw` perche l'inventario osservativo non e
        # una proprieta della famiglia: dice «questa sonda non sostiene niente,
        # e lo dice apposta», e una sonda `raw` senza contratto ha lo stesso
        # bisogno di essere dichiarata di una del profilo. Restringerlo a
        # `provider.*` faceva sembrare fantasma una dichiarazione vera.
        produced = set(re.findall(r'"((?:provider|raw)\.[a-z_]+)"', source))
        self.assertEqual(
            classified - produced,
            set(),
            "il runner pretende sonde che la misura non produce piu",
        )
        # **Esattamente** uno dei tre. Una sonda in due inventari e una
        # contraddizione — dovrebbe passare e insieme fallire — e il fatto che
        # oggi la somma dei tre torni non lo impedisce domani.
        for first, second in itertools.combinations(sorted(inventories), 2):
            self.assertEqual(
                inventories[first] & inventories[second],
                set(),
                f"sonde classificate sia fra le {first} sia fra le {second}",
            )
        self.assertEqual(
            sum(len(names) for names in inventories.values()),
            len(classified),
            "un inventario ripete una sonda che un altro gia dichiara",
        )

    def test_a_duplicated_probe_is_refused_before_it_disappears(self) -> None:
        """Due voci con lo stesso nome ne producono una sola, in silenzio.

        E il modo in cui una sonda smette di esistere senza che niente
        fallisca: il dizionario tiene l'ultima, e la matrice continua a
        mostrare quel nome come se fosse stato misurato una volta.
        """

        from scripts.check_mariadb_driver import (
            capability_violations,
            compare,
            duplicate_probes,
            servers,
        )

        self.assertEqual(duplicate_probes(["a", "b", "a", "c", "a"]), ["a"])
        self.assertEqual(duplicate_probes(["a", "b"]), [])

        fleet = servers()
        healthy = list(self.expected_probes())
        documents = {server.key: self.document(healthy) for server in fleet}
        self.assertEqual(len(compare(documents, fleet)), len(healthy))

        # Duplicato su un solo server: senza il controllo diventerebbe una
        # divergenza inventata, perche il confronto guarderebbe la seconda voce
        # contro la prima degli altri.
        one_server = dict(documents)
        one_server[fleet[1].key] = self.document(healthy + [healthy[0]])
        with self.assertRaises(RuntimeError) as raised:
            compare(one_server, fleet)
        self.assertIn(fleet[1].key, str(raised.exception))
        self.assertIn(healthy[0], str(raised.exception))

        # Duplicato identico su tutti e tre: il confronto tornerebbe `same`, e
        # la sonda sparita non lo direbbe a nessuno.
        everywhere = {server.key: self.document(healthy + [healthy[0]]) for server in fleet}
        with self.assertRaises(RuntimeError):
            compare(everywhere, fleet)

        # E lo stesso vale nel giudizio sulle capability, che costruisce un
        # dizionario dalla stessa lista.
        with self.assertRaises(RuntimeError) as raised:
            capability_violations(
                {
                    "results": [
                        {"probe": "provider.profile_read_schema", "observations": {}},
                        {"probe": "provider.profile_read_schema", "observations": {}},
                    ]
                }
            )
        self.assertIn("duplicate", str(raised.exception))

    def test_the_runner_verifies_the_image_it_measured(self) -> None:
        """Digest dichiarato e digest in esecuzione devono coincidere.

        Il documento dice quale immagine dovrebbe girare; `docker inspect`
        dice quale gira. Registrare solo il primo farebbe passare per misurata
        su una versione una corsa fatta su un'immagine sostituita sotto lo
        stesso nome — il caso che il pin per digest esiste per escludere.
        """

        source = self.RUNNER.read_text(encoding="utf-8")
        self.assertIn("def image_identities(", source)
        self.assertIn("def declares_image(", source)
        # Tutte e tre le risposte del demone, non solo l'ID: quello e il
        # digest del manifest con containerd e quello della config con il
        # graph driver, e confrontarlo con il pin passava in locale e falliva
        # sul runner.
        self.assertIn('"{{.Config.Image}}"', source)
        self.assertIn('"{{.Image}}"', source)
        self.assertIn("RepoDigests", source)
        self.assertIn("declared_digest", source)
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

        # Le due misure, entrambe: quella su MariaDB di ADR 0014 e quella
        # sulla semantica di sessione. Vale per tutt'e due la stessa ragione —
        # due terzi delle loro corse avvengono su un motore che il gate non
        # qualifica.
        measurements = {
            "mariadb_evidence.rs": "mariadb_driver_evidence",
            "session_evidence.rs": "session_semantics_evidence",
        }
        for source, entry_point in measurements.items():
            self.assertIn(source, EXCLUDED_SOURCES)
            text = (ROOT / "crates" / "plenora-db-mysql" / "src" / source).read_text(
                encoding="utf-8"
            )
            self.assertIn(f"async fn {entry_point}()", text)
            self.assertNotIn("async fn live_", text)
            self.assertIn("#[ignore", text)

        # L'inventario non deve contenere i **punti d'ingresso** delle misure.
        # La regola era scritta come "nessun nome che contenga `evidence`", ed
        # e stata un'approssimazione utile finche quella parola compariva solo
        # nei moduli di misura. Da quando i parser condivisi vivono in
        # `evidence.rs` con i propri unit test offline, quella forma
        # confondeva una misura con il test di una funzione pura: sono cose
        # diverse, e la seconda appartiene all'inventario.
        inventory = collect()
        for family in inventory.values():
            self.assertFalse(
                [
                    name
                    for name in family
                    if name.endswith(tuple(measurements.values()))
                ],
                "la misura e finita nell'inventario MySQL",
            )



class MariadbEvidenceCampaignTests(unittest.TestCase):
    """La campagna che porta le settantasei sonde su un runner pulito.

    Il runner sa gia giudicare, ma pretende tre server accesi: finche quello e
    stato l'unico modo di lanciarlo, le prove erano presidiate da chi si
    ricordava di farlo. Queste guardie tengono ferme le tre cose che rendono
    la campagna una garanzia invece di un comando: quando gira, cosa fa
    fallire, e in quale ordine tocca le cose.
    """

    WORKFLOW = ROOT / ".github" / "workflows" / "mariadb-evidence.yml"

    def campaign(self):
        import importlib

        return importlib.import_module("scripts.check_mariadb_campaign")

    def lifecycle(self):
        import importlib

        return importlib.import_module("scripts.fixture_campaign")

    def test_the_campaign_runs_on_a_schedule_and_never_on_push(self) -> None:
        """Settantasei sonde e tre server non stanno su ogni commit.

        E l'altra meta della stessa frase: se non girasse mai da sola, non
        sarebbe una garanzia. Quindi cadenza fissa piu esecuzione a mano, e
        nessun trigger su push.
        """

        workflow = self.WORKFLOW.read_text(encoding="utf-8")

        # I trigger si leggono nel blocco `on:`, non nel file: il commento che
        # spiega perche la campagna **non** gira su push contiene la parola
        # "push", ed e giusto che la contenga. Cercarla ovunque faceva fallire
        # la guardia sulla spiegazione invece che sulla configurazione.
        triggers = workflow.split(chr(10) + "on:" + chr(10), 1)[1]
        triggers = triggers.split(chr(10) + "permissions:", 1)[0]
        self.assertIn("schedule:", triggers)
        self.assertIn("workflow_dispatch:", triggers)
        self.assertIn("cron:", triggers)
        self.assertNotIn("push:", triggers)
        self.assertNotIn("pull_request:", triggers)

        # Cio che esegue, e cio che raccoglie quando va male.
        self.assertIn("scripts/check_mariadb_campaign.py", workflow)
        self.assertIn("scripts/test_check_mariadb_reference.py", workflow)
        self.assertIn("if: failure()", workflow)
        self.assertIn("upload-artifact", workflow)

        # Il ciclo di vita non sta nello YAML: uno YAML che accende container
        # e una decisione senza test.
        self.assertNotIn("docker compose", workflow)
        self.assertNotIn("docker run", workflow)

        # E niente scritture nell'albero: il preflight pretende un albero
        # pulito, e un log scritto dentro lo sporcherebbe — un gate che non
        # puo passare per costruzione.
        for line in workflow.splitlines():
            stripped = line.strip()
            neutral = stripped.replace("$RUNNER_TEMP", "").replace(
                "${{ runner.temp }}", ""
            )
            if not any(writer in neutral for writer in ("tee ", "> ", ">> ", "mkdir ")):
                continue
            self.assertIn(
                "RUNNER_TEMP",
                stripped,
                f"il workflow scrive nell'albero di lavoro: {stripped}",
            )

    def test_the_preflight_refuses_a_dirty_tree_before_docker(self) -> None:
        """Il commit registrato deve descrivere il codice misurato.

        E il controllo va fatto **prima** di accendere: scoprirlo con tre
        server su e uno spreco, e renderebbe falsa l'affermazione che precede
        Docker.
        """

        campaign = self.campaign()
        lifecycle = self.lifecycle()
        original = campaign.repository_state
        touched = []
        try:
            campaign.repository_state = lambda: {
                "commit": "abc",
                "worktree_dirty": True,
                "worktree_changes": [" M scripts/check_mariadb_campaign.py"],
            }
            with self.assertRaises(RuntimeError) as raised:
                campaign.preflight()
            self.assertIn("HEAD pulito", str(raised.exception))

            # E dentro la campagna, il rifiuto arriva senza che Docker sia
            # stato toccato.
            shared = {name: getattr(lifecycle, name) for name in ("compose", "run")}
            try:
                lifecycle.compose = lambda *arguments, **keywords: touched.append(arguments)
                lifecycle.run = lambda *arguments, **keywords: touched.append(arguments)
                with self.assertRaises(RuntimeError):
                    campaign.main([])
            finally:
                for name, value in shared.items():
                    setattr(lifecycle, name, value)
            self.assertEqual(touched, [], "la campagna ha toccato Docker con l'albero sporco")
        finally:
            campaign.repository_state = original

    def test_a_repository_that_moves_still_cleans_up(self) -> None:
        """Il postflight sta dentro il percorso protetto, non dopo.

        Fuori, un repository cambiato durante la corsa faceva fallire la
        campagna **saltando** diagnostica e pulizia: i tre server restavano
        accesi, ed e la condizione in cui la corsa successiva misura un
        accumulo invece di un riferimento. Il fallimento va bene; lasciare i
        server su no.
        """

        campaign = self.campaign()
        lifecycle = self.lifecycle()
        snapshots = iter(["A", "B"])
        calls = []

        original = {name: getattr(campaign, name) for name in ("preflight", "verdict")}
        shared = {name: getattr(lifecycle, name) for name in ("compose", "run")}
        try:
            campaign.preflight = lambda: next(snapshots)
            campaign.verdict = lambda: {"results": self.full_results()}
            lifecycle.compose = lambda file, *arguments, **keywords: calls.append(
                (file, arguments[0])
            )
            lifecycle.run = lambda *arguments, **keywords: ""
            with self.assertRaises(RuntimeError) as raised:
                campaign.main([])
        finally:
            for name, value in original.items():
                setattr(campaign, name, value)
            for name, value in shared.items():
                setattr(lifecycle, name, value)

        self.assertIn("e cambiato durante la misura", str(raised.exception))
        self.assertIn("A -> B", str(raised.exception))

        # E la pulizia c'e stata: dopo l'ultima accensione arriva uno
        # spegnimento, che e la cosa che prima non succedeva.
        verbs = [verb for _, verb in calls]
        self.assertIn("up", verbs)
        self.assertIn("down", verbs)
        self.assertGreater(
            len(verbs) - 1 - verbs[::-1].index("down"),
            len(verbs) - 1 - verbs[::-1].index("up"),
            f"nessuno spegnimento dopo l'ultima accensione: {calls}",
        )

    def test_a_capability_without_its_proof_fails_the_campaign(self) -> None:
        """Il verdetto si stampa comunque, ma l'uscita dice cosa manca.

        E la ragione per cui la campagna esiste: una sonda che smette di
        passare non e piu una misura fra due motori, e una promessa pubblicata
        che ha perso la sua prova.
        """

        campaign = self.campaign()
        lifecycle = self.lifecycle()
        # L'inventario intero, nell'ordine dichiarato: da quando il gate lo
        # verifica, un documento con le sole sonde necessarie e gia una
        # regressione — ed e giusto che lo sia.
        healthy = {"results": self.full_results()}
        regressed = {"results": [dict(entry) for entry in healthy["results"]]}
        from scripts.check_mariadb_driver import REQUIRED_ACCEPTED_PROBES

        broken = sorted(REQUIRED_ACCEPTED_PROBES)[0]
        entry = next(item for item in regressed["results"] if item["probe"] == broken)
        entry["observations"] = {
            server: {"outcome": "not_measured"}
            for server in ("mysql", "mariadb-12", "mariadb-11")
        }

        original = {
            name: getattr(campaign, name) for name in ("preflight", "verdict")
        }
        shared = {name: getattr(lifecycle, name) for name in ("compose", "run")}
        calls = []
        try:
            campaign.preflight = lambda: "commit"
            lifecycle.compose = lambda file, *arguments, **keywords: calls.append(
                (file, arguments[0])
            )
            lifecycle.run = lambda *arguments, **keywords: ""

            campaign.verdict = lambda: healthy
            self.assertEqual(campaign.main([]), 0)

            campaign.verdict = lambda: regressed
            self.assertEqual(campaign.main([]), 1)
        finally:
            for name, value in original.items():
                setattr(campaign, name, value)
            for name, value in shared.items():
                setattr(lifecycle, name, value)

        # E il ciclo di vita e stato quello: stato noto, accensione, pulizia,
        # per ciascuno dei due compose e per ciascuna delle due corse.
        for file in campaign.COMPOSE_FILES:
            for verb in ("config", "down", "up"):
                self.assertIn((file, verb), calls)

    def full_results(self) -> list[dict[str, object]]:
        """La matrice intera con gli esiti che il gate pretende."""

        from scripts.check_mariadb_driver import (
            EXPECTED_PROBES,
            QUALIFICATION_PROBES,
            REQUIRED_REJECTED_PROBES,
        )

        def outcome(probe: str) -> str:
            if probe in QUALIFICATION_PROBES:
                return QUALIFICATION_PROBES[probe][1]
            return "rejected" if probe in REQUIRED_REJECTED_PROBES else "accepted"

        return [
            {
                "probe": probe,
                "observations": {
                    server: {"outcome": outcome(probe)}
                    for server in ("mysql", "mariadb-12", "mariadb-11")
                },
            }
            for probe in EXPECTED_PROBES
        ]

    def test_a_probe_that_disappears_makes_the_gate_red(self) -> None:
        """Le violazioni di capability non vedono una sonda sparita.

        Se una `raw.*` o una osservativa smettesse di essere prodotta su tutti
        e tre i server, il totale scenderebbe di uno e l'uscita resterebbe
        zero: la matrice racconterebbe una superficie in meno senza che nulla
        lo dica. L'inventario esatto e cio che lo dice.
        """

        from scripts.check_mariadb_driver import (
            EXPECTED_PROBES,
            gate_violations,
            inventory_violations,
        )

        self.assertEqual(inventory_violations({"results": self.full_results()}), [])

        # Una sonda in meno, e non e fra quelle che sostengono una capability:
        # e proprio il caso che prima passava.
        observational = "raw.tls_cipher"
        self.assertIn(observational, EXPECTED_PROBES)
        without = [
            entry for entry in self.full_results() if entry["probe"] != observational
        ]
        violations = inventory_violations({"results": without})
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("sparita", violations[0])
        self.assertIn(observational, violations[0])
        self.assertTrue(gate_violations({"results": without}))

        # Una sonda in piu che nessuno ha dichiarato.
        extra = self.full_results() + [{"probe": "raw.inventata", "observations": {}}]
        violations = inventory_violations({"results": extra})
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("non dichiarata", violations[0])

        # E lo stesso insieme in un altro ordine: una sonda spostata e quasi
        # sempre una sonda riscritta.
        reordered = self.full_results()
        reordered[0], reordered[1] = reordered[1], reordered[0]
        violations = inventory_violations({"results": reordered})
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("un altro ordine", violations[0])

    def test_the_declared_inventory_matches_the_measure(self) -> None:
        """L'inventario e la misura non possono divergere in silenzio.

        Una sonda aggiunta e non dichiarata farebbe fallire la campagna al
        primo giro — il che va bene — ma solo **dopo** aver acceso tre server.
        Qui la differenza si vede prima, e senza server.
        """

        from scripts.check_mariadb_driver import EXPECTED_PROBES

        source = (
            ROOT / "crates" / "plenora-db-mysql" / "src" / "mariadb_evidence.rs"
        ).read_text(encoding="utf-8")
        declared = set(re.findall(r'"((?:raw|provider)\.[a-z_]+)"', source))
        self.assertEqual(
            declared - set(EXPECTED_PROBES),
            set(),
            "sonde prodotte dalla misura e non dichiarate nell'inventario",
        )
        self.assertEqual(
            set(EXPECTED_PROBES) - declared,
            set(),
            "sonde dichiarate nell'inventario che la misura non produce piu",
        )
        self.assertEqual(
            len(EXPECTED_PROBES),
            len(set(EXPECTED_PROBES)),
            "l'inventario ripete una sonda",
        )


if __name__ == "__main__":
    unittest.main()
