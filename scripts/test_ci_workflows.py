#!/usr/bin/env python3
"""Self-test dei workflow CI: cosa eseguono, e su quale revisione.

Sostituisce `test_candidate_sha_workflows.py`, che nominava quattordici
workflow uno per uno. Nove di quelli sono stati rimossi con la CI vecchia, il
file ha continuato ad aprirli, e da allora falliva con ventisette errori — ma
nessun job lo eseguiva, quindi il rosso non arrivava a nessuno. Un test che
fallisce e che nessuno guarda e peggio di un test che non esiste: occupa il
posto della verifica che manca.

Due conseguenze, e sono la forma di questo file:

* i workflow si **scoprono**, non si elencano. Un elenco scritto a mano e
  esattamente cio che e invecchiato: descriveva un repository che non c'era
  piu, e ogni file rimosso lo rendeva piu falso;
* la suite gira in CI a ogni push, dentro `rust-ci`, in un job che non
  compila niente.

Il contratto verificato e quello **corrente**: ogni job che usa i sorgenti li
prende con `actions/checkout` sulla revisione dell'evento — il default
dell'action — e nessuno la sovrascrive verso `main` o verso un altro ref. Il
vecchio contratto `CANDIDATE_SHA`, che ancorava le corse al head della PR
invece che al merge, non e stato ripristinato: la policy oggi e il merge SHA,
ed e cio che l'action fa senza input.
"""

from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIRECTORY = ROOT / ".github" / "workflows"

# Cio che tradisce un job che ha bisogno del repository: percorsi di file
# versionati e comandi che li leggono. Un job che scarica solo artifact — come
# quelli che allegano i wheel a una release — non ne ha bisogno, e imporgli un
# checkout sarebbe cerimonia.
SOURCE_MARKERS = (
    "scripts/",
    "crates/",
    "docker/",
    "docker-compose",
    "cargo ",
    "tests/",
)

# Input che spostano il checkout altrove. `ref` lo manda su un'altra
# revisione, `repository` su un altro repository: entrambi renderebbero la
# corsa una verifica di codice diverso da quello dell'evento, e la differenza
# non si vede nel risultato.
CHECKOUT_OVERRIDES = ("ref:", "repository:")


def workflow_files() -> list[Path]:
    """I workflow presenti, scoperti dalla directory."""

    return sorted(
        path
        for pattern in ("*.yml", "*.yaml")
        for path in WORKFLOW_DIRECTORY.glob(pattern)
    )


def executable_lines(block: str) -> str:
    """Il blocco senza i commenti.

    Un commento che nomina `scripts/` descrive un job, non lo esegue: leggerlo
    come se lo eseguisse farebbe pretendere un checkout a chi non tocca il
    repository. Vale anche al contrario — la spiegazione di *perche* un flag
    non c'e non deve valere come flag presente.
    """

    return "\n".join(
        line for line in block.splitlines() if not line.lstrip().startswith("#")
    )


def jobs(workflow: str) -> dict[str, str]:
    """I blocchi dei job, per identificatore.

    Diviso a mano invece che con un parser YAML: il repository non dichiara
    PyYAML fra le dipendenze dei self-test, e aggiungerlo per qualche
    `assertIn` renderebbe questa suite dipendente da un'installazione — cioe
    da qualcosa che puo mancare proprio nel job che la esegue.

    Le chiavi a due spazi esistono anche sotto `on:` (`push`, `release`,
    `workflow_dispatch`), quindi si parte dalla riga `jobs:` in colonna zero:
    prenderle per job significherebbe verificare i trigger come se fossero
    lavori.
    """

    match = re.search(r"^jobs:$", workflow, re.MULTILINE)
    if match is None:
        raise AssertionError("workflow senza sezione jobs:")
    body = workflow[match.end() :]
    starts = [
        (found.group(1), found.start())
        for found in re.finditer(r"^  ([A-Za-z0-9_-]+):$", body, re.MULTILINE)
    ]
    blocks: dict[str, str] = {}
    for index, (name, start) in enumerate(starts):
        end = starts[index + 1][1] if index + 1 < len(starts) else len(body)
        blocks[name] = body[start:end]
    return blocks


def checkout_steps(block: str) -> list[str]:
    """I passi `actions/checkout` di un blocco, uno per elemento.

    Il passo finisce dove ne comincia un altro con lo stesso rientro: e il
    `with:` che segue a portare gli input, e leggerlo insieme al passo dopo
    attribuirebbe a `checkout` opzioni che non sono sue.
    """

    lines = block.splitlines()
    steps: list[str] = []
    for index, line in enumerate(lines):
        if "uses: actions/checkout@" not in line:
            continue
        indent = len(line) - len(line.lstrip())
        collected = [line]
        for following in lines[index + 1 :]:
            if not following.strip():
                collected.append(following)
                continue
            leading = len(following) - len(following.lstrip())
            if leading <= indent:
                break
            collected.append(following)
        steps.append("\n".join(collected))
    return steps


class CiWorkflowTests(unittest.TestCase):
    """Il contratto comune a tutti i workflow, qualunque siano."""

    def test_the_workflows_are_discovered_and_are_not_an_empty_set(self) -> None:
        """La scoperta deve trovare qualcosa, altrimenti tutto passa a vuoto.

        Il pavimento non e un inventario: nuovi workflow non lo toccano, e
        toglierne uno fa fermare qui chi lo toglie — che e il momento giusto
        per chiedersi se le sue garanzie siano finite da qualche altra parte.
        """

        discovered = workflow_files()
        self.assertGreaterEqual(
            len(discovered),
            5,
            f"trovati {[path.name for path in discovered]}: la scoperta non "
            "vede i workflow, oppure ne e stato rimosso uno",
        )
        for path in discovered:
            self.assertIn("jobs:", path.read_text(encoding="utf-8"), path.name)

    def test_every_job_that_uses_the_sources_checks_them_out(self) -> None:
        """Chi legge i file versionati deve prenderli, non presumerli."""

        for path in workflow_files():
            for name, block in jobs(path.read_text(encoding="utf-8")).items():
                executable = executable_lines(block)
                markers = [
                    marker for marker in SOURCE_MARKERS if marker in executable
                ]
                if not markers:
                    continue
                with self.subTest(workflow=path.name, job=name):
                    self.assertIn(
                        "uses: actions/checkout@",
                        executable,
                        f"il job usa i sorgenti ({markers}) senza checkout",
                    )

    def test_no_checkout_silently_moves_to_another_revision(self) -> None:
        """Nessun `ref:` e nessun `repository:` sui checkout.

        Senza input, `actions/checkout` prende la revisione dell'evento: il
        commit su push, il merge commit su pull_request. E la policy corrente,
        e un override la sostituirebbe in silenzio — la corsa verificherebbe
        `main` mentre il verdetto parla della PR, e i due si somigliano
        abbastanza da non far sospettare nulla.

        Il vecchio contratto `CANDIDATE_SHA` ancorava invece le corse al head
        della PR. Non viene ripristinato: qui si fissa cio che la CI fa oggi.
        """

        for path in workflow_files():
            workflow = path.read_text(encoding="utf-8")
            steps = checkout_steps(executable_lines(workflow))
            with self.subTest(workflow=path.name):
                self.assertTrue(steps, f"{path.name}: nessun checkout trovato")
                for step in steps:
                    for override in CHECKOUT_OVERRIDES:
                        self.assertNotIn(
                            override,
                            step,
                            f"{path.name}: checkout con '{override}' "
                            "sovrascrive la revisione dell'evento",
                        )

    def test_every_adapter_is_checked_in_isolation(self) -> None:
        """Le quattro combinazioni di feature del CLI restano verificate.

        Due di esse non compilavano affatto, e nessuno se ne accorgeva perche
        la CI costruiva solo i default e `--all-features` — le due dove
        PostgreSQL c'e sempre. Girano in un job solo: la prima stesura le
        aveva messe in `strategy.matrix` e sei job in parallelo hanno fatto
        rifiutare da `codeload` il download dell'action di cache, con un 429.
        Un job che verifica quattro cose non deve moltiplicare per quattro le
        sue dipendenze di rete.
        """

        workflow = (WORKFLOW_DIRECTORY / "rust-ci.yml").read_text(encoding="utf-8")
        for features in (
            "--no-default-features --features postgres",
            "--no-default-features --features mysql",
            "--no-default-features --features sqlserver",
            "--all-features",
        ):
            self.assertIn(
                features, workflow, f"combinazione non piu verificata: {features}"
            )
        # `--all-targets` porta dentro i test, che assumevano PostgreSQL.
        self.assertIn("--all-targets $features -- -D warnings", workflow)
        # E la guardia dell'aiuto viene **eseguita**, non solo compilata: il
        # suo valore sta nelle configurazioni non di default, dove il job
        # `test-unit` non arriva.
        self.assertIn(
            "cargo test --locked -p plenora-database-cli $features usage",
            workflow,
        )
        # Il ciclo prova tutte le combinazioni prima di uscire: fermarsi alla
        # prima nasconderebbe le altre tre dietro un errore solo.
        self.assertIn("status=1", workflow)
        self.assertIn("exit $status", workflow)
        self.assertNotIn(
            "strategy:",
            jobs(workflow).get("cli-feature-matrix", ""),
            "la matrice moltiplica i job, e con essi i download della cache",
        )

    def test_the_static_job_runs_every_serverless_self_test(self) -> None:
        """I self-test che non chiedono un server girano a ogni push.

        Restare fuori dalla CI e la condizione in cui un test smette di essere
        letto: `test_candidate_sha_workflows.py` e morto cosi. PostgreSQL non
        aveva un gate veloce come MySQL, quindi le sue guardie giravano solo
        quando qualcuno lanciava il gate completo — cioe con i container su.

        MySQL resta nel suo gate statico e MariaDB nella propria suite: qui si
        aggiunge cio che non era eseguito da nessuno.
        """

        workflow = (WORKFLOW_DIRECTORY / "rust-ci.yml").read_text(encoding="utf-8")
        static = jobs(workflow)["static-self-tests"]
        for suite in (
            "scripts/test_ci_workflows.py",
            "scripts/test_check_mariadb_reference.py",
            "scripts/test_check_postgres_reference.py",
            "scripts/test_check_postgres_hardening.py",
            "scripts/test_check_sqlserver_reference.py",
        ):
            self.assertIn(f"python3 {suite}", static, f"{suite} non eseguito")

        # La duplicazione con `sqlserver-assurance` e voluta e dichiarata:
        # senza la nota, il prossimo lettore la toglie credendola una svista.
        self.assertIn("Duplicazione dichiarata", static)
        self.assertIn(
            "python3 scripts/test_check_sqlserver_reference.py",
            (WORKFLOW_DIRECTORY / "sqlserver-assurance.yml").read_text(
                encoding="utf-8"
            ),
            "la nota dichiara una duplicazione che non esiste",
        )

        # Il job resta leggero: se qui comparisse una toolchain, smetterebbe
        # di essere il posto dove si aggiunge una suite senza pensarci.
        self.assertNotIn("rust-toolchain", static)
        self.assertNotIn("rust-cache", static)

    def test_fuzz_lock_uses_the_workspace_core_version(self) -> None:
        """Il lock del fuzz segue la versione del workspace.

        Sopravvissuto alla CI vecchia perche non riguarda un workflow: `fuzz/`
        ha un lock proprio, e se resta indietro il fuzzing esercita una
        versione del core diversa da quella che il repository sviluppa.
        """

        workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        fuzz_lock = tomllib.loads(
            (ROOT / "fuzz" / "Cargo.lock").read_text(encoding="utf-8")
        )
        core = next(
            package
            for package in fuzz_lock["package"]
            if package["name"] == "plenora-database-core"
        )

        self.assertEqual(core["version"], workspace["workspace"]["package"]["version"])


class PythonWheelWorkflowTests(unittest.TestCase):
    """`python-wheel`: cosa costruisce, e cosa verifica prima di pubblicarlo.

    Il workflow e l'unico percorso con cui il SDK esce dal repository, e le
    sue garanzie non hanno un test che le esercita: girano su tre runner, a
    mano, il giorno del rilascio. Una riga tolta da questo YAML non fa
    fallire niente finche qualcuno non pubblica un wheel rotto — quindi le
    guardie stanno qui, e questa suite gira in CI a ogni push.
    """

    WORKFLOW = WORKFLOW_DIRECTORY / "python-wheel.yml"
    VERIFIER = ROOT / ".github" / "scripts" / "verify_wheel.py"
    BUILD_JOBS = ("linux", "macos", "windows")

    def jobs(self) -> dict[str, str]:
        return jobs(self.WORKFLOW.read_text(encoding="utf-8"))

    def test_every_maturin_build_is_locked(self) -> None:
        """`--locked` su tutti e tre: senza, il lock non vincola la release.

        `maturin build` senza `--locked` risolve le dipendenze al momento
        della build. Il wheel pubblicato conterrebbe versioni che nessun
        commit dichiara, e i tre wheel della stessa release potrebbero
        contenerne tre insiemi diversi — uno per runner.
        """

        blocks = self.jobs()
        for name in self.BUILD_JOBS:
            block = blocks[name]
            self.assertIn("command: build", block, f"{name}: non costruisce")
            self.assertIn("--locked", block, f"{name}: build senza --locked")
            self.assertLess(
                block.index("--release"),
                block.index("--out dist"),
                f"{name}: argomenti di build in una forma inattesa",
            )

    def test_each_platform_verifies_its_wheel_before_uploading_it(self) -> None:
        """Installa e verifica sul runner che ha costruito, prima dell'upload.

        Il caricamento del modulo nativo si prova solo dove e stato
        compilato: verificare il solo wheel Linux lasciava macOS e Windows
        senza nessuna prova che il `.dylib`/`.pyd` si carichi. E la verifica
        sta prima dell'upload perche un artefatto rotto non deve nemmeno
        diventare scaricabile.
        """

        blocks = self.jobs()
        for name in self.BUILD_JOBS:
            block = blocks[name]
            self.assertIn("actions/setup-python", block, f"{name}: senza Python")
            self.assertIn(
                "python -m pip install dist/*.whl",
                block,
                f"{name}: non installa il wheel che ha costruito",
            )
            self.assertIn(
                "python .github/scripts/verify_wheel.py",
                block,
                f"{name}: non verifica il wheel",
            )
            self.assertLess(
                block.index("verify_wheel.py"),
                block.index("upload-artifact"),
                f"{name}: verifica dopo l'upload, cioe troppo tardi",
            )
            # `shell: bash` non e un dettaglio: su Windows il default e
            # PowerShell, che non espande `dist/*.whl` e passerebbe a pip un
            # percorso letterale.
            self.assertIn("shell: bash", block, f"{name}: senza shell esplicita")

    def test_the_verifier_pins_version_identity_and_the_release_tag(self) -> None:
        """Le tre verifiche che il wheel deve superare, in un posto solo."""

        source = self.VERIFIER.read_text(encoding="utf-8")
        self.assertIn("import plenora_database as p", source)
        # Le due versioni: quella con cui il wheel e impacchettato e quella
        # compilata nel modulo nativo. Divergono se si bumpa pyproject.toml
        # e non il Cargo.toml del crate.
        self.assertIn("metadata.version(PACKAGE)", source)
        self.assertIn("p.version()", source)
        self.assertIn("if native != declared:", source)
        # E il tag della release, che deve nominare quella stessa versione.
        self.assertIn('TAG_PREFIX = "py-v"', source)
        self.assertIn('os.environ.get("GITHUB_EVENT_NAME") == "release"', source)
        self.assertIn('f"{TAG_PREFIX}{declared}"', source)
        self.assertIn("if reference != expected:", source)
        # Ogni verifica fallita deve fermare il job.
        self.assertIn("raise SystemExit(main())", source)

    def test_the_verifier_is_the_only_definition_of_verified(self) -> None:
        """Nessun job ricopia le verifiche invece di eseguirle.

        Prima il solo smoke test Linux le aveva scritte inline, in tre
        `python -c`. Ricopiarle su tre piattaforme avrebbe prodotto tre
        definizioni di "wheel verificato", e sarebbero divergute alla prima
        modifica di una sola.
        """

        workflow = self.WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(
            workflow.count("python .github/scripts/verify_wheel.py"),
            4,
            "tre build piu lo smoke test dell'artefatto scaricato",
        )
        self.assertNotIn(
            "python -c",
            workflow,
            "una verifica inline e una seconda definizione di verificato",
        )

    def test_the_workflow_has_no_path_to_publish_anywhere(self) -> None:
        """La distribuzione dei wheel e manuale, e qui non c'e come farla.

        Il job `publish-pypi` era opt-in e disattivato per default, il che
        sembra sicuro finche non lo si guarda dal lato dell'errore: un
        percorso di pubblicazione che nessuno usa resta un percorso, basta un
        input spuntato per sbaglio, e da PyPI una versione non si ritira —
        si puo solo yankare, lasciandola visibile per sempre.

        Restano i due trigger che servono: `workflow_dispatch` per il
        preflight, `release` per costruire e allegare. Nessun secret, perche
        nessun passo ne ha piu bisogno.
        """

        workflow = self.WORKFLOW.read_text(encoding="utf-8")
        for forbidden in (
            "publish_pypi",
            "publish-pypi",
            "PYPI_TOKEN",
            "MATURIN_PYPI_TOKEN",
            "environment: pypi",
            "command: upload",
            "twine",
            "gh-action-pypi-publish",
        ):
            self.assertNotIn(
                forbidden,
                workflow,
                f"'{forbidden}' rimette in piedi un percorso di pubblicazione",
            )
        self.assertNotIn(
            "secrets.",
            workflow,
            "nessun passo del workflow deve avere bisogno di un secret",
        )
        self.assertIn("  workflow_dispatch:\n", workflow, "preflight rimosso")
        self.assertIn(
            "  release:\n    types: [published]\n",
            workflow,
            "il workflow non costruisce piu sulla release",
        )
        # E la consegna che resta e una sola: gli asset della release.
        self.assertIn("softprops/action-gh-release@v2", workflow)

    def test_the_header_does_not_promise_an_intel_mac_wheel(self) -> None:
        """Il commento diceva "macOS (arm64 + x86_64)"; x86_64 non esiste.

        Il job Intel era stato rimosso e la nota accanto lo spiegava, ma
        l'intestazione — la prima cosa che si legge — continuava a
        prometterlo. Chi cerca il wheel Intel fra gli artifact non lo trova e
        non sa se sia un guasto o una scelta.
        """

        workflow = self.WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("arm64 + x86_64", workflow)
        self.assertIn("macOS (arm64)", workflow)
        self.assertIn("macOS produce **solo** ARM", workflow)
        self.assertNotIn("macos-13", workflow.split("# Nota:")[0])


if __name__ == "__main__":
    unittest.main()
