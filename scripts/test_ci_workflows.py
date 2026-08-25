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

import ast
import re
import tempfile
import tomllib
import unittest
from pathlib import Path

import yaml


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
CHECKOUT_OVERRIDES = ("ref", "repository")


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


#: Le chiavi che un job puo avere, e quelle che puo avere un passo. Una chiave
#: fuori da questi due elenchi fa fallire la lettura.
JOB_KEYS = frozenset(
    {
        "name",
        "runs-on",
        "needs",
        "if",
        "env",
        "strategy",
        "steps",
        "timeout-minutes",
        "permissions",
        "defaults",
        "continue-on-error",
        "container",
        "services",
        "outputs",
        "concurrency",
        "environment",
    }
)

STEP_KEYS = frozenset(
    {
        "name",
        "uses",
        "with",
        "run",
        "if",
        "env",
        "shell",
        "id",
        "working-directory",
        "continue-on-error",
        "timeout-minutes",
    }
)


def job_text(workflow: str, name: str) -> str:
    """Cio che i passi di un job **eseguono o usano**, un elemento per riga.

    Alcune verifiche parlano di comandi e argomenti — `--locked`, `status=1` —
    e per quelle il testo e la forma giusta. Va pero ricavato dall'albero, non
    ritagliando il file con una regex: la divisione testuale perdeva un job con
    identificatore quotato e rifiutava anchor e alias, che GitHub Actions
    ammette.

    E non serializzando il job intero, che era la prima versione di questa
    funzione: `env`, `name` e `with` finivano nella stessa stringa dei comandi,
    quindi una guardia che cercava `python3 scripts/x.py` era soddisfatta da
    una variabile d'ambiente che lo nominava, anche dopo aver cancellato il
    passo che lo lanciava. Qui entrano soltanto `run`, `uses` con i propri
    input e `shell`, cioe le chiavi che fanno accadere qualcosa o decidono
    **come** accade.
    """

    lines: list[str] = []
    for step in parsed_jobs(workflow)[name].get("steps", []):
        if isinstance(step.get("uses"), str):
            lines.append(f"uses: {step['uses']}")
            inputs = step.get("with")
            if isinstance(inputs, dict):
                lines.extend(f"  {key}: {value}" for key, value in inputs.items())
        if isinstance(step.get("shell"), str):
            lines.append(f"shell: {step['shell']}")
        if isinstance(step.get("run"), str):
            lines.append(step["run"])
    return "\n".join(lines)


def parsed_jobs(workflow: str) -> dict[str, dict]:
    """I job del workflow, letti con un parser YAML vero.

    Quattro cicli di review hanno sciolto la questione che questo file aveva
    deciso al contrario. La lettura fatta a mano era arrivata a gestire
    rientri, commenti in coda, scalari a blocco, quote e collezioni — cioe a
    reimplementare male un parser — e ogni giro faceva emergere una forma
    valida che leggeva male **in silenzio**: `with: {run: ...}` contato per un
    comando, un `env.if` scambiato per la condizione del passo, un `steps:` con
    anchor che faceva sparire l'intero job, un `run: >` letto come se
    conservasse i ritorni a capo.

    L'obiezione originale — la dipendenza puo mancare nel job che esegue
    questa suite — non regge: una dipendenza necessaria e non dichiarata e un
    problema di packaging del gate, e si risolve dichiarandola. Il repository
    ha gia `requirements-phase0.txt` con i pin, e ora
    `requirements-self-tests.txt` aggiunge `PyYAML` per questa suite; il job
    `static-self-tests` le installa.

    La validazione resta fail-closed sopra l'albero: `jobs` deve essere una
    mappa, ogni job una mappa, `steps` una sequenza di mappe, e le chiavi
    devono stare in `JOB_KEYS` e `STEP_KEYS`.

    # Raises

    `RuntimeError` su un documento che non ha questa forma.
    """

    document = yaml.safe_load(workflow)
    if not isinstance(document, dict):
        raise RuntimeError("workflow che non e una mappa YAML")
    jobs_node = document.get("jobs")
    if not isinstance(jobs_node, dict):
        raise RuntimeError("workflow senza sezione jobs: come mappa")
    for name, job in jobs_node.items():
        if not isinstance(job, dict):
            raise RuntimeError(f"job {name!r} che non e una mappa")
        unknown = set(job) - JOB_KEYS
        if unknown:
            raise RuntimeError(
                f"chiavi di job non riconosciute in {name!r}: {sorted(unknown)}. "
                "Aggiungerle a JOB_KEYS dopo aver deciso come leggerle."
            )
        steps = job.get("steps", [])
        if not isinstance(steps, list):
            raise RuntimeError(f"`steps:` di {name!r} non e una sequenza")
        for step in steps:
            if not isinstance(step, dict):
                raise RuntimeError(f"passo di {name!r} che non e una mappa")
            unknown = set(step) - STEP_KEYS
            if unknown:
                raise RuntimeError(
                    f"chiavi di passo non riconosciute in {name!r}: "
                    f"{sorted(unknown)}. Aggiungerle a STEP_KEYS dopo aver "
                    "deciso come leggerle."
                )
    return jobs_node


def executed_steps(workflow: str) -> list[dict]:
    """I passi che il workflow esegue **incondizionatamente**.

    Un `if:` sul job o sul passo lo spegne — `if: false` letteralmente, e
    qualunque altra condizione non e valutabile leggendo il file. Nessun passo
    che lancia un gate ne ha una: quelli che ce l'hanno allegano wheel a una
    release, caricano artefatti o fermano i container.
    """

    steps: list[dict] = []
    for job in parsed_jobs(workflow).values():
        if "if" in job or job.get("continue-on-error"):
            continue
        steps.extend(step for step in job.get("steps", []) if "if" not in step)
    return steps


def run_commands(workflow: str) -> str:
    """I comandi che il workflow esegue, uno per riga."""

    return "\n".join(
        step["run"] for step in executed_steps(workflow) if isinstance(step.get("run"), str)
    )


def gate_invocation(gate: str) -> re.Pattern[str]:
    """Il gate come comando, con o senza `-m`."""

    module = gate.removesuffix(".py")
    return re.compile(
        r"^[ 	]*(?:python3?|py)\s+"
        r"(?:"
        + re.escape(f"scripts/{gate}")
        + r"|-m\s+scripts\."
        + re.escape(module)
        + r")(?:\s|$)"
    )


def first_command(script: str) -> str:
    """La prima riga non vuota e non commentata di un `run`."""

    for line in script.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            return line
    return ""


def invoked_gates(workflow: str, gate: str) -> bool:
    """Il workflow esegue questo gate come **primo comando di un passo**.

    E una convenzione, non un'analisi: modellare una shell si e rivelato
    inaffidabile a ogni giro — `false && gate`, una continuazione con backtick
    di PowerShell, il corpo di un here-document, una sostituzione `$(...)`
    sono tutte forme in cui "il gate compare in una riga" e "il gate viene
    eseguito" non coincidono, e ogni regola nuova ne lasciava fuori un'altra.

    Qui la domanda cambia: un gate qualificante sta in un passo dedicato ed e
    il primo comando del suo `run`. Cio che segue puo essere qualunque cosa —
    una pipe verso `tee`, una redirezione — perche il gate ha gia deciso
    l'esito del passo. I workflow del repository rispettano la convenzione, e
    dove non la rispettavano la preparazione e stata spostata in un passo suo:
    un passo che fa una cosa sola e anche un rosso attribuibile.
    """

    pattern = gate_invocation(gate)
    return any(
        qualifies(step) and pattern.search(first_command(step["run"]))
        for step in executed_steps(workflow)
        if isinstance(step.get("run"), str)
    )


def qualifies(step: dict) -> bool:
    """Il passo lascia decidere l'esito al proprio primo comando.

    Un gate puo essere eseguito e non contare: `continue-on-error` fa passare
    il passo comunque, un `|| true` in coda scarta il codice di uscita, un `;`
    prosegue qualunque cosa sia successo e un `&` finale lo manda in
    background senza attenderlo. Verificare che il gate parta non basta —
    serve che il suo verdetto arrivi al workflow.

    `&&` invece va bene: il comando dopo gira **solo** se il gate riesce.
    E `2>&1` non e un operatore, e una redirezione: distinguerli e la ragione
    per cui questa funzione guarda le forme una per una invece di cercare il
    carattere `&`.
    """

    if step.get("continue-on-error"):
        return False
    command = first_command(step.get("run", "")).strip()
    if "||" in command or ";" in command:
        return False
    return not (command.endswith("&") and not command.endswith("&&"))


def checkout_steps(workflow: str) -> list[dict]:
    """I passi `actions/checkout`, riconosciuti dalla chiave `uses`.

    Cercare la sottostringa nel testo del job contava anche un `echo 'uses:
    actions/checkout@...'` dentro un comando: un job poteva usare i sorgenti
    senza prenderli e superare comunque la guardia.

    Qui rientrano **anche** i checkout condizionati: un `ref:` sbagliato resta
    sbagliato anche dietro un `if`, e ignorarli lasciava che un job prendesse
    `main` con una condizione mentre un altro job forniva il checkout
    incondizionato che la guardia cercava.
    """

    return [
        step
        for job in parsed_jobs(workflow).values()
        for step in job.get("steps", [])
        if isinstance(step.get("uses"), str)
        and step["uses"].startswith("actions/checkout@")
    ]


def local_bindings(tree: ast.Module) -> dict[str, tuple[str, str]]:
    """Per ogni nome locale, da quale modulo e con quale simbolo arriva.

    Serve perche un import puo essere spezzato in piu istruzioni e puo dare un
    alias: `from a.b import x as y` lega `y` al simbolo `x` di `a.b`. La
    versione precedente pretendeva tutti i simboli nello stesso `ImportFrom` e
    trattava l'alias come se fosse il nome originale — un falso rosso su un
    refactoring innocuo, e un falso verde su un alias che punta altrove.
    """

    bindings: dict[str, tuple[str, str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                bindings[alias.asname or alias.name] = (node.module, alias.name)
    return bindings


def imports_from(caller: Path, module: str, symbols: tuple[str, ...]) -> bool:
    """Il caller lega quei simboli a **quel** modulo, con il percorso completo.

    Confrontare l'ultimo segmento accettava un modulo omonimo in un package
    diverso.
    """

    bindings = local_bindings(ast.parse(caller.read_text(encoding="utf-8")))
    origins = {
        symbol
        for (origin, symbol) in bindings.values()
        if origin == module
    }
    return set(symbols) <= origins


def measures_with(caller: Path, module: str, expected: dict[str, str]) -> bool:
    """Il caller passa alla campagna, **su quella keyword**, quel simbolo.

    Le quattro versioni precedenti provavano a dedurre l'esecuzione dalla
    sintassi, e ogni volta restava un modo di certificare un caller che il gate
    non lo lancia. Qui la relazione e dichiarata e si verifica esattamente:
    `campaign(preflight=..., measure=...)` deve ricevere, su ciascuna keyword,
    un nome legato al simbolo giusto del modulo giusto.

    Ignorare le keyword accettava `campaign(foo=preflight, bar=verdict)` e
    perfino le due funzioni scambiate: la campagna avrebbe misurato con il
    preflight e viceversa, e la guardia non se ne sarebbe accorta.
    """

    tree = ast.parse(caller.read_text(encoding="utf-8"))
    bindings = local_bindings(tree)
    for node in _live_python(tree):
        if not isinstance(node, ast.Call):
            continue
        called = node.func
        name = (
            called.id
            if isinstance(called, ast.Name)
            else called.attr if isinstance(called, ast.Attribute) else None
        )
        if name != "campaign":
            continue
        satisfied = 0
        for keyword in node.keywords:
            wanted = expected.get(keyword.arg or "")
            if wanted is None or not isinstance(keyword.value, ast.Name):
                continue
            origin = bindings.get(keyword.value.id)
            if origin == (module, wanted):
                satisfied += 1
        if satisfied == len(expected):
            return True
    return False


def _live_python(tree: ast.AST) -> list[ast.AST]:
    """I nodi che non stanno in un ramo spento da una condizione costante."""

    nodes: list[ast.AST] = []
    stack: list[ast.AST] = [tree]
    while stack:
        node = stack.pop()
        nodes.append(node)
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.If | ast.While) and isinstance(
                child.test, ast.Constant
            ):
                stack.extend(child.body if child.test.value else child.orelse)
                continue
            stack.append(child)
    return nodes


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

    def test_every_action_is_pinned_to_a_commit(self) -> None:
        """Un tag e mutabile: `@v4` oggi e `@v4` domani non sono lo stesso codice.

        I workflow qui costruiscono e verificano i wheel che finiscono
        allegati a una release. Con i riferimenti mobili, due corse dello
        stesso commit possono eseguire action diverse, e "riproducibile"
        smette di voler dire qualcosa.

        La guardia si scopre come il resto del file: non elenca le action, le
        legge. Una action nuova entra gia pinnata o si ferma qui.
        """

        pinned = re.compile(r"^[0-9a-f]{40}$")
        for path in workflow_files():
            workflow = path.read_text(encoding="utf-8")
            for name, job in parsed_jobs(workflow).items():
                for step in job.get("steps", []):
                    reference = step.get("uses")
                    if not isinstance(reference, str):
                        continue
                    # Le action locali (`./...`) e i container (`docker://`) non
                    # hanno un commit da fissare.
                    if reference.startswith(("./", "docker://")):
                        continue
                    with self.subTest(workflow=path.name, job=name, uses=reference):
                        self.assertIn("@", reference, "action senza riferimento")
                        self.assertRegex(
                            reference.split("@", 1)[1],
                            pinned,
                            "riferimento mobile: va fissato al commit",
                        )

    def test_the_ignored_pyo3_advisories_stay_unreachable(self) -> None:
        """Un'eccezione motivata dalla non raggiungibilita va sorvegliata.

        `deny.toml` ignora RUSTSEC-2026-0176 e 0177 perche i simboli
        vulnerabili non compaiono nel workspace. Finche resta un commento,
        pero, basta una riga nuova per rendere il percorso raggiungibile
        lasciando `cargo deny` verde — l'eccezione sopravviverebbe al motivo
        che la giustifica.

        La guardia copre cio che si puo verificare per simbolo:
        `PyCFunction::new_closure` (0177). Per 0176 — `nth`/`nth_back` sugli
        iteratori di `PyList`/`PyTuple` — non esiste una firma testuale
        affidabile: un `.nth(` puo essere su qualunque iteratore Rust, e
        infatti i tre presenti sono su `str::split_whitespace()`. Quel lato
        resta verificato a mano, e la nota in deny.toml lo dice.
        """

        policy = (ROOT / "deny.toml").read_text(encoding="utf-8")
        ignored = "RUSTSEC-2026-0177" in policy
        if not ignored:
            self.skipTest("advisory non piu ignorata: la guardia non serve")

        forbidden = ("PyCFunction", "new_closure")
        offenders: list[str] = []
        for path in sorted((ROOT / "crates").rglob("*.rs")):
            text = path.read_text(encoding="utf-8")
            for line_number, line in enumerate(text.splitlines(), start=1):
                stripped = line.lstrip()
                if stripped.startswith("//"):
                    continue
                for symbol in forbidden:
                    if symbol in line:
                        relative = path.relative_to(ROOT).as_posix()
                        offenders.append(f"{relative}:{line_number}: {symbol}")
        self.assertEqual(
            offenders,
            [],
            "RUSTSEC-2026-0177 e ignorata perche non raggiungibile, ma il "
            f"simbolo ora compare: {offenders}. Aggiornare pyo3 a >= 0.29 "
            "oppure togliere l'eccezione da deny.toml.",
        )

    def test_every_job_that_uses_the_sources_checks_them_out(self) -> None:
        """Chi legge i file versionati deve prenderli, non presumerli.

        Sia l'uso dei sorgenti sia il checkout si leggono dall'albero: la
        versione precedente cercava le due sottostringhe nel testo del job, e
        un `echo 'uses: actions/checkout@...'` dentro un comando valeva come
        checkout — un job poteva usare i sorgenti senza prenderli e superare
        comunque la guardia.
        """

        for path in workflow_files():
            workflow = path.read_text(encoding="utf-8")
            for name, job in parsed_jobs(workflow).items():
                commands = "\n".join(
                    step["run"]
                    for step in job.get("steps", [])
                    if isinstance(step.get("run"), str)
                )
                markers = [marker for marker in SOURCE_MARKERS if marker in commands]
                if not markers:
                    continue
                steps = job.get("steps", [])
                checkout = next(
                    (
                        index
                        for index, step in enumerate(steps)
                        if isinstance(step.get("uses"), str)
                        and step["uses"].startswith("actions/checkout@")
                        and "if" not in step
                    ),
                    None,
                )
                first_use = next(
                    (
                        index
                        for index, step in enumerate(steps)
                        if isinstance(step.get("run"), str)
                        and any(marker in step["run"] for marker in SOURCE_MARKERS)
                    ),
                    None,
                )
                with self.subTest(workflow=path.name, job=name):
                    self.assertIsNotNone(
                        checkout,
                        f"il job usa i sorgenti ({markers}) senza un checkout "
                        "incondizionato nello stesso job",
                    )
                    # E **prima** di usarli: un checkout dopo il primo comando
                    # che legge i file versionati non serve a quel comando.
                    self.assertLess(
                        checkout,
                        first_use,
                        "il checkout arriva dopo il primo uso dei sorgenti",
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
            steps = checkout_steps(workflow)
            with self.subTest(workflow=path.name):
                self.assertTrue(steps, f"{path.name}: nessun checkout trovato")
                for step in steps:
                    inputs = step.get("with") or {}
                    for override in CHECKOUT_OVERRIDES:
                        self.assertNotIn(
                            override,
                            inputs,
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
        # I test vengono **eseguiti**, non solo compilati: il loro valore sta
        # nelle configurazioni non di default, dove il job `test-unit` non
        # arriva.
        self.assertIn(
            "cargo test --locked -p plenora-database-cli $features -- --skip live_",
            workflow,
        )
        # E si eseguono **tutti**. Il passo filtrava per nome — solo i test che
        # contenevano `usage` — e il filtro era il difetto: cinque test
        # assumevano PostgreSQL e restavano rossi con `--features mysql` e con
        # `--features sqlserver` senza che nessuno li lanciasse. Un filtro per
        # nome sceglie cosa guardare in base a come si chiama, ed e la stessa
        # forma che aveva lasciato rossa una suite per mesi. L'unica selezione
        # ammessa qui e `live_`, che esclude cio che ha bisogno di un server.
        matrix = parsed_jobs(workflow)["cli-feature-matrix"]
        for filtered in (" $features usage", " $features -- usage"):
            self.assertNotIn(
                filtered,
                matrix,
                "la matrice esegue un sottoinsieme dei test scelto per nome",
            )
        # Il ciclo prova tutte le combinazioni prima di uscire: fermarsi alla
        # prima nasconderebbe le altre tre dietro un errore solo.
        self.assertIn("status=1", workflow)
        self.assertIn("exit $status", workflow)
        self.assertNotIn(
            "strategy",
            parsed_jobs(workflow)["cli-feature-matrix"],
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
        static = job_text(workflow, "static-self-tests")
        for suite in (
            "scripts/test_ci_workflows.py",
            "scripts/test_check_mariadb_reference.py",
            "-m unittest scripts.test_live_inventory",
            "scripts/test_check_postgres_reference.py",
            "scripts/test_check_postgres_hardening.py",
            "scripts/test_check_session_matrix.py",
            "scripts/test_check_sqlserver_reference.py",
            "scripts/phase0_validate.py",
            "scripts/render_state.py --check",
        ):
            self.assertIn(f"python3 {suite}", static, f"{suite} non eseguito")
        self.assertIn(
            "python3 -m unittest discover -s tests -t .",
            static,
            "la discovery completa di tests/ non e eseguita",
        )

        # La duplicazione con `sqlserver-assurance` e voluta e dichiarata:
        # senza la nota, il prossimo lettore la toglie credendola una svista.
        # La nota e un **commento**, e i commenti non stanno nell'albero: va
        # cercata nel file, che e la forma in cui un lettore la incontra.
        self.assertIn("Duplicazione dichiarata", workflow)
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
        workflow = self.WORKFLOW.read_text(encoding="utf-8")
        return {
            name: job_text(workflow, name) for name in parsed_jobs(workflow)
        }

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
        # Il ref non viene fissato qui: le action sono pinnate per SHA e il
        # commento accanto ne ricorda il tag, quindi asserire la stringa
        # `@v2` avrebbe fatto fallire questa guardia al primo pinning senza
        # che nulla del percorso di pubblicazione fosse cambiato. Cio che
        # conta e *quale* action consegna, non a quale commit.
        self.assertIn("softprops/action-gh-release@", workflow)

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


class EveryGateIsExecutedBySomebody(unittest.TestCase):
    """La regola 6 di AGENTS.md, resa verificabile.

    «Un gate che nessuno esegue non e un gate.» Il gate del riferimento
    PostgreSQL ci era gia caduto — esisteva, aveva il suo self-test in CI, e
    nessun workflow lo lanciava — e un giro dopo si e scoperto che il gate
    hardening, l'unico che prova TLS privato e mTLS, era nella stessa
    condizione. Due volte lo stesso difetto significa che serve una guardia,
    non una terza correzione.

    Un gate puo legittimamente non girare in CI: campagne prestazionali,
    fixture esterne che il progetto non possiede, wrapper di comodo. Ma allora
    va **dichiarato**, con il motivo, qui: la dichiarazione e la differenza fra
    una scelta e una dimenticanza.
    """

    #: Gate che nessun workflow esegue, e perche.
    DECLARED_WITHOUT_A_WORKFLOW = {
        "check_phase0.py": "wrapper di comodo: esegue phase0_validate.py e la discovery completa di tests/, che il job static-self-tests di rust-ci lancia uno per uno",
        "check_mariadb_divergence.py": "harness manuale citato dalla documentazione MariaDB, si lancia con due riferimenti scelti dall'operatore",
        "check_postgres_matrix.py": "matrice su piu major PostgreSQL: avvia N riferimenti, dura oltre il budget di una CI a ogni push",
        "check_sqlserver_matrix.py": "matrice su piu major SQL Server, stesso motivo della matrice PostgreSQL",
        "check_postgres_performance.py": "campagna prestazionale: misura tempi, e su runner condivisi il risultato non e confrontabile",
        "check_postgres_spatial_performance.py": "campagna prestazionale bbox + KNN, stesso motivo",
        "check_mysql_performance.py": "campagna prestazionale MySQL, stesso motivo",
        "check_sqlserver_azure.py": "gate opt-in: richiede credenziali Azure SQL che il progetto non possiede",
        "check_sqlserver_polybase.py": "gate opzionale: richiede una fixture PolyBase reale",
    }

    #: Gate che un altro gate esegue per conto proprio, e **con quali misure**.
    #:
    #: I simboli non sono decorativi: sono le funzioni che la campagna passa a
    #: `campaign(preflight=..., measure=...)`. Verificarli e la differenza fra
    #: «il caller nomina il gate» e «il caller misura con il gate»: se la
    #: campagna smette di usarli, la delega non esiste piu e la guardia lo
    #: dice, invece di restare verde su un import rimasto per abitudine.
    #: gate -> (caller, modulo, {keyword della campagna: simbolo del gate})
    EXECUTED_BY_ANOTHER_GATE = {
        "check_session_matrix.py": (
            "scripts/check_session_campaign.py",
            "scripts.check_session_matrix",
            {"preflight": "preflight", "measure": "verdict"},
        ),
        "check_mariadb_driver.py": (
            "scripts/check_mariadb_campaign.py",
            "scripts.check_mariadb_driver",
            {"measure": "verdict"},
        ),
        # Il gate del SDK non e nato con una campagna: costruiva gli
        # artefatti, eseguiva la suite e confrontava i conteggi, ma pretendeva
        # i due riferimenti gia accesi, quindi gli scope `live` e `benchmark`
        # non li lanciava nessun workflow. Cio che girava in CI era la sola
        # suite offline, che non tocca un database — e la conseguenza sta in
        # `deny.toml`, dove la migrazione a pyo3 0.29 resta ferma per la
        # copertura live che mancava.
        "check_sdk_tests.py": (
            "scripts/check_sdk_campaign.py",
            "scripts.check_sdk_tests",
            {"preflight": "preflight", "measure": "measure_live_scopes"},
        ),
    }

    @staticmethod
    def executed_commands() -> str:
        # `workflow_files()` copre anche `*.yaml`: una glob piu stretta qui
        # avrebbe dichiarato non eseguito un gate lanciato da un workflow con
        # l'altra estensione.
        return "\n".join(
            run_commands(path.read_text(encoding="utf-8"))
            for path in workflow_files()
        )

    @staticmethod
    def executed_by_a_workflow(gate: str) -> bool:
        """Un workflow qualunque esegue questo gate in un passo incondizionato."""

        return any(
            invoked_gates(path.read_text(encoding="utf-8"), gate)
            for path in workflow_files()
        )

    def test_every_gate_runs_somewhere_or_says_why_not(self) -> None:
        for gate in sorted((WORKFLOW_DIRECTORY.parents[1] / "scripts").glob("check_*.py")):
            name = gate.name
            # Invocato, non nominato: `echo scripts/check_x.py` non esegue
            # niente, e contarlo sarebbe la stessa promessa vuota di una voce
            # sotto `paths:`.
            if self.executed_by_a_workflow(name):
                continue
            if name in self.EXECUTED_BY_ANOTHER_GATE:
                caller, module, expected = self.EXECUTED_BY_ANOTHER_GATE[name]
                path = WORKFLOW_DIRECTORY.parents[1] / caller
                symbols = tuple(expected.values())
                self.assertTrue(
                    self.executed_by_a_workflow(caller.split("/")[-1]),
                    f"{name} dipende da {caller}, che nessun workflow esegue",
                )
                self.assertTrue(
                    imports_from(path, module, symbols),
                    f"{caller} non importa {list(symbols)} da {module}",
                )
                self.assertTrue(
                    measures_with(path, module, expected),
                    f"{caller} non passa alla campagna {expected} da {module}: "
                    "la delega non esiste piu",
                )
                continue
            self.assertIn(
                name,
                self.DECLARED_WITHOUT_A_WORKFLOW,
                f"{name} non e eseguito da nessun workflow e non dichiara perche",
            )
            self.assertGreater(
                len(self.DECLARED_WITHOUT_A_WORKFLOW[name]),
                20,
                f"{name}: la dichiarazione deve dire il motivo",
            )

    def test_the_declaration_does_not_outlive_the_gates(self) -> None:
        """Una dichiarazione su un gate che non esiste piu non dichiara nulla."""

        scripts = WORKFLOW_DIRECTORY.parents[1] / "scripts"
        for name in {
            *self.DECLARED_WITHOUT_A_WORKFLOW,
            *self.EXECUTED_BY_ANOTHER_GATE,
        }:
            self.assertTrue((scripts / name).exists(), name)

    STEPS = "jobs:\n  j:\n    steps:\n"

    def invoked(self, workflow: str) -> bool:
        return invoked_gates(workflow, "check_inventato.py")

    def test_a_path_trigger_alone_does_not_count_as_execution(self) -> None:
        """La mutazione che la prima versione della guardia non vedeva.

        Tolto il passo che lo lancia, il gate resta nominato fra i `paths:` e
        nei commenti — e la guardia, che leggeva il file intero, restava
        verde.
        """

        workflow = (WORKFLOW_DIRECTORY / "postgres-assurance.yml").read_text(
            encoding="utf-8"
        )
        self.assertTrue(invoked_gates(workflow, "check_postgres_hardening.py"))

        mutated = "\n".join(
            line
            for line in workflow.splitlines()
            if "python3 scripts/check_postgres_hardening.py" not in line
        )
        # Il nome c'e ancora: sta fra i trigger e nei commenti del job.
        self.assertIn("scripts/check_postgres_hardening.py", mutated)
        self.assertFalse(invoked_gates(mutated, "check_postgres_hardening.py"))

    def test_a_comment_does_not_count_as_execution(self) -> None:
        """Un commento YAML e uno shell descrivono, non eseguono."""

        yaml_comment = (
            "jobs:\n  j:\n    steps:\n"
            "      # esegue scripts/check_inventato.py, prima o poi\n"
            "      - run: echo ciao\n"
        )
        self.assertFalse(self.invoked(yaml_comment))

        shell_comment = self.STEPS + (
            "      - run: echo ok # python3 scripts/check_inventato.py\n"
        )
        self.assertFalse(self.invoked(shell_comment))

    def test_a_conditional_step_or_job_does_not_count_as_execution(self) -> None:
        """Un `if:` spegne il passo, e sul job spegne tutti i suoi passi.

        Nessuna condizione e valutabile leggendo il file: contarla come
        eseguita e la stessa promessa vuota di una voce sotto `paths:`.
        """

        shapes = {
            "condizione sul passo": self.STEPS
            + "      - if: false\n        run: python3 scripts/check_inventato.py\n",
            "condizione dopo il comando": self.STEPS
            + "      - run: python3 scripts/check_inventato.py\n"
            + "        if: always()\n",
            "condizione sul job": "jobs:\n  j:\n    if: false\n    steps:\n"
            + "      - run: python3 scripts/check_inventato.py\n",
        }
        for label, workflow in shapes.items():
            with self.subTest(label):
                self.assertFalse(self.invoked(workflow))

    def test_the_parser_gives_every_scalar_its_yaml_meaning(self) -> None:
        """Le forme che la lettura fatta a mano sbagliava, ora corrette.

        `>` ripiega i ritorni a capo in spazi: il comando diventa un solo
        `echo` con il nome del gate come argomento, quindi il gate **non** e
        invocato. `|` li conserva, quindi lo e. Uno scalare quotato e un
        comando come gli altri. Un anchor viene risolto invece di far sparire
        il job.
        """

        folded = self.STEPS + (
            "      - run: >\n          echo\n          python3 scripts/check_inventato.py\n"
        )
        self.assertFalse(self.invoked(folded))

        literal = self.STEPS + (
            "      - run: |\n          python3 scripts/check_inventato.py\n          echo x\n"
        )
        self.assertTrue(self.invoked(literal))

        quoted = self.STEPS + '      - run: "python3 scripts/check_inventato.py"\n'
        self.assertTrue(self.invoked(quoted))

        anchored = (
            "jobs:\n  j:\n    steps: &common\n"
            "      - run: python3 scripts/check_inventato.py\n"
            "  k:\n    steps: *common\n"
        )
        self.assertTrue(self.invoked(anchored))

    def test_the_gate_must_be_the_first_command_of_its_step(self) -> None:
        """La convenzione al posto del modello di shell.

        Modellare una shell si e rivelato inaffidabile a ogni giro: `false &&
        gate`, una continuazione con backtick di PowerShell, il corpo di un
        here-document, una sostituzione `$(...)` sono tutte forme in cui «il
        gate compare in una riga» e «il gate viene eseguito» non coincidono, e
        ogni regola nuova ne lasciava fuori un'altra. La domanda e cambiata: un
        gate qualificante e il primo comando del proprio passo.
        """

        non_qualifica = {
            "dopo un altro comando": "      - run: |\n          echo pronto\n"
            "          python3 scripts/check_inventato.py\n",
            "dietro una continuazione": "      - run: |\n          echo \\\n"
            "          python3 scripts/check_inventato.py\n",
            "dentro un here-document": "      - run: |\n          cat <<'EOF'\n"
            "          python3 scripts/check_inventato.py\n          EOF\n",
            "dietro un cortocircuito": "      - run: |\n"
            "          false && python3 scripts/check_inventato.py\n",
        }
        for label, shape in non_qualifica.items():
            with self.subTest(label):
                self.assertFalse(self.invoked(self.STEPS + shape))

        qualifica = {
            "primo comando": "      - run: |\n"
            "          python3 scripts/check_inventato.py\n",
            "con una pipe dopo": "      - run: |\n"
            "          python3 scripts/check_inventato.py 2>&1 \\\n"
            "            | tee out.log\n",
            "dopo un commento": "      - run: |\n          # il gate\n"
            "          python3 scripts/check_inventato.py\n",
        }
        for label, shape in qualifica.items():
            with self.subTest(label):
                self.assertTrue(self.invoked(self.STEPS + shape))

    def test_every_real_gate_is_the_first_command_of_its_step(self) -> None:
        """La convenzione deve valere sui workflow che esistono davvero.

        Dove non valeva, la preparazione e stata spostata in un passo suo: un
        passo che fa una cosa sola e anche un rosso attribuibile.
        """

        gates = [path.name for path in sorted((ROOT / "scripts").glob("check_*.py"))]
        for path in workflow_files():
            workflow = path.read_text(encoding="utf-8")
            for step in executed_steps(workflow):
                script = step.get("run")
                if not isinstance(script, str):
                    continue
                for gate in gates:
                    if f"scripts/{gate}" not in script:
                        continue
                    with self.subTest(workflow=path.name, gate=gate):
                        self.assertTrue(
                            gate_invocation(gate).search(first_command(script)),
                            f"{gate} non e il primo comando del suo passo",
                        )

    def test_a_gate_named_but_not_invoked_does_not_count(self) -> None:
        """Solo un comando che **comincia** una riga conta come invocazione.

        Un separatore di shell non basta: `true || python3 gate.py` non lo
        esegue quando il primo comando riesce, e `echo "x; python3 gate.py"`
        lo stampa soltanto.
        """

        non_invocations = (
            "      - run: echo python3 scripts/check_inventato.py\n",
            '      - run: echo "x; python3 scripts/check_inventato.py --fake"\n',
            "      - run: true || python3 scripts/check_inventato.py\n",
        )
        for shape in non_invocations:
            with self.subTest(shape.strip()[:30]):
                self.assertFalse(self.invoked(self.STEPS + shape))

        invocations = (
            "      - run: python3 scripts/check_inventato.py --static\n",
            "      - run: python3 -m scripts.check_inventato\n",
            "      - run: |\n          python3 scripts/check_inventato.py 2>&1 \\\n"
            "            | tee out.log\n",
        )
        for shape in invocations:
            with self.subTest(shape.strip()[:30]):
                self.assertTrue(self.invoked(self.STEPS + shape))

    def test_an_action_input_is_not_a_command(self) -> None:
        """Gli input di un'action non sono comandi del passo."""

        for shape in (
            "      - uses: a/b@c\n        with:\n"
            "          run: python3 scripts/check_inventato.py\n",
            "      - uses: a/b@c\n"
            "        with: {run: python3 scripts/check_inventato.py}\n",
        ):
            with self.subTest(shape.strip()[:24]):
                self.assertFalse(self.invoked(self.STEPS + shape))

    def test_a_key_named_if_inside_env_is_not_a_condition(self) -> None:
        workflow = self.STEPS + (
            "      - run: python3 scripts/check_inventato.py\n"
            "        env:\n          IF: si\n"
        )
        self.assertTrue(self.invoked(workflow))

    #: Forme che l'albero YAML non convalida: la lettura fallisce invece di
    #: proseguire su una struttura che non e quella dichiarata.
    INVALID = {
        "chiave di passo sconosciuta": "jobs:\n  j:\n    steps:\n"
        "      - run: echo x\n        strategy: q\n",
        "chiave di job sconosciuta": "jobs:\n  j:\n    inventata: si\n    steps:\n"
        "      - run: echo x\n",
        "steps che non e una sequenza": "jobs:\n  j:\n    steps: qualcosa\n",
        "job che non e una mappa": "jobs:\n  j: qualcosa\n",
        "documento senza jobs": "on:\n  push:\n",
    }

    def test_every_invalid_shape_fails_instead_of_being_misread(self) -> None:
        for label, workflow in self.INVALID.items():
            with self.subTest(label):
                with self.assertRaises(RuntimeError):
                    run_commands(workflow)

    def test_every_real_workflow_is_inside_the_declared_shape(self) -> None:
        """La validazione semantica deve bastare ai workflow che esistono."""

        for path in workflow_files():
            with self.subTest(path.name):
                parsed_jobs(path.read_text(encoding="utf-8"))

    #: Il modulo e le keyword della campagna usati dai casi qui sotto.
    MODULE = "scripts.check_inventato"
    EXPECTED = {"preflight": "preflight", "measure": "verdict"}

    def test_the_delegation_is_verified_on_the_measures_not_on_a_mention(self) -> None:
        """Importare non basta, e nemmeno passare i simboli in ordine libero.

        Le quattro versioni precedenti provavano a dedurre l'esecuzione dalla
        sintassi, e ogni volta restava un modo di certificare un caller che il
        gate non lo lancia. Qui la relazione e dichiarata — quale keyword della
        campagna riceve quale simbolo di quale modulo — e si verifica
        esattamente.
        """

        casi = {
            "import senza misura": (
                "from scripts.check_inventato import preflight, verdict\n",
                False,
            ),
            "delega autentica": (
                "from scripts.check_inventato import preflight, verdict\n"
                "\ndef main():\n"
                "    return campaign(preflight=preflight, measure=verdict)\n",
                True,
            ),
            "import spezzati e con alias": (
                "from scripts.check_inventato import preflight\n"
                "from scripts.check_inventato import verdict as misura\n"
                "\ndef main():\n"
                "    return campaign(preflight=preflight, measure=misura)\n",
                True,
            ),
            "misura sostituita": (
                "from scripts.check_inventato import preflight, verdict\n"
                "\ndef main():\n"
                "    return campaign(preflight=preflight, measure=altro)\n",
                False,
            ),
            "keyword sbagliate": (
                "from scripts.check_inventato import preflight, verdict\n"
                "\ndef main():\n"
                "    return campaign(foo=preflight, bar=verdict)\n",
                False,
            ),
            "funzioni scambiate": (
                "from scripts.check_inventato import preflight, verdict\n"
                "\ndef main():\n"
                "    return campaign(preflight=verdict, measure=preflight)\n",
                False,
            ),
            "ramo spento": (
                "from scripts.check_inventato import preflight, verdict\n"
                "if False:\n"
                "    campaign(preflight=preflight, measure=verdict)\n",
                False,
            ),
            "modulo omonimo in un altro package": (
                "from altrove.check_inventato import preflight, verdict\n"
                "\ndef main():\n"
                "    return campaign(preflight=preflight, measure=verdict)\n",
                False,
            ),
        }
        with tempfile.TemporaryDirectory() as directory:
            caller = Path(directory) / "campagna.py"
            for label, (source, expected) in casi.items():
                with self.subTest(label):
                    caller.write_text(source, encoding="utf-8")
                    self.assertEqual(
                        measures_with(caller, self.MODULE, self.EXPECTED),
                        expected,
                    )

    def test_the_import_must_come_from_the_declared_module(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            caller = Path(directory) / "campagna.py"

            caller.write_text(
                "from scripts.check_inventato import preflight\n"
                "from scripts.check_inventato import verdict as misura\n",
                encoding="utf-8",
            )
            self.assertTrue(
                imports_from(caller, self.MODULE, ("preflight", "verdict")),
                "import spezzati e alias sono forme legittime",
            )

            caller.write_text(
                "from altrove.check_inventato import preflight, verdict\n",
                encoding="utf-8",
            )
            self.assertFalse(
                imports_from(caller, self.MODULE, ("preflight", "verdict")),
                "l'ultimo segmento non identifica il modulo",
            )

    def test_a_multiline_run_block_is_read(self) -> None:
        workflow = (
            "jobs:\n"
            "  vero:\n"
            "    steps:\n"
            "      - name: gate\n"
            "        run: |\n"
            "          python3 scripts/check_inventato.py 2>&1 \\\n"
            "            | tee out.log\n"
            "      - name: dopo\n"
            "        run: echo fine\n"
        )
        commands = run_commands(workflow)
        self.assertIn("scripts/check_inventato.py", commands)
        self.assertIn("echo fine", commands)

    def test_the_hardening_gate_is_executed_and_not_only_self_tested(self) -> None:
        """Il caso concreto: `rust-ci` ne lanciava solo il self-test.

        TLS privato, mTLS, verifica dell'hostname e recovery sul fixture
        cifrato potevano rompersi senza far diventare rosso niente.
        """

        assurance = (WORKFLOW_DIRECTORY / "postgres-assurance.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("python3 scripts/check_postgres_hardening.py", assurance)
        self.assertIn("docker-compose.postgres-tls.yml", assurance)


if __name__ == "__main__":
    unittest.main()
