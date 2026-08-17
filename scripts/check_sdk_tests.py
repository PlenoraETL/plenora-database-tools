#!/usr/bin/env python3
"""Suite del SDK Python, sempre sul wheel appena costruito.

Il modulo nativo `plenora_database/_native.abi3.so` e gitignorato: non e un
artefatto del repository, e nessuno lo rigenera automaticamente. Chi cambia
il Rust e poi lancia `pytest` a mano esegue il binario precedente, e ottiene
un risultato che non riguarda il codice che ha scritto — rosso su codice
corretto, o verde su codice rotto. E successo due volte in una sola
sessione: la correzione sul `SessionContext` e quella sul limite di 52
caratteri sono state entrambe "smentite" da un `.so` vecchio.

Questo runner toglie il caso dalla mano di chi esegue:

1. rifiuta di partire se l'albero di lavoro non e pulito — il verdetto
   nomina un commit, e cio che non e in quel commit non e descritto da
   quel nome;
2. costruisce il wheel con `maturin --locked` dentro l'immagine Rust, e lo
   esporta in una directory temporanea **fuori** dal repository;
3. lo installa nel container di test con `pip install --no-deps`, senza
   scrivere nulla nel source tree;
4. esegue `pytest` fuori dal source tree, senza `PYTHONPATH` verso il
   package locale, e verifica prima di partire che `plenora_database` e il
   suo modulo nativo arrivino da `site-packages`;
5. verifica che ne' la build ne' i test abbiano cambiato l'albero, e
   registra nel verdetto di cosa e fatto l'artefatto che ha girato.

Uso:

    python scripts/check_sdk_tests.py                  # Postgres + MySQL
    python scripts/check_sdk_tests.py --offline        # solo test senza server
    python scripts/check_sdk_tests.py --benchmark-only # solo i bench di parita
    python scripts/check_sdk_tests.py --allow-dirty    # verdetto non autorevole

**Tracciato, non riproducibile.** `rust:1.92` e `python:3.13-slim` sono tag
mutabili — la stessa riga puo risolvere due immagini diverse a distanza di un
giorno — e l'`apt-get install` della build prende cio che il mirror pubblica
oggi. Quel che il runner puo fare, e fa, e dire con cosa ha girato: id e
digest delle due immagini, versione di rustc e di Python effettive, pin di
pip confrontati con il `pip freeze` di chi li ha installati. Chiamarlo
"riproducibile" prometterebbe che una seconda corsa ricostruisce lo stesso
ambiente, che nessuna di queste misure garantisce.

Reti, volumi e credenziali non sono scritti a mano: si chiedono a Docker con
gli stessi helper dei tre gate di riferimento. Una password ricopiata qui
sarebbe una seconda fonte per un dato che ne ha una sola, il compose.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import (  # noqa: E402
    compose_network_arguments,
    compose_volume,
    container_variable,
)
from scripts.sdk_wheel_probe import ORIGIN_MARKER  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "crates" / "plenora-database-py"
PYPROJECT = CRATE / "pyproject.toml"
NATIVE = CRATE / "python" / "plenora_database" / "_native.abi3.so"
PROBE = Path(__file__).resolve().parent / "sdk_wheel_probe.py"
BUILD_REQUIREMENTS = ROOT / "requirements-sdk-build.txt"
TEST_REQUIREMENTS = ROOT / "requirements-sdk-tests.txt"
RUST_IMAGE = "rust:1.92"
PYTHON_IMAGE = "python:3.13-slim"

# Il repository entra nel container dei test in sola lettura, e la suite gira
# altrove: `python/tests` e un package, quindi pytest inserirebbe in
# `sys.path` la sua directory padre — cioe `python/`, dove vive la copia
# sorgente di `plenora_database`, che vincerebbe sul wheel installato.
REPOSITORY_MOUNT = "/repo"
WHEEL_MOUNT = "/wheels"
SUITE_DIRECTORY = "/suite"

# Il percorso che l'installazione di un wheel produce nell'immagine Python
# ufficiale. La verifica che conta la fa il probe contro `sysconfig` dentro
# l'interprete; questo confronto e la sua controparte sul verdetto, e vive
# accanto a `PYTHON_IMAGE` perche e da quella scelta che dipende.
SITE_PACKAGES = "/site-packages/"

# Il bench di parita confronta il SDK con il CLI in subprocess, quindi gli
# serve un binario Linux. Il percorso lo passa il runner, che e l'unico a
# sapere dove ha montato il repository: scritto dentro il test era il punto
# di mount di allora, e al primo cambio il bench si e saltato da solo.
CLI_BINARY = Path("target/release/plenora-database")

POSTGRES_CONTAINER = "dataflow-postgres"
MYSQL_CONTAINER = "dataflow-mysql"
POSTGRES_PORT = 5432

# Righe che i container stampano per il verdetto. Il prefisso le rende
# riconoscibili dentro l'output di pytest senza dover eseguire un secondo
# container solo per chiedere le versioni — che significherebbe reinstallare
# tutto e, soprattutto, misurare un ambiente diverso da quello che ha girato.
BUILD_MARKER = "PLENORA_SDK_GATE_BUILD "
PYTHON_MARKER = "PLENORA_SDK_GATE_PYTHON "
PACKAGES_MARKER = "PLENORA_SDK_GATE_PACKAGES "

# `pip freeze` non elenca gli strumenti dell'immagine, ma la lista cambia da
# una versione di pip all'altra: se compaiono, non sono una deriva dei pin.
TOOLING_PACKAGES = frozenset({"pip", "setuptools", "wheel"})

# Il pacchetto sotto esame non e un pin dell'ambiente: e cio che il gate ha
# costruito, e la sua identita sta nei digest del verdetto. Nel `pip freeze`
# compare come `plenora-database==...` o, se pip preferisce la provenienza,
# come `plenora_database @ file:///wheels/...`.
ARTIFACT_PACKAGE = "plenora-database"


def run(command: list[str], *, capture: bool = False) -> str:
    # Encoding esplicito: su Windows `text=True` decodifica con la codepage
    # locale, e l'output di cargo contiene byte che cp1252 non mappa. Il
    # thread di lettura moriva con `UnicodeDecodeError` e il runner
    # proseguiva con un output vuoto — cioe senza le righe che il verdetto
    # deve leggere, per una ragione che non riguarda ne build ne test.
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=capture,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode:
        if capture:
            sys.stderr.write(completed.stdout)
            sys.stderr.write(completed.stderr)
        raise RuntimeError(f"comando fallito: {' '.join(command[:3])}")
    return completed.stdout if capture else ""


def git(arguments: list[str]) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=False,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )
    if completed.returncode:
        raise RuntimeError(
            f"git {' '.join(arguments)} fallito: {completed.stderr.strip()}"
        )
    return completed.stdout


# --------------------------------------------------------------------------
# Pin dichiarati e pin effettivi
# --------------------------------------------------------------------------


def pinned_versions(requirements: str) -> dict[str, str]:
    """I pin `nome==versione` del file, per nome normalizzato.

    # Raises

    `RuntimeError` per una riga che non fissa una versione esatta: un
    requirement senza `==` lascia risolvere la versione al momento
    dell'installazione, ed e proprio cio che questi file esistono per
    impedire.
    """

    pins: dict[str, str] = {}
    for raw in requirements.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        name, separator, version = line.partition("==")
        if not separator or not version.strip():
            raise RuntimeError(f"requirement senza versione esatta: '{line}'")
        pins[normalise_package(name)] = version.strip()
    return pins


def normalise_package(name: str) -> str:
    """Il nome canonico PEP 503: `Pygments` e `pygments` sono lo stesso."""

    return name.strip().lower().replace("_", "-").replace(".", "-")


def freeze_name(entry: str) -> str:
    """Il nome del pacchetto in una voce di `pip freeze`.

    Le voci hanno due forme: `nome==versione` e `nome @ url`, la seconda per
    cio che e stato installato da un file. Il wheel sotto esame e installato
    proprio cosi, quindi leggere solo la prima forma lo renderebbe invisibile.
    """

    return normalise_package(entry.split("@", 1)[0].partition("==")[0])


def version_tuple(version: str) -> tuple[int, ...]:
    """I soli componenti numerici, per confrontare con i limiti dichiarati."""

    components: list[int] = []
    for part in version.split("."):
        digits = ""
        for character in part:
            if not character.isdigit():
                break
            digits += character
        if not digits:
            break
        components.append(int(digits))
    return tuple(components)


def maturin_bounds(pyproject: str) -> tuple[str, str]:
    """`(minimo incluso, massimo escluso)` dichiarati da `build-system`."""

    for raw in pyproject.splitlines():
        line = raw.strip()
        if not line.startswith("requires"):
            continue
        marker = "maturin>="
        if marker not in line:
            continue
        remainder = line.split(marker, 1)[1]
        minimum, _, rest = remainder.partition(",")
        upper = rest.split("<", 1)[1] if "<" in rest else ""
        return minimum.strip(), upper.strip(' "\']')
    raise RuntimeError("pyproject.toml non dichiara un vincolo su maturin")


def validate_maturin_pin(pyproject: str, requirements: str) -> str:
    """La maturin che il runner installa, verificata contro il pyproject.

    Il pacchetto dichiara con quale intervallo di maturin puo essere
    costruito. Installarne una fuori da quell'intervallo produce un wheel che
    il pacchetto stesso non descrive: il verdetto direbbe "costruito", e chi
    lo rilegge non avrebbe modo di accorgersene.

    # Raises

    `RuntimeError` se il pin manca o cade fuori dal vincolo.
    """

    pinned = pinned_versions(requirements).get("maturin")
    if pinned is None:
        raise RuntimeError(
            f"{BUILD_REQUIREMENTS.name} non fissa una versione di maturin"
        )
    minimum, upper = maturin_bounds(pyproject)
    if version_tuple(pinned) < version_tuple(minimum):
        raise RuntimeError(
            f"maturin {pinned} e sotto il minimo {minimum} del pyproject"
        )
    if upper and version_tuple(pinned) >= version_tuple(upper):
        raise RuntimeError(
            f"maturin {pinned} raggiunge il limite {upper} del pyproject"
        )
    return pinned


def validate_installed_pins(installed: dict[str, str]) -> None:
    """Ogni pin dichiarato e installato, e nient'altro lo e.

    Un pin che nessuno installa e una dichiarazione, non un vincolo: senza
    questo confronto il file dei requirement potrebbe restare fermo mentre il
    container risolve tutt'altro, e il verdetto registrerebbe versioni che
    nessuno ha imposto.

    # Raises

    `RuntimeError` alla prima divergenza, nominandola.
    """

    declared = pinned_versions(TEST_REQUIREMENTS.read_text(encoding="utf-8"))
    for name, version in sorted(declared.items()):
        actual = installed.get(name)
        if actual is None:
            raise RuntimeError(f"pacchetto dichiarato ma non installato: {name}")
        if actual != version:
            raise RuntimeError(
                f"{name} installato in versione {actual}, dichiarata {version}"
            )
    extra = sorted(set(installed) - set(declared) - TOOLING_PACKAGES)
    if extra:
        raise RuntimeError(f"pacchetti installati e non dichiarati: {extra}")


# --------------------------------------------------------------------------
# L'albero di lavoro: pulito prima, fermo durante
# --------------------------------------------------------------------------


def porcelain_entries() -> list[str]:
    """Le righe di `git status --porcelain -uall`.

    `-uall` e la parte che conta: senza, un file **nuovo** dentro una
    directory nuova comparirebbe come la sola directory, e un sorgente non
    ancora tracciato — cioe codice che nessun commit descrive — puo benissimo
    essere quello che la suite sta per compilare.
    """

    return [
        line for line in git(["status", "--porcelain", "-uall"]).splitlines()
        if line.strip()
    ]


def worktree_state() -> dict[str, str]:
    """Lo stato dell'albero, percorso per percorso, untracked inclusi.

    Serve a confrontare *durante* la corsa: una build che riscrive
    `Cargo.lock`, un test che lascia una fixture sul disco o una formattazione
    applicata di straforo cambierebbero il repository mentre lo si sta
    verificando, e il verdetto parlerebbe di un albero diverso da quello di
    partenza. Il conteggio di righe aggiunte e rimosse distingue due
    modifiche diverse dello stesso file, che lo stato porcelain da solo
    mostrerebbe uguali.
    """

    state: dict[str, str] = {}
    for line in porcelain_entries():
        if len(line) > 3:
            state[line[3:].strip()] = line[:2]
    for line in git(["diff", "HEAD", "--numstat"]).splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            path = parts[2].strip()
            state[path] = f"{state.get(path, '')}+{parts[0]}-{parts[1]}"
    return state


def assert_clean_worktree(entries: list[str]) -> None:
    """L'albero deve coincidere con HEAD prima che il gate parta.

    Il verdetto identifica cio che ha girato con il nome di un commit. Se
    l'albero porta modifiche staged, non staged o file mai tracciati, quel
    nome descrive qualcos'altro: il wheel viene costruito dai file su disco,
    e nessuno che rilegga il verdetto puo ricostruire quali fossero.

    # Raises

    `RuntimeError` con l'elenco delle righe di `git status`, che dicono anche
    di che tipo di divergenza si tratta.
    """

    if not entries:
        return
    raise RuntimeError(
        "l'albero di lavoro non coincide con HEAD: "
        f"{entries} — il verdetto nomina un commit, e questi file non sono in "
        "quel commit. Committa, oppure esegui con --allow-dirty per una corsa "
        "esplicitamente non autorevole."
    )


def assert_worktree_unchanged(before: dict[str, str], stage: str) -> None:
    """Confronta con lo stato di partenza e fallisce nominando i file.

    # Raises

    `RuntimeError` con l'elenco dei percorsi cambiati durante `stage`.
    """

    after = worktree_state()
    if after == before:
        return
    changed = sorted(
        set(before) ^ set(after)
        | {path for path in set(before) & set(after) if before[path] != after[path]}
    )
    raise RuntimeError(f"{stage} ha modificato l'albero di lavoro: {changed}")


# --------------------------------------------------------------------------
# Identita delle immagini
# --------------------------------------------------------------------------


def image_identity(reference: str) -> dict[str, object]:
    """Id e digest dell'immagine **locale** dietro un riferimento.

    Un tag e mutabile: `rust:1.92` oggi e `rust:1.92` fra un mese possono
    essere due immagini diverse, con due toolchain diverse, e il verdetto non
    avrebbe modo di dire quale delle due ha eseguito. Si chiede a Docker dopo
    la corsa, quando l'immagine e certamente presente.
    """

    raw = run(
        [
            "docker", "image", "inspect",
            "--format", "{{.Id}} {{json .RepoDigests}}",
            reference,
        ],
        capture=True,
    ).strip()
    identifier, _, digests = raw.partition(" ")
    return {
        "reference": reference,
        "id": identifier,
        "digests": json.loads(digests or "[]"),
    }


# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------


def build_wheel(destination: Path) -> dict[str, str]:
    """Costruisce il wheel con maturin e lo lascia in `destination`.

    Il source tree non riceve niente: il modulo nativo che vi si trovasse e
    per costruzione quello di una corsa precedente, e va tolto di mezzo —
    e proprio la copia che un `pytest` a mano finirebbe per caricare.

    # Returns

    Nome e SHA-256 del wheel, versioni di maturin e di rustc. Il solo nome non
    identifica l'artefatto: due wheel con lo stesso nome escono da qualunque
    coppia di build, e il verdetto serve proprio a dire *quale* ha girato.

    # Raises

    `RuntimeError` se la build fallisce o non produce esattamente un wheel.
    """

    if NATIVE.exists():
        NATIVE.unlink()

    script = (
        "set -e; "
        "apt-get update -qq >/dev/null 2>&1; "
        "apt-get install -y -qq python3 python3-venv >/dev/null 2>&1; "
        "python3 -m venv /tmp/v; "
        f"/tmp/v/bin/pip install -q -r /workspace/{BUILD_REQUIREMENTS.name}; "
        "cd /workspace/crates/plenora-database-py; "
        f"/tmp/v/bin/maturin build --release --locked --out {WHEEL_MOUNT}; "
        f'echo "{BUILD_MARKER}'
        "$(/tmp/v/bin/maturin --version | cut -d' ' -f2) "
        "$(rustc --version | cut -d' ' -f2)\""
    )
    output = run(
        [
            "docker", "run", "--rm",
            "-v", f"{ROOT}:/workspace",
            "-v", f"{destination}:{WHEEL_MOUNT}",
            "-v", "plenora_cargo_registry:/usr/local/cargo/registry",
            "-v", "plenora_cargo_git:/usr/local/cargo/git",
            "-v", "pln_target_docker:/workspace/target-docker",
            "-w", "/workspace",
            "-e", "CARGO_TARGET_DIR=/workspace/target-docker",
            RUST_IMAGE, "sh", "-c", script,
        ],
        capture=True,
    )
    wheels = sorted(destination.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(
            f"la build ha lasciato {len(wheels)} wheel invece di uno: "
            f"{[wheel.name for wheel in wheels]}"
        )
    marker = marker_line(output, BUILD_MARKER)
    fields = marker.split()
    if len(fields) != 2:
        raise RuntimeError(f"riga di build non interpretabile: '{marker}'")
    maturin, rustc = fields
    return {
        "wheel": wheels[0].name,
        "wheel_sha256": hashlib.sha256(wheels[0].read_bytes()).hexdigest(),
        "maturin": maturin,
        "rustc": rustc,
    }


def marker_line(output: str, marker: str) -> str:
    """La coda dell'ultima riga che porta `marker`.

    # Raises

    `RuntimeError` se la riga manca: senza, il verdetto non potrebbe dire
    con cosa e stato costruito o eseguito, e un verdetto che non lo dice non
    prova nulla.
    """

    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            return line[len(marker) :].strip()
    raise RuntimeError(f"output senza la riga '{marker.strip()}'")


# --------------------------------------------------------------------------
# Esecuzione della suite
# --------------------------------------------------------------------------


def live_environment() -> list[str]:
    """Le variabili dei riferimenti, lette dai container in esecuzione."""

    postgres_user = container_variable(POSTGRES_CONTAINER, "POSTGRES_USER")
    postgres_password = container_variable(POSTGRES_CONTAINER, "POSTGRES_PASSWORD")
    postgres_database = container_variable(POSTGRES_CONTAINER, "POSTGRES_DB")
    dsn = (
        f"host={POSTGRES_CONTAINER} port={POSTGRES_PORT} user={postgres_user} "
        f"password={postgres_password} dbname={postgres_database}"
    )
    return [
        "-e", f"PLENORA_TEST_POSTGRES_DSN={dsn}",
        "-e", f"PLENORA_TEST_MYSQL_HOST={MYSQL_CONTAINER}",
        "-e",
        f"PLENORA_TEST_MYSQL_DATABASE="
        f"{container_variable(MYSQL_CONTAINER, 'MYSQL_DATABASE')}",
        "-e",
        f"PLENORA_TEST_MYSQL_USER="
        f"{container_variable(MYSQL_CONTAINER, 'MYSQL_USER')}",
        "-e",
        f"PLENORA_TEST_MYSQL_PASSWORD="
        f"{container_variable(MYSQL_CONTAINER, 'MYSQL_PASSWORD')}",
        "-e", "PLENORA_TEST_MYSQL_CA=/mysql-tls/ca.pem",
        "-e", "PLENORA_BENCH_PARITY=1",
        "-e", f"PLENORA_CLI_BIN={REPOSITORY_MOUNT}/{CLI_BINARY.as_posix()}",
    ]


def assert_benchmark_prerequisites() -> None:
    """`--benchmark-only` senza binario CLI non misurerebbe niente.

    Nella corsa completa l'assenza del binario e uno skip visibile in mezzo a
    duecento test che hanno risposto. Qui sarebbe l'intero scope: il bench di
    parita si salta, l'altro bench gira, e il verdetto dice "passed" per una
    corsa che non ha confrontato nulla.

    # Raises

    `RuntimeError` nominando il percorso atteso e come costruirlo.
    """

    if (ROOT / CLI_BINARY).exists():
        return
    raise RuntimeError(
        f"--benchmark-only senza il binario CLI in {CLI_BINARY.as_posix()}: "
        "il bench di parita si salterebbe e lo scope non misurerebbe nulla. "
        "Costruiscilo per Linux (e il container a eseguirlo) con "
        "`cargo build --release -p plenora-database-cli`."
    )


def pytest_command(*, scope: str, wheels: Path, wheel: str) -> list[str]:
    """Il `docker run` che esegue la suite nello scope richiesto.

    Il wheel arriva dalla directory temporanea dove la build lo ha lasciato e
    viene installato con `--no-deps`: le dipendenze sono gia quelle dei pin,
    e lasciare che il wheel ne risolva altre significherebbe girare su un
    ambiente diverso da quello dichiarato.

    La suite viene copiata in `/suite` e girata da li. Non e una comodita:
    `python/tests` e un package, quindi pytest inserirebbe in `sys.path` la
    directory che lo contiene — `python/` — e da li `plenora_database` si
    importa dal source tree, con dentro qualunque `.so` sia rimasto. Il
    repository e montato in sola lettura per la stessa ragione, e nessun
    `PYTHONPATH` punta al package locale.

    `offline` non tocca i riferimenti — nessuna rete, nessuna credenziale, i
    test live si saltano da soli. `benchmark` e `live` vedono entrambi i
    riferimenti; il primo filtra i soli bench di parita, che senza filtro
    girerebbero comunque dentro la corsa completa.
    """

    command = ["docker", "run", "--rm"]
    environment: list[str] = []
    selection = ["tests"]

    if scope != "offline":
        command += compose_network_arguments(POSTGRES_CONTAINER, MYSQL_CONTAINER)
        tls_volume = compose_volume(MYSQL_CONTAINER, "/etc/mysql/tls")
        command += ["-v", f"{tls_volume}:/mysql-tls:ro"]
        environment = live_environment()
    if scope == "benchmark":
        selection += ["-k", "benchmark"]

    suite = f"{REPOSITORY_MOUNT}/crates/plenora-database-py/python/tests"
    script = (
        "set -e; "
        f"pip install -q -r {REPOSITORY_MOUNT}/{TEST_REQUIREMENTS.name}; "
        f"pip install -q --no-deps {WHEEL_MOUNT}/{wheel}; "
        f"cp -r {suite} {SUITE_DIRECTORY}/tests; "
        f"rm -rf {SUITE_DIRECTORY}/tests/__pycache__; "
        f'echo "{PYTHON_MARKER}$(python -V 2>&1 | cut -d\' \' -f2)"; '
        f'echo "{PACKAGES_MARKER}$(pip freeze | tr \'\\n\' \' \')"; '
        f"python {REPOSITORY_MOUNT}/scripts/{PROBE.name}; "
        f"python -m pytest {' '.join(selection)} -q -rs"
    )
    command += [
        "-v", f"{ROOT}:{REPOSITORY_MOUNT}:ro",
        "-v", f"{wheels}:{WHEEL_MOUNT}:ro",
        "-w", SUITE_DIRECTORY,
        *environment,
        PYTHON_IMAGE, "sh", "-c", script,
    ]
    return command


def installed_versions(output: str) -> dict[str, str]:
    """Le versioni effettive dell'ambiente che ha eseguito la suite.

    # Raises

    `RuntimeError` se i pin divergono da cio che risulta installato, o se il
    wheel sotto esame non compare affatto: `pip install` di un file puo
    fallire in modi che lasciano l'ambiente utilizzabile, e la suite
    girerebbe su una copia che nessuno ha costruito qui.
    """

    packages: dict[str, str] = {}
    entries = marker_line(output, PACKAGES_MARKER).split()
    for entry in entries:
        name, separator, version = entry.partition("==")
        if separator:
            packages[normalise_package(name)] = version
    if not any(freeze_name(entry) == ARTIFACT_PACKAGE for entry in entries):
        raise RuntimeError(
            f"{ARTIFACT_PACKAGE} non risulta installato nell'ambiente che ha "
            "eseguito la suite"
        )
    packages.pop(ARTIFACT_PACKAGE, None)
    validate_installed_pins(packages)
    packages["python"] = marker_line(output, PYTHON_MARKER)
    return packages


def installed_origin(output: str) -> dict[str, str]:
    """Da dove il package e stato importato, e il digest del nativo caricato.

    La riga la produce `sdk_wheel_probe`, che ha gia rifiutato qualunque
    origine fuori da `site-packages` prima che pytest partisse. Qui la si
    rilegge — e si ricontrolla — perche il verdetto deve poter essere
    verificato da chi non ha visto girare il probe.

    # Raises

    `RuntimeError` se la riga manca, non e interpretabile, o nomina un
    percorso che non sta in `site-packages`.
    """

    marker = marker_line(output, ORIGIN_MARKER)
    fields = marker.split()
    if len(fields) != 3:
        raise RuntimeError(f"riga di origine non interpretabile: '{marker}'")
    package, native, digest = fields
    for path in (package, native):
        if SITE_PACKAGES not in f"{path}/":
            raise RuntimeError(
                f"la suite ha importato da {path}, fuori da site-packages: "
                "il wheel costruito non e cio che ha risposto ai test"
            )
        if path.startswith(f"{REPOSITORY_MOUNT}/"):
            raise RuntimeError(
                f"la suite ha importato da {path}, dentro il repository "
                "montato: e la copia sorgente, non il wheel"
            )
    return {
        "package_path": package,
        "native_path": native,
        "native_sha256": digest,
    }


def verdict(
    *,
    scope: str,
    commit: str,
    dirty: list[str],
    artifact: dict[str, str],
    images: dict[str, object],
    versions: dict[str, str],
    summary: str,
) -> dict[str, object]:
    """Il verdetto, che deve identificare cio che ha girato.

    Il nome del wheel non lo identifica: e lo stesso per ogni build dello
    stesso pacchetto, quindi un verdetto che si fermasse li direbbe solo che
    *un* wheel e stato costruito. Servono il commit, il digest dell'artefatto
    — quello del wheel e quello del modulo che l'interprete ha caricato da
    `site-packages` — e le versioni con cui la suite ha girato.

    `authoritative` e falso quando l'albero non coincideva con HEAD: il
    commit resta scritto, ma non descrive cio che e stato costruito, e le
    righe di `git status` che compaiono accanto dicono di quanto. Un verdetto
    non autorevole non e un verdetto piu debole da leggere con indulgenza: e
    un verdetto che non risponde alla domanda "quale codice ha girato".
    """

    return {
        "schema_version": 3,
        "gate": "python-sdk-suite",
        "status": "passed",
        "scope": scope,
        "authoritative": not dirty,
        "worktree_dirty": bool(dirty),
        "worktree_changes": sorted(dirty),
        "worktree_unchanged_during_run": True,
        "git_commit": commit,
        "artifact": {
            "wheel": artifact["wheel"],
            "wheel_sha256": artifact["wheel_sha256"],
            "native_sha256": artifact["native_sha256"],
            "package_path": artifact["package_path"],
            "native_path": artifact["native_path"],
        },
        "images": images,
        "versions": {
            "maturin": artifact["maturin"],
            "pandas": versions["pandas"],
            "pyarrow": versions["pyarrow"],
            "pytest": versions["pytest"],
            "pytest_asyncio": versions["pytest-asyncio"],
            "python": versions["python"],
            "rustc": artifact["rustc"],
        },
        "pytest": summary.strip(),
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--offline",
        action="store_true",
        help="salta le reti dei riferimenti: girano solo i test senza server",
    )
    scope.add_argument(
        "--benchmark-only",
        action="store_true",
        help="dei soli bench di parita, sui riferimenti live",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help=(
            "esegue con l'albero diverso da HEAD: il verdetto si dichiara "
            "non autorevole e riporta le righe di git status"
        ),
    )
    arguments = parser.parse_args()
    if arguments.offline:
        selected = "offline"
    elif arguments.benchmark_only:
        selected = "benchmark"
    else:
        selected = "live"

    try:
        validate_maturin_pin(
            PYPROJECT.read_text(encoding="utf-8"),
            BUILD_REQUIREMENTS.read_text(encoding="utf-8"),
        )
        if selected == "benchmark":
            assert_benchmark_prerequisites()
        dirty = porcelain_entries()
        if not arguments.allow_dirty:
            assert_clean_worktree(dirty)
        elif dirty:
            print(
                f"sdk gate: albero non pulito ({len(dirty)} voci), il verdetto "
                "sara authoritative=false",
                file=sys.stderr,
            )
        commit = git(["rev-parse", "HEAD"]).strip()
        before = worktree_state()
        with tempfile.TemporaryDirectory(prefix="plenora-sdk-wheel-") as staging:
            wheels = Path(staging)
            artifact = build_wheel(wheels)
            assert_worktree_unchanged(before, "la build del wheel")
            output = run(
                pytest_command(
                    scope=selected, wheels=wheels, wheel=artifact["wheel"]
                ),
                capture=True,
            )
            assert_worktree_unchanged(before, "l'esecuzione della suite")
        artifact.update(installed_origin(output))
        versions = installed_versions(output)
        images = {
            "build": image_identity(RUST_IMAGE),
            "test": image_identity(PYTHON_IMAGE),
        }
    except RuntimeError as error:
        print(f"sdk gate: {error}", file=sys.stderr)
        return 1

    print(output)
    summary = next(
        (line for line in reversed(output.splitlines()) if " passed" in line),
        "",
    )
    print(
        json.dumps(
            verdict(
                scope=selected,
                commit=commit,
                dirty=dirty,
                artifact=artifact,
                images=images,
                versions=versions,
                summary=summary,
            ),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
