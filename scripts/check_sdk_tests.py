#!/usr/bin/env python3
"""Suite del SDK Python, sempre sul wheel appena costruito.

Il modulo nativo `plenora_database/_native.abi3.so` e gitignorato e non viene
rigenerato da `pytest`. Eseguire direttamente la suite puo quindi usare un
binario non corrispondente ai sorgenti e produrre un verdetto inattendibile.

Questo runner toglie il caso dalla mano di chi esegue:

1. rifiuta di partire se l'albero di lavoro non e pulito — il verdetto
   nomina un commit, e cio che non e in quel commit non e descritto da
   quel nome;
2. costruisce **entrambi** gli artefatti nello stesso container e con la
   stessa toolchain — il wheel con `maturin --locked` e il CLI con
   `cargo build --release --locked` — e li esporta in una directory
   temporanea **fuori** dal repository;
3. installa il wheel nel container di test con `pip install --no-deps` e
   monta il CLI in sola lettura, senza scrivere nulla nel source tree;
4. esegue `pytest` fuori dal source tree, senza `PYTHONPATH` verso il
   package locale, e verifica prima di partire che `plenora_database` e il
   suo modulo nativo arrivino da `site-packages`;
5. confronta la corsa con il contratto dello scope — quanti passati, quanti
   saltati **e per quale motivo**, quanti deselezionati — perche un test che
   non ha risposto, comunque sia sparito, in un verdetto conta come un
   fallito;
6. verifica che ne' la build ne' i test abbiano cambiato l'albero, e
   registra nel verdetto di cosa sono fatti gli artefatti che hanno girato.

Il CLI fa parte della stessa build perche il benchmark di parita lo esegue in
subprocess e ne confronta i tempi con il SDK. I due lati del rapporto devono
provenire dagli stessi sorgenti e dalla stessa toolchain.

Uso:

    python scripts/check_sdk_tests.py                  # tutti i quattro provider
    python scripts/check_sdk_tests.py --offline        # solo test senza server
    python scripts/check_sdk_tests.py --benchmark-only # solo i bench di parita
    python scripts/check_sdk_tests.py --allow-dirty    # verdetto non autorevole

**Tracciato, non riproducibile.** `rust:1.98` e `python:3.13-slim` sono tag
mutabili — la stessa riga puo risolvere due immagini diverse a distanza di un
giorno — e l'`apt-get install` della build prende cio che il mirror pubblica
oggi. Quel che il runner puo fare, e fa, e dire con cosa ha girato: id e
digest delle due immagini, versione di rustc e di Python effettive, pin di
pip confrontati con il `pip freeze` di chi li ha installati. Chiamarlo
"riproducibile" prometterebbe che una seconda corsa ricostruisce lo stesso
ambiente, che nessuna di queste misure garantisce.

Reti, volumi e credenziali non sono scritti a mano: si chiedono a Docker con
gli stessi helper dei gate di riferimento. Una password ricopiata qui
sarebbe una seconda fonte per un dato che ne ha una sola, il compose.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
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
CARGO_MANIFEST = CRATE / "Cargo.toml"
CARGO_LOCK = ROOT / "Cargo.lock"
CHANGELOG = CRATE / "CHANGELOG.md"
# Il nome con cui il crate del binding compare nel lock del workspace.
CARGO_PACKAGE = "plenora-database-py"
NATIVE = CRATE / "python" / "plenora_database" / "_native.abi3.so"
PROBE = Path(__file__).resolve().parent / "sdk_wheel_probe.py"
BUILD_REQUIREMENTS = ROOT / "requirements-sdk-build.txt"
TEST_REQUIREMENTS = ROOT / "requirements-sdk-tests.txt"
RUST_IMAGE = "rust:1.98"
PYTHON_IMAGE = "python:3.13-slim"

# Il repository entra nel container dei test in sola lettura, e la suite gira
# altrove: `python/tests` e un package, quindi pytest inserirebbe in
# `sys.path` la sua directory padre — cioe `python/`, dove vive la copia
# sorgente di `plenora_database`, che vincerebbe sul wheel installato.
REPOSITORY_MOUNT = "/repo"
# Wheel e CLI escono dalla stessa build e vivono nella stessa directory
# temporanea, montata in sola lettura dove serve.
ARTIFACT_MOUNT = "/artifacts"
SUITE_DIRECTORY = "/suite"

# Il percorso che l'installazione di un wheel produce nell'immagine Python
# ufficiale. La verifica che conta la fa il probe contro `sysconfig` dentro
# l'interprete; questo confronto e la sua controparte sul verdetto, e vive
# accanto a `PYTHON_IMAGE` perche e da quella scelta che dipende.
SITE_PACKAGES = "/site-packages/"

# Il bench di parita esegue il CLI in subprocess e confronta i suoi tempi con
# quelli del SDK. Il binario si costruisce qui, nello stesso container e con
# la stessa toolchain del wheel: preso da `target/release` del repository era
# un eseguibile di provenienza ignota — sopravvive alle sessioni e nessuno ne
# sa il commit — e il rapporto fra i due tempi metteva insieme due codici
# diversi.
#
# Le feature sono esplicite in entrambe le direzioni: oltre al bench Postgres,
# la suite attraversa la superficie CLI comune sui quattro provider. Il
# verdetto dichiara quindi tutti gli adapter presenti nell'artefatto invece di
# ereditare un default che puo cambiare senza che nessuno se ne accorga.
CLI_PACKAGE = "plenora-database-cli"
CLI_BINARY_NAME = "plenora-database"
CLI_FEATURES = ("postgres", "mysql", "sqlserver")
CLI_BUILD_COMMAND = " ".join(
    [
        "cargo", "build", "--release", "--locked",
        "-p", CLI_PACKAGE,
        "--no-default-features", "--features", ",".join(CLI_FEATURES),
    ]
)

POSTGRES_CONTAINER = "dataflow-postgres"
MYSQL_CONTAINER = "dataflow-mysql"
MARIADB_CONTAINER = "dataflow-mariadb"
SQLSERVER_CONTAINER = "dataflow-sqlserver"
# Il container si ispeziona con il nome esplicito sopra; i client invece
# devono usare il DNS Compose coperto dal certificato della fixture.
SQLSERVER_TLS_HOST = "sqlserver"
POSTGRES_PORT = 5432


@dataclass(frozen=True)
class ScopeContract:
    """Quanti test deve aver eseguito uno scope, e quali puo aver saltato.

    Un conteggio di soli `passed` non descrive una corsa: lo stesso numero puo
    uscire da una suite che salta il resto e da una che lo deseleziona, e le
    due dicono cose diverse. `skips` va oltre il totale — associa a
    ogni motivo quante volte deve comparire — perche un totale coincidente e
    esattamente cio che rende invisibile una sostituzione: uno skip nuovo che
    ne rimpiazza uno atteso lascia il numero fermo.
    """

    passed: int
    deselected: int
    skips: dict[str, int]

    @property
    def skipped(self) -> int:
        return sum(self.skips.values())


# I motivi con cui la suite si salta da sola quando i riferimenti non ci
# sono. Sono il funzionamento di `offline`, non un difetto: i test live
# leggono le variabili d'ambiente dei riferimenti e senza quelle non hanno
# nulla da interrogare.
POSTGRES_SKIP = "live test: manca env PLENORA_TEST_POSTGRES_DSN"
MYSQL_SKIP = (
    "live test MySQL: mancano env PLENORA_TEST_MYSQL_HOST "
    "e/o PLENORA_TEST_MYSQL_PASSWORD"
)
MARIADB_SKIP = (
    "live test MariaDB: mancano env PLENORA_TEST_MARIADB_HOST "
    "e/o PLENORA_TEST_MARIADB_PASSWORD"
)
SQLSERVER_SKIP = (
    "live test SQL Server: mancano env PLENORA_TEST_SQLSERVER_HOST "
    "e/o PLENORA_TEST_SQLSERVER_PASSWORD"
)
DB2_SKIP = (
    "live test Db2: mancano env PLENORA_TEST_DB2_HOST "
    "e/o PLENORA_TEST_DB2_PASSWORD"
)
BENCH_SKIP = (
    "bench opt-in: setta PLENORA_BENCH_PARITY=1 per lanciarlo "
    "(atteso ~10s di walltime per la sessione)"
)

# Il contratto di ogni scope, in un posto solo. I numeri si aggiornano quando
# la suite cambia — ed e il punto: un test aggiunto e visibile qui, mentre
# "passed" da solo cresce senza dire di cosa.
SCOPE_CONTRACTS = {
    # Il gate SDK multipiattaforma qualifica il wheel standard, che non
    # incorpora ODBC. I test Db2 appartengono al gate live DB2 dedicato e qui
    # devono restare skip espliciti, non essere assorbiti dal totale.
    "live": ScopeContract(passed=301, deselected=0, skips={DB2_SKIP: 6}),
    "offline": ScopeContract(
        passed=79,
        deselected=0,
        skips={
            POSTGRES_SKIP: 175,
            MYSQL_SKIP: 33,
            MARIADB_SKIP: 6,
            SQLSERVER_SKIP: 6,
            DB2_SKIP: 6,
            BENCH_SKIP: 2,
        },
    ),
    "benchmark": ScopeContract(passed=2, deselected=305, skips={}),
}

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
# come `plenora_database @ file:///artifacts/...`.
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


# --------------------------------------------------------------------------
# La versione della release, dichiarata in quattro posti
# --------------------------------------------------------------------------


def toml_version(document: str, table: str) -> str:
    """Il campo `version` della tabella `table` di un documento TOML.

    # Raises

    `RuntimeError` se manca o non e una stringa. Il caso non-stringa non e
    teorico: `version.workspace = true` produce una tabella, e il crate del
    binding ha per contratto una versione **propria** — il ciclo di release
    del SDK non e quello del workspace Rust.
    """

    version = tomllib.loads(document).get(table, {}).get("version")
    if not isinstance(version, str):
        raise RuntimeError(
            f"la tabella [{table}] non dichiara una versione testuale: {version!r}"
        )
    return version


def locked_version(document: str) -> str:
    """La versione con cui `Cargo.lock` registra il crate del binding.

    # Raises

    `RuntimeError` se il package non compare: senza la sua riga il lock non
    descrive questo workspace, e ogni build `--locked` fallirebbe piu tardi
    con un errore che parla di risoluzione invece che di versione.
    """

    for package in tomllib.loads(document).get("package", []):
        if package.get("name") == CARGO_PACKAGE:
            version = package.get("version")
            if not isinstance(version, str):
                raise RuntimeError(f"{CARGO_PACKAGE} nel lock senza versione")
            return version
    raise RuntimeError(f"{CARGO_PACKAGE} non compare in {CARGO_LOCK.name}")


def changelog_version(document: str) -> str:
    """La versione della prima release del CHANGELOG.

    Una sezione `[Unreleased]` non e una release e viene saltata: e il modo
    normale di lavorare fra un rilascio e l'altro, e farla fallire
    costringerebbe a rilasciare per poter eseguire il gate.

    # Raises

    `RuntimeError` se nessuna intestazione dichiara una versione.
    """

    for line in document.splitlines():
        match = re.match(r"^## \[(\d[^\]]*)\]", line)
        if match:
            return match.group(1)
    raise RuntimeError("il CHANGELOG non dichiara nessuna release")


def validate_declared_versions(
    *, pyproject: str, cargo_toml: str, cargo_lock: str, changelog: str
) -> str:
    """La versione della release, che le quattro fonti devono dire uguale.

    Non e una ripetizione ridondante: ognuna decide una cosa diversa.
    `pyproject.toml` compone il nome del wheel, `Cargo.toml` del crate decide
    cosa risponde `p.version()`, `Cargo.lock` e cio che le build `--locked`
    pretendono di ritrovare, e il CHANGELOG e quello che legge chi aggiorna.
    Due che divergono producono un artefatto che mente su se stesso, per
    esempio un nome wheel diverso dalla versione restituita dal modulo.

    # Returns

    La versione, una volta che tutte e quattro concordano.

    # Raises

    `RuntimeError` elencando fonte per fonte cosa dichiara: sapere *che*
    divergono senza sapere quale sia indietro non basta a correggerle.
    """

    versions = {
        PYPROJECT.name: toml_version(pyproject, "project"),
        f"{CRATE.name}/{CARGO_MANIFEST.name}": toml_version(cargo_toml, "package"),
        CARGO_LOCK.name: locked_version(cargo_lock),
        CHANGELOG.name: changelog_version(changelog),
    }
    distinct = set(versions.values())
    if len(distinct) != 1:
        raise RuntimeError(
            f"le fonti della versione divergono: {dict(sorted(versions.items()))}"
        )
    return distinct.pop()


def declared_version() -> str:
    """La versione dichiarata, letta dalle quattro fonti sul disco."""

    return validate_declared_versions(
        pyproject=PYPROJECT.read_text(encoding="utf-8"),
        cargo_toml=CARGO_MANIFEST.read_text(encoding="utf-8"),
        cargo_lock=CARGO_LOCK.read_text(encoding="utf-8"),
        changelog=CHANGELOG.read_text(encoding="utf-8"),
    )


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

    Un tag e mutabile: `rust:1.98` oggi e `rust:1.98` fra un mese possono
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


def build_artifacts(destination: Path) -> dict[str, object]:
    """Costruisce wheel e CLI e li lascia in `destination`.

    I due artefatti escono dallo stesso container, dalla stessa toolchain e
    dallo stesso `Cargo.lock`: il bench di parita confronta i loro tempi, e
    un rapporto fra un wheel di oggi e un eseguibile di provenienza ignota
    non misura la differenza fra due modi di chiamare la libreria — misura la
    differenza fra due codici.

    Il source tree non riceve niente: il modulo nativo che vi si trovasse e
    per costruzione quello di una corsa precedente, e va tolto di mezzo —
    e proprio la copia che un `pytest` a mano finirebbe per caricare.

    # Returns

    Nome e SHA-256 dei due artefatti, feature e comando con cui il CLI e
    stato costruito, versioni di maturin e di rustc. Il solo nome non
    identifica un artefatto: due wheel con lo stesso nome escono da qualunque
    coppia di build, e il verdetto serve proprio a dire *quale* ha girato.

    # Raises

    `RuntimeError` se la build fallisce, non produce esattamente un wheel, o
    non lascia il binario del CLI.
    """

    if NATIVE.exists():
        NATIVE.unlink()

    script = (
        "set -e; "
        f"{CLI_BUILD_COMMAND}; "
        f"cp /workspace/target-docker/release/{CLI_BINARY_NAME} "
        f"{ARTIFACT_MOUNT}/; "
        "apt-get update -qq >/dev/null 2>&1; "
        "apt-get install -y -qq python3 python3-venv >/dev/null 2>&1; "
        "python3 -m venv /tmp/v; "
        f"/tmp/v/bin/pip install -q -r /workspace/{BUILD_REQUIREMENTS.name}; "
        "cd /workspace/crates/plenora-database-py; "
        f"/tmp/v/bin/maturin build --release --locked --out {ARTIFACT_MOUNT}; "
        f'echo "{BUILD_MARKER}'
        "$(/tmp/v/bin/maturin --version | cut -d' ' -f2) "
        "$(rustc --version | cut -d' ' -f2)\""
    )
    output = run(
        [
            "docker", "run", "--rm",
            "-v", f"{ROOT}:/workspace",
            "-v", f"{destination}:{ARTIFACT_MOUNT}",
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
    cli = destination / CLI_BINARY_NAME
    if not cli.exists():
        raise RuntimeError(f"la build non ha esportato il CLI {CLI_BINARY_NAME}")
    marker = marker_line(output, BUILD_MARKER)
    fields = marker.split()
    if len(fields) != 2:
        raise RuntimeError(f"riga di build non interpretabile: '{marker}'")
    maturin, rustc = fields
    return {
        "wheel": wheels[0].name,
        "wheel_sha256": hashlib.sha256(wheels[0].read_bytes()).hexdigest(),
        "cli": {
            "binary": CLI_BINARY_NAME,
            "sha256": hashlib.sha256(cli.read_bytes()).hexdigest(),
            "features": list(CLI_FEATURES),
            "build_command": CLI_BUILD_COMMAND,
        },
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


def live_environment(*, cli: str) -> list[str]:
    """Le variabili dei riferimenti, lette dai container in esecuzione.

    `cli` e il percorso del binario che il bench di parita esegue: lo passa il
    runner, che e l'unico a sapere dove ha montato cosa. Scritto dentro il
    test era il punto di mount di allora, e al primo cambio il bench non lo ha
    piu trovato e si e saltato da solo.
    """

    postgres_user = container_variable(POSTGRES_CONTAINER, "POSTGRES_USER")
    postgres_password = container_variable(POSTGRES_CONTAINER, "POSTGRES_PASSWORD")
    postgres_database = container_variable(POSTGRES_CONTAINER, "POSTGRES_DB")
    sqlserver_database = container_variable(SQLSERVER_CONTAINER, "PLENORA_TEST_DATABASE")
    sqlserver_user = container_variable(SQLSERVER_CONTAINER, "PLENORA_TEST_USER")
    sqlserver_password = container_variable(SQLSERVER_CONTAINER, "MSSQL_SA_PASSWORD")
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
        "-e", f"PLENORA_TEST_MARIADB_HOST={MARIADB_CONTAINER}",
        "-e",
        f"PLENORA_TEST_MARIADB_DATABASE="
        f"{container_variable(MARIADB_CONTAINER, 'MARIADB_DATABASE')}",
        "-e",
        f"PLENORA_TEST_MARIADB_USER="
        f"{container_variable(MARIADB_CONTAINER, 'MARIADB_USER')}",
        "-e",
        f"PLENORA_TEST_MARIADB_PASSWORD="
        f"{container_variable(MARIADB_CONTAINER, 'MARIADB_PASSWORD')}",
        "-e", "PLENORA_TEST_MARIADB_CA=/mariadb-tls/ca.pem",
        "-e", f"PLENORA_TEST_SQLSERVER_HOST={SQLSERVER_TLS_HOST}",
        "-e", f"PLENORA_TEST_SQLSERVER_DATABASE={sqlserver_database}",
        "-e", f"PLENORA_TEST_SQLSERVER_USER={sqlserver_user}",
        "-e",
        f"PLENORA_TEST_SQLSERVER_PASSWORD={sqlserver_password}",
        "-e", "PLENORA_TEST_SQLSERVER_CA=/sqlserver-tls/ca.pem",
        "-e", "PLENORA_BENCH_PARITY=1",
        "-e", f"PLENORA_CLI_BIN={cli}",
    ]


def assert_artifacts_outside_repository(staging: Path) -> None:
    """La directory degli artefatti non puo stare dentro il repository.

    E il lato host della stessa regola di [`cli_binary_path`]: `TMPDIR` e una
    variabile d'ambiente, e se puntasse dentro l'albero il gate scriverebbe
    wheel e CLI nel repository che sta verificando — cioe esattamente la
    condizione da cui il source tree e stato liberato.

    # Raises

    `RuntimeError` nominando la directory.
    """

    if staging.resolve().is_relative_to(ROOT):
        raise RuntimeError(
            f"la directory degli artefatti {staging} sta dentro il "
            "repository: wheel e CLI devono nascere fuori dall'albero che il "
            "gate verifica (controlla TMPDIR)"
        )


def assert_cli_outside_repository(path: str) -> None:
    """Il binario del bench non puo venire dal repository montato.

    Il bench misura un eseguibile: se quell'eseguibile viene dall'albero
    invece che dalla build, il rapporto confronta il wheel di adesso con un
    binario di provenienza ignota — `target/release/` sopravvive alle
    sessioni, e nessuno sa dire di quale commit sia. La regola sta in una
    funzione perche il percorso e una stringa, e una stringa si riscrive
    senza accorgersene: qui il ritorno all'albero e un errore, non una
    riga da rivedere.

    # Raises

    `RuntimeError` nominando il percorso rifiutato.
    """

    if path == REPOSITORY_MOUNT or path.startswith(f"{REPOSITORY_MOUNT}/"):
        raise RuntimeError(
            f"il CLI del bench verrebbe da {path}, dentro il repository "
            "montato: e un artefatto che il gate non ha costruito"
        )
    if not path.startswith(f"{ARTIFACT_MOUNT}/"):
        raise RuntimeError(
            f"il CLI del bench verrebbe da {path}, fuori dalla directory "
            f"degli artefatti {ARTIFACT_MOUNT}: solo cio che questa corsa ha "
            "costruito ha un digest nel verdetto"
        )


def cli_binary_path() -> str:
    """Il percorso del CLI **dentro il container dei test**, verificato."""

    path = f"{ARTIFACT_MOUNT}/{CLI_BINARY_NAME}"
    assert_cli_outside_repository(path)
    return path


def pytest_command(*, scope: str, artifacts: Path, wheel: str) -> list[str]:
    """Il `docker run` che esegue la suite nello scope richiesto.

    Gli artefatti arrivano dalla directory temporanea dove la build li ha
    lasciati, montata in sola lettura: il wheel viene installato con
    `--no-deps` — le dipendenze sono gia quelle dei pin, e lasciare che il
    wheel ne risolva altre significherebbe girare su un ambiente diverso da
    quello dichiarato — e il CLI resta li, dove il bench di parita lo esegue
    senza copiarlo da nessuna parte.

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
        command += compose_network_arguments(
            POSTGRES_CONTAINER,
            MYSQL_CONTAINER,
            MARIADB_CONTAINER,
            SQLSERVER_CONTAINER,
        )
        mysql_tls = compose_volume(MYSQL_CONTAINER, "/etc/mysql/tls")
        mariadb_tls = compose_volume(MARIADB_CONTAINER, "/etc/mysql/tls")
        sqlserver_tls = compose_volume(SQLSERVER_CONTAINER, "/var/opt/mssql/tls")
        command += [
            "-v", f"{mysql_tls}:/mysql-tls:ro",
            "-v", f"{mariadb_tls}:/mariadb-tls:ro",
            "-v", f"{sqlserver_tls}:/sqlserver-tls:ro",
        ]
        environment = live_environment(cli=cli_binary_path())
    if scope == "benchmark":
        selection += ["-k", "benchmark"]

    suite = f"{REPOSITORY_MOUNT}/crates/plenora-database-py/python/tests"
    script = (
        "set -e; "
        f"pip install -q -r {REPOSITORY_MOUNT}/{TEST_REQUIREMENTS.name}; "
        f"pip install -q --no-deps {ARTIFACT_MOUNT}/{wheel}; "
        f"cp -r {suite} {SUITE_DIRECTORY}/tests; "
        f"rm -rf {SUITE_DIRECTORY}/tests/__pycache__; "
        f'echo "{PYTHON_MARKER}$(python -V 2>&1 | cut -d\' \' -f2)"; '
        f'echo "{PACKAGES_MARKER}$(pip freeze | tr \'\\n\' \' \')"; '
        f"python {REPOSITORY_MOUNT}/scripts/{PROBE.name}; "
        f"python -m pytest {' '.join(selection)} -q -rs"
    )
    command += [
        "-v", f"{ROOT}:{REPOSITORY_MOUNT}:ro",
        "-v", f"{artifacts}:{ARTIFACT_MOUNT}:ro",
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


def pytest_summary(output: str) -> str:
    """La riga di riepilogo di pytest.

    # Raises

    `RuntimeError` se non c'e: il verdetto la riporta, e senza riporterebbe
    una stringa vuota — cioe direbbe "passed" senza dire quanti test.
    """

    for line in reversed(output.splitlines()):
        if " passed" in line:
            return line.strip()
    raise RuntimeError("output senza la riga di riepilogo di pytest")


def pytest_counts(summary: str) -> dict[str, int]:
    """I conteggi della riga di riepilogo, per esito.

    `{"passed": 24, "skipped": 195}` da `24 passed, 195 skipped in 0.83s`. La
    durata non viene letta come conteggio perche non ha uno spazio prima
    dell'unita.
    """

    return {
        outcome: int(count)
        for count, outcome in re.findall(r"\b(\d+) ([a-z]+)\b", summary)
    }


def skip_reasons(output: str) -> dict[str, int]:
    """Quante volte compare ogni motivo di skip, secondo `-rs`.

    Le righe hanno la forma `SKIPPED [N] file:riga: motivo`, e `N` non e
    sempre 1: pytest raggruppa i test parametrizzati che saltano nello stesso
    punto. Contare le righe invece di sommare gli `N` darebbe un totale che
    non torna con il riepilogo.
    """

    reasons: dict[str, int] = {}
    for line in output.splitlines():
        match = re.match(r"^SKIPPED \[(\d+)\] [^:]+:\d+: (.+)$", line)
        if match:
            reason = match.group(2).strip()
            reasons[reason] = reasons.get(reason, 0) + int(match.group(1))
    return reasons


def assert_scope_contract(*, scope: str, output: str, summary: str) -> dict[str, int]:
    """La corsa deve corrispondere al contratto dello scope, in ogni voce.

    Un test saltato non e un test passato: di cio che doveva verificare non
    si sa niente, e salta per motivi che somigliano a un errore di
    configurazione — un binario spostato, una variabile che nessuno passa
    piu — cioe resta verde proprio quando il gate ha smesso di misurare. Lo
    stesso vale per una deselezione: `-k` che non seleziona piu niente
    produce "0 passed" e nessun errore.

    Il contratto va oltre "nessuno skip". Fissa i tre conteggi, perche gli
    stessi 24 `passed` escono da una suite che ne salta 195 e da una che ne
    deseleziona 195; e per gli skip previsti fissa **quali** e quanti,
    perche un totale coincidente e proprio cio che rende invisibile una
    sostituzione — uno skip nuovo al posto di uno atteso lascia il numero
    fermo.

    # Returns

    I conteggi osservati, che finiscono nel verdetto.

    # Raises

    `RuntimeError` con tutte le divergenze trovate, il riepilogo di pytest e
    i motivi degli skip osservati: una sola voce alla volta costringerebbe a
    rieseguire la suite per vedere la successiva.
    """

    contract = SCOPE_CONTRACTS[scope]
    counts = pytest_counts(summary)
    observed = {
        outcome: counts.get(outcome, 0)
        for outcome in ("passed", "skipped", "deselected")
    }
    expected = {
        "passed": contract.passed,
        "skipped": contract.skipped,
        "deselected": contract.deselected,
    }
    reasons = skip_reasons(output)

    problems = [
        f"{outcome}: {observed[outcome]}, attesi {expected[outcome]}"
        for outcome in ("passed", "skipped", "deselected")
        if observed[outcome] != expected[outcome]
    ]
    for reason, count in sorted(contract.skips.items()):
        if reasons.get(reason, 0) != count:
            problems.append(
                f"skip previsto '{reason}': {reasons.get(reason, 0)} volte, "
                f"attese {count}"
            )
    problems += [
        f"skip inatteso '{reason}': {count} volte"
        for reason, count in sorted(reasons.items())
        if reason not in contract.skips
    ]
    if problems:
        raise RuntimeError(
            f"scope {scope} fuori contratto: {problems}; riepilogo di pytest: "
            f"'{summary}'; motivi degli skip osservati: {reasons}"
        )
    return observed


def verdict(
    *,
    scope: str,
    commit: str,
    dirty: list[str],
    artifact: dict[str, object],
    images: dict[str, object],
    versions: dict[str, str],
    counts: dict[str, int],
    summary: str,
) -> dict[str, object]:
    """Il verdetto, che deve identificare cio che ha girato.

    Il nome del wheel non lo identifica: e lo stesso per ogni build dello
    stesso pacchetto, quindi un verdetto che si fermasse li direbbe solo che
    *un* wheel e stato costruito. Servono il commit, il digest dell'artefatto
    — quello del wheel e quello del modulo che l'interprete ha caricato da
    `site-packages` — e le versioni con cui la suite ha girato.

    Gli artefatti sono due, perche il bench di parita ne confronta due: il
    CLI porta digest, feature e il comando con cui e stato costruito. Le
    feature non sono un dettaglio di build — decidono quali provider sono
    dentro il binario — e il comando dice che sono state chieste, non
    ereditate da un default.

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
            "cli": artifact["cli"],
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
        # I conteggi accanto alla riga di riepilogo: la riga e per chi legge,
        # questi sono cio che il contratto ha verificato, e si confrontano
        # senza doverla interpretare.
        "counts": counts,
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }


#: Gli scope che hanno bisogno dei riferimenti accesi, nell'ordine in cui la
#: campagna li misura. `live` per prima: e quella che dice se il SDK funziona,
#: e se non funziona i tempi del bench non interessano piu.
LIVE_SCOPES = ("live", "benchmark")


def preconditions() -> None:
    """Cio che deve valere prima di costruire qualunque cosa.

    Le versioni dichiarate devono coincidere fra `pyproject.toml`,
    `Cargo.toml`, `Cargo.lock` e il changelog, e il pin di maturin deve stare
    dentro i limiti che il crate dichiara. Sono controlli su file, non su
    server: stanno prima di Docker perche scoprirli dopo costa la build di due
    artefatti e l'accensione dei riferimenti.

    # Raises

    `RuntimeError` per una divergenza.
    """

    declared_version()
    validate_maturin_pin(
        PYPROJECT.read_text(encoding="utf-8"),
        BUILD_REQUIREMENTS.read_text(encoding="utf-8"),
    )


def preflight() -> str:
    """Le precondizioni piu l'albero pulito, e il commit da cui si parte.

    E' la forma che [`scripts.fixture_campaign.campaign`] si aspetta: viene
    chiamata due volte, una prima di accendere i riferimenti e una a misura
    finita, e le due risposte devono coincidere. Il verdetto nomina un commit,
    e con l'albero sporco quel commit non descrive il codice che ha prodotto i
    numeri — `main` ammette il caso con `--allow-dirty`, marcando il verdetto
    non autorevole, ma una campagna non ha un operatore che legga quella
    marcatura.

    # Raises

    `RuntimeError` se una precondizione non regge o l'albero ha modifiche non
    committate.
    """

    preconditions()
    assert_clean_worktree(porcelain_entries())
    return git(["rev-parse", "HEAD"]).strip()


def measure_scopes(
    scopes: Sequence[str], *, dirty: list[str]
) -> dict[str, dict[str, object]]:
    """Costruisce gli artefatti **una volta** e misura gli scope richiesti.

    Ricostruirli per ogni scope non renderebbe la misura piu solida: la
    renderebbe meno confrontabile, perche `live` e `benchmark` parlerebbero di
    due wheel diversi mentre il bench di parita esiste proprio per confrontare
    due artefatti fra loro. Sono gli stessi, e il verdetto di ciascuno scope
    porta gli stessi digest.

    L'albero si riverifica dopo **ogni** scope, non solo alla fine: sapere
    quale corsa lo ha toccato e cio che rende l'informazione utile.

    # Raises

    `RuntimeError` da una qualunque delle fasi che gia la sollevano — build,
    esecuzione, contratto dello scope.
    """

    commit = git(["rev-parse", "HEAD"]).strip()
    before = worktree_state()
    documents: dict[str, dict[str, object]] = {}
    with tempfile.TemporaryDirectory(prefix="plenora-sdk-artifacts-") as staging:
        artifacts = Path(staging)
        assert_artifacts_outside_repository(artifacts)
        artifact = build_artifacts(artifacts)
        assert_worktree_unchanged(before, "la build degli artefatti")
        # L'identita delle immagini si chiede **dopo** la prima corsa, ed e la
        # condizione che [`image_identity`] dichiara: `python:3.13-slim`
        # arriva sul demone quando parte il container dei test, e su un runner
        # pulito chiederla prima significa chiederla di un'immagine che non
        # c'e ancora — `No such image`, e la campagna muore per una ragione
        # che non riguarda ne la build ne la suite. Si calcola una volta e si
        # riusa: gli scope girano sugli stessi due artefatti.
        images: dict[str, object] | None = None
        for scope in scopes:
            output = run(
                pytest_command(
                    scope=scope, artifacts=artifacts, wheel=artifact["wheel"]
                ),
                capture=True,
            )
            assert_worktree_unchanged(before, f"l'esecuzione della suite ({scope})")
            print(output)
            if images is None:
                images = {
                    "build": image_identity(RUST_IMAGE),
                    "test": image_identity(PYTHON_IMAGE),
                }
            summary = pytest_summary(output)
            counts = assert_scope_contract(scope=scope, output=output, summary=summary)
            # Copia, non aggiornamento in luogo: l'origine del package la
            # dichiara l'interprete che ha eseguito **quello** scope, e
            # sovrascriverla lascerebbe due verdetti che citano la stessa.
            measured = dict(artifact)
            measured.update(installed_origin(output))
            documents[scope] = verdict(
                scope=scope,
                commit=commit,
                dirty=dirty,
                artifact=measured,
                images=images,
                versions=installed_versions(output),
                counts=counts,
                summary=summary,
            )
    return documents


def measure_live_scopes() -> dict[str, object]:
    """I due scope che hanno bisogno dei riferimenti, per la campagna.

    Zero argomenti perche e cio che `campaign(measure=...)` chiama, e nessun
    `dirty`: il preflight della campagna rifiuta un albero sporco, quindi qui
    non esiste il caso.
    """

    return {
        "schema_version": 1,
        "gate": "python-sdk-suite",
        "scopes": measure_scopes(LIVE_SCOPES, dirty=[]),
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
        preconditions()
        dirty = porcelain_entries()
        if not arguments.allow_dirty:
            assert_clean_worktree(dirty)
        elif dirty:
            print(
                f"sdk gate: albero non pulito ({len(dirty)} voci), il verdetto "
                "sara authoritative=false",
                file=sys.stderr,
            )
        documents = measure_scopes([selected], dirty=dirty)
    except RuntimeError as error:
        print(f"sdk gate: {error}", file=sys.stderr)
        return 1

    print(json.dumps(documents[selected], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
