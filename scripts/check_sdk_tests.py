#!/usr/bin/env python3
"""Suite del SDK Python, sempre su un'estensione appena costruita.

Il modulo nativo `plenora_database/_native.abi3.so` e gitignorato: non e un
artefatto del repository, e nessuno lo rigenera automaticamente. Chi cambia
il Rust e poi lancia `pytest` a mano esegue il binario precedente, e ottiene
un risultato che non riguarda il codice che ha scritto — rosso su codice
corretto, o verde su codice rotto. E successo due volte in una sola
sessione: la correzione sul `SessionContext` e quella sul limite di 52
caratteri sono state entrambe "smentite" da un `.so` vecchio.

Questo runner toglie il caso dalla mano di chi esegue:

1. costruisce il wheel con `maturin --locked` dentro l'immagine Rust
   pinnata, con la maturin fissata da `requirements-sdk-build.txt`;
2. **cancella** il `.so` esistente prima di installare il nuovo, cosi un
   fallimento di build non puo lasciare in piedi quello di prima;
3. estrae il modulo dal wheel appena prodotto e lo mette al suo posto;
4. esegue `pytest` sulle reti Compose dei riferimenti attivi, in un
   ambiente Python fissato da `requirements-sdk-tests.txt`;
5. verifica che ne' la build ne' i test abbiano modificato un file
   tracciato, e registra nel verdetto di cosa e fatto l'artefatto che ha
   girato.

Uso:

    python scripts/check_sdk_tests.py                  # Postgres + MySQL
    python scripts/check_sdk_tests.py --offline        # solo test senza server
    python scripts/check_sdk_tests.py --benchmark-only # solo i bench di parita

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
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import (  # noqa: E402
    compose_network_arguments,
    compose_volume,
    container_variable,
)

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "crates" / "plenora-database-py"
PYPROJECT = CRATE / "pyproject.toml"
NATIVE = CRATE / "python" / "plenora_database" / "_native.abi3.so"
BUILD_REQUIREMENTS = ROOT / "requirements-sdk-build.txt"
TEST_REQUIREMENTS = ROOT / "requirements-sdk-tests.txt"
RUST_IMAGE = "rust:1.92"
PYTHON_IMAGE = "python:3.13-slim"

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
    requirement senza `==` rende l'ambiente non riproducibile, ed e proprio
    cio che questi file esistono per impedire.
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
# File tracciati: build e test non ne toccano nessuno
# --------------------------------------------------------------------------


def tracked_state() -> dict[str, str]:
    """Lo stato dei file **tracciati**, percorso per percorso.

    Il `.so` e i wheel sono gitignorati e non compaiono: cio che deve restare
    fermo e il sorgente. Una build che riscrive `Cargo.lock`, un test che
    lascia una fixture sul disco o una formattazione applicata di straforo
    cambierebbero il repository mentre lo si sta verificando, e il verdetto
    parlerebbe di un albero diverso da quello di partenza.
    """

    state: dict[str, str] = {}
    for line in git(["status", "--porcelain", "--untracked-files=no"]).splitlines():
        if len(line) > 3:
            state[line[3:].strip()] = line[:2]
    for line in git(["diff", "HEAD", "--numstat"]).splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            path = parts[2].strip()
            state[path] = f"{state.get(path, '')}+{parts[0]}-{parts[1]}"
    return state


def assert_tracked_unchanged(before: dict[str, str], stage: str) -> None:
    """Confronta con lo stato di partenza e fallisce nominando i file.

    # Raises

    `RuntimeError` con l'elenco dei percorsi cambiati durante `stage`.
    """

    after = tracked_state()
    if after == before:
        return
    changed = sorted(
        set(before) ^ set(after)
        | {path for path in set(before) & set(after) if before[path] != after[path]}
    )
    raise RuntimeError(f"{stage} ha modificato file tracciati: {changed}")


# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------


def build_extension() -> dict[str, str]:
    """Costruisce il wheel con maturin e installa il modulo nativo.

    # Returns

    Nome del wheel, SHA-256 del wheel e del modulo nativo estratto, versione
    di maturin. Il solo nome non identifica l'artefatto: due wheel con lo
    stesso nome escono da qualunque coppia di build, e il verdetto serve
    proprio a dire *quale* ha girato.

    # Raises

    `RuntimeError` se la build fallisce. Il `.so` precedente e gia stato
    rimosso a quel punto: meglio nessuna estensione che una vecchia, perche
    la seconda produce risultati che sembrano validi.
    """

    if NATIVE.exists():
        NATIVE.unlink()

    script = (
        "set -e; "
        "apt-get update -qq >/dev/null 2>&1; "
        "apt-get install -y -qq python3 python3-venv unzip >/dev/null 2>&1; "
        "python3 -m venv /tmp/v; "
        f"/tmp/v/bin/pip install -q -r /workspace/{BUILD_REQUIREMENTS.name}; "
        "cd /workspace/crates/plenora-database-py; "
        "/tmp/v/bin/maturin build --release --locked --out /tmp/wheels; "
        "cd /tmp/wheels; "
        "wheel=$(ls *.whl | head -n 1); "
        "unzip -o -q \"$wheel\" -d /tmp/extracted; "
        "cp /tmp/extracted/plenora_database/_native.abi3.so "
        "/workspace/crates/plenora-database-py/python/plenora_database/; "
        f'echo "{BUILD_MARKER}$wheel '
        '$(sha256sum "$wheel" | cut -d\' \' -f1) '
        '$(/tmp/v/bin/maturin --version | cut -d\' \' -f2)"'
    )
    output = run(
        [
            "docker", "run", "--rm",
            "-v", f"{ROOT}:/workspace",
            "-v", "plenora_cargo_registry:/usr/local/cargo/registry",
            "-v", "plenora_cargo_git:/usr/local/cargo/git",
            "-v", "pln_target_docker:/workspace/target-docker",
            "-w", "/workspace",
            "-e", "CARGO_TARGET_DIR=/workspace/target-docker",
            RUST_IMAGE, "sh", "-c", script,
        ],
        capture=True,
    )
    if not NATIVE.exists():
        raise RuntimeError("maturin non ha prodotto il modulo nativo")
    marker = marker_line(output, BUILD_MARKER)
    fields = marker.split()
    if len(fields) != 3:
        raise RuntimeError(f"riga di build non interpretabile: '{marker}'")
    wheel, wheel_digest, maturin = fields
    return {
        "wheel": wheel,
        "wheel_sha256": wheel_digest,
        "native_sha256": hashlib.sha256(NATIVE.read_bytes()).hexdigest(),
        "maturin": maturin,
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
    ]


def pytest_command(*, scope: str) -> list[str]:
    """Il `docker run` che esegue la suite nello scope richiesto.

    `offline` non tocca i riferimenti — nessuna rete, nessuna credenziale, i
    test live si saltano da soli. `benchmark` e `live` vedono entrambi i
    riferimenti; il primo filtra i soli bench di parita, che senza filtro
    girerebbero comunque dentro la corsa completa.
    """

    command = ["docker", "run", "--rm"]
    environment: list[str] = []
    selection = ["python/tests"]

    if scope != "offline":
        command += compose_network_arguments(POSTGRES_CONTAINER, MYSQL_CONTAINER)
        tls_volume = compose_volume(MYSQL_CONTAINER, "/etc/mysql/tls")
        command += ["-v", f"{tls_volume}:/mysql-tls:ro"]
        environment = live_environment()
    if scope == "benchmark":
        selection += ["-k", "benchmark"]

    script = (
        f"set -e; pip install -q -r /workspace/{TEST_REQUIREMENTS.name}; "
        f'echo "{PYTHON_MARKER}$(python -V 2>&1 | cut -d\' \' -f2)"; '
        f'echo "{PACKAGES_MARKER}$(pip freeze | tr \'\\n\' \' \')"; '
        "PYTHONPATH=/workspace/crates/plenora-database-py/python "
        f"python -m pytest {' '.join(selection)} -q -rs"
    )
    command += [
        "-v", f"{ROOT}:/workspace",
        "-w", "/workspace/crates/plenora-database-py",
        *environment,
        PYTHON_IMAGE, "sh", "-c", script,
    ]
    return command


def installed_versions(output: str) -> dict[str, str]:
    """Le versioni effettive dell'ambiente che ha eseguito la suite."""

    packages: dict[str, str] = {}
    for entry in marker_line(output, PACKAGES_MARKER).split():
        name, separator, version = entry.partition("==")
        if separator:
            packages[normalise_package(name)] = version
    validate_installed_pins(packages)
    packages["python"] = marker_line(output, PYTHON_MARKER)
    return packages


def verdict(
    *,
    scope: str,
    commit: str,
    artifact: dict[str, str],
    versions: dict[str, str],
    summary: str,
) -> dict[str, object]:
    """Il verdetto, che deve identificare cio che ha girato.

    Il nome del wheel non lo identifica: e lo stesso per ogni build dello
    stesso pacchetto, quindi un verdetto che si fermasse li direbbe solo che
    *un* wheel e stato costruito. Servono il commit, il digest dell'artefatto
    — quello del wheel e quello del modulo estratto, che e cio che pytest ha
    davvero caricato — e le versioni con cui la suite ha girato.
    """

    return {
        "schema_version": 2,
        "gate": "python-sdk-suite",
        "status": "passed",
        "scope": scope,
        "git_commit": commit,
        "tracked_files": "unchanged",
        "artifact": {
            "wheel": artifact["wheel"],
            "wheel_sha256": artifact["wheel_sha256"],
            "native_sha256": artifact["native_sha256"],
        },
        "versions": {
            "maturin": artifact["maturin"],
            "pandas": versions["pandas"],
            "pyarrow": versions["pyarrow"],
            "pytest": versions["pytest"],
            "pytest_asyncio": versions["pytest-asyncio"],
            "python": versions["python"],
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
        commit = git(["rev-parse", "HEAD"]).strip()
        before = tracked_state()
        artifact = build_extension()
        assert_tracked_unchanged(before, "la build del wheel")
        output = run(pytest_command(scope=selected), capture=True)
        assert_tracked_unchanged(before, "l'esecuzione della suite")
        versions = installed_versions(output)
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
                artifact=artifact,
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
