#!/usr/bin/env python3
"""Esegue i job offline dei workflow, conservando comandi e argomenti.

PyYAML (requirements-self-tests.txt) interpreta anche scalari folded/literal,
anchor e alias. Non si analizza la shell con regex: ogni run resta un programma
intero, eseguito con errexit e pipefail. Le esclusioni sono per job e richiedono
un testimone eseguibile, non il nome o un commento. Un job nuovo entra nel piano;
un'action o un contesto non supportati fermano quel job, mai un verde implicito.

I prerequisiti devono essere disponibili localmente: pip e Cargo lavorano senza
rete. SWEEP_CONTRACTS_REPOSITORY puo indicare un repository locale dal quale
leggere il checkout immutabile richiesto dalla CI, senza creare worktree.
Gli skip dei gate danno exit 2; i fallimenti exit 1. I log completi e il referto
JSON sono in target/sweep. Le matrici di OS/Python dichiarano le celle non locali.
"""
from __future__ import annotations

import copy
import itertools
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"

# Ogni esclusione lega un job preciso a un comando/action che ne dimostra il
# requisito esterno. Se il testimone scompare, occorre rivalutare il job.
EXCLUDED = {
    "age-assurance.yml/age": ("docker compose", "fixture AGE in Docker"),
    "db2-assurance.yml/db2-linux": ("docker compose", "fixture Db2 in Docker"),
    "mariadb-evidence.yml/mariadb-evidence": ("scripts/check_mariadb_campaign.py", "campagna MariaDB live in Docker"),
    "mysql-assurance.yml/reference": ("scripts/check_mysql_reference.py", "riferimento MySQL live"),
    "mysql-assurance.yml/matrix": ("scripts/check_mysql_matrix.py", "matrice MySQL live"),
    "oracle-assurance.yml/oracle-linux": ("docker compose", "fixture Oracle in Docker"),
    "postgres-assurance.yml/postgres": ("docker compose", "fixture PostgreSQL in Docker"),
    "postgres-assurance.yml/hardening": ("docker compose", "fixture PostgreSQL TLS in Docker"),
    "postgres-matrix.yml/matrix": ("scripts/check_postgres_matrix.py", "matrice PostgreSQL live"),
    "python-wheel.yml/linux": ("PyO3/maturin-action", "wheel manylinux costruito in Docker"),
    "python-wheel.yml/db2-linux": ("docker compose", "wheel qualificato con fixture Db2"),
    "python-wheel.yml/smoke-test": ("actions/download-artifact", "scarica il wheel Linux dalla CI"),
    "python-wheel.yml/abi3-runtime": ("actions/download-artifact", "scarica i wheel della matrice dalla CI"),
    "python-wheel.yml/release-sbom": ("actions/download-artifact", "scarica artefatti CI, incluso il wheel Db2 live"),
    "python-wheel.yml/attach-to-release": ("softprops/action-gh-release", "pubblicazione in rete di release e attestazioni"),
    "sdk-assurance.yml/sdk-assurance": ("scripts/check_sdk_campaign.py", "campagna SDK con fixture live"),
    "session-matrix.yml/session-matrix": ("scripts/check_session_campaign.py", "campagna sessioni con fixture live"),
    "spatial-srid-probes.yml/probes": ("scripts/sonde_srid_server.py", "sonde contro database live"),
    "spatial-srid-probes.yml/operandi": ("scripts/sonde_operandi_server.py", "sonde contro database live"),
    "sqlserver-assurance.yml/sqlserver": ("docker compose", "fixture SQL Server in Docker"),
    "sqlserver-matrix.yml/matrix": ("scripts/check_sqlserver_matrix.py", "matrice SQL Server live"),
    "supply-chain.yml/cargo-deny": ("scripts/check_cargo_deny.py", "cargo deny eseguito in Docker con database RustSec"),
    "write-admission-probes.yml/probes": ("scripts/sonde_scritture_server.py", "sonde contro database live"),
}


def discover(directory: Path = WORKFLOWS) -> dict[str, dict]:
    """Scopre tutti i job, inclusi quelli condizionati e le action senza run."""
    found = {}
    for path in sorted([*directory.glob("*.yml"), *directory.glob("*.yaml")]):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict) or not isinstance(document.get("jobs"), dict):
            raise ValueError(f"workflow non valido: {path.name}")
        for name, job in document["jobs"].items():
            if not isinstance(job, dict) or not isinstance(job.get("steps"), list):
                raise ValueError(f"job non supportato: {path.name}/{name}")
            found[f"{path.name}/{name}"] = {
                "job": job,
                "env": document.get("env", {}),
                "defaults": document.get("defaults", {}),
            }
    if not found:
        raise ValueError("nessun workflow scoperto")
    return found


def build_plan(jobs: dict[str, dict], exclusions: dict | None = None) -> dict[str, dict]:
    """Conserva i job interi: argomenti, ordine, condizioni, matrici e ambiente."""
    exclusions = EXCLUDED if exclusions is None else exclusions
    stale = set(exclusions) - set(jobs)
    if stale:
        raise ValueError(f"esclusioni scadute: {sorted(stale)}")
    plan = {}
    for key, source in jobs.items():
        if key in exclusions:
            witness, reason = exclusions[key]
            executable = "\n".join(
                "\n".join(line for line in str(step.get("run", step.get("uses", ""))).splitlines()
                          if not line.lstrip().startswith("#"))
                for step in source["job"]["steps"]
            )
            if not witness or witness not in executable or not reason or "--static" in executable:
                raise ValueError(f"esclusione senza testimone: {key}")
        else:
            plan[key] = copy.deepcopy(source)
    return plan


def coverage_errors(jobs: dict, plan: dict, exclusions: dict | None = None) -> list[str]:
    """Confronto indipendente del piano: nessuna impronta per sottocomando."""
    exclusions = EXCLUDED if exclusions is None else exclusions
    errors = []
    for key in sorted(set(jobs) | set(plan)):
        if key in exclusions:
            if key in plan:
                errors.append(f"job insieme escluso e coperto: {key}")
        elif jobs.get(key) != plan.get(key):
            errors.append(f"job mancante o comandi/contesto diversi: {key}")
    return errors


def expand(value, matrix: dict):
    """Espande solo il contesto locale noto; espressioni residue sono errori."""
    if isinstance(value, dict):
        return {key: expand(item, matrix) for key, item in value.items()}
    if isinstance(value, list):
        return [expand(item, matrix) for item in value]
    if not isinstance(value, str):
        return value
    replacements = {"github.workspace": ROOT.as_posix(), "runner.temp": (ROOT / "target/sweep/tmp").as_posix()}
    replacements.update({f"matrix.{key}": str(item) for key, item in matrix.items()})
    for key, item in replacements.items():
        value = value.replace("${{ " + key + " }}", item)
    if "${{" in value:
        raise ValueError("espressione GitHub non valutabile localmente")
    return value


def matrix_cells(job: dict) -> list[dict]:
    """Espande il prodotto cartesiano; include/exclude non vengono ignorati."""
    matrix = job.get("strategy", {}).get("matrix", {})
    if "include" in matrix or "exclude" in matrix:
        raise ValueError("matrice include/exclude non supportata localmente")
    keys = list(matrix)
    return [dict(zip(keys, values)) for values in itertools.product(*(matrix[key] for key in keys))]


def run(command: list[str], env: dict, cwd: Path = ROOT) -> subprocess.CompletedProcess:
    """Una sola subprocess alla volta, senza scartare stdout o stderr."""
    return subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", check=False)


def prepare_contracts(inputs: dict, env: dict) -> None:
    """Verifica il checkout esistente, senza cambiare HEAD, index o sorgenti."""
    repository = Path(env.get("SWEEP_CONTRACTS_REPOSITORY", str(ROOT / inputs["path"]))).resolve()
    if not (repository / ".git").exists():
        raise ValueError("checkout Git del pin assente; impostare SWEEP_CONTRACTS_REPOSITORY a un checkout locale gia pronto")
    command = ["git", "-c", f"safe.directory={repository.as_posix()}", "-C", str(repository)]
    head = run([*command, "rev-parse", "HEAD"], env)
    if head.returncode or head.stdout.strip() != inputs["ref"]:
        raise ValueError("checkout contrattuale locale non al commit richiesto dalla CI")
    diff = run([*command, "diff", "--exit-code", "HEAD"], env)
    if diff.returncode:
        raise ValueError("checkout contrattuale locale con modifiche rispetto al pin")
    print(f"    pin verificato, sola lettura: {repository}", flush=True)


def action(step: dict, env: dict) -> subprocess.CompletedProcess | None:
    """Adatta solo action note; un nuovo prerequisito non passa inosservato."""
    name = step["uses"].split("@", 1)[0]
    inputs = step.get("with", {})
    if name == "actions/checkout":
        if "repository" in inputs:
            prepare_contracts(inputs, env)
        elif inputs.get("ref") or inputs.get("path"):
            raise ValueError("checkout locale con ref/path non supportato")
    elif name == "actions/setup-python":
        wanted = str(inputs.get("python-version", ""))
        actual = f"{sys.version_info.major}.{sys.version_info.minor}"
        if wanted != actual:
            # I gate non matriciali girano con l'interprete disponibile e lo
            # dichiarano; le celle Python della matrice sono filtrate a monte.
            print(f"    interprete locale {actual}; CI richiede {wanted}", flush=True)
    elif name == "dtolnay/rust-toolchain":
        version = run(["rustc", "--version"], env)
        if version.returncode or inputs["toolchain"] not in version.stdout:
            raise ValueError("toolchain Rust fissata non disponibile")
    elif name == "PyO3/maturin-action":
        version = run([sys.executable, "-m", "maturin", "--version"], env)
        if version.returncode or version.stdout.strip() != "maturin " + inputs["maturin-version"]:
            raise ValueError("maturin fissato non disponibile")
        if "manylinux" in inputs:
            raise ValueError("manylinux richiede Docker")
        return run([sys.executable, "-m", "maturin", inputs["command"], *shlex.split(inputs["args"])], env)
    elif name in {"Swatinem/rust-cache", "actions/upload-artifact"}:
        print(f"    infrastruttura CI {name}: cache/log restano locali", flush=True)
    else:
        raise ValueError(f"action non supportata: {name}")
    return None


def execute(step: dict, env: dict, defaults: dict) -> subprocess.CompletedProcess | None:
    """Conserva il programma shell; adatta solo interprete e suffisso .exe."""
    if "uses" in step:
        return action(step, env)
    script = step["run"]
    if "dist/*.whl" in script:
        # Il wheel di prova appartiene alla corsa, non all'installazione
        # Python dell'utente. PYTHONPATH lo rende visibile ai gate seguenti.
        env = dict(env, PIP_TARGET=env["SWEEP_WHEEL_SITE"], PIP_UPGRADE="1")
    # La CI parte da un ambiente pulito. In locale pip altrimenti considera
    # soddisfatto un wheel della stessa versione ma costruito da altri sorgenti.
    script = script.replace("-m pip install dist/*.whl", "-m pip install --force-reinstall --no-deps dist/*.whl")
    shell = step.get("shell", defaults.get("shell", "bash"))
    cwd = ROOT / step.get("working-directory", defaults.get("working-directory", "."))
    # I comandi semplici possono evitare del tutto la shell. Tutti gli altri
    # mantengono il corpo originale, inclusi cicli, redirect e continuazioni.
    if "\n" not in script and not any(c in script for c in "$|&;<>*`"):
        argv = shlex.split(script)
        if not argv:
            raise ValueError("run vuoto non qualificante")
        contracts = env.get("SWEEP_CONTRACTS_REPOSITORY")
        if contracts:
            # Cambia soltanto la posizione del checkout verificato dal passo
            # precedente: il repository e il commit sono quelli della CI.
            argv = [str(Path(contracts) / arg.removeprefix(".plenora-contracts/"))
                    if arg.startswith(".plenora-contracts/") else contracts
                    if arg == ".plenora-contracts" else arg for arg in argv]
        if os.name == "nt":
            argv = [arg + ".exe" if arg == "target/debug/plenora-database" else arg for arg in argv]
        if argv[0] in {"python", "python3"}:
            argv[0] = sys.executable
        elif argv[0] in {"bash", "pwsh"}:
            executable = shutil.which(argv[0])
            if executable is None:
                raise ValueError(f"shell richiesta non installata: {argv[0]}")
            argv[0] = executable
        return run(argv, env, cwd)
    if shell.startswith("bash"):
        executable = shutil.which("bash")
        if executable is None:
            raise ValueError("Bash richiesto dal workflow non installato")
        prefix = f"function python() {{ {shlex.quote(Path(sys.executable).as_posix())} \"$@\"; }}\n"
        prefix += "function python3() { python \"$@\"; }\nexport -f python python3\n"
        return run([executable, "--noprofile", "--norc", "-euo", "pipefail", "-c", prefix + script], env, cwd)
    if shell == "pwsh":
        executable = shutil.which("pwsh")
        if executable is None:
            raise ValueError("PowerShell 7 (pwsh) richiesto dal workflow non installato")
        return run([executable, "-NoProfile", "-Command", "$ErrorActionPreference='Stop';\n" + script], env, cwd)
    raise ValueError(f"shell non supportata: {shell}")


def main() -> int:
    jobs = discover()
    plan = build_plan(jobs)
    errors = coverage_errors(jobs, plan)
    if errors:
        raise ValueError("; ".join(errors))
    output = ROOT / "target/sweep"
    output.mkdir(parents=True, exist_ok=True)
    (output / "tmp").mkdir(exist_ok=True)
    env = dict(os.environ, CARGO_BUILD_JOBS="4", CARGO_NET_OFFLINE="true",
               PIP_NO_INDEX="1", PYTHONIOENCODING="utf-8", PYTHONDONTWRITEBYTECODE="1",
               RUNNER_TEMP=(output / "tmp").as_posix(), GITHUB_WORKSPACE=ROOT.as_posix())
    # Il verificatore del wheel rifiuta qualunque import sotto il checkout.
    # Una directory temporanea separa la prova anche dal Python dell'utente.
    packages = tempfile.TemporaryDirectory(prefix="plenora-sweep-")
    local_packages = str(Path(packages.name) / "site-packages")
    env["SWEEP_WHEEL_SITE"] = local_packages
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [local_packages, env.get("PYTHONPATH")]))
    env["GITHUB_SHA"] = run(["git", "rev-parse", "HEAD"], env).stdout.strip()
    results = []

    def record(label: str, status: str, reason: str = "", elapsed: float = 0):
        results.append(dict(label=label, status=status, reason=reason, seconds=elapsed))
        print(f"{status}: {label} ({elapsed:.1f}s) {reason}", flush=True)
        (output / "results.json").write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")

    for key, (_, reason) in EXCLUDED.items():
        record(key, "escluso", reason)
    # Prima i self-test: una guardia rotta emerge senza aspettare le build.
    ordered = sorted(plan, key=lambda key: (key != "rust-ci.yml/static-self-tests", key))
    for key in ordered:
        source = plan[key]
        job = source["job"]
        try:
            cells = matrix_cells(job)
        except ValueError as error:
            record(key, "non eseguito qui", str(error))
            continue
        for cell in cells:
            label = key + (f" {cell}" if cell else "")
            reason = ""
            if "python" in cell and str(cell["python"]) != f"{sys.version_info.major}.{sys.version_info.minor}":
                reason = "interprete della cella non disponibile nel processo locale"
            if "os" in cell and (cell["os"].startswith("windows") != (os.name == "nt")):
                reason = "sistema operativo della cella diverso da quello locale"
            if os.name == "nt" and key == "fuzz.yml/fuzz":
                reason = "libFuzzer con sanitizer richiede il runner Linux/nightly del workflow"
            if os.name == "nt" and key == "coverage.yml/python-binding":
                reason = "instrumentazione della wheel con show-env --sh richiede ambiente LLVM/POSIX del runner Linux"
            if "if" in job or job.get("services") or job.get("container") or job.get("continue-on-error"):
                reason = "contesto job condizionato/container non riproducibile localmente"
            if reason:
                record(label, "non eseguito qui", reason)
                continue
            blocked = ""
            for index, original in enumerate(job["steps"]):
                step_label = f"{label}/{index}: {original.get('name', original.get('uses', 'run'))}"
                if blocked:
                    record(step_label, "non eseguito qui", f"prerequisito non riuscito: {blocked}")
                    continue
                started = time.monotonic()
                try:
                    infrastructure = original.get("uses", "").split("@", 1)[0]
                    if infrastructure in {"Swatinem/rust-cache", "actions/upload-artifact"}:
                        record(step_label, "non applicabile", "cache e referti conservati localmente")
                        continue
                    step = expand(original, cell)
                    if step.get("uses", "").startswith("actions/setup-python@"):
                        wanted = str(step.get("with", {}).get("python-version", ""))
                        actual = f"{sys.version_info.major}.{sys.version_info.minor}"
                        if wanted != actual:
                            record(step_label, "non eseguito qui", f"Python CI {wanted}; i gate seguenti usano Python locale {actual}")
                            continue
                    condition = step.get("if")
                    if condition == "runner.os == 'Linux'" and os.name == "nt":
                        record(step_label, "non applicabile", "unixODBC di Linux; Windows usa odbc32 di sistema")
                        continue
                    if "if" in step and not (condition == "runner.os == 'Linux'" and os.name != "nt"):
                        if "uses" not in step or not step["uses"].startswith("actions/upload-artifact@"):
                            raise ValueError("condizione del passo non valutabile localmente")
                    step_env = dict(env)
                    for extra in (source["env"], job.get("env", {}), step.get("env", {})):
                        step_env.update({k: str(v) for k, v in expand(extra, cell).items()})
                    step_env["CARGO_BUILD_JOBS"] = "4"
                    defaults = {**source["defaults"].get("run", {}), **job.get("defaults", {}).get("run", {})}
                    script = step.get("run", "")
                    if os.name == "nt" and "lscpu" in script:
                        record(step_label, "non eseguito qui", "lscpu e inventario CPU del runner Linux non disponibili su Windows")
                        continue
                    if script.startswith("sudo apt-get") and os.name == "nt":
                        record(step_label, "non applicabile", "pacchetti Linux; Windows usa odbc32 di sistema")
                        continue
                    if script == "cargo fetch --locked":
                        record(step_label, "non applicabile", "cache locale verificata dai build Cargo offline")
                        continue
                    if script.startswith("cargo install cargo-llvm-cov"):
                        wanted = shlex.split(script)[shlex.split(script).index("--version") + 1]
                        version = run(["cargo", "llvm-cov", "--version"], step_env)
                        if version.returncode or version.stdout.strip() != "cargo-llvm-cov " + wanted:
                            raise ValueError(f"cargo-llvm-cov {wanted} non installato; installazione richiede rete")
                        record(step_label, "ok", "versione fissata gia installata")
                        continue
                    if "runtime_vectors" in script and "PLENORA_RUNTIME_EVIDENCE" in step_env:
                        evidence = Path(step_env["PLENORA_RUNTIME_EVIDENCE"]).resolve()
                        if not evidence.is_relative_to((ROOT / "target").resolve()):
                            raise ValueError("directory evidenze fuori da target")
                        # Il validatore deve vedere soltanto le prove della
                        # corsa corrente, anche se un test omette un output.
                        for old in evidence.glob("*.json"):
                            old.unlink()
                    print(f"IN CORSO: {step_label}", flush=True)
                    completed = execute(step, step_env, defaults)
                    if completed is not None:
                        log = output / f"{len(results):04d}.log"
                        log.write_text(completed.stdout + completed.stderr, encoding="utf-8")
                        if completed.returncode:
                            record(step_label, "ROTTO", f"exit {completed.returncode}; {log.relative_to(ROOT)}", time.monotonic() - started)
                            # Un pip offline fallito resta rosso, ma non impedisce
                            # di raccogliere i verdetti dei gate con i moduli gia
                            # presenti. Una build fallita invece vieta l'uso di
                            # binari o evidenze potenzialmente residui.
                            if ("uses" in step or "cargo build" in script or "maturin build" in script
                                    or "runtime_vectors" in script or "dist/*.whl" in script):
                                blocked = step_label
                            continue
                    record(step_label, "ok", elapsed=time.monotonic() - started)
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    record(step_label, "non eseguito qui", str(error), time.monotonic() - started)
                    blocked = step_label
    failed = sum(row["status"] == "ROTTO" for row in results)
    skipped = sum(row["status"] == "non eseguito qui" for row in results)
    print(f"\nSweep: {len(plan)}/{len(jobs)} job offline nel piano; {failed} fallimenti; {skipped} non eseguiti qui.", flush=True)
    packages.cleanup()
    return 1 if failed else 2 if skipped else 0


if __name__ == "__main__":
    raise SystemExit(main())
