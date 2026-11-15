#!/usr/bin/env python3
"""Deriva la verifica PR dai workflow; nessun comando di gate duplicato.

Python 3.14 e PyYAML da requirements-self-tests.txt. La lettura del piano
funziona anche con 3.11 per diagnosticare un host non ancora predisposto.
Exit 1: almeno un fallimento; 2: copertura incompleta; 0: tutti i gate PASS.
Non installa toolchain e non accede a GitHub. I filtri PR sono conservati:
senza un evento remoto il profilo pr e il sovrainsieme di tutte le PR.
"""
from __future__ import annotations

import argparse
import copy
import datetime
import hashlib
import itertools
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import time
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
# Adattamenti infrastrutturali, mai comandi di test. Nessuna installazione.
ACTIONS = {
    "actions/checkout": "checkout",
    "actions/setup-python": "python",
    "dtolnay/rust-toolchain": "rust",
    "Swatinem/rust-cache": "cache",
}
SHELLS = {
    "bash": ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail"],
    "sh": ["sh", "-e"],
    "pwsh": ["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-File"],
    "powershell": ["powershell", "-NoLogo", "-NoProfile", "-NonInteractive", "-File"],
}
POLICY_ENV = {"CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0",
              "CARGO_NET_OFFLINE": "true", "PIP_NO_INDEX": "1",
              "PIP_DISABLE_PIP_VERSION_CHECK": "1", "PYTHONDONTWRITEBYTECODE": "1"}
EXPRESSION = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")


def read_yaml(path: Path) -> dict:
    """SafeLoader conserva i tipi; 'on' YAML 1.1 viene normalizzato qui."""
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("documento YAML non valido")
    if True in document:
        document["on"] = document.pop(True)
    return document


def local_path(root: Path, name: str) -> Path:
    """Ogni percorso locale resta nel worktree, anche dopo i symlink."""
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("percorso fuori dal worktree")
    return path


def matrix_cells(matrix: dict) -> list[dict]:
    """Prodotto, exclude parziale, include sulle combinazioni originali."""
    if not isinstance(matrix, dict):
        raise ValueError("matrice dinamica non risolvibile")
    axes = {k: v for k, v in matrix.items() if k not in {"include", "exclude"}}
    if any(not isinstance(v, list) for v in axes.values()):
        raise ValueError("asse matrice dinamico non risolvibile")
    originals = [dict(zip(axes, row)) for row in itertools.product(*axes.values())]
    originals = [row for row in originals if not any(
        all(row.get(k) == v for k, v in excluded.items())
        for excluded in matrix.get("exclude", []))]
    cells = copy.deepcopy(originals)
    extras = []
    for included in matrix.get("include", []):
        matched = False
        for original, cell in zip(originals, cells):
            if all(k not in original or original[k] == v for k, v in included.items()):
                cell.update(included)
                matched = True
        if not matched:
            extras.append(included.copy())
    if not axes and matrix.get("include"):
        return copy.deepcopy(matrix["include"])
    return cells + extras


def expand(value, context: dict):
    """Le espressioni ignote restano visibili e vietano l'esecuzione."""
    if isinstance(value, dict):
        return {k: expand(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [expand(v, context) for v in value]
    if isinstance(value, str):
        return EXPRESSION.sub(lambda m: str(context.get(m[1], m[0])), value)
    return value


def condition(value, host: str, failed: bool = False) -> bool | None:
    """Sottoinsieme esplicito del linguaggio Actions, senza eval Python."""
    if isinstance(value, bool):
        return value
    text = str(value).strip()
    if text.startswith("${{") and text.endswith("}}"):
        text = text[3:-2].strip()
    known = {"success()": not failed, "always()": True, "failure()": failed,
             "cancelled()": False, "true": True, "false": False}
    if text in known:
        return known[text]
    match = re.fullmatch(r"(runner.os|github.event_name)\s*(==|!=)\s*'([^']*)'", text)
    if match:
        actual = host if match[1] == "runner.os" else "pull_request"
        return (actual == match[3]) == (match[2] == "==")
    return None


def expand_steps(root: Path, steps: list, context: dict, env: dict,
                 defaults: dict, source: str, chain: tuple = ()) -> list[dict]:
    """Mantiene anche il nodo uses e visita le composite locali ricorsivamente."""
    result = []
    for index, raw in enumerate(steps):
        step = expand(raw, context)
        item = {"source": f"{source}/steps/{index}", "raw": raw,
                "step": step, "env": expand({**env, **step.get("env", {})}, context),
                "shell": step.get("shell", defaults.get("shell", "bash")),
                "working_directory": step.get("working-directory", defaults.get("working-directory", ".")),
                "reason": "", "structural": False}
        result.append(item)
        use = step.get("uses", "")
        if not use.startswith("./"):
            continue
        try:
            directory = local_path(root, use)
            path = next(p for p in (directory / "action.yml", directory / "action.yaml") if p.is_file())
            if path in chain:
                raise ValueError("ciclo nelle azioni locali")
            action = read_yaml(path)
            if action.get("runs", {}).get("using") != "composite":
                raise ValueError("azione locale non composite")
            inputs = {k: v.get("default", "") for k, v in action.get("inputs", {}).items()}
            inputs.update(step.get("with", {}))
            nested_context = {**context, **{f"inputs.{k}": v for k, v in inputs.items()},
                              "github.action_path": directory.as_posix()}
            children = expand_steps(root, action["runs"]["steps"], nested_context,
                                    item["env"], defaults, item["source"] + "/" + use,
                                    (*chain, path))
            for child in children:
                child.setdefault("parent_conditions", []).append(step.get("if", True))
            item["structural"] = True
            item["reason"] = "azione composite espansa nei passi figli"
            result.extend(children)
        except (ValueError, OSError, StopIteration, KeyError) as exc:
            item["reason"] = f"azione locale non riproducibile ({type(exc).__name__})"
    return result


def build_plan(root: Path = ROOT) -> dict:
    """Scopre tutti i workflow: anche i job fuori profilo restano nel piano."""
    plan = {"schema": 1, "profile": "pr", "jobs": [], "documents": {}}
    def visit(path, prefix="", inherited=None, chain=()):
        document = read_yaml(path)
        relative = path.relative_to(root).as_posix()
        plan["documents"][relative] = document
        events = document.get("on", {}) if inherited is None else inherited["events"]
        for name, raw in document.get("jobs", {}).items():
            key = prefix + relative + "/" + name
            matrix_error = ""
            try:
                cells = matrix_cells(raw.get("strategy", {}).get("matrix", {}))
                if not cells:
                    cells, matrix_error = [{}], "matrice senza celle eseguibili"
            except ValueError as exc:
                cells, matrix_error = [{}], str(exc)
            for index, matrix in enumerate(cells):
                context = {"github.workspace": root.as_posix(), "github.event_name": "pull_request",
                           "runner.temp": (root / "target/ci-locale/tmp").as_posix(),
                           **{f"matrix.{k}": v for k, v in matrix.items()}}
                if inherited:
                    context.update(inherited["context"])
                job = expand(raw, context)
                env = {**document.get("env", {}), **job.get("env", {})}
                context.update({f"env.{k}": v for k, v in env.items()})
                defaults = {**document.get("defaults", {}).get("run", {}),
                            **job.get("defaults", {}).get("run", {})}
                defaults = expand(defaults, context)
                defaults.setdefault("shell", "pwsh" if "windows" in str(job.get("runs-on", "")) else "bash")
                entry = {"id": f"{key}[{index}]", "key": key, "matrix": matrix, "raw": raw,
                         "job": job, "events": events, "reason": matrix_error,
                         "blocking": job.get("continue-on-error", False) is not True,
                         "pr": "pull_request" in events, "steps": [],
                         "services": job.get("services", {}), "toolchains": []}
                toolchain_file = root / "rust-toolchain.toml"
                if toolchain_file.is_file():
                    entry["toolchains"].append({"file": "rust-toolchain.toml",
                        "declared": tomllib.loads(toolchain_file.read_text(encoding="utf-8"))})
                if inherited:
                    entry["parent"] = inherited["parent"]
                    entry["reason"] = entry["reason"] or inherited["reason"]
                entry["steps"] = expand_steps(root, raw.get("steps", []), context, env, defaults, key)
                for step in entry["steps"]:
                    if ACTIONS.get(step["step"].get("uses", "").split("@")[0]) in {"python", "rust"}:
                        entry["toolchains"].append({"action": step["step"].get("uses"),
                                                    "with": step["step"].get("with", {})})
                        version_file = step["step"].get("with", {}).get("python-version-file")
                        if version_file and "${{" not in version_file:
                            try:
                                entry["toolchains"][-1]["declared_version"] = local_path(root, version_file).read_text(encoding="utf-8").strip()
                            except (OSError, ValueError):
                                step["reason"] = "file della versione Python non disponibile"
                plan["jobs"].append(entry)
                use = job.get("uses", "")
                if not use:
                    continue
                try:
                    if not use.startswith("./"):
                        raise ValueError("workflow riusabile remoto non disponibile")
                    child = local_path(root, use)
                    if child in (*chain, path):
                        raise ValueError("ciclo nei workflow riusabili")
                    called = read_yaml(child)
                    definitions = called.get("on", {}).get("workflow_call", {}) or {}
                    inputs = {k: v.get("default", "") for k, v in definitions.get("inputs", {}).items()}
                    inputs.update(job.get("with", {}))
                    reason = entry["reason"]
                    if job.get("secrets") or job.get("if") or job.get("needs"):
                        reason = reason or "contesto del chiamante condizionale/secrets/needs non riproducibile"
                    visit(child, entry["id"] + "/", {"events": events, "parent": entry["id"],
                          "reason": reason, "context": {f"inputs.{k}": v for k, v in inputs.items()}}, (*chain, path))
                    entry["reason"] = "workflow locale espanso nei job figli"
                except (ValueError, OSError) as exc:
                    entry["reason"] = str(exc)
    for path in sorted((root / ".github/workflows").glob("*")):
        if path.suffix in {".yml", ".yaml"}:
            visit(path)
    return plan


def resources(entry: dict, root: Path) -> list[str]:
    """Deriva servizi anche dai programmi chiamati, senza elenco di job live.

    E intenzionalmente conservativo: un riferimento Docker in un runner e
    sufficiente a richiedere opt-in. I self-test non avviano il runner importato.
    """
    found = []
    if entry["services"] or entry["job"].get("container"):
        found.append("servizi/container dichiarati nel workflow")
    for item in entry["steps"]:
        script = item["step"].get("run", "")
        if re.search(r"\b(docker|podman|vagrant|virsh)\b", script):
            found.append("Docker/VM nel comando")
        for name in re.findall(r"(?:scripts|\.github/scripts)/[\w/.-]+\.(?:py|sh)", script):
            if Path(name).name.startswith("test_") or "--static" in script:
                continue
            path = local_path(root, name)
            if path.is_file():
                body = path.read_text(encoding="utf-8")
                if re.search(r"\b(docker|podman|vagrant|virsh|fixture_campaign)\b", body):
                    found.append("servizi live richiamati da " + name)
    return sorted(set(found))


def host_os() -> str:
    return {"Darwin": "macOS"}.get(platform.system(), platform.system())


def action_command(step: dict, root: Path) -> tuple[list[str] | None, str]:
    """Le azioni setup verificano il tool presente; non simulano installazioni."""
    use = step.get("uses", "").split("@")[0]
    kind = ACTIONS.get(use)
    inputs = step.get("with", {})
    if kind == "checkout":
        if inputs:
            return None, "checkout con input: richiede ambiente Actions o checkout separato"
        return ["git", "-c", f"safe.directory={root.as_posix()}", "rev-parse", "HEAD"], ""
    if kind == "python":
        wanted = str(inputs.get("python-version", ""))
        if "python-version-file" in inputs:
            wanted = local_path(root, inputs["python-version-file"]).read_text(encoding="utf-8").strip()
        if wanted != f"{sys.version_info.major}.{sys.version_info.minor}":
            return None, f"Python {wanted} richiesto; host {platform.python_version()}"
        return [sys.executable, "--version"], ""
    if kind == "rust":
        wanted = str(inputs.get("toolchain", ""))
        if not re.fullmatch(r"[\w.-]+", wanted):
            return None, "toolchain Rust non risolta"
        return ["rustup", "run", wanted, "rustc", "--version"], ""
    if kind == "cache":
        return None, "cache Actions non eseguita; cache locale disponibile"
    return None, "azione non mappata: " + use


def native_command(script: str) -> list[str] | None:
    """Invocazioni portabili singole: niente glob, blocchi o espansioni."""
    if not re.fullmatch(r"(?:cargo|python|python3) [\w ./,:=+-]+", script):
        return None
    argv = shlex.split(script)
    if argv[0] in {"python", "python3"}:
        argv[0] = sys.executable
    return argv


def assess(entry: dict, item: dict, root: Path, live: bool = False) -> tuple[str, str, list | None]:
    """Classifica senza avviare processi; nessuna esclusione elimina un passo."""
    step = item["step"]
    if "release" in entry["events"] or re.search(
            r"\b(publish|upload|attest|release|push)\b", step.get("uses", "")) or re.search(
            r"\b(?:git\s+push|gh\s+|cargo\s+publish|twine\s+upload|npm\s+publish)\b", step.get("run", "")):
        return "NON ESEGUIBILE", "release/pubblicazione vietata", None
    if not entry["pr"]:
        return "NON ESEGUIBILE", "workflow senza evento pull_request", None
    if entry["reason"]:
        return "NON ESEGUIBILE", entry["reason"], None
    needs = resources(entry, root)
    if needs and not live:
        return "NON ESEGUIBILE", "; ".join(needs) + ": richiede --servizi-live", None
    if needs and not shutil.which("docker"):
        return "SALTATO", "servizio Docker assente dal PATH", None
    if entry["services"] or entry["job"].get("container"):
        return "NON ESEGUIBILE", "provisioning services/container Actions non mappato", None
    runner = str(entry["job"].get("runs-on", ""))
    target = next((os_name for token, os_name in (("windows", "Windows"), ("ubuntu", "Linux"), ("macos", "macOS")) if token in runner), None)
    portable = native_command(step.get("run", "")) is not None or step.get("uses", "").split("@")[0] in ACTIONS
    # Le celle della matrice OS non sono intercambiabili. I gate a runner
    # fisso con una singola invocazione portabile possono invece misurare
    # l'host locale, registrandolo senza attribuirsi una prova dell'altro OS.
    matrix_runner = "matrix." in str(entry["raw"].get("runs-on", ""))
    if target != host_os():
        if target is None or matrix_runner or not portable:
            return "SALTATO", f"piattaforma richiesta {runner}; host {host_os()}", None
    step_if = step.get("if", True)
    if not any(word in str(step_if) for word in ("success()", "failure()", "always()", "cancelled()")):
        decision = condition(step_if, host_os())
        if decision is False:
            return "SALTATO", "condizione passo falsa", None
        if decision is None:
            return "NON ESEGUIBILE", "condizione passo non risolvibile", None
    for cond in [entry["job"].get("if", True), *item.get("parent_conditions", [])]:
        verdict = condition(cond, host_os())
        if verdict is not True:
            return "NON ESEGUIBILE", "condizione job/composite falsa o non risolvibile", None
    if item["reason"]:
        return "NON ESEGUIBILE", item["reason"], None
    if "${{" in json.dumps([step, item["env"], item["working_directory"]]):
        return "NON ESEGUIBILE", "espressione Actions non risolvibile", None
    if any("GITHUB_" in str(value) for value in [step.get("run", ""), item["env"]]):
        return "NON ESEGUIBILE", "file/variabili di contesto GitHub non riprodotti", None
    try:
        local_path(root, item["working_directory"])
        if "uses" in step:
            command, reason = action_command(step, root)
            return ("ESEGUIBILE", "", command) if command else ("NON ESEGUIBILE", reason, None)
    except (OSError, ValueError):
        return "NON ESEGUIBILE", "input/percorso locale non disponibile", None
    shell = item["shell"]
    name = shell.split()[0]
    # Una singola invocazione nativa non richiede la shell. Nessun glob,
    # espansione, pipeline o blocco viene tradotto in argv.
    script = step.get("run", "")
    argv = native_command(script)
    if name in SHELLS and argv:
        return "ESEGUIBILE", "invocazione nativa singola, senza espansioni shell", argv
    if name not in SHELLS or not shutil.which(name):
        return "NON ESEGUIBILE", "shell non disponibile o non mappata: " + shell, None
    if shell != name and "{0}" not in shell:
        return "NON ESEGUIBILE", "shell personalizzata senza placeholder {0}", None
    if re.search(r"\b(?:sudo|apt-get|brew|choco)\b", step.get("run", "")):
        return "NON ESEGUIBILE", "provisioning di sistema riservato al runner CI", None
    return "ESEGUIBILE", "", None


def subprocess_result(command: list[str], cwd: Path, env: dict, log: Path, timeout: float) -> dict:
    """Il returncode e l'unica fonte del PASS; log completi fuori dai messaggi."""
    start = time.monotonic()
    result = {"command": command, "duration_seconds": 0, "outcome": "NON ESEGUITO",
              "reason": "", "returncode": None, "log": str(log)}
    try:
        with log.open("w", encoding="utf-8") as stream:
            process = subprocess.run(command, cwd=cwd, env=env, stdout=stream,
                                     stderr=subprocess.STDOUT, timeout=timeout, check=False)
        result.update(returncode=process.returncode, outcome="PASS" if process.returncode == 0 else "FAIL")
        if process.returncode:
            result["reason"] = "processo terminato con codice non zero; vedere log"
    except subprocess.TimeoutExpired:
        result.update(outcome="FAIL", reason="timeout del passo")
    except OSError:
        result["reason"] = "processo non avviabile; verificare eseguibile e directory"
    result["duration_seconds"] = round(time.monotonic() - start, 3)
    return result


def shell_command(item: dict, directory: Path, index: int) -> list[str]:
    """Il programma run intero vive in un file, senza traduzioni della shell."""
    shell = item["shell"]
    name = shell.split()[0]
    path = directory / f"step-{index}.{'ps1' if name in {'pwsh', 'powershell'} else 'sh'}"
    script = item["step"]["run"]
    if name in {"pwsh", "powershell"}:
        script = "$ErrorActionPreference = 'Stop'\n" + script + "\nif (Test-Path variable:LASTEXITCODE) { exit $LASTEXITCODE }\n"
    path.write_text(script, encoding="utf-8", newline="\n")
    if shell == name:
        return [*SHELLS[name], path.as_posix()]
    return [part.replace("{0}", path.as_posix()) for part in shlex.split(shell)]


def git_state(root: Path) -> dict:
    """Non tratta un errore Git come albero pulito."""
    def git(*args):
        result = subprocess.run(["git", "-c", f"safe.directory={root.as_posix()}", *args], cwd=root, capture_output=True, text=True, check=False)
        if result.returncode:
            raise ValueError("impossibile rilevare stato Git")
        return result.stdout.strip()
    return {"sha": git("rev-parse", "HEAD"), "dirty": bool(git("status", "--porcelain", "-uall"))}


def ordered_jobs(jobs: list[dict]) -> list[dict]:
    """Ordine YAML stabile salvo dipendenze needs dichiarate in avanti."""
    pending = list(jobs)
    result = []
    while pending:
        ready = []
        for entry in pending:
            needs = entry["job"].get("needs", [])
            needs = [needs] if isinstance(needs, str) else needs
            keys = {entry["key"].rsplit("/", 1)[0] + "/" + n for n in needs}
            if not any(other["key"] in keys for other in pending):
                ready.append(entry)
        if not ready:
            # I cicli restano rappresentati, ma non possono partire.
            for entry in pending:
                entry["reason"] = "ciclo needs non risolvibile"
            result.extend(pending)
            break
        entry = ready[0]
        pending.remove(entry)
        result.append(entry)
    return result


def execute(plan: dict, root: Path, output: Path, selected: list[str], live: bool) -> tuple[dict, int]:
    """Raccoglie tutti gli esiti; un prerequisito mancante blocca i dipendenti."""
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, **git_state(root), "host": platform.platform(),
               "python": platform.python_version(), "profile": "pr", "selected": selected,
               "services_opt_in": live, "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "workflow_sha256": {p: hashlib.sha256(local_path(root, p).read_bytes()).hexdigest() for p in plan["documents"]},
               "results": []}
    # Solo ambiente minimo: DSN e credenziali della sessione non entrano nei test.
    base = {k: v for k, v in os.environ.items() if k.upper() in {
        "PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "HOME", "USERPROFILE",
        "APPDATA", "LOCALAPPDATA", "RUSTUP_HOME", "CARGO_HOME", "NUMBER_OF_PROCESSORS",
        "INCLUDE", "LIB", "LIBPATH", "VSINSTALLDIR", "VCINSTALLDIR", "VCTOOLSINSTALLDIR",
        "WINDOWSSDKDIR", "WINDOWSSDKVERSION"}}
    temp = output / "tmp"
    temp.mkdir(exist_ok=True)
    base.update(POLICY_ENV, TMP=str(temp), TEMP=str(temp), TMPDIR=str(temp), RUNNER_TEMP=str(temp))
    completed = {}
    for entry in ordered_jobs(plan["jobs"]):
        job_base = base.copy()
        packages = output / ("job-" + hashlib.sha256(entry["id"].encode()).hexdigest()[:16]) / "site-packages"
        packages.mkdir(parents=True, exist_ok=True)
        job_base.update(PIP_TARGET=str(packages), PYTHONPATH=str(packages))
        blocked = ""
        failed = False
        needs = entry["job"].get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        for need in needs:
            dep = entry["key"].rsplit("/", 1)[0] + "/" + need
            if not completed.get(dep, False):
                blocked = "job needs non completato con PASS: " + need
        if entry["job"].get("uses") and not entry["steps"]:
            entry = {**entry, "steps": [{"source": entry["key"], "step": {"uses": entry["job"]["uses"]},
                     "raw": {}, "env": {}, "shell": "", "working_directory": ".", "reason": entry["reason"]}]}
        for item in entry["steps"]:
            step = item["step"]
            status, reason, command = assess(entry, item, root, live)
            row = {"job": entry["id"], "source": item["source"], "matrix": entry["matrix"],
                   "declared_runner": entry["job"].get("runs-on"), "actual_host": host_os(),
                   "run": step.get("run"), "uses": step.get("uses"), "shell": item["shell"],
                   "working_directory": item["working_directory"], "toolchains": entry["toolchains"],
                   "services": entry["services"], "resources": resources(entry, root),
                   "env": {**item["env"], **POLICY_ENV}, "command": command,
                   "duration_seconds": 0, "outcome": "NON ESEGUITO", "reason": reason,
                   "eligibility": status, "returncode": None}
            infrastructure = step.get("uses", "").split("@")[0] == "Swatinem/rust-cache" or item.get("structural", False)
            if selected and not any(s in (entry["id"], entry["key"], entry["key"].rsplit("/", 1)[-1]) for s in selected):
                row["reason"] = "job non selezionato"
            elif status == "SALTATO":
                row["outcome"] = "SALTATO"
            elif status == "ESEGUIBILE":
                verdict = condition(step.get("if", "success()"), host_os(), failed)
                if blocked:
                    row["reason"] = blocked
                elif verdict is not True:
                    row["reason"] = "condizione passo falsa" if verdict is False else "condizione passo non risolvibile"
                else:
                    index = len(receipt["results"])
                    command = command or shell_command(item, output, index)
                    env = {**job_base, **{k: str(v) for k, v in item["env"].items()}, **POLICY_ENV}
                    if step.get("uses", "").split("@")[0] == "dtolnay/rust-toolchain":
                        job_base["RUSTUP_TOOLCHAIN"] = str(step["with"]["toolchain"])
                    row["env"] = env
                    row.update(subprocess_result(command, local_path(root, item["working_directory"]), env,
                               output / f"step-{index}.log", float(step.get("timeout-minutes", entry["job"].get("timeout-minutes", 360))) * 60))
            if row["outcome"] == "FAIL" and step.get("continue-on-error") is not True:
                failed = True
            elif row["outcome"] in {"NON ESEGUITO", "SALTATO"} and not infrastructure and row["reason"] != "condizione passo falsa":
                blocked = "prerequisito non eseguito: " + item["source"]
            receipt["results"].append(row)
            print(f"{row['outcome']}: {entry['id']} / {step.get('name', item['source'])}: {row['reason']}", flush=True)
        completed[entry["key"]] = completed.get(entry["key"], True) and not failed and not blocked
    receipt["end_state"] = git_state(root)
    outcomes = [row["outcome"] for row in receipt["results"]]
    code = 1 if "FAIL" in outcomes else 2 if any(o != "PASS" for o in outcomes) else 0
    receipt["exit_code"] = code
    (output / "ricevuta.json").write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return receipt, code


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--piano", action="store_true")
    modes.add_argument("--esegui", action="store_true")
    parser.add_argument("--job", action="append", default=[])
    parser.add_argument("--profilo", choices=["pr"], default="pr")
    parser.add_argument("--servizi-live", action="store_true", help="abilita i servizi richiesti (mai release)")
    args = parser.parse_args(argv)
    plan = build_plan()
    known = {s for j in plan["jobs"] for s in (j["id"], j["key"], j["key"].rsplit("/", 1)[-1])}
    if any(job not in known for job in args.job):
        parser.error("--job non presente nel piano")
    if args.piano:
        for entry in plan["jobs"]:
            for item in entry["steps"]:
                status, reason, command = assess(entry, item, ROOT, args.servizi_live)
                item.update(eligibility=status, reason_local=reason, command_local=command)
        print(json.dumps(plan, indent=2, ensure_ascii=False))
        return 0
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = ROOT / "target/ci-locale" / stamp
    _, code = execute(plan, ROOT, output, args.job, args.servizi_live)
    print("Ricevuta: " + str(output / "ricevuta.json"))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
