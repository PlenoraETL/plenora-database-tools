#!/usr/bin/env python3
"""Deriva la verifica PR dai workflow; nessun comando di gate duplicato.

La versione Python e dichiarata in .python-version; PyYAML in
requirements-ci-locale.txt. Il piano diagnostica anche host non predisposti.
Exit 1: almeno un fallimento; 2: copertura incompleta; 0: tutti i gate PASS.
Non installa toolchain. I checkout esterni online possono usare HTTPS. I filtri PR sono conservati:
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
import uuid
from pathlib import Path

try:
    import yaml
except ImportError:
    print("CI locale: manca PyYAML. Installare: python -m pip install -r requirements-ci-locale.txt", file=sys.stderr)
    raise SystemExit(2) from None

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
    "python": [sys.executable],
}
PRECONDITIONS = (
    (r"\b(?:sudo|apt-get|brew|choco)\b", "provisioning", "provisioning di sistema riservato alla CI"),
    (r"\bgdal-config\b|--features[ =]+gdal-backend|--require-filegdb", "tool", "gdal-config"),
    (r"\bdocker\b", "tool", "docker"),
    (r"\bpwsh\b", "tool", "pwsh"),
    (r"\bcargo\b", "tool", "cargo"),
    (r"\bgit\b", "tool", "git"),
    (r"scripts/compare_historical_readers\.py.*--old", "historical", "scripts/compare_historical_readers.py"),
)


def policy_env(offline):
    return {"CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0",
            "CARGO_NET_OFFLINE": "true" if offline else "false",
            "PIP_NO_INDEX": "1" if offline else "0", "PIP_DISABLE_PIP_VERSION_CHECK": "1"}


def preconditions(job, step, root, command, offline):
    """Controlli prima del gate. Nessuna reinterpretazione del suo exit code."""
    step = dict(step)
    step["mapping"] = ACTIONS.get(step.get("uses", "").split("@")[0])
    step.setdefault("with", {})
    if step["mapping"] == "python":
        wanted = str(step["with"].get("python-version", ""))
        if "python-version-file" in step["with"]:
            wanted = local_path(root, step["with"]["python-version-file"]).read_text(encoding="utf-8").strip()
        step["toolchain"] = {"python": wanted}
    direct = step.get("run") or ""
    if offline:
        # Una wheelhouse deve essere esplicita e locale; la sola cache pip non basta.
        for line in command.splitlines():
            if re.search(r"\bpip(?:3)?\s+install\b", line):
                links = re.search(r"--find-links(?:=|\s+)([\w./-]+)", line)
                local = links and local_path(root, links[1]).is_dir()
                if not ("--no-index" in line and local and "http" not in line):
                    return "offline: pip install senza wheelhouse locale esplicita"
        if re.search(r"\b(?:curl|wget)\b|urlretrieve\(|urlopen\(|requests\.(?:get|post)\(|\bgit\s+(?:fetch|clone|pull)\b", command):
            return "offline: download richiesto dal comando/script"
        if re.search(r"\bcargo\s+(?:\+\S+\s+)?(?:build|test|check|clippy|run|fetch|metadata|install|update|bench)\b", command):
            return "offline: risoluzione Cargo; completezza della cache non certificata"
    for pattern, kind, value in PRECONDITIONS:
        if not re.search(pattern, direct, re.S):
            continue
        if kind == "provisioning":
            return value
        if kind == "tool" and not shutil.which(value):
            return f"strumento di sistema assente: {value}"
        if kind == "historical":
            source = local_path(root, value)
            if not source.is_file():
                return f"precondizione storica assente: {value}"
            pin = re.search(r"BASELINE_REVISION\s*=\s*['\"]([^'\"]+)", source.read_text(encoding="utf-8"))
            if not pin:
                return "revisione storica non dichiarata"
            try:
                git(root, "rev-parse", "--verify", pin[1] + "^{commit}")
            except (OSError, subprocess.SubprocessError):
                return f"storia/tag git richiesti assenti: {pin[1]}"
            for name in re.findall(r"--(?:old|new)\s+([^\s]+)", direct):
                if not local_path(root, name).is_file():
                    return f"binario storico/corrente assente: {name}"
    if step.get("mapping") == "checkout" and not step["with"].get("path") and str(step["with"].get("fetch-depth")) == "0":
        if git(root, "rev-parse", "--is-shallow-repository") == "true":
            return "storia git completa richiesta; clone shallow"
    if step.get("mapping") == "python":
        wanted = step["toolchain"].get("python", "")
        if re.fullmatch(r"\d+\.\d+(?:\.\d+)?", wanted) and tuple(map(int, wanted.split('.'))) != sys.version_info[:len(wanted.split('.'))]:
            return f"Python {wanted} richiesto; host {platform.python_version()}"
    if step.get("mapping") == "rust":
        if not shutil.which("rustup"):
            return "strumento di sistema assente: rustup"
        wanted = step["with"].get("toolchain", "")
        try:
            installed = subprocess.check_output(["rustup", "toolchain", "list"], text=True, stderr=subprocess.PIPE)
            if not any(line.split()[0] == wanted or line.startswith(wanted + "-") for line in installed.splitlines()):
                return f"toolchain Rust assente: {wanted}"
            components = [c.strip().removesuffix("-preview") for c in step["with"].get("components", "").split(",") if c.strip()]
            if components:
                present = subprocess.check_output(["rustup", "component", "list", "--installed", "--toolchain", wanted],
                                                  text=True, stderr=subprocess.PIPE)
                for component in components:
                    if not any(line == component or line.startswith(component + "-") for line in present.splitlines()):
                        return f"componente Rust assente: {component} ({wanted})"
        except (OSError, subprocess.SubprocessError):
            return "toolchain Rust non verificabile"
    return ""
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
    plan = {"schema": 2, "profile": "pr", "jobs": [], "documents": {}}
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


def checkout(root, destination, source, ref):
    """Clone indipendente: non modifica la sorgente e non sovrascrive destinazioni."""
    root = Path(root)
    target = local_path(root, destination)
    if target == root:
        raise ValueError("destinazione checkout non separata")
    existing = target.exists()
    if existing:
        if not (target / ".git").is_dir() or git(target, "remote", "get-url", "origin") != source:
            raise ValueError("destinazione checkout non appartenente alla sorgente")
        if git(target, "status", "--porcelain"):
            raise ValueError("destinazione checkout modificata")
    else:
        clone_env = os.environ.copy()
        if Path(source).is_dir():
            # La sorgente locale e esplicitamente configurata dall'operatore.
            # upload-pack e un processo figlio: -c sul clone non basta.
            trusted = [Path(source).resolve().as_posix(),
                       git(Path(source), "rev-parse", "--absolute-git-dir").replace("\\", "/")]
            offset = int(clone_env.get("GIT_CONFIG_COUNT", "0"))
            for index, directory in enumerate(trusted, offset):
                clone_env[f"GIT_CONFIG_KEY_{index}"] = "safe.directory"
                clone_env[f"GIT_CONFIG_VALUE_{index}"] = directory
            clone_env["GIT_CONFIG_COUNT"] = str(offset + len(trusted))
        subprocess.run(["git", "clone", "--no-hardlinks", "--no-checkout", "--", source, str(target)],
                       check=True, env=clone_env)
    expected = git(target, "rev-parse", "--verify", ref + "^{commit}")
    if re.fullmatch(r"[0-9a-fA-F]{40}", ref) and expected.lower() != ref.lower():
        raise ValueError("SHA richiesto non corrispondente")
    if not existing:
        subprocess.run(["git", "-C", str(target), "checkout", "--detach", expected], check=True)
    actual = git(target, "rev-parse", "HEAD")
    if actual != expected:
        raise ValueError("SHA checkout non corrispondente")
    print(json.dumps({"requested_ref": ref, "expected_sha": expected, "actual_sha": actual}))


def action_command(step: dict, root: Path, offline: bool = False) -> tuple[list[str] | None, str]:
    """Le azioni setup verificano il tool presente; non simulano installazioni."""
    use = step.get("uses", "").split("@")[0]
    kind = ACTIONS.get(use)
    inputs = step.get("with", {})
    if kind == "checkout":
        if any(k in inputs for k in ("repository", "ref", "path")):
            if set(inputs) - {"repository", "ref", "path", "fetch-depth"}:
                return None, "input checkout non mappati"
            repository, ref, destination = (str(inputs.get(k, "")) for k in ("repository", "ref", "path"))
            if (repository and not re.fullmatch(r"[\w.-]+/[\w.-]+", repository)) or not re.fullmatch(r"[\w./-]+", ref) or ref.startswith("-") or not destination:
                return None, "repository/ref/path checkout non risolti"
            target = local_path(root, destination)
            if target == root:
                return None, "destinazione checkout non separata"
            key = "CI_LOCALE_SORGENTE_" + re.sub(r"[^A-Z0-9]", "_", repository.rsplit("/", 1)[-1].upper())
            source = os.environ.get(key) if repository else str(root)
            if source:
                source = str(Path(source).resolve())
                if not Path(source).is_dir():
                    return None, "sorgente checkout locale assente: " + key
            elif offline:
                return None, "offline: checkout senza sorgente locale " + key
            else:
                source = "https://github.com/" + repository + ".git"
            return [sys.executable, "-c", "import runpy,sys; runpy.run_path(sys.argv[1])['checkout'](*sys.argv[2:])",
                    str(Path(__file__).resolve()), str(root), destination, source, ref], ""
        if set(inputs) - {"fetch-depth"}:
            return None, "checkout con input: richiede ambiente Actions o checkout separato"
        return ["git", "-c", f"safe.directory={root.as_posix()}", "rev-parse", "HEAD"], ""
    if kind == "python":
        wanted = str(inputs.get("python-version", ""))
        if "python-version-file" in inputs:
            wanted = local_path(root, inputs["python-version-file"]).read_text(encoding="utf-8").strip()
        if not re.fullmatch(r"\d+\.\d+(?:\.\d+)?", wanted) or tuple(map(int, wanted.split('.'))) != sys.version_info[:len(wanted.split('.'))]:
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


def assess(entry: dict, item: dict, root: Path, live: bool = False, offline: bool = False) -> tuple[str, str, list | None]:
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
    try:
        script = step.get("run", "")
        for name in re.findall(r"(?:scripts|\.github/scripts)/[\w/.-]+\.(?:py|sh|ps1)", script):
            path = local_path(root, name)
            if path.is_file():
                script += "\n" + path.read_text(encoding="utf-8")
        missing = preconditions(entry, step, root, script, offline)
        if missing:
            return "NON ESEGUIBILE", missing, None
    except (OSError, ValueError, subprocess.SubprocessError):
        return "NON ESEGUIBILE", "precondizione locale non verificabile", None
    needs = resources(entry, root)
    if needs and not live:
        return "NON ESEGUIBILE", "; ".join(needs) + ": richiede --servizi-live", None
    if needs and not shutil.which("docker"):
        return "NON ESEGUIBILE", "servizio Docker assente dal PATH", None
    if entry["services"] or entry["job"].get("container"):
        return "NON ESEGUIBILE", "provisioning services/container Actions non mappato", None
    runner = str(entry["job"].get("runs-on", ""))
    target = next((os_name for token, os_name in (("windows", "Windows"), ("ubuntu", "Linux"), ("macos", "macOS")) if token in runner), None)
    if target != host_os():
        return "NON ESEGUIBILE", f"piattaforma richiesta {runner}; host {host_os()}", None
    step_if = step.get("if", True)
    if not any(word in str(step_if) for word in ("success()", "failure()", "always()", "cancelled()")):
        decision = condition(step_if, host_os())
        if decision is False:
            return "NON ESEGUIBILE", "condizione passo falsa", None
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
    if any(re.search(r"GITHUB_(?!OUTPUT\b)\w+", str(value)) for value in [step.get("run", ""), item["env"]]):
        return "NON ESEGUIBILE", "file/variabili di contesto GitHub non riprodotti", None
    try:
        if not local_path(root, item["working_directory"]).is_dir():
            return "NON ESEGUIBILE", "working-directory assente", None
        if "uses" in step:
            command, reason = action_command(step, root, offline)
            return ("ESEGUIBILE", "", command) if command else ("NON ESEGUIBILE", reason, None)
    except (OSError, ValueError):
        return "NON ESEGUIBILE", "input/percorso locale non disponibile", None
    shell = item["shell"]
    name = shell.split()[0]
    # Anche i comandi portabili conservano la precondizione sulla shell.
    # Nessun glob, espansione, pipeline o blocco viene tradotto in argv.
    script = step.get("run", "")
    argv = native_command(script)
    if name not in SHELLS or not shutil.which(SHELLS[name][0]):
        return "NON ESEGUIBILE", "shell non disponibile o non mappata: " + shell, None
    if shell != name and "{0}" not in shell:
        return "NON ESEGUIBILE", "shell personalizzata senza placeholder {0}", None
    if shell == name and argv:
        return "ESEGUIBILE", "invocazione nativa singola, senza espansioni shell", argv
    if re.search(r"\b(?:sudo|apt-get|brew|choco)\b", step.get("run", "")):
        return "NON ESEGUIBILE", "provisioning di sistema riservato al runner CI", None
    return "ESEGUIBILE", "", None


def subprocess_result(command: list[str], cwd: Path, env: dict, log: Path, timeout: float) -> dict:
    """Il returncode e l'unica fonte del PASS; log completi fuori dai messaggi."""
    start = time.monotonic()
    result = {"command": command, "duration_seconds": 0, "status": "NON ESEGUITO",
              "reason": "", "exit_code": None, "log": None}
    try:
        with log.open("w", encoding="utf-8") as stream:
            process = subprocess.run(command, cwd=cwd, env=env, stdout=stream,
                                     stderr=subprocess.STDOUT, timeout=timeout, check=False)
        result.update(log=str(log), exit_code=process.returncode, status="PASS" if process.returncode == 0 else "FAIL")
        if process.returncode == 0 and re.search(r"\b(?:skipping|skipped|NON ESEGUITO)\b", log.read_text(encoding="utf-8", errors="replace"), re.I):
            result.update(status="NON ESEGUITO", reason="il log dichiara prove saltate; consultare il log")
        if process.returncode:
            result["reason"] = "processo terminato con codice non zero; vedere log"
    except subprocess.TimeoutExpired:
        result.update(status="FAIL", reason="timeout del passo", log=str(log))
    except OSError:
        result["reason"] = "processo non avviabile; verificare eseguibile e directory"
    result["duration_seconds"] = round(time.monotonic() - start, 3)
    return result


def shell_command(item: dict, directory: Path, index: int) -> list[str]:
    """Il programma run intero vive in un file, senza traduzioni della shell."""
    shell = item["shell"]
    name = shell.split()[0]
    suffix = "ps1" if name in {"pwsh", "powershell"} else "py" if name == "python" else "sh"
    path = directory / f"step-{index}.{suffix}"
    script = item["step"]["run"]
    if name in {"bash", "sh"}:
        interpreter = shlex.quote(Path(sys.executable).as_posix())
        prefix = f'python() {{ {interpreter} "$@"; }}\npython3() {{ {interpreter} "$@"; }}\n'
        script = prefix + ("export -f python python3\n" if name == "bash" else "") + script
    if name in {"pwsh", "powershell"}:
        interpreter = sys.executable.replace("'", "''")
        script = ("$ErrorActionPreference = 'Stop'\n" +
                  f"function python {{ & '{interpreter}' @args }}\nfunction python3 {{ & '{interpreter}' @args }}\n" +
                  script + "\nif (Test-Path variable:LASTEXITCODE) { exit $LASTEXITCODE }\n")
    path.write_text(script, encoding="utf-8", newline="\n")
    if shell == name:
        command = [*SHELLS[name], path.as_posix()]
    else:
        command = [part.replace("{0}", path.as_posix()) for part in shlex.split(shell)]
    command[0] = shutil.which(command[0]) or command[0]
    return command


def git(root, *args):
    return subprocess.check_output(["git", "-c", f"safe.directory={root.as_posix()}", *args], cwd=root, text=True, stderr=subprocess.PIPE).strip()


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


def execute(plan: dict, root: Path, output: Path, selected: list[str], live: bool, offline: bool = False, timeout: float = 1800) -> tuple[dict, int]:
    """Raccoglie tutti gli esiti; un prerequisito mancante blocca i dipendenti."""
    policy = policy_env(offline)
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 2, **git_state(root), "host": platform.platform(),
               "python": platform.python_version(), "profile": "pr", "selected_jobs": selected, "network_mode": "offline" if offline else "online", "overrides": policy, "jobs": [],
               "services_opt_in": live, "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "sources": {p: hashlib.sha256(local_path(root, p).read_bytes()).hexdigest() for p in plan["documents"]},
               "steps": []}
    # Solo ambiente minimo: DSN e credenziali della sessione non entrano nei test.
    base = {k: v for k, v in os.environ.items() if k.upper() in {
        "PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "HOME", "USERPROFILE",
        "APPDATA", "LOCALAPPDATA", "RUSTUP_HOME", "CARGO_HOME", "NUMBER_OF_PROCESSORS",
        "INCLUDE", "LIB", "LIBPATH", "VSINSTALLDIR", "VCINSTALLDIR", "VCTOOLSINSTALLDIR",
        "WINDOWSSDKDIR", "WINDOWSSDKVERSION"}}
    temp = output / "tmp"
    temp.mkdir(exist_ok=True)
    base.update(policy, TMP=str(temp), TEMP=str(temp), TMPDIR=str(temp), RUNNER_TEMP=str(temp))
    # Include composite definitions as well as workflow documents.
    for entry in plan["jobs"]:
        for item in entry["steps"]:
            use = item["step"].get("uses", "")
            if use.startswith("./"):
                for name in ("action.yml", "action.yaml"):
                    source = local_path(root, use) / name
                    if source.is_file():
                        receipt["sources"][source.relative_to(root).as_posix()] = hashlib.sha256(source.read_bytes()).hexdigest()
    def save():
        receipt["counts"] = {status: sum(row["status"] == status for row in receipt["steps"])
                             for status in {row["status"] for row in receipt["steps"]}}
        (output / "ricevuta.json").write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
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
        job_record = {"id": entry["id"], "matrix": entry["matrix"], "needs": needs, "reason": entry["reason"]}
        receipt["jobs"].append(job_record)
        records = []
        toolchain = {}
        context = {}
        for item in entry["steps"]:
            item = {**item, "step": expand(item["step"], context), "env": expand(item["env"], context),
                    "working_directory": expand(item["working_directory"], context)}
            step = item["step"]
            status, reason, command = assess(entry, item, root, live, offline)
            mapping = ACTIONS.get(step.get("uses", "").split("@")[0])
            if mapping == "rust":
                toolchain["rust"] = step.get("with", {})
            elif mapping == "python":
                inputs = step.get("with", {})
                toolchain["python"] = str(inputs.get("python-version", ""))
                if "python-version-file" in inputs:
                    try:
                        toolchain["python"] = local_path(root, inputs["python-version-file"]).read_text(encoding="utf-8").strip()
                    except (OSError, ValueError):
                        pass  # assess has recorded the missing prerequisite.
            row = {"job": entry["id"], "source": item["source"], "matrix": entry["matrix"],
                   "declared_runner": entry["job"].get("runs-on"), "actual_host": host_os(),
                   "run": step.get("run"), "uses": step.get("uses"), "shell": item["shell"],
                   "working_directory": item["working_directory"], "toolchain": dict(toolchain),
                   "services": entry["services"], "resources": resources(entry, root),
                   "env": item["env"], "command": step.get("run") or step.get("uses"),
                   "duration_seconds": 0, "status": "NON ESEGUITO", "reason": reason,
                   "eligibility": status, "exit_code": None, "log": None,
                   "raw": item.get("raw", {}), "name": step.get("name", item["source"]),
                   "with": step.get("with", {}), "if": step.get("if", "success()"),
                   "continue_on_error": step.get("continue-on-error", False),
                   "container": entry["job"].get("container"), "environment": {**item["env"], **policy}, "condition_false": False}
            infrastructure = step.get("uses", "").split("@")[0] == "Swatinem/rust-cache" or item.get("structural", False)
            if selected and not any(s in (entry["id"], entry["key"], entry["key"].rsplit("/", 1)[-1]) for s in selected):
                row["reason"] = "job non selezionato"
            elif condition(step.get("if", "success()"), host_os(), failed) is False:
                row.update(reason="condizione passo falsa", condition_false=True)
            elif status == "ESEGUIBILE":
                verdict = condition(step.get("if", "success()"), host_os(), failed)
                if blocked:
                    row["reason"] = blocked
                elif verdict is not True:
                    row["reason"] = "condizione passo falsa" if verdict is False else "condizione passo non risolvibile"
                else:
                    index = len(receipt["steps"])
                    command = command or shell_command(item, output, index)
                    env = {**job_base, **{k: str(v) for k, v in item["env"].items()}, **policy}
                    output_file = output / f"step-{index}.outputs"
                    output_file.write_text("", encoding="utf-8")
                    env["GITHUB_OUTPUT"] = output_file.as_posix()
                    if step.get("uses", "").split("@")[0] == "dtolnay/rust-toolchain":
                        job_base["RUSTUP_TOOLCHAIN"] = str(step["with"]["toolchain"])
                    row["environment"] = env
                    row.update(subprocess_result(command, local_path(root, item["working_directory"]), env,
                               output / f"step-{index}.log", min(timeout, float(step.get("timeout-minutes", entry["job"].get("timeout-minutes", 360))) * 60)))
                    if row["status"] == "PASS" and step.get("id"):
                        for line in output_file.read_text(encoding="utf-8-sig").splitlines():
                            key, separator, value = line.partition("=")
                            if not separator or not re.fullmatch(r"[\w-]+", key):
                                row.update(status="FAIL", reason="formato GITHUB_OUTPUT non supportato", exit_code=1)
                                break
                            context[f"steps.{step['id']}.outputs.{key}"] = value
            if row["status"] == "FAIL" and step.get("continue-on-error") is not True:
                failed = True
            elif row["status"] in {"NON ESEGUITO", "SALTATO"} and not infrastructure and row["reason"] != "condizione passo falsa":
                blocked = "prerequisito non eseguito: " + item["source"]
            receipt["steps"].append(row)
            records.append(row)
            save()
            print(f"{row['status']}: {entry['id']} / {step.get('name', item['source'])}: {row['reason']}", flush=True)
        job_record["status"] = "FAIL" if any(r["status"] == "FAIL" for r in records) else "PASS" if records and all(r["status"] == "PASS" for r in records) else "NON ESEGUITO"
        completed[entry["key"]] = completed.get(entry["key"], True) and job_record["status"] == "PASS"
    final = git_state(root)
    receipt.update(final_sha=final["sha"], final_dirty=final["dirty"], finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    outcomes = [row["status"] for row in receipt["steps"]]
    code = 1 if "FAIL" in outcomes else 2 if not receipt["jobs"] or any(j["status"] != "PASS" for j in receipt["jobs"]) else 0
    receipt["exit_code"] = code
    save()
    return receipt, code


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--piano", action="store_true")
    modes.add_argument("--esegui", action="store_true")
    parser.add_argument("--job", action="append", default=[])
    parser.add_argument("--profilo", choices=["pr"], default="pr")
    parser.add_argument("--servizi-live", action="store_true", help="abilita i servizi richiesti (mai release)")
    parser.add_argument("--offline", action="store_true", help="non eseguire passi che richiedono rete")
    parser.add_argument("--timeout", type=int, default=1800, help="secondi per passo")
    args = parser.parse_args(argv)
    plan = build_plan(ROOT)
    plan["network_mode"] = "offline" if args.offline else "online"
    known = {s for j in plan["jobs"] for s in (j["id"], j["key"], j["key"].rsplit("/", 1)[-1])}
    if any(job not in known for job in args.job):
        parser.error("--job non presente nel piano")
    if args.piano:
        for entry in plan["jobs"]:
            for item in entry["steps"]:
                status, reason, command = assess(entry, item, ROOT, args.servizi_live, args.offline)
                if args.job and not any(j in (entry["id"], entry["key"], entry["key"].rsplit("/", 1)[-1]) for j in args.job):
                    status, reason = "NON ESEGUIBILE", "job non selezionato"
                item.update(availability=status, availability_reason=reason, command_local=command)
        print(json.dumps(plan, indent=2, ensure_ascii=False))
        return 0
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = ROOT / "target/ci-locale" / (stamp + "-" + uuid.uuid4().hex[:8])
    _, code = execute(plan, ROOT, output, args.job, args.servizi_live, args.offline, args.timeout)
    print("Ricevuta: " + str(output / "ricevuta.json"))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
