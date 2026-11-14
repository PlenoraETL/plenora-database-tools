"""Single Python policy gate: .python-version owns the supported minor."""
from __future__ import annotations
import argparse
import hashlib
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def declared(root=ROOT):
    value = (root / ".python-version").read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"3\.\d+", value):
        raise ValueError(".python-version must declare one Python minor")
    return value


def reference_errors(path, source, version):
    errors = []
    patterns = (
        r"(?i)python(?:[\s*:=><~!\"'_-]|version|language|minimo|requires|\|){0,35}(3[._]\d+)(?:\.\d+)?",
        r"(?i)(?:cp|abi3-py)(3\d{1,2})\b",
        r"version_info[^\n]{0,30}\(\s*(3\s*,\s*\d+)\s*\)",
    )
    for number, line in enumerate(source.splitlines(), 1):
        for pattern in patterns:
            for match in re.finditer(pattern, line):
                raw = match[1].replace("_", ".").replace(" ", "").replace(",", ".")
                actual = raw if "." in raw else raw[0] + "." + raw[1:]
                if actual != version:
                    errors.append(f"{path}:{number}: Python reference disagrees with .python-version")
        if re.search(r"(?:from|import)\s+(?:tomli|typing_extensions)\b", line):
            errors.append(f"{path}:{number}: obsolete compatibility import")
    return errors


def wheel_errors(version, requires_python, tags, interpreter):
    errors = []
    if requires_python != ">=" + version:
        errors.append("wheel Requires-Python differs from the declared minimum")
    prefix = "cp" + version.replace(".", "") + "-abi3-"
    if not tags or any(not tag.startswith(prefix) for tag in tags):
        errors.append("wheel ABI tag differs from the declared minimum")
    if tuple(map(int, version.split("."))) != tuple(interpreter[:2]):
        errors.append("wheel qualification uses another Python minor")
    return errors


def installed_wheel_errors(package="plenora-database"):
    from importlib.metadata import distribution
    dist = distribution(package)
    tags = [line.removeprefix("Tag: ") for line in (dist.read_text("WHEEL") or "").splitlines() if line.startswith("Tag: ")]
    return wheel_errors(declared(), dist.metadata.get("Requires-Python"), tags, sys.version_info)


def check(root=ROOT, runtime=False):
    import yaml
    version = declared(root)
    errors = []
    if runtime and tuple(map(int, version.split("."))) != sys.version_info[:2]:
        errors.append("the gate must run on the declared Python minor")
    sdk = root / "crates/plenora-database-py"
    project = tomllib.loads((sdk / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    if project["requires-python"] != ">=" + version:
        errors.append("requires-python does not match the declared minimum")
    classifiers = {c.rsplit(" :: ", 1)[-1] for c in project["classifiers"] if re.search(r"Python :: 3\.", c)}
    if classifiers != {version}:
        errors.append("Python classifiers differ from the declared minor")
    cargo = tomllib.loads((sdk / "Cargo.toml").read_text(encoding="utf-8"))
    if {f for f in cargo["dependencies"]["pyo3"]["features"] if f.startswith("abi3-py")} != {"abi3-py" + version.replace(".", "")}:
        errors.append("PyO3 stable ABI differs from the minimum")
    history = json.loads((root / "scripts/python_historical_evidence.json").read_text(encoding="utf-8"))
    names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root).decode().splitlines()
    for name in names:
        if name.startswith(("vendor/", ".plenora-contracts/")):
            continue  # Third-party contracts are not a project interpreter policy.
        path = root / name
        if path.suffix not in {".rs", ".py", ".pyi", ".yml", ".yaml", ".toml", ".md", ".txt", ".json", ".sh"} and path.name != "Dockerfile":
            continue
        try:
            source = path.read_text(encoding="utf-8")
        except UnicodeError:
            continue
        if name in history:
            if hashlib.sha256(source.encode()).hexdigest() != history[name]:
                errors.append(f"historical evidence changed: {name}")
            continue
        errors.extend(reference_errors(name, source, version))
    for path in (root / ".github/workflows").glob("*.yml"):
        for job in yaml.safe_load(path.read_text(encoding="utf-8")).get("jobs", {}).values():
            matrix = job.get("strategy", {}).get("matrix", {})
            if "python" in matrix and matrix["python"] != [version]:
                errors.append(f"{path.name}: Python matrix is not the declared minor only")
            for step in job.get("steps", []):
                if step.get("uses", "").startswith("actions/setup-python@"):
                    config = step.get("with", {})
                    if config.get("python-version-file") != ".python-version" or "python-version" in config:
                        errors.append(f"{path.name}: setup-python must read .python-version")
    installer = (root / "scripts/install_python.sh").read_text(encoding="utf-8")
    release = re.search(r"^release=(.+)$", installer, re.M)
    if not release or not release[1].startswith(version + "."):
        errors.append("container Python source differs from the declared minor")
    if not re.search(r"^sha256=[0-9a-f]{64}$", installer, re.M) or "sha256sum --check" not in installer:
        errors.append("container Python source is not checksum verified")
    return errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", action="store_true")
    failures = check(runtime=parser.parse_args().runtime)
    for failure in failures:
        print(failure)
    print(f"python-policy: {declared()}, {len(failures)} violations")
    raise SystemExit(bool(failures))
