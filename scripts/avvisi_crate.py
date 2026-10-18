#!/usr/bin/env python3
"""Inventario offline degli avvisi Rust, derivato dai grafi Cargo di release.

`--aggiorna` raccoglie dalla cache e dai fork; il comando senza opzioni e il
gate. I testi assenti sono registrati, ma non autorizzano la distribuzione.
Il registro versionato permette di verificare gli archivi senza Cargo.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "assurance/registries/avvisi-crate.json"
TEXTS = ROOT / "assurance/licenze-crate"
TARGETS = {"linux": "x86_64-unknown-linux-gnu", "windows": "x86_64-pc-windows-msvc"}
PROFILES = {
    "cli": ("plenora-database-cli", []),
    "cli-sdk": ("plenora-database-cli", ["--no-default-features", "--features", "postgres,mysql,sqlserver"]),
    "cli-full": ("plenora-database-cli", ["--all-features"]),
    "wheel": ("plenora-database-py", []),
    "wheel-db2": ("plenora-database-py", ["--features", "db2"]),
}
NOTICE = re.compile(r"^(LICEN[CS]E|COPYING|NOTICE|COPYRIGHT|UNLICENSE)", re.I)


def digest(data: bytes) -> str:
    """Git puo convertire CRLF: le impronte dei testi normalizzano solo CRLF."""
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def encoded(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def cargo(*args: str) -> str:
    return subprocess.check_output(
        ["cargo", *args, "--offline", "--locked"], cwd=ROOT, encoding="utf-8"
    )


def graph(metadata: dict, package: str, flags: list[str], target: str) -> list[dict]:
    # tree, diversamente dal resolve workspace di metadata, unifica le feature
    # per il prodotto selezionato. Nessun arco dev/build o proc macro host.
    output = cargo("tree", "-p", package, "--target", target, "--edges",
                   "normal,no-proc-macro", "--prefix", "none", "--format", "{p}", *flags)
    keys = set()
    for line in output.splitlines():
        match = re.match(r"^(\S+) v(\S+)", line)
        if not match:
            raise ValueError(f"riga cargo tree non riconosciuta: {line}")
        keys.add(match.groups())
    packages = [p for p in metadata["packages"] if (p["name"], p["version"]) in keys]
    if len(packages) != len(keys):
        raise ValueError("identita Cargo ambigua o assente nel metadata")
    return packages


def package_files(package: dict) -> list[Path]:
    root = Path(package["manifest_path"]).parent
    # Include anche licenze annidate (ring, codice C vendorizzato, ecc.).
    files = {p for p in root.rglob("*") if p.is_file() and NOTICE.match(p.name)}
    if package.get("license_file"):
        path = (root / package["license_file"]).resolve()
        if not path.is_file():
            raise ValueError(f"license-file assente: {package['name']}")
        files.add(path)
    return sorted(files)


def collect(previous: dict) -> tuple[dict, dict[str, bytes]]:
    metadata = json.loads(cargo("metadata", "--format-version", "1"))
    ours = set(metadata["workspace_members"])
    profiles, packages = {}, {}
    for platform, target in TARGETS.items():
        for profile, (package, flags) in PROFILES.items():
            if platform == "windows" and profile == "wheel-db2":
                continue
            selected = graph(metadata, package, flags, target)
            third_party = [p for p in selected if p["id"] not in ours]
            keys = sorted(f"{p['name']}-{p['version']}" for p in third_party)
            profiles[f"{profile}-{platform}"] = {
                "package": package, "target": target, "cargo_features": flags,
                "cargo_profile": "release", "crates": keys,
            }
            packages.update({f"{p['name']}-{p['version']}": p for p in third_party})
    result, contents = {}, {}
    for key, package in sorted(packages.items()):
        root = Path(package["manifest_path"]).parent
        record = {"name": package["name"], "version": package["version"],
                  "license": package["license"], "source": package["source"] or root.relative_to(ROOT).as_posix(),
                  "files": []}
        for path in package_files(package):
            name = path.relative_to(root).as_posix()
            data = path.read_bytes()
            record["files"].append({"file": name, "sha256": digest(data), "origin": "cargo-package"})
            contents[f"{key}/{name}"] = data
        old = previous.get("crates", {}).get(key, {})
        if not record["files"]:
            for entry in old.get("files", []):
                if entry["origin"] == "cargo-package":
                    continue
                data = (TEXTS / key / entry["file"]).read_bytes()
                if digest(data) != entry["sha256"]:
                    raise ValueError(f"{key}: impronta del testo supplementare diversa")
                record["files"].append(entry)
                contents[f"{key}/{entry['file']}"] = data
            if not record["files"]:
                record["missing_reason"] = old.get("missing_reason") or (
                    "Nessun testo di licenza nel pacchetto della cache Cargo; "
                    "upstream non consultato (raccolta offline). Il solo SPDX non e un avviso."
                )
                record["authors"] = package["authors"]
                record["repository"] = package["repository"]
                vcs = root / ".cargo_vcs_info.json"
                if vcs.is_file():
                    record["vcs"] = json.loads(vcs.read_text(encoding="utf-8"))
        result[key] = record
    inputs = {"Cargo.lock", "Cargo.toml"}
    inputs.update(Path(p["manifest_path"]).relative_to(ROOT).as_posix()
                  for p in metadata["packages"] if p["source"] is None)
    return {"schema_version": 1, "profiles": profiles, "crates": result,
            "inputs": {name: digest((ROOT / name).read_bytes()) for name in sorted(inputs)}}, contents


def read_registry() -> dict:
    return json.loads(REGISTRY.read_text(encoding="utf-8"))


def safe_name(name: str) -> bool:
    return bool(name) and "\\" not in name and ":" not in name and all(
        part not in ("", ".", "..") for part in name.split("/")
    )


def validate(registry: dict) -> list[str]:
    errors = []
    used = {key for profile in registry["profiles"].values() for key in profile["crates"]}
    if used != set(registry["crates"]):
        errors.append("registro: crate assenti o obsoleti rispetto ai profili")
    expected = set()
    for key, record in registry["crates"].items():
        if not safe_name(key):
            errors.append("identita crate non valida")
            continue
        if not record["files"] or record.get("missing_reason"):
            errors.append(f"{key}: {record.get('missing_reason', 'nessun testo')}")
        for entry in record["files"]:
            name = entry["file"]
            if not safe_name(name):
                errors.append(f"{key}: percorso avviso non valido")
                continue
            expected.add(f"{key}/{name}")
            path = TEXTS / key / name
            if not path.is_file() or not path.read_bytes().strip() or digest(path.read_bytes()) != entry["sha256"]:
                errors.append(f"{key}/{name}: assente, vuoto o impronta diversa")
    actual = {p.relative_to(TEXTS).as_posix() for p in TEXTS.rglob("*") if p.is_file()}
    if actual != expected:
        errors.append("testi versionati mancanti o non dichiarati dal registro")
    return errors


def bundle(profile: str, registry: dict | None = None) -> dict[str, bytes]:
    registry = registry or read_registry()
    for name, sha in registry["inputs"].items():
        if not safe_name(name) or digest((ROOT / name).read_bytes()) != sha:
            raise ValueError("input Cargo diverso dal registro: rieseguire il gate offline")
    keys = sorted(registry["crates"]) if profile == "source" else registry["profiles"][profile]["crates"]
    selected = {key: registry["crates"][key] for key in keys}
    files = {"LICENSE": (ROOT / "LICENSE").read_bytes()}
    for key, record in selected.items():
        if not record["files"] or record.get("missing_reason"):
            raise ValueError(f"{key}: testo di licenza mancante; distribuzione bloccata")
        for entry in record["files"]:
            if not safe_name(key) or not safe_name(entry["file"]):
                raise ValueError("percorso avviso non valido")
            data = (TEXTS / key / entry["file"]).read_bytes()
            if not data.strip() or digest(data) != entry["sha256"]:
                raise ValueError(f"{key}: testo vuoto o impronta diversa")
            files[f"LICENSES/crate-rust/{key}/{entry['file']}"] = data
    files["LICENSES/crate-rust/CRATE.json"] = encoded({"profile": profile, "crates": selected})
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggiorna", action="store_true")
    parser.add_argument("--solo-registro", action="store_true")
    args = parser.parse_args()
    try:
        old = read_registry() if REGISTRY.exists() else {}
        errors = []
        if not args.solo_registro:
            current, contents = collect(old)
            if args.aggiorna:
                REGISTRY.parent.mkdir(parents=True, exist_ok=True)
                REGISTRY.write_bytes(encoded(current))
                for name, data in contents.items():
                    path = TEXTS / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(data)
                old = current
            elif old != current:
                errors.append("registro diverso dai grafi/testi Cargo offline: rieseguire --aggiorna")
        errors += validate(old)
        print(f"{len(old['crates'])} crate; {sum(len(c['files']) for c in old['crates'].values())} avvisi; {len(errors)} errori")
        for error in errors:
            print(error)
        return bool(errors)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"avvisi crate: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
