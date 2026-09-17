#!/usr/bin/env python3
"""Combine artifact discovery with the source dependency inventory.

The inventory is a union of workspace/fuzz lock graphs and pinned Python
qualification requirements, not a claim that every dependency is in each
binary. Wheel runtime requirements are checked separately and fail closed.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from email.parser import BytesParser
import hashlib
import json
from pathlib import Path
import re
import tomllib
from urllib.parse import quote
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def property_value(name: str, value: str) -> dict[str, str]:
    return {"name": "plenora:" + name, "value": value}


def cargo_inventory(root: Path) -> tuple[dict, dict]:
    components, edges = {}, {}
    manifest = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    patches = manifest.get("patch", {}).get("crates-io", {})
    for relative in ("Cargo.lock", "fuzz/Cargo.lock"):
        packages = tomllib.loads((root / relative).read_text(encoding="utf-8"))["package"]
        refs = {}
        for package in packages:
            name, version = package["name"], package["version"]
            source = package.get("source", "local")
            ref = f"cargo:{name}@{version}:{source}"
            refs[(name, version, source)] = ref
            component = components.setdefault(ref, {
                "type": "library", "bom-ref": ref, "name": name, "version": version,
                "purl": f"pkg:cargo/{quote(name)}@{quote(version)}",
                "properties": [],
            })
            component["properties"].append(property_value("source-lock", relative))
            if "checksum" in package:
                component["hashes"] = [{"alg": "SHA-256", "content": package["checksum"]}]
            if source == "local":
                component["properties"].append(property_value("source-kind", "workspace-or-patched"))
                if name in patches and "path" in patches[name]:
                    path = root / patches[name]["path"]
                    digest = hashlib.sha256()
                    files = [path / "Cargo.toml", *sorted((path / "src").rglob("*.rs"))]
                    for file in files:
                        digest.update(file.relative_to(path).as_posix().encode() + b"\0")
                        content = file.read_bytes().replace(b"\r\n", b"\n")
                        digest.update(hashlib.sha256(content).digest())
                    component["properties"].extend([
                        property_value("patch-path", patches[name]["path"]),
                        property_value("patch-source-sha256", digest.hexdigest()),
                    ])
            edges.setdefault(ref, set())
        for package in packages:
            ref = refs[(package["name"], package["version"], package.get("source", "local"))]
            for dependency in package.get("dependencies", []):
                parts = dependency.split()
                matches = [value for (name, version, source), value in refs.items()
                           if name == parts[0] and (len(parts) == 1 or version == parts[1])
                           and (len(parts) < 3 or source == parts[2].strip("()"))]
                if len(matches) != 1:
                    raise ValueError(f"ambiguous Cargo dependency in {relative}: {dependency}")
                edges[ref].add(matches[0])
    return components, edges


def python_inventory(root: Path) -> dict:
    components = {}
    pattern = re.compile(r"([A-Za-z0-9_.-]+)(?:\[[^]]+\])?==([^;\s]+)(?:\s*;.*)?$")
    for path in sorted(root.glob("requirements*.txt")):
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.split("#", 1)[0].strip()
            if not line or line.startswith(("-r ", "-c ")):
                continue
            match = pattern.fullmatch(line)
            if not match:
                raise ValueError(f"unresolved Python requirement in {path.name}")
            name = re.sub(r"[-_.]+", "-", match[1]).lower()
            ref = f"pkg:pypi/{quote(name)}@{quote(match[2])}"
            component = components.setdefault(ref, {
                "type": "library", "bom-ref": ref, "purl": ref,
                "name": name, "version": match[2], "properties": [
                    property_value("scope", "qualification-environment; not wheel runtime"),
                ],
            })
            component["properties"].append(property_value("requirement", path.name + ": " + line))
    return components


def wheel_inventory(dist: Path, version: str) -> dict:
    components = {}
    wheels = sorted(dist.glob("*.whl"))
    if not wheels:
        raise ValueError("release wheels missing")
    for wheel in wheels:
        with zipfile.ZipFile(wheel) as archive:
            metadata_files = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(metadata_files) != 1:
                raise ValueError("wheel metadata ambiguous")
            metadata = BytesParser().parsebytes(archive.read(metadata_files[0]))
        if metadata["Version"] != version or metadata["Name"] != "plenora-database":
            raise ValueError("wheel identity differs from source")
        # The SDK has no mandatory Python runtime dependencies. If one is
        # introduced, its resolved graph must be added before publication.
        if metadata.get_all("Requires-Dist", []):
            raise ValueError("wheel runtime dependencies require a resolved SBOM graph")
        ref = "wheel:" + wheel.name
        components[ref] = {
            "type": "library", "bom-ref": ref, "name": wheel.name, "version": version,
            "hashes": [{"alg": "SHA-256", "content": hashlib.sha256(wheel.read_bytes()).hexdigest()}],
            "properties": [property_value("python-runtime-dependencies", "none (wheel METADATA)")],
        }
    return components


def render(root: Path, dist: Path, artifact_bom: dict) -> dict:
    if artifact_bom.get("bomFormat") != "CycloneDX":
        raise ValueError("artifact SBOM must be CycloneDX")
    bom = deepcopy(artifact_bom)
    version = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))["workspace"]["package"]["version"]
    cargo, edges = cargo_inventory(root)
    python = python_inventory(root)
    wheels = wheel_inventory(dist, version)
    supplied = {c["bom-ref"]: c for c in bom.get("components", [])}
    for group in (cargo, python, wheels):
        if supplied.keys() & group.keys():
            raise ValueError("duplicate SBOM component reference")
        supplied.update(group)
    root_ref = "plenora-release:" + version
    bom.setdefault("metadata", {})["component"] = {
        "type": "application", "bom-ref": root_ref, "name": "plenora-database-tools", "version": version,
        "properties": [property_value("scope", "source workspace/fuzz lock union, Python qualification pins and release artifact discovery; not per-binary reachability or an OS inventory")],
    }
    bom["components"] = [supplied[ref] for ref in sorted(supplied)]
    # Native scanner edges remain valid; the inventory root identifies the
    # aggregate. Cargo edges represent the lock graph, including build/dev.
    dependencies = {d["ref"]: set(d.get("dependsOn", [])) for d in bom.get("dependencies", [])}
    dependencies.update(edges)
    dependencies[root_ref] = set(supplied)
    bom["dependencies"] = [{"ref": ref, "dependsOn": sorted(values)} for ref, values in sorted(dependencies.items())]
    known = set(supplied) | {root_ref}
    if any(ref not in known or not values <= known for ref, values in dependencies.items()):
        raise ValueError("dangling SBOM dependency reference")
    return bom


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--artifact-sbom", type=Path)
    mode.add_argument("--check", type=Path)
    parser.add_argument("--dist", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.check:
        validate(ROOT, args.dist, json.loads(args.check.read_text(encoding="utf-8")))
        print("SBOM: source graphs and wheel metadata covered")
        return 0
    if args.output is None:
        parser.error("--output is required when generating")
    bom = render(ROOT, args.dist, json.loads(args.artifact_sbom.read_text(encoding="utf-8")))
    args.output.write_text(json.dumps(bom, indent=2) + "\n", encoding="utf-8")
    print(f"SBOM: {len(bom['components'])} components; Cargo and Python coverage verified")
    return 0


def validate(root: Path, dist: Path, bom: dict) -> None:
    version = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))["workspace"]["package"]["version"]
    if bom.get("bomFormat") != "CycloneDX" or bom["metadata"]["component"]["version"] != version:
        raise ValueError("SBOM identity mismatch")
    cargo, edges = cargo_inventory(root)
    expected = cargo | python_inventory(root) | wheel_inventory(dist, version)
    actual = {c["bom-ref"]: c for c in bom["components"]}
    if len(actual) != len(bom["components"]):
        raise ValueError("duplicate SBOM component reference")
    if any(actual.get(ref) != component for ref, component in expected.items()):
        raise ValueError("SBOM dependency inventory incomplete or stale")
    dependencies = {d["ref"]: set(d.get("dependsOn", [])) for d in bom["dependencies"]}
    if any(dependencies.get(ref) != values for ref, values in edges.items()):
        raise ValueError("SBOM Cargo graph incomplete or stale")


if __name__ == "__main__":
    raise SystemExit(main())
