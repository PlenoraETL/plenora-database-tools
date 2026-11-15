#!/usr/bin/env python3
"""Esercita i decoder privati TDS pin senza server, su copia sotto target.

Il sorgente di produzione resta intatto: si aggiunge soltanto un entry point
di prova alla copia. La guardia Checked e letta dal provider, non duplicata.
"""
from pathlib import Path
import json
import os
import shutil
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def main():
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--offline", "--format-version", "1"], cwd=ROOT))
    package = next(p for p in metadata["packages"] if p["name"] == "tiberius-ng")
    source = Path(package["manifest_path"]).parent
    target = ROOT / "target/temporal-tds-wire"
    shutil.copytree(source, target, dirs_exist_ok=True, ignore=shutil.ignore_patterns(".cargo*", "Cargo.lock"))
    original = tomllib.loads((source / "Cargo.toml").read_text())
    node = next(n for n in metadata["resolve"]["nodes"] if n["id"] == package["id"])
    packages = {p["id"]: p for p in metadata["packages"]}
    manifest = '[package]\nname = "tiberius-ng"\nversion = "0.13.1"\nedition = "2021"\n[lib]\nname = "tiberius"\n[workspace]\n[features]\n'
    for feature in sorted(set(original["features"]) | set(node["features"]) | {"winauth", "time", "rust_decimal"}):
        manifest += f'{feature} = []\n'
    declared = dict(original["dependencies"])
    for table in original.get("target", {}).values():
        declared.update(table.get("dependencies", {}))
    for dep in node["deps"]:
        resolved = packages[dep["pkg"]]
        key = next(k for k in declared if k.replace("-", "_") == dep["name"])
        spec = declared[key]
        manifest += f'\n[dependencies.{key}]\nversion = "={resolved["version"]}"\n'
        if "package" in spec:
            manifest += f'package = "{spec["package"]}"\n'
        if not spec.get("default-features", True):
            manifest += 'default-features = false\n'
        manifest += 'features = ' + json.dumps(spec.get("features", [])) + '\n'
    manifest += '\n[dependencies.tokio]\nversion = "=1.53.3"\nfeatures = ["macros", "rt-multi-thread"]\n[[bin]]\nname = "temporal_probe"\npath = "src/temporal_probe.rs"\n'
    (target / "Cargo.toml").write_text(manifest)
    shutil.copyfile(ROOT / "Cargo.lock", target / "Cargo.lock")
    checked = (ROOT / "crates/plenora-db-sqlserver/src/temporal.rs").read_text().split('#[cfg(test)]')[0]
    checked = checked.replace('tiberius::', 'crate::')
    (target / "src/plenora_checked.rs").write_text(checked)
    probe = (ROOT / "scripts/temporal_tds_wire_probe.rs").read_text()
    lib = target / "src/lib.rs"
    lib.write_text(lib.read_text() + '\nmod plenora_checked;\n' + probe)
    (target / "src/temporal_probe.rs").write_text('fn main() {}\n#[tokio::test]\nasync fn temporal_tds_wire() { tiberius::plenora_temporal_wire_probe().await; }\n')
    env = dict(os.environ, CARGO_BUILD_JOBS="4", CARGO_TARGET_DIR=str(ROOT / "target/temporal-tds-build"))
    return subprocess.call(["cargo", "test", "--offline", "--manifest-path", str(target / "Cargo.toml"),
        "--no-default-features", "--features", ",".join(node["features"]), "--bin", "temporal_probe"], cwd=ROOT, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
