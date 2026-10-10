#!/usr/bin/env python3
"""Un consumatore esterno di `plenora-db-oracle` riceve il fork, non l'upstream.

Cargo applica `[patch]` solo dal workspace radice. Chi usa database-tools
come dipendenza — per path o per git — e la radice del **proprio**
workspace: un fork entrato con `[patch.crates-io]` per lui non esiste, e
riceverebbe `oracle-rs` da crates.io senza il delta, senza alcun errore.

La prova costruisce quel consumatore: un crate fuori dal workspace, in una
cartella temporanea, che dipende da `crates/plenora-db-oracle` per path.
`cargo metadata` deve risolvere la libreria `oracle_rs` sul manifest di
`vendor/oracle-rs`, senza sorgente di registro. Con `--build` il consumatore
viene anche compilato: `plenora-db-oracle` legge il marcatore del fork, quindi
contro l'upstream la compilazione fallirebbe invece di produrre un driver
diverso.

Il `Cargo.lock` del workspace viene copiato nel consumatore, cosi le versioni
risolte sono quelle qualificate; cargo scarta da solo le voci inutilizzate.
Con lui `rust-toolchain.toml`: la MSRV dichiarata e quella che compila.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ORACLE_CRATE = ROOT / "crates" / "plenora-db-oracle"
FORK_MANIFEST = ROOT / "vendor" / "oracle-rs" / "Cargo.toml"
DRIVER_LIB = "oracle_rs"


def consumer_manifest(oracle_crate: Path) -> str:
    # Il percorso in forma POSIX: TOML lo legge uguale su Linux e Windows.
    return (
        "[package]\n"
        'name = "plenora-consumer-probe"\n'
        'version = "0.0.0"\n'
        'edition = "2021"\n'
        "publish = false\n"
        "\n"
        "# Workspace proprio: e la condizione del consumatore reale, per cui\n"
        "# i `[patch]` di database-tools non valgono.\n"
        "[workspace]\n"
        "\n"
        "[dependencies]\n"
        f'plenora-db-oracle = {{ path = "{oracle_crate.as_posix()}" }}\n'
    )


def driver_packages(metadata: dict) -> list[dict]:
    """I pacchetti che forniscono la libreria del driver Oracle."""

    return [
        package
        for package in metadata["packages"]
        if any(
            target["name"] == DRIVER_LIB and "lib" in target["kind"]
            for target in package["targets"]
        )
    ]


def verdict(metadata: dict, fork_manifest: Path) -> list[str]:
    """Gli scostamenti: una lista vuota e il verde."""

    packages = driver_packages(metadata)
    if len(packages) != 1:
        return [f"attesa una sola libreria {DRIVER_LIB}, trovate {len(packages)}"]
    package = packages[0]
    problems = []
    if package["source"] is not None:
        problems.append(
            f"{DRIVER_LIB} arriva da {package['source']}: il consumatore "
            "riceve l'upstream senza il delta del fork"
        )
    if Path(package["manifest_path"]).resolve() != fork_manifest.resolve():
        problems.append(f"{DRIVER_LIB} non e risolto su vendor/oracle-rs")
    return problems


def run(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        command, cwd=cwd, check=False, text=True, capture_output=True
    )
    if completed.returncode:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise RuntimeError(f"comando fallito: {' '.join(command[:3])}")
    return completed.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--build",
        action="store_true",
        help="compila anche il consumatore (cargo check)",
    )
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="plenora-consumer-") as directory:
        consumer = Path(directory)
        (consumer / "src").mkdir()
        (consumer / "src" / "lib.rs").write_text("", encoding="utf-8")
        (consumer / "Cargo.toml").write_text(
            consumer_manifest(ORACLE_CRATE), encoding="utf-8"
        )
        shutil.copyfile(ROOT / "Cargo.lock", consumer / "Cargo.lock")
        # La toolchain fissata: la MSRV del workspace e quella che compila.
        shutil.copyfile(
            ROOT / "rust-toolchain.toml", consumer / "rust-toolchain.toml"
        )
        metadata = json.loads(
            run(["cargo", "metadata", "--format-version", "1"], consumer)
        )
        problems = verdict(metadata, FORK_MANIFEST)
        if problems:
            for problem in problems:
                print(f"consumatore: {problem}")
            return 1
        if arguments.build:
            # Il target del workspace riusa gli artefatti delle dipendenze di
            # registro gia compilate dagli altri passi.
            environment = dict(os.environ)
            environment.setdefault("CARGO_TARGET_DIR", str(ROOT / "target"))
            completed = subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=consumer,
                env=environment,
                check=False,
            )
            if completed.returncode:
                print("consumatore: la compilazione contro il fork e fallita")
                return 1
    print(f"consumatore: {DRIVER_LIB} risolto sul fork vendor/oracle-rs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
