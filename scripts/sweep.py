#!/usr/bin/env python3
"""La spazzata prima di un push: cio che la CI eseguira, senza aspettarla.

# Perche esiste

La spazzata deve restare equivalente ai gate offline della CI. Un sottoinsieme
scelto a memoria nasconderebbe proprio i controlli omessi; per questo un
self-test confronta i due elenchi.

# Cosa esegue

Tutto cio che `rust-ci.yml` esegue senza Docker: i self-test Python, il
formato, `cargo check`, clippy pedantico e le prove — con gli stessi comandi,
non con varianti che sembrano equivalenti.

Le prove live sono escluse per nome: pretendono server accesi e credenziali che
i gate forniscono, e qui non ci sono. Escluderle e la ragione per cui questa
spazzata dura secondi.

# Cosa non esegue

Niente che richieda Docker: i gate di riferimento, le matrici, le campagne.
Quelli restano da lanciare quando servono, e questa spazzata non pretende di
sostituirli — pretende di non lasciar passare cio che si puo vedere senza.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: I passi, nell'ordine in cui conviene eseguirli: prima quelli che falliscono
#: in fretta.
#:
#: Ogni riga e (etichetta, comando). L'etichetta e cio che si legge nel
#: resoconto; il comando e cio che gira, ed e lo stesso che la CI esegue —
#: `test_ci_workflows.py` pretende che questa lista copra i passi statici di
#: `rust-ci.yml`, cosi non puo restare indietro in silenzio.
STEPS: tuple[tuple[str, list[str]], ...] = (
    ("cargo fmt", ["cargo", "fmt", "--all", "--", "--check"]),
    (
        "cargo fmt fuzz",
        [
            "cargo",
            "fmt",
            "--manifest-path",
            "fuzz/Cargo.toml",
            "--",
            "--check",
        ],
    ),
    (
        "cargo clippy",
        [
            "cargo",
            "clippy",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ],
    ),
    # Le prove unitarie, con lo **stesso** comando che la CI esegue: non
    # `--lib`, che ne lascerebbe fuori i test di integrazione dei crate.
    (
        "cargo test (senza live)",
        ["cargo", "test", "--workspace", "--", "--skip", "live_"],
    ),
    ("cargo check", ["cargo", "check", "--workspace", "--all-targets"]),
    (
        "cargo build CLI",
        ["cargo", "build", "--locked", "-p", "plenora-database-cli"],
    ),
    (
        "cargo check fuzz",
        [
            "cargo",
            "check",
            "--manifest-path",
            "fuzz/Cargo.toml",
            "--locked",
            "--all-targets",
        ],
    ),
    ("test_ci_workflows.py", [sys.executable, "scripts/test_ci_workflows.py"]),
    (
        "test_check_mariadb_reference.py",
        [sys.executable, "scripts/test_check_mariadb_reference.py"],
    ),
    (
        "test_check_db2_reference.py",
        [sys.executable, "scripts/test_check_db2_reference.py"],
    ),
    (
        "test_name_db2_wheel.py",
        [sys.executable, "scripts/test_name_db2_wheel.py"],
    ),
    (
        "test_check_mysql_reference.py",
        [sys.executable, "scripts/test_check_mysql_reference.py"],
    ),
    ("test_live_inventory.py", [sys.executable, "-m", "unittest", "scripts.test_live_inventory"]),
    (
        "test_check_postgres_reference.py",
        [sys.executable, "scripts/test_check_postgres_reference.py"],
    ),
    (
        "test_check_postgres_hardening.py",
        [sys.executable, "scripts/test_check_postgres_hardening.py"],
    ),
    (
        "test_check_session_matrix.py",
        [sys.executable, "scripts/test_check_session_matrix.py"],
    ),
    (
        "test_check_sqlserver_reference.py",
        [sys.executable, "scripts/test_check_sqlserver_reference.py"],
    ),
    ("phase0_validate.py", [sys.executable, "scripts/phase0_validate.py"]),
    (
        "test_render_adoption_manifest.py",
        [sys.executable, "scripts/test_render_adoption_manifest.py"],
    ),
    ("check_docs.py", [sys.executable, "scripts/check_docs.py"]),
    ("check_comments.py", [sys.executable, "scripts/check_comments.py"]),
    ("test_check_coverage.py", [sys.executable, "scripts/test_check_coverage.py"]),
    ("check_test_layout.py", [sys.executable, "scripts/check_test_layout.py"]),
    ("check_relational_ir.py", [sys.executable, "scripts/check_relational_ir.py"]),
    ("check_typed_metadata.py", [sys.executable, "scripts/check_typed_metadata.py"]),
    ("code_size.py --check", [sys.executable, "scripts/code_size.py", "--check"]),
    ("tests/", [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."]),
    ("render_state.py --check", [sys.executable, "scripts/render_state.py", "--check"]),
)


def main() -> int:
    falliti: list[str] = []
    for label, command in STEPS:
        started = time.monotonic()
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        elapsed = time.monotonic() - started
        esito = "ok " if completed.returncode == 0 else "ROTTO"
        print(f"  {esito}  {label:38} {elapsed:6.1f}s", flush=True)
        if completed.returncode != 0:
            falliti.append(label)
            # Le ultime righe bastano: chi vuole il resto rilancia il passo.
            coda = (completed.stdout + completed.stderr).strip().splitlines()[-12:]
            for line in coda:
                print(f"         {line}", flush=True)

    if falliti:
        print(f"\nspazzata FALLITA: {', '.join(falliti)}")
        return 1
    print(f"\nspazzata completa: {len(STEPS)} passi, nessun difetto")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
