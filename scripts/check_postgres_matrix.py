#!/usr/bin/env python3
"""Matrice live sulle major PostgreSQL supportate e immagini PostGIS stabili."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"
# Rete privata della matrice, creata e distrutta dal gate: non e la rete
# di un progetto Compose e il nome non deve farlo credere.
NETWORK = "plenora-postgres-matrix"
CACHE = ROOT.parent / "plenora-cargo-cache"
PASSWORD = "plenora_matrix_test_2026"
# I riferimenti della matrice, fissati per **digest**.
#
# Erano fissati per tag — `postgis/postgis:14-3.5` — e il gate rileggeva poi
# dal server la versione che aveva davvero ottenuto, quindi il verdetto non
# mentiva mai. Cio che mancava non era l'onesta del resoconto: era la
# **riproducibilita**. Un tag si muove, e la stessa corsa fra un mese avrebbe
# misurato un'altra patch senza che il comando cambiasse di una lettera.
#
# E' anche la sola forma che divergeva dalle altre tre matrici del repository,
# dove `references.json` porta scritta la ragione: un digest non cambia mai
# contenuto, quindi la riga e la prova della versione avviata invece di una
# promessa su quale versione si otterra.
#
# Il tag resta accanto al digest e non e decorativo: e cio che si rilegge per
# rinnovare la riga quando esce una patch, e senza di esso il digest sarebbe
# un numero senza provenienza.
#
# Risolti il 2026-08-26. Le versioni che ne sono uscite — 14.18, 15.13, 16.9,
# 17.5, 18.6, con PostGIS 3.5.2 e 3.6.4 — sono nel verdetto della corsa, e il
# gate continua a pretenderle: un digest che rendesse una major diversa da
# quella dichiarata viene rifiutato.
TARGETS = [
    (
        "14",
        "3.5",
        "postgis/postgis:14-3.5",
        "sha256:e5b5020fcac75f7f5468f4cf3cd54159110ad937b695391729a0730d2b56fba4",
    ),
    (
        "15",
        "3.5",
        "postgis/postgis:15-3.5",
        "sha256:679bd74e581fc4ebeadabe987077fceb3d1179c007971e9270255d9d93e0726b",
    ),
    (
        "16",
        "3.5",
        "postgis/postgis:16-3.5",
        "sha256:8828b03e9a95269f808abe62aa83215b22a2d3710f54a55e8040327b7b5f9932",
    ),
    (
        "17",
        "3.5",
        "postgis/postgis:17-3.5",
        "sha256:624f5195b91d424dbebf018890148cc0e5a3e80db5467da8b53cc2ed2ce49216",
    ),
    (
        "18",
        "3.6",
        "postgis/postgis:18-3.6",
        "sha256:8d67cc8fe5f45808d54fe95cc210b05ce6b3ea3682e9a97c36362f3e1b8ff939",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="scrive il report JSON completo, anche quando un target fallisce",
    )
    return parser.parse_args()


def run(command: list[str], *, capture: bool = False) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=capture,
    )
    if completed.returncode:
        if capture:
            sys.stderr.write(completed.stdout)
            sys.stderr.write(completed.stderr)
        raise RuntimeError(f"check fallito: {command[0]}")
    return completed.stdout if capture else ""


def ensure_network() -> None:
    inspected = subprocess.run(
        ["docker", "network", "inspect", NETWORK],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    if inspected.returncode:
        run(["docker", "network", "create", NETWORK])


def wait_until_ready(container: str) -> None:
    deadline = time.monotonic() + 120
    stable_identity: str | None = None
    stable_checks = 0
    while time.monotonic() < deadline:
        ready = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "psql",
                "-U",
                "plenora_matrix",
                "-d",
                "plenora_matrix",
                "-At",
                "-F",
                "|",
                "-c",
                (
                    "SELECT pg_postmaster_start_time()::text, "
                    "to_regclass('plenora_fixture.advanced_types') IS NOT NULL"
                ),
            ],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        identity = ready.stdout.strip()
        if ready.returncode == 0 and identity.endswith("|t"):
            if identity == stable_identity:
                stable_checks += 1
            else:
                stable_identity = identity
                stable_checks = 1
            if stable_checks >= 2:
                return
        else:
            stable_identity = None
            stable_checks = 0
        state = run(
            ["docker", "inspect", "--format", "{{.State.Status}}", container],
            capture=True,
        ).strip()
        if state == "exited":
            raise RuntimeError(f"container {container} terminato durante l'avvio")
        time.sleep(1)
    raise RuntimeError(f"timeout healthcheck {container}")


def cargo_test(container: str) -> None:
    dsn = (
        f"host={container} port=5432 user=plenora_matrix "
        f"password={PASSWORD} dbname=plenora_matrix"
    )
    run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            NETWORK,
            "-e",
            # Il gate pretende che le prove live **misurino**. Quindici di esse
            # saltavano in silenzio quando la DSN mancava, dichiarandosi passate: con
            # questo segnale acceso una DSN assente e un fallimento, e arriva qui
            # invece che in produzione.
            f"PLENORA_TEST_POSTGRES_DSN={dsn}",
            "-e",
            "PLENORA_REQUIRE_LIVE_POSTGRES=1",
            "-v",
            f"{ROOT}:/workspace",
            "-v",
            f"{CACHE}:/usr/local/cargo/registry",
            "-w",
            "/workspace",
            IMAGE,
            "cargo",
            "test",
            "-p",
            "plenora-db-postgres",
            "--lib",
            "--",
            "--nocapture",
        ]
    )


def inspect_versions(container: str) -> dict[str, str]:
    output = run(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-U",
            "plenora_matrix",
            "-d",
            "plenora_matrix",
            "-At",
            "-F",
            "|",
            "-c",
            "SELECT current_setting('server_version'), postgis_lib_version()",
        ],
        capture=True,
    ).strip()
    postgres, postgis = output.split("|", maxsplit=1)
    return {"postgres": postgres, "postgis": postgis}


def test_target(postgres: str, postgis: str, tag: str, digest: str) -> dict[str, str]:
    container = f"plenora-matrix-pg{postgres}"
    # Si avvia il digest, non il tag: e la sola forma che rende la corsa
    # ripetibile. Il tag resta nel resoconto come provenienza della riga.
    image = f"postgis/postgis@{digest}"
    run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            container,
            "--network",
            NETWORK,
            "-e",
            "POSTGRES_USER=plenora_matrix",
            "-e",
            f"POSTGRES_PASSWORD={PASSWORD}",
            "-e",
            "POSTGRES_DB=plenora_matrix",
            "-v",
            f"{ROOT / 'docker' / 'postgres' / 'init'}:/docker-entrypoint-initdb.d:ro",
            image,
        ]
    )
    try:
        wait_until_ready(container)
        cargo_test(container)
        versions = inspect_versions(container)
        if versions["postgres"].split(".", maxsplit=1)[0] != postgres:
            raise RuntimeError(
                f"major PostgreSQL inattesa per {image}: {versions['postgres']}"
            )
        if not versions["postgis"].startswith(f"{postgis}."):
            raise RuntimeError(
                f"versione PostGIS inattesa per {image}: {versions['postgis']}"
            )
        return {
            "declared_postgres": postgres,
            "declared_postgis": postgis,
            "image": image,
            "declared_tag": tag,
            "actual_postgres": versions["postgres"],
            "actual_postgis": versions["postgis"],
            "status": "passed",
        }
    except (RuntimeError, ValueError):
        logs = subprocess.run(
            ["docker", "logs", "--tail", "120", container],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        sys.stderr.write(logs.stdout)
        sys.stderr.write(logs.stderr)
        raise
    finally:
        subprocess.run(
            ["docker", "stop", "--time", "10", container],
            cwd=ROOT,
            check=False,
            capture_output=True,
        )


def write_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as target:
        json.dump(report, target, ensure_ascii=False, indent=2, sort_keys=True)
        target.write("\n")


def main() -> int:
    args = parse_args()
    CACHE.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, str]] = []
    failure: str | None = None
    try:
        ensure_network()
        for postgres, postgis, tag, digest in TARGETS:
            try:
                results.append(test_target(postgres, postgis, tag, digest))
            except (RuntimeError, ValueError) as error:
                failure = str(error)
                results.append(
                    {
                        "declared_postgres": postgres,
                        "declared_postgis": postgis,
                        "image": f"postgis/postgis@{digest}",
                        "declared_tag": tag,
                        "actual_postgres": "",
                        "actual_postgis": "",
                        "status": "failed",
                        "error": failure,
                    }
                )
                break
    except (RuntimeError, ValueError) as error:
        failure = str(error)
    report: dict[str, object] = {
        "schema_version": 1,
        "gate": "postgres-postgis-supported-major-matrix",
        "status": "failed" if failure else "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": bool(results),
        "secrets_persisted": False,
        "targets": results,
    }
    if failure:
        report["failure"] = failure
    if args.output:
        write_report(args.output.resolve(), report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    if failure:
        print(f"postgres matrix gate: {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
