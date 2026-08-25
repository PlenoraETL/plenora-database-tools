#!/usr/bin/env python3
"""Gate di hardening live del provider PostgreSQL/PostGIS."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import live_inventory  # noqa: E402
from scripts.compose_network import (  # noqa: E402
    compose_network_arguments,
    compose_volume,
)

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.92"

DEFAULT_DSN = (
    "host=dataflow-postgres port=5432 user=dataflow "
    "password=dataflow_test_2026 dbname=dataflow_test"
)
DEFAULT_TLS_DSN = (
    "host=dataflow-postgres-tls port=5432 user=dataflow_tls "
    "password=dataflow_tls_test_2026 dbname=dataflow_tls_test"
)
# Il volume dei certificati porta il prefisso del progetto Compose: si scopre
# dai mount del container, come la rete. Scritto a mano diventava stale al
# rename del progetto, e il sintomo sarebbe stato un mount vuoto — il gate
# avrebbe visto una directory senza certificati, non un nome sbagliato.
TLS_CERTS_DESTINATION = "/tls"


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


# I due riferimenti PostgreSQL — plaintext e TLS — vivono in progetti Compose
# distinti, quindi su reti distinte. Il gate li interroga entrambi nella stessa
# esecuzione e si attacca a entrambe le reti: prima pretendeva che
# condividessero il progetto, e la separazione dei progetti lo ha rotto.
POSTGRES_CONTAINERS = ("dataflow-postgres", "dataflow-postgres-tls")


def cargo(
    arguments: list[str],
    dsn: str | None = None,
    tls_dsn: str | None = None,
) -> list[str]:
    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{ROOT}:/workspace",
        "-v",
        f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-w",
        "/workspace",
    ]
    if dsn is not None or tls_dsn is not None:
        command += compose_network_arguments(*POSTGRES_CONTAINERS)
    if dsn is not None:
        command += [
            "-e",
            # Il gate pretende che le prove live **misurino**. Quindici di esse
            # saltavano in silenzio quando la DSN mancava, dichiarandosi passate: con
            # questo segnale acceso una DSN assente e un fallimento, e arriva qui
            # invece che in produzione.
            f"PLENORA_TEST_POSTGRES_DSN={dsn}",
            "-e",
            "PLENORA_REQUIRE_LIVE_POSTGRES=1",
        ]
    if tls_dsn is not None:
        command += [
            "-v",
            f"{compose_volume(POSTGRES_CONTAINERS[1], TLS_CERTS_DESTINATION)}:/tls:ro",
            "-e",
            f"PLENORA_TEST_POSTGRES_TLS_DSN={tls_dsn}",
            "-e",
            "PLENORA_TEST_POSTGRES_TLS_CA=/tls/ca.crt",
            "-e",
            "PLENORA_TEST_POSTGRES_TLS_CLIENT_CERT=/tls/client.crt",
            "-e",
            "PLENORA_TEST_POSTGRES_TLS_CLIENT_KEY=/tls/client.key",
        ]
    return [*command, IMAGE, "cargo", *arguments]


# Il test che **solo questo gate** puo qualificare.
#
# `live_private_ca_mtls_and_cancellation_when_configured` legge quattro
# variabili — DSN TLS, CA privata, certificato e chiave del client — e senza di
# esse ritorna subito, riportando comunque `ok`. Il gate del riferimento lo
# dichiara percio non qualificante: il suo compose e plaintext. Qui le quattro
# variabili ci sono tutte (vedi `cargo(..., tls_dsn)`), quindi il test prova
# davvero mTLS su CA privata — e va preteso per nome, perche un `ok` da solo
# non distingue la prova dal ritorno anticipato.
REQUIRED_LIVE_TESTS = frozenset(
    {
        "live_private_ca_mtls_and_cancellation_when_configured",
    }
)


def validate_required_live_tests(output: str) -> list[str]:
    """I test che questo gate dichiara devono comparire fra gli eseguiti."""

    executed = {
        live_inventory.leaf(name) for name in live_inventory.executed_tests(output)
    }
    missing = sorted(REQUIRED_LIVE_TESTS - executed)
    if missing:
        raise RuntimeError(
            f"test dichiarati dal gate hardening ma non eseguiti: {missing}"
        )
    return sorted(REQUIRED_LIVE_TESTS)


def live_cli_probe_command(tls_dsn: str) -> list[str]:
    """Il comando dei probe CLI mTLS. Eseguirlo spetta a chi registra i passi.

    Prima questa funzione eseguiva **e** validava, e il passo veniva registrato
    dal chiamante: un comando fuori dall'unico esecutore che tiene il conto.
    Il verdetto restava corretto per costruzione, ma la garanzia che `steps`
    fosse completa valeva solo finche nessuno aggiungeva un altro helper.
    """

    return cargo(
        [
            "test",
            "-p",
            "plenora-database-cli",
            "--test",
            "live_probe",
            "private_ca_mtls",
            "--locked",
            "--",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ],
        tls_dsn=tls_dsn,
    )


def validate_live_cli_probes(output: str) -> None:
    """I due probe devono essere passati, e devono essere quei due."""

    expected = {
        "live_database_probe_postgres_private_ca_mtls",
        "live_legacy_postgres_probe_private_ca_mtls",
    }
    executed = set(
        re.findall(r"^test ([^ ]+) \.\.\. ok$", output, re.MULTILINE)
    )
    if executed != expected:
        raise RuntimeError(f"probe CLI PostgreSQL inattesi: {sorted(executed)}")
    if not re.search(
        r"test result: ok\. 2 passed; 0 failed; 0 ignored;", output
    ):
        raise RuntimeError("risultato probe CLI PostgreSQL inatteso")


def main() -> int:
    dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN", DEFAULT_DSN)
    tls_dsn = os.environ.get("PLENORA_TEST_POSTGRES_TLS_DSN", DEFAULT_TLS_DSN)
    # I passi che il gate ha **davvero** completato, registrati mentre
    # accadono. La versione precedente pubblicava cinquantotto voci tematiche
    # scritte a mano, nessuna legata al nome di un test: cancellare il test che
    # sostiene `write_deadline_verified_rollback` non toglieva la voce e non
    # faceva fallire niente, e l'artifact continuava a dichiarare `passed` su
    # una prova che non esisteva piu.
    steps: list[str] = []

    def step(name: str, command: list[str], *, capture: bool = False) -> str:
        """Esegue, e **solo dopo** registra il passo.

        L'ordine non e un dettaglio di stile: una riga scritta prima del
        comando resta vera anche quando il comando fallisce o sparisce, ed e
        esattamente la forma delle cinquantotto attestazioni statiche che
        questo verdetto pubblicava. Passare da qui per ogni comando rende
        `steps` l'inventario completo di cio che il gate ha fatto, non di
        alcuni suoi punti scelti a mano.
        """

        output = run(command, capture=capture)
        steps.append(name)
        return output

    try:
        step(
            "gate_self_test",
            [sys.executable, str(ROOT / "scripts" / "test_check_postgres_hardening.py")],
        )
        step(
            "tls_reference_started",
            [
                "docker",
                "compose",
                "-f",
                "docker-compose.postgres-tls.yml",
                "up",
                "-d",
                "--wait",
            ],
        )
        state = run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.Health.Status}}",
                "dataflow-postgres",
            ],
            capture=True,
        ).strip()
        if state != "running|healthy":
            raise RuntimeError("container PostgreSQL non healthy")
        steps.append("container_health")
        tls_state = run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.Health.Status}}",
                "dataflow-postgres-tls",
            ],
            capture=True,
        ).strip()
        if tls_state != "running|healthy":
            raise RuntimeError("container PostgreSQL mTLS non healthy")
        steps.append("tls_container_health")
        step("rustfmt", cargo(["fmt", "--all", "--", "--check"]))
        step(
            "clippy_deny_warnings",
            cargo(
                [
                    "clippy",
                    "-p",
                    "plenora-db-postgres",
                    "--all-targets",
                    "--",
                    "-D",
                    "warnings",
                ]
            ),
        )
        step(
            "core_and_sql_unit_tests",
            cargo(
                [
                    "test",
                    "-p",
                    "plenora-database-core",
                    "-p",
                    "plenora-database-sql",
                ]
            ),
        )
        # Niente `--nocapture`, per la stessa ragione del gate di riferimento:
        # di questa corsa si legge solo l'elenco delle righe
        # `test <nome> ... ok`, e con le stampe dei test sullo stesso flusso
        # quelle righe si spezzano. La cattura bufferizza per test ed emette
        # righe intere anche in parallelo, e l'output di un test che fallisce
        # lo stampa lo stesso.
        provider_output = step(
            "provider_suite_against_plaintext_and_tls",
            cargo(
                [
                    "test",
                    "-p",
                    "plenora-db-postgres",
                    "--lib",
                ],
                dsn,
                tls_dsn,
            ),
            capture=True,
        )
        validate_required_live_tests(provider_output)
        steps.append("private_ca_mtls_test_observed")
        validate_live_cli_probes(
            step(
                "public_cli_probes_against_private_ca",
                live_cli_probe_command(tls_dsn),
                capture=True,
            )
        )
    except RuntimeError as error:
        print(f"postgres hardening gate: {error}", file=sys.stderr)
        return 1

    report = {
        "schema_version": 1,
        "gate": "postgres-postgis-hardening-v1",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": True,
        "secrets_persisted": False,
        "reference_target": "PostgreSQL 16 / PostGIS 3.4",
        "steps": steps,
        "required_live_tests": sorted(REQUIRED_LIVE_TESTS),
        "remaining_external_matrix": [
            "public_ca_tls",
            "linux_arm64",
            "managed_postgres_services",
        ],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
