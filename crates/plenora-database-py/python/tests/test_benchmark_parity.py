"""F3-8 — Benchmark parity: SDK in-process vs subprocess CLI.

Il PFM può scegliere se consumare la libreria via:
  A) subprocess `plenora-database` CLI (già usato in produzione)
  B) SDK Python native (F3.x, questo package)

Il test misura per lo stesso workload il tempo speso in ognuna delle
due modalità. Il vantaggio del SDK è evitare fork+startup+JSON per
ogni operazione; il ratio atteso è >= 3x (conservativo — tipicamente
10-15x sul happy path scalar).

Il bench è **opt-in**: settare `PLENORA_BENCH_PARITY=1` per abilitarlo.
Il default `pytest` lo salta per non appesantire CI/dev loop.

Prerequisiti:
  - env `PLENORA_TEST_POSTGRES_DSN` valorizzato
  - env `PLENORA_BENCH_PARITY=1`
  - binario CLI compilato in `target/release/plenora-database`
    (o path override via env `PLENORA_CLI_BIN`)
"""
from __future__ import annotations

import os
import subprocess
import time

import pytest

import plenora_database as p

DSN_ENV = "PLENORA_TEST_POSTGRES_DSN"
BENCH_ENV = "PLENORA_BENCH_PARITY"
DEFAULT_CLI_BIN = "/workspace/target/release/plenora-database"
# Target ratio SDK / CLI (subprocess CLI overhead >= 3x).
MIN_SPEEDUP = 3.0
# Numero iterazioni per campionamento. Sufficiente a smorzare rumore
# senza rendere la CI troppo lenta (~10s con CLI subprocess).
ITERATIONS = 20


def _dsn_or_skip() -> str:
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        pytest.skip(f"bench: manca env {DSN_ENV}")
    return dsn


def _bench_enabled_or_skip() -> None:
    if os.environ.get(BENCH_ENV) != "1":
        pytest.skip(
            f"bench opt-in: setta {BENCH_ENV}=1 per lanciarlo "
            "(atteso ~10s di walltime per la sessione)"
        )


def _cli_bin_or_skip() -> str:
    candidate = os.environ.get("PLENORA_CLI_BIN", DEFAULT_CLI_BIN)
    if not os.path.exists(candidate):
        pytest.skip(
            f"bench: CLI binary non trovato in {candidate}. "
            "Build con `cargo build --release -p plenora-database-cli` "
            "o setta PLENORA_CLI_BIN al path corretto."
        )
    return candidate


def _run_cli_scalar(bin_path: str, dsn: str) -> None:
    env = {**os.environ, DSN_ENV: dsn}
    subprocess.run(
        [
            bin_path,
            "execute-scalar",
            DSN_ENV,
            "SELECT 42",
            "--type=i32",
        ],
        env=env,
        capture_output=True,
        check=True,
    )


def _bench(fn, iterations: int) -> float:
    """Ritorna il tempo totale in secondi per `iterations` esecuzioni di `fn`."""
    t0 = time.perf_counter()
    for _ in range(iterations):
        fn()
    return time.perf_counter() - t0


def test_bench_execute_scalar_sdk_beats_subprocess_by_at_least_3x(
    capsys,
) -> None:
    _bench_enabled_or_skip()
    dsn = _dsn_or_skip()
    cli_bin = _cli_bin_or_skip()

    # Warm-up: prima invocazione ha overhead JIT / caching / connect.
    _run_cli_scalar(cli_bin, dsn)
    with p.connect(dsn) as warmup:
        warmup.execute_scalar("SELECT 42")

    # CLI subprocess
    cli_seconds = _bench(lambda: _run_cli_scalar(cli_bin, dsn), ITERATIONS)

    # SDK in-process (una sola Session riusata)
    with p.connect(dsn) as s:
        sdk_seconds = _bench(lambda: s.execute_scalar("SELECT 42"), ITERATIONS)

    cli_ms = 1000.0 * cli_seconds / ITERATIONS
    sdk_ms = 1000.0 * sdk_seconds / ITERATIONS
    speedup = cli_seconds / sdk_seconds

    with capsys.disabled():
        print(
            f"\n[bench] execute-scalar (SELECT 42), N={ITERATIONS}\n"
            f"        CLI subprocess : {cli_ms:8.2f} ms/call ({cli_seconds:.2f} s totali)\n"
            f"        SDK in-process : {sdk_ms:8.2f} ms/call ({sdk_seconds:.2f} s totali)\n"
            f"        SPEEDUP        : {speedup:6.1f}x (target >= {MIN_SPEEDUP:.1f}x)"
        )
    assert speedup >= MIN_SPEEDUP, (
        f"SDK non è {MIN_SPEEDUP:.1f}x più veloce del subprocess CLI: "
        f"CLI={cli_ms:.2f}ms SDK={sdk_ms:.2f}ms speedup={speedup:.1f}x"
    )


def test_bench_session_reuse_amortizes_connect_cost(capsys) -> None:
    """Un secondo test che mostra il costo del connect ammortizzato:
    misura il tempo per N=100 execute_scalar su una sola Session vs
    N=100 execute_scalar ognuna con Session dedicata (connect+close)."""
    _bench_enabled_or_skip()
    dsn = _dsn_or_skip()
    n = 50

    # Warm-up
    with p.connect(dsn) as w:
        w.execute_scalar("SELECT 1")

    # Reused session
    with p.connect(dsn) as s:
        reused = _bench(lambda: s.execute_scalar("SELECT 1"), n)

    def one_shot() -> None:
        with p.connect(dsn) as one:
            one.execute_scalar("SELECT 1")

    per_shot = _bench(one_shot, n)

    with capsys.disabled():
        print(
            f"\n[bench] session reuse (SELECT 1), N={n}\n"
            f"        Reused Session : {1000.0 * reused / n:8.2f} ms/call\n"
            f"        New Session    : {1000.0 * per_shot / n:8.2f} ms/call\n"
            f"        SPEEDUP        : {per_shot / reused:6.1f}x"
        )
    # Session reuse deve essere almeno 2x più veloce di connect-per-call.
    assert per_shot / reused >= 2.0
