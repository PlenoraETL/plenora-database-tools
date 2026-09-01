#!/usr/bin/env python3
"""Gate live del driver PostgreSQL/PostGIS di riferimento."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Il gate viene invocato sia come modulo del pacchetto sia come script: la
# radice del repository deve restare importabile in entrambi i casi.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import live_inventory  # noqa: E402
from scripts.compose_network import compose_network  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "rust:1.98"
CONTAINER = "dataflow-postgres"
DEFAULT_DSN = (
    "host=dataflow-postgres port=5432 user=dataflow "
    "password=dataflow_test_2026 dbname=dataflow_test"
)
# Test live che il gate non può dichiarare senza averli visti passare. Saltano
# con un early-return quando la DSN non è impostata, quindi un `test result: ok`
# non prova che siano stati eseguiti: solo il nome lo prova.
REQUIRED_LIVE_TESTS = frozenset(
    {
        "live_provider_row_diagnostics_matches_confirmed_rollback_oracle",
        "live_provider_row_diagnostics_lost_rollback_ack_is_quarantined",
        "live_provider_row_diagnostics_commit_ambiguity_partitions_all_rows_unknown",
        "live_keyset_checkpoint_persists_reopens_without_duplicates_or_gaps",
    }
)


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
            sys.stderr.write(completed.stderr)
        raise RuntimeError(f"check fallito: {command[0]}")
    return completed.stdout if capture else ""


def postgres_network() -> str:
    """Rete Compose osservata sul container di riferimento.

    Il nome dipende dal progetto Compose, cioè dalla directory del checkout: un
    valore cablato rende il gate ineseguibile in un worktree, dove i cargo con
    DSN finiscono su una rete che non contiene il container e falliscono con
    errori `Protocol`/`Connect` indistinguibili da un difetto del provider. La
    scoperta è fail-closed — senza label di progetto, senza la rete attesa o
    senza l'alias del container il gate fallisce invece di indovinare.
    """

    return compose_network(CONTAINER, required_alias=CONTAINER)


def cargo(
    arguments: list[str],
    dsn: str | None = None,
    *,
    insecure_local: bool = False,
) -> list[str]:
    """Comando `docker run` per un cargo dentro l'immagine pinnata.

    `insecure_local` esporta `PLENORA_TLS_INSECURE_LOCAL` al CLI. Serve
    perche il riferimento di questo gate e **plaintext** per costruzione — il
    riferimento TLS e un Compose separato, `dataflow-postgres-tls`, ed e
    `check_postgres_hardening.py` a provarne la verifica. Il CLI richiede TLS
    per default, quindi il fixture plaintext deve abilitarne esplicitamente la
    deroga locale.

    L'interruttore vale solo per i passi che lo chiedono. Estenderlo a tutta
    la suite renderebbe invisibile una regressione sul default sicuro.
    """

    command = [
        "docker", "run", "--rm",
        "-v", f"{ROOT}:/workspace",
        "-v", f"{ROOT.parent / 'plenora-cargo-cache'}:/usr/local/cargo/registry",
        "-v", "plenora-conformance-current-target:/workspace/target",
        "-w", "/workspace",
    ]
    if insecure_local:
        command += ["-e", "PLENORA_TLS_INSECURE_LOCAL=1"]
    if dsn is not None:
        command += [
            "--network", postgres_network(),
            # Il gate pretende che le prove live **misurino**. Quindici di esse
            # saltavano in silenzio quando la DSN mancava, dichiarandosi passate: con
            # questo segnale acceso una DSN assente e un fallimento, e arriva qui
            # invece che in produzione.
            "-e", f"PLENORA_TEST_POSTGRES_DSN={dsn}",
            "-e", "PLENORA_REQUIRE_LIVE_POSTGRES=1",
        ]
    return [*command, IMAGE, "cargo", *arguments]


def check_ipc_materialization(dsn: str) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix=".postgres-ipc-gate-", dir=ROOT) as directory:
        output = Path(directory) / "schema-cache.arrow"
        container_output = "/workspace/" + output.relative_to(ROOT).as_posix()
        materialized = json.loads(run(
            cargo([
                "run", "--quiet", "-p", "plenora-database-cli", "--",
                "postgres-read-ipc", "PLENORA_TEST_POSTGRES_DSN",
                "plenora_fixture", "schema_cache_probe", container_output,
                "--max-rows", "100", "--max-output-bytes", "10485760",
                "--timeout-ms", "120000", "--order-by", "id",
            ], dsn, insecure_local=True),
            capture=True,
        ))
        inspected = json.loads(run(
            cargo([
                "run", "--quiet", "-p", "plenora-database-cli", "--",
                "inspect-dataset", container_output,
            ]),
            capture=True,
        ))
        geometry = next(field for field in inspected["fields"] if field["name"] == "geom")
        if materialized["status"] != "materialized" or materialized["rows"] != 1:
            raise RuntimeError("materializzazione IPC PostgreSQL incompleta")
        if materialized["row_order"] != "deterministic":
            raise RuntimeError("ordine IPC PostgreSQL non deterministico")
        if materialized["durability"] not in {"confirmed", "unconfirmed"}:
            raise RuntimeError("durability IPC PostgreSQL non dichiarata")
        if inspected["rows"] != 1 or inspected["contract_version"] != "1":
            raise RuntimeError("rilettura IPC PostgreSQL incoerente")
        if geometry["metadata"].get("plenora.geometry.encoding") != "ewkb":
            raise RuntimeError("encoding geometria IPC PostgreSQL non preservato")
        if geometry["metadata"].get("plenora.geometry.srid") != "4326":
            raise RuntimeError("SRID geometria IPC PostgreSQL non preservato")
        if list(Path(directory).glob(".*.partial-*")):
            raise RuntimeError("staging IPC PostgreSQL residuo")
        return {
            "rows": materialized["rows"],
            "batches": materialized["batches"],
            "row_order": materialized["row_order"],
            "durability": materialized["durability"],
            "geometry_encoding": geometry["metadata"]["plenora.geometry.encoding"],
            "geometry_srid": geometry["metadata"]["plenora.geometry.srid"],
        }


PROVIDER_SOURCES = ROOT / "crates" / "plenora-db-postgres" / "src"

# Test live che **questo** riferimento non puo qualificare, e perche.
#
# PF.2 e PF.3 verificano il ramo negativo della probe capability: un
# PostgreSQL **senza** PostGIS, che il compose di questo gate non avvia. I due
# test leggono `POSTGRES_URL_BARE`, e senza quella variabile stampano una riga
# di skip e ritornano — ma `cargo test` li conta comunque come `ok`.
#
# Sono elencati qui perche il report smetta di lasciarli passare per prove.
# Dichiararli non qualificanti non li fa girare: li rende visibili. Toglierli
# da questa lista significa aver aggiunto il fixture bare al gate.
NON_QUALIFYING_LIVE_TESTS = {
    "preflight_pf2_capability_negative_reports_no_postgis": "richiede POSTGRES_URL_BARE: PostgreSQL senza PostGIS, non avviato da questo compose",
    "preflight_pf3_spatial_query_without_postgis_fails_cleanly": "richiede POSTGRES_URL_BARE: PostgreSQL senza PostGIS, non avviato da questo compose",
    "live_private_ca_mtls_and_cancellation_when_configured": "richiede le quattro variabili TLS di una CA privata: il compose di questo gate e plaintext, e il test ritorna subito riportando comunque `ok`",
}

# L'inventario e condiviso con il gate SQL Server: vedi `scripts/live_inventory`.
# Portarne due copie significava correggerne una sola, che e esattamente cio
# che era successo.
def live_test_inventory() -> set[str]:
    """I test live che i sorgenti del provider definiscono, ora.

    Resta derivato dai sorgenti perche copre un caso che il binario non copre:
    un test che esiste nel codice ma non arriva nella suite — modulo non
    incluso, `cfg` che lo esclude — sparisce anche dalla lista di `cargo`, e
    confrontare cargo con cargo non lo vedrebbe mai.

    Il prefisso `live_` non basta: anche un helper può portarlo. Serve
    l'attributo Rust che rende la funzione un test eseguibile.
    """

    return live_inventory.source_inventory(
        list(PROVIDER_SOURCES.rglob("*.rs")),
        keep=lambda name: name.startswith("live_"),
    )


def listed_live_tests(output: str) -> set[str]:
    """I test live che il binario compilato contiene, con il nome completo.

    `cargo test -- --list` enumera cio che la suite eseguirebbe, ignorati
    compresi: e la stessa vista che produce le righe `... ok`, quindi il
    confronto e fra nomi omogenei e nessuna forma sintattica del sorgente puo
    falsarlo.
    """

    return live_inventory.listed_tests(
        output, keep=lambda name: live_inventory.leaf(name).startswith("live_")
    )


def validate_live_inventory(output: str, listing: str) -> list[str]:
    """Ogni test live deve essere nella suite, ed essere passato.

    Il report elencava quarantatre voci tematiche — `read_write_spatial_live`,
    `advanced_catalog_introspection`, ... — scritte a mano e legate a niente:
    affermavano una copertura che nessun passo verificava, e restavano vere
    anche se il test corrispondente veniva cancellato.

    Le prove qui sono due, e servono a cose diverse. La prima confronta i
    sorgenti con la suite compilata: un test che esiste nel codice ma non
    arriva nel binario e sparito senza che nessuno lo dica. La seconda
    confronta la suite con l'esecuzione, sui **nomi completi**: un test
    presente e non passato non puo scomparire dietro un omonimo di un altro
    modulo. Restituisce i nomi eseguiti, che entrano nel report: cosi il
    documento dice cosa e successo, non cosa doveva.
    """

    listed = listed_live_tests(listing)
    declared = live_test_inventory()
    absent = sorted(declared - {live_inventory.leaf(name) for name in listed})
    if absent:
        raise RuntimeError(
            f"test live PostgreSQL definiti nei sorgenti ma assenti dalla suite "
            f"compilata ({len(absent)} su {len(declared)}): {absent}"
        )
    executed = live_inventory.executed_tests(output)
    missing = sorted(listed - executed)
    if missing:
        raise RuntimeError(
            f"test live PostgreSQL nella suite ma non eseguiti ({len(missing)} su "
            f"{len(listed)}): {missing}"
        )
    return sorted(listed)


def validate_live_row_diagnostics(output: str) -> None:
    """Verifica che i test live row diagnostics siano nella matrice eseguita.

    Questi test escono con un early-return quando la DSN non è impostata, e in
    quel caso riportano comunque `ok`: il solo esito non prova che le
    asserzioni siano state eseguite. Il gate copre la seconda metà della prova
    imponendo la DSN a ogni invocazione (`cargo(..., dsn)`); questo controllo
    copre la prima, cioè che i tre test siano ancora compilati e inclusi nella
    corsa. Un test rinominato, cancellato o filtrato via smette di essere una
    prova e qui fallisce invece di sparire in silenzio.
    """

    executed = set(
        re.findall(r"^test test_suite::tests::([^ ]+) \.\.\. ok$", output, re.MULTILINE)
    )
    missing = sorted(REQUIRED_LIVE_TESTS - executed)
    if missing:
        raise RuntimeError(
            f"test live PostgreSQL dichiarati ma non eseguiti: {missing}"
        )


def main() -> int:
    dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN", DEFAULT_DSN)
    # I passi completati sono registrati dopo l'esecuzione. Un passo saltato o
    # fallito non può quindi comparire nell'attestazione.
    steps: list[str] = []
    try:
        state = run(
            ["docker", "inspect", "--format",
             "{{.State.Status}}|{{.State.Health.Status}}", CONTAINER],
            capture=True,
        ).strip()
        if state != "running|healthy":
            raise RuntimeError("container PostgreSQL non healthy")
        steps.append("container_health")
        run(cargo(["clippy", "--workspace", "--all-targets", "--", "-D", "warnings"]))
        steps.append("clippy_deny_warnings")
        run(cargo([
            "test",
            "-p", "plenora-database-core",
            "-p", "plenora-database-sql",
        ]))
        steps.append("core_and_sql_unit_tests")
        # `--include-ignored` allinea la corsa all'inventario del provider; un
        # test non eseguito non può entrare nel report.
        #
        # Niente `--nocapture`, ed e il punto di questa invocazione.
        #
        # Di questa corsa si legge **solo** l'elenco delle righe
        # `test <nome> ... ok`: le usano `validate_live_row_diagnostics` e
        # `validate_live_inventory`, e nessuna delle due guarda cio che i test
        # stampano. Con `--nocapture` quelle stampe finivano sullo stesso
        # flusso delle righe di esito mentre i test giravano, e in parallelo le
        # due cose si intrecciavano: una riga `test <nome> ... ok` che si
        # ritrova dentro la stampa di un altro test smette di essere
        # riconoscibile.
        #
        # Il gate e diventato rosso cosi, una corsa su due, su
        # `live_postgres_concurrent_cancellation_recovers_pool`: dichiarato
        # «nella suite ma non eseguito» mentre era stato eseguito e passato.
        # Quel test non ha `#[ignore]` e non puo essere saltato — se il DSN
        # manca ritorna subito, ma la riga di esito la stampa lo stesso — il
        # che rendeva l'accusa impossibile e il verdetto una moneta.
        #
        # `--test-threads=1` sembrava la risposta e non lo era: serializzare
        # rende la rottura **deterministica** invece di toglierla, perche
        # l'harness stampa `test <nome> ... `, lascia scrivere il test, e solo
        # dopo aggiunge `ok`. Con la serializzazione i due benchmark, che
        # stampano, sparivano dall'inventario a ogni corsa. Meglio di un flake,
        # ma sempre sbagliato.
        #
        # Con la cattura attiva l'harness bufferizza per test ed emette righe
        # intere, anche in parallelo — e l'output di un test che fallisce lo
        # stampa lo stesso, che era l'unica ragione per cui `--nocapture`
        # poteva sembrare utile qui.
        provider_output = run(
            cargo(
                [
                    "test",
                    "-p",
                    "plenora-db-postgres",
                    "--",
                    "--include-ignored",
                ],
                dsn,
            ),
            capture=True,
        )
        steps.append("provider_live_suite_with_ignored")
        validate_live_row_diagnostics(provider_output)
        steps.append("required_live_tests_observed")
        # La suite compilata, chiesta a cargo: e la sola vista omogenea a
        # quella che produce le righe `... ok`, e regge dove una regex sui
        # sorgenti si romperebbe su una forma sintattica nuova.
        listing = run(
            cargo(
                ["test", "-p", "plenora-db-postgres", "--", "--list", "--include-ignored"],
                dsn,
            ),
            capture=True,
        )
        executed_live_tests = validate_live_inventory(provider_output, listing)
        steps.append("live_inventory_matches_sources_and_run")
        # Le fixture live del CLI: `#[ignore]` per default, quindi invisibili
        # a `cargo test`. Finche nessun gate le lanciava, una di esse poteva
        # restare rotta per una campagna intera senza che niente lo dicesse.
        for suite in ("live_f5", "contract_snapshot"):
            run(
                cargo(
                    [
                        "test",
                        "-p",
                        "plenora-database-cli",
                        "--test",
                        suite,
                        "--",
                        "--include-ignored",
                        "--test-threads=1",
                    ],
                    dsn,
                    insecure_local=True,
                )
            )
            steps.append(f"cli_live_fixture_{suite}")
        ipc_materialization = check_ipc_materialization(dsn)
        steps.append("postgres_read_ipc_materialization_and_readback")
        output = run(
            cargo([
                "test", "-p", "plenora-db-postgres",
                "live_copy_vs_prepared_benchmark", "--",
                "--ignored", "--nocapture",
            ], dsn),
            capture=True,
        )
        matches = re.findall(
            r'\{"rows":1000,"copy_text_micros":\d+,'
            r'"copy_binary_micros":\d+,'
            r'"prepared_micros":\d+,"differences":0\}',
            output,
        )
        if not matches:
            raise RuntimeError("risultato benchmark non trovato")
        benchmark = json.loads(matches[-1])
        steps.append("copy_text_binary_prepared_differential")
    except RuntimeError as error:
        print(f"postgres reference gate: {error}", file=sys.stderr)
        return 1
    report = {
        "schema_version": 1,
        "gate": "postgres-postgis-reference-freeze",
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_connections_opened": True,
        "secrets_persisted": False,
        "steps": steps,
        "executed_live_tests": executed_live_tests,
        # Non "eseguiti": *qualificanti*. I due test del ramo negativo girano
        # e riportano `ok`, ma senza il riferimento bare non asseriscono
        # niente, e un report che li contasse direbbe il falso.
        "declared_not_qualifying": {
            name: reason for name, reason in sorted(NON_QUALIFYING_LIVE_TESTS.items())
        },
        "benchmark": benchmark,
        "ipc_materialization": ipc_materialization,
        "freeze_scope": "postgres-postgis-data-path-v3",
        "advanced_scope": "postgres-postgis-advanced-profile-v1",
        "release_reference": "postgres-provider-v0.1",
        "open_non_blocking": [],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
