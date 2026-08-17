# plenora-database

Python SDK per `plenora-database-tools` — bindings PyO3 sopra al core
Rust del progetto. Espone Postgres/PostGIS e MySQL con API sync + async
Pythonic, portable AST builder, error hierarchy tipizzata, spatial
predicates e context manager per transazioni.

- **Status**: Fase 3 completata (F3-1 → F3-8 + P0.7 + P0.8)
- **Postgres**: OLTP + PostGIS coperti
- **MySQL**: esposto sync e async (`connect_mysql` / `aconnect_mysql`)
  con la stessa superficie di Postgres — execute*, `begin` con
  `SessionContext`, `read`/`aread` streaming Arrow, `copy_from`/`acopy_from`
  bulk e builder AST portabili. `TruncateInsert` resta fail-closed
- **SQL Server**: driver Rust presente nel workspace ma non ancora
  esposto al SDK Python
- **Async**: `asyncio` bridge sopra al runtime tokio condiviso
- **Performance**: ~13× più veloce del subprocess CLI (vedi bench)

## Install

Richiede Python **3.10+**. Il wheel è `abi3-py310` → un unico wheel
copre 3.10 / 3.11 / 3.12 / 3.13 per la stessa piattaforma.

### Da wheel pre-costruito (produzione)

```bash
pip install plenora-database
```

### Da sorgenti (dev)

Richiede Rust 1.92+ e [maturin](https://maturin.rs).

```bash
pip install maturin
cd crates/plenora-database-py
maturin develop --release
python -c "import plenora_database as p; print(p.version())"
```

## Quickstart

### Sync

```python
import plenora_database as p

with p.connect("host=localhost user=me dbname=app") as s:
    # SQL raw con parametri positional
    n = s.execute("INSERT INTO users(name) VALUES ($1)", ["Ada"])
    cnt = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")
    rows = s.execute_returning_rows(
        "SELECT id, name FROM users WHERE id = $1", [1]
    )

    # Portable AST — provider-agnostic
    row = s.select("users").columns("id", "name").where_eq("id", 1).one()
    new = s.insert("users").values(name="Alan").returning("id").one()
    s.update("users").set(name="Grace").where_eq("id", 1).execute()
    s.delete("users").where_lt("last_seen", "2020-01-01").execute()
```

### Async

```python
import asyncio
import plenora_database as p

async def main():
    async with await p.aconnect("host=localhost user=me dbname=app") as s:
        cnt = await s.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")
        rows = await s.select("users").where_eq("active", True).all()

        # Concorrenza: gather non blocca l'event loop
        results = await asyncio.gather(
            *(s.execute_scalar("SELECT $1::int", [i]) for i in range(10))
        )

asyncio.run(main())
```

## Transaction context

```python
with s.begin(isolation="serializable", read_only=False) as tx:
    tx.execute("INSERT INTO t(x) VALUES ($1)", [1])
    row = tx.select("t").where_eq("x", 1).one()
    # commit auto su exit normale, rollback su eccezione

# Async equivalente:
async with await s.begin(isolation="serializable") as tx:
    await tx.insert("t").values(x=1).returning("id").one()
```

### Savepoints

```python
with s.begin() as tx:
    tx.execute("INSERT INTO t(x) VALUES ($1)", [1])
    tx.savepoint("sp1")
    try:
        tx.execute("... query rischiosa ...")
    except p.PlenoraError:
        tx.rollback_to_savepoint("sp1")
    tx.release_savepoint("sp1")
    # commit finale
```

## Typed params (bypass auto-inference)

Python `str` è ambiguo (text? uuid? timestamp?). Helper esplicit:

```python
s.execute(
    "INSERT INTO events(id, ts, amount) VALUES ($1, $2, $3)",
    [
        p.uuid("550e8400-e29b-41d4-a716-446655440000"),
        p.timestamptz("2026-08-13T10:00:00+02:00"),
        p.decimal("1234.56"),   # preserva precisione, no float
    ],
)

# NULL con hint di tipo (quando il target non è inferibile)
s.execute("INSERT INTO t(val) VALUES ($1)", [p.null("text")])
```

Formati:
- `p.uuid(str)` — 36 char con dash
- `p.date(str)` — `YYYY-MM-DD`
- `p.timestamp(str)` — ISO-8601 senza tz (`YYYY-MM-DDTHH:MM:SS`)
- `p.timestamptz(str)` — RFC-3339 (`YYYY-MM-DDTHH:MM:SS±HH:MM`)
- `p.decimal(str)` — stringa numerica precisa
- `p.null(type_name)` — hint tipo colonna

## Spatial (PostGIS)

```python
# 1. Estrai EWKB di riferimento
ref_ewkb = s.execute_scalar(
    "SELECT ST_AsEWKB(ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))"
)
ref = p.spatial.geometry(ewkb=ref_ewkb, srid=4326)
# oppure p.spatial.geography(...) per calcoli metrici geodetici

# 2. Predicato spatial chain-abile con altri where_*
rows = (
    s.select("poi")
     .columns("id", "name")
     .where_spatial("geom", "intersects", ref)
     .where_eq("category", "restaurant")
     .all()
)

# DWithin con distanza
near = (
    s.select("poi")
     .where_spatial("g", "d_within", ref, distance_meters=500.0)
     .all()
)
```

Predicati: `intersects` / `contains` / `within` / `bounding_box` / `d_within`.

## Bulk write (COPY)

Per import massivo Postgres usa COPY internamente (via `prepare_write` +
`write` del provider). Il consumer Python passa dati Arrow:

```python
import pyarrow as pa

tbl = pa.table({
    "id": pa.array(range(1, 100_001), type=pa.int64()),
    "label": [f"row-{i}" for i in range(1, 100_001)],
    "amount": pa.array([i * 10 for i in range(1, 100_001)], type=pa.int32()),
})

# Append in target esistente
outcome = s.copy_from("public", "measurements", tbl, mode="append")
# {"status": "committed", "rows": {"received": 100000, "confirmed": 100000, ...}}

# ETL scratch — crea tabella dallo schema Arrow (nessun DDL preventivo)
outcome = s.copy_from("public", "measurements_new", tbl, mode="create")

# Async equivalente
outcome = await s.acopy_from("public", "measurements", tbl)
```

`source` accetta `pyarrow.Table`, `pyarrow.RecordBatch`, iterable di batch
o `bytes` (Arrow IPC stream self-contained, per zero-copy da altri produttori).

Mode:
- `append` (**default**) — INSERT bulk via COPY nel target esistente
- `create` — CREATE TABLE dallo schema Arrow + COPY (fallisce se
  target esiste)
- `replace` — CREATE TABLE staging + COPY + swap atomico verso target
- `truncate_insert` — TRUNCATE + INSERT bulk (target deve esistere)
- `update` / `upsert` / `delete_by_keys` — supportati dal provider ma
  richiedono `keys` / `update_columns` (parametri v0.3+ per esporli
  dal SDK; per ora usa il path OLTP `Transaction.upsert`)
Transaction profile: `single_transaction` (default) / `chunk_committed` /
`staged_swap` / `best_effort_ddl`.
Mapping policy: `compatible` (default) / `strict` / `lossy` / `native`.
`strict` boccia ogni loss anche minore (es. Arrow nullable → PG NOT NULL);
`compatible` tollera le loss non-DataLoss — scelta consigliata per input
pyarrow tipici (dove i campi sono nullable per default).

L'outcome è un dict con struttura `WriteOutcome` del core (status,
rows.confirmed / .inserted / .failed / .skipped, layer_outcomes, recovery).

## Observability

`Session.metrics()` restituisce uno snapshot dict compatibile con export
Prometheus / OpenTelemetry:

```python
snap = s.metrics()
# {"database": {"queries_total": N, "queries_failed": N,
#               "write_committed_total": N, ...},
#  "connection_pool": {"in_use": N, "available": N, ...}}

# Esempio integrazione OpenTelemetry (structured log)
import logging
logger = logging.getLogger("plenora.db")
logger.info(
    "db.snapshot",
    extra={
        "db.queries_total": snap["database"]["queries_total"],
        "db.pool_in_use": snap["connection_pool"]["in_use"],
        "service.version": p.version(),
    },
)
```

Per catch degli errori con contesto strutturato, tutti i campi diagnostici
sono attributi sulle `PlenoraError`:

```python
try:
    s.execute(sql, params)
except p.PlenoraError as e:
    logger.error(
        "db.error",
        extra={
            "db.category": e.category,
            "db.phase": e.phase,
            "db.retry": e.retry,
            "db.remote_effect": e.remote_effect,
            "db.provider": e.provider,
            "db.execution_id": e.execution_id,
        },
    )
    raise
```

## Error hierarchy

Tutti gli errori discendono da `PlenoraError` (che a sua volta
discende da `RuntimeError` → retro-compat con `except RuntimeError`).

```python
try:
    s.select("users").where_eq("id", 42).one()
except p.PlenoraNotFoundError as e:
    # oggetto SQL inesistente
    log.warn("target missing", extra={
        "category": e.category,        # "not_found"
        "phase": e.phase,              # "read" / "prepare" / ...
        "retry": e.retry,              # "never" / "safe" / "requires_recovery" / ...
        "remote_effect": e.remote_effect,  # "none" / "rolled_back" / ...
        "provider": e.provider,        # "postgres"
    })
except p.PlenoraTimeoutError:
    ...
except p.PlenoraCancelledError:
    ...
except p.PlenoraConcurrentModificationError:
    ...
except p.PlenoraError:
    # catch-all su tutti gli errori del SDK
    ...
```

Le 19 sottoclassi corrispondono 1:1 al `ErrorCategory` del core Rust:

`PlenoraInvalidPlanError`, `PlenoraInvalidConfigurationError`,
`PlenoraSchemaError`, `PlenoraDataMappingError`, `PlenoraCrsError`,
`PlenoraUnsupportedError`, `PlenoraNotFoundError`, `PlenoraConflictError`,
`PlenoraConcurrentModificationError`, `PlenoraAuthenticationError`,
`PlenoraAuthorizationError`, `PlenoraTimeoutError`, `PlenoraCancelledError`,
`PlenoraResourceLimitError`, `PlenoraIoError`, `PlenoraProtocolError`,
`PlenoraTransientError`, `PlenoraExecutionError`, `PlenoraInternalError`.

## Optimistic conflict pattern

```python
# UPDATE ottimistico con expected_version
n = (
    s.update("orders")
     .set(status="paid", version=current + 1)
     .where_eq("id", order_id)
     .where_eq("version", current)    # stale-check
     .execute()
)
if n == 0:
    # version cambiato sotto — refresh e riprova
    fresh = s.select("orders").columns("version").where_eq("id", order_id).scalar()
    ...
```

## Performance

Bench live PG 16 + PostGIS 3.4 su docker (`test_benchmark_parity.py`,
opt-in con `PLENORA_BENCH_PARITY=1`):

| Modalità | ms/call | Speedup |
|---|---|---|
| Subprocess `plenora-database` CLI | 8.40 | 1× |
| **SDK in-process (Session riusata)** | **0.62** | **13.5×** |
| SDK new-Session per call | 5.62 | 1.5× |

Il vantaggio deriva da:
1. Connection pooling (~5 ms/call risparmiati)
2. No fork+exec+startup (~2 ms/call risparmiati)
3. No serialize JSON stdin/stdout (~0.5 ms/call risparmiati)

Per endpoint FastAPI con 3-5 query/richiesta, il PFM passa da **~40 ms** a **~3 ms** di tempo DB.

## Compatibility

| | Python | Rust | Postgres | PostGIS |
|---|---|---|---|---|
| Min supportato | 3.10 | 1.92 | 12+ | 3.0+ |
| Testato in CI | 3.10, 3.11, 3.12, 3.13 | 1.92 | 16.4 | 3.4.3 |

Wheel: `abi3-py310` → un solo wheel per platform copre tutte le
versioni Python ≥ 3.10.

## Limitations (v0.1)

- **SQL Server** — raggiungibile solo tramite il driver Rust del
  workspace, non ancora esposto al SDK Python. Postgres e MySQL sono
  entrambi esposti.
- **Portable spatial DWithin unità SRS** — per predicato DWithin su
  colonna `geometry(*, 4326)` la distanza è in gradi, non metri.
  Usare `spatial.geography(...)` per unità metriche geodetiche.

## Sviluppo

### Test

```bash
python scripts/check_sdk_tests.py                  # Postgres + MySQL live
python scripts/check_sdk_tests.py --offline        # solo i test senza server
python scripts/check_sdk_tests.py --benchmark-only # solo i bench di parita
python scripts/check_sdk_tests.py --allow-dirty    # verdetto non autorevole
```

**Albero pulito.** Il runner rifiuta di partire se `git status --porcelain
-uall` non e vuoto: modifiche staged, non staged e file mai tracciati sono
tutti e tre un motivo di rifiuto. Il verdetto nomina un commit, e il wheel
si costruisce dai file su disco: se i due non coincidono, quel nome descrive
altro codice. `--allow-dirty` esiste per le corse esplorative e non fa
finta di niente — il verdetto esce con `authoritative: false`,
`worktree_dirty: true` e le righe di `git status` che lo hanno reso tale.

Il runner costruisce **sempre** il wheel con `maturin` prima di `pytest`, e
lo esporta in una directory temporanea fuori dal repository: nel source tree
non installa niente, e un `.so` rimasto da una corsa precedente viene
rimosso.

Non lanciare `pytest` a mano: `_native.abi3.so` e gitignorato e nessuno lo
rigenera. Dopo un cambio al Rust, un `pytest` diretto esegue il binario di
prima e risponde su codice che non e quello scritto — rosso su codice
corretto, o verde su codice rotto. E successo due volte in una sola
sessione, e le due correzioni sembravano entrambe sbagliate.

**La suite verifica il wheel, non i sorgenti accanto.** Il container di test
installa l'artefatto con `pip install --no-deps`, monta il repository in sola
lettura, copia `python/tests` fuori dal source tree e gira da li senza
`PYTHONPATH` verso il package locale. Prima di pytest,
`scripts/sdk_wheel_probe.py` chiede a `importlib` da dove verrebbero
`plenora_database` e `_native`, e fallisce se la risposta non e
`site-packages`: le tre strade che riportano alla copia sorgente —
`PYTHONPATH`, `cwd`, e l'inserimento di `sys.path` che pytest fa risalire al
padre di `tests/`, che e un package — non si vedono nel risultato, perche i
test sono gli stessi e passano uguale.

Reti Compose, volume della CA MySQL e **credenziali** dei riferimenti
vengono chiesti a Docker dal runner, non scritti a mano: valgono anche in
un worktree con un altro progetto Compose, e non esiste una seconda copia
della password da tenere allineata al compose.

I pin di pip sono fissati: `requirements-sdk-build.txt` per `maturin` —
dentro il vincolo dichiarato da `pyproject.toml`, che il runner verifica — e
`requirements-sdk-tests.txt` per la chiusura completa delle dipendenze di
test. Il runner confronta i pin con il `pip freeze` del container e
fallisce se divergono.

**Tracciato, non riproducibile.** `rust:1.92` e `python:3.13-slim` sono tag
mutabili e l'`apt-get` della build prende cio che il mirror pubblica oggi:
una seconda corsa non ricostruisce necessariamente lo stesso ambiente. Cio
che il runner garantisce e di dire con cosa ha girato — id e digest delle due
immagini, versione di rustc e di Python effettive.

Il verdetto JSON identifica cio che ha girato: commit, SHA-256 del wheel e
del modulo nativo **caricato da site-packages**, percorsi da cui e stato
importato, identita delle immagini e versioni effettive di Python, rustc,
maturin, pyarrow, pandas e pytest. Il nome del wheel da solo non identifica
un artefatto — e lo stesso a ogni build. Il runner verifica inoltre che ne'
la build ne' i test abbiano cambiato l'albero di lavoro, untracked inclusi.

### Benchmark opt-in

I benchmark di parita girano dentro il runner live (`PLENORA_BENCH_PARITY`
e gia impostato). Il confronto SDK / CLI ha bisogno del binario Linux in
`target/release/plenora-database` — e il container a eseguirlo — e il runner
gli passa il percorso con `PLENORA_CLI_BIN`: scriverlo dentro il test lo
legava al punto di mount di allora, e al primo cambio il bench si e saltato
da solo. In `--benchmark-only` il binario assente e un rifiuto, non uno
skip: senza, lo scope direbbe "passed" per un confronto mai avvenuto.

Per lanciarli da soli, sempre su un wheel appena costruito, c'e l'opzione
dedicata:

```bash
python scripts/check_sdk_tests.py --benchmark-only
```

### Struttura

```
crates/plenora-database-py/
├── Cargo.toml           # cdylib + pyo3 abi3-py310 + pyo3-async-runtimes
├── pyproject.toml       # maturin backend, python-source=python/
├── src/                 # bindings PyO3
│   ├── lib.rs           # #[pymodule] + init runtime
│   ├── session.rs       # Session sync
│   ├── transaction.rs   # Transaction sync
│   ├── async_session.rs # AsyncSession
│   ├── async_transaction.rs # AsyncTransaction
│   ├── py_convert.rs    # Python ↔ ParameterValue conversion
│   └── errors.rs        # PlenoraError gerarchia
└── python/
    ├── plenora_database/
    │   ├── __init__.py           # entry point
    │   ├── _session.py           # wrapper Session
    │   ├── _transaction.py       # wrapper Transaction
    │   ├── _async_session.py     # wrapper AsyncSession
    │   ├── _async_transaction.py # wrapper AsyncTransaction
    │   ├── query.py              # builder Select/Insert/Update/Delete/Upsert
    │   ├── async_query.py        # subclass async dei builder
    │   ├── spatial.py            # SpatialReference + helpers
    │   ├── types.py              # TypedValue + p.uuid/date/decimal/...
    │   ├── errors.py             # reexport PlenoraError classi
    │   └── _ast.py               # helper serializzazione AST
    └── tests/                    # pytest (sync + async)
```

## License

Proprietary. Vedi `Cargo.toml` workspace.
