# Migrazione subprocess CLI → SDK in-process

Guida operativa per il team PFM. Mappa ogni sotto-comando `plenora-database`
oggi invocato via `subprocess` al chiamante Python nativo equivalente.

## TL;DR

- **Vantaggio**: ~13× più veloce per operazione (bench live: CLI
  8.40 ms/call vs SDK 0.62 ms/call). Vedi `README.md#performance`.
- **Cambio operativo**: pattern `subprocess.run([...], check=True) + json.loads(stdout)` diventa una chiamata diretta di metodo che ritorna già l'oggetto Python tipizzato.
- **Retro-compat**: nessuna. Da fare endpoint-by-endpoint (runbook sotto).
- **Rollout consigliato**: hot path per primo (endpoint OLTP con QPS alto). Cold path (job batch giornalieri) può restare CLI se non c'è urgenza.

## Setup una volta

Prima di ogni consumer, sostituire l'apertura connessione:

```python
# Prima (CLI):
os.environ["PFM_POSTGRES_DSN"] = dsn
# ogni call rifà connect + probe

# Dopo (SDK):
import plenora_database as p
_session = p.connect(dsn)   # global module-level, apre una volta
# tutte le call riusano _session
```

Per FastAPI:
```python
# app startup
app.state.pg = p.connect(dsn)
# oppure per async
app.state.pg = await p.aconnect(dsn)

# app shutdown
app.state.pg.close()
```

## Mappa 1:1 dei sub-comandi

Legenda:
- `DSN_ENV` = nome della variabile che contiene la DSN (nel CLI è argument)
- `SQL` = stringa SQL
- Le equivalenze SDK assumono `import plenora_database as p` e `s = p.connect(dsn)`.

### `execute-scalar`

```bash
plenora-database execute-scalar PFM_DSN "SELECT $1::int + $2::int" \
    --type=i32 --param 10:i32 --param 20:i32
# → {"status":"ok","value":30}
```

```python
value = s.execute_scalar(
    "SELECT $1::int + $2::int",
    [10, 20],
)  # → 30 (int Python nativo, no dict wrapping)
```

Note:
- Nel SDK il tipo di ritorno è inferito dal server (no `--type` esplicito). Se serve un cast preciso, usarlo in SQL (`SELECT ...::uuid::text`).
- I typed params (`--param VALUE:TYPE`) diventano `p.uuid()`, `p.decimal()`, ecc.: vedi `types.py`.

### `execute-sql`

```bash
plenora-database execute-sql PFM_DSN "UPDATE t SET x=$1 WHERE id=$2" \
    --param abc:text --param 5:i32
# → {"status":"ok","affected_rows":1}
```

```python
n = s.execute("UPDATE t SET x=$1 WHERE id=$2", ["abc", 5])   # → 1
```

### `execute-ddl`

```bash
plenora-database execute-ddl PFM_DSN "CREATE INDEX idx_users_email ON users(email)"
```

```python
s.execute("CREATE INDEX idx_users_email ON users(email)")
```

Il SDK non distingue DDL da DML (una singola `execute` copre entrambi).

### `postgres-query` / `portable-execute`

```bash
plenora-database portable-execute PFM_DSN '{"type":"select",...}'
```

```python
# Preferisci il builder (produce l'AST per te):
rows = s.select("users").columns("id","email").where_eq("id", 1).all()

# Oppure raw AST se vuoi controllo totale:
rows = s._native.execute_portable_rows(json_ast_string)
```

### `postgres-read-summary`

```bash
plenora-database postgres-read-summary PFM_DSN public users
# → {"provider":"postgres","rows":N,"batches":M,"fields":[...]}
```

```python
# Streaming Arrow non ancora esposto al SDK (roadmap Fase 4).
# Per ora usa il facade con execute_returning_rows se il dataset
# entra in memoria; altrimenti resta col CLI per read grossi.
rows = s.execute_returning_rows("SELECT * FROM public.users")
```

**Nota**: `postgres-read-ipc` (streaming Arrow su file IPC) NON ha
equivalente SDK oggi. È l'unico use case dove il CLI resta necessario
finché non arriva il batch reader esposto in Fase 4.

### `bulk-write`

```bash
plenora-database bulk-write PFM_DSN append public.events data.arrow ...
```

**Non c'è equivalente SDK diretto** per bulk COPY Arrow (roadmap).
Per bulk insert modesti (≤10k righe), usa il builder multi-row:

```python
s.insert("events").rows([
    {"ts": ts, "kind": k, "payload": p} for (ts,k,p) in batch
]).execute()
```

Per bulk grossi (>10k righe), resta col CLI.

### `inspect-database` / `inspect-schemas` / `inspect-tables`

```bash
plenora-database inspect-schemas PFM_DSN
# → {"schemas":[{"name":"public"},...]}
```

```python
# Namespace nativo (F4-1):
schemas = s.inspect.schemas()          # ['public', ...]  (system esclusi)
catalogs = s.inspect.catalogs()        # ['app_prod', ...]
tables = s.inspect.tables("public")    # [{'name': 't1', 'kind': 'table', 'is_partition': False}, ...]
desc = s.inspect.describe("public", "users")
# → {'schema': ..., 'columns': [...], 'schema_token': ...}
```

### `doctor` / `diagnose` / `profile-check`

Servono per operations (verifica salute + conformance profile PFM).
Restano tipicamente CLI-only perché sono orchestrazione, non hot-path.
Se serve integrarli in un job Python, il pattern è:

```python
# Doctor equivalente minimale
def doctor(s: p.Session) -> dict:
    return {
        "server_version": s.server_version,
        "postgis_version": s.postgis_version,
        "current_database": s.execute_scalar("SELECT current_database()"),
        "current_user": s.execute_scalar("SELECT current_user"),
        "connections_active": s.execute_scalar(
            "SELECT COUNT(*)::BIGINT FROM pg_stat_activity WHERE state='active'"
        ),
    }
```

I probe `probe_pfm_core_v1` / `probe_pfm_gis_v1` sono in Rust ma non
esposti al SDK: se il PFM ha bisogno di lanciarli programmaticamente,
va aggiunto un binding (roadmap).

### `benchmark-oltp` / `benchmark-read` / `benchmark-write` / `benchmark-spatial`

CLI-only. Non hanno equivalente SDK — sono strumenti di
misurazione infrastrutturale, non pattern applicativi. Restano invariati.

### `conditional-update`

```bash
plenora-database conditional-update PFM_DSN t "id=1 AND version=5" \
    "status='done', version=6"
```

```python
# Pattern optimistic-conflict via builder:
n = (
    s.update("t")
     .set(status="done", version=6)
     .where_eq("id", 1)
     .where_eq("version", 5)
     .execute()
)
if n == 0:
    # conflict — refresh e retry
    current = s.select("t").columns("version").where_eq("id", 1).scalar()
    ...
```

Il metodo `execute_conditional_update` del trait Rust
(con distinzione `NotFound` vs `ConcurrentModification`)
non è ancora esposto al SDK — roadmap.

### `pool-status` / `explain`

`pool-status` è debug tooling → resta CLI.
`explain`: usa SQL diretto per ora:

```python
plan = s.execute_returning_rows("EXPLAIN (FORMAT JSON) SELECT ...")
```

## Runbook migrazione (endpoint-per-endpoint)

Consigliato: **una PR per endpoint** invece di un big-bang.

### Passo 1 — Identifica il pattern subprocess

```bash
grep -rn "subprocess.*plenora-database" src/pfm/
```

Categorizzali per hot-path vs cold-path:

- **Hot path** (per-request): candidati per switch immediato.
  Beneficio: latency HTTP -40..-80%, throughput +5..10×.
- **Cold path** (job cron, migrations, one-off): switch opzionale.
  Beneficio marginale.

### Passo 2 — Boot Session al app startup

Prima della prima PR, aggiungi al bootstrap dell'app:

```python
# pfm/db.py
import plenora_database as p

_pg: p.Session | None = None

def get_pg() -> p.Session:
    global _pg
    if _pg is None:
        _pg = p.connect(os.environ["PFM_POSTGRES_DSN"])
    return _pg

def close_pg() -> None:
    global _pg
    if _pg is not None:
        _pg.close()
        _pg = None
```

Per FastAPI + asyncio:
```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    app.state.pg = await p.aconnect(os.environ["PFM_POSTGRES_DSN"])
    try:
        yield
    finally:
        app.state.pg.close()

app = FastAPI(lifespan=lifespan)
```

### Passo 3 — Sostituisci un endpoint alla volta

Per ogni endpoint hot:

1. Trova il subprocess: `result = subprocess.run(["plenora-database", ...], capture_output=True, check=True)`
2. Trova la `json.loads(result.stdout)` corrispondente
3. Sostituisci con la chiamata SDK dalla tabella sopra
4. Rimuovi il pattern `os.environ["PFM_DSN"] = dsn` (non serve più — il SDK ha la DSN già bindata alla Session)
5. Adatta l'error handling: `subprocess.CalledProcessError` → `p.PlenoraError` (gerarchia specifica per branching)
6. Test unit + integration
7. Deploy dietro feature flag se preferisci graduale

### Passo 4 — Gestione errori tipizzata

Prima:
```python
try:
    result = subprocess.run([...], check=True, ...)
except subprocess.CalledProcessError as e:
    # e.stderr contiene JSON envelope error del CLI, va parsato
    envelope = json.loads(e.stderr)
    category = envelope["error"]["category"]
    if category == "not_found":
        ...
```

Dopo:
```python
try:
    row = s.select("t").where_eq("id", id).one()
except p.PlenoraNotFoundError:
    # gerarchia già tipizzata
    ...
except p.PlenoraTimeoutError:
    ...
except p.PlenoraError as e:
    # catch-all — attributi ispezionabili
    log.error("db error", extra={
        "category": e.category,
        "phase": e.phase,
        "retry": e.retry,
        "remote_effect": e.remote_effect,
        "provider": e.provider,
    })
```

### Passo 5 — Osservabilità

Il SDK espone i contatori interni del provider (F4-1):

```python
snap = s.metrics()
# → dict con ~25 chiavi u64
for name, value in snap.items():
    metrics_client.gauge(f"pfm.db.{name}", value)
```

Chiavi principali per l'oncall:
- `pool_checkouts` / `pool_reuses` / `pool_timeouts` / `pool_new_connections`
- `schema_cache_hits` / `schema_cache_misses` / `schema_cache_evictions` /
  `schema_cache_invalidations` (cold cache detection)
- `catalog_introspections` (introspezione al server)
- `read_batches` / `read_rows` / `read_bytes`
- `writes_committed` / `writes_outcome_unknown` (> 0 = commit ambiguo,
  verificare out-of-band)
- `cancellations` / `invalidated_sessions`

Per instrumentation puntuale per-operazione (latency histograms),
wrappa le chiamate hot in un decorator che misura:

```python
import time

def measured(op: str):
    def deco(fn):
        def wrapped(*a, **kw):
            t0 = time.perf_counter()
            try:
                return fn(*a, **kw)
            finally:
                elapsed = time.perf_counter() - t0
                metrics.histogram("pfm.db.op.duration", elapsed, tags={"op": op})
        return wrapped
    return deco

@measured("get_user_by_id")
def get_user_by_id(uid):
    return get_pg().select("users").where_eq("id", uid).one()
```

## FAQ

**Q**: Il SDK gestisce pool di connessioni?
**A**: Sì. `PostgresProvider` (dietro `Session`) ha un pool interno di
   default 10 connessioni. Puoi tenere UNA `Session` globale nell'app
   e il pool sotto il cofano gestisce la concorrenza (async in
   particolare gira più query in parallelo sullo stesso Session).

**Q**: Cosa succede se la connessione si rompe (network partition)?
**A**: `PlenoraIoError` o `PlenoraTransientError` (in base al fase).
   L'attributo `retry` dice `Safe` per read idempotenti,
   `RequiresRecovery` per commit ambigui. La `Session` non si
   auto-close: puoi ritentare o chiuderla e aprirne una nuova.

**Q**: Il SDK funziona su Windows?
**A**: Sì — il wheel abi3 è built per Linux/macOS/Windows via CI. Il
   PFM in dev su Windows non ha problemi.

**Q**: Posso mescolare sync e async nello stesso processo?
**A**: Sì — hanno oggetti separati (`Session` vs `AsyncSession`).
   Condividono lo stesso tokio runtime sotto il cofano. Puoi avere
   un'app FastAPI async che chiama un job sync in un ThreadPool
   senza contention di runtime.

**Q**: Cosa devo fare al team platform per il rollout?
**A**:
   1. Aggiungi `plenora-database` a `requirements.txt` (pin la versione)
   2. Il wheel abi3 non ha dipendenze runtime Python extra (solo
      Postgres server accessibile via DSN)
   3. Il container base dell'app deve avere glibc >= 2.34 (o musl
      compatibile) — è manylinux_2_34
   4. Nessuna variabile ambiente nuova richiesta

**Q**: Serve installare Rust in produzione?
**A**: No. Il wheel contiene la libreria nativa già compilata (`.so` /
   `.dylib` / `.pyd`). Solo `pip install` in prod.
