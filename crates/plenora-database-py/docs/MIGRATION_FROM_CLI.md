# Migrazione dal CLI allo SDK Python 2.x

Questa guida sostituisce gradualmente chiamate `subprocess` al CLI
`plenora-database` con lo SDK in-process. Il CLI resta appropriato per
diagnostica, benchmark e operazioni manuali; lo SDK evita invece un nuovo
processo e una nuova connessione per ogni operazione applicativa.

Il contratto 2.x non mantiene le factory Python 1.x. Gli unici ingressi
pubblici per il lifecycle sono `engine_from_url`, `async_engine_from_url` ed
`EngineConfig`.

## Installazione

Scaricare il wheel adatto dalla
[pagina delle release](https://github.com/PlenoraETL/plenora-database-tools/releases)
e installare il file locale:

```bash
python -m pip install ./plenora_database-<version>-cp310-abi3-<platform>.whl
```

Gli asset ufficiali sono distribuiti tramite GitHub Releases, insieme a SBOM e
attestazioni. Non esiste una pubblicazione su package index.

## Lifecycle

Un processo applicativo mantiene un `Engine` longevo.
Ogni request/task apre invece la propria `Session`; una sessione non viene
condivisa fra operazioni concorrenti.

```python
import os

import plenora_database as p

engine = p.engine_from_url(os.environ["DATABASE_URL"])


def load_user(user_id: int) -> p.Row | None:
    users = p.table("users", "id", "email")
    statement = p.select(users.c.id, users.c.email).where(
        users.c.id == p.bind("user_id", p.BindType.INTEGER)
    )
    with engine.session() as session:
        return session.execute(statement, {"user_id": user_id}).one_or_none()


# allo shutdown del processo
engine.dispose()
```

La variante async usa lo stesso confine:

```python
engine = await p.async_engine_from_url(os.environ["DATABASE_URL"])

async with engine.session() as session:
    row = await session.execute(statement, {"user_id": user_id})

engine.dispose()
```

## Equivalenze operative

### SQL nativo

Il confine fra query e mutazioni è esplicito. Una query produce `Result`; DDL e
DML producono `MutationResult`.

```python
row = session.query_sql(
    "SELECT id, email FROM users WHERE id = $1",
    [user_id],
).one_or_none()

outcome = session.execute_sql(
    "UPDATE users SET email = $1 WHERE id = $2",
    [email, user_id],
)
updated = outcome.affected_rows

value = session.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")
```

`session.execute(...)` è riservato agli statement portabili. Non riceve SQL
testuale.

### Statement portabili

```python
users = p.table("users", "id", "email")
statement = (
    p.select(users.c.id, users.c.email)
    .select_from(users)
    .where(users.c.id == p.bind("user_id", p.BindType.INTEGER))
)
row = session.execute(statement, {"user_id": user_id}).one_or_none()
```

Ogni bind dichiara il proprio `BindType`; i valori restano separati dallo
statement e dai messaggi di errore.

### Letture Arrow

`read` sostituisce i comandi CLI di lettura tabellare e conserva la
backpressure del consumer:

```python
reader = session.read(
    "public",
    "events",
    projection=["tenant_id", "event_id", "payload"],
    order_by=[("tenant_id", "asc"), ("event_id", "asc")],
    limit=10_000,
)
for chunk in reader:
    process(chunk)
```

Lo streaming non implica un cursore server-side riapribile: la capability
`server_cursor` resta `false`. Una ripresa persistente usa invece
`ReadCheckpoint` e un ordinamento keyset qualificato.

### Scritture Arrow

`copy_from` e `acopy_from` sostituiscono `bulk-write`. La `mapping_policy` è
obbligatoria: il chiamante deve scegliere esplicitamente il trattamento delle
conversioni.

```python
outcome = session.copy_from(
    "public",
    "events",
    arrow_table,
    mode="append",
    mapping_policy="compatible",
)

outcome = await session.acopy_from(
    "public",
    "events",
    arrow_table,
    mode="append",
    mapping_policy="compatible",
)
```

Usare `strict` quando anche una conversione compatibile deve essere rifiutata.
Le mode distruttive o dipendenti da chiavi richiedono inoltre le opzioni
esplicite documentate nel [README dello SDK](../README.md#bulk-write-arrow).

### Introspezione, probe ed explain

```python
schemas = session.inspect.schemas()
tables = session.inspect.tables("public")
description = session.inspect.describe("public", "users")

health = p.probe_engine(engine)
plan = p.explain(
    session,
    "SELECT id FROM users WHERE id = $1",
    [user_id],
)
```

Le capability osservate sono disponibili su `session.capabilities`. Un campo
assente o non misurato non autorizza una feature.

## Runbook incrementale

Per ogni chiamata applicativa al CLI:

1. classificare l'operazione come query, mutazione, lettura Arrow, scrittura
   Arrow oppure tooling operativo;
2. creare l'engine nel bootstrap dell'applicazione, non dentro l'endpoint;
3. aprire una nuova sessione nello scope della singola request o unità di
   lavoro;
4. sostituire `subprocess.run` e il parsing JSON con il metodo tipizzato
   corrispondente;
5. adattare il risultato a `Row`, `Result` o `MutationResult` senza affidarsi a
   dizionari impliciti;
6. sostituire `CalledProcessError` con le sottoclassi di `PlenoraError` e
   decidere retry/recovery dagli attributi strutturati;
7. eseguire test di integrazione sul provider realmente usato;
8. rimuovere il percorso CLI soltanto dopo il rollout del singolo consumer.

Benchmark, campagne di assurance e diagnostica amministrativa possono restare
CLI: sono strumenti operativi, non hot path applicativi.

## Errori e osservabilità

```python
try:
    outcome = session.execute_sql(sql, params)
except p.PlenoraTimeoutError as error:
    handle_timeout(error.retry)
except p.PlenoraError as error:
    logger.error(
        "database operation failed",
        extra={
            "category": error.category,
            "phase": error.phase,
            "retry": error.retry,
            "remote_effect": error.remote_effect,
            "provider": error.provider,
        },
    )
    raise
```

Non inserire SQL, parametri o DSN nei log applicativi. Gli hook
`instrument_engine` seguono lo stesso confine e registrano soltanto contesto
operativo privo di payload.
