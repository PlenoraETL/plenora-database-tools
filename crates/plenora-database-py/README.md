# plenora-database

Python SDK per `plenora-database-tools` — bindings PyO3 sopra al core
Rust del progetto. Espone PostgreSQL/PostGIS, MySQL, MariaDB, SQL Server e IBM
Db2 LUW con API sync + async Pythonic, portable AST builder, error hierarchy tipizzata, spatial
predicates e context manager per transazioni.

- **Postgres**: OLTP + PostGIS coperti
- **MySQL e MariaDB**: esposti sync e async con factory distinte
  (`connect_mysql` / `aconnect_mysql`, `connect_mariadb` /
  `aconnect_mariadb`)
  con la stessa superficie di Postgres — execute*, `begin` con
  `SessionContext`, `read`/`aread` streaming Arrow, `copy_from`/`acopy_from`
  bulk e builder AST portabili. `TruncateInsert` resta fail-closed
- **SQL Server**: esposto sync e async (`connect_sqlserver` /
  `aconnect_sqlserver`)
- **IBM Db2 LUW**: esposto sync e async (`connect_db2` / `aconnect_db2`) negli
  artefatti costruiti con la feature `db2`; i wheel standard mantengono la
  stessa API ma rifiutano l'uso con `PlenoraUnsupportedError`
- **Async**: `asyncio` bridge sopra al runtime tokio condiviso
- **Performance**: benchmark di parita SDK/CLI disponibile come test opt-in

## Install

Richiede Python **3.10+**. Il wheel è `abi3-py310` → un unico wheel
copre 3.10 / 3.11 / 3.12 / 3.13 per la stessa piattaforma.

### Da wheel pre-costruito (produzione)

```bash
pip install plenora-database
```

### Da sorgenti (dev)

Richiede Rust 1.98+ e [maturin](https://maturin.rs).

```bash
pip install maturin
cd crates/plenora-database-py
maturin develop --release
python -c "import plenora_database as p; print(p.version())"
```

## Quickstart

Per un server applicativo, il confine consigliato e un `Engine` longevo e una
sessione per request. PostgreSQL, MySQL, MariaDB, SQL Server e Db2 espongono lo
stesso lifecycle; le factory `connect*` restano adapter compatibili sopra di
esso.

```python
engine = p.create_engine(dsn)

def handle_request(user_id: int):
    with engine.session() as session:
        with session.begin(native_query_policy="deny") as tx:
            return tx.select("users").where_eq("id", user_id).one_or_none()

# allo shutdown dell'applicazione
engine.dispose()
```

La variante asyncio si crea con `await p.create_async_engine(dsn)` e usa
`async with engine.session()`; l'esempio completo sync/async e in
[`examples/core_v3_repository.py`](examples/core_v3_repository.py).
Gli altri provider usano factory esplicite, per esempio
`p.create_mysql_engine(host, database, user, password)` e
`await p.create_async_sqlserver_engine(...)`: la classe restituita resta
`Engine`/`AsyncEngine`, mentre la factory dichiara senza ambiguita il prodotto.

### Expression language Core v3

Per query componibili, l'API nuova costruisce oggetti immutabili direttamente
sull'IR relazionale canonico. Lo stesso statement viene compilato dal renderer
Rust nel dialetto della sessione; i valori dei bind viaggiano separati e
`execute` restituisce un `Result` uniforme.

```python
import plenora_database as p

users = p.table("users", "id", "team_id", "name", schema="app").alias("u")
teams = p.table("teams", "id", "name", schema="app").alias("t")

statement = (
    p.select(
        users.c.id,
        users.c.name,
        teams.c.name.label("team"),
        p.func.count(users.c.id)
        .over(partition_by=(users.c.team_id,))
        .label("team_size"),
    )
    .select_from(users)
    .join(teams, users.c.team_id == teams.c.id)
    .where(users.c.id >= p.bind("minimum_id"))
    .order_by(users.c.id)
    .limit(50)
)

with engine.session() as session:
    result = session.execute(statement, {"minimum_id": 100})
    rows = result.all()

engine.dispose()
```

`Result` offre `all()`, `first()`, `one()`, `one_or_none()`, `scalar()` e le
varianti di cardinalita stretta `scalar_one()`/`scalar_one_or_none()`. Il path
SQL raw resta invariato: `session.execute(sql, valori_posizionali)` continua a
restituire il numero di righe interessate.

La superficie comprende inoltre `IN`/`BETWEEN`/`LIKE`/test di null, funzioni
scalar e aggregate tramite `func`, `GROUP BY`/`HAVING`, finestre e frame,
subquery scalari o derivate, `EXISTS`, CTE e `UNION`/`INTERSECT`/`EXCEPT`.
Una CTE o subquery richiede label esplicite quando i nomi della proiezione non
sono determinabili. `Result.rows()` e i terminali `row_*` restituiscono `Row`,
accessibile per posizione, nome o descrittore `Column`; i terminali storici
continuano a restituire dizionari per compatibilita.

`bind()` resta non tipizzato per compatibilita quando il contesto della colonna
basta al database. Per una proiezione senza sorgente, o quando il server non
puo inferire il tipo, si usa un hint logico chiuso:

```python
statement = p.select(
    p.bind("answer", p.BindType.INTEGER).label("answer")
)
```

`BindType` non accetta dichiarazioni SQL arbitrarie: il renderer traduce
`BOOLEAN`, `INTEGER`, `BIG_INTEGER`, `FLOAT`, `STRING`, `BINARY`, `DATE` e
`TIMESTAMP` per il dialect. In particolare Db2 rifiuta ancora in prepare una
projection con bind non tipizzato, mentre la forma tipizzata viene resa con un
`CAST` e `SYSIBM.SYSDUMMY1`.

### ORM-like dichiarativo sync e async

L'ORM riusa le stesse `Table`, `Column`, transazioni e mutazioni canoniche. La
sessione ORM apre e possiede una transazione Core: il context manager esegue
`flush` e commit in uscita normale, rollback in caso di errore.

```python
import plenora_database as p

class Account(p.DeclarativeBase):
    __tablename__ = "accounts"
    __schema__ = "app"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    name: p.Mapped[str] = p.mapped_column(nullable=False)
    version: p.Mapped[int] = p.mapped_column(version=True)

with engine.session() as core_session:
    with p.OrmSession(core_session) as orm:
        account = orm.get(Account, 7)
        if account is None:
            account = Account(id=7, name="Ada")
            orm.add(account)
        else:
            account.name = "Grace"
```

Le chiavi primarie possono essere semplici o composite; `get` riceve uno
scalare nel primo caso e una tupla nel secondo. L'identity map rende stabile
l'oggetto restituito; i setter validano i tipi Python dichiarati, tracciano gli
attributi modificati e una colonna `version=True` aggiunge il controllo di
concorrenza ottimistica a update e delete. `inspect_instance` espone stato,
identita e nomi dirty senza esporre valori applicativi.
`mapped_column(int)` corrisponde a SQL `INTEGER` e rifiuta valori fuori dal suo
intervallo prima dell'I/O; una futura superficie `BIGINT` richiedera un tipo
ORM distinto, cosi il binding non deve indovinare la larghezza dal valore.

Le query di entita partono da `orm.query(Account)` e compongono gli stessi
predicati dell'expression language. Oltre a ordinamento e paginazione espongono
join tramite relationship, proiezioni, tuple di entita e caricamento eager con
`selectinload` o `joinedload`. Quest'ultimo e limitato alle relazioni scalari;
le collezioni usano `selectinload`, cosi limit e paginazione non duplicano le
entita root. I valori restano bind separati. `refresh`, `expire`, `expunge` e
`merge` completano il lifecycle; l'autoflush e attivo per default e si puo
disabilitare nel costruttore della sessione.

Una chiave `generated=True` e una colonna `server_default=True` possono essere
omesse dal costruttore. PostgreSQL, MariaDB e SQL Server le idratano dallo
statement di insert; MySQL e Db2 leggono prima l'identita locale della
connessione e poi i default nella stessa transazione. Un provider fuori da
questo insieme fallisce prima dell'I/O. Per generare DDL, il solo marker
`server_default=True` non basta: va fornito un `ServerDefault` esplicito.

`relationship` copre many-to-one, one-to-many, one-to-one e many-to-many con
tabella `secondary`. `back_populates` mantiene coerenti i due lati e le cascade
`save-update`, `delete` e `delete-orphan` sono sempre esplicite: il default non
cascada nulla. Il descrittore non effettua mai I/O; si usa `load` oppure un
loader eager. Il flush ordina il grafo parent/child, propaga chiavi generate e
risolve con un update differito i cicli che hanno almeno una FK nullable.

Le colonne geometriche si dichiarano con
`mapped_column(p.Geometry(srid=4326), ...)`: l'assegnazione verifica EWKB,
SRID, dimensioni, semantica e, se dichiarato, il tipo geometrico concreto
tramite il validatore nativo. `Geometry.bind`, `predicate` e `function`
costruiscono nodi spatial dell'IR senza incorporare il payload. Su PostgreSQL,
`get`/query proiettano la colonna come EWKB e insert/update costruiscono il
valore da un bind EWKB tramite il DML canonico. MySQL e MariaDB qualificano
Geometry ORM XY per i tipi OGC lineari, con SRID verificato a ogni round trip;
geography e le dimensioni Z/M restano chiuse. SQL Server qualifica Point,
LineString e Polygon XY/XYZ sia `geometry` sia `geography`; Db2 qualifica gli
stessi tipi e dimensioni con semantica `geometry`. Entrambi verificano il frame
SRID a ogni idratazione. Le dimensioni M/XYZM e i tipi non nominati restano
fail-closed.

`UniqueConstraint` e `ForeignKeyConstraint` descrivono anche vincoli compositi;
l'ereditarieta pubblicata e la forma concrete esplicita. `OrmMetadata` compila
e applica create/drop table nei dialetti qualificati. `MigrationRunner` e la
variante async eseguono catene lineari, una revisione per transazione, mentre
`OrmSession.listen` registra hook locali sul lifecycle del flush.

`AsyncOrmSession` espone lo stesso mapping, identity map, planner del flush e
regole di concorrenza. Le operazioni che fanno I/O sono coroutine; `add`,
`delete` e la composizione della query restano locali:

```python
async with p.AsyncOrmSession(session) as orm:
    orm.add(Account(id=1, name="Ada"))

async with p.AsyncOrmSession(session) as orm:
    account = await orm.query(Account).where(
        Account.name == p.bind("wanted")
    ).one({"wanted": "Ada"})
```

### Ingresso JSON tipizzato

JSON resta un formato di ingresso del bordo Python, non il protocollo fra SDK
e provider. `JsonInput` richiede un `JsonSchema` chiuso e converte ogni record
prima in valori Python tipizzati, poi, a scelta, in istanze ORM transienti o
in `pyarrow.RecordBatch`. Non esiste inferenza dai primi record: un campo
assente, aggiuntivo, nullo contro schema o di tipo diverso fallisce prima
dell'I/O.

```python
import plenora_database as p

ingress = p.JsonInput(
    p.JsonSchema(
        [
            p.JsonField("id", int),
            p.JsonField("name", str),
            p.JsonField("note", str, nullable=True),
        ]
    )
)

# Un mapping, un documento oggetto/array o un iterabile di mapping.
records = ingress.records(
    '[{"id":1,"name":"Ada","note":null}]'
)

# Un file iterato viene interpretato come JSON Lines e letto una riga alla
# volta. Il generatore produce batch bounded senza caricare il file intero.
with open("accounts.jsonl", encoding="utf-8") as lines:
    outcome = session.copy_from(
        "app", "accounts", ingress.batches(lines, batch_size=1_024)
    )
```

`copy_from` continua a ricevere Arrow: l'adattatore non introduce un secondo
data plane. La lettura e la validazione JSON Lines sono incrementali; la
chiamata bulk serializza i batch risultanti nello stream IPC richiesto dal
binding nativo. Per mantenere bounded anche ogni singola scrittura, il
chiamante può inviare un batch per chiamata e osserva quindi esplicitamente i
relativi confini transazionali.

Un mapper dichiarativo è già uno schema esplicito. I campi generati o con
default server-side sono esclusi dalla forma derivata:

```python
ingress = p.JsonInput.for_model(Account)
accounts = ingress.objects(json_lines, Account)
```

Per sorgenti asincrone che producono un mapping o una riga JSON completa per
iterazione, sono disponibili `arecords`, `aobjects` e `abatches`:

```python
async for batch in ingress.abatches(async_lines, batch_size=1_024):
    await session.acopy_from("app", "accounts", batch)
```

In questa forma ogni `acopy_from` ha il proprio esito; non viene promessa una
transazione implicita che copra l'intero iteratore.

Le geometrie richiedono sempre un CRS dichiarato nello schema. Il payload
GeoJSON non può sostituirlo o ridefinirlo:

```python
place_input = p.JsonInput(
    p.JsonSchema(
        [
            p.JsonField("id", int),
            p.JsonField(
                "shape",
                p.JsonGeometry(
                    srid=4326,
                    dimensions="xy",
                    semantics="geometry",
                    geometry_type="point",
                    encoding="ewkb",
                ),
            ),
        ]
    )
)
```

La conversione supporta Point, LineString, Polygon, i rispettivi Multi e
GeometryCollection, in XY o XYZ. X/Y/M e X/Y/Z/M restano chiusi perché GeoJSON
non permette di distinguere in modo affidabile l'asse M. `encoding="wkb"` e
`encoding="ewkb"` sono scelte esplicite: non tutti i provider qualificano la
stessa forma spatial in scrittura. I batch portano `geoarrow.wkb`, SRID,
dimensioni, semantica e dichiarazione del tipo nei metadata canonici.

`JsonInputError` espone un codice stabile, l'indice del record e, quando il
campo proviene dallo schema, il suo nome. Il messaggio non include mai il
valore rifiutato, la riga JSON o altri frammenti del payload.

### Sync

```python
import plenora_database as p

with p.connect("host=localhost user=me dbname=app") as s:
    caps = s.capabilities
    schemas = s.inspect.schemas()
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
    engine = await p.create_async_engine("host=localhost user=me dbname=app")

    async def scalar(i: int):
        # Ogni task usa la propria sessione; l'Engine condiviso governa il pool.
        async with engine.session() as session:
            return await session.execute_scalar("SELECT $1::int", [i])

    results = await asyncio.gather(*(scalar(i) for i in range(10)))
    engine.dispose()

asyncio.run(main())
```

## Graph con Apache AGE

La superficie graph usa PostgreSQL con l'estensione AGE. Le capability si
aprono soltanto sulla coppia qualificata AGE 1.7.0 / PostgreSQL 18; la versione
installata e il documento separato sono leggibili da `age_version` e
`age_capabilities`. Le operazioni amministrative hanno un documento additivo
separato, `age_admin_capabilities`, cosi il contratto AGE v1 resta invariato.

```python
with plenora_database.connect(dsn, tls_mode="insecure_local") as session:
    if "people" not in session.list_graphs():
        session.create_graph("people")
    rows = session.cypher(
        "people",
        "MATCH (p:Person) WHERE p.name = $name RETURN p",
        columns=["person"],
        params={"name": "Alice"},
        max_rows=10_000,
    )
    person = rows[0]["person"]  # Vertex
```

`columns` e obbligatorio perche AGE richiede la forma del record restituito.
Nomi di grafo, colonne e parametri sono identificatori validati; i valori
viaggiano nella mappa `agtype` bindata e non vengono interpolati nella query.
`Vertex`, `Edge` e `Path` preservano identita, label, estremi e proprieta.
`max_rows` e obbligatoriamente compreso tra 1 e 1.000.000: l'adapter chiede al
server una sola riga sentinella oltre il limite e fallisce con
`ResourceLimit`, senza materializzare un risultato arbitrariamente grande.

`list_graphs()`, `create_graph()` e `drop_graph(..., cascade=...)` legano nomi
e opzioni come parametri PostgreSQL. La forma async usa gli stessi nomi come
coroutine. `cascade=False` e intenzionale: la cancellazione dei dati dipendenti
richiede una scelta esplicita.

La stessa firma e disponibile su `Transaction`, `AsyncSession` e
`AsyncTransaction`. Le scritture Cypher partecipano a commit, rollback e
savepoint PostgreSQL.

Il gate live attraversa le clausole AGE documentate (`MATCH`, `WITH`,
`RETURN`, `ORDER BY`, `SKIP`, `LIMIT`, `CREATE`, `DELETE`, `SET`, `REMOVE`,
`MERGE`, `UNWIND`), parametri preparati, percorsi a lunghezza variabile,
funzioni su path, concorrenza, limiti, cancellazione e timeout. Cypher resta
testo opaco: l'adapter non riscrive il linguaggio e quindi non impedisce
funzioni AGE aggiuntive, ma pubblica solo cio che il gate qualifica.

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
- `p.int32(int)` — forza un bind binario `integer`/`int4`
- `p.int64(int)` — forza un bind binario `bigint`/`int8`, anche per valori piccoli
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

## Bulk write Arrow

Il consumer Python passa dati Arrow a `prepare_write` + `write`; ogni provider
sceglie il proprio data path bulk. PostgreSQL usa COPY internamente:

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
- `update` / `upsert` / `delete_by_keys` — richiedono `keys` e, per update,
  accettano `update_columns`
Transaction profile: `single_transaction` (default) / `chunk_committed` /
`staged_swap` / `best_effort_ddl`.
Mapping policy: `compatible` (default) / `strict` / `lossy` / `native`.
`strict` boccia ogni loss anche minore (es. Arrow nullable → PG NOT NULL);
`compatible` tollera le loss non-DataLoss — scelta consigliata per input
pyarrow tipici (dove i campi sono nullable per default).

L'outcome è un dict con struttura `WriteOutcome` del core (status,
rows.confirmed / .inserted / .failed / .skipped, recovery).

## Observability

`Session.metrics()` restituisce un dizionario piatto di contatori interi,
compatibile con export Prometheus / OpenTelemetry:

```python
snap = s.metrics()
# {"pool_checkouts": N, "pool_reuses": N,
#  "schema_cache_hits": N, "writes_committed": N, ...}

# Esempio integrazione OpenTelemetry (structured log)
import logging
logger = logging.getLogger("plenora.db")
logger.info(
    "db.snapshot",
    extra={
        "db.pool_checkouts": snap["pool_checkouts"],
        "db.schema_cache_hits": snap["schema_cache_hits"],
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

`test_benchmark_parity.py` confronta live, sullo stesso commit e nello stesso
runner, una sessione SDK riusata con il subprocess CLI. E opt-in con
`PLENORA_BENCH_PARITY=1`: il risultato appartiene alla singola corsa e viene
registrato dal runner, non copiato qui come se valesse per ogni macchina.

## Compatibility

| superficie | dichiarata | verificata dai workflow |
|---|---|---|
| Python ABI | 3.10+ | import e suite wheel su Python 3.12; assurance SDK su 3.13 |
| Rust | 1.98 | 1.98 |
| piattaforme wheel standard | Linux x86_64, macOS arm64, Windows x86_64 | una suite offline per ciascun artefatto; DB2 fail-closed |
| artefatto DB2 | Linux x86_64 live; Windows x86_64 build-only | gate Db2 Linux; build, import e profilo nativo Windows |

Wheel: `abi3-py310` → un solo wheel per platform copre tutte le
versioni Python ≥ 3.10.

## Limitazioni

- **Selezione del prodotto** — PostgreSQL, MySQL, MariaDB, SQL Server e Db2 hanno
  factory distinte; non esiste selezione automatica dal server raggiunto.
- **Db2 su macOS** — non viene compilato nel wheel standard: manca una matrice
  client IBM supportata da qualificare, quindi la factory resta fail-closed.
- **Ripresa dello stream** — la lettura e incrementale, ma nessun provider
  pubblica un cursore riapribile da una seconda sessione.
- **Portable spatial DWithin unità SRS** — per predicato DWithin su
  colonna `geometry(*, 4326)` la distanza è in gradi, non metri.
  Usare `spatial.geography(...)` per unità metriche geodetiche.
- **ORM-like** — non c'e lazy loading implicito. Le relationship verso chiavi
  composite richiedono ancora un mapping FK esplicito e restano fail-closed;
  `joinedload` non accetta collezioni. L'ereditarieta e solo concrete, le
  migrazioni sono lineari e il runner Db2 non e ancora qualificato. Geometry
  ORM e qualificata sui cinque provider nei limiti dichiarati sopra; tipi,
  dimensioni o semantiche fuori da quella matrice restano chiusi.

## Sviluppo

### Test

```bash
python scripts/check_sdk_tests.py                  # i quattro provider del wheel standard
python scripts/check_sdk_tests.py --offline        # solo i test senza server
python scripts/check_sdk_tests.py --benchmark-only # solo i bench di parita
python scripts/check_sdk_tests.py --allow-dirty    # verdetto non autorevole
python scripts/check_db2_reference.py              # wheel DB2 e provider live dedicati
```

**Albero pulito.** Il runner rifiuta di partire se `git status --porcelain
-uall` non e vuoto: modifiche staged, non staged e file mai tracciati sono
tutti e tre un motivo di rifiuto. Il verdetto nomina un commit, e il wheel
si costruisce dai file su disco: se i due non coincidono, quel nome descrive
altro codice. `--allow-dirty` esiste per le corse esplorative e non fa
finta di niente — il verdetto esce con `authoritative: false`,
`worktree_dirty: true` e le righe di `git status` che lo hanno reso tale.

Il runner costruisce **sempre** wheel e CLI prima di `pytest` — stesso
container, stessa toolchain, stesso `Cargo.lock` — e li esporta in una
directory temporanea fuori dal repository: nel source tree non installa
niente, e un `.so` rimasto da una corsa precedente viene rimosso.

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

**Tracciato, non riproducibile.** `rust:1.98` e `python:3.13-slim` sono tag
mutabili e l'`apt-get` della build prende cio che il mirror pubblica oggi:
una seconda corsa non ricostruisce necessariamente lo stesso ambiente. Cio
che il runner garantisce e di dire con cosa ha girato — id e digest delle due
immagini, versione di rustc e di Python effettive.

**Ogni scope ha un contratto, e la corsa deve corrispondergli.** I conteggi
sono letti da `SCOPE_CONTRACTS` nel runner, che e l'unica fonte: ricopiarli qui
li farebbe diventare falsi al primo test aggiunto.

Un conteggio di soli `passed` non descrive una corsa. Uno skip e un
test che non ha risposto, e salta per motivi che somigliano a un errore di
configurazione — un binario spostato, una variabile che nessuno passa piu —
cioe resta verde proprio quando il gate ha smesso di misurare; una
deselezione fa lo stesso da un'altra porta, perche un `-k` che non seleziona
piu niente non e un errore per pytest.

Per `offline` il contratto va oltre il totale e fissa **quali** skip, e
quanti per motivo per i provider del wheel standard, DB2 escluso in modo
tipizzato, e per i benchmark opt-in.
Un totale coincidente e proprio cio che rende invisibile
una sostituzione — uno skip nuovo al posto di uno atteso lascia il numero
fermo. I valori stanno tutti in `SCOPE_CONTRACTS`, dentro il runner: quando
la suite cambia si aggiornano li, ed e il punto, perche un test aggiunto
diventa visibile invece di far crescere un numero senza dire di cosa.

Il verdetto JSON identifica cio che ha girato: commit, SHA-256 del wheel e
del modulo nativo **caricato da site-packages**, percorsi da cui e stato
importato, SHA-256 del CLI con feature e comando di build, i tre conteggi
verificati dal contratto, identita delle immagini e versioni effettive di
Python, rustc, maturin, pyarrow, pandas e pytest. Il nome del wheel da solo non identifica un artefatto — e lo stesso a
ogni build. Il runner verifica inoltre che ne' la build ne' i test abbiano
cambiato l'albero di lavoro, untracked inclusi.

### Benchmark opt-in

I benchmark di parita girano dentro il runner live (`PLENORA_BENCH_PARITY`
e gia impostato). Il confronto SDK / CLI e un rapporto fra due tempi, quindi
i due lati devono essere lo stesso codice: il CLI viene costruito dalla
stessa corsa che costruisce il wheel — con le feature esplicite registrate nel
verdetto — esportato
accanto ad esso e montato in sola lettura, e il runner ne passa il percorso
con `PLENORA_CLI_BIN`. Un binario preso da `target/release` del repository
sopravvive alle sessioni e nessuno ne sa il commit: il rapporto misurava due
codici diversi. Il verdetto ne porta digest, feature e comando di build, e
una guardia rifiuta qualunque percorso che ricada dentro il repository.

Per lanciarli da soli, sempre su artefatti appena costruiti, c'e l'opzione
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

Software proprietario. Vedi il file `LICENSE` alla radice del workspace.
