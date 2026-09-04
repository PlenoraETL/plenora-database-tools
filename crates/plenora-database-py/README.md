# plenora-database

Python SDK per `plenora-database-tools` — bindings PyO3 sopra al core
Rust del progetto. Espone PostgreSQL/PostGIS, MySQL, MariaDB, SQL Server,
Oracle e IBM Db2 LUW con API sync + async Pythonic, portable AST builder,
error hierarchy tipizzata, spatial predicates e context manager per
transazioni.

- **Provider**: PostgreSQL/PostGIS/AGE, MySQL, MariaDB, SQL Server, Oracle e
  IBM Db2 LUW condividono `EngineConfig` e lo stesso lifecycle sync/async
- **Sessioni**: nascono sempre dall'engine; SQL portabile e SQL raw hanno
  metodi e risultati distinti
- **Bulk**: mapping policy obbligatoria e capability fail-closed per provider
- **IBM Db2 LUW**: disponibile negli artefatti costruiti con feature `db2`;
  il wheel standard rifiuta l'uso con `PlenoraUnsupportedError`
- **Async**: `asyncio` bridge sopra al runtime tokio condiviso
- **Performance**: benchmark di parita SDK/CLI disponibile come test opt-in

## Install

Richiede Python **3.10+**. Il wheel è `abi3-py310` → un unico wheel
copre 3.10 / 3.11 / 3.12 / 3.13 / 3.14 per la stessa piattaforma.

### Da wheel pre-costruito (produzione)

Scaricare l'asset appropriato dalla
[pagina delle release](https://github.com/PlenoraETL/plenora-database-tools/releases),
quindi installare il file locale:

```bash
pip install ./plenora_database-<version>-cp310-abi3-<platform>.whl
```

Gli asset standard ufficiali sono pubblicati nelle release GitHub per Linux
x86_64 e Windows x86_64. Db2 usa invece l'asset Linux x86_64 con build tag
`1db2`, installabile esplicitamente:

```bash
pip install ./plenora_database-<version>-1db2-cp310-abi3-linux_x86_64.whl
```

Il runtime Db2 richiede unixODBC e il client IBM Db2 12.1 sul sistema. Il
driver `libdb2o` deve essere registrato in `odbcinst.ini` con il nome
`IBM DB2 ODBC DRIVER`, che e il default usato dall'adapter; `DB2DIR`,
`IBM_DB_HOME` e il percorso delle librerie devono puntare all'installazione
IBM. La qualifica live dell'asset avviene prima che il workflow lo alleghi alla
release; Windows resta build-only e non e distribuito come runtime Db2.

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
sessione per request. PostgreSQL, MySQL, MariaDB, SQL Server, Oracle e Db2
espongono lo stesso lifecycle provider-neutral.

```python
engine = p.engine_from_url(
    p.EngineConfig.from_url(
        "postgresql://user:password@database/application"
        "?max_connections=8&acquire_timeout_ms=5000"
    )
)

def handle_request(user_id: int):
    with engine.session() as session:
        with session.begin(native_query_policy="deny") as tx:
            users = p.table("users", "id", "name")
            statement = p.select(users.c.id, users.c.name).where(
                users.c.id == p.bind("identity", p.BindType.INTEGER)
            )
            return tx.execute(statement, {"identity": user_id}).one_or_none()

# allo shutdown dell'applicazione
engine.close()
```

La variante asyncio si crea con `await p.async_engine_from_url(config)` e usa
`async with engine.session()`; l'esempio completo sync/async e in
[`examples/core_v3_repository.py`](examples/core_v3_repository.py).
Gli ingressi pubblici sono `engine_from_url` e `async_engine_from_url`; le
factory per singolo provider non fanno parte del contratto 2.0.
Il prodotto è dichiarato nello schema URL (`mysql`, `mariadb`, `sqlserver`,
`oracle`, `db2`, `postgresql` o `age`) e viene verificato dalla probe prima che
l'engine sia restituito.

La stessa configurazione si puo costruire senza URL. `PoolConfig` rende
espliciti limite e timeout di acquisizione; Oracle e Db2 rifiutano questa
opzione finche i rispettivi provider non dispongono di una qualifica
equivalente:

```python
config = p.EngineConfig.from_postgres_dsn(
    dsn,
    pool=p.PoolConfig(max_connections=8, acquire_timeout_ms=5_000),
)
```

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
    .where(users.c.id >= p.bind("minimum_id", p.BindType.INTEGER))
    .order_by(users.c.id)
    .limit(50)
)

with engine.session() as session:
    result = session.execute(statement, {"minimum_id": 100})
    rows = result.all()

engine.close()
```

`Result` offre `all()`, `first()`, `one()`, `one_or_none()`, `scalar()` e le
varianti di cardinalita stretta `scalar_one()`/`scalar_one_or_none()`. Tutti i
terminali producono `Row`; SQL raw usa `query_sql` per un `Result` oppure
`execute_sql` per un `MutationResult`.

La superficie comprende inoltre `IN`/`BETWEEN`/`LIKE`/test di null, funzioni
scalar e aggregate tramite `func`, `GROUP BY`/`HAVING`, finestre e frame,
subquery scalari o derivate, `EXISTS`, CTE e `UNION`/`INTERSECT`/`EXCEPT`.
Espone anche aritmetica fra espressioni, `cast`, `case`, `NULLS FIRST/LAST`,
`DISTINCT ON`, join laterali e locking pessimista. Le combinazioni che il
contratto vieta, per esempio locking insieme a distinct o aggregazioni,
falliscono durante la preparazione.
Una CTE o subquery richiede label esplicite quando i nomi della proiezione non
sono determinabili. `Row` è accessibile per posizione, nome o descrittore
`Column`; `as_dict()` è la conversione esplicita verso un mapping.

`bind()` richiede sempre un tipo logico chiuso:

```python
statement = p.select(
    p.bind("answer", p.BindType.INTEGER).label("answer")
)
```

`BindType` non accetta dichiarazioni SQL arbitrarie: il renderer traduce
`BOOLEAN`, `INTEGER`, `BIG_INTEGER`, `FLOAT`, `STRING`, `BINARY`, `DATE`,
`TIMESTAMP`, `TIMESTAMP_TZ`, `DECIMAL`, `UUID` e `JSON` per il dialect. Db2
rende la forma tipizzata con un `CAST` e `SYSIBM.SYSDUMMY1`.
Oracle usa bind `:N`; il driver thin qualificato non conserva il tipo
`TIMESTAMP WITH TIME ZONE` in scrittura, quindi quel bind resta fail-closed.
La lettura dello stesso tipo, incluso l'offset UTC, e invece coperta dal gate
live.

### Letture riprendibili

Una lettura Arrow ordinata puo essere riaperta con un checkpoint keyset
persistente. Il chiamante cattura i valori ordinati dell'ultima riga realmente
consegnata, salva il JSON e lo passa a una nuova sessione:

```python
checkpoint = p.ReadCheckpoint(
    "postgres",
    "public",
    "events",
    [("tenant_id", "asc"), ("event_id", "asc")],
    [last_tenant_id, last_event_id],
    projection=["tenant_id", "event_id", "payload"],
)
stored = checkpoint.to_json()

resumed = p.ReadCheckpoint.from_json(stored)
reader = session.read(
    "public",
    "events",
    projection=["tenant_id", "event_id", "payload"],
    order_by=[("tenant_id", "asc"), ("event_id", "asc")],
    limit=10_000,
    checkpoint=resumed,
)
```

Provider, catalogo, sorgente, proiezione e ordinamento appartengono allo scope
firmato: un token riusato su una lettura diversa viene rifiutato prima
dell'I/O. `limit` puo cambiare fra le pagine. Il `repr` e gli errori pubblici
non espongono i valori contenuti nel token. La forma async usa lo stesso
oggetto con `await session.aread(...)`.

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
intervallo prima dell'I/O. `mapped_column(p.BIGINT)` dichiara invece SQL
`BIGINT`, verifica l'intervallo signed a 64 bit e usa un bind `int64` esplicito,
cosi il binding non deve indovinare la larghezza dal valore.
`String(length)`, `Numeric(precision, scale)`, `UUID`, `JSON` e
`DateTime(timezone=...)` completano i tipi dichiarativi. Le combinazioni non
qualificate, come un timestamp timezone nel DDL MySQL o Db2, restano chiuse.

Le query di entita partono da `orm.query(Account)` e compongono gli stessi
predicati dell'expression language. Oltre a ordinamento e paginazione espongono
join tramite relationship, proiezioni, tuple di entita e caricamento eager con
`selectinload` o `joinedload`. `joinedload` accetta anche collezioni e
deduplica le entita root mentre accumula i figli nell'identity map. I valori
restano bind separati. I loader accettano percorsi annidati tipizzati, per
esempio `selectinload(Customer.orders, Order.items)`; ogni livello successivo
viene caricato in batch. La query espone anche `offset`, `distinct`,
`group_by`, `having`,
`count`, `exists` e bulk `update`/`delete`. Il bulk DML resta chiuso per
joined-table e per query con join, grouping, ordering o paginazione, dove una
mutazione portabile e atomica non e qualificata. `refresh`, `expire`,
`expunge` e `merge` completano il lifecycle; l'autoflush e attivo per default
e si puo disabilitare nel costruttore o temporaneamente con `no_autoflush()`.
`partitions(batch_size)` e `stream(batch_size)` iterano query ordinate in
finestre limitate; `detach=True` mantiene limitata anche l'identity map. Non
sono dichiarate come server cursor: ogni finestra e una query ordinata.

Il flush raggruppa in un singolo insert multi-riga le istanze compatibili e
senza default da reidratare; `insert_batch_size` ne limita la dimensione.
`bulk_insert` e `bulk_upsert` lavorano direttamente su mapping omogenei quando
non serve associare istanze allo Unit of Work. SQL Server mantiene l'upsert
portabile a una riga per volta, come richiesto dal relativo lowering.

`savepoint`, `rollback_to_savepoint`, `release_savepoint` e il context manager
`begin_nested(name)` delegano ai savepoint della transazione Core e
ripristinano insieme lo stato del database e quello dello Unit of Work. La
superficie async mantiene lo stesso contratto con operazioni awaitable.

Una chiave `generated=True` e una colonna `server_default=True` possono essere
omesse dal costruttore. PostgreSQL, MariaDB e SQL Server le idratano dallo
statement di insert; MySQL e Db2 leggono prima l'identita locale della
connessione e poi i default nella stessa transazione. Un provider fuori da
questo insieme fallisce prima dell'I/O. Per generare DDL, il solo marker
`server_default=True` non basta: va fornito un `ServerDefault` esplicito.

`relationship` copre many-to-one, one-to-many, one-to-one e many-to-many con
tabella `secondary`, incluse chiavi composite su entrambi i lati.
`back_populates` mantiene coerenti i due lati e le cascade
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

`UniqueConstraint`, `CheckConstraint`, `ForeignKeyConstraint` e `OrmIndex`
descrivono vincoli e indici; i check usano una forma strutturale e non
accettano SQL raw. Le foreign key includono `ON DELETE` e `ON UPDATE`, mentre
`passive_deletes=True` si apre soltanto quando il mapping del figlio dichiara
la corrispondente foreign key `ON DELETE CASCADE`.
mixin astratti e forma concrete esplicita conservano colonne, vincoli e
relationship ereditati. Single-table supporta campi e relationship locali di
sottotipo e gerarchie multilivello; i campi locali devono essere nullable o
avere un server default per non invalidare le righe degli altri sottotipi.
Joined-table supporta gerarchie multilivello e applica insert, update e delete
a tutti i frammenti della lineage. `OrmMetadata` compila e applica create/drop
table nei dialetti qualificati. `MigrationRunner` e la variante async ordinano
un DAG di revisioni con branch e merge ed eseguono una revisione per
transazione, mentre `OrmSession.listen` registra hook locali sul lifecycle del
flush.

`AsyncOrmSession` espone lo stesso mapping, identity map, planner del flush e
regole di concorrenza. Le operazioni che fanno I/O sono coroutine; `add`,
`delete` e la composizione della query restano locali:

```python
async with p.AsyncOrmSession(session) as orm:
    orm.add(Account(id=1, name="Ada"))

async with p.AsyncOrmSession(session) as orm:
    account = await orm.query(Account).where(
        Account.name == p.bind("wanted", p.BindType.STRING)
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
        "app",
        "accounts",
        ingress.batches(lines, batch_size=1_024),
        mapping_policy="compatible",
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
    await session.acopy_from(
        "app", "accounts", batch, mapping_policy="compatible"
    )
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

with p.engine_from_url("postgresql://user:password@localhost/app") as engine:
    with engine.session() as s:
        caps = s.capabilities
        schemas = s.inspect.schemas()
        # SQL raw esplicito con parametri posizionali
        outcome = s.execute_sql(
            "INSERT INTO users(name) VALUES ($1)", ["Ada"]
        )
        assert outcome.affected_rows == 1
        cnt = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")
        rows = s.query_sql(
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
    engine = await p.async_engine_from_url(
        "postgresql://user:password@localhost/application"
    )

    async def scalar(i: int):
        # Ogni task usa la propria sessione; l'Engine condiviso governa il pool.
        async with engine.session() as session:
            return await session.execute_scalar("SELECT $1::int", [i])

    results = await asyncio.gather(*(scalar(i) for i in range(10)))
    await engine.aclose()

asyncio.run(main())
```

## Migrazione a 4.0

La 4.0 rende esplicito il confine fra contratto dell'artefatto e misure del
database connesso:

- `session.capabilities` e `AsyncSession.capabilities` restituiscono il
  documento comune Capability Discovery 2.0;
- `session.provider_capabilities` conserva il documento misurato sul provider;
- `test_connection` e `atest_connection` sono gli ingressi dedicati per una
  verifica redatta della connessione;
- `Engine.close()` e `await AsyncEngine.aclose()` sono gli alias di lifecycle
  canonici; `dispose()` resta disponibile per compatibilita;
- `PlenoraError.retry` e ora il mapping tipizzato del contratto comune, non una
  stringa. I dettagli strutturati sono in `PlenoraError.details`, con le
  diagnostiche di riga sotto `details["row_diagnostics"]`.

Il cambiamento di forma di `capabilities` e `retry` e incompatibile e motiva
la nuova major. Le applicazioni che interrogavano supporto specifico del
database devono passare a `provider_capabilities`.

## Graph con Apache AGE

La superficie graph usa PostgreSQL con l'estensione AGE. Le capability si
aprono soltanto sulla coppia qualificata AGE 1.7.0 / PostgreSQL 18; la versione
installata e il documento separato sono leggibili da `age_version` e
`age_capabilities`. Le operazioni amministrative hanno un documento additivo
separato, `age_admin_capabilities`, cosi il contratto AGE v1 resta invariato.

```python
config = plenora_database.EngineConfig.from_postgres_dsn(
    dsn, tls_mode="insecure_local"
)
with plenora_database.engine_from_url(config) as engine, engine.session() as session:
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

`vertex_model` ed `edge_model` associano dataclass o classi tipizzate a label,
identita ed estremi senza introdurre lazy I/O. `bulk_vertices`/`bulk_edges` e
le varianti async inviano batch tramite `UNWIND` con payload separato; gli edge
risolvono gli endpoint tramite business key. `graph_property_index_sql` e
`graph_unique_constraint_sql` producono DDL PostgreSQL qualificato sulle
proprieta AGE. Label, proprieta e parole chiave usate come nomi sono sempre
validate e quotate.

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
    tx.execute_sql("INSERT INTO t(x) VALUES ($1)", [1])
    row = tx.select("t").where_eq("x", 1).one()
    # commit auto su exit normale, rollback su eccezione

# Async equivalente:
async with await s.begin(isolation="serializable") as tx:
    await tx.insert("t").values(x=1).returning("id").one()
```

### Savepoints

```python
with s.begin() as tx:
    tx.execute_sql("INSERT INTO t(x) VALUES ($1)", [1])
    tx.savepoint("sp1")
    try:
        tx.execute_sql("... query rischiosa ...")
    except p.PlenoraError:
        tx.rollback_to_savepoint("sp1")
    tx.release_savepoint("sp1")
    # commit finale
```

## Typed params (bypass auto-inference)

Python `str` è ambiguo (text? uuid? timestamp?). Helper esplicit:

```python
s.execute_sql(
    "INSERT INTO events(id, ts, amount) VALUES ($1, $2, $3)",
    [
        p.uuid("550e8400-e29b-41d4-a716-446655440000"),
        p.timestamptz("2026-08-13T10:00:00+02:00"),
        p.decimal("1234.56"),   # preserva precisione, no float
    ],
)

# NULL con hint di tipo (quando il target non è inferibile)
s.execute_sql("INSERT INTO t(val) VALUES ($1)", [p.null("text")])
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
outcome = s.copy_from(
    "public", "measurements", tbl,
    mode="append", mapping_policy="compatible",
)
# {"status": "committed", "rows": {"received": 100000, "confirmed": 100000, ...}}

# ETL scratch — crea tabella dallo schema Arrow (nessun DDL preventivo)
outcome = s.copy_from(
    "public", "measurements_new", tbl,
    mode="create", mapping_policy="compatible",
)

# Async equivalente
outcome = await s.acopy_from(
    "public", "measurements", tbl, mapping_policy="compatible"
)
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
Mapping policy obbligatoria: `compatible` / `strict` / `lossy` / `native`.
`strict` boccia ogni loss anche minore (es. Arrow nullable → PG NOT NULL);
`compatible` tollera le loss non-DataLoss — scelta consigliata per input
pyarrow tipici (dove i campi sono nullable per default).

L'outcome è un dict con struttura `WriteOutcome` del core (status,
rows.confirmed / .inserted / .failed / .skipped, recovery).

## Integrazione applicativa

La configurazione provider-neutral conserva comunque una scelta esplicita del
prodotto nel protocollo URL:

```python
engine = p.engine_from_url(
    "mysql://app:password@db.internal/app?tls_mode=require"
)
```

`repr(EngineConfig)` non include utente, password o URL. Per asyncio usare
`await p.async_engine_from_url(...)`.

`compare_schema(OrmMetadata(...), reflected_metadata)` restituisce un piano
immutabile con fingerprint e rischio per operazione. `apply()` pre-valida
l'intero piano prima di eseguire DDL; `migration()` lo inserisce nel runner
esistente. Le strategie ORM `single` e `joined` si dichiarano in
`__mapper_args__`. La linea corrente qualifica gerarchie multilivello, campi
locali single-table e lifecycle joined-table su ogni frammento.

Per AGE, `graph_query()` costruisce MATCH, predicati e RETURN con parametri
separati, mentre `GraphSchema` / `compare_graph_schema()` gestiscono creazione
del grafo e indici dichiarati. Le label AGE non osservate non sono mai dedotte.

`DatabaseASGIMiddleware` e `session_dependency()` offrono una sessione async
per request. `explain()` restituisce `ExplainPlan`, `probe_engine()` un
`ProbeReport`, e `instrument_engine()` integra un tracer OpenTelemetry senza
registrare testo SQL, parametri o DSN.

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
    s.execute_sql(sql, params)
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

Le sottoclassi pubbliche corrispondono alle categorie del core Rust. L'elenco
autorevole è esportato da
[`errors.py`](python/plenora_database/errors.py) e descritto dagli stub PEP 561;
la documentazione non ne mantiene una seconda copia.

## Optimistic conflict pattern

```python
# UPDATE ottimistico con expected_version
outcome = (
    s.update("orders")
     .set(status="paid", version=current + 1)
     .where_eq("id", order_id)
     .where_eq("version", current)    # stale-check
     .execute()
)
if outcome.affected_rows == 0:
    # version cambiato sotto — refresh e riprova
    fresh = s.select("orders").columns("version").where_eq("id", order_id).scalar()
    ...
```

## Performance

`test_benchmark_parity.py` confronta live, sullo stesso commit e nello stesso
runner, una sessione SDK riusata con il subprocess CLI. E opt-in con
`PLENORA_BENCH_PARITY=1`: il risultato appartiene alla singola corsa e viene
registrato dal runner, non copiato qui come se valesse per ogni macchina.

## Compatibilità

Versioni, crate e capability dichiarate sono generati in
[`docs/STATO.md`](../../docs/STATO.md). Le piattaforme realmente distribuite si
leggono dagli asset della release e dal
[`python-wheel.yml`](../../.github/workflows/python-wheel.yml) che li costruisce
e li verifica; un artefatto assente non è supporto implicito.

## Limitazioni

- **Oracle** - provider thin, Arrow e ORM Spatial sono qualificati sul
  riferimento AMD64 fissato dal gate. I nomi di tabella, schema e colonna
  Spatial creati dall'ORM devono essere canonici maiuscoli, coerentemente con
  la normalizzazione di `USER_SDO_GEOM_METADATA`. Pool configurabile, identita
  generate, geography, TCPS live e bind timestamp con timezone restano chiusi
  e falliscono prima di promettere una semantica non provata. La matrice
  esatta delle capability aperte e generata in `docs/STATO.md`.
- **Selezione del prodotto** — il prodotto è dichiarato nell'URL o in
  `EngineConfig` e verificato dalla probe; non viene inferito dal server
  raggiunto.
- **Cursore server-side riapribile** — non viene pubblicata un'identita di
  cursore lato server. La ripresa supportata e keyset e usa `ReadCheckpoint`;
  e qualificata live sui cinque provider.
- **Portable spatial DWithin unità SRS** — per predicato DWithin su
  colonna `geometry(*, 4326)` la distanza è in gradi, non metri.
  Usare `spatial.geography(...)` per unità metriche geodetiche.
- **ORM-like** — non c'e lazy loading implicito. Le relationship verso chiavi
  composite richiedono un mapping FK esplicito. Il bulk DML non attraversa
  tabelle joined e non accetta join, grouping, ordering o paginazione.
  Geometry ORM e qualificata sui provider indicati dalla matrice generata;
  tipi, dimensioni o semantiche fuori da quella matrice restano chiusi.

## Sviluppo

### Test

```bash
python scripts/check_sdk_tests.py                  # i quattro provider del wheel standard
python scripts/check_sdk_tests.py --offline        # solo i test senza server
python scripts/check_sdk_tests.py --benchmark-only # solo i bench di parita
python scripts/check_sdk_tests.py --stabilization-only # 10 cicli runtime sensibili
python scripts/check_sdk_tests.py --allow-dirty    # verdetto non autorevole
python scripts/check_db2_reference.py              # wheel DB2 e provider live dedicati
python scripts/check_oracle_reference.py           # Oracle thin, CLI e wheel live
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

### Stabilizzazione ripetuta

La campagna schedulata esegue anche uno scope dedicato sul wheel installato:
dieci cicli di cancellazione tramite statement timeout, recupero della
sessione, rollback esplicito e venti query async concorrenti. Lo scope si puo
eseguire da solo, sempre con i riferimenti live accesi:

```bash
python scripts/check_sdk_tests.py --stabilization-only
```

## License

Software proprietario. Vedi il file `LICENSE` alla radice del workspace.
