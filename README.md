# plenora-database-tools

Toolkit database multipiattaforma in Rust, con SDK Python e CLI, per
applicazioni, pipeline dati e workload geospaziali. Un unico modello
relazionale governa query, transazioni, mapping ORM-like, streaming Arrow e
comportamento dei provider.

Il progetto è costruito attorno a tre idee:

- i valori applicativi restano separati dagli statement tramite bind;
- una capability è disponibile soltanto quando una prova riproducibile la
  sostiene;
- ciò che un provider non ha qualificato fallisce prima dell'I/O, invece di
  degradare silenziosamente.

La matrice corrente dei provider, delle capability, dei crate e dei test è in
[`docs/STATO.md`](docs/STATO.md), generato direttamente dai sorgenti.

## Cosa offre

| superficie | ruolo |
| --- | --- |
| Core relazionale | expression language immutabile, query e DML con bind separati, risultati tipizzati e transazioni esplicite |
| ORM-like | mapping dichiarativo sync/async, identity map, unit of work, relazioni, eager loading, concorrenza ottimistica, DDL e migrazioni lineari |
| Data plane | lettura streaming e scritture bulk basate su Arrow |
| Spatial | metadati GeoArrow, geometrie WKB/EWKB, validazione CRS e operazioni spatial qualificate per provider |
| Graph | Cypher parametrizzato e valori `agtype` tipizzati tramite Apache AGE su PostgreSQL |
| SDK Python | API applicativa, engine/session lifecycle, factory per provider, stub PEP 561 e binding PyO3 |
| CLI | inspect, probe, read, write e diagnostica sugli stessi contratti del core |
| Assurance | contratti JSON Schema, golden test, fixture reali, matrici live, benchmark e fuzzing |

Il dettaglio dell'API Python, inclusi esempi sync/async e limiti dichiarati, è
nel [`README dello SDK`](crates/plenora-database-py/README.md).

## Come viaggiano i dati

Plenora distingue il percorso applicativo dal percorso bulk:

- query OLTP e ORM usano parametri tipizzati e restituiscono `Result`, righe o
  oggetti mappati;
- letture e scritture ad alto volume usano stream Arrow, senza convertire ogni
  riga in un oggetto intermedio;
- le geometrie viaggiano come WKB/EWKB con frame CRS e metadati GeoArrow,
  mantenendo il payload fuori dall'AST e dai messaggi di errore.

JSON non è il formato del data plane né il contratto interno fra i layer. Lo
SDK Python espone però un adattatore di ingresso: valida mapping, documenti
JSON e JSON Lines contro uno schema chiuso, poi produce oggetti ORM o batch
Arrow. Tipi, nullability e CRS vengono dichiarati prima di leggere i dati;
non sono inferiti dal payload.

```python
import plenora_database as p

ingress = p.JsonInput(
    p.JsonSchema(
        [
            p.JsonField("id", int),
            p.JsonField("name", str),
            p.JsonField(
                "shape",
                p.JsonGeometry(
                    srid=4326,
                    geometry_type="point",
                    encoding="ewkb",
                ),
            ),
        ]
    )
)

with open("places.jsonl", encoding="utf-8") as lines:
    session.copy_from("app", "places", ingress.batches(lines))
```

Il file JSON Lines viene letto incrementalmente; ogni geometria GeoJSON viene
convertita al bordo in WKB/EWKB e il batch porta i metadata GeoArrow canonici.
La forma async, la conversione verso modelli e i limiti operativi sono nel
[`README dello SDK`](crates/plenora-database-py/README.md#ingresso-json-tipizzato).

## Installazione Python

Da wheel:

```bash
pip install plenora-database
```

Da sorgenti, per lo sviluppo:

```bash
pip install maturin
cd crates/plenora-database-py
maturin develop --release
python -c "import plenora_database as p; print(p.version())"
```

Versioni Python supportate, modalità di build e particolarità del wheel Db2
sono mantenute nel [`README dello SDK`](crates/plenora-database-py/README.md#install).

## Quickstart Core

L'`Engine` è longevo e condivisibile; ogni request o task apre la propria
sessione. Gli statement sono oggetti immutabili e i valori rimangono nei bind.

```python
import os

import plenora_database as p

accounts = p.table("accounts", "id", "name", schema="app")
statement = (
    p.select(accounts.c.id, accounts.c.name)
    .select_from(accounts)
    .where(accounts.c.id == p.bind("account_id"))
)

engine = p.create_engine(os.environ["PLENORA_DATABASE_DSN"])
try:
    with engine.session() as session:
        account = session.execute(
            statement,
            {"account_id": 7},
        ).one_or_none()
finally:
    engine.dispose()
```

Per un esempio completo con repository e lifecycle sync/async, vedere
[`core_v3_repository.py`](crates/plenora-database-py/examples/core_v3_repository.py).

## Quickstart ORM-like

Il mapping dichiarativo riusa le stesse `Table`, `Column`, espressioni e
transazioni del Core. La sessione ORM possiede una transazione: il context
manager esegue flush e commit in uscita normale e rollback in caso di errore.

```python
import os

import plenora_database as p


class Account(p.DeclarativeBase):
    __tablename__ = "accounts"
    __schema__ = "app"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    name: p.Mapped[str] = p.mapped_column(nullable=False)
    version: p.Mapped[int] = p.mapped_column(version=True)


with p.create_engine(os.environ["PLENORA_DATABASE_DSN"]) as engine:
    with engine.session() as core_session:
        with p.OrmSession(core_session) as orm:
            account = orm.get(Account, 7)
            if account is None:
                orm.add(Account(id=7, name="Ada"))
            else:
                account.name = "Grace"
```

La stessa mappatura è utilizzabile con `AsyncOrmSession`. Relazioni, loader,
chiavi composite, default generati, schema lifecycle e limitazioni puntuali
sono documentati nella sezione
[`ORM-like dichiarativo sync e async`](crates/plenora-database-py/README.md#orm-like-dichiarativo-sync-e-async).

### Geometrie mappate

Una colonna ORM geometrica dichiara esplicitamente SRID, dimensioni, semantica
e, quando serve, tipo concreto:

```python
import plenora_database as p


class Place(p.DeclarativeBase):
    __tablename__ = "places"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    shape: p.Mapped[p.SpatialReference] = p.mapped_column(
        p.Geometry(srid=4326, geometry_type="Point"),
        nullable=False,
    )
```

Il supporto spatial non viene generalizzato per somiglianza fra dialetti: la
connessione pubblica le capability effettive del server raggiunto e i percorsi
non qualificati restano chiusi.

## Architettura

```text
SDK Python / CLI / API Rust
            |
            v
    IR relazionale canonico
            |
            v
 validazione contratto + capability
            |
            v
 lowering e bind del provider
            |
            v
 PostgreSQL / MySQL / MariaDB / SQL Server / Db2
```

L'ORM-like è un layer applicativo sopra lo stesso IR; non introduce un secondo
linguaggio SQL. Arrow resta il percorso dedicato ai flussi tabellari e
geospaziali di volume, mentre query e ORM coprono il lavoro OLTP.

Le capability dipendono anche dal server concreto, dalle estensioni installate
e dalla piattaforma. Per questo la fonte non è una tabella copiata qui:

- [`docs/STATO.md`](docs/STATO.md) espone lo stato generato dal codice;
- `session.capabilities` e `database-probe` descrivono il target realmente
  raggiunto a runtime.

## Confini intenzionali

Plenora non promette parità indistinta con SQLAlchemy. Pubblica una superficie
ORM-like verificata e lascia chiuso ciò che non possiede ancora una prova. In
particolare, l'I/O delle relazioni non è mai implicito: il caricamento avviene
con una richiesta esplicita o con un loader dichiarato nella query.

I limiti precisi e aggiornati vivono accanto all'implementazione e agli stub
nel [`README dello SDK`](crates/plenora-database-py/README.md#limitazioni). La
direzione di evoluzione, che non rappresenta lo stato corrente, è descritta in
[`database-core-v3-roadmap.md`](docs/database-core-v3-roadmap.md).

## Orientarsi nel repository

| serve | sta in |
| --- | --- |
| stato generato di codice, capability e prove | [`docs/STATO.md`](docs/STATO.md) |
| guida completa dello SDK Python | [`crates/plenora-database-py/README.md`](crates/plenora-database-py/README.md) |
| contratto pubblico | [`contracts/v2/`](contracts/v2/README.md) |
| uso dei fixture e operazioni locali | [`docs/operativo.md`](docs/operativo.md) |
| benchmark e relativi harness | [`benchmarks/README.md`](benchmarks/README.md) |
| evidenza generata MariaDB | [`docs/mariadb/`](docs/mariadb/EVIDENCE.md) |
| regole non negoziabili del repository | [`AGENTS.md`](AGENTS.md) |
| motivazione storica delle decisioni | `git log` |

`docs/STATO.md` non si modifica a mano. Se deve cambiare, cambia la sorgente da
cui viene generato e si riesegue il relativo gate.

## Costruire

Il workspace usa la toolchain fissata in `rust-toolchain.toml`.

```powershell
cargo build --workspace --all-features
cargo build --release --package plenora-database-cli --all-features
```

Il secondo comando produce il CLI `plenora-database`. I wheel Python hanno il
proprio workflow in `.github/workflows/python-wheel.yml`.

## Cosa fa girare le prove

Il percorso offline canonico è lo sweep. I gate live sono separati per
provider: un risultato verde su un database non qualifica gli altri.

```powershell
python scripts\sweep.py                    # intera suite offline locale
python scripts\phase0_validate.py          # contratti, esempi, golden, domini
python scripts\check_docs.py               # coerenza strutturale dei documenti
python scripts\check_comments.py           # standard dei commenti
python scripts\check_test_layout.py         # test Rust separati dal prodotto
python scripts\check_cargo_deny.py          # supply chain in Docker, tool fissato
python scripts\check_mysql_reference.py --static
python scripts\check_mysql_reference.py     # gate MySQL live
python scripts\check_postgres_reference.py  # gate PostgreSQL live
python scripts\check_age_reference.py       # gate AGE 1.7.0 / PostgreSQL 18 live
python scripts\check_sqlserver_reference.py # gate SQL Server live
python scripts\check_db2_reference.py       # gate IBM Db2 LUW live
python scripts\check_sdk_campaign.py        # wheel + fixture relazionali e AGE
cargo test --workspace -- --skip live_      # unit, senza server
cargo deny check                            # alternativa se cargo-deny è installato
```

I singoli runner di riferimento presuppongono il proprio fixture già healthy;
le campagne e i workflow gestiscono invece l'intero ciclo di vita Compose.
Ogni provider usa un progetto e volumi dedicati.

## Regole che non cambiano

- una capability resta `false` finché non esiste una prova riproducibile che
  la sostiene; un gate saltato non è un gate passato;
- una modifica incompatibile del contratto richiede una nuova major;
- i documenti non duplicano fatti del codice: li generano oppure rimandano
  alla fonte;
- gli errori pubblici non includono valori di celle, righe, DSN, token o SQL
  bindato;
- un gate nuovo entra in un workflow che viene realmente eseguito.

Le regole complete sono in [`AGENTS.md`](AGENTS.md).

## Licenza

Software proprietario. I termini sono in [`LICENSE`](LICENSE).
