# Piano baseline della Fase 0

## 1. Scopo

Misurare il backend Python prima del port e separare:

- costo applicativo;
- SQLAlchemy/DataFrame;
- client/driver;
- rete;
- server;
- conversione spatial;
- formato finale.

La baseline principale di database-tools termina ad Arrow. CSV/Parquet/XLSX
sono misure informative del backend esistente, non target del port.

## 2. Implementazioni confrontate

```text
python-backend     comportamento corrente
python-driver-raw  client Python senza orchestration
rust-driver-raw    client Rust scelto, Fase 1+
rust-plenora       engine completo, Fase 2+
```

La Fase 0 produce le prime due.

## 3. Harness

Layout previsto:

```text
benchmarks/
├── manifests/
├── fixtures/
├── python/
├── sql/
├── arcgis/
├── raw/
└── baseline/
```

Ogni run emette JSONL:

```json
{
  "schema_version": 1,
  "benchmark": "read.full_scan.scalars",
  "implementation": "python-backend",
  "provider": "postgres",
  "server_version": "...",
  "rows": 1000000,
  "iterations": 5,
  "profile": "local",
  "transaction_profile": "read_only",
  "metrics": {
    "wall_ms": 0,
    "cpu_ms": 0,
    "peak_rss_bytes": 0,
    "rows_per_second": 0,
    "round_trips": 0
  }
}
```

## 4. Ambiente

Registrare:

- commit/snapshot backend;
- Python e lockfile;
- OS/kernel/container digest;
- CPU/RAM;
- database/versione/edition;
- estensione spatial;
- config server rilevante;
- driver/client library;
- storage;
- schema, indici e statistiche;
- latenza, banda e packet loss;
- cold/warm connection;
- cold/warm server cache;
- isolamento/durability;
- numero di run concorrenti.

Credenziali e endpoint sensibili non entrano nell'output.

## 5. Dataset iniziali

Riutilizzare i golden di `behavioral-compatibility.md` alle scale:

- 0;
- 1;
- 1.000;
- 100.000;
- 1.000.000;
- 10.000.000.

100M viene introdotto solo dopo verifica dei costi infrastrutturali.

Fixture generate deterministicamente da seed e checksum.

## 6. Read baseline

Per PostgreSQL, MySQL/MariaDB, SQL Server, Oracle e SQLite:

- connection test cold/warm;
- list tables/views;
- describe schema;
- preview 100/1.000;
- full scan narrow;
- full scan wide;
- projection;
- filter selectivity 1/50/100%;
- ordered read;
- decimal/timezone/LOB;
- geometry semplice/complessa;
- consumer veloce/lento;
- row cap;
- errore/cancellazione intermedia.

Misure:

- time to first row/batch;
- total wall/CPU;
- peak RSS;
- fetch count e size;
- rows/s e MB/s;
- conversion time;
- round trip;
- bytes rete quando disponibili.

## 7. Write baseline

Per ogni mode corrente:

- create;
- append;
- replace;
- update;
- upsert;
- truncate_insert.

Profili:

- all-or-nothing;
- per-batch;
- prepared/executemany/fast path disponibile;
- staging/swap;
- indice/constraint;
- spatial.

Scale e batch:

- 1k, 100k, 1M;
- batch 100, 1k, 10k righe;
- righe strette/larghe;
- conflitto 0/50/100%;
- rollback 1/50/99%;
- target vuoto/popolato.

Misure:

- rows/s;
- peak RSS;
- batch/commit/round trip;
- bind/conversion/server/finalize time;
- staging/index/swap time;
- righe insert/update/skip/fail;
- stato remoto;
- WAL/redo/temp quando disponibile.

## 8. Spatial baseline

- WKB/WKT/oggetto nativo path;
- Point 1M;
- LineString 50 e 10k vertici;
- Polygon con hole;
- MultiPolygon/collection;
- Z/M;
- SRID match/mismatch;
- null/empty;
- indice spatial;
- query bbox/esatta.

Misure:

- geometrie/s;
- coordinate/s;
- WKB byte/s;
- conversioni;
- payload p50/p95/max;
- memoria;
- round trip.

## 9. ArcGIS baseline

Profili:

- fake REST deterministico per gate;
- ArcGIS Online reale per smoke/baseline controllata;
- ArcGIS Enterprise per matrice versione.

Scenari:

- token cold/warm/refresh;
- folder/item/service discovery;
- layer introspection;
- count;
- read paginato offset;
- read tramite ObjectID;
- add/update/delete;
- upsert;
- publish existing/new service;
- multi-layer;
- geometrie semplici/grandi;
- `maxRecordCount` basso;
- HTTP 429;
- partial feature failure;
- timeout dopo invio edit.

Misure:

- HTTP requests;
- time to first page/batch;
- features/s e payload MB/s;
- page/edit size;
- conversione Esri JSON ↔ WKB;
- retry/rate-limit wait;
- token refresh;
- outcome per feature/batch/layer;
- servizi/layer orfani.

## 10. Strumentazione

Client:

- wall/CPU;
- process peak RSS;
- allocation sampling dove disponibile;
- conteggi a livello engine/driver;
- query/request counter tramite proxy/wrapper.

Server:

- PostgreSQL `pg_stat_statements`/WAL quando disponibile;
- SQL Server query stats/log bytes;
- MySQL performance schema;
- Oracle session/sql stats;
- Db2 monitor;
- ArcGIS request log/response headers dove accessibile.

Strumentazione non disponibile viene marcata `unknown`, mai zero.

## 11. Metodo statistico

- warmup separato;
- almeno 5 run per scenario breve;
- almeno 3 per run costose;
- mediana, p95 e dispersione;
- ordine randomizzato;
- fixture ripristinata;
- no altri carichi sui runner gate;
- timeout massimo;
- raw output conservato;
- fallimenti inclusi, non scartati silenziosamente.

## 12. Gate Fase 0

Prima dello scaffold Rust devono esistere almeno:

- baseline connection/introspection/read/write su PostgreSQL;
- baseline read/write su MySQL, SQL Server e Oracle;
- baseline read SQLite;
- baseline ArcGIS fake REST e uno smoke reale;
- memory slope read e write a due scale;
- round trip count;
- un caso spatial per provider corrente;
- fault case su rollback e risposta commit/edit persa.

Db2, DuckDB, SpatiaLite completo e ArcGIS Enterprise possono entrare come
baseline nuove durante le rispettive fasi, ma harness e manifest devono essere
già compatibili.

