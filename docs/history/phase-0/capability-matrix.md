# Matrice capability iniziale

Legenda:

- **C** — comportamento presente nel backend e da caratterizzare;
- **N** — nuovo nel port Rust;
- **P** — parziale/da verificare;
- **—** — non applicabile;
- **?** — decisione/prova necessaria.

La matrice non dichiara supporto di produzione. È la checklist di Fase 0.

## 1. Provider

| Capability | PG | MySQL | MariaDB | MSSQL | Oracle | SQLite | Db2 | DuckDB | ArcGIS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| test connection | C | C | P | C | C | C | N | N | C |
| server identity/version | C | C | C | C | C | C | N | N | C |
| secret redaction | C | C | C | C | C | C | N | N | C |
| TLS policy | P | P | C | C | P | — | N | — | C |
| capability probe | P | P | P | P | P | P | N | N | P |
| cancellation | P | P | P | P | P | P | N | N | HTTP cancel |
| pool/session reset | P | P | P | P | P | P | N | N | token/session |

## 2. Introspezione

| Capability | PG | MySQL | MariaDB | MSSQL | Oracle | SQLite | Db2 | DuckDB | ArcGIS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| list catalog/schema | C | C | C | C | C | P | N | N | folders/items |
| list table/layer | C | C | C | C | C | C | N | N | C |
| list views | C | C | C | C | C | C | N | N | service/layer |
| describe columns/fields | C | C | C | C | C | C | N | N | C |
| nullability/default | P | P | P | P | P | P | N | N | C/P |
| PK/unique/foreign key | P | P | P | P | P | P | N | N | OID/GID |
| indexes | P | P | P | P | P | P | N | N | service indexes P |
| generated/identity | P | P | P | P | P | P | N | N | ObjectID |
| native type preservation | P | P | P | P | P | P | N | N | field type/domain |
| geometry subtype | C | C | C/P | P | P | P | N | N | C |
| SRID/CRS | C | C/P | C/P | P | P | P | N | N | C |
| spatial index | P | P | P | P | P | P | N | N | capability P |

## 3. Lettura

| Capability | PG | MySQL | MariaDB | MSSQL | Oracle | SQLite | Db2 | DuckDB | ArcGIS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Arrow batch streaming | N | N | N | N | N | N | N | N | N |
| cursor/page bounded | C/P | C/P | C/P | C/P | C/P | C | N | N | C/P |
| projection pushdown | C | C | C | C | C | C | N | N | C |
| filter bind-safe | C | C | C | C | C | C | N | N | C/P |
| order/limit | C | C | C | C | C | C | N | N | C |
| aggregate/scalar | C | C | C | C | C | C | N | N | count/stat P |
| spatial WKB output | P | P | P | P | P | P | N | N | Esri JSON→WKB N |
| deterministic resume | ? | ? | ? | ? | ? | P | N | N | ObjectID windows P |
| schema drift detection | P | P | P | P | P | P | N | N | layer metadata P |

## 4. Scrittura

| Capability | PG | MySQL | MariaDB | MSSQL | Oracle | SQLite | Db2 | DuckDB | ArcGIS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| create | C | C | C | C | C | P | N | N | C |
| append | C | C | C | C | C | P | N | N | C/addFeatures |
| update by key | C | C | C | C | C | P | N | N | C/updateFeatures |
| upsert | C | C | C | C | C | P | N | N | C/P |
| truncate-insert | C | C | C | C | C | P | N | N | — |
| replace | C | C | C | C | C | P | N | N | C/P publish mode |
| delete by keys | P | P | P | P | P | P | N | N | C |
| bulk/native path | P | P | P | C/P | C/P | P | N | N | applyEdits/append P |
| streaming Arrow input | N | N | N | N | N | N | N | N | N |
| spatial write | C | C | C/P | C | C | P | N | N | C |
| spatial index | C | C/P | C/P | C | C | P | N | N | service-managed |
| multi-layer | — | — | — | — | — | — | — | — | C |

## 5. Atomicità e recovery

| Capability | SQL provider | ArcGIS |
|---|---|---|
| read-only | session/snapshot capability | paged query |
| all-or-nothing | transaction capability | solo scope provato di applyEdits |
| per-batch | commit esplicito | edit request per batch |
| staged replace | shadow/swap/redefinition | service/layer publish strategy |
| savepoint | capability vendor | non equivalente |
| rollback-on-failure | transaction | flag REST capability-gated |
| outcome unknown | commit response persa | edit response persa |
| idempotency | key/recovery protocol | GlobalID/chiave + protocollo |
| partial result | `ChunkCommitted` | per-feature/per-batch/per-layer |
| cleanup | staging/table | item/service/layer zombie |

## 6. Type mapping minimo

Ogni provider SQL deve caratterizzare:

- signed/unsigned integer;
- Decimal128/Decimal256 e overflow;
- float speciali;
- bool nativo/emulato;
- Unicode, charset, collation;
- binary/LOB;
- date/time/timestamp/timezone;
- interval;
- UUID/GUID;
- JSON/XML;
- array/composite/domain/enum;
- generated/identity;
- geometry/geography e Z/M.

ArcGIS deve caratterizzare:

- `esriFieldTypeOID`, GlobalID, GUID;
- small/integer/single/double;
- string e lunghezza;
- date e timezone/epoch semantics;
- blob/raster se esposti;
- coded-value/range domain;
- subtype;
- nullable/editable/default;
- Point, Multipoint, Polyline, Polygon, Envelope;
- `wkid/latestWkid/wkt`;
- hasZ/hasM;
- attachment e related record come capability separata.

## 7. Server/versione da congelare

La baseline iniziale deve registrare almeno una versione concreta per:

- PostgreSQL + PostGIS;
- MySQL 8;
- MariaDB;
- SQL Server con ODBC Driver 18;
- Oracle con Oracle Spatial;
- SQLite + eventuale SpatiaLite;
- Db2 + Spatial Extender;
- DuckDB + Spatial;
- ArcGIS Online;
- una versione ArcGIS Enterprise.

Finché una versione non è provata, la cella resta `?`/`N`, non “supportata”.

