# Inventario del backend Plenora

Snapshot iniziale di:

```text
C:\Users\Marco\Desktop\plenora\backend
```

L'inventario descrive la semantica osservata; non promuove automaticamente il
codice Python a specifica normativa.

## 1. Driver e dipendenze correnti

| Tipo backend | Client Python osservato | Lettura/introspezione | Scrittura | Spatial |
|---|---|---:|---:|---:|
| PostgreSQL | psycopg + SQLAlchemy | sì | sì | PostGIS |
| MySQL | PyMySQL + SQLAlchemy | sì | sì | tipi MySQL Spatial |
| MariaDB | ramo MySQL/SQLAlchemy | sì, compatibilità | sì, compatibilità | da caratterizzare |
| SQL Server | pyodbc + SQLAlchemy | sì | sì | geometry/geography |
| Oracle | python-oracledb + SQLAlchemy | sì | sì | SDO_GEOMETRY |
| SQLite | sqlite3 | sì | no nel writer | nessuna SpatiaLite completa |
| Db2 | assente | no | no | no |
| DuckDB | assente | no | no | no |
| ArcGIS Online/Enterprise | REST/provider | sì | sì | feature geometry Esri |

ArcGIS è incluso come provider non SQL.

## 2. Connessioni

Sorgenti principali:

- `app/core/connections/service.py`;
- `app/core/connections/models.py`;
- `app/core/connections/db.py`;
- `app/core/connections/connection_sets.py`;
- `app/shared/connections/mssql_tls.py`;
- `app/core/carica/router.py`;
- `app/core/estrai/database_inspector.py`.

Funzioni da portare come comportamento della black box:

| Funzione Python | Operazione candidata | Note |
|---|---|---|
| `ConnectionService.test_connection` | `database.test_connection` | dispatch driver |
| `_test_postgres` | driver probe PostgreSQL | `SELECT version()`, timeout 5s |
| `_test_mysql` | driver probe MySQL/MariaDB | `SELECT VERSION()` |
| `_test_mssql` | driver probe SQL Server | ODBC 18/17, TLS obbligatorio |
| `_test_oracle` | driver probe Oracle | query `v$version` |
| `_test_sqlite` | driver probe SQLite | `sqlite_version()` |
| `DefaultConnectionFactory.create_connection` | `DatabaseDriver::connect` | factory vendor |

Comportamenti normativi:

- timeout di connessione distinto;
- chiusura della sessione anche in errore;
- errore pubblico sanitizzato;
- TLS SQL Server abilitato;
- `TrustServerCertificate` solo come eccezione esplicita per connessione;
- escaping dei valori della connection string ODBC;
- client non installato classificato come capability build-time assente.

Fuori scope:

- CRUD della configurazione connessioni;
- connection set e risoluzione tenant/progetto;
- cifratura/persistenza delle password applicative.

Il runtime Rust riceverà `Endpoint` e `SecretProvider`; non replicherà le
tabelle di configurazione del backend.

## 3. Introspezione

Sorgente centrale:

```text
app/core/estrai/database_inspector.py
```

Componenti:

- `DefaultConnectionFactory`;
- `SQLQueries`;
- `TypeMapper`;
- `ResultProcessor`;
- `DatabaseInspector`.

Operazioni:

| Funzione | Operazione canonica |
|---|---|
| `get_tables_and_columns` | `database.list_objects` + `describe_object` |
| `get_views_and_columns` | list/describe view |
| `_inspect_postgres` | driver PostgreSQL catalog |
| `_inspect_mssql` | driver SQL Server catalog |
| `_inspect_mysql` | driver MySQL/MariaDB catalog |
| `_inspect_oracle` | driver Oracle catalog |
| `_inspect_sqlite` | driver SQLite catalog |

Metadata osservati:

- nome tabella/view;
- nome colonna;
- tipo nativo;
- PostGIS subtype e SRID tramite catalogo geometrico;
- MySQL SRS_ID quando disponibile, con fallback per versioni che non espongono
  la colonna;
- Oracle precision/scale;
- SQLite `PRAGMA table_info`.

Gap rispetto al contratto obiettivo:

- nullability;
- ordinalità esplicita in tutti i risultati;
- default e generated/identity;
- precisione/scala/lunghezza conservate;
- primary/unique/foreign key;
- indici normali e spaziali;
- collation/charset;
- timezone;
- geometry/geography SQL Server con SRID;
- Oracle SDO metadata;
- statistiche e capability per oggetto;
- tipi vendor-specific senza fallback silenzioso a stringa.

## 4. Mapping tipi corrente

`TypeMapper` normalizza verso:

```text
int | real | str | bool | date | geometry[:subtype[:srid]]
```

Regole particolari osservate:

- PostgreSQL `USER-DEFINED/geometry` usa subtype/SRID PostGIS;
- MySQL `tinyint(1)` diventa boolean;
- MySQL spatial conserva subtype e, se disponibile, SRS_ID;
- Oracle `NUMBER(1)` diventa boolean;
- Oracle `NUMBER(*, 0)` diventa intero;
- SQLite usa affinity euristica;
- tipi sconosciuti diventano stringa.

Classificazione: **legacy compatibile, non normativa**.

Il port Rust deve conservare il tipo nativo completo e mapparlo ad Arrow con
`MappingPolicy` e `LossReport`. Gli alias semplificati possono essere esposti
solo come vista compatibile per il canvas esistente.

## 5. Generazione query di lettura

Sorgenti:

- `app/core/estrai/query_generator.py`;
- `query_projection.py`;
- `query_filters.py`;
- `query_joins.py`;
- `app/db/sql_translator.py`;
- `app/db/sql_*_functions.py`;
- `app/db/sql_functions_registry.json`;
- `app/db/sql_safety.py`.

Funzioni osservate:

- SELECT e projection;
- FROM qualificato;
- join attributivi e spaziali;
- WHERE parametrizzato;
- GROUP BY;
- HAVING;
- ORDER BY;
- funzioni tipizzate/tradotte per dialect;
- validazione identificatori;
- preview con limit vendor-specific;
- generazione parametri.

Per database-tools il port viene diviso:

- AST e renderer portabile in `plenora-database-sql`;
- query native come operazione policy-gated;
- logica del canvas/grafo fuori scope.

La black box non riceve oggetti `Node`/`Edge` del backend: riceve un piano
database dichiarativo già indipendente dall'applicazione.

## 6. Esecuzione e streaming read

Sorgenti:

- `app/core/estrai/executor.py`;
- `app/core/query_execution/query_streaming.py`.

Comportamenti:

- esecuzione query SQL con parametri;
- preview;
- deduplicazione nomi colonna;
- Oracle normalizza nomi colonna in lowercase;
- timeout sessione best-effort:
  - PostgreSQL `statement_timeout`;
  - MySQL `max_execution_time`;
  - Oracle `callTimeout`;
- fetch `CHUNK_SIZE = 10_000`;
- limite configurabile, default osservato 5.000.000 righe;
- output CSV/Parquet/XLSX senza materializzazione completa nel percorso
  streaming;
- Decimal CSV serializzato senza passaggio via float.

Port Rust:

- produce `RecordBatch`, non file;
- mantiene limite incrementale;
- mantiene precisione decimal nel tipo Arrow;
- non normalizza case dei nomi senza una policy dichiarata;
- separa `fetch_rows`, `target_batch_bytes` e `max_batch_bytes`;
- espone timeout/fallback come metriche, non come debug silenzioso.

## 7. Configurazione write

Sorgente:

```text
app/core/carica/models.py
```

Write mode:

- `create`;
- `append`;
- `replace`;
- `update`;
- `upsert`;
- `truncate_insert`.

Transaction mode:

- `all_or_nothing`;
- `per_batch`.

Conflict policy per append:

- `skip`;
- `force`;
- `abort`.

Configurazione osservata:

- connessione, schema, tabella;
- column mappings;
- match/update columns;
- geometry column e SRID;
- batch size, default 1.000 righe;
- spatial index;
- `if_table_exists`;
- unique key;
- `allow_unsafe_append`.

Regola normativa: `append + per_batch + nessuna match column` è rifiutato per
default; richiede opt-in esplicito perché un retry può duplicare righe.

Regole da correggere:

- batch deve avere anche limite in byte;
- SRID non deve avere default implicito 4326 quando sconosciuto;
- `allow_unsafe_append` diventa un profilo non-atomico esplicito;
- configurazione non deve contenere segreti;
- tipi nativi arbitrari non devono essere stringhe non validate.

## 8. Writer e preflight

Sorgenti:

- `database_writer.py`;
- `writer_preflight_lifecycle.py`;
- `writer_data_prep.py`;
- `writer_data_prep_helpers.py`;
- `writer_introspection.py`;
- `column_mapper.py`;
- `conflict_checker.py`.

Preflight osservato:

- esistenza tabella;
- requisiti per mode;
- presenza match columns per update/upsert;
- column mapping;
- conflitti append;
- schema hints;
- generazione/creazione tabella;
- indice spatial opzionale;
- gate append non sicuro.

Preparazione dati:

- mapping colonna;
- normalizzazione valori;
- geometria convertita per il binder/dialect;
- righe in batch;
- progress reporting.

Il port Rust sposta ogni controllo remoto possibile in `prepare`, prima della
prima mutazione. I controlli dipendenti dai dati restano incrementali.

## 9. DDL e SQL di scrittura

Sorgente:

```text
app/core/carica/ddl_generator.py
```

Funzioni:

- qualificazione e quoting identificatori;
- allowlist/grammar per tipo SQL nativo;
- mapping pandas/Plenora → tipo SQL;
- create table;
- spatial index;
- drop/drop-if-exists;
- create-like;
- swap rename;
- truncate;
- insert;
- update;
- upsert:
  - PostgreSQL `ON CONFLICT`;
  - MySQL/MariaDB `ON DUPLICATE KEY`;
  - SQL Server `MERGE`;
  - Oracle `MERGE`;
- introspezione schema/tabelle/colonne.

Comportamenti da preservare:

- identificatori sempre quotati/escaped;
- valori bindati;
- tipo nativo validato con allowlist;
- placeholder geometry specifico del dialect;
- query catalogo parametrizzate dove possibile.

Comportamenti da riesaminare:

- uppercase automatico Oracle;
- `MERGE` SQL Server e relative condizioni di concorrenza;
- comment/version gate per SRID MySQL;
- fallback tipo a TEXT;
- literal necessari nei comandi utility;
- semantica dei trigger/default nel bulk path.

## 10. Transazioni e batch

Sorgenti:

- `writer_transaction_exec.py`;
- `writer_batch_loop.py`;
- `writer_batch_dispatch.py`.

Semantica osservata:

- `ALL_OR_NOTHING`: singola transazione, rollback totale su errore;
- `PER_BATCH`: commit per batch, batch precedenti restano;
- progress dopo ogni batch;
- zero righe è no-op riuscito;
- insert/update/upsert condividono il loop;
- SQL Server può usare `fast_executemany`;
- Oracle ha setup sessione specifico.

Gap:

- nessuno stato `OutcomeUnknown`;
- `WriteStatus.WRITE_ERROR` può nascondere se il commit è stato richiesto;
- conteggi upsert insert/update possono essere derivati, non provati;
- input è un DataFrame materializzato, non uno stream Arrow;
- batch size solo per righe.

## 11. Replace e truncate-insert

Sorgenti:

- `writer_replace_strategies.py`;
- `writer_oracle_redef.py`;
- `writer_preflight_lifecycle.py`.

Strategie osservate:

- replace atomico tramite shadow/staging;
- truncate-insert in transazione per carichi sotto soglia;
- stage + swap sopra soglia;
- Oracle `DBMS_REDEFINITION` per alcune operazioni;
- replica schema SQL Server sullo shadow;
- rename/swap vendor-specific;
- cleanup/telemetria.

Questa è la parte più delicata da congelare con fault injection. “Atomico” va
ridefinito per singolo driver/versione e non dedotto dal nome del metodo
Python.

## 12. Spatial write

Comportamenti osservati:

- PostGIS `geometry(Geometry, SRID)`;
- Oracle `SDO_GEOMETRY` e metadata/index;
- SQL Server `geometry`;
- MySQL/MariaDB `GEOMETRY` con SRID version-gated;
- placeholder/costruttori geometry specifici;
- indice spatial opzionale;
- geometry column auto-detection tramite nomi/tipi canvas.

Da correggere:

- niente auto-detection semantica basata solo sul nome;
- niente SRID 4326 implicito;
- geometry vs geography SQL Server esplicito;
- Z/M e subtype nel contratto;
- WKB canonico al confine;
- `LossReport` per conversioni.

## 13. Test Python già riutilizzabili come oracolo

### 13.1 Provider ArcGIS

Sorgenti principali:

- `app/shared/connections/arcgis_connection.py`;
- `app/shared/connections/arcgis_provider.py`;
- `app/core/features/arcgis_auth.py`;
- `app/core/features/arcgis_client.py`;
- `app/core/features/arcgis_read.py`;
- `app/core/features/arcgis_feature_operations.py`;
- `app/core/features/arcgis_feature_writer.py`;
- `app/core/features/arcgis_feature_write_methods.py`;
- `app/core/features/arcgis_rest_write.py`;
- `app/core/features/arcgis_service_discovery.py`;
- `app/core/features/arcgis_service_lifecycle.py`;
- `app/core/connections/arcgis_publish_modes.py`;
- `app/core/connections/arcgis_write_mapping.py`;
- `app/core/connections/arcgis_sql_helpers.py`.

Funzioni osservate:

- autenticazione e cache token;
- test connessione;
- listing folder/item/feature service;
- ricerca e lifecycle di servizi;
- introspezione layer e campi;
- query feature e conteggio;
- paginazione/lettura;
- creazione service/layer;
- mapping dataframe → campi/feature;
- add/update/delete;
- upsert mediante lookup della chiave e ObjectID;
- modalità publish su layer esistente;
- più layer;
- cleanup di servizi zombie;
- boundary per impedire rete live nei test unitari.

Port Rust:

| Python | Operazione/provider Rust |
|---|---|
| auth/token cache | `ArcGisAuthSession` + secret provider |
| provider/client | `plenora-provider-arcgis` |
| query layer | `provider.read` |
| count | `provider.scalar/count` |
| item/folder/service listing | `provider.list_objects` |
| layer schema | `provider.describe_object` |
| add features | append |
| update features | update |
| delete + add | upsert fallback esplicito |
| create service/layer | create |
| publish modes | replace/append/update physical strategies |
| multilayer | output per layer e orchestrazione bounded |

Regole:

- Esri JSON è formato interno al provider; il confine pubblico resta
  GeoArrow-WKB;
- spatial reference ArcGIS viene convertita in `CrsContract`, senza assumere
  che ogni `wkid/latestWkid` sia un EPSG equivalente;
- ObjectID e GlobalID sono metadata/chiavi speciali;
- token, referer e portal URL sensibili sono redatti;
- filtri `where` non vengono concatenati da valori non fidati;
- paginazione è capability-gated: offset quando affidabile, altrimenti finestre
  ObjectID;
- `maxRecordCount`, payload HTTP e feature per richiesta sono limiti distinti;
- retry rispetta `Retry-After` e non ripete edit con esito incerto;
- outcome e errori sono per feature, batch e layer;
- `rollbackOnFailure=true` non viene interpretato come transazione globale
  senza prova della capability e dello scope;
- replace di service/layer ha recovery e cleanup analoghi allo staging SQL.

Test oracolo specifici:

- `tests2/features/unit/test_arcgis_*`;
- `tests2/carica/unit/test_*arcgis*`;
- `tests2/connections/test_arcgis_*`;
- `tests2/shared/test_arcgis_provider_boundary_guard.py`;
- `tests2/integration/test_arcgis_real_*`;
- `test-e2e-v2/fake-rest/routes/arcgis.py`;
- `test-e2e-v2/fake-rest/fixtures/arcgis_layer.json`.

Gap:

- definire matrice Online/Enterprise e versioni REST;
- caratterizzare paginazione su layer senza `supportsPagination`;
- congelare semantica upsert e duplicati chiave;
- modellare edit result parziali e timeout dopo invio;
- definire replace senza finestra di indisponibilità quando possibile;
- benchmark rate limit, grandi geometrie e attachment;
- decidere attachment/related records come capability v1 o successiva.

Famiglie rilevate:

- `tests2/estrai/unit/test_database_inspector_*`;
- `tests2/estrai/unit/test_query_executor_streaming_contract.py`;
- `tests2/connections/test_*credential*`;
- `tests2/connections/test_mssql_*tls*`;
- `tests2/carica/unit/test_ddl_generator_*`;
- `tests2/carica/unit/test_database_writer_*`;
- `tests2/carica/unit/test_writer_*`;
- `tests2/carica/unit/test_bulk_writers_contract.py`;
- `tests2/carica/unit/test_uncertain_recovery_contract.py`;
- `tests2/integration/test_*_connection_and_estrai_preview.py`;
- `tests2/integration/test_*_carica_roundtrip.py`;
- `tests2/integration/test_*_carica_stage_swap.py`;
- `tests2/integration/test_rollback_regression_write_modes.py`;
- fixture SQL in `test-e2e-v2/sql/{postgres,mysql,sqlserver,oracle}`.

Questi test vengono caratterizzati, non copiati meccanicamente. Ogni caso
entra nel manifest differenziale con stato normativo/legacy/corretto.

## 14. Mappa iniziale Python → crate Rust

| Python | Rust |
|---|---|
| `connections/service.py` probe | driver crate + engine |
| `estrai/database_inspector.py` | driver introspection + core contracts |
| `estrai/query_generator.py` | database-sql |
| `db/sql_*` | database-sql catalog/renderers |
| `query_execution/query_streaming.py` | engine read executor |
| `carica/models.py` | database-core plan/outcomes |
| `carica/ddl_generator.py` | database-sql dialect DDL/DML |
| `carica/column_mapper.py` | core mapping + driver mapper |
| `carica/database_writer.py` | engine write executor |
| `writer_transaction_exec.py` | transaction protocol |
| `writer_replace_strategies.py` | driver physical strategies |
| `writer_oracle_redef.py` | Oracle driver |
| `conflict_checker.py` | engine preflight/data validation |
| `core/features/arcgis_*` | provider ArcGIS |
| `shared/connections/arcgis_provider.py` | trait/session ArcGIS |
| FastAPI/Celery/project storage | fuori scope |
