# Matrice di maturità dei provider di riferimento

Questa matrice descrive il codice corrente e i gate riproducibili. Non sostituisce
il manifesto di release e non estende implicitamente le capability pubblicate da
`probe_capabilities`.

Legenda:

- **live**: comportamento esercitato contro il database di riferimento;
- **offline**: coperto da test senza database;
- **fail-closed**: capability non pubblicata e chiamata rifiutata;
- **aperto**: necessario per la parità indicata.

## Riferimenti

| Provider | Riferimento | Gate |
| --- | --- | --- |
| PostgreSQL/PostGIS | PostgreSQL 16 / PostGIS 3.4 | `python scripts/check_postgres_reference.py` |
| SQL Server | SQL Server 2022, compatibility level 160, immagine fissata per digest | `python scripts/check_sqlserver_reference.py` |
| MySQL | MySQL 9.7 LTS (baseline v1.2), 8.4.11 e 8.0.46 (matrice retrocompat), immagini fissate per digest | `python scripts/check_mysql_matrix.py` e `python scripts/check_mysql_reference.py` |

MariaDB non è dedotto come compatibile con MySQL e non appartiene al riferimento
MySQL.

## Capability e assurance

| Area | PostgreSQL/PostGIS | SQL Server | MySQL 9.7 (8.4/8.0 compat) |
| --- | --- | --- | --- |
| Connessione e identità server | live | live | live |
| TLS sul data path | live | live, inclusa CA privata/hostname/rotazione | live; CA privata, hostname positivo/negativo e `require-secure-transport=ON` |
| Pool bounded | live | live | live |
| Bootstrap dopo reset | live | live | live; stessa `CONNECTION_ID()` dopo reset, `setup` riapplicato |
| Timeout acquire/connect | live | live | offline/live; lease acquire e apertura connessione hanno budget distinti |
| Timeout operazione/deadline e quarantena | live | live | live; envelope `Timeout` distinto da cancellazione richiesta |
| Cancellazione in-flight e quarantena | live | live | live |
| Redazione credenziali | offline/live | offline/live | offline |
| Catalogo e describe | live | live | live |
| Lettura Arrow bounded/streaming | live | live | live; allocazione pre-bounded e drop anticipato con cleanup bounded |
| Projection/filter/order/limit bind-safe | live | live | live |
| Query relazionale pubblica | live | live | live; lifecycle prepare/query e drain completo |
| Scrittura bulk (Append) | live | live | live; SingleTransaction, rollback o quarantine |
| Scrittura bulk (Create, Replace, TruncateInsert) | live: Replace = `DELETE FROM` + insert in transazione (target conservato), `TruncateInsert` = `TRUNCATE` transazionale | live: Replace usa ancora staging + publish con backup, quindi **il target viene ricreato** — non riallineato al contratto | live: Create e Replace (`DELETE FROM` + insert nella stessa transazione InnoDB, target conservato). `TruncateInsert` fail-closed: `TRUNCATE` è DDL con commit implicito |
| Scrittura bulk (Upsert, Update, DeleteByKeys) | live | live | live: tutti e 3 (v1.2); Upsert via `ON DUPLICATE KEY UPDATE`, Update via staging TEMPORARY, DeleteByKeys via `WHERE (keys) IN` |
| Transazioni OLTP (`begin_transaction` + savepoints + `execute_ddl`) | live | fail-closed (aperto) | live (v1.2); `MysqlTransaction` con START/COMMIT/ROLLBACK/SAVEPOINT/ROLLBACK TO/RELEASE + conditional_update |
| Tipi scalari di riferimento | live, profilo esteso | live, profilo esteso | live: integer, decimal, bool, UTF-8, binary, date, datetime, JSON |
| Spatial generico | live | live | live: `GEOMETRY -> mixed` |
| Spatial tipizzato | live | live | live: `POINT` e `GEOMETRYCOLLECTION -> exact` |
| Dimensioni spatial | XY/XYZ/XYM/XYZM secondo gate | geometry/geography secondo gate | XY; Z/M/ZM non pubblicate |
| Contratto schema canonico | offline/live | offline/live | offline/live tramite `validate_schema_contract` |
| Gate fmt + Clippy `-D warnings` | sì | sì | sì |
| Gate live corrente | suite reference | 45 test live attesi | 37 live default + 25 live reference attesi, su 9.7.2 (baseline), 8.4.11 e 8.0.46 |

## Esito di parità

MySQL ha raggiunto disciplina di assurance e parità per le capability pubblicate:
connessione TLS, introspezione, query relazionale, lettura streaming bounded,
scrittura bulk (Append/Create/Replace/Upsert/DeleteByKeys/Update;
`TruncateInsert` fail-closed),
transazioni OLTP con savepoints + conditional_update, DDL raw via
`execute_ddl`, tipi dichiarati, spatial XY/SRID, reset, timeout, cancellazione,
rollback e quarantena.

La superficie non deduce capability non provate: Z, M e ZM continuano a fallire
chiuso; MariaDB non è qualificato; geography e spatial index non sono pubblicati.
Le funzioni `ST_*` fuori dal subset verified (`X`/`Y`/`Z`/`M`, `AsGeoJson`,
`DWithin`, `Transform`, ecc.) restano `Unsupported`. Questi limiti non possono
essere nascosti tramite feature flag né descritti come supportati.

## Decisione della metadata candidate 1.1.0

La minor release espone la nuova superficie MySQL relazionale e write già
qualificata live. Non dichiara equivalenza oltre il profilo pubblicato e non
promuove claim di sistema: le catene cross-library PostgreSQL/MySQL restano un
gate separato prima del tag.

## v1.2 MySQL — estensione delle capability qualificate

Post-1.1.0 il driver MySQL è stato esteso in tre blocchi:

- **Blocco B (OLTP)**: `Provider::begin_transaction` + `TransactionScope`
  (execute/query/savepoint/rollback_to/release/commit/rollback + conditional_update)
  + `Provider::execute_ddl`. Chiude il gap di parità con PostgreSQL sul path
  applicativo OLTP. Il `TransactionCapabilities.savepoints` passa da `false`
  a `true`.

- **Blocco A (write modes)**: 5 modi bulk aggiuntivi oltre Append —
  **Create** (CREATE TABLE dallo schema Arrow),
  **Replace** (`DELETE FROM` + INSERT bulk nella stessa transazione InnoDB,
  su un target che deve già esistere e non viene ricreato),
  **Upsert** (`INSERT ... ON DUPLICATE KEY UPDATE`),
  **DeleteByKeys** (`DELETE ... WHERE (keys) IN (...)`),
  **Update** (staging TEMPORARY + `UPDATE JOIN`).
  **Sei WriteMode disponibili su sette**: `TruncateInsert` resta fail-closed
  perché `TRUNCATE` è DDL con commit implicito e non è rollback-safe.

- **Blocco C (spatial functions verified)**: 26 funzioni `ST_*` dichiarate
  verified in `crate::query::VERIFIED_SPATIAL_FUNCTIONS` — metadata (7),
  predicate binary (5), metriche (3), constructor (3), transform (2),
  set operation (6). Il renderer condiviso `plenora-database-sql` è stato
  esteso: `render_spatial_predicate` supporta ora `Dialect::Mysql` con
  `ST_GeomFromWKB` (vs `ST_GeomFromEWKB` per Postgres) e `DialectCapabilities.
  spatial_intersects` passa da `false` a `true`. Test live: query con
  `ST_Area` in projection + WHERE `ST_Intersects(field, ?)` — entrambi
  pass end-to-end contro dataflow-mysql 8.4.
- **Blocco D (dims XYZ)**: non affrontato — MySQL 8/9 non ha supporto
  nativo per dimensioni 3D/M (`Z`/`M`/`ZM`); `spatial.dimensions` resta
  `[Xy]`. Non pianificato.

### Gate baseline aggiornato a MySQL 9.7 LTS

Post-Blocchi B/A/C, il gate `docker-compose.mysql.yml` usa MySQL 9.7 LTS
(primo LTS dopo 8.4, rilasciato 21 aprile 2026 — 5 anni premier + 3
estesi). Tutti i 62 test live — 37 default piu 25 reference — passano
identici su 9.7, 8.4 e 8.0: il protocollo binario MySQL è retrocompat sul
subset OLTP + Arrow bulk. La matrice 8.0.46/8.4.11/9.7 resta qualificata via
`check_mysql_matrix.py`.

### Consumer surface (post-sessione)

Dopo Blocchi B/A/C + upgrade 9.7:

- **CLI MySQL**: 9 sub-comandi (`mysql-probe`, `mysql-describe`,
  `mysql-inspect-schemas`, `mysql-inspect-tables`, `mysql-execute-sql`,
  `mysql-execute-ddl`, `mysql-execute-scalar`, `mysql-transaction-test`,
  `mysql-conditional-update`). Restano non esposti bulk-write,
  benchmark-*, diagnose, doctor, explain, pool-status, portable-execute e
  i test-* — nuova tranche futura.
- **SDK Python MySQL**, sync e async: `connect_mysql` / `aconnect_mysql`
  restituiscono una sessione con execute / execute_scalar /
  execute_returning_rows / execute_ddl, `begin` con savepoint,
  `SessionContext` e `native_query_policy`, `read`/`aread` streaming Arrow
  IPC bounded, `copy_from`/`acopy_from` bulk e builder AST portabili
  (`select/insert/update/delete/upsert`), piu context manager. Non
  esposto: spatial predicates + `SpatialReference`.

Inventario corrente del provider MySQL, verificato contro la sorgente da
`scripts/mysql_inventory.py` a ogni esecuzione del gate: **150 unit**,
**37 live default** (test live senza `#[ignore]`) e
**25 live reference** (test live `#[ignore]`). I conteggi
non vanno aggiornati a mano: il gate fallisce se divergono.
