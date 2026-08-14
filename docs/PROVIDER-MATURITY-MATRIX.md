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
| MySQL | MySQL 8.0.46 e 8.4.11, immagini fissate per digest | `python scripts/check_mysql_matrix.py` e `python scripts/check_mysql_reference.py` |

MariaDB non è dedotto come compatibile con MySQL e non appartiene al riferimento
MySQL.

## Capability e assurance

| Area | PostgreSQL/PostGIS | SQL Server | MySQL 8.4 LTS |
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
| Scrittura bulk (Create, Replace, TruncateInsert) | live | live | live: tutti e 3 (v1.2). Replace usa staging persistent + `RENAME TABLE` atomico multi-table + DROP backup |
| Scrittura bulk (Upsert, Update, DeleteByKeys) | live | live | live: tutti e 3 (v1.2); Upsert via `ON DUPLICATE KEY UPDATE`, Update via staging TEMPORARY, DeleteByKeys via `WHERE (keys) IN` |
| Transazioni OLTP (`begin_transaction` + savepoints + `execute_ddl`) | live | fail-closed (aperto) | live (v1.2); `MysqlTransaction` con START/COMMIT/ROLLBACK/SAVEPOINT/ROLLBACK TO/RELEASE + conditional_update |
| Tipi scalari di riferimento | live, profilo esteso | live, profilo esteso | live: integer, decimal, bool, UTF-8, binary, date, datetime, JSON |
| Spatial generico | live | live | live: `GEOMETRY -> mixed` |
| Spatial tipizzato | live | live | live: `POINT` e `GEOMETRYCOLLECTION -> exact` |
| Dimensioni spatial | XY/XYZ/XYM/XYZM secondo gate | geometry/geography secondo gate | XY; Z/M/ZM non pubblicate |
| Contratto schema canonico | offline/live | offline/live | offline/live tramite `validate_schema_contract` |
| Gate fmt + Clippy `-D warnings` | sì | sì | sì |
| Gate live corrente | suite reference | 44 test live attesi | 34 test live attesi (23 v1.1 + 11 v1.2: OLTP + write modes) su 8.0.46 e 8.4.11 |

## Esito di parità

MySQL ha raggiunto disciplina di assurance e parità per le capability pubblicate:
connessione TLS, introspezione, query relazionale, lettura streaming bounded,
scrittura bulk (Append/Create/TruncateInsert/Upsert/DeleteByKeys/Update),
transazioni OLTP con savepoints + conditional_update, DDL raw via
`execute_ddl`, tipi dichiarati, spatial XY/SRID, reset, timeout, cancellazione,
rollback e quarantena.

La superficie non deduce capability non provate: Z, M e ZM continuano a fallire
chiuso; MariaDB non è qualificato; geography e spatial index non sono pubblicati;
spatial functions dichiarate `Vec::new()` — nessuna funzione ST_* è marked
verified nel capability probe. Questi limiti non possono essere nascosti tramite
feature flag né descritti come supportati.

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

- **Blocco A (write modes)**: 6 modi bulk aggiuntivi oltre Append —
  **Create** (CREATE TABLE dallo schema Arrow), **TruncateInsert**
  (TRUNCATE + INSERT bulk), **Upsert** (`INSERT ... ON DUPLICATE KEY UPDATE`),
  **DeleteByKeys** (`DELETE ... WHERE (keys) IN (...)`),
  **Update** (staging TEMPORARY + `UPDATE JOIN`),
  **Replace** (staging persistent + `RENAME TABLE` atomico multi-table
  + DROP backup). **Tutti 7 WriteMode ora qualificati per MySQL.**

- **Blocchi C/D (spatial)**: non affrontati in questo giro; le funzioni ST_*
  restano marcate `Vec::new()` nel capability probe (dichiarate ma non
  qualificate) e le dimensioni Z/M/ZM restano fail-closed. Non pianificati per
  MySQL v1.2 — MySQL 8 non ha geography né tipi 3D nativi.

Test live totali dopo v1.2: **126 unit + integration** (110 v1.1 + 16 nuovi
v1.2: 6 OLTP + 3 Create/TruncateInsert + 2 Upsert + 2 DeleteByKeys + 2 Update
+ 1 Replace).
