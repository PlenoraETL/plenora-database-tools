# Provider MySQL

Baseline di riferimento: **MySQL 9.7.2**. La matrice versionata qualifica
anche **MySQL 8.4.11 LTS** e **MySQL 8.0.46** come riferimenti di
compatibilità, per installazioni legacy. Versione esatta e digest immutabile
di ciascun riferimento sono dichiarati una volta sola in
[`docker/mysql/references.json`](../../docker/mysql/references.json): il
compose della baseline, i due gate e i test live leggono quel documento, così
nessuno può affermare una versione diversa da quella effettivamente avviata.
I 50 test live passano identici su 9.7, 8.4 e 8.0 — la superficie
`plenora-db-mysql` è dialect-invariante tra 8.x e 9.x. Il provider usa protocollo
nativo asincrono e TLS rustls; MariaDB resta fuori scope fino a una qualifica
indipendente.

## Stato qualificato

- configurazione strutturata e credenziali redatte;
- TLS obbligatorio; CA privata isolata dal server, rigenerazione completa su
  artefatti parziali e hostname DNS/IP positivo/negativo provati live;
- budget connect/operazione/acquire configurabili; il checkout acquisisce prima
  un permit del semaforo di pool entro `acquire_timeout`, poi attende la
  connessione entro `connect_timeout`;
- bootstrap UTC, strict SQL mode e autocommit deterministico;
- bootstrap applicato sia all'apertura sia dopo il reset della connessione;
- quarantena su timeout, cancellazione ed errori fatali di trasporto;
- errori pubblici redatti, outcome write ambiguo e quarantena della sessione;
- introspezione e lettura Arrow streaming bounded provate live, incluso drop
  anticipato del consumer con cleanup bounded;
- `GEOMETRY -> mixed` e tipi spatial concreti, inclusi `POINT` e
  `GEOMETRYCOLLECTION -> exact`, validati contro il contratto canonico;
- query relazionale qualificata con bind posizionale e lifecycle bounded;
- write bulk qualificato dentro `SingleTransaction`, con rollback certo
  prima del commit e outcome `Unknown` più quarantena quando il commit è
  ambiguo. **v1.2**: tutti 7 modi qualificati — Append + **Create** (CREATE
  TABLE dallo schema Arrow) + **TruncateInsert** (TRUNCATE + INSERT bulk) +
  **Upsert** (`INSERT ... ON DUPLICATE KEY UPDATE`) + **DeleteByKeys**
  (`DELETE ... WHERE (keys) IN (...)`) + **Update** (staging TEMPORARY +
  `UPDATE JOIN`) + **Replace** (staging persistent + `RENAME TABLE` atomico
  multi-table + DROP backup);
- **v1.2**: transazioni OLTP applicative via `Provider::begin_transaction`
  qualificate — `START TRANSACTION` con isolation/access mode/statement
  timeout, savepoint annidati (`SAVEPOINT` / `ROLLBACK TO SAVEPOINT` /
  `RELEASE SAVEPOINT`), `execute_conditional_update` con pattern
  savepoint-check-rollback per optimistic-lock. `TransactionCapabilities.
  savepoints` passa da `false` a `true`;
- **v1.2**: DDL raw via `Provider::execute_ddl` — MySQL fa autocommit su
  DDL (non transazionale, come dichiarato dalle capabilities esistenti);
- WKB XY con SRID dichiarato qualificato in lettura e scrittura; SRID embedded,
  Z, M e ZM restano fail-closed sulle versioni della matrice;
- Staged swap raw e `LOCAL INFILE` non sono capability pubblicate.
  Geography è assente in MySQL 8 e non emulata;
- **v1.2 Blocco C**: 26 funzioni spatial `ST_*` verified pubblicate in
  `mysql_spatial_capabilities().functions` — metadata (`ST_GeometryType`,
  `ST_SRID`, `ST_Dimension`, `ST_NumPoints`, `ST_IsEmpty`, `ST_IsValid`,
  `ST_IsClosed`), predicati (`ST_Intersects`, `ST_Contains`, `ST_Within`,
  `ST_Disjoint`, `ST_Equals`), metriche (`ST_Distance`, `ST_Area`,
  `ST_Length`), constructor (`ST_StartPoint`, `ST_EndPoint`, `ST_PointN`),
  transform (`ST_Buffer`, `ST_Envelope`), set operation
  (`ST_Intersection`, `ST_Union`, `ST_Difference`, `ST_SymDifference`,
  `ST_ConvexHull`, `ST_Centroid`). Il renderer condiviso
  `plenora-database-sql::render_spatial_predicate` è stato esteso per
  `Dialect::Mysql` (usa `ST_GeomFromWKB` invece di `ST_GeomFromEWKB`).
  Funzioni fuori dal subset (`ST_X`/`Y`/`Z`/`M`, `ST_AsGeoJSON`,
  `ST_DWithin`, `ST_Transform`, ecc.) restano `Unsupported`.

Il gate riproducibile è `python scripts/check_mysql_reference.py`. Esegue fmt,
Clippy con warning negati, i test della fixture e tre famiglie di test
identificate per nome sulla baseline fissata per digest:

| Famiglia | Runner | Test |
| --- | --- | --- |
| unit | `cargo test -- --skip live_` | 121 |
| live default | `cargo test live_` | 25 |
| live reference | `cargo test live_ -- --ignored` | 25 |

`live default` sono i test live **non** `#[ignore]`: una `cargo test` nuda li
esegue, quindi richiedono comunque il riferimento acceso e hanno un runner
proprio invece di comparire come rumore. Prima di avviare qualunque container
il gate confronta i tre inventari con la sorgente Rust
(`scripts/mysql_inventory.py`): un test aggiunto e mai eseguito, o rimosso e
mai notato, fa fallire il gate invece di restare invisibile.

Il gate prestazionale `python scripts/check_mysql_performance.py`
applica un budget assoluto a read e Append/SingleTransaction, richiede
differenziale zero e un solo commit osservato; non dichiara una baseline
misurata finché non ne esiste una comparabile. La matrice completa
(baseline 9.7 più compatibilità 8.4 e 8.0) è
`python scripts/check_mysql_matrix.py`, che per ogni riferimento riesegue
entrambe le famiglie live senza esclusioni. La matrice comparativa dei provider è
in [`docs/PROVIDER-MATURITY-MATRIX.md`](../PROVIDER-MATURITY-MATRIX.md).

Il DDL atomico MySQL non viene equiparato a DDL transazionale (autocommit
implicito). L'implementazione v1.2 di Create/TruncateInsert/Replace usa
`exec_control` via text protocol; Replace usa staging PERSISTENT (non
TEMPORARY perché `RENAME TABLE` rifiuta temporanee) con cleanup del
backup post-swap; se la swap fallisce, la staging orfana viene droppata
via best-effort.

## Consumer surface

Post-Blocchi B/A/C, il driver MySQL è accessibile dai consumer:

**CLI** (8 sub-comandi, tutti behind `--features full`):

```
plenora-database mysql-probe <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>]
plenora-database mysql-describe <args...> <schema> <object>
plenora-database mysql-inspect-schemas <args...>
plenora-database mysql-inspect-tables <args...> <schema>
plenora-database mysql-execute-sql <args...> <sql>
plenora-database mysql-execute-ddl <args...> <sql>
plenora-database mysql-execute-scalar <args...> <sql>
plenora-database mysql-transaction-test <args...>
```

**SDK Python** (v0.4-alpha, scaffold):

```python
import plenora_database as p

with p.connect_mysql("localhost", "mydb", "user", "pwd") as s:
    n = s.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
    v = s.execute_scalar("SELECT COUNT(*) FROM t")
    rows = s.execute_returning_rows("SELECT id, label FROM t WHERE id > ?", [0])
    s.execute_ddl("CREATE INDEX idx_label ON t(label(64))")
```

Non incluso nel SDK MySQL v0.4 (roadmap): `begin()` + `Transaction`
(savepoints), `copy_from` bulk write, `read()` streaming Arrow,
portable AST builders (`select/insert/update/delete/upsert`), spatial
predicates + `SpatialReference`, `AsyncMysqlSession`.

Non incluso nel CLI MySQL v1.2 (roadmap): `bulk-write` (Arrow IPC),
`benchmark-*`, `diagnose`, `doctor`, `explain`, `pool-status`,
`conditional-update`, `test-cancellation/streaming/spatial/concurrency`,
`portable-execute`.
