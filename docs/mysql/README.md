# Provider MySQL

Baseline di riferimento: MySQL 8.4 LTS. La matrice versionata qualifica MySQL
8.0.46 e 8.4.11 su immagini fissate per digest. Il provider usa protocollo
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
  ambiguo. **v1.2**: modi qualificati Append + **Create** (CREATE TABLE
  dallo schema Arrow) + **TruncateInsert** (TRUNCATE + INSERT bulk) +
  **Upsert** (`INSERT ... ON DUPLICATE KEY UPDATE`) + **DeleteByKeys**
  (`DELETE ... WHERE (keys) IN (...)`) + **Update** (staging TEMPORARY +
  `UPDATE JOIN`). Residuo Replace (staging + RENAME atomico) — non
  pubblicato;
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
- Replace, staged swap e `LOCAL INFILE` non sono capability pubblicate.
  Le funzioni spatial ST_* sono dichiarate `Vec::new()` nel capability
  probe (nessuna funzione marked verified); geography è assente in
  MySQL 8 e non emulata.

Il gate riproducibile è `python scripts/check_mysql_reference.py`. Esegue fmt,
Clippy con warning negati, test della fixture, ~110 test offline e ~32 test live
identificati per nome sul riferimento fissato per digest (v1.2: 22 live baseline
+ 10 live v1.2). Il gate prestazionale `python scripts/check_mysql_performance.py`
applica un budget assoluto a read e Append/SingleTransaction, richiede
differenziale zero e un solo commit osservato; non dichiara una baseline
misurata finché non ne esiste una comparabile. La matrice 8.0/8.4 è
`python scripts/check_mysql_matrix.py`. La matrice comparativa dei provider è
in [`docs/PROVIDER-MATURITY-MATRIX.md`](../PROVIDER-MATURITY-MATRIX.md).

Il DDL atomico MySQL non viene equiparato a DDL transazionale (autocommit
implicito). L'implementazione v1.2 di Create/TruncateInsert usa `exec_control`
via text protocol; Replace (staging + RENAME atomico) richiederà un
lifecycle di recovery dedicato quando affrontato.
