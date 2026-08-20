# Provider MySQL

Baseline di riferimento: **MySQL 9.7.2**. La matrice versionata qualifica
anche **MySQL 8.4.11 LTS** e **MySQL 8.0.46** come riferimenti di
compatibilità, per installazioni legacy. Versione esatta e digest immutabile
di ciascun riferimento sono dichiarati una volta sola in
[`docker/mysql/references.json`](../../docker/mysql/references.json): il
compose della baseline, i due gate e i test live leggono quel documento, così
nessuno può affermare una versione diversa da quella effettivamente avviata.
I 62 test live — 37 default piu 25 reference — passano identici su 9.7,
8.4 e 8.0: la superficie
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
  **delle righe** prima del commit e outcome `Unknown` più quarantena quando
  il commit è ambiguo. Il rollback non copre il DDL: `Create` prepara il
  target con `CREATE TABLE`, che su MySQL fa commit implicito, quindi un
  fallimento successivo lascia la tabella vuota e lo dichiara con
  `remote_effect = partial` e `retry = requires_recovery`. È la combinazione
  `rollback_on_failure = true` + `transactional_ddl = false` descritta nel
  contratto delle capability. L'unica altra modalità che emette DDL è
  **Update**, che crea una `TEMPORARY TABLE` di staging: è session-scoped,
  non tocca l'identità del target e sparisce con la connessione, quindi non
  lascia residui da ripulire. Le restanti quattro non emettono DDL affatto.
  **Sei modalità disponibili su sette**: Append + **Create** (CREATE
  TABLE dallo schema Arrow; `keys` opzionali diventano la PRIMARY KEY, e
  devono essere colonne non-nullable e non ripetute) + **Replace** (`DELETE FROM` + INSERT bulk nella
  stessa transazione InnoDB) + **Upsert** (`INSERT ... ON DUPLICATE KEY
  UPDATE`) + **DeleteByKeys** (`DELETE ... WHERE (keys) IN (...)`) +
  **Update** (staging TEMPORARY + `UPDATE JOIN`). **TruncateInsert** resta
  fail-closed: `TRUNCATE` è DDL con commit implicito, quindi non
  rollback-safe, e non viene emulata con `DELETE` perché avrebbe semantica
  diversa (`AUTO_INCREMENT` non azzerato, trigger e log riga per riga
  attivi). Il rifiuto arriva in `prepare_write`, prima del checkout dal pool
  e quindi prima di qualunque effetto remoto;
- **Replace non ricrea il target.** Deve già esistere — altrimenti
  `NotFound` — e sopravvive alla scrittura con la stessa object identity e
  con indici, unique, foreign key, trigger, check, default, grant e
  `AUTO_INCREMENT` invariati. Un errore o una cancellazione dopo il `DELETE`
  annulla tutto e riporta le righe precedenti;
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
| unit | `cargo test -- --skip live_` | 174 |
| live default | `cargo test live_` | 37 |
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
implicito). `Create` usa `exec_control` via text protocol. `Replace` non usa
DDL affatto: è `DELETE FROM` seguito dal bulk INSERT, entrambi dentro la
transazione InnoDB dichiarata dal profilo `SingleTransaction`. Non esistono
più staging persistenti, `RENAME TABLE` o tabelle di backup da ripulire —
e con loro sono spariti gli stati intermedi che il vecchio pattern poteva
lasciare se il processo moriva tra il `CREATE` e lo swap.

## Progetti Compose e migrazione

Ogni `docker-compose.*.yml` dichiara il proprio progetto, quindi ciascun
riferimento vive in una rete separata:

| Compose | Progetto | Rete |
| --- | --- | --- |
| `docker-compose.mysql.yml` | `plenora-mysql` | `plenora-mysql_default` |
| `docker-compose.postgres.yml` | `plenora-postgres` | `plenora-postgres_default` |
| `docker-compose.postgres-tls.yml` | `plenora-postgres-tls` | `plenora-postgres-tls_default` |
| `docker-compose.sqlserver.yml` | `plenora-sqlserver` | `plenora-sqlserver_default` |

Senza `name:` Compose derivava il progetto dalla directory e i quattro file
finivano nello stesso: `down --remove-orphans` su uno cancellava i container
degli altri. I gate non contengono il nome della rete — lo chiedono a Docker
con `scripts/compose_network.py`, leggendo le label del container — e un test
impedisce che qualcuno lo riscriva a mano.

**Migrazione (una tantum).** A collidere sono soltanto i **container**: i
`container_name` sono fissi, quindi quelli del vecchio progetto `database-tools`
occupano i nomi che i nuovi progetti vogliono usare. I volumi no — sono
prefissati dal progetto, quindi `database-tools_mysql_data` e
`plenora-mysql_mysql_data` convivono senza conflitto.

Rimuovere i soli container, prima del primo `up`:

```bash
docker rm -f dataflow-mariadb \
             dataflow-mariadb-certgen \
             dataflow-mariadb-11 \
             dataflow-mariadb-11-certgen \
             dataflow-mysql \
             dataflow-mysql-certgen \
             dataflow-postgres \
             dataflow-postgres-tls \
             dataflow-postgres-tls-certgen \
             dataflow-sqlserver \
             dataflow-sqlserver-certgen \
             dataflow-sqlserver-init
```

**Non** cancellare i volumi del vecchio progetto: non e necessario e non e
reversibile. Restano orfani e inerti; chi vuole recuperare lo spazio puo
elencarli con `docker volume ls | grep database-tools` e rimuoverli quando ha
verificato che i nuovi riferimenti funzionano — ma e una decisione sua, non un
passo della migrazione.

I nuovi progetti ripartono da volumi vuoti e le fixture nascono dagli script in
`docker/*/init`, che il primo avvio riesegue.

## Consumer surface

Post-Blocchi B/A/C, il driver MySQL è accessibile dai consumer:

**CLI** (9 sub-comandi, dietro `--features mysql`; `--features full` li
compila insieme agli altri provider):

```
plenora-database mysql-probe <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>]
plenora-database mysql-describe <args...> <schema> <object>
plenora-database mysql-inspect-schemas <args...>
plenora-database mysql-inspect-tables <args...> <schema>
plenora-database mysql-execute-sql <args...> <sql>
plenora-database mysql-execute-ddl <args...> <sql>
plenora-database mysql-execute-scalar <args...> <sql>
plenora-database mysql-transaction-test <args...>
plenora-database mysql-conditional-update <args...> <UPDATE_SQL> <EXPECTED_AFFECTED>
```

**SDK Python**, sync e async, con la stessa superficie:

```python
import plenora_database as p

with p.connect_mysql("localhost", "mydb", "user", "pwd") as s:
    n = s.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
    v = s.execute_scalar("SELECT COUNT(*) FROM t")
    rows = s.execute_returning_rows("SELECT id, label FROM t WHERE id > ?", [0])
    s.execute_ddl("CREATE INDEX idx_label ON t(label(64))")

    with s.begin(isolation="repeatable_read") as tx:      # + SessionContext
        tx.execute("UPDATE t SET label = ? WHERE id = ?", ["y", 1])

    for chunk in s.read("mydb", "events", limit=10_000):  # Arrow IPC bounded
        ...
    outcome = s.copy_from("mydb", "events", table, mode="replace")
    rows = s.select("t").where_eq("id", 1).all()          # builder AST
```

`aconnect_mysql` e l'equivalente async: `aread` e `acopy_from` al posto
di `read` e `copy_from`, il resto identico.

La suite del SDK si esegue con `python scripts/check_sdk_tests.py`, che
costruisce il wheel con `maturin` e lo installa nel container prima di
`pytest`: il `.so` e gitignorato, e un `pytest` diretto dopo un cambio al
Rust risponde sul binario precedente. Il runner pretende un albero di lavoro
pulito — il verdetto nomina un commit — e verifica che il package importato
arrivi da `site-packages`, non dal source tree accanto ai test.

Non esposto al SDK MySQL: spatial predicates + `SpatialReference`.

Non esposto al CLI MySQL: `bulk-write` (Arrow IPC), `benchmark-*`,
`diagnose`, `doctor`, `explain`, `pool-status`,
`test-cancellation/streaming/spatial/concurrency`, `portable-execute`.
