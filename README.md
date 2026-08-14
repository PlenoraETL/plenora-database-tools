# plenora-database-tools

Black box Rust autoconsistente per introspezione, lettura e scrittura di dati
tabellari e geospaziali.

Target v1:

- PostgreSQL/PostGIS;
- MySQL 8.0 e 8.4 LTS; MariaDB richiede una qualifica indipendente;
- SQL Server geometry/geography;
- Oracle/Oracle Spatial;
- Db2/Db2 Spatial;
- SQLite/SpatiaLite;
- DuckDB/Spatial;
- ArcGIS Online ed Enterprise Feature Service.

ArcGIS è un provider REST, non un dialect SQL. Tutti i provider condividono
contratti Arrow/GeoArrow-WKB, limiti, mapping, osservabilità e outcome.

## Stato

La preparazione offline della Fase 0 e la Fase 1 sono eseguibili.
PostgreSQL 16/PostGIS 3.4 è il driver di riferimento read/write/spatial:
il profilo avanzato include AST relazionale e spatial bounded, 72 funzioni
PostGIS tipizzate, operatori GiST/KNN, geometrie XY/XYZ/XYM/XYZM, pool bounded,
cancellazione server-side, COPY text/binario, introspezione strutturale e
schema evolution additiva. Passa gate live, fault matrix e benchmark
differenziali. Il relativo safety case rende esplicite prove, assunzioni e
rischi residui; non costituisce certificazione aeronautica. SQL Server 2022 ha
un gate reference separato. MySQL 9.7 LTS (baseline v1.2), 8.4.11 e 8.0.46 hanno una superficie stabile
per query relazionale, lettura bounded, scrittura bulk (**tutti 7 WriteMode**:
Append + Create + TruncateInsert + Upsert + DeleteByKeys + Update + Replace
— v1.2), transazioni OLTP con savepoints e `conditional_update` (v1.2),
DDL raw via `execute_ddl` (v1.2), **26 funzioni spatial `ST_*` verified**
(metadata + predicati + metriche + constructor + transform + set operation
— v1.2), TLS verificato tramite CA privata e hostname, catalogo,
spatial XY/SRID, reset, timeout, cancellazione, rollback e quarantena provati
live. MariaDB e le dimensioni Z/M/ZM restano fail-closed.

Documenti principali:

- [Architetture.md](Architetture.md);
- [Prestazioni.md](Prestazioni.md);
- [Python SDK v0.3](crates/plenora-database-py/README.md) (`pip install plenora-database`, ~13× più veloce del subprocess CLI; include bulk `copy_from` con upsert/create/replace/etc.);
- [guida migrazione CLI → SDK](crates/plenora-database-py/docs/MIGRATION_FROM_CLI.md);
- [gate pre-database](docs/history/phase-0/pre-database-gate.md);
- [stato Fase 1 Rust](docs/history/phase-1/README.md);
- [driver PostgreSQL/PostGIS](docs/postgres/README.md);
- [freeze PostgreSQL/PostGIS v0.1](docs/postgres/REFERENCE-V0.1.md);
- [hardening PostgreSQL/PostGIS](docs/postgres/HARDENING.md);
- [safety case PostgreSQL/PostGIS](docs/postgres/SAFETY-CASE.md);
- [compatibilità PostgreSQL/PostGIS](docs/postgres/COMPATIBILITY.md);
- [campagna prestazionale PostgreSQL/PostGIS](docs/postgres/PERFORMANCE.md);
- [contratto canonico Arrow condiviso](docs/adr/0011-canonical-field-contract.md);
- [provider MySQL](docs/mysql/README.md);
- [matrice di maturità dei provider](docs/PROVIDER-MATURITY-MATRIX.md);
- [decisioni che richiedono i target](docs/history/phase-0/open-decisions.md);
- [contratti v1](contracts/v1/README.md);
- [benchmark Fase 0](benchmarks/README.md).

## Verifica completa pre-database

Da PowerShell o da un altro terminale:

```powershell
python scripts\check_pre_database.py
```

Lo stato candidato corrente è dichiarato in
[`release/1.1.0.json`](release/1.1.0.json). I manifesti
[`release/final-readiness.json`](release/final-readiness.json),
[`release/rc1-readiness.json`](release/rc1-readiness.json) e
[`release/development.json`](release/development.json) restano record storici
immutabili. I gate sono eseguibili anche separatamente:

```powershell
python scripts/check_release_manifest.py --repo . release/development.json release/rc1-readiness.json release/final-readiness.json release/1.1.0.json
python scripts\test_check_release_manifest.py
python scripts\check_final_readiness.py --repo . release\1.1.0.json
python scripts\check_final_readiness.py --repo . release\final-readiness.json
python scripts\test_check_final_readiness.py
```

La RC1 taggata con claim `verified_internally` è descritta in
[`docs/RC1-READINESS.md`](docs/RC1-READINESS.md) e registrata nel manifesto
[`release/rc1-readiness.json`](release/rc1-readiness.json). Il gate dedicato
impedisce claim prematuri e divergenze fra baseline ed evidenze:

```powershell
python scripts\check_rc1_readiness.py --repo . release\rc1-readiness.json
python scripts\test_check_rc1_readiness.py
```

Il comando valida sorgenti, JSON Schema, esempi, golden cases, manifest,
documentazione, rustfmt, Clippy, test Python/Rust e CLI. Non apre connessioni
database.

## Probe provider-neutral

Il confine CLI comune espone il test di connessione e le capability runtime dei
tre adapter implementati:

```text
plenora-database database-probe postgres <dsn-env> \
  [--tls-ca-path-env <ca-path-env> \
   [--tls-client-cert-path-env <cert-path-env> \
    --tls-client-key-path-env <key-path-env>]]
plenora-database database-probe mysql <password-env> <host> <database> <username> [port] \
  [--tls-ca-path-env <ca-path-env>]
plenora-database database-probe sqlserver <password-env> <host> <database> <username> [port] \
  [--tls-ca-path-env <ca-path-env>]
```

Gli argomenti `*-env` sono esclusivamente nomi di variabili ambiente. La
variabile secret contiene il DSN PostgreSQL o la password MySQL/SQL Server; le
variabili TLS contengono path locali ai file CA, certificato client e chiave
client. Valori secret, path e materiale PEM non vengono accettati direttamente
negli argomenti del processo.

Senza opzioni CA, PostgreSQL richiede TLS verificato tramite WebPKI. Con
`--tls-ca-path-env` usa esclusivamente la CA privata indicata; certificato e
chiave client sono PostgreSQL-only, devono essere forniti insieme e richiedono
la CA privata. Ogni file PEM letto dalla CLI è bounded a 1 MiB. MySQL e SQL
Server supportano la CA privata mantenendo verifica di catena e hostname; non
espongono un opt-out dalla verifica attraverso questo comando. Host, database,
username e porta restano argomenti strutturati e la porta usa il default del
provider quando omessa.

`mariadb`, `oracle`, `db2`, `sqlite`, `duckdb` e `arcgis` appartengono al catalogo
del contratto ma non hanno ancora un adapter Rust: `database-probe` li rifiuta
prima di leggere secret o aprire la rete con un errore canonico `unsupported`.
La validazione sintattica di un piano non implica che il relativo provider sia
eseguibile.

Il precedente `postgres-probe <dsn-env>` resta disponibile come alias
compatibile e accetta le stesse opzioni CA privata/mTLS del route
provider-neutral.

Il gate live del riferimento PostgreSQL/PostGIS è:

```powershell
python scripts\check_postgres_reference.py
```

La verifica offline di un dataset Arrow IPC, senza connessione a database, è:

```powershell
cargo run --locked -p plenora-database-cli -- inspect-dataset dataset.arrow
```

Il comando valida la versione del contratto e i metadati canonici nel core,
ispeziona ogni cella WKB/EWKB con limiti hard e produce un singolo JSON su
stdout. Un conflitto fra rappresentazioni CRS termina con codice non zero.

Le dipendenze del solo tooling Python sono congelate in
`requirements-phase0.txt`. Non sono dipendenze runtime della futura libreria
Rust.

## Regola sui segreti

I piani serializzati contengono soltanto `connection_ref`. DSN, password e
token devono entrare tramite variabili d’ambiente o secret resolver e non
devono comparire in log, risultati o fixture versionate.
