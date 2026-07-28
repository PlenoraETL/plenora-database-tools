# plenora-database-tools

Black box Rust autoconsistente per introspezione, lettura e scrittura di dati
tabellari e geospaziali.

Target v1:

- PostgreSQL/PostGIS;
- MySQL/MariaDB;
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
rischi residui; non costituisce certificazione aeronautica. Gli altri provider
richiedono target concordati.

Documenti principali:

- [Architetture.md](Architetture.md);
- [Prestazioni.md](Prestazioni.md);
- [gate pre-database](docs/phase-0/pre-database-gate.md);
- [stato Fase 1 Rust](docs/phase-1/README.md);
- [driver PostgreSQL/PostGIS](docs/postgres/README.md);
- [freeze PostgreSQL/PostGIS v0.1](docs/postgres/REFERENCE-V0.1.md);
- [hardening PostgreSQL/PostGIS](docs/postgres/HARDENING.md);
- [safety case PostgreSQL/PostGIS](docs/postgres/SAFETY-CASE.md);
- [compatibilità PostgreSQL/PostGIS](docs/postgres/COMPATIBILITY.md);
- [campagna prestazionale PostgreSQL/PostGIS](docs/postgres/PERFORMANCE.md);
- [decisioni che richiedono i target](docs/phase-0/open-decisions.md);
- [contratti v1](contracts/v1/README.md);
- [benchmark Fase 0](benchmarks/README.md).

## Verifica completa pre-database

Da PowerShell o da un altro terminale:

```powershell
python scripts\check_pre_database.py
```

Il comando valida sorgenti, JSON Schema, esempi, golden cases, manifest,
documentazione, rustfmt, Clippy, test Python/Rust e CLI. Non apre connessioni
database.

Il gate live del riferimento PostgreSQL/PostGIS è:

```powershell
python scripts\check_postgres_reference.py
```

Le dipendenze del solo tooling Python sono congelate in
`requirements-phase0.txt`. Non sono dipendenze runtime della futura libreria
Rust.

## Regola sui segreti

I piani serializzati contengono soltanto `connection_ref`. DSN, password e
token devono entrare tramite variabili d’ambiente o secret resolver e non
devono comparire in log, risultati o fixture versionate.
