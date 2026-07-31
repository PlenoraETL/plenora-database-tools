# Fase 1 — Core, SQL e testkit

Stato: **scaffold offline compilabile**

La prima implementazione concreta PostgreSQL/PostGIS è documentata in
`../postgres/README.md`.

## Implementato senza database

- workspace Rust 1.92 con versioni dipendenza centralizzate;
- `plenora-database-core`:
  - re-export Arrow 59.1;
  - piano tipizzato;
  - capability;
  - limiti;
  - GeoArrow-WKB metadata;
  - validatore unico del contratto canonico Arrow e della versione di schema;
  - AST query portabile e catalogo di 29 funzioni spatial;
  - parametri decimal, UUID, JSON e WKB tipizzati;
  - mapping policy e `LossReport`;
  - outcome committed/rollback/partial/unknown;
  - errori pubblici redatti;
  - trait runtime-agnostic per provider e batch stream;
- `plenora-database-sql`:
  - identificatori validati;
  - quoting PostgreSQL, MySQL, SQL Server, Oracle, Db2, SQLite e DuckDB;
  - placeholder separati dai valori;
  - SELECT/filter/order/limit;
  - `IN`, `BETWEEN`, `LIKE` e predicati spatial capability-gated;
- `plenora-database-engine`:
  - parsing bounded;
  - validazione fail-closed;
  - fingerprint SHA-256 deterministico;
  - prepare contro capability runtime;
- `plenora-database-testkit`:
  - golden v1 incorporato;
  - controllo marker-segreto;
- CLI `validate-plan` e `inspect-dataset` offline per Arrow IPC.

`inspect-dataset <file.arrow>` usa `arrow-ipc =59.1.0`, conserva nel report i
metadati osservati e ispeziona ogni cella geometrica con il parser EWKB
bounded. Non apre connessioni e fallisce chiuso su versioni future, enum non
validi, metadati legacy/GeoArrow divergenti, payload incoerenti e conflitti
CRS.

## Non ancora implementato in modo generale

- pool e client per i provider diversi da PostgreSQL;
- introspezione remota;
- streaming cursor/bulk path;
- transazioni remote e fault injection;
- conversioni native spatial per singolo provider;
- client ArcGIS HTTP e conversione Esri JSON.

PostgreSQL copre connessione e pool bounded, cancellazione server-side,
keepalive, introspezione avanzata, read e query AST streaming, tutte le
modalità write, preflight, schema evolution additiva, byte budget, timeout,
TLS, fault injection e i bulk path COPY text/binario; il gate live di
congelamento è elencato nel documento del driver. Gli altri punti
richiedono spike con i target e restano nel gate descritto in
`../phase-0/open-decisions.md`.

## Gate

```powershell
python scripts\check_pre_database.py `
  --output benchmarks\baseline\pre-database-complete.json
```

Il runner usa Cargo locale oppure il container `rust:1.92`; esegue test Python,
JSON Schema, rustfmt, Clippy con warning negati, test Rust e smoke CLI. Non
apre connessioni.

Il percorso d'errore usa il protocollo JSON versionato descritto in
[`../cli/ERROR-PROTOCOL.md`](../cli/ERROR-PROTOCOL.md): i quattro assi
rimangono campi machine-readable su `stderr`, con exit code non-zero.
