# Gate pre-database

Data verifica: 2026-07-27

## Scopo

Rendere il repository pronto a iniziare gli spike sui provider senza
installare o avviare database.

## Checklist offline

- [x] confine black box e provider ArcGIS separato dal dialect SQL;
- [x] inventario iniziale del backend Python;
- [x] matrice capability e compatibilità comportamentale;
- [x] contratti JSON v1 per piano, capability, loss report e write outcome;
- [x] esempi contrattuali validabili;
- [x] golden manifest per scalar, temporal, binary, schema, geometry, write,
  outcome, ArcGIS e security;
- [x] harness con warmup, ripetizioni e output JSONL atomico;
- [x] aggregatore mediana/p95 e digest di stabilità;
- [x] ADR bloccanti del core;
- [x] validazione offline automatica;
- [x] workspace Rust con core, SQL AST, engine, testkit e CLI;
- [x] rustfmt, Clippy `-D warnings` e unit test offline;
- [ ] selezione finale di crate/runtime per provider;
- [ ] test differenziali contro target reali;
- [ ] baseline statistiche per ogni provider/versione.

Gli ultimi tre punti appartengono al gate database e non bloccano la
preparazione offline.

## Regola di ingresso al gate database

Si procede solo dopo istruzioni esplicite di Marco. Per ogni provider servono
endpoint/versione, modalità credenziali, privilegi, isolamento e cleanup. Il
runner non installa automaticamente server, estensioni spatial o client
nativi.

## Ordine consigliato quando i target saranno disponibili

1. PostgreSQL/PostGIS come reference implementation.
2. ArcGIS fake REST, poi Online/Enterprise reale.
3. SQLite/SpatiaLite e DuckDB Spatial.
4. MySQL/MariaDB.
5. SQL Server.
6. Oracle Spatial.
7. Db2 Spatial.

L’ordine riduce il rischio del core; non definisce priorità commerciali né
limita il supporto finale.
