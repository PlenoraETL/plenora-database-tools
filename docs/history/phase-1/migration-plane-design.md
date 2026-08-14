# Migration/DDL plane — decisione: FUORI SCOPE

**Stato**: cancellato dal roadmap della libreria.
**Data decisione**: 2026-08-11.

## Cosa era stato proposto

DBT-PFM-008 della roadmap PFM chiedeva che `plenora-database-tools` diventasse anche migration engine, con:
- tabella `plenora_migration_history` gestita dalla libreria dentro il DB del consumer
- checksum + drift detection
- advisory lock anti-concorrenza
- API `apply_all(migrations)` con orchestrazione

## Perché è stato cancellato

Il principio dichiarato della libreria è: *"black-box che uniforma read/write, robusta, veloce, contratti chiari"*. Il migration management è un'altra cosa:

1. **Scope creep evidente**. La libreria è un runtime foundation per read/write. Diventare anche migration engine è un salto di categoria (da "interfaccia" a "framework").
2. **Accoppiamento invasivo**. La tabella `plenora_migration_history` sarebbe imposta a ogni consumer nel suo DB. Superficie che il consumer non ha chiesto.
3. **Problema già risolto altrove**. Flyway, Liquibase, sqlx-migrate, refinery, atlas — tool maturi, provider-agnostici, integrabili. Reimplementarli è duplicazione.
4. **Uniformazione cross-provider costosa**. Migration semantics divergono (Postgres transazionale vs MySQL non transazionale). Le stesse divergenze che avevano portato a ridimensionare lo scope MySQL nel piano di uniformazione.
5. **La roadmap PFM esprime un desiderio, non un vincolo tecnico**. Il PFM può soddisfare "un punto unico di accesso al DB" combinando `plenora-database-tools` (runtime) + un migration tool esterno (schema evolution). Non serve fondere i due.

## Cosa resta della libreria per supportare il PFM sul migration case

Le primitive già presenti sono sufficienti per costruirci sopra un migration tool esterno:

- `Statement` con SQL raw → puoi passare qualunque DDL
- `TransactionScope` → DDL in transazione dove il DB lo supporta
- `NativeQueryPolicy::Allow` → sblocca DDL per il contesto migration specifico
- `execute_conditional_update` con probe → controllo precondizioni su righe

Manca (se emerge necessità):
- Un metodo per **eseguire statement fuori transazione** (per `CREATE INDEX CONCURRENTLY` e simili). Banale da aggiungere se serve.

## Cosa dovrebbe fare il PFM

- Scegliere un migration tool per Rust (raccomandazione: `refinery` o `sqlx::migrate!`).
- Usarlo direttamente contro il DB per l'evoluzione schema.
- Usare `plenora-database-tools` per tutto il resto (OLTP transazionale, read, write bulk).

## Aggiornamento roadmap PFM richiesto

Rinegoziare DBT-PFM-008: la libreria fornisce il canale (execute DDL + tx), non l'engine di migration. Il PFM soddisfa il requisito "unico punto di accesso" a livello architetturale, non facendo tutto dentro la stessa libreria.
