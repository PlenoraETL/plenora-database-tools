# docs/history — archivio storico

Questa cartella raccoglie documentazione di **fasi completate** del
progetto, conservata per tracciabilità ma non più in evoluzione.

## Contenuto

- **`phase-0/`** — Discovery e caratterizzazione pre-implementation
  (baseline, capability matrix, gate pre-database, decisioni aperte
  al tempo della Fase 0, ecc.). Fase chiusa il **2026-07-26**.

- **`phase-1/`** — Scaffold del core Rust (SQL, testkit, engine) e
  design del migration plane (poi esplicitamente cancellato da
  scope). Fase chiusa con l'integrazione dei driver Postgres/MySQL/
  SQL Server.

## Perché archiviato e non cancellato

I documenti restano referenziabili per audit e per capire *perché*
alcune decisioni sono state prese. In particolare:

- `phase-0/pre-database-gate.md`, `phase-0/open-decisions.md` e
  `phase-1/README.md` sono ancora validati dal gate offline
  `scripts/phase0_validate.py` (verifica esistenza + code fence
  bilanciati).
- `phase-0/baseline-plan.md` è citato da `benchmarks/README.md` come
  origine metodologica della baseline.
- `phase-1/migration-plane-design.md` è citato da `docs/interfaces.md`
  come riferimento della decisione di cancellare il migration plane.

## Cosa non si trova qui

I **release readiness** (`docs/RC1-READINESS.md`,
`docs/FINAL-1.0.0-READINESS.md`, `docs/FINAL-1.1.0-READINESS.md`)
NON sono archiviati perché ancora referenziati dai manifest
`release/*.json` fossilizzati e dagli hash SHA-256 codificati in
`scripts/test_check_final_readiness.py`. Restano in `docs/`.
