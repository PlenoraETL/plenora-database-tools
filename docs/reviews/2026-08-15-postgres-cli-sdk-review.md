# Review 2026-08-15 — PostgreSQL, CLI, SDK

**Data**: 2026-08-15
**Perimetro**: `plenora-db-postgres`, `plenora-database-cli`,
`plenora-database-py` (SDK Python), `plenora-database-core`.
**Modalità**: review statica esterna (2 round) + audit duplicazioni
(3 round).

## Sintesi

Prima review ha prodotto 15 findings (7 alta priorità + 6 media + 2
minore). Round successivi hanno identificato regressioni introdotte
dai fix del refactor + ulteriori 7 findings emergenti su duplicazioni.

**Stato al 2026-08-15 (commit `b8a822e`)**:

- 12/15 findings originali chiusi in codice.
- 3 residui: #2 `ResourceBudget` in tx (refactor grosso), #6 Arrow
  bulk streaming reale (refactor), #14 `max_output_bytes` semantica
  (decisione, coperta da ADR-003).
- 7 findings del secondo round chiusi (identifier propagate, limiti
  per dialetto, Contains/Within+Geography, EWKB validation, hash
  stabile, commit helper unico, CLI exit code residui).

## Findings originali (round 1)

| # | Priorità | Titolo | Stato | Commit |
|---|----------|--------|-------|--------|
| 1 | Alta | TLS disabilitato in path operativi | ADR-001 → 0.9.0 | pending |
| 2 | Alta | ResourceBudget ignorato in tx | Aperto | roadmap |
| 3 | Alta | Cancellazione solo pre-check | ✅ | `b8a822e` |
| 4 | Alta | SpatialSemantics ignorata in compiler | ✅ | `ed2eef3` |
| 5 | Alta | DWithin.distance_meters gradi vs metri | ✅ + ADR-002 | `ed2eef3` |
| 6 | Alta | Arrow bulk fully-loaded pre-budget | Aperto | roadmap |
| 7 | Alta | RemoteEffect::None sync commit unknown | ✅ | `5ae6d9f` |
| 8 | Media | Errori write collassati a Protocol | ✅ | `a6f9040` |
| 9 | Media | Pool HashMap senza LRU/TTL globale | Aperto | roadmap |
| 10 | Media | CLI exit code 0 su fallimento logico | ✅ | `a6f9040`, `39ba11e` |
| 11 | Media | Session.close() no-op | Aperto | roadmap |
| 12 | Media | Index name troncato UTF-8-unsafe + collisioni | ✅ | `a6f9040`, `cc4c5bf` |
| 13 | Media | Insert.rows chiavi extra + JSON float non-finito | ✅ | `a6f9040`, `6f619b9` |
| 14 | Minore | max-output-bytes semantica → ADR-003 | Decisione | ADR-003 |
| 15 | Minore | CLI parser rimozione apici indiscriminata | ✅ | `a6f9040` |

## Findings round 2 (dopo refactor)

| # | Titolo | Stato | Commit |
|---|--------|-------|--------|
| R2.1 | Renderer::quote fallback silente | ✅ | `cc4c5bf` |
| R2.2 | Limiti identificatori collassati (MySQL 63b vs 64c) | ✅ | `cc4c5bf` |
| R2.3 | Contains/Within + Geography → SQL invalido | ✅ | `cc4c5bf` |
| R2.4 | distance_meters ancora unsafe per Geometry proiettato | ADR-002 | pending |
| R2.5 | EWKB/SRID mismatch bypassa policy | ✅ | `b8a822e` |
| R2.6 | TLS produzione senza factory secure esplicita | ADR-001 → 0.9.0 | pending |
| R2.7 | Cancellazione tx solo pre-check | ✅ | `b8a822e` |
| R2.8 | CLI exit code residui (profile_check, transaction_test) | ✅ | `39ba11e` |
| R2.9 | Commit outcome: ErrorPhase::Write invece di Commit | ✅ | `b8a822e` |
| R2.10 | Test spatial legacy contraddicono policy | ✅ | `cc4c5bf` |
| R2.11 | Hash indice DefaultHasher non stable cross-version | ✅ | `cc4c5bf` |

## Duplicazioni identificate (round 3)

**Chiuse**:
- SRID geografici hardcoded 2× → `spatial_policy::GEOGRAPHIC_SRIDS`
- Identificatori quoting/validazione 3× → `identifier::quote_identifier`
- Cast PostGIS + validazione 3× → `spatial_policy::validate_predicate`
- `default_budget()` × 5 → `budget::session_budget/write_bulk_budget`
- Commit outcome unknown × 7 → `errors_commit::commit_outcome_unknown`

**Aperte** (refactor pianificati):
- 4 renderer SQL (compiler/renderer/spatial/query_plan) → unificazione
  in worktree isolato.
- 4 `run_tx` SDK (session/async/mysql/async_mysql) → vincoli PyO3
  limitano il consolidamento.
- Reader Arrow sync/async duplicati.
- CLI PostgresCommandContext usato solo in 4/30 call sites.
- `params.rs` vs `parameter_codec.rs` divergenti su WKB/NULL/error
  category.

## Decisioni ADR

| ADR | Titolo | Stato |
|-----|--------|-------|
| ADR-011 | TLS secure-by-default | Accettato, target 0.9.0 |
| ADR-012 | Unità spaziali portable (`DistanceUnit` enum) | Accettato, target 0.9.0 |
| ADR-013 | Semantica dei `ResourceBudget` limits | Accettato, target 0.9.0 |

## Roadmap post-review

**PFM-ITS-DB-001** (blocco corrente):
1. TLS Require default + `insecure_local()` factory (0.9.0).
2. `OutcomeUnknown` tipizzato con Commit phase + provider.
3. `NativeQueryPolicy::Deny` nei path PFM.
4. `SessionContext` Python.

**Post-PFM**:
1. `ResourceBudget` realmente applicato alle tx.
2. Unificazione renderer SQL (worktree isolato, ~1 settimana).
3. Arrow bulk streaming reale (no fully-load pre-budget).
4. `Session.close()` reale (rilascio pool/connessioni).
5. Deduplicazione SDK sync/async e wrapper Python.

## Note

- Nessun rilascio `py-v0.8.2` con cambi TLS/distanza — preparare
  `py-v0.9.0`.
- Findings review documentati anche nel CHANGELOG per audit trail.
