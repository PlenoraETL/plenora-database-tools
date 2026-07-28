# PostgreSQL coverage assurance

Coverage is measured with `cargo-llvm-cov 0.8.7`, Rust `1.92.0`, and the
PostgreSQL 16 / PostGIS 3.4 live fixture. It is evidence of exercised code,
not evidence that unobserved behaviors are correct.

## Baseline

The first local live campaign on 2026-07-28 produced:

| Scope | Lines | Regions | Functions |
|---|---:|---:|---:|
| Full workspace | 81.55% (11,374 / 13,947) | 78.64% (14,632 / 18,606) | 77.65% (813 / 1,047) |
| Production-focused | 80.27% (8,419 / 10,489) | 77.78% (11,371 / 14,619) | 79.41% (721 / 908) |
| `write.rs` | 83.04% (2,003 / 2,412) | 80.41% (3,165 / 3,936) | 81.58% (186 / 228) |
| `ewkb.rs` | 84.93% (231 / 272) | 84.09% (354 / 421) | 75.86% (22 / 29) |

La riga `write.rs` fotografa la baseline precedente alla scomposizione
architetturale del writer. Le campagne successive devono valutare insieme
`write.rs` e `write/*.rs`: spostare codice fra queste unità non è un aumento né
una diminuzione di copertura.

The production-focused scope excludes only:

- `plenora-database-cli/src/main.rs`, whose process boundary needs dedicated
  CLI integration tests;
- `plenora-db-postgres/src/test_suite.rs`, because counting the test harness
  would inflate the production gate.

Inline unit-test regions remain visible where Rust compiles them into a
production source file. Full and production-focused JSON/LCOV reports are
therefore both preserved for audit.

## Regression thresholds

The CI gate fails below:

- 80% covered production-focused lines;
- 79% covered production-focused functions;
- 77% covered production-focused regions.

These are baseline regression guards, not completion targets. Raising a
threshold requires a green reproducible campaign; lowering one requires an
explicit review with a documented rationale.

LLVM branch and MC/DC counters are currently unavailable for this Rust
instrumentation and must not be reported as zero-percent coverage claims.
