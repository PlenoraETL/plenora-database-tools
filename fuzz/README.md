# Fuzzing

This detached `cargo-fuzz` crate exercises the parsing, validation, and
rendering surfaces of the workspace without opening a database connection.
Every target runs offline: no Docker, no server, no credentials.

## Targets

### Contract decoding

- `ewkb_parser` — database-core EWKB scanner. The first three input bytes are
  bounded component/depth controls, the remainder is untrusted EWKB. It checks
  identical acceptance and error category between simple and detailed scans,
  fail-closed zero/component/depth limits, stable metadata when limits are
  loosened, and rejection of trailing bytes after an accepted geometry.
- `plan_contract` — `plenora-database-engine` `parse_and_validate`. It checks
  that every rejection is an `InvalidPlan` in the `Validate` phase with no
  remote effect, and that an accepted plan survives its own canonical
  round-trip with an unchanged fingerprint.
- `field_contract` — canonical Arrow metadata of `FieldContract::parse` and
  `validate_schema_contract`. Fields are assembled from the input over the
  canonical, legacy, and GeoArrow key sets.

### Portable SQL builder

- `query_ast_render` — `QueryOperation` deserialization plus rendering on all
  seven dialects. It checks that an accepted render implies a valid portable
  AST, that bind ordinals stay dense and one-based, and that the native
  spatial parts recompose exactly into the native statement.
- `sql_select_render` — byte-driven `Select`/`Expression` trees through
  `render_select` and `render_filter`. It checks the identifier quoting
  round-trip on each dialect, which is the only defence against injection
  through an identifier.

### Provider surfaces reachable offline

- `mysql_read_plan` — `MysqlObjectDescription` plus `ReadOperation` through
  `MysqlReadPlan::compile`, cross-checked against `validate_schema_contract`.
- `mysql_query_render` — `plenora_db_mysql::render_query`, the MySQL dialect
  gating over the portable AST.
- `sqlserver_read_plan` — `SqlServerObjectDescription` through
  `SqlServerReadPlan::compile`, cross-checked against
  `validate_schema_contract`.
- `sqlserver_recovery` — the pure transaction recovery state machine driven by
  arbitrary wire event sequences. It checks that quarantine is absorbing and
  that no event sequence ends optimistically.
- `postgres_tls_pem` — PEM trust store and mTLS identity parsing of
  `PostgresTlsConfig`. The input is split into CA, client chain, and client
  key sections. Run this target with `-max_len=8192`: its cost grows faster
  than linearly with the PEM size (three full parses per execution), so larger
  inputs slow the campaign down without adding coverage.

The PostgreSQL wire decoding, Arrow conversion, and parameter codec are not
reachable from this crate: `plenora-db-postgres` keeps those modules private
and exposes them only through connected code paths.

## Running

Run a bounded local campaign from the repository root, starting from the
versioned seeds:

```text
mkdir -p fuzz/corpus/ewkb_parser
cp fuzz/seeds/ewkb_parser/* fuzz/corpus/ewkb_parser/
rustup run nightly-2026-07-27 cargo fuzz run ewkb_parser fuzz/corpus/ewkb_parser -- \
  -max_total_time=60 -timeout=10 -rss_limit_mb=2048
```

## Seeds and CI

`fuzz/seeds/<target>/` holds a small, versioned seed corpus for every target,
stored byte for byte (`.gitattributes`). The structured seeds are inputs the
target accepts: plan examples from `contracts/v2/examples/`, the
`tests/fixtures/tls/parser-ca.pem` test certificate, EWKB geometries of each
shape, and JSON documents serialized from catalog descriptions and queries
shaped like those of the read-plan benchmarks (`MysqlReadPlan::compile`,
`SqlServerReadPlan::compile`, `validate_query_operation` and `render_query`
accepted them when they were generated). The byte-cursor targets get short
deterministic byte patterns. A new target needs its seed directory:
`scripts/test_ci_workflows.py` fails otherwise.

`.github/workflows/fuzz.yml` runs every target: one minute each on pull
requests touching `fuzz/`, ten minutes each every Sunday and on manual
dispatch. A crash, timeout or non-zero exit fails the job and the reproducer
is uploaded as a run artifact. The corpus a campaign grows (`fuzz/corpus/`)
and crash artifacts (`fuzz/artifacts/`) stay ignored by Git; a reproducer
worth keeping becomes a regression test or a new seed.
