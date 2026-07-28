# EWKB fuzzing

This detached `cargo-fuzz` crate exercises the database-core EWKB scanner
without opening a database connection.

The target treats the first three input bytes as bounded component/depth
controls and the remainder as untrusted EWKB. It checks:

- no panic for arbitrary input;
- identical acceptance and error category between simple and detailed scans;
- fail-closed zero, component, and nesting-depth limits;
- stable metadata when limits are loosened;
- rejection of trailing bytes after an accepted geometry;
- checked arithmetic in release builds (`overflow-checks = true`).

Run a bounded local campaign from the repository root:

```text
rustup run nightly-2026-07-27 cargo fuzz run ewkb_parser -- \
  -max_total_time=60 -timeout=10 -rss_limit_mb=2048
```

Crash artifacts and generated corpora are intentionally ignored by Git.
