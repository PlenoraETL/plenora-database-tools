# plenora-database

Python SDK per `plenora-database-tools` (Postgres/PostGIS, MySQL, SQL Server).

**Stato**: skeleton (Fase 3, milestone F3-1). Espone solo `version()`.

## Build (dev)

Richiede Python 3.10+ e [maturin](https://maturin.rs).

```bash
pip install maturin
cd crates/plenora-database-py
maturin develop --release
python -c "import plenora_database; print(plenora_database.version())"
```

## Milestone roadmap

- **F3-1** — Skeleton + `version()` [current]
- **F3-2** — `Session` + `connect()` context manager
- **F3-3** — `execute()`, `execute_scalar()`, `execute_returning_rows()`
- **F3-4** — Portable AST builder
- **F3-5** — Spatial + typed params + transaction context
- **F3-6** — Error mapping (`PlenoraError` gerarchia)
- **F3-7** — `AsyncSession` (asyncio)
- **F3-8** — E2E test + benchmark parity vs CLI subprocess
