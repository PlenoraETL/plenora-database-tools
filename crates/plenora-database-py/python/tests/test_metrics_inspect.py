"""Test live per snapshot delle metriche e helper di introspezione."""
from __future__ import annotations

import os

import pytest

import plenora_database as p

from ._harness import connect_postgres, postgres_dsn_or_skip


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    try:
        yield s
    finally:
        s.close()


# ---------------- Metrics ----------------


def test_metrics_returns_dict_with_known_counters(session) -> None:
    m = session.metrics()
    assert isinstance(m, dict)
    # Chiavi chiave che il PFM/oncall vuole vedere.
    for key in [
        "pool_checkouts",
        "pool_reuses",
        "schema_cache_hits",
        "schema_cache_misses",
        "catalog_introspections",
        "read_rows",
        "read_bytes",
        "writes_committed",
        "cancellations",
    ]:
        assert key in m, f"chiave metrics mancante: {key}"
        assert isinstance(m[key], int), f"{key} deve essere int"
        assert m[key] >= 0


def test_metrics_counters_increment_on_activity(session) -> None:
    before = session.metrics()
    for _ in range(3):
        session.execute_scalar("SELECT 1")
    after = session.metrics()
    # Almeno pool_checkouts + pool_reuses è cresciuto (3 execute_scalar
    # aprono tx = pool checkout ognuno).
    assert (
        after["pool_checkouts"] + after["pool_reuses"]
        > before["pool_checkouts"] + before["pool_reuses"]
    )


# ---------------- Inspect namespace ----------------


def test_inspect_catalogs_includes_current_database(session) -> None:
    catalogs = session.inspect.catalogs()
    assert isinstance(catalogs, list)
    assert all(isinstance(c, str) for c in catalogs)
    current = session.execute_scalar("SELECT current_database()")
    assert current in catalogs


def test_inspect_schemas_excludes_system_schemas(session) -> None:
    schemas = session.inspect.schemas()
    assert isinstance(schemas, list)
    assert all(isinstance(s, str) for s in schemas)
    # `public` deve esserci; gli schemi di sistema no.
    assert "public" in schemas
    for banned in ("pg_catalog", "information_schema", "pg_toast"):
        assert banned not in schemas, f"system schema '{banned}' non deve comparire"


def test_inspect_tables_returns_dicts_with_name_kind(session) -> None:
    session.execute_sql("DROP TABLE IF EXISTS _pyf41_tbl")
    session.execute_sql("CREATE TABLE _pyf41_tbl (id INT PRIMARY KEY, x TEXT)")
    try:
        tables = session.inspect.tables("public")
        assert isinstance(tables, list)
        found = next((t for t in tables if t.get("name") == "_pyf41_tbl"), None)
        assert found is not None, f"tabella _pyf41_tbl non trovata in {tables[:5]}..."
        assert "kind" in found
        assert "is_partition" in found
    finally:
        session.execute_sql("DROP TABLE IF EXISTS _pyf41_tbl")


def test_inspect_describe_returns_columns(session) -> None:
    session.execute_sql("DROP TABLE IF EXISTS _pyf41_desc")
    session.execute_sql(
        "CREATE TABLE _pyf41_desc ("
        " id INT PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " qty NUMERIC(10,2))"
    )
    try:
        desc = session.inspect.describe("public", "_pyf41_desc")
        assert isinstance(desc, dict)
        assert "columns" in desc, f"descrizione senza 'columns': {desc}"
        col_names = [c.get("name") for c in desc["columns"]]
        assert col_names == ["id", "label", "qty"]
    finally:
        session.execute_sql("DROP TABLE IF EXISTS _pyf41_desc")


def test_inspect_describe_on_missing_table_raises(session) -> None:
    with pytest.raises(p.PlenoraError):
        session.inspect.describe("public", "_pyf41_absolutely_nothere_xyz")
