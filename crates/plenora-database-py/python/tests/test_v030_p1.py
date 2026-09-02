"""Upsert, update, delete-by-keys, filtri e input pandas/list-of-dict."""
from __future__ import annotations

import io
import os

import pytest

import plenora_database as p

from ._harness import connect_postgres, postgres_dsn_or_skip

pyarrow = pytest.importorskip("pyarrow")
pyarrow_ipc = pytest.importorskip("pyarrow.ipc")


# ============================ P1.1 — upsert / update / delete_by_keys ==================


@pytest.fixture(name="upsert_session")
def _upsert_session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.execute_sql("DROP TABLE IF EXISTS _v030_upsert")
    s.execute_sql(
        "CREATE TABLE _v030_upsert ("
        " id BIGINT PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " amount INT NOT NULL)"
    )
    s.execute_sql("INSERT INTO _v030_upsert VALUES (1, 'existing-1', 100)")
    s.execute_sql("INSERT INTO _v030_upsert VALUES (2, 'existing-2', 200)")
    try:
        yield s
    finally:
        try:
            s.execute_sql("DROP TABLE IF EXISTS _v030_upsert")
        finally:
            s.close()


def test_copy_from_upsert_updates_existing_and_inserts_new(upsert_session) -> None:
    tbl = pyarrow.table(
        {
            "id": pyarrow.array([1, 3], type=pyarrow.int64()),
            "label": pyarrow.array(["updated-1", "new-3"]),
            "amount": pyarrow.array([999, 300], type=pyarrow.int32()),
        }
    )
    outcome = upsert_session.copy_from(
        "public", "_v030_upsert", tbl,
        mode="upsert", mapping_policy="compatible", keys=["id"],
    )
    assert outcome["status"] == "committed"

    # Verifica: id=1 aggiornato, id=2 invariato, id=3 nuovo
    rows = upsert_session.query_sql(
        "SELECT id, label, amount FROM _v030_upsert ORDER BY id"
    )
    ids = [r["id"] for r in rows]
    labels = [r["label"] for r in rows]
    assert ids == [1, 2, 3]
    assert labels == ["updated-1", "existing-2", "new-3"]


def test_copy_from_upsert_without_keys_raises_invalid_plan(upsert_session) -> None:
    tbl = pyarrow.table({"id": [1], "label": ["x"], "amount": [1]})
    with pytest.raises(p.PlenoraInvalidPlanError) as exc:
        upsert_session.copy_from(
            "public", "_v030_upsert", tbl,
            mode="upsert", mapping_policy="compatible",
        )
    assert "richiede almeno una key" in str(exc.value)


def test_copy_from_delete_by_keys_removes_matching_rows(upsert_session) -> None:
    # Solo la colonna key deve essere nel dataset per DeleteByKeys
    tbl = pyarrow.table({"id": pyarrow.array([1, 2], type=pyarrow.int64())})
    outcome = upsert_session.copy_from(
        "public", "_v030_upsert", tbl,
        mode="delete_by_keys", mapping_policy="compatible", keys=["id"],
    )
    assert outcome["status"] == "committed"

    remaining = upsert_session.execute_scalar(
        "SELECT COUNT(*)::BIGINT FROM _v030_upsert"
    )
    assert remaining == 0


def test_copy_from_keys_rejected_for_append_mode(upsert_session) -> None:
    tbl = pyarrow.table({"id": [4], "label": ["z"], "amount": [1]})
    with pytest.raises(p.PlenoraInvalidPlanError) as exc:
        upsert_session.copy_from(
            "public", "_v030_upsert", tbl,
            mode="append", mapping_policy="compatible", keys=["id"],
        )
    assert "keys" in str(exc.value).lower()


# ============================ P1.2 — read filters ==================================


@pytest.fixture(name="read_session")
def _read_session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.execute_sql("DROP TABLE IF EXISTS _v030_read")
    s.execute_sql(
        "CREATE TABLE _v030_read ("
        " id BIGINT PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " amount INT NOT NULL)"
    )
    s.execute_sql(
        "INSERT INTO _v030_read (id, label, amount) "
        "SELECT gs, 'row-' || gs::TEXT, gs * 10 "
        "FROM generate_series(1, 100) gs"
    )
    try:
        yield s
    finally:
        try:
            s.execute_sql("DROP TABLE IF EXISTS _v030_read")
        finally:
            s.close()


def _read_all(reader) -> "pyarrow.Table":
    parts = []
    for chunk in reader:
        parts.append(pyarrow_ipc.open_stream(io.BytesIO(chunk)).read_all())
    if not parts:
        return pyarrow.table({})
    return pyarrow.concat_tables(parts)


def test_read_projection_selects_only_named_columns(read_session) -> None:
    reader = read_session.read(
        "public", "_v030_read", projection=["id", "amount"]
    )
    tbl = _read_all(reader)
    assert tbl.column_names == ["id", "amount"]
    assert tbl.num_rows == 100


def test_read_limit_bounds_result_set(read_session) -> None:
    reader = read_session.read("public", "_v030_read", limit=10)
    tbl = _read_all(reader)
    assert tbl.num_rows == 10


def test_read_order_by_asc_and_limit(read_session) -> None:
    reader = read_session.read(
        "public", "_v030_read",
        order_by=[("id", "asc")], limit=5,
    )
    tbl = _read_all(reader)
    assert tbl.column("id").to_pylist() == [1, 2, 3, 4, 5]


def test_read_order_by_desc_and_limit(read_session) -> None:
    reader = read_session.read(
        "public", "_v030_read",
        order_by=[("id", "desc")], limit=3,
    )
    tbl = _read_all(reader)
    assert tbl.column("id").to_pylist() == [100, 99, 98]


def test_read_invalid_order_by_direction_raises(read_session) -> None:
    with pytest.raises(p.PlenoraInvalidPlanError):
        list(read_session.read(
            "public", "_v030_read",
            order_by=[("id", "sideways")],
        ))


# ============================ P1.3 — pandas + list[dict] ==========================


@pytest.fixture(name="pandas_session")
def _pandas_session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.execute_sql("DROP TABLE IF EXISTS _v030_pandas")
    s.execute_sql(
        "CREATE TABLE _v030_pandas ("
        " id BIGINT PRIMARY KEY, "
        " label TEXT NOT NULL, "
        " amount DOUBLE PRECISION NOT NULL)"
    )
    try:
        yield s
    finally:
        try:
            s.execute_sql("DROP TABLE IF EXISTS _v030_pandas")
        finally:
            s.close()


def test_copy_from_accepts_list_of_dict(pandas_session) -> None:
    rows = [
        {"id": 1, "label": "a", "amount": 10.5},
        {"id": 2, "label": "b", "amount": 20.5},
        {"id": 3, "label": "c", "amount": 30.5},
    ]
    outcome = pandas_session.copy_from(
        "public", "_v030_pandas", rows, mapping_policy="compatible"
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 3


def test_copy_from_accepts_pandas_dataframe(pandas_session) -> None:
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame(
        {
            "id": [10, 11, 12],
            "label": ["x", "y", "z"],
            "amount": [10.5, 11.5, 12.5],
        }
    )
    outcome = pandas_session.copy_from(
        "public", "_v030_pandas", df, mapping_policy="compatible"
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 3

    count = pandas_session.execute_scalar(
        "SELECT COUNT(*)::BIGINT FROM _v030_pandas"
    )
    assert count == 3


def test_copy_from_list_of_dict_empty_raises(pandas_session) -> None:
    with pytest.raises(ValueError):
        pandas_session.copy_from(
            "public", "_v030_pandas", [], mapping_policy="compatible"
        )


def test_copy_from_list_of_int_raises_type_error(pandas_session) -> None:
    with pytest.raises(TypeError):
        pandas_session.copy_from(
            "public", "_v030_pandas", [1, 2, 3], mapping_policy="compatible"
        )
