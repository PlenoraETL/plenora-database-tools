"""Campagna ripetuta sulle superfici runtime piu sensibili del wheel."""

from __future__ import annotations

import asyncio

import pytest

import plenora_database as p

from ._harness import aconnect_postgres, connect_postgres, postgres_dsn_or_skip


# Dieci iterazioni rendono visibili race e regressioni intermittenti senza
# trasformare il gate settimanale in un benchmark dipendente dal runner.
CYCLES = range(10)


@pytest.mark.parametrize("cycle", CYCLES)
def test_stabilization_timeout_cancellation_recovers_the_session(cycle: int) -> None:
    del cycle
    dsn = postgres_dsn_or_skip()
    with connect_postgres(dsn) as session:
        with pytest.raises(p.PlenoraCancelledError):
            with session.begin(statement_timeout_ms=50) as transaction:
                transaction.execute_sql("SELECT pg_sleep(2)")
        assert session.execute_scalar("SELECT 1") == 1


@pytest.mark.parametrize("cycle", CYCLES)
def test_stabilization_explicit_rollback_leaves_no_row(cycle: int) -> None:
    dsn = postgres_dsn_or_skip()
    table = f"_plenora_stabilization_{cycle}"
    with connect_postgres(dsn) as session:
        session.execute_sql(f"DROP TABLE IF EXISTS {table}")
        session.execute_sql(f"CREATE TABLE {table} (id INT PRIMARY KEY)")
        try:
            transaction = session.begin()
            transaction.execute_sql(f"INSERT INTO {table} (id) VALUES ($1)", [cycle])
            transaction.rollback()
            assert session.execute_scalar(f"SELECT COUNT(*)::BIGINT FROM {table}") == 0
        finally:
            session.execute_sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.asyncio
@pytest.mark.parametrize("cycle", CYCLES)
async def test_stabilization_concurrent_queries_share_the_runtime(cycle: int) -> None:
    dsn = postgres_dsn_or_skip()
    async with await aconnect_postgres(dsn) as session:
        expected = [cycle * 20 + offset for offset in range(20)]
        observed = await asyncio.gather(
            *(session.execute_scalar("SELECT $1::int", [value]) for value in expected)
        )
    assert observed == expected
