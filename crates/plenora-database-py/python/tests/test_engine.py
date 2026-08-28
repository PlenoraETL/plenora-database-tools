"""Lifecycle applicativo del vero Engine Core v3 esposto a Python."""

from __future__ import annotations

import pytest

import plenora_database as p

from ._harness import LOCAL_TLS_MODE, postgres_dsn_or_skip


def test_engine_reuses_one_pool_across_request_sessions() -> None:
    engine = p.create_engine(postgres_dsn_or_skip(), LOCAL_TLS_MODE)
    assert engine.provider_kind == "postgres"
    with engine.session() as session:
        assert session.execute_scalar("SELECT 1::BIGINT") == 1
    statistics = engine.statistics()
    assert statistics["sessions_opened"] == 1
    assert statistics["active_sessions"] == 0
    disposed_session = engine.session()
    engine.dispose()
    assert engine.is_disposed
    with pytest.raises(p.PlenoraError):
        disposed_session.inspect.schemas()
    disposed_session.close()


def test_engine_owned_transaction_closes_its_request_session() -> None:
    with p.create_engine(postgres_dsn_or_skip(), LOCAL_TLS_MODE) as engine:
        with engine.session() as session:
            with session.begin(native_query_policy="allow") as transaction:
                assert transaction.execute_scalar("SELECT 2::BIGINT") == 2
        assert engine.statistics()["active_sessions"] == 0


@pytest.mark.asyncio
async def test_async_engine_reuses_one_pool_across_request_sessions() -> None:
    engine = await p.create_async_engine(postgres_dsn_or_skip(), LOCAL_TLS_MODE)
    assert engine.provider_kind == "postgres"
    async with engine.session() as session:
        assert await session.execute_scalar("SELECT 3::BIGINT") == 3
    statistics = engine.statistics()
    assert statistics["sessions_opened"] == 1
    assert statistics["active_sessions"] == 0
    disposed_session = engine.session()
    engine.dispose()
    assert engine.is_disposed
    with pytest.raises(p.PlenoraError):
        await disposed_session.inspect.schemas()
    disposed_session.close()


@pytest.mark.asyncio
async def test_async_engine_owned_transaction_closes_its_request_session() -> None:
    async with await p.create_async_engine(
        postgres_dsn_or_skip(), LOCAL_TLS_MODE
    ) as engine:
        async with engine.session() as session:
            async with await session.begin(
                native_query_policy="allow"
            ) as transaction:
                assert await transaction.execute_scalar("SELECT 4::BIGINT") == 4
        assert engine.statistics()["active_sessions"] == 0
