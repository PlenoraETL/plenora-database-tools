"""Lifecycle applicativo del vero Engine Core v3 esposto a Python."""

from __future__ import annotations

import pytest

import plenora_database as p

from ._harness import (
    LOCAL_TLS_MODE,
    mariadb_config_or_skip,
    mysql_config_or_skip,
    postgres_dsn_or_skip,
    sqlserver_config_or_skip,
)


FAMILY_ENGINES = (
    ("mysql", p.create_mysql_engine, p.create_async_mysql_engine, mysql_config_or_skip),
    (
        "mariadb",
        p.create_mariadb_engine,
        p.create_async_mariadb_engine,
        mariadb_config_or_skip,
    ),
    (
        "sqlserver",
        p.create_sqlserver_engine,
        p.create_async_sqlserver_engine,
        sqlserver_config_or_skip,
    ),
)


def _family_engine(factory, config):
    host, database, user, password, ca_pem = config()
    return factory(host, database, user, password, tls_ca_pem=ca_pem)


def _postgres_expression_statement():
    tables = p.table("tables", "table_name", schema="information_schema")
    return (
        p.select(tables.c.table_name)
        .where(tables.c.table_name >= p.bind("minimum"))
        .order_by(tables.c.table_name)
        .limit(1)
    )


def _catalog_aggregate_statement():
    objects = p.table("tables", "table_schema", schema="information_schema")
    grouping = objects.c.table_schema
    total = p.func.count()
    return (
        p.select(total.label("object_count"))
        .select_from(objects)
        .group_by(grouping)
        .having(total >= total)
        .order_by(grouping)
        .limit(1)
    )


def _window_cte_statement():
    objects = p.table("tables", "table_schema", schema="information_schema")
    ranked = p.select(
        p.func.count(objects.c.table_schema).over().label("position")
    ).cte("ranked")
    return p.select(ranked.c.position).select_from(ranked).limit(1)


def test_engine_reuses_one_pool_across_request_sessions() -> None:
    engine = p.create_engine(postgres_dsn_or_skip(), LOCAL_TLS_MODE)
    assert engine.provider_kind == "postgres"
    with engine.session() as session:
        assert session.execute_scalar("SELECT 1::BIGINT") == 1
        result = session.execute(_postgres_expression_statement(), {"minimum": "a"})
        assert isinstance(result.scalar_one(), str)
        aggregate = session.execute(_catalog_aggregate_statement())
        assert aggregate.scalar_one() >= 1
        assert session.execute(_window_cte_statement()).scalar_one() >= 1
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


def test_engine_session_is_exclusive_during_transaction_and_reusable_after() -> None:
    with p.create_engine(postgres_dsn_or_skip(), LOCAL_TLS_MODE) as engine:
        with engine.session() as session:
            transaction = session.begin(native_query_policy="allow")
            with pytest.raises(RuntimeError, match="transazione esplicita"):
                session.execute_scalar("SELECT 1::BIGINT")
            transaction.rollback()
            assert session.execute_scalar("SELECT 2::BIGINT") == 2


@pytest.mark.asyncio
async def test_async_engine_reuses_one_pool_across_request_sessions() -> None:
    engine = await p.create_async_engine(postgres_dsn_or_skip(), LOCAL_TLS_MODE)
    assert engine.provider_kind == "postgres"
    async with engine.session() as session:
        assert await session.execute_scalar("SELECT 3::BIGINT") == 3
        result = await session.execute(_postgres_expression_statement(), {"minimum": "a"})
        assert isinstance(result.scalar_one(), str)
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


@pytest.mark.asyncio
async def test_async_engine_session_is_exclusive_and_reusable() -> None:
    async with await p.create_async_engine(
        postgres_dsn_or_skip(), LOCAL_TLS_MODE
    ) as engine:
        async with engine.session() as session:
            transaction = await session.begin(native_query_policy="allow")
            with pytest.raises(RuntimeError, match="transazione esplicita"):
                await session.execute_scalar("SELECT 1::BIGINT")
            await transaction.rollback()
            assert await session.execute_scalar("SELECT 2::BIGINT") == 2


@pytest.mark.parametrize(
    ("provider_kind", "factory", "_async_factory", "config"),
    FAMILY_ENGINES,
    ids=[entry[0] for entry in FAMILY_ENGINES],
)
def test_family_engine_uses_the_core_lifecycle(
    provider_kind, factory, _async_factory, config
) -> None:
    engine = _family_engine(factory, config)
    assert engine.provider_kind == provider_kind
    with engine.session() as session:
        assert session.execute_scalar("SELECT 5") == 5
        statement = p.select(p.bind("answer").label("answer"))
        assert session.execute(statement, {"answer": 15}).scalar_one() == 15
        aggregate = session.execute(_catalog_aggregate_statement())
        assert aggregate.scalar_one() >= 1
        assert session.execute(_window_cte_statement()).scalar_one() >= 1
    assert engine.statistics() == {
        "sessions_opened": 1,
        "active_sessions": 0,
        "disposed": False,
    }
    engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_kind", "_factory", "async_factory", "config"),
    FAMILY_ENGINES,
    ids=[entry[0] for entry in FAMILY_ENGINES],
)
async def test_async_family_engine_uses_the_core_lifecycle(
    provider_kind, _factory, async_factory, config
) -> None:
    engine = await _family_engine(async_factory, config)
    assert engine.provider_kind == provider_kind
    async with engine.session() as session:
        assert await session.execute_scalar("SELECT 6") == 6
        statement = p.select(p.bind("answer").label("answer"))
        assert (await session.execute(statement, {"answer": 16})).scalar_one() == 16
    assert engine.statistics()["active_sessions"] == 0
    engine.dispose()
