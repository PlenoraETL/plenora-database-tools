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
    ("mysql", mysql_config_or_skip),
    ("mariadb", mariadb_config_or_skip),
    ("sqlserver", sqlserver_config_or_skip),
)


def test_engine_orm_factory_closes_session_when_construction_fails() -> None:
    class NativeEngine:
        def session(self):
            return object()

    class CoreSession:
        capabilities = {"provider": "postgres"}

        def __init__(self, *, fail_begin: bool) -> None:
            self.fail_begin = fail_begin
            self.closed = False

        def begin(self):
            if self.fail_begin:
                raise RuntimeError("begin failure")
            return object()

        def close(self) -> None:
            self.closed = True

    sync_session = CoreSession(fail_begin=True)
    engine = p.Engine(NativeEngine(), lambda _: sync_session)
    with pytest.raises(RuntimeError, match="begin failure"):
        engine.orm_session()
    assert sync_session.closed

    async_session = CoreSession(fail_begin=False)
    async_engine = p.AsyncEngine(NativeEngine(), lambda _: async_session)
    with pytest.raises(ValueError, match="insert_batch_size"):
        async_engine.orm_session(insert_batch_size=0)
    assert async_session.closed


def test_pool_config_is_explicit_validated_and_forwarded(monkeypatch) -> None:
    pool = p.PoolConfig(max_connections=9, acquire_timeout_ms=2_500)
    config = p.EngineConfig.from_postgres_dsn("dbname=app", pool=pool)
    captured: dict[str, object] = {}

    def factory(dsn, tls_mode, *, max_connections, acquire_timeout_ms):
        captured.update(
            dsn=dsn,
            tls_mode=tls_mode,
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
        return "engine"

    monkeypatch.setattr(p, "_create_postgres_engine", factory)
    assert p.engine_from_url(config) == "engine"
    assert captured == {
        "dsn": "dbname=app",
        "tls_mode": "require",
        "max_connections": 9,
        "acquire_timeout_ms": 2_500,
    }

    for invalid in (0, -1, True):
        with pytest.raises(ValueError):
            p.PoolConfig(max_connections=invalid)
        with pytest.raises(ValueError):
            p.PoolConfig(acquire_timeout_ms=invalid)
    with pytest.raises(ValueError, match="Db2"):
        p.EngineConfig("db2", pool=pool)
    monkeypatch.setattr(p, "_create_db2_engine", lambda *args, **kwargs: kwargs)
    config = p.EngineConfig("db2", "host", "db", "user", "password", tls_mode="insecure_local")
    assert p.engine_from_url(config)["tls_mode"] == "disable"

    monkeypatch.setattr(p, "_create_oracle_engine", lambda *args, **kwargs: (args, kwargs))
    oracle = p.EngineConfig.from_url(
        "oracle://user:password@host:1521/FREEPDB1?tls_mode=insecure_local"
        "&max_connections=9&acquire_timeout_ms=2500"
    )
    args, kwargs = p.engine_from_url(oracle)
    assert args == ("host", "FREEPDB1", "user", "password", 1521)
    assert kwargs == {
        "tls_ca_path": None,
        "tls_mode": "disable",
        "max_connections": 9,
        "acquire_timeout_ms": 2_500,
    }


def test_pool_url_options_are_control_fields_not_provider_dsn_content() -> None:
    config = p.EngineConfig.from_url(
        "postgresql://user:secret@db/app?application_name=sdk"
        "&max_connections=7&acquire_timeout_ms=3210"
    )
    assert config.pool == p.PoolConfig(7, 3_210)
    assert config._raw_url is not None
    assert "application_name=sdk" in config._raw_url
    assert "max_connections" not in config._raw_url
    assert "acquire_timeout_ms" not in config._raw_url


def test_oracle_wrapper_forwards_pool_controls_to_the_native_factory(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def native_factory(*args, **kwargs):
        captured.update(args=args, kwargs=kwargs)
        return object()

    monkeypatch.setattr(p, "_native_create_oracle_engine", native_factory)
    p._create_oracle_engine(
        "host",
        "service",
        "user",
        "password",
        1521,
        None,
        "disable",
        max_connections=9,
        acquire_timeout_ms=2_500,
    )

    assert captured == {
        "args": ("host", "service", "user", "password"),
        "kwargs": {
            "port": 1521,
            "tls_ca_path": None,
            "tls_mode": "disable",
            "max_connections": 9,
            "acquire_timeout_ms": 2_500,
        },
    }


@pytest.mark.asyncio
async def test_async_oracle_wrapper_forwards_pool_controls_to_the_native_factory(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    async def native_factory(*args, **kwargs):
        captured.update(args=args, kwargs=kwargs)
        return object()

    monkeypatch.setattr(p, "_native_create_async_oracle_engine", native_factory)
    await p._create_async_oracle_engine(
        "host",
        "service",
        "user",
        "password",
        1521,
        None,
        "disable",
        max_connections=9,
        acquire_timeout_ms=2_500,
    )

    assert captured == {
        "args": ("host", "service", "user", "password"),
        "kwargs": {
            "port": 1521,
            "tls_ca_path": None,
            "tls_mode": "disable",
            "max_connections": 9,
            "acquire_timeout_ms": 2_500,
        },
    }


def _family_config(provider, config):
    host, database, user, password, ca_pem = config()
    return p.EngineConfig(
        provider,
        host=host,
        database=database,
        user=user,
        password=password,
        tls_ca=None if ca_pem is None else ca_pem.decode("utf-8"),
    )


def _postgres_expression_statement():
    tables = p.table("tables", "table_name", schema="information_schema")
    return (
        p.select(tables.c.table_name)
        .where(tables.c.table_name >= p.bind("minimum", p.BindType.STRING))
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
    engine = p.engine_from_url(
        p.EngineConfig.from_postgres_dsn(
            postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
        )
    )
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
    with p.engine_from_url(
        p.EngineConfig.from_postgres_dsn(
            postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
        )
    ) as engine:
        with engine.session() as session:
            with session.begin(native_query_policy="allow") as transaction:
                assert transaction.execute_scalar("SELECT 2::BIGINT") == 2
        assert engine.statistics()["active_sessions"] == 0


def test_engine_session_is_exclusive_during_transaction_and_reusable_after() -> None:
    with p.engine_from_url(
        p.EngineConfig.from_postgres_dsn(
            postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
        )
    ) as engine:
        with engine.session() as session:
            transaction = session.begin(native_query_policy="allow")
            with pytest.raises(RuntimeError, match="transazione esplicita"):
                session.execute_scalar("SELECT 1::BIGINT")
            transaction.rollback()
            assert session.execute_scalar("SELECT 2::BIGINT") == 2


@pytest.mark.asyncio
async def test_async_engine_reuses_one_pool_across_request_sessions() -> None:
    engine = await p.async_engine_from_url(
        p.EngineConfig.from_postgres_dsn(
            postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
        )
    )
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
    async with await p.async_engine_from_url(
        p.EngineConfig.from_postgres_dsn(
            postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
        )
    ) as engine:
        async with engine.session() as session:
            async with await session.begin(
                native_query_policy="allow"
            ) as transaction:
                assert await transaction.execute_scalar("SELECT 4::BIGINT") == 4
        assert engine.statistics()["active_sessions"] == 0


@pytest.mark.asyncio
async def test_async_engine_session_is_exclusive_and_reusable() -> None:
    async with await p.async_engine_from_url(
        p.EngineConfig.from_postgres_dsn(
            postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
        )
    ) as engine:
        async with engine.session() as session:
            transaction = await session.begin(native_query_policy="allow")
            with pytest.raises(RuntimeError, match="transazione esplicita"):
                await session.execute_scalar("SELECT 1::BIGINT")
            await transaction.rollback()
            assert await session.execute_scalar("SELECT 2::BIGINT") == 2


@pytest.mark.parametrize(
    ("provider_kind", "config"),
    FAMILY_ENGINES,
    ids=[entry[0] for entry in FAMILY_ENGINES],
)
def test_family_engine_uses_the_core_lifecycle(provider_kind, config) -> None:
    engine = p.engine_from_url(_family_config(provider_kind, config))
    assert engine.provider_kind == provider_kind
    with engine.session() as session:
        assert session.execute_scalar("SELECT 5") == 5
        statement = p.select(p.bind("answer", p.BindType.INTEGER).label("answer"))
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
    ("provider_kind", "config"),
    FAMILY_ENGINES,
    ids=[entry[0] for entry in FAMILY_ENGINES],
)
async def test_async_family_engine_uses_the_core_lifecycle(
    provider_kind, config
) -> None:
    engine = await p.async_engine_from_url(_family_config(provider_kind, config))
    assert engine.provider_kind == provider_kind
    async with engine.session() as session:
        assert await session.execute_scalar("SELECT 6") == 6
        statement = p.select(p.bind("answer", p.BindType.INTEGER).label("answer"))
        assert (await session.execute(statement, {"answer": 16})).scalar_one() == 16
    assert engine.statistics()["active_sessions"] == 0
    engine.dispose()
