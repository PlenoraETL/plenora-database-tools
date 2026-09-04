"""Fail-close delle factory native prima di qualsiasi connessione."""

from __future__ import annotations

from collections.abc import Callable

import pytest

import plenora_database as p
from plenora_database import PlenoraUnsupportedError
from plenora_database import _native as native

SECRET = "database-password-that-must-not-leak-884"
INVALID_TLS = "invalid-mode-that-must-not-leak-225"


def assert_rejected_without_payload(call: Callable[[], object], fragment: str) -> None:
    with pytest.raises(RuntimeError) as error:
        call()
    message = str(error.value)
    assert fragment in message
    assert SECRET not in message
    assert INVALID_TLS not in message


@pytest.mark.parametrize(
    "call",
    [
        lambda: native.connect("password=" + SECRET, INVALID_TLS),
        lambda: native.create_engine("password=" + SECRET, INVALID_TLS, 4, 1_000),
    ],
    ids=["session-postgres", "engine-postgres"],
)
def test_postgres_factories_reject_unknown_tls_before_network(call) -> None:
    assert_rejected_without_payload(call, "tls_mode non riconosciuto")


@pytest.mark.parametrize(
    "factory",
    [native.connect_mysql, native.connect_mariadb, native.connect_sqlserver],
    ids=["mysql", "mariadb", "sqlserver"],
)
def test_family_sessions_reject_unknown_tls_before_network(factory) -> None:
    assert_rejected_without_payload(
        lambda: factory("host", "database", "user", SECRET, None, None, INVALID_TLS),
        "tls_mode non riconosciuto",
    )


@pytest.mark.parametrize(
    "factory",
    [
        native.create_mysql_engine,
        native.create_mariadb_engine,
        native.create_sqlserver_engine,
    ],
    ids=["mysql", "mariadb", "sqlserver"],
)
def test_family_engines_reject_unknown_tls_before_network(factory) -> None:
    assert_rejected_without_payload(
        lambda: factory(
            "host", "database", "user", SECRET, None, None, INVALID_TLS, 4, 1_000
        ),
        "tls_mode non riconosciuto",
    )


@pytest.mark.parametrize("factory", [native.connect_oracle, native.create_oracle_engine])
def test_oracle_factories_reject_unknown_tls_before_network(factory) -> None:
    assert_rejected_without_payload(
        lambda: factory("host", "service", "user", SECRET, None, None, INVALID_TLS),
        "tls_mode Oracle non riconosciuto",
    )


@pytest.mark.parametrize("factory", [native.connect_mysql, native.connect_sqlserver])
def test_certificate_size_limit_precedes_network(factory) -> None:
    oversized = b"x" * (1024 * 1024 + 1)
    assert_rejected_without_payload(
        lambda: factory("host", "database", "user", SECRET, None, oversized, "require"),
        "CA PEM oltre 1 MiB",
    )


def test_standard_wheel_db2_stubs_are_typed_and_payload_free() -> None:
    calls = (
        lambda: native.connect_db2("host", "database", "user", SECRET),
        lambda: native.create_db2_engine("host", "database", "user", SECRET),
        lambda: native.create_async_db2_engine("host", "database", "user", SECRET),
    )
    for call in calls:
        with pytest.raises(PlenoraUnsupportedError) as error:
            call()
        assert error.value.category == "unsupported"
        assert error.value.phase == "prepare"
        assert error.value.provider == "db2"
        assert SECRET not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "factory",
    [
        native.aconnect_mysql,
        native.aconnect_mariadb,
        native.aconnect_sqlserver,
    ],
    ids=["mysql", "mariadb", "sqlserver"],
)
async def test_async_family_sessions_reject_unknown_tls_before_network(factory) -> None:
    with pytest.raises(RuntimeError) as error:
        await factory("host", "database", "user", SECRET, None, None, INVALID_TLS)
    assert SECRET not in str(error.value)
    assert INVALID_TLS not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "factory",
    [
        native.create_async_mysql_engine,
        native.create_async_mariadb_engine,
        native.create_async_sqlserver_engine,
    ],
    ids=["mysql", "mariadb", "sqlserver"],
)
async def test_async_family_engines_reject_unknown_tls_before_network(factory) -> None:
    with pytest.raises(RuntimeError) as error:
        await factory(
            "host", "database", "user", SECRET, None, None, INVALID_TLS, 4, 1_000
        )
    assert SECRET not in str(error.value)
    assert INVALID_TLS not in str(error.value)


@pytest.mark.asyncio
async def test_async_postgres_and_oracle_reject_unknown_tls_before_network() -> None:
    with pytest.raises(RuntimeError) as postgres_error:
        await native.aconnect("password=" + SECRET, INVALID_TLS)
    with pytest.raises(RuntimeError) as oracle_error:
        await native.create_async_oracle_engine(
            "host", "service", "user", SECRET, None, None, INVALID_TLS
        )
    for error in (postgres_error, oracle_error):
        assert SECRET not in str(error.value)
        assert INVALID_TLS not in str(error.value)


def test_public_oracle_engine_wrapper_reaches_native_validation() -> None:
    assert_rejected_without_payload(
        lambda: p._create_oracle_engine(
            "host",
            "service",
            "user",
            SECRET,
            tls_mode=INVALID_TLS,
            max_connections=4,
            acquire_timeout_ms=1_000,
        ),
        "tls_mode Oracle non riconosciuto",
    )


@pytest.mark.asyncio
async def test_public_async_oracle_engine_wrapper_reaches_native_validation() -> None:
    with pytest.raises(RuntimeError) as error:
        await p._create_async_oracle_engine(
            "host",
            "service",
            "user",
            SECRET,
            tls_mode=INVALID_TLS,
            max_connections=4,
            acquire_timeout_ms=1_000,
        )
    assert "tls_mode Oracle non riconosciuto" in str(error.value)
    assert SECRET not in str(error.value)
    assert INVALID_TLS not in str(error.value)
