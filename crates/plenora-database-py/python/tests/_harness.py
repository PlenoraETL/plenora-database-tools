"""Connessioni condivise per i test funzionali del SDK.

Il riferimento di sviluppo PostgreSQL è plaintext, mentre il default del SDK
richiede TLS verificato. Gli helper rendono esplicita questa deroga in un solo
punto.

I test che verificano il **default** sicuro costruiscono invece un
`EngineConfig` senza la deroga.
"""

from __future__ import annotations

import os

import pytest

import plenora_database as p

POSTGRES_DSN_ENV = "PLENORA_TEST_POSTGRES_DSN"
AGE_DSN_ENV = "PLENORA_TEST_AGE_DSN"

# Il riferimento di sviluppo non parla TLS: chiederlo qui e l'unica differenza
# rispetto a un uso di produzione del SDK.
LOCAL_TLS_MODE = "insecure_local"


def postgres_dsn_or_skip() -> str:
    """La DSN del riferimento, o salta il test se non e configurata."""

    dsn = os.environ.get(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"live test: manca env {POSTGRES_DSN_ENV}")
    return dsn


def connect_postgres(dsn: str | None = None):
    """Sessione sync verso il riferimento plaintext.

    Salta il test se la DSN non e configurata.
    """

    config = p.EngineConfig.from_postgres_dsn(
        dsn or postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
    )
    return p.engine_from_url(config).session()


async def aconnect_postgres(dsn: str | None = None):
    """Sessione async verso il riferimento plaintext.

    Salta il test se la DSN non e configurata.
    """

    config = p.EngineConfig.from_postgres_dsn(
        dsn or postgres_dsn_or_skip(), tls_mode=LOCAL_TLS_MODE
    )
    return (await p.async_engine_from_url(config)).session()


def age_dsn_or_skip() -> str:
    dsn = os.environ.get(AGE_DSN_ENV)
    if not dsn:
        pytest.skip(f"live test AGE: manca env {AGE_DSN_ENV}")
    return dsn


def connect_age():
    return connect_postgres(age_dsn_or_skip())


async def aconnect_age():
    return await aconnect_postgres(age_dsn_or_skip())


def _network_config(
    provider: str,
    values: tuple,
) -> p.EngineConfig:
    host, database, user, password, ca_pem = values
    tls_ca = None if ca_pem is None else ca_pem.decode("utf-8")
    return p.EngineConfig(
        provider,
        host=host,
        database=database,
        user=user,
        password=password,
        tls_ca=tls_ca,
    )


# ================================== MySQL ===================================
#
# Il riferimento MySQL, al contrario di quello Postgres, **impone** TLS con
# una CA privata: qui non si deroga a nulla, si fornisce il materiale.

MYSQL_HOST_ENV = "PLENORA_TEST_MYSQL_HOST"
MYSQL_DB_ENV = "PLENORA_TEST_MYSQL_DATABASE"
MYSQL_USER_ENV = "PLENORA_TEST_MYSQL_USER"
MYSQL_PWD_ENV = "PLENORA_TEST_MYSQL_PASSWORD"
MYSQL_CA_ENV = "PLENORA_TEST_MYSQL_CA"


def mysql_config_or_skip() -> tuple:
    """`(host, database, user, password, ca_pem)`, o salta il test.

    Senza `ca_pem` la connessione fallirebbe con un errore I/O redatto, che
    non dice al lettore che manca il materiale TLS: il percorso arriva da
    `PLENORA_TEST_MYSQL_CA`, come nei gate.
    """

    host = os.environ.get(MYSQL_HOST_ENV)
    password = os.environ.get(MYSQL_PWD_ENV)
    if not host or not password:
        pytest.skip(
            f"live test MySQL: mancano env {MYSQL_HOST_ENV} e/o {MYSQL_PWD_ENV}"
        )
    ca_pem = None
    ca_path = os.environ.get(MYSQL_CA_ENV)
    if ca_path:
        with open(ca_path, "rb") as handle:
            ca_pem = handle.read()
    return (
        host,
        os.environ.get(MYSQL_DB_ENV, "dataflow_test"),
        os.environ.get(MYSQL_USER_ENV, "dataflow"),
        password,
        ca_pem,
    )


def connect_mysql_reference():
    """Sessione `MySQL` sync verso il riferimento, con la CA privata."""

    return p.engine_from_url(_network_config("mysql", mysql_config_or_skip())).session()


async def aconnect_mysql_reference():
    """Sessione `MySQL` async verso il riferimento, con la CA privata."""

    engine = await p.async_engine_from_url(
        _network_config("mysql", mysql_config_or_skip())
    )
    return engine.session()


# ================================ MariaDB ==================================

MARIADB_HOST_ENV = "PLENORA_TEST_MARIADB_HOST"
MARIADB_DB_ENV = "PLENORA_TEST_MARIADB_DATABASE"
MARIADB_USER_ENV = "PLENORA_TEST_MARIADB_USER"
MARIADB_PWD_ENV = "PLENORA_TEST_MARIADB_PASSWORD"
MARIADB_CA_ENV = "PLENORA_TEST_MARIADB_CA"


def mariadb_config_or_skip() -> tuple:
    host = os.environ.get(MARIADB_HOST_ENV)
    password = os.environ.get(MARIADB_PWD_ENV)
    if not host or not password:
        pytest.skip(
            f"live test MariaDB: mancano env {MARIADB_HOST_ENV} e/o {MARIADB_PWD_ENV}"
        )
    ca_pem = None
    ca_path = os.environ.get(MARIADB_CA_ENV)
    if ca_path:
        with open(ca_path, "rb") as handle:
            ca_pem = handle.read()
    return (
        host,
        os.environ.get(MARIADB_DB_ENV, "dataflow_test"),
        os.environ.get(MARIADB_USER_ENV, "dataflow"),
        password,
        ca_pem,
    )


def connect_mariadb_reference():
    return p.engine_from_url(
        _network_config("mariadb", mariadb_config_or_skip())
    ).session()


async def aconnect_mariadb_reference():
    engine = await p.async_engine_from_url(
        _network_config("mariadb", mariadb_config_or_skip())
    )
    return engine.session()


# ============================== SQL Server ================================

SQLSERVER_HOST_ENV = "PLENORA_TEST_SQLSERVER_HOST"
SQLSERVER_DB_ENV = "PLENORA_TEST_SQLSERVER_DATABASE"
SQLSERVER_USER_ENV = "PLENORA_TEST_SQLSERVER_USER"
SQLSERVER_PWD_ENV = "PLENORA_TEST_SQLSERVER_PASSWORD"
SQLSERVER_CA_ENV = "PLENORA_TEST_SQLSERVER_CA"


def sqlserver_config_or_skip() -> tuple:
    host = os.environ.get(SQLSERVER_HOST_ENV)
    password = os.environ.get(SQLSERVER_PWD_ENV)
    if not host or not password:
        pytest.skip(
            "live test SQL Server: mancano env "
            f"{SQLSERVER_HOST_ENV} e/o {SQLSERVER_PWD_ENV}"
        )
    ca_pem = None
    ca_path = os.environ.get(SQLSERVER_CA_ENV)
    if ca_path:
        with open(ca_path, "rb") as handle:
            ca_pem = handle.read()
    return (
        host,
        os.environ.get(SQLSERVER_DB_ENV, "dataflow_test"),
        os.environ.get(SQLSERVER_USER_ENV, "dataflow"),
        password,
        ca_pem,
    )


def connect_sqlserver_reference():
    return p.engine_from_url(
        _network_config("sqlserver", sqlserver_config_or_skip())
    ).session()


async def aconnect_sqlserver_reference():
    engine = await p.async_engine_from_url(
        _network_config("sqlserver", sqlserver_config_or_skip())
    )
    return engine.session()


# ============================== IBM Db2 LUW ==============================

DB2_HOST_ENV = "PLENORA_TEST_DB2_HOST"
DB2_DB_ENV = "PLENORA_TEST_DB2_DATABASE"
DB2_USER_ENV = "PLENORA_TEST_DB2_USER"
DB2_PWD_ENV = "PLENORA_TEST_DB2_PASSWORD"
DB2_PORT_ENV = "PLENORA_TEST_DB2_PORT"
DB2_CA_ENV = "PLENORA_TEST_DB2_CA"
DB2_TLS_MODE_ENV = "PLENORA_TEST_DB2_TLS_MODE"


def db2_config_or_skip() -> tuple:
    host = os.environ.get(DB2_HOST_ENV)
    password = os.environ.get(DB2_PWD_ENV)
    if not host or not password:
        pytest.skip(f"live test Db2: mancano env {DB2_HOST_ENV} e/o {DB2_PWD_ENV}")
    return (
        host,
        os.environ.get(DB2_DB_ENV, "plenora"),
        os.environ.get(DB2_USER_ENV, "db2inst1"),
        password,
        int(os.environ.get(DB2_PORT_ENV, "50000")),
        os.environ.get(DB2_CA_ENV),
        os.environ.get(DB2_TLS_MODE_ENV, "require"),
    )


def connect_db2_reference():
    host, database, user, password, port, ca_path, tls_mode = db2_config_or_skip()
    config = p.EngineConfig(
        "db2", host, database, user, password, port, tls_mode, ca_path
    )
    return p.engine_from_url(config).session()


async def aconnect_db2_reference():
    host, database, user, password, port, ca_path, tls_mode = db2_config_or_skip()
    config = p.EngineConfig(
        "db2", host, database, user, password, port, tls_mode, ca_path
    )
    return (await p.async_engine_from_url(config)).session()


# ================================ Oracle ==================================

ORACLE_HOST_ENV = "PLENORA_TEST_ORACLE_HOST"
ORACLE_SERVICE_ENV = "PLENORA_TEST_ORACLE_SERVICE"
ORACLE_USER_ENV = "PLENORA_TEST_ORACLE_USER"
ORACLE_PWD_ENV = "PLENORA_TEST_ORACLE_PASSWORD"
ORACLE_PORT_ENV = "PLENORA_TEST_ORACLE_PORT"
ORACLE_CA_ENV = "PLENORA_TEST_ORACLE_CA"
ORACLE_TLS_MODE_ENV = "PLENORA_TEST_ORACLE_TLS_MODE"


def oracle_config_or_skip() -> p.EngineConfig:
    host = os.environ.get(ORACLE_HOST_ENV)
    password = os.environ.get(ORACLE_PWD_ENV)
    if not host or not password:
        pytest.skip(
            f"live test Oracle: mancano env {ORACLE_HOST_ENV} e/o {ORACLE_PWD_ENV}"
        )
    return p.EngineConfig(
        "oracle",
        host=host,
        database=os.environ.get(ORACLE_SERVICE_ENV, "FREEPDB1"),
        user=os.environ.get(ORACLE_USER_ENV, "plenora"),
        password=password,
        port=int(os.environ.get(ORACLE_PORT_ENV, "1521")),
        tls_ca=os.environ.get(ORACLE_CA_ENV),
        tls_mode=os.environ.get(ORACLE_TLS_MODE_ENV, "require"),
    )


def connect_oracle_reference():
    return p.engine_from_url(oracle_config_or_skip()).session()


async def aconnect_oracle_reference():
    return (await p.async_engine_from_url(oracle_config_or_skip())).session()
