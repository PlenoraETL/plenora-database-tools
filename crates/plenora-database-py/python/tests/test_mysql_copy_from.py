"""Contratto Replace / TruncateInsert attraverso l'SDK MySQL.

Il provider e gia coperto dai test live Rust; qui si verifica che l'SDK
trasmetta il contratto senza addolcirlo: `replace` deve funzionare e non
ricreare il target, `truncate_insert` deve sollevare un'eccezione **tipizzata**
invece di degradare a un errore generico o, peggio, a un successo.

Copre sia il percorso sincrono (`copy_from`) sia quello async
(`acopy_from`), perche sono due binding distinti sullo stesso piano.
"""
from __future__ import annotations

import os

import pytest
import pytest_asyncio

import plenora_database as p

pyarrow = pytest.importorskip("pyarrow")

MYSQL_HOST_ENV = "PLENORA_TEST_MYSQL_HOST"
MYSQL_DB_ENV = "PLENORA_TEST_MYSQL_DATABASE"
MYSQL_USER_ENV = "PLENORA_TEST_MYSQL_USER"
MYSQL_PWD_ENV = "PLENORA_TEST_MYSQL_PASSWORD"
MYSQL_CA_ENV = "PLENORA_TEST_MYSQL_CA"

TABLE = "_sdk_replace_target"


def _config_or_skip():
    host = os.environ.get(MYSQL_HOST_ENV)
    password = os.environ.get(MYSQL_PWD_ENV)
    if not host or not password:
        pytest.skip(f"live test MySQL: mancano env {MYSQL_HOST_ENV} e/o {MYSQL_PWD_ENV}")
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


def _connect():
    host, database, user, password, ca_pem = _config_or_skip()
    return p.connect_mysql(host, database, user, password, tls_ca_pem=ca_pem)


def _database() -> str:
    return _config_or_skip()[1]


def _table(ids, labels):
    """Tabella Arrow con nullability esplicita.

    `pyarrow.table` marca le colonne nullable per default, mentre il target e
    `NOT NULL`: il provider rifiuta la scrittura con `PlenoraDataMappingError`
    prima ancora di toccare i dati. Lo schema va dichiarato.
    """
    schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64(), nullable=False),
            pyarrow.field("label", pyarrow.string(), nullable=False),
        ]
    )
    return pyarrow.table(
        {
            "id": pyarrow.array(ids, pyarrow.int64()),
            "label": pyarrow.array(labels, pyarrow.string()),
        },
        schema=schema,
    )


def _reset(session) -> None:
    """Ricrea il target con un indice secondario e un valore di default.

    Sono le due cose che una Replace implementata come staging + rename
    perderebbe: il test le controlla dopo la scrittura.
    """
    session.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
    session.execute_ddl(
        f"CREATE TABLE {TABLE} ("
        " id BIGINT NOT NULL PRIMARY KEY,"
        " label VARCHAR(64) NOT NULL DEFAULT 'etichetta-default',"
        f" UNIQUE KEY {TABLE}_label_uk (label)"
        ") ENGINE=InnoDB"
    )
    session.execute(f"INSERT INTO {TABLE} (id, label) VALUES (1, 'prima'), (2, 'seconda')")


def _rows(session):
    return session.execute_returning_rows(f"SELECT id, label FROM {TABLE} ORDER BY id")


def _index_names(session):
    rows = session.execute_returning_rows(
        "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{TABLE}' "
        "ORDER BY INDEX_NAME"
    )
    return sorted(row["INDEX_NAME"] for row in rows)


@pytest.fixture(name="session")
def _session():
    session = _connect()
    _reset(session)
    yield session
    session.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
    session.close()


# ---------------- sync ----------------


def test_copy_from_replace_swaps_rows_and_keeps_the_indexes(session):
    """`replace` sostituisce le righe senza ricreare la tabella."""
    indexes_before = _index_names(session)
    assert f"{TABLE}_label_uk" in indexes_before

    outcome = session.copy_from(
        schema=_database(),
        table=TABLE,
        source=_table([10, 11], ["nuova-a", "nuova-b"]),
        mode="replace",
    )

    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 2
    assert [(row["id"], row["label"]) for row in _rows(session)] == [
        (10, "nuova-a"),
        (11, "nuova-b"),
    ]
    assert _index_names(session) == indexes_before


def test_copy_from_truncate_insert_raises_a_typed_unsupported_error(session):
    """`truncate_insert` solleva l'eccezione tipizzata e non tocca il target."""
    before = [(row["id"], row["label"]) for row in _rows(session)]

    with pytest.raises(p.PlenoraUnsupportedError) as excinfo:
        session.copy_from(
            schema=_database(),
            table=TABLE,
            source=_table([10], ["nuova-a"]),
            mode="truncate_insert",
        )

    # Il messaggio deve indicare l'alternativa qualificata, non lasciare il
    # consumer a indovinare.
    assert "Replace" in str(excinfo.value)
    assert [(row["id"], row["label"]) for row in _rows(session)] == before


def test_copy_from_replace_on_a_missing_target_raises_not_found(session):
    session.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")

    with pytest.raises(p.PlenoraNotFoundError):
        session.copy_from(
            schema=_database(),
            table=TABLE,
            source=_table([10], ["nuova-a"]),
            mode="replace",
        )


# ---------------- async ----------------


@pytest_asyncio.fixture(name="async_session")
async def _async_session():
    host, database, user, password, ca_pem = _config_or_skip()
    setup = p.connect_mysql(host, database, user, password, tls_ca_pem=ca_pem)
    _reset(setup)
    session = await p.aconnect_mysql(host, database, user, password, tls_ca_pem=ca_pem)
    yield session
    session.close()
    setup.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
    setup.close()


@pytest.mark.asyncio
async def test_acopy_from_replace_swaps_rows(async_session):
    outcome = await async_session.acopy_from(
        schema=_database(),
        table=TABLE,
        source=_table([20, 21], ["async-a", "async-b"]),
        mode="replace",
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 2


@pytest.mark.asyncio
async def test_acopy_from_truncate_insert_raises_a_typed_unsupported_error(async_session):
    with pytest.raises(p.PlenoraUnsupportedError):
        await async_session.acopy_from(
            schema=_database(),
            table=TABLE,
            source=_table([20], ["async-a"]),
            mode="truncate_insert",
        )
