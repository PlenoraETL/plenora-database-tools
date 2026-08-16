"""Le capability MySQL che la documentazione dichiara, provate live.

I documenti affermano che il SDK MySQL ha la stessa superficie di Postgres
meno spatial: `begin` con savepoint e `SessionContext`, `read` streaming
Arrow IPC, `copy_from` bulk e i builder AST portabili, sia sync sia async.
Finche nessun test le esercitava, quelle righe erano una promessa — e per una
tranche intera hanno descritto uno scaffold che non le aveva.

Il contratto Replace/TruncateInsert di `copy_from` sta in
`test_mysql_copy_from.py`; qui si verifica che la capability **esista** su
entrambi i percorsi, non come si comporta ai bordi.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

import plenora_database as p

from ._harness import aconnect_mysql_reference, connect_mysql_reference

pyarrow = pytest.importorskip("pyarrow")

TABLE = "_sdk_capabilities"


def _rows(ids, labels, amounts):
    """Tabella Arrow con nullability dichiarata.

    `pyarrow.table` marca ogni colonna nullable, mentre il target e NOT NULL:
    il provider rifiuta la scrittura prima di toccare i dati, e ha ragione.
    """

    schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64(), nullable=False),
            pyarrow.field("label", pyarrow.string(), nullable=False),
            pyarrow.field("amount", pyarrow.int64(), nullable=False),
        ]
    )
    return pyarrow.table(
        {
            "id": pyarrow.array(ids, pyarrow.int64()),
            "label": pyarrow.array(labels, pyarrow.string()),
            "amount": pyarrow.array(amounts, pyarrow.int64()),
        },
        schema=schema,
    )


def _create(session) -> None:
    session.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
    session.execute_ddl(
        f"CREATE TABLE {TABLE} ("
        " id BIGINT PRIMARY KEY,"
        " label VARCHAR(64) NOT NULL,"
        " amount BIGINT NOT NULL) ENGINE=InnoDB"
    )
    session.execute(
        f"INSERT INTO {TABLE} (id, label, amount) VALUES (1, ?, 10), (2, ?, 20)",
        ["uno", "due"],
    )


@pytest.fixture(name="session")
def _session():
    session = connect_mysql_reference()
    _create(session)
    try:
        yield session
    finally:
        session.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
        session.close()


@pytest_asyncio.fixture(name="async_session")
async def _async_session():
    setup = connect_mysql_reference()
    _create(setup)
    session = await aconnect_mysql_reference()
    try:
        yield session
    finally:
        session.close()
        setup.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
        setup.close()


# ============================== OLTP + savepoint =============================


def test_begin_commits_and_rolls_back(session) -> None:
    with session.begin() as tx:
        tx.execute(f"UPDATE {TABLE} SET amount = 99 WHERE id = 1")
    assert session.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 99

    with pytest.raises(RuntimeError, match="rollback voluto"):
        with session.begin() as tx:
            tx.execute(f"UPDATE {TABLE} SET amount = 1 WHERE id = 1")
            raise RuntimeError("rollback voluto")
    assert session.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 99


def test_begin_accepts_an_isolation_level(session) -> None:
    with session.begin(isolation="repeatable_read") as tx:
        assert tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 2


def test_begin_carries_a_session_context(session) -> None:
    """`SessionContext` viaggia con la transazione, con le sue classificazioni."""

    # I nomi sono qualificati (`namespace.name`): senza namespace due
    # componenti diversi scriverebbero la stessa chiave senza accorgersene.
    context = p.SessionContext()
    context.insert_public("app.tenant", "acme")
    context.insert_internal("app.request", 42)
    context.insert_sensitive("app.operator", "marco")
    assert context.classification("app.operator") == "sensitive"

    with session.begin(context=context) as tx:
        assert tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 2


def test_native_query_policy_deny_is_accepted(session) -> None:
    """La policy e un parametro del binding MySQL, non solo di Postgres."""

    with session.begin(native_query_policy="deny") as tx:
        assert tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 2


# ================================ read Arrow =================================


def test_read_streams_arrow_ipc(session) -> None:
    import io as _io

    import pyarrow.ipc as ipc

    # `limit` senza `order_by` non ha un risultato definito, e il provider lo
    # rifiuta invece di restituire righe arbitrarie.
    rows = 0
    for chunk in session.read(
        _schema(),
        TABLE,
        projection=["id", "label"],
        order_by=[("id", "asc")],
        limit=10,
    ):
        batch = ipc.open_stream(_io.BytesIO(chunk)).read_all()
        assert batch.schema.names == ["id", "label"]
        rows += batch.num_rows
    assert rows == 2


def _schema() -> str:
    from ._harness import mysql_config_or_skip

    return mysql_config_or_skip()[1]


# ================================= copy_from =================================


def test_copy_from_appends_rows(session) -> None:
    outcome = session.copy_from(
        _schema(), TABLE, _rows([3, 4], ["tre", "quattro"], [30, 40]), mode="append"
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 2
    assert session.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 4


# =============================== builder AST =================================


def test_ast_builders_select_insert_update_delete(session) -> None:
    rows = session.select(TABLE).columns("label").where_eq("id", 1).all()
    assert rows == [{"label": "uno"}]

    assert session.insert(TABLE).values(id=5, label="cinque", amount=50).execute() == 1
    assert session.update(TABLE).set(amount=51).where_eq("id", 5).execute() == 1
    assert session.select(TABLE).columns("amount").where_eq("id", 5).one() == {
        "amount": 51
    }
    assert session.delete(TABLE).where_eq("id", 5).execute() == 1


# ================================== async ====================================


@pytest.mark.asyncio
async def test_async_begin_with_context_and_policy(async_session) -> None:
    context = p.SessionContext()
    context.insert_public("app.tenant", "acme")
    async with await async_session.begin(
        isolation="repeatable_read", context=context, native_query_policy="deny"
    ) as tx:
        assert await tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 2


@pytest.mark.asyncio
async def test_aread_streams_arrow_ipc(async_session) -> None:
    import io as _io

    import pyarrow.ipc as ipc

    reader = await async_session.aread(
        _schema(), TABLE, order_by=[("id", "asc")], limit=10
    )
    rows = 0
    async for chunk in reader:
        rows += ipc.open_stream(_io.BytesIO(chunk)).read_all().num_rows
    assert rows == 2


@pytest.mark.asyncio
async def test_acopy_from_appends_rows(async_session) -> None:
    outcome = await async_session.acopy_from(
        _schema(), TABLE, _rows([6], ["sei"], [60]), mode="append"
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 1


@pytest.mark.asyncio
async def test_async_ast_builders(async_session) -> None:
    rows = await async_session.select(TABLE).columns("label").where_eq("id", 2).all()
    assert rows == [{"label": "due"}]
