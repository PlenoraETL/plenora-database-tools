"""Prove live della superficie MySQL dichiarata dal binding Python.

Coprono transazioni, session context, Arrow, bulk write e builder portabili
sui percorsi sync e async.

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
    session.execute_sql(
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
        tx.execute_sql(f"UPDATE {TABLE} SET amount = 99 WHERE id = 1")
    assert session.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 99

    with pytest.raises(RuntimeError, match="rollback voluto"):
        with session.begin() as tx:
            tx.execute_sql(f"UPDATE {TABLE} SET amount = 1 WHERE id = 1")
            raise RuntimeError("rollback voluto")
    assert session.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 99


def test_begin_accepts_an_isolation_level(session) -> None:
    with session.begin(isolation="repeatable_read") as tx:
        assert tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 2


def test_begin_carries_a_session_context(session) -> None:
    """I valori del context si rileggono **dal server**, non dall'oggetto.

    Verificare che `begin(context=...)` non sollevi non prova nulla: il
    context potrebbe non essere mai stato scritto. I valori si rileggono
    dalle variabili utente che il provider imposta, con i tipi che il core
    dichiara — testo e intero, il secondo serializzato come stringa.
    """

    # I nomi sono qualificati (`namespace.name`): senza namespace due
    # componenti diversi scriverebbero la stessa chiave senza accorgersene.
    context = p.SessionContext()
    context.insert_public("app.tenant", "acme")
    context.insert_internal("app.request", 42)
    context.insert_sensitive("app.operator", "marco")
    assert context.classification("app.operator") == "sensitive"

    with session.begin(context=context) as tx:
        assert tx.execute_scalar("SELECT @`plenora_ctx_app.tenant`") == "acme"
        assert tx.execute_scalar("SELECT @`plenora_ctx_app.request`") == "42"
        # Anche il valore classificato `sensitive` raggiunge il server: la
        # classificazione governa il logging, non la trasmissione.
        assert tx.execute_scalar("SELECT @`plenora_ctx_app.operator`") == "marco"


def test_a_context_key_too_long_for_mysql_is_refused(session) -> None:
    """52 caratteri e il massimo: il prefisso occupa il resto dei 64."""

    # 52 passa.
    accepted = p.SessionContext()
    accepted.insert_public("ns." + "a" * 49, "ok")
    with session.begin(context=accepted) as tx:
        assert tx.execute_scalar("SELECT 1") == 1

    # 53 no, e il rifiuto arriva prima di aprire la transazione.
    refused = p.SessionContext()
    refused.insert_public("ns." + "a" * 50, "ko")
    with pytest.raises(p.PlenoraError) as raised:
        session.begin(context=refused)
    assert "52" in str(raised.value), str(raised.value)


def test_native_query_policy_deny_refuses_a_forbidden_statement(session) -> None:
    """`deny` lascia passare l'OLTP e blocca il resto.

    Accettare il parametro non e la capability: la capability e che uno
    statement fuori dall'allowlist venga **rifiutato**. Con la sola prova
    che `SELECT` funziona, una policy ignorata sarebbe indistinguibile da
    una applicata.
    """

    with session.begin(native_query_policy="deny") as tx:
        assert tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 2

        # DDL: fuori dall'allowlist OLTP.
        with pytest.raises(p.PlenoraError) as raised:
            tx.execute_sql(f"CREATE TABLE {TABLE}_vietata (id BIGINT)")
        assert "deny" in str(raised.value).lower(), str(raised.value)

        # Multi-statement: rifiutato anche se ogni pezzo sarebbe ammesso.
        with pytest.raises(p.PlenoraError):
            tx.execute_sql(f"SELECT 1; SELECT 2 FROM {TABLE}")

    # Con la policy di default lo stesso DDL passa: e la policy a fare la
    # differenza, non lo statement.
    with session.begin() as tx:
        tx.execute_sql(f"CREATE TABLE {TABLE}_ammessa (id BIGINT)")
    session.execute_ddl(f"DROP TABLE IF EXISTS {TABLE}_ammessa")


def test_repeatable_read_does_not_see_a_concurrent_commit(session) -> None:
    """L'isolamento ha un effetto osservabile, non solo un parametro accettato.

    In `repeatable_read` la seconda lettura nella stessa transazione deve
    restituire il valore della prima, anche se un'altra connessione ha nel
    frattempo committato. Con `read_committed` cambierebbe: e la differenza
    che rende il test una prova.
    """

    other = connect_mysql_reference()
    try:
        with session.begin(isolation="repeatable_read") as tx:
            first = tx.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1")
            assert first == 10

            other.execute_sql(f"UPDATE {TABLE} SET amount = 777 WHERE id = 1")
            assert (
                other.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 777
            ), "l'altra connessione non ha committato: il test non proverebbe nulla"

            assert (
                tx.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 10
            ), "repeatable_read ha visto un commit concorrente"

        # Chiusa la transazione, il nuovo valore e visibile.
        assert session.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 777
    finally:
        other.close()


def test_savepoint_rolls_back_only_what_follows_it(session) -> None:
    """Savepoint con effetti verificabili riga per riga."""

    with session.begin() as tx:
        tx.execute_sql(f"INSERT INTO {TABLE} (id, label, amount) VALUES (10, 'dieci', 100)")
        tx.savepoint("sp1")
        tx.execute_sql(f"INSERT INTO {TABLE} (id, label, amount) VALUES (11, 'undici', 110)")
        assert tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE} WHERE id >= 10") == 2
        tx.rollback_to_savepoint("sp1")
        assert tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE} WHERE id >= 10") == 1
        tx.release_savepoint("sp1")

    # Dopo il commit sopravvive solo cio che precedeva il savepoint.
    rows = session.query_sql(
        f"SELECT id FROM {TABLE} WHERE id >= 10 ORDER BY id"
    )
    assert [row["id"] for row in rows] == [10]


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
        _schema(),
        TABLE,
        _rows([3, 4], ["tre", "quattro"], [30, 40]),
        mode="append",
        mapping_policy="strict",
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 2
    assert session.execute_scalar(f"SELECT COUNT(*) FROM {TABLE}") == 4


# =============================== builder AST =================================


def test_ast_builders_select_insert_update_delete(session) -> None:
    rows = session.select(TABLE).columns("label").where_eq("id", 1).all()
    assert [row.as_dict() for row in rows] == [{"label": "uno"}]

    assert (
        session.insert(TABLE)
        .values(id=5, label="cinque", amount=50)
        .execute()
        .affected_rows
        == 1
    )
    assert (
        session.update(TABLE)
        .set(amount=51)
        .where_eq("id", 5)
        .execute()
        .affected_rows
        == 1
    )
    assert (
        session.select(TABLE).columns("amount").where_eq("id", 5).one().as_dict()
        == {"amount": 51}
    )
    assert session.delete(TABLE).where_eq("id", 5).execute().affected_rows == 1


# ================================== async ====================================


@pytest.mark.asyncio
async def test_async_begin_with_context_and_policy(async_session) -> None:
    """Sul percorso async valgono le stesse prove, non le stesse promesse."""

    assert async_session.provider_capabilities["provider"] == "mysql"
    assert isinstance(await async_session.inspect.catalogs(), list)
    assert isinstance(await async_session.inspect.schemas(), list)

    context = p.SessionContext()
    context.insert_public("app.tenant", "acme")
    context.insert_internal("app.request", 42)
    async with await async_session.begin(
        isolation="repeatable_read", context=context, native_query_policy="deny"
    ) as tx:
        assert await tx.execute_scalar("SELECT @`plenora_ctx_app.tenant`") == "acme"
        assert await tx.execute_scalar("SELECT @`plenora_ctx_app.request`") == "42"

        with pytest.raises(p.PlenoraError) as raised:
            await tx.execute_sql(f"CREATE TABLE {TABLE}_vietata_async (id BIGINT)")
        assert "deny" in str(raised.value).lower(), str(raised.value)


@pytest.mark.asyncio
async def test_async_repeatable_read_does_not_see_a_concurrent_commit(
    async_session,
) -> None:
    """L'isolamento ha lo stesso effetto osservabile sul percorso async.

    Analogo del test sync: la seconda lettura nella stessa transazione deve
    restituire il valore della prima anche dopo un commit concorrente. Senza
    questo, del percorso async si sapeva solo che accetta il parametro.
    """

    other = connect_mysql_reference()
    try:
        async with await async_session.begin(isolation="repeatable_read") as tx:
            first = await tx.execute_scalar(
                f"SELECT amount FROM {TABLE} WHERE id = 1"
            )
            assert first == 10

            other.execute_sql(f"UPDATE {TABLE} SET amount = 888 WHERE id = 1")
            assert (
                other.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1") == 888
            ), "l'altra connessione non ha committato: il test non proverebbe nulla"

            assert (
                await tx.execute_scalar(f"SELECT amount FROM {TABLE} WHERE id = 1")
                == 10
            ), "repeatable_read async ha visto un commit concorrente"

        assert (
            await async_session.execute_scalar(
                f"SELECT amount FROM {TABLE} WHERE id = 1"
            )
            == 888
        )
    finally:
        other.close()


@pytest.mark.asyncio
async def test_async_savepoint_rolls_back_only_what_follows_it(async_session) -> None:
    async with await async_session.begin() as tx:
        await tx.execute_sql(
            f"INSERT INTO {TABLE} (id, label, amount) VALUES (20, 'venti', 200)"
        )
        await tx.savepoint("sp_async")
        await tx.execute_sql(
            f"INSERT INTO {TABLE} (id, label, amount) VALUES (21, 'ventuno', 210)"
        )
        await tx.rollback_to_savepoint("sp_async")
        assert await tx.execute_scalar(f"SELECT COUNT(*) FROM {TABLE} WHERE id >= 20") == 1

    rows = await async_session.query_sql(
        f"SELECT id FROM {TABLE} WHERE id >= 20 ORDER BY id"
    )
    assert [row["id"] for row in rows] == [20]


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
        _schema(),
        TABLE,
        _rows([6], ["sei"], [60]),
        mode="append",
        mapping_policy="strict",
    )
    assert outcome["status"] == "committed"
    assert outcome["rows"]["confirmed"] == 1


@pytest.mark.asyncio
async def test_async_ast_builders(async_session) -> None:
    rows = await async_session.select(TABLE).columns("label").where_eq("id", 2).all()
    assert [row.as_dict() for row in rows] == [{"label": "due"}]
