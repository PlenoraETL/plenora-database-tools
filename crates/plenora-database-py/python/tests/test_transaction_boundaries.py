import asyncio
import hashlib
import plenora_database as p
import pytest


def migration(revision, parent=None, down=None):
    return p.Migration(
        revision,
        parent,
        lambda tx: None,
        down,
        hashlib.sha256(revision.encode()).hexdigest(),
    )


def row(m):
    return {"revision": m.revision, "checksum": m.checksum, "state": "applied"}


class Tx:
    def __init__(self, session):
        self.session = session
        self.staged = [dict(item) for item in session.rows]
        self.committed = False

    def query_sql(self, sql):
        return [] if "= '__plenora_lock__'" in sql else self.staged

    def execute(self, statement, params):
        if isinstance(statement, p.InsertStatement):
            self.staged.append(
                {
                    "revision": params["orm_revision"],
                    "checksum": params["orm_checksum"],
                    "state": params["orm_state"],
                }
            )
        elif isinstance(statement, p.DeleteStatement):
            self.staged = [
                r for r in self.staged if r["revision"] != params["orm_revision"]
            ]
        else:
            for item in self.staged:
                if item["revision"] == params["orm_revision"]:
                    item["state"] = params["orm_state"]

    def commit(self):
        self.session.rows = self.staged
        self.committed = True
        if self.session.lose_ack:
            self.session.lose_ack = False
            raise ConnectionError("commit acknowledgement lost")

    def rollback(self):
        pass


class Session:
    provider_capabilities = {"provider": "postgres"}

    def __init__(self, rows=(), race=(), lose_ack=False):
        self.rows = list(rows)
        self.race = list(race)
        self.lose_ack = lose_ack

    def execute_ddl(self, sql):
        pass

    def execute_sql(self, sql):
        pass

    def query_sql(self, sql):
        return list(self.rows)

    def begin(self):
        self.rows.extend(self.race)
        self.race = []
        return Tx(self)


class ATx:
    def __init__(self, tx):
        self.tx = tx

    async def query_sql(self, sql):
        return self.tx.query_sql(sql)

    async def execute(self, stmt, params):
        return self.tx.execute(stmt, params)

    async def commit(self):
        return self.tx.commit()

    async def rollback(self):
        return self.tx.rollback()


class ASession(Session):
    async def execute_ddl(self, sql):
        pass

    async def execute_sql(self, sql):
        pass

    async def query_sql(self, sql):
        return Session.query_sql(self, sql)

    async def begin(self):
        return ATx(Session.begin(self))


@pytest.mark.parametrize("asynchronous", [False, True])
def test_migration_rollback_must_recheck_children_under_lock(asynchronous):
    downgraded = []
    a = migration("a", down=lambda tx: downgraded.append("a"))
    b = migration("b", "a", lambda tx: downgraded.append("b"))
    cls = ASession if asynchronous else Session
    session = cls([row(a)], [row(b)])
    runner = (p.AsyncMigrationRunner if asynchronous else p.MigrationRunner)((a, b))
    with pytest.raises(p.OrmStateError):
        if asynchronous:
            asyncio.run(runner.rollback(session))
        else:
            runner.rollback(session)
    assert downgraded == []


@pytest.mark.parametrize("asynchronous", [False, True])
def test_lost_commit_ack_must_not_overwrite_applied_migration(asynchronous):
    a = migration("a")
    session = (ASession if asynchronous else Session)(lose_ack=True)
    runner = (p.AsyncMigrationRunner if asynchronous else p.MigrationRunner)((a,))
    with pytest.raises(ConnectionError):
        if asynchronous:
            asyncio.run(runner.apply(session))
        else:
            runner.apply(session)
    assert session.rows == [row(a)]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_rollback_error_must_survive_listener_failure(asynchronous):
    original = ConnectionError("rollback acknowledgement lost")

    class Transaction:
        def rollback(self):
            raise original

    class Core:
        provider_capabilities = {"provider": "postgres"}

        def begin(self):
            return Transaction()

    def listener(session):
        raise ValueError("listener failed")

    if asynchronous:

        class ATransaction:
            async def rollback(self):
                raise original

        class ACore(Core):
            async def begin(self, **options):
                return ATransaction()

        async def run():
            orm = p.AsyncOrmSession(ACore())
            await orm._ensure_started()
            orm.listen("after_rollback", listener)
            with pytest.raises(ConnectionError) as caught:
                await orm.rollback()
            assert caught.value is original

        asyncio.run(run())
    else:
        orm = p.OrmSession(Core())
        orm.listen("after_rollback", listener)
        with pytest.raises(ConnectionError) as caught:
            orm.rollback()
        assert caught.value is original


def test_reentrant_commit_must_not_discard_pending_insert():
    class Item(p.DeclarativeBase):
        __registry__ = p.Registry()
        __tablename__ = "deep_review_items"
        id: p.Mapped[int] = p.mapped_column(primary_key=True)

    class Transaction:
        committed = False

        def commit(self):
            self.committed = True

        def rollback(self):
            pass

    transaction = Transaction()

    class Core:
        provider_capabilities = {"provider": "postgres"}

        def begin(self):
            return transaction

    orm = p.OrmSession(Core())
    orm.add(Item(id=1))
    orm.listen("before_flush", lambda session: session.commit())
    with pytest.raises(p.OrmStateError):
        orm.flush()
    assert not transaction.committed


@pytest.mark.parametrize("asynchronous", [False, True])
def test_apply_rechecks_parent_after_previous_iteration(asynchronous):
    upgraded = []
    a = migration("a")
    b = p.Migration(
        "b",
        "a",
        lambda tx: upgraded.append("b"),
        None,
        hashlib.sha256(b"b").hexdigest(),
    )

    class RacingSession(Session):
        starts = 0

        def begin(self):
            self.starts += 1
            if self.starts == 2:
                self.rows = []
            return super().begin()

    class AsyncRacingSession(RacingSession):
        async def execute_ddl(self, sql):
            pass

        async def execute_sql(self, sql):
            pass

        async def query_sql(self, sql):
            return Session.query_sql(self, sql)

        async def begin(self):
            return ATx(RacingSession.begin(self))

    session = (AsyncRacingSession if asynchronous else RacingSession)()
    runner = (p.AsyncMigrationRunner if asynchronous else p.MigrationRunner)((a, b))
    with pytest.raises(p.OrmStateError):
        if asynchronous:
            asyncio.run(runner.apply(session))
        else:
            runner.apply(session)
    assert upgraded == []
    assert session.rows == []


@pytest.mark.parametrize("asynchronous", [False, True])
def test_recover_cannot_mark_orphan_applied(asynchronous):
    a, b = migration("a"), migration("b", "a")
    failed = dict(row(b), state="failed")
    session = (ASession if asynchronous else Session)([failed])
    runner = (p.AsyncMigrationRunner if asynchronous else p.MigrationRunner)((a, b))
    with pytest.raises(p.OrmStateError):
        if asynchronous:
            asyncio.run(runner.recover(session, "b", assume_applied=True))
        else:
            runner.recover(session, "b", assume_applied=True)
    assert session.rows == [failed]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "operation",
    [
        "commit",
        "rollback",
        "close",
        "savepoint",
        "rollback_to_savepoint",
        "release_savepoint",
        "begin_nested",
    ],
)
def test_flush_callback_cannot_change_transaction_boundary(asynchronous, operation):
    class Item(p.DeclarativeBase):
        __registry__ = p.Registry()
        __tablename__ = "boundary_items"
        id: p.Mapped[int] = p.mapped_column(primary_key=True)

    calls = []

    class Transaction:
        def commit(self):
            calls.append("commit")

        def rollback(self):
            calls.append("rollback")

    class Core:
        provider_capabilities = {"provider": "postgres"}

        def begin(self):
            return Transaction()

    args = (
        ("s",)
        if operation
        in {"savepoint", "rollback_to_savepoint", "release_savepoint", "begin_nested"}
        else ()
    )
    if asynchronous:

        class ATransaction:
            async def commit(self):
                calls.append("commit")

            async def rollback(self):
                calls.append("rollback")

        class ACore(Core):
            async def begin(self, **options):
                return ATransaction()

        async def run():
            orm = p.AsyncOrmSession(ACore())
            orm.add(Item(id=1))

            async def listener(session):
                outcome = getattr(session, operation)(*args)
                if operation != "begin_nested":
                    await outcome

            orm.listen("before_flush", listener)
            with pytest.raises(p.OrmStateError):
                await orm.flush()
            assert not orm._active

        asyncio.run(run())
    else:
        orm = p.OrmSession(Core())
        orm.add(Item(id=1))
        orm.listen("before_flush", lambda session: getattr(session, operation)(*args))
        with pytest.raises(p.OrmStateError):
            orm.flush()
        assert not orm._active
    assert calls == ["rollback"]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("failure", ["rollback_to_savepoint", "release_savepoint"])
def test_nested_cleanup_preserves_body_exception_and_invalidates_session(
    asynchronous, failure
):
    original = ValueError("body failure")
    calls = []

    class Transaction:
        def savepoint(self, name):
            pass

        def rollback_to_savepoint(self, name):
            if failure == "rollback_to_savepoint":
                raise ConnectionError("cleanup")

        def release_savepoint(self, name):
            if failure == "release_savepoint":
                raise ConnectionError("cleanup")

        def rollback(self):
            calls.append("rollback")

    class Core:
        provider_capabilities = {"provider": "postgres"}

        def begin(self):
            return Transaction()

    if asynchronous:

        class ATransaction(Transaction):
            async def savepoint(self, name):
                return Transaction.savepoint(self, name)

            async def rollback_to_savepoint(self, name):
                return Transaction.rollback_to_savepoint(self, name)

            async def release_savepoint(self, name):
                return Transaction.release_savepoint(self, name)

            async def rollback(self):
                return Transaction.rollback(self)

        class ACore(Core):
            async def begin(self, **options):
                return ATransaction()

        async def run():
            orm = p.AsyncOrmSession(ACore())
            with pytest.raises(ValueError) as caught:
                async with orm.begin_nested("s"):
                    raise original
            assert caught.value is original
            assert not orm._active

        asyncio.run(run())
    else:
        orm = p.OrmSession(Core())
        with pytest.raises(ValueError) as caught:
            with orm.begin_nested("s"):
                raise original
        assert caught.value is original
        assert not orm._active
    assert calls == ["rollback"]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["commit", "rollback"])
def test_terminal_listener_error_survives_owned_session_close(asynchronous, operation):
    original = ValueError("listener failed")
    closed = []

    class Transaction:
        def commit(self):
            pass

        def rollback(self):
            pass

    class Core:
        provider_capabilities = {"provider": "postgres"}

        def begin(self):
            return Transaction()

        def close(self):
            closed.append(True)
            raise ConnectionError("close failed")

    def listener(session):
        raise original

    if asynchronous:

        class ATransaction:
            async def commit(self):
                pass

            async def rollback(self):
                pass

        class ACore(Core):
            async def begin(self, **options):
                return ATransaction()

        async def run():
            orm = p.AsyncOrmSession(ACore(), close_session=True)
            orm.listen("after_" + operation, listener)
            with pytest.raises(ValueError) as caught:
                await getattr(orm, operation)()
            assert caught.value is original
            assert not orm._active

        asyncio.run(run())
    else:
        orm = p.OrmSession(Core(), close_session=True)
        orm.listen("after_" + operation, listener)
        with pytest.raises(ValueError) as caught:
            getattr(orm, operation)()
        assert caught.value is original
        assert not orm._active
    assert closed == [True]
