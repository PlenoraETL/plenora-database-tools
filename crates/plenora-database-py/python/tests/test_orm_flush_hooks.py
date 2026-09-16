"""Gli hook non possono far perdere lavoro a un flush riuscito."""

import asyncio
import inspect

import plenora_database as p
import pytest

from ._harness import aconnect_postgres, connect_postgres
from .test_orm import (
    CompositeOwner,
    CompositeTag,
    LiveOrmRackMachine,
    _AsyncFakeSession,
    _AsyncFakeTransaction,
    _FakeSession,
    _FakeTransaction,
)


class HookItem(p.DeclarativeBase):
    __registry__ = p.Registry()
    __tablename__ = "_plenora_hook_items"
    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    name: p.Mapped[str] = p.mapped_column(nullable=False)
    audit: p.Mapped[str] = p.mapped_column(nullable=False)


async def call(operation, *args):
    result = operation(*args)
    return await result if inspect.isawaitable(result) else result


def session_pair(asynchronous, rows=None):
    tx = (_AsyncFakeTransaction if asynchronous else _FakeTransaction)(rows)
    orm = (
        p.AsyncOrmSession(_AsyncFakeSession(tx))
        if asynchronous
        else p.OrmSession(_FakeSession(tx))
    )
    return orm, tx


def add_once():
    fired = False

    def callback(session, *args):
        nonlocal fired
        if not fired:
            fired = True
            session.add(HookItem(id=2, name="second", audit="before"))

    return callback


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("event", [
    "before_insert", "after_insert", "before_update", "after_update",
    "before_delete", "after_delete", "after_flush",
])
def test_commit_drains_objects_added_by_hooks(asynchronous, event):
    async def run():
        orm, tx = session_pair(asynchronous, [{"id": 1, "name": "old", "audit": "before"}])
        if "update" in event or "delete" in event:
            item = await call(orm.get, HookItem, 1)
            if "update" in event:
                item.name = "new"
            else:
                orm.delete(item)
        else:
            orm.add(HookItem(id=1, name="first", audit="before"))
        callback = add_once()

        async def async_callback(session, *args):
            await asyncio.sleep(0)
            callback(session, *args)

        orm.listen(event, async_callback if asynchronous else callback)
        await call(orm.commit)
        inserted = [params for stmt, params in tx.executed if isinstance(stmt, p.InsertStatement)]
        assert any(params.get("orm_insert_0") == 2 for params in inserted)
        assert tx.committed and not tx.rolled_back

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("joined", [False, True])
def test_before_update_includes_newly_modified_columns(asynchronous, joined):
    async def run():
        model = LiveOrmRackMachine if joined else HookItem
        row = (
            {"id": 1, "kind": "rack-machine", "name": "old", "cores": 4, "rack_units": 1}
            if joined else {"id": 1, "name": "old", "audit": "before"}
        )
        orm, tx = session_pair(asynchronous, [row])
        item = await call(orm.get, model, 1)
        item.name = "new"

        def callback(session, instance):
            if joined:
                instance.cores = 8
                instance.rack_units = 2
            else:
                instance.audit = "after"

        orm.listen("before_update", callback)
        await call(orm.commit)
        values = [value for stmt, params in tx.executed if isinstance(stmt, p.UpdateStatement) for value in params.values()]
        assert "new" in values
        if joined:
            assert 8 in values and 2 in values
        else:
            assert "after" in values
        assert tx.committed

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("boundary", ["flush", "savepoint", "commit"])
def test_after_flush_changes_are_saved_before_boundary(asynchronous, boundary):
    async def run():
        orm, tx = session_pair(asynchronous)
        item = HookItem(id=1, name="first", audit="before")
        orm.add(item)
        orm.listen("after_flush", lambda session: setattr(item, "audit", "after"))
        await call(getattr(orm, boundary), *(["checkpoint"] if boundary == "savepoint" else []))
        updates = [params for stmt, params in tx.executed if isinstance(stmt, p.UpdateStatement)]
        assert len(updates) == 1 and "after" in updates[0].values()
        if boundary != "commit":
            await call(orm.rollback)
        assert tx.committed == (boundary == "commit")

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_after_flush_many_to_many_changes_are_saved(asynchronous):
    async def run():
        orm, tx = session_pair(asynchronous)
        owner = CompositeOwner(tenant_id=1, code="owner", tags=[])
        tag = CompositeTag(tenant_id=1, code="tag", owners=[])
        orm.add_all([owner, tag])
        fired = False

        def callback(session):
            nonlocal fired
            if not fired:
                fired = True
                owner.tags.append(tag)

        orm.listen("after_flush", callback)
        await call(orm.commit)
        inserts = [params for stmt, params in tx.executed if isinstance(stmt, p.InsertStatement)]
        assert len(inserts) == 3
        assert tx.committed

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_unbounded_hook_work_rolls_back(asynchronous):
    async def run():
        orm, tx = session_pair(asynchronous)
        item = HookItem(id=1, name="first", audit="before")
        orm.add(item)
        calls = 0

        def callback(session):
            nonlocal calls
            calls += 1
            item.audit = str(calls)

        orm.listen("after_flush", callback)
        with pytest.raises(p.OrmStateError, match="non stabilizzano il flush"):
            await call(orm.commit)
        assert tx.rolled_back and not tx.committed
        assert 1 < calls <= 100

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("event", ["before_insert", "after_insert", "after_flush"])
def test_live_postgres_hook_mutations_are_persisted(asynchronous, event):
    async def run():
        core = await aconnect_postgres() if asynchronous else connect_postgres()
        try:
            await call(core.execute_ddl, "CREATE TABLE _plenora_hook_items (id INT PRIMARY KEY, name TEXT NOT NULL, audit TEXT NOT NULL)")
            try:
                orm = p.AsyncOrmSession(core) if asynchronous else p.OrmSession(core)
                orm.add(HookItem(id=1, name="first", audit="before"))
                orm.listen(event, add_once())
                await call(orm.commit)
                assert await call(core.execute_scalar, "SELECT count(*) FROM _plenora_hook_items") == 2
                orm = p.AsyncOrmSession(core) if asynchronous else p.OrmSession(core)
                item = await call(orm.get, HookItem, 1)
                item.name = "new"
                orm.listen("before_update", lambda session, instance: setattr(instance, "audit", "after"))
                await call(orm.commit)
                assert await call(core.execute_scalar, "SELECT audit FROM _plenora_hook_items WHERE id=1") == item.audit == "after"
            finally:
                await call(core.execute_ddl, "DROP TABLE _plenora_hook_items")
        finally:
            await call(core.aclose if asynchronous else core.close)

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_live_postgres_before_update_changes_all_joined_fragments(asynchronous):
    async def run():
        core = await aconnect_postgres() if asynchronous else connect_postgres()
        tables = [
            ("_plenora_orm_assets", "id INT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL"),
            ("_plenora_orm_machines", "id INT PRIMARY KEY REFERENCES _plenora_orm_assets(id), cores INT NOT NULL"),
            ("_plenora_orm_rack_machines", "id INT PRIMARY KEY REFERENCES _plenora_orm_machines(id), rack_units INT NOT NULL"),
        ]
        created = []
        try:
            for table, columns in tables:
                await call(core.execute_ddl, f"CREATE TABLE {table} ({columns})")
                created.append(table)
            orm = p.AsyncOrmSession(core) if asynchronous else p.OrmSession(core)
            orm.add(LiveOrmRackMachine(id=1, name="old", cores=4, rack_units=1))
            await call(orm.commit)
            orm = p.AsyncOrmSession(core) if asynchronous else p.OrmSession(core)
            item = await call(orm.get, LiveOrmRackMachine, 1)
            item.name = "new"

            def callback(session, instance):
                instance.cores = 8
                instance.rack_units = 2

            orm.listen("before_update", callback)
            await call(orm.commit)
            assert await call(core.execute_scalar, "SELECT name FROM _plenora_orm_assets WHERE id=1") == "new"
            assert await call(core.execute_scalar, "SELECT cores FROM _plenora_orm_machines WHERE id=1") == 8
            assert await call(core.execute_scalar, "SELECT rack_units FROM _plenora_orm_rack_machines WHERE id=1") == 2
        finally:
            for table in reversed(created):
                await call(core.execute_ddl, f"DROP TABLE {table}")
            await call(core.aclose if asynchronous else core.close)

    asyncio.run(run())
