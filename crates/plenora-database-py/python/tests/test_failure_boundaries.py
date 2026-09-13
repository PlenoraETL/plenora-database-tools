"""Regressioni offline dei confini pubblici e del cleanup ORM."""

import asyncio
import traceback

import plenora_database as p
import pytest

from plenora_database._session import Session


@pytest.mark.parametrize("provider", ["postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2"])
@pytest.mark.parametrize("slot", ["port", "pool", "host"])
def test_url_errors_redact_invalid_input(provider, slot):
    secret = "private" + "-url-value"
    urls = {
        "port": f"{provider}://user:password@localhost:{secret}/app",
        "pool": f"{provider}://user:password@localhost/app?max_connections={secret}",
        "host": f"{provider}://user:password@[{secret}]/app",
    }
    with pytest.raises(ValueError) as caught:
        p.EngineConfig.from_url(urls[slot])
    assert secret not in "".join(traceback.format_exception(caught.value))


def test_copy_from_redacts_cell_conversion_before_native_io():
    secret = "private" + "-cell-value"

    class Native:
        def copy_from(self, *args):
            raise AssertionError("invalid Arrow input reached native I/O")

    with pytest.raises(ValueError) as caught:
        Session(Native()).copy_from("app", "items", [{"v": 1}, {"v": secret}], mapping_policy="strict")
    assert secret not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("cancelled", [False, True])
def test_async_failed_begin_closes_orm_and_cannot_silently_skip_flush(cancelled):
    async def scenario():
        failure = asyncio.CancelledError() if cancelled else TimeoutError("begin failed")

        class Core:
            provider_capabilities = {"provider": "postgres"}
            closed = False

            async def begin(self, **options):
                raise failure

            def close(self):
                self.closed = True

        core = Core()
        orm = p.AsyncOrmSession(core, close_session=True)
        with pytest.raises(type(failure)) as caught:
            await orm.flush()
        assert caught.value is failure
        assert not orm.is_active
        assert core.closed
        with pytest.raises(p.OrmStateError):
            await orm.flush()

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["flush", "commit", "context"])
def test_sync_cleanup_preserves_original_failure_and_closes_owned_session(operation):
    original = RuntimeError("original operation failure")

    class Transaction:
        is_active = True

        def commit(self):
            raise original

        def rollback(self):
            raise RuntimeError("secondary rollback failure")

    class Core:
        provider_capabilities = {"provider": "postgres"}
        closed = False

        def begin(self):
            return Transaction()

        def close(self):
            self.closed = True

    core = Core()
    orm = p.OrmSession(core, close_session=True)
    if operation == "flush":
        def fail(*args):
            raise original
        orm._listeners["before_flush"] = [fail]
    with pytest.raises(RuntimeError) as caught:
        if operation == "context":
            with orm:
                raise original
        else:
            getattr(orm, operation)()
    assert caught.value is original
    assert not orm.is_active
    assert core.closed


@pytest.mark.parametrize("operation", ["flush", "commit", "context"])
def test_async_cleanup_preserves_original_failure_and_closes_owned_session(operation):
    async def scenario():
        original = RuntimeError("original operation failure")

        class Transaction:
            async def commit(self):
                raise original

            async def rollback(self):
                raise RuntimeError("secondary rollback failure")

        class Core:
            provider_capabilities = {"provider": "postgres"}
            closed = False

            async def begin(self, **options):
                return Transaction()

            def close(self):
                self.closed = True

        core = Core()
        orm = p.AsyncOrmSession(core, close_session=True)
        if operation == "flush":
            def fail(*args):
                raise original
            orm._listeners["before_flush"] = [fail]
        with pytest.raises(RuntimeError) as caught:
            if operation == "context":
                async with orm:
                    raise original
            else:
                await getattr(orm, operation)()
        assert caught.value is original
        assert not orm.is_active
        assert core.closed

    asyncio.run(scenario())
