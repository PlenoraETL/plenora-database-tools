"""B-13 release campaign: deterministic server barrier, no client sleeps."""
import asyncio
import threading
import time

import pytest
import plenora_database as p
from ._harness import connect_postgres, aconnect_postgres, postgres_dsn_or_skip

CONDITIONS = ("server_error", "disconnect", "cancel", "deadline", "interrupt")
TIMEOUT = 90
LOCK = 731100013


def install(admin):
    admin.execute_sql("""CREATE OR REPLACE FUNCTION public._b13_gate(i integer) RETURNS integer
        LANGUAGE plpgsql VOLATILE AS $$ BEGIN
        IF i = 100000 THEN
            PERFORM set_config('lock_timeout', '0', true);
            PERFORM pg_advisory_lock(731100013);
            PERFORM pg_advisory_unlock(731100013);
            RAISE EXCEPTION USING ERRCODE = '22012', MESSAGE = 'controlled stream failure';
        END IF;
        RETURN i; END $$""")
    admin.execute_sql("CREATE OR REPLACE VIEW public._b13_stream AS SELECT public._b13_gate(gs) AS id FROM generate_series(1, 200000) gs")


def blocked_pid(admin):
    until = time.monotonic() + TIMEOUT
    while time.monotonic() < until:
        pid = admin.execute_scalar("SELECT pid FROM pg_stat_activity WHERE query LIKE '%_b13_stream%' AND pid <> pg_backend_pid() AND wait_event = 'advisory' LIMIT 1")
        if pid is not None:
            return pid
    raise AssertionError("server barrier not reached")


def trigger(admin, locker, pid, condition):
    if condition == "server_error":
        locker.rollback()
    elif condition == "disconnect":
        admin.execute_scalar("SELECT pg_terminate_backend($1)", [pid])
    elif condition == "cancel":
        admin.execute_scalar("SELECT pg_cancel_backend($1)", [pid])
    # The resource deadline and PostgreSQL statement timeout both belong to the provider.
    # Their original envelopes differ and must survive unchanged; no DSN override.


def verify(source, error, condition):
    assert isinstance(error, p.PlenoraError)
    expected = {
        "server_error": {("execution", "rolled_back", "never")},
        "disconnect": {("transient", "unknown", "quarantine"), ("io", "none", "quarantine")},
        "cancel": {("cancelled", "rolled_back", "never")},
        "deadline": {("timeout", "none", "never"), ("cancelled", "rolled_back", "never")},
        "interrupt": {("cancelled", "none", "never")},
    }
    assert error.phase == "read"
    assert (error.category, error.remote_effect, error.retry["kind"]) in expected[condition]
    saved = source.stream_state
    fields = ("category", "phase", "remote_effect", "retry", "provider", "execution_id", "details", "read_loss")
    assert {key: getattr(saved["error"], key) for key in fields} == {key: getattr(error, key) for key in fields}
    source.close()
    assert source.stream_state["status"] == saved["status"]
    assert source.stream_state["error"].retry == error.retry


def cleanup(admin):
    admin.execute_sql("DROP VIEW IF EXISTS public._b13_stream")
    admin.execute_sql("DROP FUNCTION IF EXISTS public._b13_gate(integer)")


@pytest.mark.parametrize("condition", CONDITIONS)
def test_live_b13_sync_midstream(condition):
    with connect_postgres() as admin, connect_postgres() as session, connect_postgres() as lock_session:
        install(admin)
        locker = lock_session.begin()
        locker.execute_scalar("SELECT pg_advisory_xact_lock($1) IS NULL", [LOCK])
        source = None
        consumer = None
        outcome = []
        try:
            source = session.read("public", "_b13_stream")
            adapted = p.ArrowStreamReader(source)
            assert next(adapted).num_rows > 0

            def consume():
                try:
                    adapted.read_all()
                except p.PlenoraError as error:
                    outcome.append(error)

            consumer = threading.Thread(target=consume, daemon=True)
            consumer.start()
            pid = blocked_pid(admin)
            if condition == "interrupt":
                source.close()
            else:
                trigger(admin, locker, pid, condition)
            consumer.join(TIMEOUT)
            assert not consumer.is_alive(), "reader did not release"
            assert len(outcome) == 1
            verify(source, outcome[0], condition)
            assert admin.execute_scalar("SELECT 1") == 1
        finally:
            if source is not None:
                source.close()
            if locker.is_active:
                locker.rollback()
            if consumer is not None:
                consumer.join(TIMEOUT)
            cleanup(admin)


@pytest.mark.parametrize("condition", CONDITIONS)
@pytest.mark.asyncio
async def test_live_b13_async_midstream(condition):
    # All synchronous handles, including their final references, belong to one thread.
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import ExitStack
    from functools import partial
    worker = ThreadPoolExecutor(max_workers=1)
    owned = {}

    async def blocking(function, *args):
        return await asyncio.get_running_loop().run_in_executor(worker, partial(function, *args))

    def open_owner():
        stack = owned["stack"] = ExitStack()
        admin = owned["admin"] = stack.enter_context(connect_postgres())
        lock_session = stack.enter_context(connect_postgres())
        install(admin)
        stack.callback(cleanup, admin)
        locker = owned["locker"] = lock_session.begin()
        stack.callback(lambda: locker.rollback() if locker.is_active else None)
        locker.execute_scalar("SELECT pg_advisory_xact_lock($1) IS NULL", [LOCK])

    def close_owner():
        try:
            if "stack" in owned:
                owned["stack"].close()
        finally:
            owned.clear()

    source = task = session = None
    try:
        await blocking(open_owner)
        session = await aconnect_postgres()
        source = await session.aread("public", "_b13_stream")
        adapted = p.AsyncArrowStreamReader(source)
        assert (await adapted.__anext__()).num_rows > 0
        task = asyncio.create_task(adapted.read_all())
        pid = await blocking(lambda: blocked_pid(owned["admin"]))
        if condition == "interrupt":
            task.cancel()
        else:
            await blocking(lambda: trigger(owned["admin"], owned["locker"], pid, condition))
        with pytest.raises(p.PlenoraError) as raised:
            await asyncio.wait_for(task, TIMEOUT)
        verify(source, raised.value, condition)
        await source.aclose()
        assert await blocking(lambda: owned["admin"].execute_scalar("SELECT 1")) == 1
    finally:
        try:
            if source is not None:
                await source.aclose()
        finally:
            try:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                if session is not None:
                    await session.aclose()
            finally:
                try:
                    await blocking(close_owner)
                finally:
                    await asyncio.to_thread(worker.shutdown, wait=True)
