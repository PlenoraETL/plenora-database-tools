"""Offline server doubles through real PyArrow C callbacks, never message parsing."""
import asyncio
import threading

import pytest
import pyarrow as pa
from plenora_database import _native
from plenora_database import ArrowStreamReader, AsyncArrowStreamReader
from plenora_database import _arrow_io as io
from plenora_database.errors import PlenoraError, _SdkAxes


class Failure(_SdkAxes, PlenoraError):
    _phase = "read"


def failure(category):
    error = Failure("lettura interrotta")
    error.category = category
    error.remote_effect = "unknown"
    error.retry = {"kind": "never"}
    error.read_loss = {"schema_version": 1, "status": "failed", "losses": []}
    error.details = {"read_loss": error.read_loss}
    return error


def ipc(batch):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


class Server:
    def __init__(self, category="execution", position=1):
        self.batch = pa.record_batch([pa.array([1])], names=["id"])
        self.chunk = ipc(self.batch)
        self.error = failure(category)
        self.position = position
        self.count = 0
        self.released = False
        self.stream_state = {"error": None, "status": "open", "read_loss": None}

    def fail(self, error):
        if self.stream_state["error"] is None:
            self.stream_state.update(error=error, status="failed", read_loss=error.read_loss)
        self.released = True
        return self.stream_state["error"]

    def _record_arrow_failure(self):
        return self.fail(failure("data_mapping"))

    def _record_arrow_cancellation(self):
        return self.fail(failure("cancelled"))

    def close(self):
        self.released = True
        if self.stream_state["status"] == "open":
            self.stream_state.update(error=failure("cancelled"), status="interrupted")

    async def aclose(self):
        self.close()

    def schema_bytes(self):
        if self.position == "schema":
            raise self.fail(self.error)
        return self.chunk

    def __iter__(self):
        return self

    def __next__(self):
        if self.count == self.position:
            raise self.fail(self.error)
        self.count += 1
        return self.chunk

    def __arrow_c_stream__(self, requested_schema=None):
        return io.c_stream_from_reader(self, requested_schema)


class AsyncServer(Server):
    async def schema_bytes(self):
        return super().schema_bytes()

    async def __anext__(self):
        return super().__next__()


@pytest.fixture
def native_types(monkeypatch):
    # Only the test module replaces the exact trusted native types.
    monkeypatch.setattr(_native, "BatchReader", Server)
    monkeypatch.setattr(_native, "AsyncBatchReader", AsyncServer)


CATEGORIES = {"server_error": "execution", "disconnect": "io", "cancel": "cancelled",
              "deadline": "timeout", "interrupt": "cancelled"}


CONDITIONS = ("server_error", "disconnect", "cancel", "deadline", "interrupt")


def assert_failure(source, error):
    assert error is source.stream_state["error"]
    assert (error.category, error.phase, error.remote_effect, error.retry) == (
        source.error.category, "read", "unknown", {"kind": "never"})
    source.close()
    assert source.released
    assert source.stream_state["error"] is error
    assert error.__cause__ is None
    assert error.details == {"read_loss": source.stream_state["read_loss"]}


@pytest.mark.parametrize("condition", CONDITIONS)
@pytest.mark.parametrize("position", ["schema", 0, 1, 2])
@pytest.mark.parametrize("boundary", ["capsule_export", "lazy_callback", "table_materialize",
                                    "capsule_import", "imported_reader", "write_source"])
def test_c_boundaries(native_types, condition, position, boundary):
    source = Server(CATEGORIES[condition], position)
    with pytest.raises(PlenoraError) as raised:
        if boundary == "table_materialize":
            io.table_from_reader(source)
        elif boundary == "write_source":
            io._to_ipc_bytes(source)
        elif boundary == "imported_reader":
            raw = pa.RecordBatchReader.from_stream(source)
            ArrowStreamReader(source, reader=raw).read_all()
        else:
            ArrowStreamReader(source).read_all()
    assert_failure(source, raised.value)


@pytest.mark.parametrize("condition", CONDITIONS)
@pytest.mark.parametrize("position", ["schema", 0, 1, 2])
@pytest.mark.parametrize("boundary", ["async_schema_decode", "async_batch_decode", "async_table"])
@pytest.mark.asyncio
async def test_async_boundaries(native_types, condition, position, boundary):
    source = AsyncServer(CATEGORIES[condition], position)
    adapter = AsyncArrowStreamReader(source)
    with pytest.raises(PlenoraError) as raised:
        if boundary == "async_table":
            await adapter.read_all()
        else:
            while True:
                await adapter.__anext__()
    assert_failure(source, raised.value)
    await adapter.aclose()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.asyncio
async def test_corrupt_ipc_records_local_failure_before_close(native_types, async_mode):
    source = AsyncServer(position=3) if async_mode else Server(position=3)
    source.chunk = b"PRIVATE_PAYLOAD"
    with pytest.raises(PlenoraError) as raised:
        if async_mode:
            await AsyncArrowStreamReader(source).read_all()
        else:
            ArrowStreamReader(source).read_all()
    assert raised.value.category == "data_mapping"
    assert source.stream_state["error"] is raised.value
    assert source.released
    assert "PRIVATE_PAYLOAD" not in str(raised.value)


@pytest.mark.asyncio
async def test_cancel_during_decoder_releases_source_without_blocking_loop(native_types, monkeypatch):
    source = AsyncServer(position=3)
    adapter = AsyncArrowStreamReader(source)
    await adapter._schema()
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = io._decode_chunk

    def decode(*args):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(10), "test worker was not released"
        return original(*args)

    monkeypatch.setattr(io, "_decode_chunk", decode)
    task = asyncio.create_task(adapter.__anext__())
    try:
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        with pytest.raises(PlenoraError) as raised:
            await task
        assert raised.value.category == "cancelled"
        assert source.released
        assert source.stream_state["error"] is raised.value
    finally:
        release.set()
        await adapter.aclose()


@pytest.mark.parametrize("release", ["capsule", "reader"])
def test_releasing_unstarted_capsule_closes_source(native_types, release):
    import gc
    source = Server(position=3)
    if release == "capsule":
        capsule = source.__arrow_c_stream__()
        del capsule
        gc.collect()
    else:
        imported = pa.RecordBatchReader.from_stream(source)
        imported.close()
    assert source.released
    assert source.stream_state["status"] == "interrupted"


def test_raw_c_failure_remains_available_without_parsing_arrow_message(native_types):
    source = Server("io", 1)
    raw = pa.RecordBatchReader.from_stream(source)
    assert raw.read_next_batch().num_rows == 1
    with pytest.raises(pa.ArrowException):
        raw.read_all()
    assert source.stream_state["error"].category == "io"
    with pytest.raises(PlenoraError) as raised:
        ArrowStreamReader(source, reader=raw).read_all()
    assert_failure(source, raised.value)


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.asyncio
async def test_lazy_batch_decode_records_failure_after_valid_schema(native_types, async_mode):
    source = AsyncServer(position=3) if async_mode else Server(position=3)
    if async_mode:
        adapter = AsyncArrowStreamReader(source)
        await adapter._schema()
    else:
        adapter = ArrowStreamReader(source)
    source.chunk = b"PRIVATE_PAYLOAD"
    with pytest.raises(PlenoraError) as raised:
        if async_mode:
            await adapter.__anext__()
        else:
            next(adapter)
    assert raised.value.category == "data_mapping"
    assert source.stream_state["error"] is raised.value
    assert source.released


def test_export_failure_precedes_owner_release(native_types):
    source = Server(position=3)
    with pytest.raises(PlenoraError) as raised:
        source.__arrow_c_stream__(object())
    assert raised.value.category == "data_mapping"
    assert source.stream_state["error"] is raised.value
    assert source.released


def test_construction_failure_precedes_owner_release(native_types, monkeypatch):
    from types import SimpleNamespace
    def fail(*args):
        raise ValueError("PRIVATE_PAYLOAD")
    fake = SimpleNamespace(ipc=pa.ipc, py_buffer=pa.py_buffer,
                           RecordBatchReader=SimpleNamespace(from_batches=fail))
    monkeypatch.setattr(io, "_require_pyarrow", lambda: fake)
    source = Server(position=3)
    with pytest.raises(PlenoraError) as raised:
        io._record_batch_reader(source)
    assert raised.value.category == "data_mapping"
    assert source.stream_state["error"] is raised.value
    assert source.released


@pytest.mark.parametrize("boundary", ["schema", "next"])
@pytest.mark.asyncio
async def test_cancel_native_await_releases_source(native_types, boundary):
    source = AsyncServer(position=3)
    adapter = AsyncArrowStreamReader(source)
    entered = asyncio.Event()
    async def pending():
        entered.set()
        await asyncio.Future()
    if boundary == "schema":
        source.schema_bytes = pending
    else:
        source.__anext__ = pending
    task = asyncio.create_task(adapter.__anext__())
    await asyncio.wait_for(entered.wait(), 10)
    task.cancel()
    with pytest.raises(PlenoraError) as raised:
        await task
    assert raised.value.category == "cancelled"
    assert source.released
    assert source.stream_state["error"] is raised.value


@pytest.mark.asyncio
async def test_cancel_final_materialization_after_native_eof_is_not_mapping_error(native_types):
    from plenora_database.arrow_stream import _await_source
    source = AsyncServer()
    source.stream_state["status"] = "complete"
    entered = asyncio.Event()
    async def pending():
        entered.set()
        await asyncio.Future()
    task = asyncio.create_task(_await_source(source, pending()))
    await asyncio.wait_for(entered.wait(), 10)
    task.cancel()
    with pytest.raises(PlenoraError) as raised:
        await task
    assert raised.value.category == "cancelled"
    assert source.stream_state["error"] is raised.value
    assert source.released


@pytest.mark.parametrize("surface", ["sync", "wrapped", "async"])
@pytest.mark.asyncio
async def test_keyboard_interrupt_after_eof_keeps_cancelled_category(native_types, surface):
    from plenora_database.arrow_stream import _consume, _await_source
    source = AsyncServer() if surface == "async" else Server()
    consumer = ArrowStreamReader(source) if surface == "wrapped" else source
    source.stream_state["status"] = "complete"
    def interrupted():
        raise KeyboardInterrupt()
    async def interrupted_async():
        interrupted()
    with pytest.raises(PlenoraError) as raised:
        if surface == "async":
            await _await_source(source, interrupted_async())
        else:
            _consume(consumer, interrupted)
    assert raised.value.category == "cancelled"
    assert source.stream_state["error"] is raised.value
    assert source.released
