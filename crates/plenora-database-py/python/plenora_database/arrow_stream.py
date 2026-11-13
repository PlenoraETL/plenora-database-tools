"""Consumo C con accesso allo stato strutturato della sorgente SDK."""
from __future__ import annotations

import asyncio

from typing import Any


def _native_reader(source: Any) -> bool:
    from ._native import BatchReader, AsyncBatchReader
    return type(source) in (BatchReader, AsyncBatchReader)


def _sdk_source(source: Any) -> bool:
    return _native_reader(source) or type(source) is ArrowStreamReader


def _consume(source: Any, call: Any) -> Any:
    """Il testo di ArrowInvalid non e mai un protocollo di errore."""
    try:
        return call()
    except (StopIteration, StopAsyncIteration):
        raise
    except Exception:
        error = source.stream_state["error"] if _sdk_source(source) else None
        if error is None:
            from ._arrow_io import _mapping_error
            error = _mapping_error("stream Arrow SDK non leggibile", "read")
    # Raise outside the except block: suppressed __context__ still retains data.
    raise error from None


def _validated(value: Any) -> Any:
    value.validate(full=True)
    return value


class ArrowStreamReader:
    """Reader SDK che conserva il canale strutturato accanto allo stream C.

    ``ArrowStreamReader(source)`` importa la capsule. Per una capsule gia
    importata, ``ArrowStreamReader(source, reader=reader)`` associa il reader
    PyArrow alla sorgente originale, che resta viva fino alla fine.
    ``source.stream_state`` resta consultabile anche dopo un errore di un
    consumatore esterno che espone soltanto ArrowInvalid.
    """

    def __init__(self, source: Any, *, reader: Any = None) -> None:
        from ._arrow_io import _require_pyarrow, _SourceTypeError
        from ._native import BatchReader
        if type(source) is not BatchReader:
            raise _SourceTypeError("ArrowStreamReader richiede una sorgente SDK")
        self._source = source
        self._reader = reader if reader is not None else _consume(
            source, lambda: _require_pyarrow().RecordBatchReader.from_stream(source)
        )
        if type(self._reader) is not _require_pyarrow().RecordBatchReader:
            raise _SourceTypeError("reader deve essere un RecordBatchReader PyArrow")

    @property
    def stream_state(self) -> dict:
        return self._source.stream_state

    @property
    def schema(self) -> Any:
        return _consume(self._source, lambda: self._reader.schema)

    def __iter__(self) -> ArrowStreamReader:
        return self

    def __next__(self) -> Any:
        return _consume(self._source, lambda: _validated(next(self._reader)))

    def read_all(self) -> Any:
        return _consume(self._source, lambda: _validated(self._reader.read_all()))

    def close(self) -> None:
        self._source.close()
        self._reader.close()

    def __enter__(self) -> ArrowStreamReader:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class AsyncArrowStreamReader:
    """Consuma un AsyncBatchReader senza eseguire attese sincrone nel loop."""

    def __init__(self, source: Any) -> None:
        from ._native import AsyncBatchReader
        from ._arrow_io import _SourceTypeError
        if type(source) is not AsyncBatchReader:
            raise _SourceTypeError("AsyncArrowStreamReader richiede un reader async SDK")
        self._source = source

    @property
    def stream_state(self) -> dict:
        return self._source.stream_state

    def __aiter__(self) -> AsyncArrowStreamReader:
        return self

    async def __anext__(self) -> Any:
        from ._arrow_io import _require_pyarrow, _translated
        chunk = await self._source.__anext__()
        pa = _require_pyarrow()
        return await asyncio.to_thread(
            _translated, "read", "batch Arrow async non leggibile",
            lambda: _validated(pa.ipc.open_stream(pa.py_buffer(chunk)).read_next_batch()))

    async def read_all(self) -> Any:
        from ._arrow_io import _require_pyarrow, _translated
        pa = _require_pyarrow()
        schema_bytes = await self._source.schema_bytes()
        schema = await asyncio.to_thread(
            _translated, "read", "schema Arrow async non leggibile",
            lambda: pa.ipc.open_stream(pa.py_buffer(schema_bytes)).schema)
        batches = [batch async for batch in self]
        return await asyncio.to_thread(
            _translated, "read", "tabella Arrow async non leggibile",
            lambda: _validated(pa.Table.from_batches(batches, schema)))
