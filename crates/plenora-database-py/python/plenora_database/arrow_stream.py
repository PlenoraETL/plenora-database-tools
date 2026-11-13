"""Consumo C con accesso allo stato strutturato della sorgente SDK."""
from __future__ import annotations

import asyncio

from typing import Any


def _native_reader(source: Any) -> bool:
    from ._native import BatchReader, AsyncBatchReader
    return type(source) in (BatchReader, AsyncBatchReader)


def _sdk_source(source: Any) -> bool:
    return _native_reader(source) or type(source) is ArrowStreamReader


def _stream_failure(source: Any) -> Exception:
    if _sdk_source(source):
        native = source._source if type(source) is ArrowStreamReader else source
        error = native.stream_state["error"]
        return error if error is not None else native._record_arrow_failure()
    from ._arrow_io import _mapping_error
    return _mapping_error("stream Arrow SDK non leggibile", "read")


def _interrupt(source: Any) -> Exception:
    if _sdk_source(source):
        native = source._source if type(source) is ArrowStreamReader else source
        error = native._record_arrow_cancellation()
        native.close()
        return error
    return _stream_failure(source)


def _consume(source: Any, call: Any) -> Any:
    """State is the only error channel, never the text of ArrowInvalid."""
    if _sdk_source(source):
        error = source.stream_state["error"]
        if error is not None:
            raise error from None
    try:
        return call()
    except (StopIteration, StopAsyncIteration):
        raise
    except KeyboardInterrupt:
        error = _interrupt(source)
    except Exception:
        error = _stream_failure(source)
    raise error from None


async def _await_source(source: Any, awaitable: Any) -> Any:
    try:
        return await awaitable
    except (StopIteration, StopAsyncIteration):
        raise
    except (asyncio.CancelledError, KeyboardInterrupt):
        error = _interrupt(source)
        await asyncio.shield(source.aclose())
    except Exception:
        error = _stream_failure(source)
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
        try:
            self._reader.close()
        except Exception:
            error = _stream_failure(self._source)
        else:
            return
        raise error from None

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

    async def _schema(self) -> Any:
        from ._arrow_io import _require_pyarrow
        if not hasattr(self, "_cached_schema"):
            raw = await _await_source(self._source, self._source.schema_bytes())
            pa = _require_pyarrow()
            self._cached_schema = await _await_source(self._source, asyncio.to_thread(
                _consume, self._source, lambda: pa.ipc.open_stream(pa.py_buffer(raw)).schema))
        return self._cached_schema

    async def __anext__(self) -> Any:
        from ._arrow_io import _decode_chunk
        schema = await self._schema()
        chunk = await _await_source(self._source, self._source.__anext__())
        batches = await _await_source(self._source, asyncio.to_thread(
            _consume, self._source, lambda: _decode_chunk(chunk, schema)))
        if len(batches) != 1:
            raise _stream_failure(self._source) from None
        return batches[0]

    async def read_all(self) -> Any:
        from ._arrow_io import _require_pyarrow
        schema = await self._schema()
        batches = [batch async for batch in self]
        return await _await_source(self._source, asyncio.to_thread(_consume, self._source,
            lambda: _validated(_require_pyarrow().Table.from_batches(batches, schema))))

    async def aclose(self) -> None:
        await self._source.aclose()

    async def close(self) -> None:
        await self.aclose()

    async def __aenter__(self) -> AsyncArrowStreamReader:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()
