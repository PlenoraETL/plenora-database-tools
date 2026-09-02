"""Hook OpenTelemetry opt-in che non registrano SQL, parametri o DSN."""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter
from typing import Any

_OPERATIONS = {
    "begin",
    "cypher",
    "execute",
    "execute_sql",
    "execute_ddl",
    "query_sql",
    "execute_scalar",
    "read",
    "write",
}


class InstrumentedEngine:
    __slots__ = ("_engine", "_meter", "_tracer")

    def __init__(self, engine: Any, tracer: Any, meter: Any = None) -> None:
        self._engine = engine
        self._tracer = tracer
        self._meter = meter

    def session(self) -> Any:
        return _InstrumentedSession(
            self._engine.session(),
            self._tracer,
            self._meter,
            getattr(self._engine, "provider_kind", "unknown"),
        )

    def __enter__(self) -> InstrumentedEngine:  # noqa: PYI034
        self._engine.__enter__()
        return self

    def __exit__(self, *args: object) -> bool:
        return bool(self._engine.__exit__(*args))

    async def __aenter__(self) -> InstrumentedEngine:  # noqa: PYI034
        await self._engine.__aenter__()
        return self

    async def __aexit__(self, *args: object) -> bool:
        return bool(await self._engine.__aexit__(*args))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._engine, name)


class _InstrumentedSession:
    __slots__ = ("_meter", "_provider", "_session", "_tracer")

    def __init__(self, session: Any, tracer: Any, meter: Any, provider: str) -> None:
        self._session = session
        self._tracer = tracer
        self._meter = meter
        self._provider = provider

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._session, name)
        if name not in _OPERATIONS or not callable(target):
            return target
        if inspect.iscoroutinefunction(target):

            async def async_call(*args: Any, **kwargs: Any) -> Any:
                started = perf_counter()
                with self._span(name) as span:
                    try:
                        return await target(*args, **kwargs)
                    except BaseException:
                        _mark_error(span)
                        raise
                    finally:
                        self._record(name, started)

            return async_call

        def call(*args: Any, **kwargs: Any) -> Any:
            started = perf_counter()
            with self._span(name) as span:
                try:
                    return target(*args, **kwargs)
                except BaseException:
                    _mark_error(span)
                    raise
                finally:
                    self._record(name, started)

        return call

    def __enter__(self) -> _InstrumentedSession:  # noqa: PYI034
        self._session.__enter__()
        return self

    def __exit__(self, *args: object) -> bool:
        return bool(self._session.__exit__(*args))

    async def __aenter__(self) -> _InstrumentedSession:  # noqa: PYI034
        await self._session.__aenter__()
        return self

    async def __aexit__(self, *args: object) -> bool:
        return bool(await self._session.__aexit__(*args))

    @contextmanager
    def _span(self, operation: str) -> Iterator[Any]:
        with self._tracer.start_as_current_span(f"db.{operation}") as span:
            setter = getattr(span, "set_attribute", None)
            if callable(setter):
                setter("db.system", self._provider)
                setter("db.operation.name", operation)
            yield span

    def _record(self, operation: str, started: float) -> None:
        if self._meter is None:
            return
        histogram = self._meter.create_histogram("plenora.database.duration")
        histogram.record(
            perf_counter() - started,
            {"db.system": self._provider, "db.operation.name": operation},
        )


def instrument_engine(
    engine: Any, *, tracer: Any | None = None, meter: Any | None = None
) -> InstrumentedEngine:
    """Avvolge un Engine; nessun dato applicativo viene aggiunto agli span."""

    if tracer is None:
        try:
            from opentelemetry import trace
        except ImportError as error:
            raise RuntimeError("OpenTelemetry non installato") from error
        tracer = trace.get_tracer("plenora_database")
    if not callable(getattr(tracer, "start_as_current_span", None)):
        raise TypeError("tracer OpenTelemetry non valido")
    return InstrumentedEngine(engine, tracer, meter)


def _mark_error(span: Any) -> None:
    setter = getattr(span, "set_attribute", None)
    if callable(setter):
        setter("error.type", "database-operation-error")


__all__ = ["InstrumentedEngine", "instrument_engine"]
