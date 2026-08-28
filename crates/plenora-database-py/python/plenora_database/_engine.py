"""Engine applicativi PostgreSQL sopra il lifecycle del Core v3."""

from __future__ import annotations

from typing import Any

from ._async_session import AsyncSession
from ._native import AsyncEngine as _NativeAsyncEngine
from ._native import Engine as _NativeEngine
from ._session import Session


class Engine:
    """Factory condivisibile di sessioni sync, una per unita di lavoro."""

    __slots__ = ("_native",)

    def __init__(self, native: _NativeEngine) -> None:
        self._native = native

    @property
    def provider_kind(self) -> str:
        return self._native.provider_kind

    @property
    def is_disposed(self) -> bool:
        return self._native.is_disposed

    def session(self) -> Session:
        return Session(self._native.session())

    def statistics(self) -> dict:
        return self._native.statistics()

    def dispose(self) -> None:
        self._native.dispose()

    def __enter__(self) -> Engine:
        return self

    def __exit__(self, *_args: Any) -> bool:
        self.dispose()
        return False

    def __repr__(self) -> str:
        return repr(self._native)


class AsyncEngine:
    """Factory condivisibile di sessioni asyncio, una per unita di lavoro."""

    __slots__ = ("_native",)

    def __init__(self, native: _NativeAsyncEngine) -> None:
        self._native = native

    @property
    def provider_kind(self) -> str:
        return self._native.provider_kind

    @property
    def is_disposed(self) -> bool:
        return self._native.is_disposed

    def session(self) -> AsyncSession:
        return AsyncSession(self._native.session())

    def statistics(self) -> dict:
        return self._native.statistics()

    def dispose(self) -> None:
        self._native.dispose()

    async def __aenter__(self) -> AsyncEngine:
        return self

    async def __aexit__(self, *_args: Any) -> bool:
        self.dispose()
        return False

    def __repr__(self) -> str:
        return repr(self._native)
