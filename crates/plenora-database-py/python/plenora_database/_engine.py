"""Engine applicativi provider-neutral sopra il lifecycle del Core v3."""

from __future__ import annotations

from typing import Any, Callable

from ._async_session import AsyncSession
from ._native import AsyncEngine as _NativeAsyncEngine
from ._native import Engine as _NativeEngine
from ._session import Session
from .metadata import MetaData


class Engine:
    """Factory condivisibile di sessioni sync, una per unita di lavoro."""

    __slots__ = ("_native", "_wrap_session")

    def __init__(
        self,
        native: _NativeEngine,
        wrap_session: Callable[[Any], Any] | None = None,
    ) -> None:
        self._native = native
        self._wrap_session = wrap_session or Session

    @property
    def provider_kind(self) -> str:
        return self._native.provider_kind

    @property
    def is_disposed(self) -> bool:
        return self._native.is_disposed

    @property
    def metadata_cache_entries(self) -> int:
        return self._native.metadata_cache_entries

    def session(self) -> Any:
        return self._wrap_session(self._native.session())

    def orm_session(self, **options: Any) -> Any:
        from .orm import OrmSession

        options.setdefault("close_session", True)
        session = self.session()
        try:
            return OrmSession(session, **options)
        except BaseException:
            try:
                session.close()
            except BaseException:  # la chiusura non maschera l'errore originale
                pass
            raise

    def statistics(self) -> dict:
        return self._native.statistics()

    def reflect_table(
        self,
        schema: str | None,
        table: str,
        *,
        catalog: str | None = None,
        refresh: bool = False,
    ) -> MetaData:
        return MetaData.from_document(
            self._native.reflect_table(
                schema, table, catalog=catalog, refresh=refresh
            )
        )

    def invalidate_metadata(
        self,
        schema: str | None = None,
        table: str | None = None,
        *,
        catalog: str | None = None,
    ) -> int:
        return self._native.invalidate_metadata(schema, table, catalog=catalog)

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

    __slots__ = ("_native", "_wrap_session")

    def __init__(
        self,
        native: _NativeAsyncEngine,
        wrap_session: Callable[[Any], Any] | None = None,
    ) -> None:
        self._native = native
        self._wrap_session = wrap_session or AsyncSession

    @property
    def provider_kind(self) -> str:
        return self._native.provider_kind

    @property
    def is_disposed(self) -> bool:
        return self._native.is_disposed

    @property
    def metadata_cache_entries(self) -> int:
        return self._native.metadata_cache_entries

    def session(self) -> Any:
        return self._wrap_session(self._native.session())

    def orm_session(self, **options: Any) -> Any:
        from .orm import AsyncOrmSession

        options.setdefault("close_session", True)
        session = self.session()
        try:
            return AsyncOrmSession(session, **options)
        except BaseException:
            try:
                session.close()
            except BaseException:  # la chiusura non maschera l'errore originale
                pass
            raise

    def statistics(self) -> dict:
        return self._native.statistics()

    async def reflect_table(
        self,
        schema: str | None,
        table: str,
        *,
        catalog: str | None = None,
        refresh: bool = False,
    ) -> MetaData:
        return MetaData.from_document(
            await self._native.reflect_table(
                schema, table, catalog=catalog, refresh=refresh
            )
        )

    def invalidate_metadata(
        self,
        schema: str | None = None,
        table: str | None = None,
        *,
        catalog: str | None = None,
    ) -> int:
        return self._native.invalidate_metadata(schema, table, catalog=catalog)

    def dispose(self) -> None:
        self._native.dispose()

    async def __aenter__(self) -> AsyncEngine:
        return self

    async def __aexit__(self, *_args: Any) -> bool:
        self.dispose()
        return False

    def __repr__(self) -> str:
        return repr(self._native)
