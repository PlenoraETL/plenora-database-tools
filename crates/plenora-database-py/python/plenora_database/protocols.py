"""Protocolli strutturali della superficie sessione provider-neutral."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from .expression import ExecutableStatement
from .result import MutationResult, Result


class SessionProtocol(Protocol):
    @property
    def capabilities(self) -> dict[str, Any]: ...

    def execute(
        self,
        statement: ExecutableStatement,
        params: Mapping[str, Any] | None = None,
    ) -> Result | MutationResult: ...

    def execute_sql(
        self, sql: str, params: list[Any] | None = None
    ) -> MutationResult: ...

    def query_sql(self, sql: str, params: list[Any] | None = None) -> Result: ...

    def close(self) -> None: ...

    def __enter__(self) -> SessionProtocol: ...

    def __exit__(self, *args: Any) -> bool: ...


class AsyncSessionProtocol(Protocol):
    @property
    def capabilities(self) -> dict[str, Any]: ...

    async def execute(
        self,
        statement: ExecutableStatement,
        params: Mapping[str, Any] | None = None,
    ) -> Result | MutationResult: ...

    async def execute_sql(
        self, sql: str, params: list[Any] | None = None
    ) -> MutationResult: ...

    async def query_sql(
        self, sql: str, params: list[Any] | None = None
    ) -> Result: ...

    def close(self) -> None: ...

    async def __aenter__(self) -> AsyncSessionProtocol: ...

    async def __aexit__(self, *args: Any) -> bool: ...


__all__ = ["AsyncSessionProtocol", "SessionProtocol"]
