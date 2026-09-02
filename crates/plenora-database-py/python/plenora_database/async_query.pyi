"""Type stubs per i builder portable AST (async).

Le classi Async ereditano dai builder sync e overriddano solo i
terminali (`all` / `one` / `scalar` / `execute`) come coroutine.
"""
from typing import Any

from .query import Delete, Insert, Select, Update, Upsert
from .result import MutationResult, Row


class AsyncSelect(Select):
    async def all(self) -> list[Row]: ...  # type: ignore[override]
    async def one(self) -> Row | None: ...  # type: ignore[override]
    async def scalar(self) -> Any: ...  # type: ignore[override]


class AsyncInsert(Insert):
    async def execute(self) -> MutationResult: ...  # type: ignore[override]
    async def all(self) -> list[Row]: ...  # type: ignore[override]
    async def one(self) -> Row: ...  # type: ignore[override]


class AsyncUpdate(Update):
    async def execute(self) -> MutationResult: ...  # type: ignore[override]
    async def all(self) -> list[Row]: ...  # type: ignore[override]
    async def one(self) -> Row: ...  # type: ignore[override]


class AsyncDelete(Delete):
    async def execute(self) -> MutationResult: ...  # type: ignore[override]
    async def all(self) -> list[Row]: ...  # type: ignore[override]
    async def one(self) -> Row: ...  # type: ignore[override]


class AsyncUpsert(Upsert):
    async def execute(self) -> MutationResult: ...  # type: ignore[override]
    async def all(self) -> list[Row]: ...  # type: ignore[override]
    async def one(self) -> Row: ...  # type: ignore[override]
