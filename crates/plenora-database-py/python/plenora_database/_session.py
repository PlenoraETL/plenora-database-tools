"""Wrapper Python di alto livello attorno alla Session nativa.

La Session nativa (`plenora_database._native.Session`) espone metodi
di basso livello. Questo wrapper:
  - Aggiunge factory methods `select` / `insert` / `update` / `delete`
    / `upsert` che ritornano i builder di `plenora_database.query`.
  - Fa da forward proxy trasparente per gli altri metodi/getters.
  - Implementa il context manager Python (`__enter__` / `__exit__`).
"""
from __future__ import annotations

from typing import Any

from ._native import Session as _NativeSession
from .query import Delete, Insert, Select, Update, Upsert


class Session:
    """Handle alla sessione Postgres. Restituita da
    `plenora_database.connect(dsn)`. Context-manager friendly."""

    __slots__ = ("_native",)

    def __init__(self, native: _NativeSession) -> None:
        self._native = native

    # ---------------------------- attributi ----------------------------

    @property
    def server_version(self) -> str:
        return self._native.server_version

    @property
    def postgis_version(self) -> str | None:
        return self._native.postgis_version

    @property
    def is_closed(self) -> bool:
        return self._native.is_closed

    # -------------------------- lifecycle ------------------------------

    def close(self) -> None:
        self._native.close()

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *_args: Any) -> bool:
        self.close()
        return False

    def __repr__(self) -> str:
        return repr(self._native)

    # --------------------------- SQL raw --------------------------------

    def execute(self, sql: str, params: list | None = None) -> int:
        return self._native.execute(sql, params)

    def execute_scalar(self, sql: str, params: list | None = None) -> Any:
        return self._native.execute_scalar(sql, params)

    def execute_returning_rows(self, sql: str, params: list | None = None) -> list[dict]:
        return self._native.execute_returning_rows(sql, params)

    # -------------------- portable AST builders -------------------------

    def select(self, table: str, schema: str | None = None) -> Select:
        return Select(self, table, schema)

    def insert(self, table: str, schema: str | None = None) -> Insert:
        return Insert(self, table, schema)

    def update(self, table: str, schema: str | None = None) -> Update:
        return Update(self, table, schema)

    def delete(self, table: str, schema: str | None = None) -> Delete:
        return Delete(self, table, schema)

    def upsert(self, table: str, schema: str | None = None) -> Upsert:
        return Upsert(self, table, schema)

    # ------- API interne consumate dai builder (via json AST) -----------

    def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return self._native.execute_portable_rows(ast_json)

    def _execute_portable_count(self, ast_json: str) -> int:
        return self._native.execute_portable_count(ast_json)
