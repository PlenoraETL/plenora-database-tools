"""Wrapper Python della Transaction nativa.

Aggiunge sopra al `_native.Transaction`:
  - Factory methods `select` / `insert` / `update` / `delete` / `upsert`
    che ritornano i builder di `plenora_database.query`. I builder
    funzionano sia con Session sia con Transaction (duck typing sul
    metodo `_execute_portable_rows` / `_execute_portable_count`).
  - Delega trasparente delle properties (`is_active`) e dei metodi
    (execute, commit, rollback, savepoint...).
  - Implementa il context manager al livello Python.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, overload

from .expression import ExecutableStatement, _execute_statement
from .query import _BuilderFactory
from .result import MutationResult, Result

if TYPE_CHECKING:
    from ._native import Transaction as _NativeTransaction


class Transaction(_BuilderFactory):
    """Transazione user-managed. Costruita da `Session.begin(...)`.

    Uso raccomandato con context manager:

        with s.begin() as tx:
            tx.execute("INSERT ...")
            row = tx.select("t").where_eq("id", 1).one()
        # commit su uscita normale, rollback su eccezione

    Anche esplicito:

        tx = s.begin()
        try:
            tx.execute(...)
            tx.commit()
        except:
            tx.rollback()
            raise
    """

    __slots__ = ("_native", "_provider")

    def __init__(self, native: "_NativeTransaction", provider: str = "postgres") -> None:
        self._native = native
        self._provider = provider

    # ---------------------------- attributi ----------------------------

    @property
    def is_active(self) -> bool:
        return self._native.is_active

    # ------------------------- lifecycle --------------------------------

    def commit(self) -> None:
        self._native.commit()

    def rollback(self) -> None:
        self._native.rollback()

    def savepoint(self, name: str) -> None:
        self._native.savepoint(name)

    def rollback_to_savepoint(self, name: str) -> None:
        self._native.rollback_to_savepoint(name)

    def release_savepoint(self, name: str) -> None:
        self._native.release_savepoint(name)

    def conditional_update(
        self,
        update_sql: str,
        update_params: list | None = None,
        expected_affected_rows: int = 1,
        key_probe_sql: str | None = None,
        key_probe_params: list | None = None,
    ) -> None:
        """Update ottimistico condizionato con classificazione errore
        precisa (NotFound vs ConcurrentModification).

        Rispetto al pattern manuale `.update().where_eq("version", cur).execute()`
        + check `n == 0`, questo metodo distingue:
          - `PlenoraNotFoundError` — chiave assente (probe conferma)
          - `PlenoraConcurrentModificationError` — chiave esiste ma
            versione diversa (o probe assente)

        Args:
            update_sql: UPDATE con WHERE key + version, tipicamente
                `UPDATE t SET x=$1, version=$2 WHERE id=$3 AND version=$4`.
            update_params: parametri positional per l'UPDATE.
            expected_affected_rows: righe attese (default 1).
            key_probe_sql: SELECT che verifica se la chiave esiste
                (usato solo su mismatch). Se None, tutti i mismatch
                sono classificati come ConcurrentModification.
            key_probe_params: parametri positional per il probe.

        Raises:
            PlenoraNotFoundError: chiave assente.
            PlenoraConcurrentModificationError: mismatch versione.
        """
        self._native.conditional_update(
            update_sql,
            update_params,
            expected_affected_rows,
            key_probe_sql,
            key_probe_params,
        )

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        # Delega al lato nativo: commit su nessuna eccezione, rollback altrimenti.
        return self._native.__exit__(exc_type, exc_value, traceback)

    def __repr__(self) -> str:
        return repr(self._native)

    # --------------------------- SQL raw --------------------------------

    @overload
    def execute(self, sql: str, params: list | None = None) -> int: ...

    @overload
    def execute(
        self,
        sql: ExecutableStatement,
        params: Mapping[str, Any] | None = None,
    ) -> Result | int | MutationResult: ...

    def execute(self, sql, params=None):
        if isinstance(sql, ExecutableStatement):
            return _execute_statement(self._native, sql, params, self._provider)
        return self._native.execute(sql, params)

    def execute_scalar(self, sql: str, params: list | None = None) -> Any:
        return self._native.execute_scalar(sql, params)

    def execute_returning_rows(self, sql: str, params: list | None = None) -> list[dict]:
        return self._native.execute_returning_rows(sql, params)

    # ------- API interne consumate dai builder (via json AST) -----------

    def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return self._native.execute_portable_rows(ast_json)

    def _execute_portable_count(self, ast_json: str) -> int:
        return self._native.execute_portable_count(ast_json)
