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
from .graph import GraphValue, _decode_rows
from .query import _BuilderFactory
from .result import MutationResult, Result

if TYPE_CHECKING:
    from ._native import Transaction as _NativeTransaction


def _require_native(transaction: "Transaction") -> "_NativeTransaction":
    native = transaction._native
    if native is None:
        raise RuntimeError(
            "transaction non attiva: gia committata o rollback-ata "
            "(o chiusa dal context manager)"
        )
    return native


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
        self._native: _NativeTransaction | None = native
        self._provider = provider

    # ---------------------------- attributi ----------------------------

    @property
    def is_active(self) -> bool:
        native = self._native
        return native is not None and native.is_active

    # ------------------------- lifecycle --------------------------------

    def commit(self) -> None:
        native = _require_native(self)
        try:
            native.commit()
        finally:
            # `_native.Transaction` e thread-affine. Il wrapper puo restare
            # nel traceback e migrare a un altro thread, l'oggetto nativo no.
            self._native = None
            del native

    def rollback(self) -> None:
        native = _require_native(self)
        try:
            native.rollback()
        finally:
            self._native = None
            del native

    def savepoint(self, name: str) -> None:
        _require_native(self).savepoint(name)

    def rollback_to_savepoint(self, name: str) -> None:
        _require_native(self).rollback_to_savepoint(name)

    def release_savepoint(self, name: str) -> None:
        _require_native(self).release_savepoint(name)

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
        _require_native(self).conditional_update(
            update_sql,
            update_params,
            expected_affected_rows,
            key_probe_sql,
            key_probe_params,
        )

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        native = self._native
        if native is None:
            return False
        self._native = None
        # `native` viene distrutto su questo thread anche se `self` resta nel
        # traceback consegnato a un exception handler asincrono.
        try:
            return native.__exit__(exc_type, exc_value, traceback)
        finally:
            # Copre anche un errore prodotto da commit/rollback nativo.
            del native

    def __repr__(self) -> str:
        native = self._native
        return repr(native) if native is not None else "<Transaction active=False>"

    # --------------------------- SQL raw --------------------------------

    def execute(
        self,
        statement: ExecutableStatement,
        params: Mapping[str, Any] | None = None,
    ) -> Result | MutationResult:
        native = _require_native(self)
        if not isinstance(statement, ExecutableStatement):
            raise TypeError("execute richiede uno statement relazionale")
        return _execute_statement(native, statement, params, self._provider)

    def execute_sql(self, sql: str, params: list | None = None) -> MutationResult:
        affected = _require_native(self).execute(sql, params)
        return MutationResult("sql", self._provider, affected)

    def query_sql(self, sql: str, params: list | None = None) -> Result:
        rows = _require_native(self).execute_returning_rows(sql, params)
        return Result(rows)

    def execute_scalar(self, sql: str, params: list | None = None) -> Any:
        return _require_native(self).execute_scalar(sql, params)

    def cypher(
        self,
        graph: str,
        query: str,
        columns: list[str],
        params: dict[str, Any] | None = None,
        *,
        max_rows: int = 10_000,
    ) -> list[dict[str, GraphValue]]:
        return _decode_rows(
            _require_native(self).cypher(
                graph, query, columns, params, max_rows=max_rows
            )
        )

    # ------- API interne consumate dai builder (via json AST) -----------

    def _execute_portable_rows(self, ast_json: str) -> list[dict]:
        return _require_native(self).execute_portable_rows(ast_json)

    def _execute_portable_count(self, ast_json: str) -> int:
        return _require_native(self).execute_portable_count(ast_json)
