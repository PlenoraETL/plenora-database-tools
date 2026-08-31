"""Typed params helpers (F3-6b).

Python `int`, `float`, `str` sono ambigui rispetto al tipo Postgres di
destinazione (str → text? uuid? timestamp?). Questi helper wrappano
il valore con un tag esplicito che il layer nativo usa per costruire
la variant `ParameterValue` corretta.

Esempi:

    import plenora_database as p

    with p.connect(dsn) as s:
        s.execute("INSERT INTO t(id) VALUES ($1)",
                  [p.uuid("550e8400-e29b-41d4-a716-446655440000")])
        s.execute("INSERT INTO t(ts) VALUES ($1)",
                  [p.timestamptz("2026-01-01T00:00:00Z")])
        s.execute("INSERT INTO t(bal) VALUES ($1)",
                  [p.decimal("1234.56")])
        s.execute("INSERT INTO t(row_version) VALUES ($1)",
                  [p.int64(1)])
        s.execute("INSERT INTO t(val) VALUES ($1)",
                  [p.null("text")])   # NULL con hint tipo colonna

Funzionano anche nei builder portable:

    s.select("t").where_eq("uid", p.uuid("...")).one()
"""
from __future__ import annotations

from typing import Any


class TypedValue:
    """Wrapper opaco per un valore con tag di tipo esplicito.

    Il layer nativo cerca gli attributi `_plenora_typed_kind` (str) e
    `_plenora_typed_value` (payload) sull'oggetto passato come
    parametro; se presenti, costruisce direttamente la
    `ParameterValue` variant con quel tag invece di fare inferenza dal
    tipo Python.
    """

    __slots__ = ("_plenora_typed_kind", "_plenora_typed_value")

    def __init__(self, kind: str, value: Any) -> None:
        self._plenora_typed_kind = kind
        self._plenora_typed_value = value

    def __repr__(self) -> str:
        return f"TypedValue({self._plenora_typed_kind!r}, {self._plenora_typed_value!r})"


def uuid(value: str) -> TypedValue:
    """Forza il parametro a `ParameterValue::Uuid`."""
    return TypedValue("uuid", value)


def int64(value: int) -> TypedValue:
    """Forza un ``int`` Python a ``ParameterValue::I64``.

    Utile quando il valore rientra nell'intervallo ``int4`` ma PostgreSQL si
    aspetta un parametro binario ``bigint``/``int8``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("int64 richiede un int Python")
    if not -(1 << 63) <= value < (1 << 63):
        raise OverflowError("int64 fuori dall'intervallo signed 64-bit")
    return TypedValue("i64", value)


def date(value: str) -> TypedValue:
    """Forza il parametro a `ParameterValue::Date` (ISO 'YYYY-MM-DD')."""
    return TypedValue("date", value)


def timestamp(value: str) -> TypedValue:
    """Forza il parametro a `ParameterValue::Timestamp` (senza timezone)."""
    return TypedValue("timestamp", value)


def timestamptz(value: str) -> TypedValue:
    """Forza il parametro a `ParameterValue::TimestampTz` (con timezone)."""
    return TypedValue("timestamp_tz", value)


def decimal(value: str) -> TypedValue:
    """Forza il parametro a `ParameterValue::Decimal` (rappresentazione
    testuale precisa; accetta str per evitare precision loss)."""
    return TypedValue("decimal", str(value))


def null(type_name: str) -> TypedValue:
    """NULL con hint di tipo per il target. Il `type_name` è passato al
    driver Postgres come hint del tipo colonna (`int4`, `text`, `uuid`,
    `jsonb`, ...). Necessario quando il tipo target non può essere
    inferito dal contesto SQL (es. bind $1 in una tabella multi-colonna)."""
    return TypedValue("null", {"type_name": type_name})
