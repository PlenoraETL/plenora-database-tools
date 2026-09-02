"""Risultato bufferizzato uniforme per gli statement relazionali."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from .expression import Column
from .errors import PlenoraConflictError, PlenoraNotFoundError


class NoResultFound(PlenoraNotFoundError):
    """Lo statement non ha restituito la riga richiesta."""


class MultipleResultsFound(PlenoraConflictError):
    """Lo statement ha restituito più righe di quelle ammesse."""


class MutationResult:
    """Esito DML quando il conteggio non ha la stessa semantica ovunque."""

    __slots__ = ("operation", "provider", "affected_rows")

    def __init__(
        self, operation: str, provider: str, affected_rows: int | None
    ) -> None:
        self.operation = operation
        self.provider = provider
        self.affected_rows = affected_rows

    @property
    def count_is_known(self) -> bool:
        return self.affected_rows is not None

    def __bool__(self) -> bool:
        raise TypeError("MutationResult richiede un controllo esplicito dell'esito")

    def __repr__(self) -> str:
        state = "known" if self.count_is_known else "unknown"
        return (
            f"MutationResult(operation={self.operation!r}, "
            f"provider={self.provider!r}, count={state})"
        )


class Row:
    """Riga immutabile accessibile per posizione, nome o colonna."""

    __slots__ = ("_keys", "_values", "_mapping")

    def __init__(self, values: Mapping[str, Any]) -> None:
        self._keys = tuple(values)
        self._values = tuple(values.values())
        self._mapping = dict(zip(self._keys, self._values, strict=True))

    def __getitem__(self, key: int | str | Column) -> Any:
        if isinstance(key, int) and not isinstance(key, bool):
            return self._values[key]
        if isinstance(key, Column):
            key = key.name
        if isinstance(key, str):
            try:
                return self._mapping[key]
            except KeyError as error:
                raise KeyError("colonna non presente nella riga") from error
        raise TypeError("la riga accetta posizione, nome o Column")

    def keys(self) -> tuple[str, ...]:
        return self._keys

    def values(self) -> tuple[Any, ...]:
        return self._values

    def items(self) -> tuple[tuple[str, Any], ...]:
        return tuple(zip(self._keys, self._values, strict=True))

    def get(self, key: str | Column, default: Any = None) -> Any:
        if isinstance(key, Column):
            key = key.name
        if not isinstance(key, str):
            raise TypeError("Row.get accetta un nome o una Column")
        return self._mapping.get(key, default)

    def as_dict(self) -> dict[str, Any]:
        return dict(self._mapping)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __contains__(self, key: object) -> bool:
        if isinstance(key, Column):
            key = key.name
        return isinstance(key, str) and key in self._mapping

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"Row(keys={self._keys!r})"


class Result:
    """Snapshot di righe consumabile senza trattenere la sessione."""

    __slots__ = ("_rows",)

    def __init__(self, rows: list[Mapping[str, Any]]) -> None:
        self._rows = tuple(Row(row) for row in rows)

    def keys(self) -> tuple[str, ...]:
        return () if not self._rows else self._rows[0].keys()

    def all(self) -> list[Row]:
        return list(self._rows)

    def first(self) -> Row | None:
        return None if not self._rows else self._rows[0]

    def one(self) -> Row:
        if not self._rows:
            raise NoResultFound("lo statement non ha restituito righe")
        if len(self._rows) != 1:
            raise MultipleResultsFound("lo statement ha restituito più di una riga")
        return self._rows[0]

    def one_or_none(self) -> Row | None:
        if len(self._rows) > 1:
            raise MultipleResultsFound("lo statement ha restituito più di una riga")
        return self.first()

    def tuples(self) -> list[tuple[Any, ...]]:
        return [row.values() for row in self._rows]

    def scalar(self) -> Any:
        row = self.first()
        return None if row is None else row[0]

    def scalar_one(self) -> Any:
        return self.one()[0]

    def scalar_one_or_none(self) -> Any:
        row = self.one_or_none()
        return None if row is None else row[0]

    def __iter__(self) -> Iterator[Row]:
        return iter(self._rows)

    def __len__(self) -> int:
        return len(self._rows)
