"""Risultato bufferizzato uniforme per gli statement relazionali."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


class NoResultFound(LookupError):
    """Lo statement non ha restituito la riga richiesta."""


class MultipleResultsFound(LookupError):
    """Lo statement ha restituito più righe di quelle ammesse."""


class Result:
    """Snapshot di righe consumabile senza trattenere la sessione."""

    __slots__ = ("_rows",)

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = tuple(dict(row) for row in rows)

    def keys(self) -> tuple[str, ...]:
        return () if not self._rows else tuple(self._rows[0])

    def all(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._rows]

    def first(self) -> dict[str, Any] | None:
        return None if not self._rows else dict(self._rows[0])

    def one(self) -> dict[str, Any]:
        if not self._rows:
            raise NoResultFound("lo statement non ha restituito righe")
        if len(self._rows) != 1:
            raise MultipleResultsFound("lo statement ha restituito più di una riga")
        return dict(self._rows[0])

    def one_or_none(self) -> dict[str, Any] | None:
        if len(self._rows) > 1:
            raise MultipleResultsFound("lo statement ha restituito più di una riga")
        return self.first()

    def scalar(self) -> Any:
        row = self.first()
        return None if row is None else next(iter(row.values()))

    def scalar_one(self) -> Any:
        return next(iter(self.one().values()))

    def scalar_one_or_none(self) -> Any:
        row = self.one_or_none()
        return None if row is None else next(iter(row.values()))

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.all())

    def __len__(self) -> int:
        return len(self._rows)
