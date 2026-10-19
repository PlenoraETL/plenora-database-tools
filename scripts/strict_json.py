"""Parse JSON without collapsing repeated members or exposing document data."""

import json
from typing import Any


class DuplicateJsonMember(ValueError):
    """An object contains an ambiguous member, at any depth."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJsonMember("chiave JSON ripetuta")
        result[key] = value
    return result


def loads(document: str | bytes) -> Any:
    """Reject duplicates recursively; errors never include keys or values."""
    try:
        return json.loads(document, object_pairs_hook=_unique_object)
    except DuplicateJsonMember:
        raise DuplicateJsonMember("chiave JSON ripetuta") from None
    except (ValueError, TypeError):
        raise ValueError("JSON non valido") from None
