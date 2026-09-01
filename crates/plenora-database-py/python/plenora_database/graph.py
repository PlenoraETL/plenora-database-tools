"""Tipi pubblici per i risultati Apache AGE."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import MISSING, dataclass, fields, is_dataclass
from decimal import Decimal
from typing import Any, TypeAlias, TypeVar


@dataclass(frozen=True, slots=True)
class Vertex:
    id: int
    label: str
    properties: dict[str, GraphValue]


@dataclass(frozen=True, slots=True)
class Edge:
    id: int
    label: str
    start_id: int
    end_id: int
    properties: dict[str, GraphValue]


@dataclass(frozen=True, slots=True)
class Path:
    elements: tuple[GraphValue, ...]


GraphValue: TypeAlias = (
    None
    | bool
    | int
    | float
    | Decimal
    | str
    | list[Any]
    | dict[str, Any]
    | Vertex
    | Edge
    | Path
)

_Model = TypeVar("_Model")


def _identifier(value: str, role: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 63
        or not (value[0].isalpha() or value[0] == "_")
        or not all(
            character.isascii() and (character.isalnum() or character == "_")
            for character in value
        )
    ):
        raise ValueError(f"{role} non e un identificatore ASCII semplice")
    return value


def _cypher_identifier(value: str) -> str:
    """Quota un identificatore gia validato, inclusi i keyword Cypher."""

    return f"`{value}`"


def _row_value(name: str) -> str:
    """Accede a una map agtype senza interpretarne la chiave come keyword."""

    return f'row["{name}"]'


def vertex_model(
    label: str, *, id_field: str | None = None
) -> Callable[[type[_Model]], type[_Model]]:
    """Associa una dataclass a una label AGE senza introdurre I/O implicito."""

    label = _identifier(label, "label vertex")
    if id_field is not None:
        _identifier(id_field, "campo id vertex")

    def decorate(model: type[_Model]) -> type[_Model]:
        if not is_dataclass(model):
            raise TypeError("vertex_model richiede una dataclass")
        names = {item.name for item in fields(model)}
        if id_field is not None and id_field not in names:
            raise TypeError("il campo id vertex non appartiene alla dataclass")
        model.__plenora_graph_kind__ = "vertex"  # type: ignore[attr-defined]
        model.__plenora_graph_label__ = label  # type: ignore[attr-defined]
        model.__plenora_graph_id_field__ = id_field  # type: ignore[attr-defined]
        return model

    return decorate


def edge_model(
    label: str,
    *,
    id_field: str | None = None,
    start_id_field: str = "start_id",
    end_id_field: str = "end_id",
) -> Callable[[type[_Model]], type[_Model]]:
    """Associa una dataclass a una label edge AGE."""

    label = _identifier(label, "label edge")
    for name in (start_id_field, end_id_field):
        _identifier(name, "campo endpoint edge")
    if id_field is not None:
        _identifier(id_field, "campo id edge")

    def decorate(model: type[_Model]) -> type[_Model]:
        if not is_dataclass(model):
            raise TypeError("edge_model richiede una dataclass")
        names = {item.name for item in fields(model)}
        required = {start_id_field, end_id_field}
        if id_field is not None:
            required.add(id_field)
        if not required <= names:
            raise TypeError("i campi identity edge non appartengono alla dataclass")
        model.__plenora_graph_kind__ = "edge"  # type: ignore[attr-defined]
        model.__plenora_graph_label__ = label  # type: ignore[attr-defined]
        model.__plenora_graph_id_field__ = id_field  # type: ignore[attr-defined]
        model.__plenora_graph_start_id_field__ = start_id_field  # type: ignore[attr-defined]
        model.__plenora_graph_end_id_field__ = end_id_field  # type: ignore[attr-defined]
        return model

    return decorate


def graph_entity_to_model(entity: Vertex | Edge, model: type[_Model]) -> _Model:
    """Materializza una dataclass registrata, fallendo su mapping incompleti."""

    kind = getattr(model, "__plenora_graph_kind__", None)
    expected_label = getattr(model, "__plenora_graph_label__", None)
    if not is_dataclass(model) or expected_label != entity.label:
        raise TypeError("entita graph incompatibile con il modello dichiarato")
    if (kind == "vertex") != isinstance(entity, Vertex):
        raise TypeError("tipo graph incompatibile con il modello dichiarato")
    if (kind == "edge") != isinstance(entity, Edge):
        raise TypeError("tipo graph incompatibile con il modello dichiarato")

    values = dict(entity.properties)
    id_field = getattr(model, "__plenora_graph_id_field__", None)
    if id_field is not None:
        values[id_field] = entity.id
    if isinstance(entity, Edge):
        values[model.__plenora_graph_start_id_field__] = entity.start_id  # type: ignore[attr-defined]
        values[model.__plenora_graph_end_id_field__] = entity.end_id  # type: ignore[attr-defined]
    declared = {item.name: item for item in fields(model)}
    if set(values) - set(declared):
        raise TypeError("le proprieta graph contengono campi non mappati")
    missing = {
        name
        for name, field in declared.items()
        if name not in values
        and field.default is MISSING
        and field.default_factory is MISSING
    }
    if missing:
        raise TypeError("le proprieta graph non coprono i campi obbligatori")
    return model(**values)


def graph_model_properties(instance: Any) -> dict[str, GraphValue]:
    """Estrae le sole proprieta da un modello graph dichiarato."""

    if not is_dataclass(instance) or getattr(
        instance, "__plenora_graph_kind__", None
    ) not in {
        "vertex",
        "edge",
    }:
        raise TypeError("istanza priva di mapping graph")
    excluded = {
        getattr(instance, "__plenora_graph_id_field__", None),
        getattr(instance, "__plenora_graph_start_id_field__", None),
        getattr(instance, "__plenora_graph_end_id_field__", None),
    }
    return {
        field.name: getattr(instance, field.name)
        for field in fields(instance)
        if field.name not in excluded
    }


def _normalized_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [dict(row) for row in rows]
    if not normalized:
        return []
    names = tuple(normalized[0])
    if not names or len(set(names)) != len(names):
        raise ValueError("un batch graph richiede proprieta univoche")
    for name in names:
        _identifier(name, "proprieta graph")
    expected = set(names)
    if any(set(row) != expected for row in normalized):
        raise ValueError("le righe graph richiedono lo stesso insieme di proprieta")
    return normalized


def _chunks(
    rows: list[dict[str, Any]], batch_size: int
) -> Iterable[list[dict[str, Any]]]:
    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or not 1 <= batch_size <= 10_000
    ):
        raise ValueError("batch_size graph deve essere tra 1 e 10000")
    for offset in range(0, len(rows), batch_size):
        yield rows[offset : offset + batch_size]


def bulk_vertices(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    merge_key: str | None = None,
    batch_size: int = 100,
) -> int:
    """Crea o fa MERGE di vertex con UNWIND e parametri separati."""

    _identifier(graph, "nome graph")
    label = _identifier(label, "label vertex")
    normalized = _normalized_rows(rows)
    if not normalized:
        return 0
    if merge_key is not None:
        merge_key = _identifier(merge_key, "chiave merge vertex")
        if merge_key not in normalized[0]:
            raise ValueError("la chiave merge vertex manca dal batch")
    label_sql = _cypher_identifier(label)
    assignments = ", ".join(
        f"{_cypher_identifier(name)}: {_row_value(name)}" for name in normalized[0]
    )
    action = (
        f"MERGE (node:{label_sql} "
        f"{{{_cypher_identifier(merge_key)}: {_row_value(merge_key)}}}) "
        f"SET node = {{{assignments}}}"
        if merge_key is not None
        else f"CREATE (node:{label_sql} {{{assignments}}})"
    )
    query = f"UNWIND $rows AS row {action} RETURN count(node)"
    affected = 0
    for chunk in _chunks(normalized, batch_size):
        result = executor.cypher(
            graph, query, ["affected"], {"rows": chunk}, max_rows=1
        )
        if len(result) != 1 or not isinstance(result[0].get("affected"), int):
            raise RuntimeError("bulk vertex AGE privo di conteggio affidabile")
        affected += result[0]["affected"]
    return affected


def bulk_edges(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    start_label: str,
    start_key: str,
    end_label: str,
    end_key: str,
    start_value_field: str = "start",
    end_value_field: str = "end",
    batch_size: int = 100,
) -> int:
    """Crea edge in batch risolvendo gli endpoint per business key."""

    _identifier(graph, "nome graph")
    for value, role in (
        (label, "label edge"),
        (start_label, "label vertex iniziale"),
        (start_key, "chiave vertex iniziale"),
        (end_label, "label vertex finale"),
        (end_key, "chiave vertex finale"),
        (start_value_field, "campo endpoint iniziale"),
        (end_value_field, "campo endpoint finale"),
    ):
        _identifier(value, role)
    normalized = _normalized_rows(rows)
    if not normalized:
        return 0
    required = {start_value_field, end_value_field}
    if not required <= set(normalized[0]):
        raise ValueError("il batch edge non contiene entrambi gli endpoint")
    property_names = tuple(name for name in normalized[0] if name not in required)
    properties = (
        " {"
        + ", ".join(
            f"{_cypher_identifier(name)}: {_row_value(name)}" for name in property_names
        )
        + "}"
        if property_names
        else ""
    )
    start_label_sql = _cypher_identifier(start_label)
    start_key_sql = _cypher_identifier(start_key)
    end_label_sql = _cypher_identifier(end_label)
    end_key_sql = _cypher_identifier(end_key)
    label_sql = _cypher_identifier(label)
    query = (
        f"UNWIND $rows AS row "
        f"MATCH (source_node:{start_label_sql}) "
        f"WHERE source_node.{start_key_sql} = {_row_value(start_value_field)} "
        f"MATCH (target_node:{end_label_sql}) "
        f"WHERE target_node.{end_key_sql} = {_row_value(end_value_field)} "
        f"CREATE (source_node)-[edge:{label_sql}{properties}]->(target_node) "
        "RETURN count(edge)"
    )
    affected = 0
    for chunk in _chunks(normalized, batch_size):
        result = executor.cypher(
            graph, query, ["affected"], {"rows": chunk}, max_rows=1
        )
        if len(result) != 1 or not isinstance(result[0].get("affected"), int):
            raise RuntimeError("bulk edge AGE privo di conteggio affidabile")
        affected += result[0]["affected"]
    return affected


async def abulk_vertices(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    merge_key: str | None = None,
    batch_size: int = 100,
) -> int:
    """Variante async di :func:`bulk_vertices`."""

    _identifier(graph, "nome graph")
    label = _identifier(label, "label vertex")
    normalized = _normalized_rows(rows)
    if not normalized:
        return 0
    if merge_key is not None:
        merge_key = _identifier(merge_key, "chiave merge vertex")
        if merge_key not in normalized[0]:
            raise ValueError("la chiave merge vertex manca dal batch")
    label_sql = _cypher_identifier(label)
    assignments = ", ".join(
        f"{_cypher_identifier(name)}: {_row_value(name)}" for name in normalized[0]
    )
    action = (
        f"MERGE (node:{label_sql} "
        f"{{{_cypher_identifier(merge_key)}: {_row_value(merge_key)}}}) "
        f"SET node = {{{assignments}}}"
        if merge_key is not None
        else f"CREATE (node:{label_sql} {{{assignments}}})"
    )
    query = f"UNWIND $rows AS row {action} RETURN count(node)"
    affected = 0
    for chunk in _chunks(normalized, batch_size):
        result = await executor.cypher(
            graph, query, ["affected"], {"rows": chunk}, max_rows=1
        )
        if len(result) != 1 or not isinstance(result[0].get("affected"), int):
            raise RuntimeError("bulk vertex AGE privo di conteggio affidabile")
        affected += result[0]["affected"]
    return affected


async def abulk_edges(
    executor: Any,
    graph: str,
    label: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    start_label: str,
    start_key: str,
    end_label: str,
    end_key: str,
    start_value_field: str = "start",
    end_value_field: str = "end",
    batch_size: int = 100,
) -> int:
    """Variante async di :func:`bulk_edges`."""

    _identifier(graph, "nome graph")
    for value, role in (
        (label, "label edge"),
        (start_label, "label vertex iniziale"),
        (start_key, "chiave vertex iniziale"),
        (end_label, "label vertex finale"),
        (end_key, "chiave vertex finale"),
        (start_value_field, "campo endpoint iniziale"),
        (end_value_field, "campo endpoint finale"),
    ):
        _identifier(value, role)
    normalized = _normalized_rows(rows)
    if not normalized:
        return 0
    required = {start_value_field, end_value_field}
    if not required <= set(normalized[0]):
        raise ValueError("il batch edge non contiene entrambi gli endpoint")
    property_names = tuple(name for name in normalized[0] if name not in required)
    properties = (
        " {"
        + ", ".join(
            f"{_cypher_identifier(name)}: {_row_value(name)}" for name in property_names
        )
        + "}"
        if property_names
        else ""
    )
    start_label_sql = _cypher_identifier(start_label)
    start_key_sql = _cypher_identifier(start_key)
    end_label_sql = _cypher_identifier(end_label)
    end_key_sql = _cypher_identifier(end_key)
    label_sql = _cypher_identifier(label)
    query = (
        f"UNWIND $rows AS row "
        f"MATCH (source_node:{start_label_sql}) "
        f"WHERE source_node.{start_key_sql} = {_row_value(start_value_field)} "
        f"MATCH (target_node:{end_label_sql}) "
        f"WHERE target_node.{end_key_sql} = {_row_value(end_value_field)} "
        f"CREATE (source_node)-[edge:{label_sql}{properties}]->(target_node) "
        "RETURN count(edge)"
    )
    affected = 0
    for chunk in _chunks(normalized, batch_size):
        result = await executor.cypher(
            graph, query, ["affected"], {"rows": chunk}, max_rows=1
        )
        if len(result) != 1 or not isinstance(result[0].get("affected"), int):
            raise RuntimeError("bulk edge AGE privo di conteggio affidabile")
        affected += result[0]["affected"]
    return affected


def graph_property_index_sql(
    graph: str,
    label: str,
    property_name: str,
    index_name: str,
    *,
    unique: bool = False,
) -> str:
    """Rende il DDL PostgreSQL dell'indice su una proprieta agtype AGE."""

    for value, role in (
        (graph, "nome graph"),
        (label, "label graph"),
        (property_name, "proprieta graph"),
        (index_name, "nome indice graph"),
    ):
        _identifier(value, role)
    uniqueness = "UNIQUE " if unique else ""
    agtype_key = f'"{property_name}"'
    return (
        f'CREATE {uniqueness}INDEX "{index_name}" ON "{graph}"."{label}" '
        "USING btree (ag_catalog.agtype_access_operator("
        f"VARIADIC ARRAY[properties, '{agtype_key}'::ag_catalog.agtype]))"
    )


def graph_unique_constraint_sql(
    graph: str,
    label: str,
    property_name: str,
    constraint_name: str,
) -> str:
    """Rende il vincolo univoco AGE come indice expression PostgreSQL."""

    return graph_property_index_sql(
        graph,
        label,
        property_name,
        constraint_name,
        unique=True,
    )


def _decode_value(encoded: dict[str, Any]) -> GraphValue:
    kind = encoded["type"]
    if kind == "null":
        return None
    value = encoded.get("value")
    if kind in {"bool", "integer", "float", "string"}:
        return value
    if kind == "numeric":
        return Decimal(value)
    if kind == "list":
        return [_decode_value(item) for item in value]
    if kind == "map":
        return {key: _decode_value(item) for key, item in value.items()}
    if kind == "vertex":
        return Vertex(
            id=value["id"],
            label=value["label"],
            properties={
                key: _decode_value(item) for key, item in value["properties"].items()
            },
        )
    if kind == "edge":
        return Edge(
            id=value["id"],
            label=value["label"],
            start_id=value["start_id"],
            end_id=value["end_id"],
            properties={
                key: _decode_value(item) for key, item in value["properties"].items()
            },
        )
    if kind == "path":
        return Path(tuple(_decode_value(item) for item in value))
    raise ValueError("tipo agtype non riconosciuto")


def _decode_rows(rows: list[dict[str, dict[str, Any]]]) -> list[dict[str, GraphValue]]:
    return [
        {column: _decode_value(value) for column, value in row.items()} for row in rows
    ]


__all__ = [
    "Edge",
    "GraphValue",
    "Path",
    "Vertex",
    "abulk_edges",
    "abulk_vertices",
    "bulk_edges",
    "bulk_vertices",
    "edge_model",
    "graph_entity_to_model",
    "graph_model_properties",
    "graph_property_index_sql",
    "graph_unique_constraint_sql",
    "vertex_model",
]
