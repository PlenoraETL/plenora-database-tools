from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

import plenora_database as p
import pytest
from plenora_database._async_session import AsyncSession
from plenora_database._session import Session
from plenora_database.graph import Edge, Path, Vertex, _decode_rows

from ._harness import aconnect_age, connect_age


def test_graph_decoder_preserves_age_types() -> None:
    encoded = [
        {
            "path": {
                "type": "path",
                "value": [
                    {
                        "type": "vertex",
                        "value": {
                            "id": 1,
                            "label": "Person",
                            "properties": {
                                "name": {"type": "string", "value": "Alice"}
                            },
                        },
                    },
                    {
                        "type": "edge",
                        "value": {
                            "id": 2,
                            "label": "KNOWS",
                            "start_id": 1,
                            "end_id": 3,
                            "properties": {},
                        },
                    },
                ],
            },
            "amount": {"type": "numeric", "value": "1.25"},
        }
    ]
    decoded = _decode_rows(encoded)
    assert decoded[0]["amount"] == Decimal("1.25")
    path = decoded[0]["path"]
    assert isinstance(path, Path)
    assert isinstance(path.elements[0], Vertex)
    assert isinstance(path.elements[1], Edge)


@p.vertex_model("Person", id_field="graph_id")
@dataclass(frozen=True)
class PersonModel:
    graph_id: int
    external_id: str
    name: str


@p.edge_model("KNOWS", id_field="graph_id")
@dataclass(frozen=True)
class KnowsModel:
    graph_id: int
    start_id: int
    end_id: int
    since: int


def test_graph_model_mapping_is_typed_and_fail_closed() -> None:
    person = p.graph_entity_to_model(
        Vertex(7, "Person", {"external_id": "a", "name": "Ada"}),
        PersonModel,
    )
    assert person == PersonModel(7, "a", "Ada")
    assert p.graph_model_properties(person) == {"external_id": "a", "name": "Ada"}
    knows = p.graph_entity_to_model(Edge(9, "KNOWS", 7, 8, {"since": 2026}), KnowsModel)
    assert knows == KnowsModel(9, 7, 8, 2026)
    assert p.graph_model_properties(knows) == {"since": 2026}
    with pytest.raises(TypeError, match="campi non mappati"):
        p.graph_entity_to_model(
            Vertex(7, "Person", {"external_id": "a", "name": "Ada", "secret": 1}),
            PersonModel,
        )


class _BulkExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def cypher(self, graph, query, columns, params, *, max_rows):
        self.calls.append((graph, query, columns, params, max_rows))
        return [{"affected": len(params["rows"])}]


def test_graph_bulk_builders_bind_payload_chunk_and_validate_identifiers() -> None:
    executor = _BulkExecutor()
    assert (
        p.bulk_vertices(
            executor,
            "people",
            "Person",
            [
                {"external_id": "private-a", "name": "Ada"},
                {"external_id": "private-b", "name": "Grace"},
            ],
            merge_key="external_id",
            batch_size=1,
        )
        == 2
    )
    assert len(executor.calls) == 2
    assert all("private-" not in call[1] for call in executor.calls)
    assert "UNWIND $rows" in executor.calls[0][1]
    assert "MERGE (node:`Person`" in executor.calls[0][1]

    assert (
        p.bulk_edges(
            executor,
            "people",
            "KNOWS",
            [{"start": "private-a", "end": "private-b", "since": 2026}],
            start_label="Person",
            start_key="external_id",
            end_label="Person",
            end_key="external_id",
        )
        == 1
    )
    assert "CREATE (source_node)-[edge:`KNOWS`" in executor.calls[-1][1]
    assert 'row["end"]' in executor.calls[-1][1]
    with pytest.raises(ValueError, match="identificatore"):
        p.bulk_vertices(executor, "people", "Person) MATCH", [{"id": 1}])


def test_graph_property_index_sql_uses_qualified_agtype_access_without_payload() -> (
    None
):
    statement = p.graph_property_index_sql(
        "people", "Person", "external_id", "person_external_id_uq", unique=True
    )
    assert statement.startswith(
        'CREATE UNIQUE INDEX "person_external_id_uq" ON "people"."Person"'
    )
    assert "ag_catalog.agtype_access_operator" in statement
    assert "::ag_catalog.agtype" in statement


class _AsyncBulkExecutor:
    async def cypher(self, graph, query, columns, params, *, max_rows):
        return [{"affected": len(params["rows"])}]


@pytest.mark.asyncio
async def test_async_graph_bulk_surface_matches_sync_counts() -> None:
    executor = _AsyncBulkExecutor()
    assert (
        await p.abulk_vertices(
            executor,
            "people",
            "Person",
            [{"external_id": "a", "name": "Ada"}],
            merge_key="external_id",
        )
        == 1
    )
    assert (
        await p.abulk_edges(
            executor,
            "people",
            "KNOWS",
            [{"start": "a", "end": "b"}],
            start_label="Person",
            start_key="external_id",
            end_label="Person",
            end_key="external_id",
        )
        == 1
    )


class _SyncNative:
    age_admin_capabilities: ClassVar[dict[str, bool | int]] = {
        "schema_version": 1,
        "list_graphs": True,
        "create_graph": True,
        "drop_graph": True,
    }

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def list_graphs(self) -> list[str]:
        return ["alpha"]

    def create_graph(self, graph: str) -> None:
        self.calls.append(("create", graph))

    def drop_graph(self, graph: str, *, cascade: bool) -> None:
        self.calls.append(("drop", graph, cascade))

    def cypher(self, *args, **kwargs):
        self.calls.append(("cypher", args, kwargs))
        return []


def test_sync_graph_api_forwards_admin_and_row_limit() -> None:
    native = _SyncNative()
    session = Session(native)
    assert session.age_admin_capabilities["create_graph"] is True
    assert session.list_graphs() == ["alpha"]
    session.create_graph("beta")
    session.drop_graph("beta", cascade=True)
    session.cypher("alpha", "RETURN 1", ["one"], max_rows=7)
    assert native.calls == [
        ("create", "beta"),
        ("drop", "beta", True),
        ("cypher", ("alpha", "RETURN 1", ["one"], None), {"max_rows": 7}),
    ]


class _AsyncNative:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def age_admin_capabilities(self) -> dict:
        return {"schema_version": 1, "create_graph": True}

    async def list_graphs(self) -> list[str]:
        return ["alpha"]

    async def create_graph(self, graph: str) -> None:
        self.calls.append(("create", graph))

    async def drop_graph(self, graph: str, *, cascade: bool) -> None:
        self.calls.append(("drop", graph, cascade))

    async def cypher(self, *args, **kwargs):
        self.calls.append(("cypher", args, kwargs))
        return []


@pytest.mark.asyncio
async def test_async_graph_api_forwards_admin_and_row_limit() -> None:
    native = _AsyncNative()
    session = AsyncSession(native)
    assert (await session.age_admin_capabilities())["create_graph"] is True
    assert await session.list_graphs() == ["alpha"]
    await session.create_graph("beta")
    await session.drop_graph("beta", cascade=True)
    await session.cypher("alpha", "RETURN 1", ["one"], max_rows=7)
    assert native.calls == [
        ("create", "beta"),
        ("drop", "beta", True),
        ("cypher", ("alpha", "RETURN 1", ["one"], None), {"max_rows": 7}),
    ]


def test_live_sync_age_lifecycle_cypher_and_bounds() -> None:
    graph = "plenora_python_sync"
    with connect_age() as session:
        assert session.age_version == "1.7.0"
        assert session.age_capabilities["query"] is True
        assert session.age_admin_capabilities["create_graph"] is True
        if graph in session.list_graphs():
            session.drop_graph(graph, cascade=True)
        session.create_graph(graph)
        try:
            assert graph in session.list_graphs()
            rows = session.cypher(
                graph,
                "UNWIND $values AS value RETURN value ORDER BY value",
                ["value"],
                {"values": [3, 1, 2]},
            )
            assert [row["value"] for row in rows] == [1, 2, 3]
            with pytest.raises(Exception, match="limite righe"):
                session.cypher(
                    graph,
                    "UNWIND [1, 2] AS value RETURN value",
                    ["value"],
                    max_rows=1,
                )
        finally:
            session.drop_graph(graph, cascade=True)


@pytest.mark.asyncio
async def test_live_async_age_lifecycle_and_typed_results() -> None:
    graph = "plenora_python_async"
    async with await aconnect_age() as session:
        capabilities = await session.age_admin_capabilities()
        assert capabilities["drop_graph"] is True
        if graph in await session.list_graphs():
            await session.drop_graph(graph, cascade=True)
        await session.create_graph(graph)
        try:
            rows = await session.cypher(
                graph,
                "CREATE (a:Person {name: $name}) RETURN a",
                ["person"],
                {"name": "Async"},
            )
            assert isinstance(rows[0]["person"], Vertex)
            assert rows[0]["person"].properties["name"] == "Async"
        finally:
            await session.drop_graph(graph, cascade=True)


def test_live_age_bulk_edges_mapping_and_unique_property_index() -> None:
    graph = "plenora_python_bulk"
    with connect_age() as session:
        if graph in session.list_graphs():
            session.drop_graph(graph, cascade=True)
        session.create_graph(graph)
        try:
            assert (
                p.bulk_vertices(
                    session,
                    graph,
                    "Person",
                    [
                        {"external_id": "a", "name": "Ada"},
                        {"external_id": "b", "name": "Grace"},
                    ],
                    merge_key="external_id",
                )
                == 2
            )
            session.execute_ddl(
                p.graph_unique_constraint_sql(
                    graph,
                    "Person",
                    "external_id",
                    "person_external_id_uq",
                )
            )
            with pytest.raises(p.PlenoraError):
                p.bulk_vertices(
                    session,
                    graph,
                    "Person",
                    [{"external_id": "a", "name": "duplicate"}],
                )
            assert (
                p.bulk_edges(
                    session,
                    graph,
                    "KNOWS",
                    [{"start": "a", "end": "b", "since": 2026}],
                    start_label="Person",
                    start_key="external_id",
                    end_label="Person",
                    end_key="external_id",
                )
                == 1
            )
            rows = session.cypher(
                graph,
                "MATCH (a:Person)-[edge:KNOWS]->(b:Person) RETURN a, edge, b",
                ["start", "edge", "end"],
            )
            assert len(rows) == 1
            assert p.graph_entity_to_model(rows[0]["start"], PersonModel).name == "Ada"
            assert p.graph_entity_to_model(rows[0]["edge"], KnowsModel).since == 2026
        finally:
            session.drop_graph(graph, cascade=True)
