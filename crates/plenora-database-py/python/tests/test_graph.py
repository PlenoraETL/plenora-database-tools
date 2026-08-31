from decimal import Decimal

import pytest

from plenora_database._async_session import AsyncSession
from plenora_database._session import Session
from plenora_database.graph import Edge, Path, Vertex, _decode_rows

from ._harness import aconnect_age, connect_age


def test_graph_decoder_preserves_age_types() -> None:
    encoded = [{
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
    }]
    decoded = _decode_rows(encoded)
    assert decoded[0]["amount"] == Decimal("1.25")
    path = decoded[0]["path"]
    assert isinstance(path, Path)
    assert isinstance(path.elements[0], Vertex)
    assert isinstance(path.elements[1], Edge)


class _SyncNative:
    age_admin_capabilities = {
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
