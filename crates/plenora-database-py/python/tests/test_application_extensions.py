"""Estensioni applicative additive previste per la linea 1.2."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import Any, ClassVar

import plenora_database as p

from .test_orm import (
    _AsyncFakeSession,
    _AsyncFakeTransaction,
    _FakeSession,
    _FakeTransaction,
)

registry = p.Registry()


class Base(p.DeclarativeBase):
    __registry__ = registry


class Animal(Base):
    __tablename__ = "inherit_animals"
    __mapper_args__: ClassVar[dict[str, str]] = {
        "polymorphic_on": "kind",
        "polymorphic_identity": "animal",
    }

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    kind: p.Mapped[str] = p.mapped_column(str, nullable=False)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)


class Cat(Animal):
    __mapper_args__: ClassVar[dict[str, str]] = {"polymorphic_identity": "cat"}


class Asset(Base):
    __tablename__ = "inherit_assets"
    __mapper_args__: ClassVar[dict[str, str]] = {
        "polymorphic_on": "kind",
        "polymorphic_identity": "asset",
    }

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    kind: p.Mapped[str] = p.mapped_column(str, nullable=False)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)


class Server(Asset):
    __tablename__ = "inherit_servers"
    __mapper_args__: ClassVar[dict[str, str]] = {
        "inheritance": "joined",
        "polymorphic_identity": "server",
    }

    cores: p.Mapped[int] = p.mapped_column(int, nullable=False)


def test_single_table_inheritance_filters_and_hydrates_subtype() -> None:
    transaction = _FakeTransaction([{"id": 1, "kind": "cat", "name": "Milo"}])
    orm = p.OrmSession(_FakeSession(transaction))

    cat = orm.query(Cat).one()

    assert isinstance(cat, Cat)
    assert transaction.executed[0][1]["orm_polymorphic_identity"] == "cat"
    assert Cat.__table__ is Animal.__table__


def test_joined_table_inheritance_ddl_query_and_insert_are_two_table() -> None:
    ddl = p.OrmMetadata(registry, models=(Asset, Server)).ddl("postgres")
    assert 'CREATE TABLE "inherit_assets"' in ddl[0]
    assert 'CREATE TABLE "inherit_servers"' in ddl[1]
    assert 'REFERENCES "inherit_assets" ("id") ON DELETE CASCADE' in ddl[1]

    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    server = Server(id=7, name="api", cores=8)
    orm.add(server)
    orm.flush()

    assert server.kind == "server"
    assert [item[0].target.name for item in transaction.executed] == [
        "inherit_assets",
        "inherit_servers",
    ]
    query = orm.query(Server)
    assert query._statement.source.name == "inherit_assets"
    assert query._statement.joins[0].table.name == "inherit_servers"


def test_typed_cypher_builder_binds_values_and_rejects_missing_parameters() -> None:
    node = p.GraphNode("person", "Person")
    query = (
        p.graph_query("people")
        .match(node)
        .where(node.property("email").equals("email"))
        .returning(node, names=("person",))
        .limit(1)
    )
    cypher, columns = query.compile()
    assert "$email" in cypher
    assert "secret@example.test" not in cypher
    assert columns == ["person"]

    class Executor:
        def cypher(self, graph, text, names, params, *, max_rows):
            assert params == {"email": "secret@example.test"}
            return []

    assert query.execute(Executor(), {"email": "secret@example.test"}) == []


def test_graph_schema_diff_uses_measured_graphs_and_admin_capability() -> None:
    desired = p.GraphSchema(
        "people",
        ("Person",),
        indexes=(p.GraphIndex("Person", "email", "people_email", unique=True),),
    )
    diff = p.compare_graph_schema(desired, observed_graphs=())
    assert [item.kind for item in diff.operations] == ["create-graph", "create-index"]

    class Session:
        age_admin_capabilities: ClassVar[dict[str, bool]] = {"create_graph": True}

        def __init__(self) -> None:
            self.calls: list[str] = []

        def create_graph(self, name: str) -> None:
            self.calls.append(f"graph:{name}")

        def execute_ddl(self, statement: str) -> None:
            self.calls.append(statement)

    session = Session()
    diff.apply(session)
    assert session.calls[0] == "graph:people"
    assert "CREATE UNIQUE INDEX" in session.calls[1]


def test_engine_config_repr_never_contains_credentials() -> None:
    config = p.EngineConfig.from_url(
        "mysql://marco:super-secret@db.internal:3306/app?tls_mode=require"
    )
    assert config.provider == "mysql"
    assert config.password == "super-secret"
    assert "super-secret" not in repr(config)
    assert "marco" not in repr(config)


def test_explain_and_probe_are_structured() -> None:
    class Session:
        capabilities: ClassVar[dict[str, str]] = {"provider": "postgres"}

        def execute_returning_rows(self, statement: str, params: list | None):
            assert statement.startswith("EXPLAIN (FORMAT JSON")
            return [{"Plan Rows": 42}]

    plan = p.explain(Session(), "SELECT * FROM users")
    assert plan.provider == "postgres"
    assert plan.estimated_rows == 42

    class Engine:
        provider_kind = "postgres"
        is_disposed = False

        @staticmethod
        def statistics() -> dict:
            return {"sessions": 0}

    assert p.probe_engine(Engine()).healthy

    Session.capabilities = {"provider": "db2"}
    try:
        p.explain(Session(), "SELECT * FROM users")
    except ValueError as error:
        assert "non qualificato" in str(error)
    else:
        raise AssertionError("Db2 non deve dichiarare EXPLAIN senza prova live")


def test_asgi_dependency_and_telemetry_do_not_capture_payload() -> None:
    events: list[tuple[str, Any]] = []

    class Span:
        def set_attribute(self, name: str, value: Any) -> None:
            events.append((name, value))

    class Tracer:
        @contextmanager
        def start_as_current_span(self, name: str):
            events.append(("span", name))
            yield Span()

    class Session:
        def execute(self, statement: str, params: list[str]) -> int:
            return 1

    class Engine:
        provider_kind = "postgres"

        @staticmethod
        def session() -> Session:
            return Session()

    wrapped = p.instrument_engine(Engine(), tracer=Tracer())
    assert wrapped.session().execute("SELECT secret", ["payload-secret"]) == 1
    rendered = repr(events)
    assert "SELECT secret" not in rendered
    assert "payload-secret" not in rendered


def test_async_joined_graph_and_asgi_lifecycle() -> None:
    async def scenario() -> None:
        transaction = _AsyncFakeTransaction()
        orm = p.AsyncOrmSession(_AsyncFakeSession(transaction))
        server = Server(id=9, name="worker", cores=4)
        orm.add(server)
        await orm.flush()
        assert [item[0].target.name for item in transaction.executed] == [
            "inherit_assets",
            "inherit_servers",
        ]

        desired = p.GraphSchema("people", ("Person",))
        diff = p.compare_graph_schema(desired, observed_graphs=())

        class GraphSession:
            async def age_admin_capabilities(self) -> dict[str, bool]:
                return {"create_graph": True}

            async def create_graph(self, name: str) -> None:
                assert name == "people"

        assert await diff.apply_async(GraphSession()) == ("create-graph",)

        lifecycle: list[str] = []

        class RequestSession:
            async def __aenter__(self):
                lifecycle.append("enter")
                return self

            async def __aexit__(self, *_args: object) -> bool:
                lifecycle.append("exit")
                return False

        class RequestEngine:
            @staticmethod
            def session() -> RequestSession:
                return RequestSession()

        async def app(scope, receive, send) -> None:
            assert "database" in scope["state"]
            lifecycle.append("app")

        middleware = p.DatabaseASGIMiddleware(app, RequestEngine())
        await middleware({"type": "http"}, None, None)
        assert lifecycle == ["enter", "app", "exit"]

    asyncio.run(scenario())
