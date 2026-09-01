"""Contratto ORM offline e qualifiche live per mapping, query e UoW."""

from __future__ import annotations

import struct
from typing import ClassVar

import plenora_database as p
import pytest
from plenora_database.result import Result

from ._harness import (
    aconnect_postgres,
    connect_db2_reference,
    connect_mariadb_reference,
    connect_mysql_reference,
    connect_postgres,
    connect_sqlserver_reference,
)


class Account(p.DeclarativeBase):
    __tablename__ = "orm_accounts"
    __schema__ = "app"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    name: p.Mapped[str] = p.mapped_column(nullable=False)
    version: p.Mapped[int] = p.mapped_column(version=True)


class Place(p.DeclarativeBase):
    __tablename__ = "orm_places"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    shape: p.Mapped[p.SpatialReference] = p.mapped_column(
        p.Geometry(srid=4326), nullable=False
    )


class GeneratedAccount(p.DeclarativeBase):
    __tablename__ = "orm_generated_accounts"

    id: p.Mapped[int] = p.mapped_column(primary_key=True, generated=True)
    name: p.Mapped[str] = p.mapped_column(nullable=False)
    created_by: p.Mapped[str] = p.mapped_column(nullable=False, server_default=True)


class AuditEntry(p.DeclarativeBase):
    __tablename__ = "orm_audit_entries"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    account_id: p.Mapped[int] = p.mapped_column(nullable=False)
    message: p.Mapped[str] = p.mapped_column(nullable=False)
    account: p.Relationship[GeneratedAccount] = p.relationship(
        GeneratedAccount, foreign_key="account_id"
    )


class LiveAccount(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_accounts"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    name: p.Mapped[str] = p.mapped_column(nullable=False)
    version: p.Mapped[int] = p.mapped_column(version=True)


class LiveGeneratedAccount(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_generated_accounts"

    id: p.Mapped[int] = p.mapped_column(primary_key=True, generated=True)
    name: p.Mapped[str] = p.mapped_column(nullable=False)
    created_by: p.Mapped[str] = p.mapped_column(nullable=False, server_default=True)


class LiveAuditEntry(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_audit_entries"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    account_id: p.Mapped[int] = p.mapped_column(nullable=False)
    message: p.Mapped[str] = p.mapped_column(nullable=False)
    account: p.Relationship[LiveGeneratedAccount] = p.relationship(
        LiveGeneratedAccount, foreign_key="account_id"
    )


class LivePlace(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_places"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    shape: p.Mapped[p.SpatialReference] = p.mapped_column(
        p.Geometry(srid=4326), nullable=False
    )


class LiveAsyncPlace(p.DeclarativeBase):
    __tablename__ = "_plenora_async_orm_places"

    id: p.Mapped[int] = p.mapped_column(primary_key=True)
    shape: p.Mapped[p.SpatialReference] = p.mapped_column(
        p.Geometry(srid=4326), nullable=False
    )


_MYSQL_POINT = p.Geometry(srid=4326, geometry_type="point")
_MYSQL_LINESTRING = p.Geometry(srid=4326, geometry_type="linestring")
_MYSQL_POLYGON = p.Geometry(srid=4326, geometry_type="polygon")


class LiveMysqlGeometry(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_mysql_geometry"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    point: p.Mapped[p.SpatialReference] = p.mapped_column(
        _MYSQL_POINT, nullable=False
    )
    line: p.Mapped[p.SpatialReference] = p.mapped_column(
        _MYSQL_LINESTRING, nullable=False
    )
    polygon: p.Mapped[p.SpatialReference] = p.mapped_column(
        _MYSQL_POLYGON, nullable=False
    )
    optional_point: p.Mapped[p.SpatialReference | None] = p.mapped_column(
        _MYSQL_POINT
    )


class LiveMysqlGeometryXyz(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_mysql_geometry_xyz"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    shape: p.Mapped[p.SpatialReference] = p.mapped_column(
        p.Geometry(srid=4326, dimensions="xyz", geometry_type="point"),
        nullable=False,
    )


_PORTABLE_POINT = p.Geometry(srid=4326, geometry_type="point")
_PORTABLE_LINESTRING = p.Geometry(srid=4326, geometry_type="linestring")
_PORTABLE_POLYGON = p.Geometry(srid=4326, geometry_type="polygon")


class LivePortableGeometry(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_portable_geometry"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    point: p.Mapped[p.SpatialReference] = p.mapped_column(
        _PORTABLE_POINT, nullable=False
    )
    line: p.Mapped[p.SpatialReference] = p.mapped_column(
        _PORTABLE_LINESTRING, nullable=False
    )
    polygon: p.Mapped[p.SpatialReference] = p.mapped_column(
        _PORTABLE_POLYGON, nullable=False
    )
    optional_point: p.Mapped[p.SpatialReference | None] = p.mapped_column(
        _PORTABLE_POINT
    )


class LivePortableGeometryXyz(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_portable_geometry_xyz"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    shape: p.Mapped[p.SpatialReference] = p.mapped_column(
        p.Geometry(srid=4326, dimensions="xyz", geometry_type="point"),
        nullable=False,
    )


class LiveSqlServerGeography(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_sqlserver_geography"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    shape: p.Mapped[p.SpatialReference] = p.mapped_column(
        p.Geometry(srid=4326, semantics="geography", geometry_type="point"),
        nullable=False,
    )


class LivePortableGenerated(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_portable_generated"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True, generated=True)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)
    created_by: p.Mapped[str] = p.mapped_column(
        str,
        nullable=False,
        server_default=p.ServerDefault.literal("database"),
    )


class OrmChild(p.DeclarativeBase):
    __tablename__ = "orm_children"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    parent_id: p.Mapped[int] = p.mapped_column(int, nullable=False)
    label: p.Mapped[str] = p.mapped_column(str, nullable=False)
    parent: p.Relationship[OrmParent] = p.relationship(
        "OrmParent", foreign_key="parent_id", back_populates="children"
    )


class OrmParent(p.DeclarativeBase):
    __tablename__ = "orm_parents"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)
    children: p.Relationship[OrmChild] = p.relationship(
        OrmChild,
        foreign_key="parent_id",
        uselist=True,
        back_populates="parent",
        cascade="save-update, delete, delete-orphan",
    )


article_tags = p.table("orm_article_tags", "article_id", "tag_id")


class OrmTag(p.DeclarativeBase):
    __tablename__ = "orm_tags"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    articles: p.Relationship[OrmArticle] = p.relationship(
        "OrmArticle",
        uselist=True,
        back_populates="tags",
        secondary=article_tags,
        secondary_local_key="tag_id",
        secondary_remote_key="article_id",
    )


class OrmArticle(p.DeclarativeBase):
    __tablename__ = "orm_articles"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    tags: p.Relationship[OrmTag] = p.relationship(
        OrmTag,
        uselist=True,
        back_populates="articles",
        cascade="save-update",
        secondary=article_tags,
        secondary_local_key="article_id",
        secondary_remote_key="tag_id",
    )


class CompositeRecord(p.DeclarativeBase):
    __tablename__ = "orm_composite_records"

    tenant_id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    record_id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    label: p.Mapped[str] = p.mapped_column(str, nullable=False)


class ConcreteAccount(Account):
    __tablename__ = "orm_concrete_accounts"
    __mapper_args__: ClassVar[dict[str, bool]] = {"concrete": True}

    category: p.Mapped[str] = p.mapped_column(str, nullable=False)


class CycleLeft(p.DeclarativeBase):
    __tablename__ = "orm_cycle_left"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    right_id: p.Mapped[int] = p.mapped_column(int)
    right: p.Relationship[CycleRight] = p.relationship(
        "CycleRight", foreign_key="right_id"
    )


class CycleRight(p.DeclarativeBase):
    __tablename__ = "orm_cycle_right"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    left_id: p.Mapped[int] = p.mapped_column(int)
    left: p.Relationship[CycleLeft] = p.relationship(CycleLeft, foreign_key="left_id")


class OrmProfile(p.DeclarativeBase):
    __tablename__ = "orm_profiles"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    parent_id: p.Mapped[int] = p.mapped_column(int, nullable=False, unique=True)


class OrmProfileOwner(p.DeclarativeBase):
    __tablename__ = "orm_profile_owners"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    profile: p.Relationship[OrmProfile] = p.relationship(
        OrmProfile, foreign_key="parent_id", cascade="save-update"
    )


def _ewkb_point(x: float, y: float, srid: int = 4326) -> bytes:
    return (
        b"\x01"
        + (0x2000_0001).to_bytes(4, "little")
        + srid.to_bytes(4, "little")
        + struct.pack("<dd", x, y)
    )


def _ewkb_point_xyz(x: float, y: float, z: float, srid: int = 4326) -> bytes:
    return (
        b"\x01"
        + (0xA000_0001).to_bytes(4, "little")
        + srid.to_bytes(4, "little")
        + struct.pack("<ddd", x, y, z)
    )


def _ewkb_linestring(points: tuple[tuple[float, float], ...], srid: int = 4326) -> bytes:
    return (
        b"\x01"
        + (0x2000_0002).to_bytes(4, "little")
        + srid.to_bytes(4, "little")
        + len(points).to_bytes(4, "little")
        + b"".join(struct.pack("<dd", *point) for point in points)
    )


def _ewkb_polygon(
    ring: tuple[tuple[float, float], ...], srid: int = 4326
) -> bytes:
    return (
        b"\x01"
        + (0x2000_0003).to_bytes(4, "little")
        + srid.to_bytes(4, "little")
        + (1).to_bytes(4, "little")
        + len(ring).to_bytes(4, "little")
        + b"".join(struct.pack("<dd", *point) for point in ring)
    )


def _root_wkb(ewkb: bytes) -> bytes:
    type_word = int.from_bytes(ewkb[1:5], "little")
    iso_type = type_word & 0x0FFF_FFFF
    if type_word & 0x8000_0000:
        iso_type += 1_000
    if type_word & 0x4000_0000:
        iso_type += 2_000
    offset = 9 if type_word & 0x2000_0000 else 5
    return ewkb[:1] + iso_type.to_bytes(4, "little") + ewkb[offset:]


class _FakeTransaction:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = [] if rows is None else rows
        self.executed: list[tuple[object, dict]] = []
        self.affected: list[int] = []
        self.is_active = True
        self.committed = False
        self.rolled_back = False
        self.scalar = 1
        self.result_batches: list[list[dict]] = []

    def execute(self, statement, params=None):
        parameters = {} if params is None else dict(params)
        self.executed.append((statement, parameters))
        if isinstance(statement, p.SelectStatement) or getattr(
            statement, "returning_names", ()
        ):
            return Result(
                self.result_batches.pop(0) if self.result_batches else self.rows
            )
        return self.affected.pop(0) if self.affected else 1

    def commit(self) -> None:
        self.committed = True
        self.is_active = False

    def execute_scalar(self, statement, params=None):
        self.executed.append((statement, {} if params is None else dict(params)))
        return self.scalar

    def rollback(self) -> None:
        self.rolled_back = True
        self.is_active = False


class _FakeSession:
    def __init__(
        self, transaction: _FakeTransaction, provider: str = "postgres"
    ) -> None:
        self.transaction = transaction
        self.begin_count = 0
        self.capabilities = {"provider": provider}

    def begin(self) -> _FakeTransaction:
        self.begin_count += 1
        return self.transaction


class _AsyncFakeTransaction(_FakeTransaction):
    async def execute(self, statement, params=None):
        return super().execute(statement, params)

    async def commit(self) -> None:
        super().commit()

    async def rollback(self) -> None:
        super().rollback()

    async def execute_scalar(self, statement, params=None):
        return super().execute_scalar(statement, params)


class _AsyncFakeSession:
    def __init__(
        self, transaction: _AsyncFakeTransaction, provider: str = "postgres"
    ) -> None:
        self.transaction = transaction
        self.begin_count = 0
        self.capabilities = {"provider": provider}

    async def begin(self) -> _AsyncFakeTransaction:
        self.begin_count += 1
        return self.transaction


def test_declarative_mapping_reuses_canonical_table_and_columns() -> None:
    assert Account.__table__.schema == "app"
    assert Account.__table__.name == "orm_accounts"
    assert Account.id is Account.__table__.c.id
    assert Account.name is Account.__table__.c.name
    assert Account.__mapper__.primary_key.name == "id"
    assert Account.__mapper__.version.name == "version"
    assert isinstance(Account.id == p.bind("identity"), p.Predicate)


def test_add_flush_update_delete_tracks_state_and_version() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    account = Account(id=7, name="Ada")

    assert p.inspect_instance(account).state is p.ObjectState.TRANSIENT
    orm.add(account)
    assert p.inspect_instance(account).state is p.ObjectState.PENDING
    orm.flush()
    assert p.inspect_instance(account).state is p.ObjectState.PERSISTENT
    assert account.version == 1

    account.name = "Grace"
    assert p.inspect_instance(account).dirty == ("name",)
    orm.flush()
    assert account.version == 2
    assert p.inspect_instance(account).dirty == ()

    orm.delete(account)
    assert p.inspect_instance(account).state is p.ObjectState.DELETED
    orm.flush()
    assert p.inspect_instance(account).state is p.ObjectState.DELETED
    orm.commit()
    assert p.inspect_instance(account).state is p.ObjectState.DETACHED
    assert transaction.committed

    kinds = [type(statement) for statement, _ in transaction.executed]
    assert kinds == [p.InsertStatement, p.UpdateStatement, p.DeleteStatement]


def test_get_populates_identity_map_and_avoids_second_query() -> None:
    transaction = _FakeTransaction([{"id": 11, "name": "Lin", "version": 4}])
    orm = p.OrmSession(_FakeSession(transaction))

    first = orm.get(Account, 11)
    second = orm.get(Account, 11)

    assert first is second
    assert first is not None and first.name == "Lin"
    assert p.inspect_instance(first).state is p.ObjectState.PERSISTENT
    assert len(transaction.executed) == 1
    orm.rollback()
    assert p.inspect_instance(first).state is p.ObjectState.DETACHED


def test_entity_query_hydrates_models_and_reuses_identity_map() -> None:
    transaction = _FakeTransaction(
        [
            {"id": 11, "name": "Lin", "version": 4},
            {"id": 12, "name": "Ada", "version": 2},
        ]
    )
    orm = p.OrmSession(_FakeSession(transaction))

    rows = (
        orm.query(Account)
        .where(Account.name == p.bind("wanted"))
        .order_by(Account.id)
        .all({"wanted": "Lin"})
    )

    assert [row.id for row in rows] == [11, 12]
    assert orm.get(Account, 11) is rows[0]
    assert len(transaction.executed) == 1
    statement, parameters = transaction.executed[0]
    assert isinstance(statement, p.SelectStatement)
    assert parameters == {"wanted": "Lin"}


def test_refresh_discards_dirty_values_and_rollback_keeps_entry_snapshot() -> None:
    transaction = _FakeTransaction([{"id": 21, "name": "before", "version": 3}])
    orm = p.OrmSession(_FakeSession(transaction))
    account = orm.get(Account, 21)
    assert account is not None
    account.name = "local"
    transaction.rows = [{"id": 21, "name": "server", "version": 4}]

    orm.refresh(account)

    assert account.name == "server"
    assert account.version == 4
    assert p.inspect_instance(account).dirty == ()
    orm.rollback()
    assert account.name == "before"
    assert account.version == 3


def test_optimistic_mismatch_rolls_back_without_exposing_values() -> None:
    transaction = _FakeTransaction([{"id": 13, "name": "before", "version": 1}])
    transaction.affected[:] = [0]
    orm = p.OrmSession(_FakeSession(transaction))
    account = orm.get(Account, 13)
    assert account is not None
    account.name = "private-value-orm-72"

    with pytest.raises(p.StaleObjectError) as error:
        orm.commit()

    assert "private-value-orm-72" not in str(error.value)
    assert transaction.rolled_back
    assert account.name == "before"
    assert p.inspect_instance(account).state is p.ObjectState.DETACHED


def test_rollback_after_successful_flush_restores_entry_snapshot() -> None:
    transaction = _FakeTransaction([{"id": 17, "name": "before", "version": 3}])
    orm = p.OrmSession(_FakeSession(transaction))
    account = orm.get(Account, 17)
    assert account is not None
    account.name = "after"

    orm.flush()
    assert account.name == "after"
    assert account.version == 4
    orm.rollback()

    assert account.name == "before"
    assert account.version == 3
    assert p.inspect_instance(account).state is p.ObjectState.DETACHED


def test_geometry_mapping_uses_canonical_ewkb_and_unqualified_providers_fail_closed() -> None:
    with pytest.raises(ValueError, match="geometry_type"):
        p.Geometry(srid=4326, geometry_type="not-a-geometry")

    declared = p.SpatialReference(
        b"not-inspected-because-crs-mismatches", 3857, "xy", "geometry"
    )
    with pytest.raises(ValueError, match="incompatibile"):
        Place(id=1, shape=declared)

    ewkb = _ewkb_point(9.19, 45.46)
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    place = Place(id=1, shape=ewkb)
    orm.add(place)
    orm.flush()

    statement, parameters = transaction.executed[0]
    assert statement.to_ast()["rows"][0][1]["kind"] == "spatial_value"
    assert set(parameters.values()) == {1, ewkb}
    assert place.shape.ewkb == ewkb

    read_transaction = _FakeTransaction([{"id": 2, "shape": ewkb}])
    reader = p.OrmSession(_FakeSession(read_transaction))
    loaded = reader.get(Place, 2)
    assert loaded is not None and loaded.shape.ewkb == ewkb
    projection = read_transaction.executed[0][0].to_ast()["projection"][1]
    assert projection["expression"]["kind"] == "spatial_output"

    for provider in ("mysql", "mariadb"):
        mysql_transaction = _FakeTransaction()
        mysql = p.OrmSession(_FakeSession(mysql_transaction, provider=provider))
        mysql.add(Place(id=3, shape=ewkb))
        mysql.flush()
        _, parameters = mysql_transaction.executed[0]
        assert _root_wkb(ewkb) in parameters.values()

        mysql_read = _FakeTransaction(
            [
                {
                    "id": 3,
                    "shape": _root_wkb(ewkb),
                    "orm_geometry_srid_shape": 4326,
                }
            ]
        )
        mysql_reader = p.OrmSession(_FakeSession(mysql_read, provider=provider))
        loaded = mysql_reader.get(Place, 3)
        assert loaded is not None and loaded.shape.ewkb == _root_wkb(ewkb)
        projections = mysql_read.executed[0][0].to_ast()["projection"]
        assert projections[2]["alias"] == "orm_geometry_srid_shape"
        predicate = _MYSQL_POINT.predicate(
            "intersects", Place.shape, _MYSQL_POINT.bind("reference")
        )
        mysql_reader.query(Place).where(predicate).all({"reference": ewkb})
        assert mysql_read.executed[-1][1]["reference"] == _root_wkb(ewkb)
        mysql.rollback()
        mysql_reader.rollback()

        missing_srid = p.OrmSession(
            _FakeSession(
                _FakeTransaction([{"id": 4, "shape": _root_wkb(ewkb)}]),
                provider=provider,
            )
        )
        with pytest.raises(p.OrmMappingError, match="frame Geometry"):
            missing_srid.get(Place, 4)
        missing_srid.rollback()

    rejected_transaction = _FakeTransaction()
    rejected = p.OrmSession(
        _FakeSession(rejected_transaction, provider="sqlserver")
    )
    rejected.add(Place(id=4, shape=ewkb))
    with pytest.raises(p.OrmUnsupportedError, match="Geometry ORM"):
        rejected.flush()
    assert rejected_transaction.executed == []

    orm.rollback()
    reader.rollback()


@pytest.mark.parametrize("provider", ("sqlserver", "db2"))
def test_portable_geometry_mapping_frames_sqlserver_and_db2(provider: str) -> None:
    point = _ewkb_point(9.19, 45.46)
    line = _ewkb_linestring(((9.18, 45.45), (9.20, 45.47)))
    polygon = _ewkb_polygon(
        (
            (9.10, 45.40),
            (9.30, 45.40),
            (9.30, 45.55),
            (9.10, 45.55),
            (9.10, 45.40),
        )
    )
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction, provider=provider))
    orm.add(
        LivePortableGeometry(
            id=1,
            point=point,
            line=line,
            polygon=polygon,
            optional_point=None,
        )
    )
    orm.flush()
    _, parameters = transaction.executed[0]
    assert _root_wkb(point) in parameters.values()
    assert _root_wkb(line) in parameters.values()
    assert _root_wkb(polygon) in parameters.values()
    optional_value = parameters["orm_insert_4"]
    if provider == "sqlserver":
        assert optional_value._plenora_typed_kind == "null"
        assert optional_value._plenora_typed_value == {"type_name": "varbinary"}
    else:
        assert optional_value is None
    orm.rollback()

    def encoded(value: bytes) -> bytes | str:
        return value.hex().upper() if provider == "db2" else value

    row = {
        "id": 1,
        "point": encoded(_root_wkb(point)),
        "orm_geometry_srid_point": 4326,
        "line": encoded(_root_wkb(line)),
        "orm_geometry_srid_line": 4326,
        "polygon": encoded(_root_wkb(polygon)),
        "orm_geometry_srid_polygon": 4326,
        "optional_point": None,
        "orm_geometry_srid_optional_point": None,
    }
    read_transaction = _FakeTransaction([row])
    reader = p.OrmSession(_FakeSession(read_transaction, provider=provider))
    loaded = reader.get(LivePortableGeometry, 1)
    assert loaded is not None
    assert loaded.point.ewkb == _root_wkb(point)
    assert loaded.line.ewkb == _root_wkb(line)
    assert loaded.polygon.ewkb == _root_wkb(polygon)
    assert loaded.optional_point is None
    assert loaded.point.srid == 4326
    reader.rollback()

    if provider == "db2":
        invalid_row = dict(row, point="WKB-non-esadecimale")
        invalid_reader = p.OrmSession(
            _FakeSession(_FakeTransaction([invalid_row]), provider=provider)
        )
        with pytest.raises(p.OrmMappingError, match="WKB Geometry ORM Db2"):
            invalid_reader.get(LivePortableGeometry, 1)
        invalid_reader.rollback()


@pytest.mark.parametrize("provider", ("sqlserver", "db2"))
def test_portable_geometry_keeps_unqualified_types_closed(provider: str) -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction, provider=provider))
    orm.add(Place(id=1, shape=_ewkb_point(9.19, 45.46)))
    with pytest.raises(p.OrmUnsupportedError, match="tipo Geometry ORM"):
        orm.flush()
    assert transaction.executed == []


@pytest.mark.parametrize(
    "namespace",
    ({"__tablename__": "missing_pk", "value": p.mapped_column()},),
)
def test_mapping_requires_a_primary_key(namespace: dict) -> None:
    with pytest.raises(p.OrmMappingError, match="almeno una chiave"):
        type("InvalidModel", (p.DeclarativeBase,), namespace)


def test_postgres_generated_key_and_server_default_are_hydrated() -> None:
    transaction = _FakeTransaction([{"id": 71, "created_by": "database"}])
    orm = p.OrmSession(_FakeSession(transaction))
    account = GeneratedAccount(name="Ada")
    orm.add(account)

    orm.flush()

    assert account.id == 71
    assert account.created_by == "database"
    assert p.inspect_instance(account).identity == (71,)
    statement, parameters = transaction.executed[0]
    assert statement.returning_names == ("id", "created_by")
    assert set(parameters.values()) == {"Ada"}


def test_mysql_generated_key_uses_identity_then_reads_defaults() -> None:
    transaction = _FakeTransaction([{"id": 73, "created_by": "database"}])
    transaction.scalar = 73
    orm = p.OrmSession(_FakeSession(transaction, provider="mysql"))
    account = GeneratedAccount(name="Ada")
    orm.add(account)

    orm.flush()

    assert account.id == 73
    assert account.created_by == "database"
    assert len(transaction.executed) == 3


def test_db2_generated_key_casts_identity_to_the_mapped_integer() -> None:
    transaction = _FakeTransaction([{"id": 73, "created_by": "database"}])
    transaction.scalar = 73
    orm = p.OrmSession(_FakeSession(transaction, provider="db2"))
    account = GeneratedAccount(name="Ada")
    orm.add(account)

    orm.flush()

    assert account.id == 73
    assert transaction.executed[1][0] == (
        "VALUES CAST(IDENTITY_VAL_LOCAL() AS INTEGER)"
    )


@pytest.mark.asyncio
async def test_async_db2_generated_key_uses_the_same_typed_identity() -> None:
    transaction = _AsyncFakeTransaction([{"id": 74, "created_by": "database"}])
    transaction.scalar = 74
    orm = p.AsyncOrmSession(_AsyncFakeSession(transaction, provider="db2"))
    account = GeneratedAccount(name="Ada")
    orm.add(account)

    await orm.flush()

    assert account.id == 74
    assert transaction.executed[1][0] == (
        "VALUES CAST(IDENTITY_VAL_LOCAL() AS INTEGER)"
    )


def test_relationship_orders_parent_before_child_and_propagates_generated_key() -> None:
    transaction = _FakeTransaction([{"id": 81, "created_by": "database"}])
    orm = p.OrmSession(_FakeSession(transaction))
    parent = GeneratedAccount(name="Ada")
    child = AuditEntry(id=1, message="created", account=parent)
    orm.add(child)
    orm.add(parent)

    orm.flush()

    assert child.account_id == 81
    assert child.account is parent
    targets = [statement.target.name for statement, _ in transaction.executed]
    assert targets == ["orm_generated_accounts", "orm_audit_entries"]


def test_relationship_does_not_cascade_a_transient_parent() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    child = AuditEntry(
        id=1,
        message="created",
        account=GeneratedAccount(name="not-added"),
    )
    orm.add(child)

    with pytest.raises(p.OrmStateError, match="non aggiunto"):
        orm.flush()

    assert transaction.executed == []


def test_relationship_load_is_explicit_and_reuses_get() -> None:
    transaction = _FakeTransaction([{"id": 1, "account_id": 81, "message": "created"}])
    orm = p.OrmSession(_FakeSession(transaction))
    audit = orm.get(AuditEntry, 1)
    assert audit is not None
    with pytest.raises(p.OrmStateError, match="non caricata"):
        _ = audit.account
    transaction.rows = [{"id": 81, "name": "Ada", "created_by": "database"}]

    account = orm.load(audit, AuditEntry.account)

    assert account is not None
    assert audit.account is account
    assert account.id == 81
    assert len(transaction.executed) == 2


def test_collection_back_populates_cascade_and_orphan_delete() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    child = OrmChild(id=2, label="child")
    parent = OrmParent(id=1, name="parent", children=[child])

    orm.add(parent)
    orm.flush()

    assert child.parent is parent
    assert child.parent_id == 1
    assert [statement.target.name for statement, _ in transaction.executed] == [
        "orm_parents",
        "orm_children",
    ]

    parent.children.remove(child)
    assert p.inspect_instance(child).state is p.ObjectState.DELETED
    orm.flush()
    assert isinstance(transaction.executed[-1][0], p.DeleteStatement)


def test_relationship_changes_restore_on_rollback_and_late_cascade_adds() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    child = OrmChild(id=2, label="child")
    parent = OrmParent(id=1, name="parent", children=[child])
    orm.add(parent)
    orm.flush()

    parent.children.remove(child)
    orm.rollback()

    assert list(parent.children) == [child]
    assert child.parent is parent
    assert child.parent_id == 1
    assert p.inspect_instance(parent).state is p.ObjectState.TRANSIENT
    assert p.inspect_instance(child).state is p.ObjectState.TRANSIENT

    second = _FakeTransaction()
    second_orm = p.OrmSession(_FakeSession(second))
    owner = OrmProfileOwner(id=10)
    profile = OrmProfile(id=11)
    second_orm.add(owner)
    owner.profile = profile
    second_orm.flush()
    assert profile.parent_id == 10
    assert [statement.target.name for statement, _ in second.executed] == [
        "orm_profile_owners",
        "orm_profiles",
    ]


def test_selectinload_batches_collection_and_sets_backref() -> None:
    transaction = _FakeTransaction()
    transaction.result_batches = [
        [{"id": 1, "name": "parent"}],
        [{"id": 2, "parent_id": 1, "label": "child"}],
    ]
    orm = p.OrmSession(_FakeSession(transaction), autoflush=False)

    parents = orm.query(OrmParent).options(p.selectinload("children")).all()

    assert len(transaction.executed) == 2
    assert [item.label for item in parents[0].children] == ["child"]
    assert parents[0].children[0].parent is parents[0]


def test_many_to_many_flushes_each_association_once() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    tag = OrmTag(id=4)
    article = OrmArticle(id=3, tags=[tag])

    orm.add(article)
    orm.flush()

    assert list(tag.articles) == [article]
    targets = [statement.target.name for statement, _ in transaction.executed]
    assert targets == ["orm_articles", "orm_tags", "orm_article_tags"]


def test_query_join_eager_projection_and_multiple_entities() -> None:
    transaction = _FakeTransaction(
        [
            {
                "id": 2,
                "parent_id": 1,
                "label": "child",
                "orm_eager_parent_id": 1,
                "orm_eager_parent_name": "parent",
            }
        ]
    )
    orm = p.OrmSession(_FakeSession(transaction), autoflush=False)

    child = (
        orm.query(OrmChild)
        .join(OrmChild.parent)
        .options(p.joinedload(OrmChild.parent))
        .one()
    )

    assert child.parent.name == "parent"
    statement = transaction.executed[0][0]
    assert len(statement.joins) == 2

    transaction.rows = [{"label": "child"}]
    values = orm.query(OrmChild).project(OrmChild.label.label("label")).all()
    assert values == [{"label": "child"}]

    transaction.rows = [
        {
            "orm_entity_0_id": 2,
            "orm_entity_0_parent_id": 1,
            "orm_entity_0_label": "child",
            "orm_entity_1_id": 1,
            "orm_entity_1_name": "parent",
        }
    ]
    pair = (
        orm.query(OrmChild)
        .join(OrmChild.parent)
        .with_entities(OrmChild, OrmParent)
        .all()[0]
    )
    assert pair[0] is child
    assert pair[1] is child.parent


def test_composite_identity_concrete_inheritance_and_strong_types() -> None:
    transaction = _FakeTransaction(
        [{"tenant_id": 7, "record_id": 9, "label": "record"}]
    )
    orm = p.OrmSession(_FakeSession(transaction), autoflush=False)

    record = orm.get(CompositeRecord, (7, 9))

    assert record is not None
    assert p.inspect_instance(record).identity == (7, 9)
    assert len(CompositeRecord.__mapper__.primary_keys) == 2
    statement, parameters = transaction.executed[0]
    assert statement.predicate is not None
    assert set(parameters.values()) == {7, 9}
    concrete = ConcreteAccount(id=1, name="Ada", category="staff")
    assert ConcreteAccount.__mapper__.inherits is Account.__mapper__
    assert concrete.category == "staff"
    with pytest.raises(TypeError, match="tipo Python"):
        CompositeRecord(tenant_id="wrong", record_id=1, label="record")
    with pytest.raises(ValueError, match="SQL INTEGER"):
        CompositeRecord(tenant_id=2**31, record_id=1, label="record")


def test_nullable_fk_cycle_uses_two_phase_insert() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    left = CycleLeft(id=1)
    right = CycleRight(id=2)
    left.right = right
    right.left = left
    orm.add(left)
    orm.add(right)

    orm.flush()

    kinds = [type(statement) for statement, _ in transaction.executed]
    assert kinds.count(p.InsertStatement) == 2
    assert kinds.count(p.UpdateStatement) == 1
    assert left.right_id == 2
    assert right.left_id == 1


def test_autoflush_expire_expunge_merge_and_hooks() -> None:
    transaction = _FakeTransaction([{"id": 8, "name": "server", "version": 1}])
    orm = p.OrmSession(_FakeSession(transaction))
    events: list[str] = []
    orm.listen("before_flush", lambda session: events.append("before_flush"))
    orm.listen("after_insert", lambda session, instance: events.append("after_insert"))
    pending = Account(id=7, name="pending")
    orm.add(pending)

    loaded = orm.get(Account, 8)

    assert loaded is not None
    assert isinstance(transaction.executed[0][0], p.InsertStatement)
    assert events == ["before_flush", "after_insert"]
    orm.expire(loaded, "name")
    with pytest.raises(p.OrmStateError, match="scaduto"):
        _ = loaded.name
    transaction.rows = [{"id": 8, "name": "refreshed", "version": 2}]
    orm.refresh(loaded)
    assert loaded.name == "refreshed"
    orm.expunge(loaded)
    loaded.name = "detached"
    transaction.rows = [{"id": 8, "name": "database", "version": 2}]
    merged = orm.merge(loaded)
    assert merged is not loaded
    assert merged.name == "detached"
    assert p.inspect_instance(merged).dirty == ("name",)


def test_ddl_constraints_defaults_and_migration_chain() -> None:
    registry = p.Registry()

    class DdlBase(p.DeclarativeBase):
        __registry__ = registry

    class DdlParent(DdlBase):
        __tablename__ = "ddl_parent"

        tenant: p.Mapped[int] = p.mapped_column(int, primary_key=True)
        code: p.Mapped[str] = p.mapped_column(str, primary_key=True)

    class DdlChild(DdlBase):
        __tablename__ = "ddl_child"
        __table_args__ = (
            p.UniqueConstraint("tenant", "label", name="uq_child_label"),
            p.ForeignKeyConstraint(
                ("tenant", "parent_code"),
                DdlParent,
                ("tenant", "code"),
                on_delete="CASCADE",
            ),
        )

        id: p.Mapped[int] = p.mapped_column(int, primary_key=True, generated=True)
        tenant: p.Mapped[int] = p.mapped_column(int, nullable=False)
        parent_code: p.Mapped[str] = p.mapped_column(str, nullable=False)
        label: p.Mapped[str] = p.mapped_column(str, nullable=False)
        created_at: p.Mapped[str] = p.mapped_column(
            str, nullable=False, server_default=p.ServerDefault.current_timestamp()
        )

    ddl = p.OrmMetadata(registry).ddl("postgres", checkfirst=True)

    assert len(ddl) == 2
    assert "CREATE TABLE IF NOT EXISTS" in ddl[0]
    assert '"tenant" INTEGER' in ddl[0]
    assert "FOREIGN KEY" in ddl[1]
    assert "UNIQUE" in ddl[1]
    assert "DEFAULT CURRENT_TIMESTAMP" in ddl[1]

    class DdlSession:
        capabilities: ClassVar[dict[str, str]] = {"provider": "postgres"}

        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute_ddl(self, statement: str) -> None:
            self.statements.append(statement)

        def execute(self, statement: str) -> None:
            raise AssertionError("il DDL non deve usare execute")

    ddl_session = DdlSession()
    metadata = p.OrmMetadata(registry)
    metadata.create_all(ddl_session)
    metadata.drop_all(ddl_session)
    assert len(ddl_session.statements) == 4
    migrations = (
        p.Migration("001", None, lambda tx: None, lambda tx: None),
        p.Migration("002", "001", lambda tx: None, lambda tx: None),
    )
    runner = p.MigrationRunner(migrations)
    assert [item.revision for item in runner.migrations] == ["001", "002"]
    with pytest.raises(p.OrmMappingError, match="catena lineare"):
        p.MigrationRunner((p.Migration("002", "missing", lambda tx: None),))


def test_migration_runner_applies_and_rolls_back_transactionally() -> None:
    calls: list[str] = []

    class MigrationSession:
        capabilities: ClassVar[dict[str, str]] = {"provider": "postgres"}

        def __init__(self) -> None:
            self.applied: list[dict[str, str]] = []
            self.raw: list[str] = []
            self.transactions: list[_FakeTransaction] = []

        def execute(self, statement: str) -> int:
            self.raw.append(statement)
            return 0

        def execute_returning_rows(self, statement: str) -> list[dict[str, str]]:
            self.raw.append(statement)
            return list(self.applied)

        def begin(self) -> _FakeTransaction:
            transaction = _FakeTransaction()
            self.transactions.append(transaction)
            return transaction

    migrations = (
        p.Migration(
            "001",
            None,
            lambda tx: calls.append("up-001"),
            lambda tx: calls.append("down-001"),
        ),
        p.Migration(
            "002",
            "001",
            lambda tx: calls.append("up-002"),
            lambda tx: calls.append("down-002"),
        ),
    )
    session = MigrationSession()
    runner = p.MigrationRunner(migrations)

    assert runner.apply(session) == ("001", "002")
    assert calls == ["up-001", "up-002"]
    assert all(transaction.committed for transaction in session.transactions)
    session.applied = [{"revision": "002"}, {"revision": "001"}]
    assert runner.rollback(session) == ("002",)
    assert calls[-1] == "down-002"


def test_geometry_type_and_spatial_query_nodes() -> None:
    geometry = p.Geometry(srid=4326, geometry_type="Point")
    point = geometry.validate(_ewkb_point(9.19, 45.46))
    predicate = geometry.predicate(
        "intersects", Place.shape, geometry.bind("reference")
    )
    function = geometry.function("area", Place.shape)

    assert point.srid == 4326
    assert (
        p.select(predicate).to_ast()["projection"][0]["expression"]["kind"] == "spatial"
    )
    assert (
        p.select(function.label("area")).to_ast()["projection"][0]["expression"]["kind"]
        == "spatial"
    )
    orm = p.OrmSession(_FakeSession(_FakeTransaction()))
    projections = orm.query(Place)._statement.to_ast()["projection"]
    assert projections[1]["alias"] == "shape"
    orm.rollback()


@pytest.mark.asyncio
async def test_async_orm_reuses_mapping_uow_and_relationship_planner() -> None:
    transaction = _AsyncFakeTransaction([{"id": 91, "created_by": "database"}])
    orm = p.AsyncOrmSession(_AsyncFakeSession(transaction))
    parent = GeneratedAccount(name="Ada")
    child = AuditEntry(id=1, message="created", account=parent)
    orm.add(child)
    orm.add(parent)

    await orm.flush()

    assert parent.id == 91
    assert child.account_id == 91
    assert [statement.target.name for statement, _ in transaction.executed] == [
        "orm_generated_accounts",
        "orm_audit_entries",
    ]
    transaction.rows = [{"id": 12, "name": "Lin", "version": 3}]
    account = await orm.query(Account).one()
    assert account.name == "Lin"
    account.name = "Grace"
    await orm.commit()
    assert p.inspect_instance(account).state is p.ObjectState.DETACHED
    assert transaction.committed


@pytest.mark.parametrize(
    ("provider", "connector"),
    (
        ("postgres", connect_postgres),
        ("mysql", connect_mysql_reference),
        ("mariadb", connect_mariadb_reference),
        ("sqlserver", connect_sqlserver_reference),
    ),
)
def test_live_portable_generated_defaults_and_ddl(provider: str, connector) -> None:
    session = connector()
    metadata = p.OrmMetadata(models=(LivePortableGenerated,))
    try:
        metadata.drop_all(session)
        metadata.create_all(session)
        with p.OrmSession(session) as orm:
            instance = LivePortableGenerated(name="Ada")
            orm.add(instance)
        assert instance.id is not None
        assert instance.created_by == "database"
        with p.OrmSession(session) as orm:
            loaded = orm.get(LivePortableGenerated, instance.id)
            assert loaded is not None
            assert loaded.name == "Ada"
            orm.delete(loaded)
        count_sql = {
            "postgres": "SELECT COUNT(*)::BIGINT FROM _plenora_orm_portable_generated",
            "mysql": "SELECT COUNT(*) FROM _plenora_orm_portable_generated",
            "mariadb": "SELECT COUNT(*) FROM _plenora_orm_portable_generated",
            "sqlserver": "SELECT COUNT_BIG(*) FROM _plenora_orm_portable_generated",
        }[provider]
        assert session.execute_scalar(count_sql) == 0
    finally:
        metadata.drop_all(session)
        session.close()


def test_live_db2_generated_defaults_and_ddl() -> None:
    session = connect_db2_reference()
    metadata = p.OrmMetadata(models=(LivePortableGenerated,))
    created = False
    try:
        schema = session.execute_scalar("VALUES CURRENT SCHEMA")
        tables = session.inspect.tables(schema)
        if any(
            item.get("name", "").lower() == LivePortableGenerated.__table__.name.lower()
            for item in tables
        ):
            metadata.drop_all(session, checkfirst=False)
        metadata.create_all(session)
        created = True
        with p.OrmSession(session) as orm:
            instance = LivePortableGenerated(name="Ada")
            orm.add(instance)
        assert instance.id is not None
        assert instance.created_by == "database"
        with p.OrmSession(session) as orm:
            loaded = orm.get(LivePortableGenerated, instance.id)
            assert loaded is not None and loaded.name == "Ada"
            orm.delete(loaded)
    finally:
        if created:
            metadata.drop_all(session, checkfirst=False)
        session.close()


@pytest.mark.parametrize(
    ("provider", "connector"),
    (
        ("mysql", connect_mysql_reference),
        ("mariadb", connect_mariadb_reference),
    ),
)
def test_live_mysql_family_geometry_orm_qualification(provider: str, connector) -> None:
    session = connector()
    assert session.capabilities["provider"] == provider
    metadata = p.OrmMetadata(models=(LiveMysqlGeometry,))
    point = _ewkb_point(9.19, 45.46)
    moved_point = _ewkb_point(9.20, 45.47)
    line = _ewkb_linestring(((9.18, 45.45), (9.20, 45.47)))
    polygon = _ewkb_polygon(
        (
            (9.10, 45.40),
            (9.30, 45.40),
            (9.30, 45.55),
            (9.10, 45.55),
            (9.10, 45.40),
        )
    )
    try:
        metadata.drop_all(session)
        metadata.create_all(session)

        with pytest.raises(ValueError):
            LiveMysqlGeometry(
                id=99,
                point=b"EWKB-invalid",
                line=line,
                polygon=polygon,
            )
        with pytest.raises(ValueError, match="SRID"):
            LiveMysqlGeometry(
                id=99,
                point=_ewkb_point(9.19, 45.46, srid=3857),
                line=line,
                polygon=polygon,
            )
        with pytest.raises(p.OrmUnsupportedError, match="XY"):
            p.OrmMetadata(models=(LiveMysqlGeometryXyz,)).create_all(session)

        with p.OrmSession(session) as orm:
            orm.add(
                LiveMysqlGeometry(
                    id=1,
                    point=point,
                    line=line,
                    polygon=polygon,
                    optional_point=None,
                )
            )

        with p.OrmSession(session) as orm:
            loaded = orm.get(LiveMysqlGeometry, 1)
            assert loaded is not None
            assert loaded.point.ewkb == _root_wkb(point)
            assert loaded.line.ewkb == _root_wkb(line)
            assert loaded.polygon.ewkb == _root_wkb(polygon)
            assert loaded.optional_point is None
            assert loaded.point.srid == 4326
            loaded.point = moved_point
            loaded.optional_point = point

        predicate = _MYSQL_POINT.predicate(
            "intersects", LiveMysqlGeometry.point, _MYSQL_POINT.bind("reference")
        )
        with p.OrmSession(session) as orm:
            [queried] = orm.query(LiveMysqlGeometry).where(predicate).all(
                {"reference": moved_point}
            )
            assert queried.point.ewkb == _root_wkb(moved_point)
            assert queried.optional_point is not None
            assert queried.optional_point.ewkb == _root_wkb(point)
            orm.delete(queried)

        assert (
            session.execute_scalar(
                "SELECT COUNT(*) FROM _plenora_orm_mysql_geometry"
            )
            == 0
        )
    finally:
        metadata.drop_all(session)
        session.close()


def _exercise_live_portable_geometry(provider: str, connector) -> None:
    session = connector()
    assert session.capabilities["provider"] == provider
    metadata = p.OrmMetadata(
        models=(LivePortableGeometry, LivePortableGeometryXyz)
    )
    point = _ewkb_point(9.19, 45.46)
    moved_point = _ewkb_point(9.20, 45.47)
    point_xyz = _ewkb_point_xyz(9.19, 45.46, 120.0)
    line = _ewkb_linestring(((9.18, 45.45), (9.20, 45.47)))
    polygon = _ewkb_polygon(
        (
            (9.10, 45.40),
            (9.30, 45.40),
            (9.30, 45.55),
            (9.10, 45.55),
            (9.10, 45.40),
        )
    )
    try:
        metadata.drop_all(session)
        metadata.create_all(session)
        with pytest.raises(ValueError):
            LivePortableGeometry(
                id=99,
                point=b"EWKB-invalid",
                line=line,
                polygon=polygon,
            )
        with pytest.raises(ValueError, match="SRID"):
            LivePortableGeometry(
                id=99,
                point=_ewkb_point(9.19, 45.46, srid=3857),
                line=line,
                polygon=polygon,
            )

        with p.OrmSession(session) as orm:
            orm.add(
                LivePortableGeometry(
                    id=1,
                    point=point,
                    line=line,
                    polygon=polygon,
                    optional_point=None,
                )
            )
            orm.add(LivePortableGeometryXyz(id=1, shape=point_xyz))

        with p.OrmSession(session) as orm:
            loaded = orm.get(LivePortableGeometry, 1)
            loaded_xyz = orm.get(LivePortableGeometryXyz, 1)
            assert loaded is not None and loaded_xyz is not None
            assert loaded.point.ewkb == _root_wkb(point)
            assert loaded.line.ewkb == _root_wkb(line)
            assert loaded.polygon.ewkb == _root_wkb(polygon)
            assert loaded.optional_point is None
            assert loaded.point.srid == 4326
            assert loaded_xyz.shape.ewkb == _root_wkb(point_xyz)
            assert loaded_xyz.shape.dimensions == "xyz"
            loaded.point = moved_point
            loaded.optional_point = point

        predicate = _PORTABLE_POINT.predicate(
            "intersects",
            LivePortableGeometry.point,
            _PORTABLE_POINT.bind("reference"),
        )
        with p.OrmSession(session) as orm:
            [queried] = orm.query(LivePortableGeometry).where(predicate).all(
                {"reference": moved_point}
            )
            assert queried.point.ewkb == _root_wkb(moved_point)
            assert queried.optional_point is not None
            assert queried.optional_point.ewkb == _root_wkb(point)
            queried.optional_point = None

        with p.OrmSession(session) as orm:
            loaded = orm.get(LivePortableGeometry, 1)
            assert loaded is not None
            assert loaded.optional_point is None
            orm.delete(loaded)
            xyz = orm.get(LivePortableGeometryXyz, 1)
            assert xyz is not None
            orm.delete(xyz)

        table_name = (
            '"_plenora_orm_portable_geometry"'
            if provider == "db2"
            else "_plenora_orm_portable_geometry"
        )
        assert session.execute_scalar(f"SELECT COUNT(*) FROM {table_name}") == 0
    finally:
        metadata.drop_all(session)
        session.close()


def test_live_sqlserver_geometry_orm_qualification() -> None:
    _exercise_live_portable_geometry("sqlserver", connect_sqlserver_reference)

    session = connect_sqlserver_reference()
    metadata = p.OrmMetadata(models=(LiveSqlServerGeography,))
    point = _ewkb_point(9.19, 45.46)
    try:
        metadata.drop_all(session)
        metadata.create_all(session)
        with p.OrmSession(session) as orm:
            orm.add(LiveSqlServerGeography(id=1, shape=point))
        predicate = p.Geometry(
            srid=4326, semantics="geography", geometry_type="point"
        ).predicate(
            "intersects",
            LiveSqlServerGeography.shape,
            p.Geometry(
                srid=4326, semantics="geography", geometry_type="point"
            ).bind("reference"),
        )
        with p.OrmSession(session) as orm:
            [loaded] = orm.query(LiveSqlServerGeography).where(predicate).all(
                {"reference": point}
            )
            assert loaded.shape.ewkb == _root_wkb(point)
            assert loaded.shape.semantics == "geography"
            orm.delete(loaded)
    finally:
        metadata.drop_all(session)
        session.close()


def test_live_db2_geometry_orm_qualification() -> None:
    _exercise_live_portable_geometry("db2", connect_db2_reference)


@pytest.mark.asyncio
async def test_live_postgres_async_orm_geometry_lifecycle() -> None:
    setup = await aconnect_postgres()
    await setup.execute("DROP TABLE IF EXISTS _plenora_async_orm_places")
    await setup.execute(
        "CREATE TABLE _plenora_async_orm_places ("
        "id INT PRIMARY KEY, shape geometry(Point, 4326) NOT NULL)"
    )
    try:
        first_point = _ewkb_point(9.19, 45.46)
        second_point = _ewkb_point(9.20, 45.47)
        async with p.AsyncOrmSession(setup) as orm:
            orm.add(LiveAsyncPlace(id=1, shape=first_point))
        async with p.AsyncOrmSession(setup) as orm:
            place = await orm.get(LiveAsyncPlace, 1)
            assert place is not None and place.shape.ewkb == first_point
            place.shape = second_point
        async with p.AsyncOrmSession(setup) as orm:
            place = await orm.query(LiveAsyncPlace).one()
            assert place.shape.ewkb == second_point
            orm.delete(place)
        assert (
            await setup.execute_scalar(
                "SELECT COUNT(*)::BIGINT FROM _plenora_async_orm_places"
            )
            == 0
        )
    finally:
        await setup.execute("DROP TABLE IF EXISTS _plenora_async_orm_places")
        setup.close()


def test_live_postgres_orm_lifecycle_and_optimistic_conflict() -> None:
    setup = connect_postgres()
    setup.execute("DROP TABLE IF EXISTS _plenora_orm_accounts")
    setup.execute("DROP TABLE IF EXISTS _plenora_orm_audit_entries")
    setup.execute("DROP TABLE IF EXISTS _plenora_orm_generated_accounts")
    setup.execute("DROP TABLE IF EXISTS _plenora_orm_places")
    setup.execute(
        "CREATE TABLE _plenora_orm_accounts ("
        "id INT PRIMARY KEY, name TEXT NOT NULL, version INT NOT NULL)"
    )
    setup.execute(
        "CREATE TABLE _plenora_orm_generated_accounts ("
        "id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
        "name TEXT NOT NULL, created_by TEXT NOT NULL DEFAULT 'database')"
    )
    setup.execute(
        "CREATE TABLE _plenora_orm_audit_entries ("
        "id INT PRIMARY KEY, account_id INT NOT NULL REFERENCES "
        "_plenora_orm_generated_accounts(id), message TEXT NOT NULL)"
    )
    setup.execute(
        "CREATE TABLE _plenora_orm_places ("
        "id INT PRIMARY KEY, shape geometry(Point, 4326) NOT NULL)"
    )
    try:
        with p.OrmSession(setup) as orm:
            orm.add(LiveAccount(id=1, name="Ada"))
        with p.OrmSession(setup) as orm:
            generated = LiveGeneratedAccount(name="Ada")
            audit = LiveAuditEntry(id=1, message="created", account=generated)
            orm.add(audit)
            orm.add(generated)
        assert generated.id == 1
        assert generated.created_by == "database"
        assert audit.account_id == generated.id

        first_point = _ewkb_point(9.19, 45.46)
        second_point = _ewkb_point(9.20, 45.47)
        with p.OrmSession(setup) as orm:
            orm.add(LivePlace(id=1, shape=first_point))
        with p.OrmSession(setup) as orm:
            place = orm.get(LivePlace, 1)
            assert place is not None
            assert place.shape.ewkb == first_point
            place.shape = second_point
        with p.OrmSession(setup) as orm:
            place = orm.query(LivePlace).one()
            assert place.shape.ewkb == second_point

        first_core = connect_postgres()
        second_core = connect_postgres()
        try:
            first = p.OrmSession(first_core)
            second = p.OrmSession(second_core)
            stale = first.get(LiveAccount, 1)
            winner = second.get(LiveAccount, 1)
            assert stale is not None and winner is not None
            winner.name = "Grace"
            second.commit()
            stale.name = "Lin"
            with pytest.raises(p.StaleObjectError):
                first.commit()
        finally:
            first_core.close()
            second_core.close()

        with p.OrmSession(setup) as orm:
            current = orm.get(LiveAccount, 1)
            assert current is not None
            assert current.name == "Grace"
            assert current.version == 2
            queried = (
                orm.query(LiveAccount)
                .where(LiveAccount.name == p.bind("wanted"))
                .one({"wanted": "Grace"})
            )
            assert queried is current
            orm.delete(current)
        assert (
            setup.execute_scalar("SELECT COUNT(*)::BIGINT FROM _plenora_orm_accounts")
            == 0
        )
    finally:
        setup.execute("DROP TABLE IF EXISTS _plenora_orm_accounts")
        setup.execute("DROP TABLE IF EXISTS _plenora_orm_audit_entries")
        setup.execute("DROP TABLE IF EXISTS _plenora_orm_generated_accounts")
        setup.execute("DROP TABLE IF EXISTS _plenora_orm_places")
        setup.close()
