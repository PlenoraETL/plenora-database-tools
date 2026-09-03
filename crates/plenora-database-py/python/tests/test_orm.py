"""Contratto ORM offline e qualifiche live per mapping, query e UoW."""

from __future__ import annotations

import hashlib
import struct
from datetime import datetime, timezone
from decimal import Decimal
from typing import ClassVar
from uuid import UUID

import plenora_database as p
import pytest
from plenora_database._native import compile_relational_query
from plenora_database.result import Result

from ._harness import (
    aconnect_postgres,
    connect_db2_reference,
    connect_mariadb_reference,
    connect_mysql_reference,
    connect_postgres,
    connect_sqlserver_reference,
)


def _migration(
    revision: str,
    down_revision: str | tuple[str, ...] | None,
    upgrade,
    downgrade=None,
) -> p.Migration:
    return p.Migration(
        revision,
        down_revision,
        upgrade,
        downgrade,
        hashlib.sha256(revision.encode("utf-8")).hexdigest(),
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
    point: p.Mapped[p.SpatialReference] = p.mapped_column(_MYSQL_POINT, nullable=False)
    line: p.Mapped[p.SpatialReference] = p.mapped_column(
        _MYSQL_LINESTRING, nullable=False
    )
    polygon: p.Mapped[p.SpatialReference] = p.mapped_column(
        _MYSQL_POLYGON, nullable=False
    )
    optional_point: p.Mapped[p.SpatialReference | None] = p.mapped_column(_MYSQL_POINT)


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


class LiveOrmEntity(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_entities"
    __mapper_args__: ClassVar[dict[str, str]] = {
        "polymorphic_on": "kind",
        "polymorphic_identity": "entity",
    }

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    kind: p.Mapped[str] = p.mapped_column(str, nullable=False)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)


class LiveOrmService(LiveOrmEntity):
    __mapper_args__: ClassVar[dict[str, str]] = {"polymorphic_identity": "service"}

    port: p.Mapped[int] = p.mapped_column(int)


class LiveOrmDatabase(LiveOrmService):
    __mapper_args__: ClassVar[dict[str, str]] = {"polymorphic_identity": "database"}

    engine: p.Mapped[str] = p.mapped_column(str)


class LiveOrmAsset(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_assets"
    __mapper_args__: ClassVar[dict[str, str]] = {
        "polymorphic_on": "kind",
        "polymorphic_identity": "asset",
    }

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    kind: p.Mapped[str] = p.mapped_column(str, nullable=False)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)


class LiveOrmMachine(LiveOrmAsset):
    __tablename__ = "_plenora_orm_machines"
    __mapper_args__: ClassVar[dict[str, str]] = {
        "inheritance": "joined",
        "polymorphic_identity": "machine",
    }

    cores: p.Mapped[int] = p.mapped_column(int, nullable=False)


class LiveOrmRackMachine(LiveOrmMachine):
    __tablename__ = "_plenora_orm_rack_machines"
    __mapper_args__: ClassVar[dict[str, str]] = {
        "inheritance": "joined",
        "polymorphic_identity": "rack-machine",
    }

    rack_units: p.Mapped[int] = p.mapped_column(int, nullable=False)


class LiveOrmBigRecord(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_big_records"

    id: p.Mapped[int] = p.mapped_column(p.BIGINT, primary_key=True)
    counter: p.Mapped[int] = p.mapped_column(p.BIGINT, nullable=False)


class LiveOrmLoaderRoot(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_loader_roots"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    middles: p.Relationship[LiveOrmLoaderMiddle] = p.relationship(
        "LiveOrmLoaderMiddle",
        foreign_key="root_id",
        uselist=True,
        cascade="save-update, delete",
    )


class LiveOrmLoaderMiddle(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_loader_middles"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    root_id: p.Mapped[int] = p.mapped_column(int, nullable=False)
    leaves: p.Relationship[LiveOrmLoaderLeaf] = p.relationship(
        "LiveOrmLoaderLeaf",
        foreign_key="middle_id",
        uselist=True,
        cascade="save-update, delete",
    )


class LiveOrmLoaderLeaf(p.DeclarativeBase):
    __tablename__ = "_plenora_orm_loader_leaves"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    middle_id: p.Mapped[int] = p.mapped_column(int, nullable=False)


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


class CompositeChild(p.DeclarativeBase):
    __tablename__ = "orm_composite_children"

    tenant_id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    child_id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    parent_tenant_id: p.Mapped[int] = p.mapped_column(int, nullable=False)
    parent_code: p.Mapped[str] = p.mapped_column(str, nullable=False)
    label: p.Mapped[str] = p.mapped_column(str, nullable=False)
    parent: p.Relationship[CompositeParent] = p.relationship(
        "CompositeParent",
        foreign_key=("parent_tenant_id", "parent_code"),
        back_populates="children",
    )


class CompositeParent(p.DeclarativeBase):
    __tablename__ = "orm_composite_parents"

    tenant_id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    code: p.Mapped[str] = p.mapped_column(str, primary_key=True)
    label: p.Mapped[str] = p.mapped_column(str, nullable=False)
    children: p.Relationship[CompositeChild] = p.relationship(
        CompositeChild,
        foreign_key=("parent_tenant_id", "parent_code"),
        uselist=True,
        back_populates="parent",
        cascade="save-update",
    )


composite_links = p.table(
    "orm_composite_links", "owner_tenant", "owner_code", "tag_tenant", "tag_code"
)


class CompositeTag(p.DeclarativeBase):
    __tablename__ = "orm_composite_tags"

    tenant_id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    code: p.Mapped[str] = p.mapped_column(str, primary_key=True)
    owners: p.Relationship[CompositeOwner] = p.relationship(
        "CompositeOwner",
        uselist=True,
        back_populates="tags",
        secondary=composite_links,
        secondary_local_key=("tag_tenant", "tag_code"),
        secondary_remote_key=("owner_tenant", "owner_code"),
    )


class CompositeOwner(p.DeclarativeBase):
    __tablename__ = "orm_composite_owners"

    tenant_id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    code: p.Mapped[str] = p.mapped_column(str, primary_key=True)
    tags: p.Relationship[CompositeTag] = p.relationship(
        CompositeTag,
        uselist=True,
        back_populates="owners",
        cascade="save-update",
        secondary=composite_links,
        secondary_local_key=("owner_tenant", "owner_code"),
        secondary_remote_key=("tag_tenant", "tag_code"),
    )


class ConcreteAccount(Account):
    __tablename__ = "orm_concrete_accounts"
    __mapper_args__: ClassVar[dict[str, bool]] = {"concrete": True}

    category: p.Mapped[str] = p.mapped_column(str, nullable=False)


class AuditMixin(p.DeclarativeBase):
    __abstract__ = True

    created_by: p.Mapped[str] = p.mapped_column(str, nullable=False)


class MixinRecord(AuditMixin):
    __tablename__ = "orm_mixin_records"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    label: p.Mapped[str] = p.mapped_column(str, nullable=False)


class InheritedGroup(p.DeclarativeBase):
    __tablename__ = "orm_inherited_groups"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)


class RelatedAccount(p.DeclarativeBase):
    __tablename__ = "orm_related_accounts"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    group_id: p.Mapped[int] = p.mapped_column(int, nullable=False)
    group: p.Relationship[InheritedGroup] = p.relationship(
        InheritedGroup, foreign_key="group_id"
    )


class ConcreteRelatedAccount(RelatedAccount):
    __tablename__ = "orm_concrete_related_accounts"
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


class OrmBigRecord(p.DeclarativeBase):
    __tablename__ = "orm_big_records"

    id: p.Mapped[int] = p.mapped_column(p.BIGINT, primary_key=True)
    counter: p.Mapped[int] = p.mapped_column(p.BIGINT, nullable=False)


class OrmLoaderRoot(p.DeclarativeBase):
    __tablename__ = "orm_loader_roots"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    middles: p.Relationship[OrmLoaderMiddle] = p.relationship(
        "OrmLoaderMiddle", foreign_key="root_id", uselist=True
    )


class OrmLoaderMiddle(p.DeclarativeBase):
    __tablename__ = "orm_loader_middles"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    root_id: p.Mapped[int] = p.mapped_column(int, nullable=False)
    leaves: p.Relationship[OrmLoaderLeaf] = p.relationship(
        "OrmLoaderLeaf", foreign_key="middle_id", uselist=True
    )


class OrmLoaderLeaf(p.DeclarativeBase):
    __tablename__ = "orm_loader_leaves"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    middle_id: p.Mapped[int] = p.mapped_column(int, nullable=False)


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


def _ewkb_linestring(
    points: tuple[tuple[float, float], ...], srid: int = 4326
) -> bytes:
    return (
        b"\x01"
        + (0x2000_0002).to_bytes(4, "little")
        + srid.to_bytes(4, "little")
        + len(points).to_bytes(4, "little")
        + b"".join(struct.pack("<dd", *point) for point in points)
    )


def _ewkb_polygon(ring: tuple[tuple[float, float], ...], srid: int = 4326) -> bytes:
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


def _wkb_structure_and_coordinates(
    value: bytes,
) -> tuple[tuple[int, ...], tuple[float, ...]]:
    assert value[:1] == b"\x01"
    geometry_type = int.from_bytes(value[1:5], "little")
    dimensions = 3 if geometry_type // 1000 in {1, 3} else 2
    base_type = geometry_type % 1000
    offset = 5
    structure = [geometry_type]
    coordinates: list[float] = []

    def read_points(count: int) -> None:
        nonlocal offset
        width = count * dimensions
        coordinates.extend(struct.unpack_from(f"<{width}d", value, offset))
        offset += width * 8

    if base_type == 1:
        read_points(1)
    elif base_type == 2:
        count = int.from_bytes(value[offset : offset + 4], "little")
        offset += 4
        structure.append(count)
        read_points(count)
    elif base_type == 3:
        rings = int.from_bytes(value[offset : offset + 4], "little")
        offset += 4
        structure.append(rings)
        for _ in range(rings):
            count = int.from_bytes(value[offset : offset + 4], "little")
            offset += 4
            structure.append(count)
            read_points(count)
    else:
        raise AssertionError("tipo WKB del fixture non supportato")
    assert offset == len(value)
    return tuple(structure), tuple(coordinates)


def _assert_portable_wkb(actual: bytes, expected: bytes, provider: str) -> None:
    if provider != "db2":
        assert actual == expected
        return
    actual_structure, actual_coordinates = _wkb_structure_and_coordinates(actual)
    expected_structure, expected_coordinates = _wkb_structure_and_coordinates(expected)
    assert actual_structure == expected_structure
    assert actual_coordinates == pytest.approx(expected_coordinates, rel=0.0, abs=1e-12)


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
        self.savepoint_calls: list[tuple[str, str]] = []

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

    def savepoint(self, name: str) -> None:
        self.savepoint_calls.append(("savepoint", name))

    def rollback_to_savepoint(self, name: str) -> None:
        self.savepoint_calls.append(("rollback", name))

    def release_savepoint(self, name: str) -> None:
        self.savepoint_calls.append(("release", name))


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

    async def savepoint(self, name: str) -> None:
        super().savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        super().rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        super().release_savepoint(name)


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
    assert Account.__mapper__.attribute("id").type_ is int
    assert Account.__mapper__.attribute("name").type_ is str
    assert Account.__mapper__.attribute("version").type_ is int
    assert isinstance(Account.id == p.bind("identity", p.BindType.INTEGER), p.Predicate)


def test_flush_batches_compatible_pending_instances() -> None:
    transaction = _FakeTransaction()
    transaction.affected = [2]
    orm = p.OrmSession(_FakeSession(transaction))
    first = Account(id=101, name="first")
    second = Account(id=102, name="second")

    orm.add_all((first, second))
    orm.flush()

    assert len(transaction.executed) == 1
    statement, parameters = transaction.executed[0]
    assert isinstance(statement, p.InsertStatement)
    assert len(statement.rows) == 2
    assert len(parameters) == 6
    assert p.inspect_instance(first).state is p.ObjectState.PERSISTENT
    assert p.inspect_instance(second).state is p.ObjectState.PERSISTENT

    hooked_transaction = _FakeTransaction()
    hooked_transaction.affected = [1, 1]
    hooked = p.OrmSession(_FakeSession(hooked_transaction))
    hooked.listen("before_insert", lambda _session, instance: None)
    hooked.add_all(
        (Account(id=111, name="first"), Account(id=112, name="second"))
    )
    hooked.flush()
    assert len(hooked_transaction.executed) == 2


def test_bulk_mapping_insert_and_upsert_are_single_round_trips() -> None:
    transaction = _FakeTransaction()
    transaction.affected = [2, 2]
    orm = p.OrmSession(_FakeSession(transaction))
    rows = ({"id": 301, "name": "one"}, {"id": 302, "name": "two"})

    assert orm.bulk_insert(Account, rows) == 2
    assert orm.bulk_upsert(
        Account,
        rows,
        conflict_columns=("id",),
        update_values={"name": "updated"},
    ) == 2

    insert_statement, insert_parameters = transaction.executed[0]
    upsert_statement, upsert_parameters = transaction.executed[1]
    assert isinstance(insert_statement, p.InsertStatement)
    assert len(insert_statement.rows) == 2
    assert isinstance(upsert_statement, p.UpsertStatement)
    assert len(upsert_statement.rows) == 2
    assert upsert_statement.conflict_names == ("id",)
    assert len(insert_parameters) == 6
    assert len(upsert_parameters) == 7

    sqlserver = p.OrmSession(_FakeSession(_FakeTransaction(), "sqlserver"))
    with pytest.raises(p.OrmUnsupportedError, match="una riga alla volta"):
        sqlserver.bulk_upsert(Account, rows, conflict_columns=("id",))


def test_passive_delete_requires_and_uses_declared_database_cascade() -> None:
    registry = p.Registry()

    class Base(p.DeclarativeBase):
        __registry__ = registry

    class PassiveParent(Base):
        __tablename__ = "passive_parents"

        id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
        children: p.Relationship["PassiveChild"] = p.relationship(
            "PassiveChild",
            foreign_key="parent_id",
            uselist=True,
            cascade="delete",
            passive_deletes=True,
        )

    class PassiveChild(Base):
        __tablename__ = "passive_children"
        __table_args__ = (
            p.ForeignKeyConstraint(
                ("parent_id",),
                "PassiveParent",
                ("id",),
                on_delete="CASCADE",
            ),
        )

        id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
        parent_id: p.Mapped[int] = p.mapped_column(int, nullable=False)

    transaction = _FakeTransaction([{"id": 401}])
    orm = p.OrmSession(_FakeSession(transaction))
    parent = orm.get(PassiveParent, 401)
    assert parent is not None

    orm.delete(parent)
    orm.flush()

    assert isinstance(transaction.executed[-1][0], p.DeleteStatement)


def test_session_options_no_autoflush_and_bounded_partitions() -> None:
    transaction = _FakeTransaction()

    class ConfigurableSession(_FakeSession):
        def __init__(self) -> None:
            super().__init__(transaction)
            self.options: dict[str, object] = {}

        def begin(self, **options):
            self.options = options
            return self.transaction

    session = ConfigurableSession()
    orm = p.OrmSession(
        session,
        isolation="serializable",
        read_only=False,
        statement_timeout_ms=500,
        insert_batch_size=2,
    )
    assert session.options == {
        "isolation": "serializable",
        "read_only": False,
        "statement_timeout_ms": 500,
    }
    pending = Account(id=103, name="pending")
    orm.add(pending)
    with orm.no_autoflush():
        assert orm.query(Account).all() == []
    assert len(transaction.executed) == 1

    transaction.result_batches = [
        [
            {"id": 201, "name": "one", "version": 1},
            {"id": 202, "name": "two", "version": 1},
        ],
        [{"id": 203, "name": "three", "version": 1}],
    ]
    batches = list(
        orm.query(Account)
        .order_by(Account.id)
        .partitions(2, detach=True)
    )
    assert [len(batch) for batch in batches] == [2, 1]
    assert all(
        p.inspect_instance(instance).state is p.ObjectState.DETACHED
        for batch in batches
        for instance in batch
    )
    selects = [
        statement
        for statement, _ in transaction.executed
        if isinstance(statement, p.SelectStatement)
    ]
    assert [(item.row_limit, item.row_offset) for item in selects[-2:]] == [
        (2, 0),
        (2, 2),
    ]
    with pytest.raises(p.OrmStateError, match="chiavi primarie"):
        list(orm.query(Account).order_by(Account.name).partitions(2))


def test_owned_core_session_is_closed_and_defaults_reject_nonfinite_values() -> None:
    transaction = _FakeTransaction()

    class ClosableSession(_FakeSession):
        def __init__(self) -> None:
            super().__init__(transaction)
            self.closed = False

        def close(self) -> None:
            self.closed = True

    session = ClosableSession()
    orm = p.OrmSession(session, close_session=True)
    orm.rollback()
    assert session.closed

    with pytest.raises(ValueError, match="non finito"):
        p.ServerDefault.literal(float("nan"))
    with pytest.raises(p.OrmMappingError, match="non finito"):
        p.CheckConstraint("score", ">", float("inf"), name="ck_finite")


def test_big_integer_ddl_and_dml_keep_signed_64_bit_typing() -> None:
    for provider in ("postgres", "mysql", "mariadb", "sqlserver", "db2"):
        ddl = p.OrmMetadata(models=(OrmBigRecord,)).ddl(provider)
        assert "BIGINT" in ddl[0]

    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    record = OrmBigRecord(id=2**40, counter=7)
    orm.add(record)
    orm.flush()

    statement, parameters = transaction.executed[0]
    ast = statement.to_ast()
    assert all(item["kind"] == "typed_parameter" for item in ast["rows"][0])
    assert all(value._plenora_typed_kind == "i64" for value in parameters.values())

    with pytest.raises(ValueError, match="signed 64-bit"):
        OrmBigRecord(id=2**63, counter=1)


def test_oracle_23_ddl_is_explicit_and_unqualified_features_fail_closed() -> None:
    class OracleRecord(p.DeclarativeBase):
        __tablename__ = "orm_oracle_records"

        id: p.Mapped[int] = p.mapped_column(primary_key=True)
        large_id: p.Mapped[int] = p.mapped_column(p.BIGINT, nullable=False)
        label: p.Mapped[str] = p.mapped_column(p.String(32), nullable=False)
        description: p.Mapped[str] = p.mapped_column(str, nullable=False)
        amount: p.Mapped[Decimal] = p.mapped_column(p.Numeric(12, 2), nullable=False)
        token: p.Mapped[str] = p.mapped_column(p.UUID, nullable=False)
        payload: p.Mapped[dict] = p.mapped_column(p.JSON, nullable=False)
        active: p.Mapped[bool] = p.mapped_column(bool, nullable=False)
        observed_at: p.Mapped[datetime] = p.mapped_column(datetime, nullable=False)
        content: p.Mapped[bytes] = p.mapped_column(bytes, nullable=False)
        ratio: p.Mapped[float] = p.mapped_column(float, nullable=False)

    ddl = p.OrmMetadata(models=(OracleRecord,)).ddl("oracle")[0]
    for declaration in (
        '"id" NUMBER(10)',
        '"large_id" NUMBER(19)',
        '"label" VARCHAR2(32)',
        '"description" VARCHAR2(4000)',
        '"amount" NUMBER(12, 2)',
        '"token" VARCHAR2(36)',
        '"payload" JSON',
        '"active" BOOLEAN',
        '"observed_at" TIMESTAMP',
        '"content" BLOB',
        '"ratio" BINARY_DOUBLE',
    ):
        assert declaration in ddl

    with pytest.raises(p.OrmUnsupportedError, match="IF NOT EXISTS"):
        p.OrmMetadata(models=(OracleRecord,)).ddl("oracle", checkfirst=True)

    class OracleGenerated(p.DeclarativeBase):
        __tablename__ = "orm_oracle_generated"

        id: p.Mapped[int] = p.mapped_column(primary_key=True, generated=True)

    with pytest.raises(p.OrmUnsupportedError, match="identity generated"):
        p.OrmMetadata(models=(OracleGenerated,)).ddl("oracle")

    class OracleAwareTimestamp(p.DeclarativeBase):
        __tablename__ = "orm_oracle_aware_timestamp"

        id: p.Mapped[int] = p.mapped_column(primary_key=True)
        observed_at: p.Mapped[datetime] = p.mapped_column(
            p.DateTime(timezone=True), nullable=False
        )

    with pytest.raises(p.OrmUnsupportedError, match="timezone"):
        p.OrmMetadata(models=(OracleAwareTimestamp,)).ddl("oracle")


def test_oracle_spatial_ddl_registers_metadata_before_spatial_index() -> None:
    class OracleSpatialDdlRecord(p.DeclarativeBase):
        __tablename__ = "ORM_ORACLE_SPATIAL"
        __table_args__ = (p.OrmIndex("SHAPE", name="IX_ORM_ORACLE_SPATIAL"),)

        ID: p.Mapped[int] = p.mapped_column(primary_key=True)
        SHAPE: p.Mapped[p.SpatialReference] = p.mapped_column(
            p.Geometry(srid=4326, geometry_type="point"), nullable=False
        )

    ddl = p.OrmMetadata(models=(OracleSpatialDdlRecord,)).ddl("oracle")
    assert len(ddl) == 3
    assert '"SHAPE" MDSYS.SDO_GEOMETRY NOT NULL' in ddl[0]
    assert "USER_SDO_GEOM_METADATA" in ddl[1]
    assert "MDSYS.SDO_DIM_ELEMENT('LONGITUDE', -180, 180, 0.005)" in ddl[1]
    assert "4326" in ddl[1]
    assert ddl[2].endswith("INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2")

    class InvalidOracleSpatialIndex(p.DeclarativeBase):
        __tablename__ = "ORM_ORACLE_INVALID_SPATIAL"
        __table_args__ = (
            p.OrmIndex("SHAPE", name="UQ_ORM_ORACLE_SPATIAL", unique=True),
        )

        ID: p.Mapped[int] = p.mapped_column(primary_key=True)
        SHAPE: p.Mapped[p.SpatialReference] = p.mapped_column(
            p.Geometry(srid=4326, geometry_type="point"), nullable=False
        )

    class LowercaseOracleSpatial(p.DeclarativeBase):
        __tablename__ = "ORM_ORACLE_LOWERCASE_SPATIAL"

        id: p.Mapped[int] = p.mapped_column(primary_key=True)
        shape: p.Mapped[p.SpatialReference] = p.mapped_column(
            p.Geometry(srid=4326, geometry_type="point"), nullable=False
        )

    with pytest.raises(p.OrmUnsupportedError, match="uppercase"):
        p.OrmMetadata(models=(LowercaseOracleSpatial,)).ddl("oracle")

    with pytest.raises(p.OrmUnsupportedError, match="non univoco"):
        p.OrmMetadata(models=(InvalidOracleSpatialIndex,)).ddl("oracle")


def test_portable_scalar_types_validate_and_render_without_guessing() -> None:
    class PortableScalarRecord(p.DeclarativeBase):
        __tablename__ = "orm_portable_scalars"

        id: p.Mapped[int] = p.mapped_column(primary_key=True)
        label: p.Mapped[str] = p.mapped_column(p.String(32), nullable=False)
        amount: p.Mapped[Decimal] = p.mapped_column(p.Numeric(12, 2), nullable=False)
        token: p.Mapped[str] = p.mapped_column(p.UUID, nullable=False, unique=True)
        payload: p.Mapped[dict] = p.mapped_column(p.JSON, nullable=False)
        observed_at: p.Mapped[datetime] = p.mapped_column(
            p.DateTime(timezone=True), nullable=False
        )

    token = "12345678-1234-5678-1234-567812345678"
    observed = datetime(2026, 9, 3, 12, 30, tzinfo=timezone.utc)
    record = PortableScalarRecord(
        id=1,
        label="measured",
        amount=Decimal("12.30"),
        token=UUID(token),
        payload={"status": "ok"},
        observed_at=observed,
    )
    assert record.token == token
    assert record.amount == Decimal("12.30")

    ddl = p.OrmMetadata(models=(PortableScalarRecord,)).ddl("postgres")[0]
    assert "VARCHAR(32)" in ddl
    assert "DECIMAL(12, 2)" in ddl
    assert "UUID" in ddl
    assert "JSONB" in ddl
    assert "TIMESTAMPTZ" in ddl

    with pytest.raises(ValueError):
        record.label = "x" * 33
    with pytest.raises(ValueError):
        record.amount = Decimal("12345678901.23")
    with pytest.raises(p.OrmUnsupportedError):
        p.OrmMetadata(models=(PortableScalarRecord,)).ddl("mysql")

    class NaiveDateTimeRecord(p.DeclarativeBase):
        __tablename__ = "orm_naive_datetimes"

        id: p.Mapped[int] = p.mapped_column(primary_key=True)
        observed_at: p.Mapped[datetime] = p.mapped_column(nullable=False)

    with pytest.raises(ValueError, match=r"DateTime\(timezone=True\)"):
        NaiveDateTimeRecord(id=1, observed_at=observed)


def test_query_count_exists_pagination_distinct_and_bulk_dml() -> None:
    transaction = _FakeTransaction([{"count": 3}])
    orm = p.OrmSession(_FakeSession(transaction))
    query = orm.query(Account).where(Account.name == p.bind("wanted", p.BindType.STRING))

    shaped = (
        query.offset(4)
        .distinct()
        .group_by(Account.name)
        .having(p.func.count() > p.bind("minimum", p.BindType.INTEGER))
    )
    assert shaped._statement.row_offset == 4
    assert shaped._statement.is_distinct
    assert shaped._statement.groupings == (Account.name,)
    assert shaped._statement.having_predicate is not None
    assert query.count({"wanted": "Ada"}) == 3
    assert transaction.executed[-1][0].source is Account.__table__
    assert query.distinct().count({"wanted": "Ada"}) == 3
    assert transaction.executed[-1][0].source.qualifier == "_plenora_orm_count"
    distinct_on = query.distinct_on(Account.id).order_by(Account.id)
    assert distinct_on.count({"wanted": "Ada"}) == 3
    count_statement = transaction.executed[-1][0]
    sql, names = compile_relational_query(count_statement.to_json(), "postgres")
    assert "DISTINCT ON" in sql and "ORDER BY" in sql
    assert names == ["wanted"]

    transaction.rows = [{"id": 1}]
    assert query.exists({"wanted": "Ada"})

    transaction.affected = [4]
    assert query.update({"name": "Grace"}, {"wanted": "Ada"}) == 4
    update_statement, update_parameters = transaction.executed[-1]
    assert isinstance(update_statement, p.UpdateStatement)
    assert update_parameters["wanted"] == "Ada"

    transaction.affected = [2]
    assert query.delete({"wanted": "Grace"}) == 2
    assert isinstance(transaction.executed[-1][0], p.DeleteStatement)


def test_orm_savepoint_restores_unit_of_work_state() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    account = Account(id=31, name="before")
    orm.add(account)
    orm.flush()
    orm.savepoint("edit_account")

    account.name = "after"
    orm.flush()
    orm.rollback_to_savepoint("edit_account")

    assert account.name == "before"
    assert p.inspect_instance(account).state is p.ObjectState.PERSISTENT
    assert p.inspect_instance(account).dirty == ()
    orm.release_savepoint("edit_account")
    assert transaction.savepoint_calls == [
        ("savepoint", "edit_account"),
        ("rollback", "edit_account"),
        ("release", "edit_account"),
    ]


def test_nested_selectinload_batches_each_level() -> None:
    transaction = _FakeTransaction()
    transaction.result_batches = [
        [{"id": 1}],
        [{"id": 10, "root_id": 1}],
        [{"id": 100, "middle_id": 10}],
    ]
    orm = p.OrmSession(_FakeSession(transaction))

    roots = (
        orm.query(OrmLoaderRoot)
        .options(p.selectinload(OrmLoaderRoot.middles, OrmLoaderMiddle.leaves))
        .all()
    )

    assert len(transaction.executed) == 3
    assert roots[0].middles[0].leaves[0].id == 100


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
        .where(Account.name == p.bind("wanted", p.BindType.STRING))
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


def test_geometry_mapping_uses_canonical_ewkb_and_unqualified_providers_fail_closed() -> (
    None
):
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
    rejected = p.OrmSession(_FakeSession(rejected_transaction, provider="sqlserver"))
    rejected.add(Place(id=4, shape=ewkb))
    with pytest.raises(p.OrmUnsupportedError, match="Geometry ORM"):
        rejected.flush()
    assert rejected_transaction.executed == []

    orm.rollback()
    reader.rollback()


@pytest.mark.parametrize("provider", ("sqlserver", "db2", "oracle"))
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
    statement, parameters = transaction.executed[0]
    optional_expression = statement.to_ast()["rows"][0][4]
    assert optional_expression["kind"] == "spatial_value"
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


@pytest.mark.parametrize("provider", ("sqlserver", "db2", "oracle"))
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

    parents = orm.query(OrmParent).options(p.selectinload(OrmParent.children)).all()

    assert len(transaction.executed) == 2
    assert [item.label for item in parents[0].children] == ["child"]
    assert parents[0].children[0].parent is parents[0]


def test_joinedload_collection_deduplicates_roots_and_accumulates_children() -> None:
    transaction = _FakeTransaction(
        [
            {
                "id": 1,
                "name": "parent",
                "orm_eager_children_id": 2,
                "orm_eager_children_parent_id": 1,
                "orm_eager_children_label": "first",
            },
            {
                "id": 1,
                "name": "parent",
                "orm_eager_children_id": 3,
                "orm_eager_children_parent_id": 1,
                "orm_eager_children_label": "second",
            },
        ]
    )
    orm = p.OrmSession(_FakeSession(transaction), autoflush=False)

    parents = orm.query(OrmParent).options(p.joinedload(OrmParent.children)).all()

    assert len(parents) == 1
    assert [item.label for item in parents[0].children] == ["first", "second"]
    assert all(item.parent is parents[0] for item in parents[0].children)


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


def test_composite_many_to_many_flushes_all_key_components_once() -> None:
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction))
    tag = CompositeTag(tenant_id=7, code="etl")
    owner = CompositeOwner(tenant_id=7, code="source", tags=[tag])

    orm.add(owner)
    orm.flush()

    assert list(tag.owners) == [owner]
    assert [statement.target.name for statement, _ in transaction.executed] == [
        "orm_composite_owners",
        "orm_composite_tags",
        "orm_composite_links",
    ]
    _, parameters = transaction.executed[-1]
    assert parameters == {
        "orm_link_local_0": 7,
        "orm_link_local_1": "source",
        "orm_link_remote_0": 7,
        "orm_link_remote_1": "etl",
    }


def test_composite_many_to_many_selectinload_groups_by_full_owner_identity() -> None:
    transaction = _FakeTransaction()
    transaction.result_batches = [
        [{"tenant_id": 7, "code": "source"}],
        [
            {
                "tenant_id": 7,
                "code": "etl",
                "orm_eager_owner_0": 7,
                "orm_eager_owner_1": "source",
            }
        ],
    ]
    orm = p.OrmSession(_FakeSession(transaction), autoflush=False)

    owners = (
        orm.query(CompositeOwner).options(p.selectinload(CompositeOwner.tags)).all()
    )

    assert [(item.tenant_id, item.code) for item in owners[0].tags] == [(7, "etl")]
    statement, parameters = transaction.executed[1]
    assert len(statement.joins) == 1
    assert set(parameters.values()) == {7, "source"}


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
    assert [row.as_dict() for row in values] == [{"label": "child"}]

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


def test_abstract_mixin_and_concrete_inheritance_preserve_relationship_mapping() -> (
    None
):
    record = MixinRecord(id=1, label="mixed", created_by="system")
    assert record.created_by == "system"
    assert {item.name for item in MixinRecord.__mapper__.attributes} == {
        "created_by",
        "id",
        "label",
    }

    group = InheritedGroup(id=7)
    account = ConcreteRelatedAccount(id=1, category="staff", group=group)
    assert account.group_id == 7
    assert ConcreteRelatedAccount.__mapper__.inherits is RelatedAccount.__mapper__
    inherited = ConcreteRelatedAccount.__mapper__.relationship("group")
    assert inherited.owner is ConcreteRelatedAccount
    assert inherited is not RelatedAccount.__mapper__.relationship("group")


def test_composite_relationship_synchronizes_flushes_and_loads_as_one_identity() -> (
    None
):
    transaction = _FakeTransaction()
    orm = p.OrmSession(_FakeSession(transaction), autoflush=False)
    parent = CompositeParent(tenant_id=7, code="root", label="parent")
    child = CompositeChild(tenant_id=7, child_id=9, label="child")
    parent.children = [child]

    assert child.parent is parent
    assert (child.parent_tenant_id, child.parent_code) == (7, "root")
    orm.add(parent)
    orm.flush()
    assert [statement.target.name for statement, _ in transaction.executed] == [
        "orm_composite_parents",
        "orm_composite_children",
    ]

    transaction.rows = [
        {
            "tenant_id": 7,
            "child_id": 9,
            "parent_tenant_id": 7,
            "parent_code": "root",
            "label": "child",
        }
    ]
    loaded = orm.load(parent, CompositeParent.children)
    assert list(loaded) == [child]
    _, parameters = transaction.executed[-1]
    assert set(parameters.values()) == {7, "root"}


def test_selectinload_composite_relationship_uses_all_key_components() -> None:
    transaction = _FakeTransaction()
    transaction.result_batches = [
        [
            {"tenant_id": 7, "code": "a", "label": "first"},
            {"tenant_id": 7, "code": "b", "label": "second"},
        ],
        [
            {
                "tenant_id": 7,
                "child_id": 1,
                "parent_tenant_id": 7,
                "parent_code": "a",
                "label": "child-a",
            },
            {
                "tenant_id": 7,
                "child_id": 2,
                "parent_tenant_id": 7,
                "parent_code": "b",
                "label": "child-b",
            },
        ],
    ]
    orm = p.OrmSession(_FakeSession(transaction), autoflush=False)
    parents = (
        orm.query(CompositeParent)
        .options(p.selectinload(CompositeParent.children))
        .all()
    )

    assert [[child.label for child in parent.children] for parent in parents] == [
        ["child-a"],
        ["child-b"],
    ]
    _, parameters = transaction.executed[-1]
    assert set(parameters.values()) == {7, "a", "b"}


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
            p.CheckConstraint("tenant", ">", 0, name="ck_child_tenant"),
            p.OrmIndex("parent_code", name="ix_child_parent_code"),
            p.ForeignKeyConstraint(
                ("tenant", "parent_code"),
                DdlParent,
                ("tenant", "code"),
                on_delete="CASCADE",
                on_update="CASCADE",
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

    assert len(ddl) == 3
    assert "CREATE TABLE IF NOT EXISTS" in ddl[0]
    assert '"tenant" INTEGER' in ddl[0]
    assert "FOREIGN KEY" in ddl[1]
    assert "UNIQUE" in ddl[1]
    assert 'CHECK ("tenant" > 0)' in ddl[1]
    assert "ON UPDATE CASCADE" in ddl[1]
    assert "DEFAULT CURRENT_TIMESTAMP" in ddl[1]
    assert ddl[2] == (
        'CREATE INDEX IF NOT EXISTS "ix_child_parent_code" '
        'ON "ddl_child" ("parent_code")'
    )

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
    assert len(ddl_session.statements) == 5
    migrations = (
        _migration("001", None, lambda tx: None, lambda tx: None),
        _migration("002", "001", lambda tx: None, lambda tx: None),
    )
    runner = p.MigrationRunner(migrations)
    assert [item.revision for item in runner.migrations] == ["001", "002"]
    with pytest.raises(p.OrmMappingError, match="genitore assente"):
        p.MigrationRunner((_migration("002", "missing", lambda tx: None),))


def test_migration_runner_orders_branches_and_merges_and_rejects_broken_history() -> (
    None
):
    noop = lambda tx: None
    runner = p.MigrationRunner(
        (
            _migration("merge", ("left", "right"), noop, noop),
            _migration("root", None, noop, noop),
            _migration("right", "root", noop, noop),
            _migration("left", "root", noop, noop),
        )
    )
    assert [item.revision for item in runner.migrations] == [
        "root",
        "right",
        "left",
        "merge",
    ]

    class BrokenHistory:
        capabilities: ClassVar[dict[str, str]] = {"provider": "postgres"}

        def execute_ddl(self, statement: str) -> None:
            pass

        def execute_sql(self, statement: str) -> p.MutationResult:
            return p.MutationResult("seed", "postgres", 0)

        def query_sql(self, statement: str) -> p.Result:
            return p.Result(
                [
                    {
                        "revision": "merge",
                        "checksum": hashlib.sha256(b"merge").hexdigest(),
                        "state": "applied",
                    }
                ]
            )

    with pytest.raises(p.OrmStateError, match="antenati"):
        runner.apply(BrokenHistory())

    with pytest.raises(p.OrmMappingError, match="ciclico"):
        p.MigrationRunner(
            (
                _migration("a", "b", noop),
                _migration("b", "a", noop),
            )
        )


def test_db2_migration_history_ddl_is_idempotent_and_uses_the_ddl_channel() -> None:
    class Db2Session:
        capabilities: ClassVar[dict[str, str]] = {"provider": "db2"}

        def __init__(self) -> None:
            self.ddl: list[str] = []
            self.exists = False

        def execute_ddl(self, statement: str) -> None:
            self.ddl.append(statement)
            self.exists = True

        def execute_sql(self, statement: str) -> p.MutationResult:
            return p.MutationResult("seed", "db2", 0)

        def query_sql(self, statement: str) -> p.Result:
            return p.Result([])

        def execute_scalar(self, statement: str) -> int:
            return int(self.exists)

    session = Db2Session()
    assert p.MigrationRunner(()).apply(session) == ()
    assert p.MigrationRunner(()).apply(session) == ()
    assert len(session.ddl) == 1
    assert session.ddl[0].startswith('CREATE TABLE "_plenora_orm_migrations"')
    assert "CURRENT TIMESTAMP" in session.ddl[0]


def test_oracle_migration_history_uses_catalog_guard_and_native_merge() -> None:
    class OracleSession:
        capabilities: ClassVar[dict[str, str]] = {"provider": "oracle"}

        def __init__(self) -> None:
            self.ddl: list[str] = []
            self.sql: list[str] = []
            self.catalog_queries: list[str] = []
            self.exists = False

        def execute_ddl(self, statement: str) -> None:
            self.ddl.append(statement)
            self.exists = True

        def execute_sql(self, statement: str) -> p.MutationResult:
            self.sql.append(statement)
            return p.MutationResult("seed", "oracle", 0)

        def query_sql(self, statement: str) -> p.Result:
            return p.Result([])

        def execute_scalar(self, statement: str) -> int:
            self.catalog_queries.append(statement)
            return int(self.exists)

    session = OracleSession()
    assert p.MigrationRunner(()).apply(session) == ()
    assert p.MigrationRunner(()).apply(session) == ()
    assert len(session.ddl) == 1
    assert session.ddl[0].startswith('CREATE TABLE "_plenora_orm_migrations"')
    assert "VARCHAR2(255)" in session.ddl[0]
    assert all("USER_TABLES" in statement for statement in session.catalog_queries)
    assert all("MERGE INTO" in statement and "FROM DUAL" in statement for statement in session.sql)


def test_migration_runner_applies_and_rolls_back_transactionally() -> None:
    calls: list[str] = []

    class MigrationTransaction:
        def __init__(self, session) -> None:
            self.session = session
            self.staged = [dict(row) for row in session.applied]
            self.committed = False
            self.rolled_back = False

        def query_sql(self, statement: str) -> p.Result:
            if "= '__plenora_lock__'" in statement:
                self.session.lock_reads += 1
                return p.Result([{"revision": "__plenora_lock__"}])
            return p.Result(self.staged)

        def execute(self, statement, params=None) -> p.MutationResult:
            parameters = {} if params is None else dict(params)
            revision = parameters.get("orm_revision")
            if isinstance(statement, p.InsertStatement):
                self.staged.append(
                    {
                        "revision": revision,
                        "checksum": parameters["orm_checksum"],
                        "state": parameters["orm_state"],
                    }
                )
            elif isinstance(statement, p.UpdateStatement):
                for row in self.staged:
                    if row["revision"] == revision:
                        row["state"] = parameters["orm_state"]
            elif isinstance(statement, p.DeleteStatement):
                self.staged = [
                    row for row in self.staged if row["revision"] != revision
                ]
            return p.MutationResult("migration", "postgres", 1)

        def commit(self) -> None:
            self.session.applied = self.staged
            self.committed = True

        def rollback(self) -> None:
            self.rolled_back = True

    class MigrationSession:
        capabilities: ClassVar[dict[str, str]] = {"provider": "postgres"}

        def __init__(self) -> None:
            self.applied: list[dict[str, str]] = []
            self.raw: list[str] = []
            self.transactions: list[MigrationTransaction] = []
            self.lock_reads = 0

        def execute_sql(self, statement: str) -> p.MutationResult:
            self.raw.append(statement)
            return p.MutationResult("seed", "postgres", 0)

        def execute_ddl(self, statement: str) -> None:
            self.raw.append(statement)

        def query_sql(self, statement: str) -> p.Result:
            self.raw.append(statement)
            return p.Result(self.applied)

        def begin(self) -> MigrationTransaction:
            transaction = MigrationTransaction(self)
            self.transactions.append(transaction)
            return transaction

    migrations = (
        _migration(
            "001",
            None,
            lambda tx: calls.append("up-001"),
            lambda tx: calls.append("down-001"),
        ),
        _migration(
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
    assert session.lock_reads == 2
    session.applied = [
        {
            "revision": migration.revision,
            "checksum": migration.checksum,
            "state": "applied",
        }
        for migration in reversed(migrations)
    ]
    assert runner.rollback(session) == ("002",)
    assert calls[-1] == "down-002"

    session.applied.append(
        {
            "revision": migrations[1].revision,
            "checksum": migrations[1].checksum,
            "state": "failed",
        }
    )
    with pytest.raises(p.OrmStateError, match="incompleta"):
        runner.apply(session)
    runner.recover(session, "002")
    assert [row["revision"] for row in session.applied] == ["001"]
    assert runner.apply(session) == ("002",)

    session.applied[0]["checksum"] = "f" * 64
    with pytest.raises(p.OrmStateError, match="drift checksum"):
        runner.apply(session)


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
    try:
        _drop_db2_models_if_present(session, LivePortableGenerated)
        metadata.create_all(session)
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
        _drop_db2_models_if_present(session, LivePortableGenerated)
        session.close()


def test_live_db2_migration_dag_is_idempotent_and_reversible() -> None:
    session = connect_db2_reference()

    def existing_tables() -> set[str]:
        schema = session.execute_scalar("VALUES CURRENT SCHEMA")
        return {item.get("name", "").lower() for item in session.inspect.tables(schema)}

    def cleanup() -> None:
        existing = existing_tables()
        for table_name in ("_plenora_migration_probe", "_plenora_orm_migrations"):
            if table_name in existing:
                session.execute_ddl(f'DROP TABLE "{table_name}"')

    migrations = (
        _migration(
            "root",
            None,
            lambda tx: tx.execute_sql(
                'CREATE TABLE "_plenora_migration_probe" '
                '("id" INTEGER NOT NULL PRIMARY KEY)'
            ),
            lambda tx: tx.execute_sql('DROP TABLE "_plenora_migration_probe"'),
        ),
        _migration("left", "root", lambda tx: None, lambda tx: None),
        _migration("right", "root", lambda tx: None, lambda tx: None),
        _migration("merge", ("left", "right"), lambda tx: None, lambda tx: None),
    )
    runner = p.MigrationRunner(migrations)
    try:
        cleanup()
        assert runner.apply(session) == ("root", "left", "right", "merge")
        assert runner.apply(session) == ()
        assert "_plenora_migration_probe" in existing_tables()
        assert runner.rollback(session, steps=4) == (
            "merge",
            "right",
            "left",
            "root",
        )
        assert "_plenora_migration_probe" not in existing_tables()
    finally:
        cleanup()
        session.close()


def _drop_db2_models_if_present(
    session: p.DatabaseSession, *models: type[p.DeclarativeBase]
) -> None:
    schema = session.execute_scalar("VALUES CURRENT SCHEMA")
    existing = {item.get("name", "").lower() for item in session.inspect.tables(schema)}
    for model in reversed(models):
        table_name = model.__table__.name.lower()
        if table_name in existing:
            p.OrmMetadata(models=(model,)).drop_all(session, checkfirst=False)
            existing.remove(table_name)


_LIVE_ADVANCED_ORM_MODELS = (
    LiveOrmEntity,
    LiveOrmService,
    LiveOrmDatabase,
    LiveOrmAsset,
    LiveOrmMachine,
    LiveOrmRackMachine,
    LiveOrmBigRecord,
    LiveOrmLoaderRoot,
    LiveOrmLoaderMiddle,
    LiveOrmLoaderLeaf,
)


def _exercise_live_advanced_orm(provider: str, connector) -> None:
    session = connector()
    assert session.capabilities["provider"] == provider
    metadata = p.OrmMetadata(models=_LIVE_ADVANCED_ORM_MODELS)
    try:
        if provider == "db2":
            _drop_db2_models_if_present(session, *_LIVE_ADVANCED_ORM_MODELS)
        else:
            metadata.drop_all(session)
        metadata.create_all(session)

        with p.OrmSession(session) as orm:
            orm.add(
                LiveOrmDatabase(
                    id=1,
                    name="primary",
                    port=5432,
                    engine="postgres",
                )
            )
            orm.add(
                LiveOrmRackMachine(
                    id=2,
                    name="rack-01",
                    cores=32,
                    rack_units=2,
                )
            )
            orm.add_all(
                (
                    LiveOrmBigRecord(id=2**40, counter=7),
                    LiveOrmBigRecord(id=2**40 + 1, counter=9),
                )
            )
            orm.add(
                LiveOrmLoaderRoot(
                    id=3,
                    middles=[
                        LiveOrmLoaderMiddle(
                            id=4,
                            leaves=[LiveOrmLoaderLeaf(id=5)],
                        )
                    ],
                )
            )

        with p.OrmSession(session) as orm:
            entity = orm.query(LiveOrmEntity).one()
            assert isinstance(entity, LiveOrmDatabase)
            assert entity.engine == "postgres"
            machine = orm.query(LiveOrmRackMachine).one()
            assert machine.cores == 32 and machine.rack_units == 2
            roots = (
                orm.query(LiveOrmLoaderRoot)
                .options(
                    p.selectinload(
                        LiveOrmLoaderRoot.middles, LiveOrmLoaderMiddle.leaves
                    )
                )
                .all()
            )
            assert roots[0].middles[0].leaves[0].id == 5
            assert orm.query(LiveOrmBigRecord).count() == 2
            assert orm.query(LiveOrmBigRecord).exists()
            partitions = list(
                orm.query(LiveOrmBigRecord)
                .order_by(LiveOrmBigRecord.id.asc().nulls_last())
                .partitions(1, detach=True)
            )
            assert [len(partition) for partition in partitions] == [1, 1]

        if provider == "postgres":
            with p.OrmSession(session) as orm:
                locked = (
                    orm.query(LiveOrmBigRecord)
                    .where(
                        LiveOrmBigRecord.id
                        == p.bind("locked_id", p.BindType.BIG_INTEGER)
                    )
                    .with_for_update(nowait=True)
                    .one({"locked_id": 2**40})
                )
                assert locked.counter == 7
                assert (
                    orm.query(LiveOrmBigRecord)
                    .distinct_on(LiveOrmBigRecord.id)
                    .order_by(LiveOrmBigRecord.id)
                    .count()
                    == 2
                )

        with p.OrmSession(session) as orm:
            record = orm.get(LiveOrmBigRecord, 2**40)
            assert record is not None
            try:
                with orm.begin_nested("advanced_orm_edit"):
                    record.counter = 99
                    orm.flush()
                    raise RuntimeError("rollback fixture")
            except RuntimeError:
                pass
            assert record.counter == 7

        with p.OrmSession(session) as orm:
            selected = orm.query(LiveOrmBigRecord).where(
                LiveOrmBigRecord.id == p.bind("record_id", p.BindType.BIG_INTEGER)
            )
            assert selected.update({"counter": 8}, {"record_id": 2**40}) == 1
        with p.OrmSession(session) as orm:
            record = orm.get(LiveOrmBigRecord, 2**40)
            assert record is not None and record.counter == 8
            assert (
                orm.query(LiveOrmBigRecord)
                .where(
                    LiveOrmBigRecord.id
                    == p.bind("record_id", p.BindType.BIG_INTEGER)
                )
                .delete({"record_id": 2**40})
                == 1
            )
    finally:
        if provider == "db2":
            _drop_db2_models_if_present(session, *_LIVE_ADVANCED_ORM_MODELS)
        else:
            metadata.drop_all(session)
        session.close()


@pytest.mark.parametrize(
    ("provider", "connector"),
    (
        ("postgres", connect_postgres),
        ("mysql", connect_mysql_reference),
        ("mariadb", connect_mariadb_reference),
        ("sqlserver", connect_sqlserver_reference),
    ),
)
def test_live_advanced_orm_qualification(provider: str, connector) -> None:
    _exercise_live_advanced_orm(provider, connector)


def test_live_db2_advanced_orm_qualification() -> None:
    _exercise_live_advanced_orm("db2", connect_db2_reference)


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
            [queried] = (
                orm.query(LiveMysqlGeometry)
                .where(predicate)
                .all({"reference": moved_point})
            )
            assert queried.point.ewkb == _root_wkb(moved_point)
            assert queried.optional_point is not None
            assert queried.optional_point.ewkb == _root_wkb(point)
            orm.delete(queried)

        assert (
            session.execute_scalar("SELECT COUNT(*) FROM _plenora_orm_mysql_geometry")
            == 0
        )
    finally:
        metadata.drop_all(session)
        session.close()


def _exercise_live_portable_geometry(provider: str, connector) -> None:
    session = connector()
    assert session.capabilities["provider"] == provider
    metadata = p.OrmMetadata(models=(LivePortableGeometry, LivePortableGeometryXyz))
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
        if provider == "db2":
            _drop_db2_models_if_present(
                session, LivePortableGeometry, LivePortableGeometryXyz
            )
        else:
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
            _assert_portable_wkb(loaded.point.ewkb, _root_wkb(point), provider)
            _assert_portable_wkb(loaded.line.ewkb, _root_wkb(line), provider)
            _assert_portable_wkb(loaded.polygon.ewkb, _root_wkb(polygon), provider)
            assert loaded.optional_point is None
            assert loaded.point.srid == 4326
            _assert_portable_wkb(loaded_xyz.shape.ewkb, _root_wkb(point_xyz), provider)
            assert loaded_xyz.shape.dimensions == "xyz"
            loaded.point = moved_point
            loaded.optional_point = point

        predicate = _PORTABLE_POINT.predicate(
            "intersects",
            LivePortableGeometry.point,
            _PORTABLE_POINT.bind("reference"),
        )
        with p.OrmSession(session) as orm:
            [queried] = (
                orm.query(LivePortableGeometry)
                .where(predicate)
                .all({"reference": moved_point})
            )
            _assert_portable_wkb(queried.point.ewkb, _root_wkb(moved_point), provider)
            assert queried.optional_point is not None
            _assert_portable_wkb(
                queried.optional_point.ewkb, _root_wkb(point), provider
            )
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
        if provider == "db2":
            _drop_db2_models_if_present(
                session, LivePortableGeometry, LivePortableGeometryXyz
            )
        else:
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
            p.Geometry(srid=4326, semantics="geography", geometry_type="point").bind(
                "reference"
            ),
        )
        with p.OrmSession(session) as orm:
            [loaded] = (
                orm.query(LiveSqlServerGeography)
                .where(predicate)
                .all({"reference": point})
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
    await setup.execute_sql("DROP TABLE IF EXISTS _plenora_async_orm_places")
    await setup.execute_sql(
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
        await setup.execute_sql("DROP TABLE IF EXISTS _plenora_async_orm_places")
        setup.close()


def test_live_postgres_orm_lifecycle_and_optimistic_conflict() -> None:
    setup = connect_postgres()
    setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_accounts")
    setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_audit_entries")
    setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_generated_accounts")
    setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_places")
    setup.execute_sql(
        "CREATE TABLE _plenora_orm_accounts ("
        "id INT PRIMARY KEY, name TEXT NOT NULL, version INT NOT NULL)"
    )
    setup.execute_sql(
        "CREATE TABLE _plenora_orm_generated_accounts ("
        "id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
        "name TEXT NOT NULL, created_by TEXT NOT NULL DEFAULT 'database')"
    )
    setup.execute_sql(
        "CREATE TABLE _plenora_orm_audit_entries ("
        "id INT PRIMARY KEY, account_id INT NOT NULL REFERENCES "
        "_plenora_orm_generated_accounts(id), message TEXT NOT NULL)"
    )
    setup.execute_sql(
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
                .where(LiveAccount.name == p.bind("wanted", p.BindType.STRING))
                .one({"wanted": "Grace"})
            )
            assert queried is current
            orm.delete(current)
        assert (
            setup.execute_scalar("SELECT COUNT(*)::BIGINT FROM _plenora_orm_accounts")
            == 0
        )
    finally:
        setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_accounts")
        setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_audit_entries")
        setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_generated_accounts")
        setup.execute_sql("DROP TABLE IF EXISTS _plenora_orm_places")
        setup.close()
