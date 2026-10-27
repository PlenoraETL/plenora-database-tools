"""Qualifica live della superficie applicativa Oracle thin."""

from __future__ import annotations

import struct
from datetime import datetime, timedelta, timezone

import plenora_database as p
import pytest

from ._harness import (
    aconnect_oracle_reference,
    connect_oracle_reference,
    oracle_config_or_skip,
)


def _drop_probe(session, name: str) -> None:
    try:
        session.execute_ddl(f'DROP TABLE "{name}" PURGE')
    except p.PlenoraError:
        pass


def _ewkb_point(x: float, y: float, srid: int = 4326) -> bytes:
    return struct.pack("<BII2d", 1, 0x20000001, srid, x, y)


def test_oracle_sync_catalog_and_portable_crud() -> None:
    session = connect_oracle_reference()
    name = "PLENORA_PY_ORACLE_PROBE"
    try:
        assert session.provider_capabilities["provider"] == "oracle"
        assert "PLENORA" in session.inspect.schemas()
        _drop_probe(session, name)
        session.execute_ddl(
            f'CREATE TABLE "{name}" ('
            '"ID" NUMBER(10) NOT NULL PRIMARY KEY, '
            '"LABEL" VARCHAR2(64) NOT NULL)'
        )
        assert session.inspect.describe("PLENORA", name)["columns"]
        target = p.table(name, "ID", "LABEL")
        transaction = session.begin()
        try:
            inserted = transaction.execute(
                p.insert(target).values(
                    ID=p.bind("identity", p.BindType.INTEGER),
                    LABEL=p.bind("label", p.BindType.STRING),
                ),
                {"identity": 7, "label": "first"},
            )
            assert inserted.affected_rows == 1
            transaction.savepoint("before_merge")
            merged = transaction.execute(
                p.upsert(target)
                .values(
                    ID=p.bind("identity", p.BindType.INTEGER),
                    LABEL=p.bind("insert_label", p.BindType.STRING),
                )
                .on_conflict(target.c.ID)
                .set(LABEL=p.bind("update_label", p.BindType.STRING)),
                {
                    "identity": 7,
                    "insert_label": "ignored",
                    "update_label": "merged",
                },
            )
            assert merged.affected_rows == 1
            assert transaction.execute(
                p.select(target.c.LABEL)
                .where(target.c.ID == p.bind("identity", p.BindType.INTEGER))
                .limit(1),
                {"identity": 7},
            ).scalar_one() == "merged"
            transaction.rollback_to_savepoint("before_merge")
            transaction.commit()
        except BaseException:
            transaction.rollback()
            raise
        assert session.execute_scalar(
            f'SELECT "LABEL" FROM "{name}" WHERE "ID" = :1', [7]
        ) == "first"
    finally:
        try:
            _drop_probe(session, name)
        finally:
            session.close()


@pytest.mark.asyncio
async def test_oracle_async_engine_and_bound_scalar() -> None:
    session = await aconnect_oracle_reference()
    try:
        assert session.provider_capabilities["provider"] == "oracle"
        assert await session.execute_scalar(
            "SELECT CAST(:1 AS NUMBER(10)) FROM DUAL", [42]
        ) == 42
        assert "PLENORA" in await session.inspect.schemas()
    finally:
        session.close()


def test_oracle_assigned_key_orm_crud() -> None:
    class OracleApplicationRecord(p.DeclarativeBase):
        __tablename__ = "PLENORA_PY_ORACLE_ORM"

        id: p.Mapped[int] = p.mapped_column(primary_key=True)
        label: p.Mapped[str] = p.mapped_column(p.String(64), nullable=False)
        version: p.Mapped[int] = p.mapped_column(version=True, nullable=False)

    session = connect_oracle_reference()
    metadata = p.OrmMetadata(models=(OracleApplicationRecord,))
    try:
        _drop_probe(session, OracleApplicationRecord.__tablename__)
        metadata.create_all(session)
        with p.OrmSession(session) as orm:
            record = OracleApplicationRecord(id=11, label="created", version=1)
            orm.add(record)
        with p.OrmSession(session) as orm:
            loaded = orm.get(OracleApplicationRecord, 11)
            assert loaded is not None
            loaded.label = "updated"
        with p.OrmSession(session) as orm:
            loaded = orm.get(OracleApplicationRecord, 11)
            assert loaded is not None
            assert loaded.label == "updated"
            assert loaded.version == 2
            orm.delete(loaded)
        assert session.execute_scalar(
            'SELECT COUNT(*) FROM "PLENORA_PY_ORACLE_ORM"'
        ) == 0
    finally:
        try:
            _drop_probe(session, OracleApplicationRecord.__tablename__)
        finally:
            session.close()


def test_oracle_spatial_orm_crud_index_and_predicates() -> None:
    geometry = p.Geometry(
        srid=4326, geometry_type="point", semantics="geography"
    )

    class OracleSpatialRecord(p.DeclarativeBase):
        __tablename__ = "PLENORA_PY_ORACLE_SPATIAL_ORM"
        __table_args__ = (
            p.OrmIndex("SHAPE", name="IX_PLENORA_PY_ORACLE_SPATIAL"),
        )

        ID: p.Mapped[int] = p.mapped_column(primary_key=True)
        SHAPE: p.Mapped[p.SpatialReference] = p.mapped_column(
            geometry, nullable=False
        )

    session = connect_oracle_reference()
    metadata = p.OrmMetadata(models=(OracleSpatialRecord,))
    point = _ewkb_point(12.5, 41.9)
    moved = _ewkb_point(12.51, 41.91)
    try:
        _drop_probe(session, OracleSpatialRecord.__tablename__)
        metadata.create_all(session)
        assert session.provider_capabilities["spatial"]["geometry"] is True
        assert session.provider_capabilities["spatial"]["geography"] is True
        # Il mapping e USER_SDO_GEOM_METADATA non sono una prova del vincolo
        # SRID. Il compilatore non legge il catalogo: INSERT/UPDATE restano
        # chiusi finche una guardia qualificata non fornisce quella prova.
        with pytest.raises(p.PlenoraUnsupportedError, match="senza verifica dello SRID"):
            with p.OrmSession(session) as orm:
                orm.add(OracleSpatialRecord(ID=1, SHAPE=point))
        assert session.execute_scalar(
            'SELECT COUNT(*) FROM "PLENORA_PY_ORACLE_SPATIAL_ORM"'
        ) == 0
        # Fixture nativo esplicito per continuare a provare lettura e DELETE
        # ORM, che non scrivono valori geometrici ne confrontano geometrie.
        session.execute_sql(
            'INSERT INTO "PLENORA_PY_ORACLE_SPATIAL_ORM" ("ID", "SHAPE") VALUES '
            '(1, MDSYS.SDO_GEOMETRY(2001, 4326, '
            'MDSYS.SDO_POINT_TYPE(12.5, 41.9, NULL), NULL, NULL))'
        )
        with pytest.raises(p.PlenoraUnsupportedError, match="senza verifica dello SRID"):
            with p.OrmSession(session) as orm:
                loaded = orm.get(OracleSpatialRecord, 1)
                assert loaded is not None
                assert loaded.SHAPE.srid == 4326
                assert loaded.SHAPE.dimensions == "xy"
                loaded.SHAPE = moved
        assert session.execute_scalar(
            'SELECT t."SHAPE".SDO_POINT.X FROM "PLENORA_PY_ORACLE_SPATIAL_ORM" t'
        ) == 12.5

        intersects = geometry.predicate(
            "intersects", OracleSpatialRecord.SHAPE, geometry.bind("reference")
        )
        within = geometry.predicate(
            "d_within",
            OracleSpatialRecord.SHAPE,
            geometry.bind("reference"),
            p.bind("distance", p.BindType.FLOAT),
        )
        for predicate, parameters in (
            (intersects, {"reference": moved}),
            (within, {"reference": moved, "distance": 1.0}),
        ):
            with pytest.raises(p.PlenoraUnsupportedError, match="non verifica lo SRID"):
                with p.OrmSession(session) as orm:
                    orm.query(OracleSpatialRecord).where(predicate).all(parameters)
        with p.OrmSession(session) as orm:
            loaded = orm.get(OracleSpatialRecord, 1)
            assert loaded is not None
            orm.delete(loaded)
        assert session.execute_scalar(
            'SELECT COUNT(*) FROM "PLENORA_PY_ORACLE_SPATIAL_ORM"'
        ) == 0
    finally:
        try:
            _drop_probe(session, OracleSpatialRecord.__tablename__)
        finally:
            session.close()


def test_oracle_configurable_pool_is_accepted_by_the_live_engine() -> None:
    config = oracle_config_or_skip()
    pooled = p.EngineConfig(
        config.provider,
        config.host,
        config.database,
        config.user,
        config.password,
        config.port,
        config.tls_mode,
        config.tls_ca,
        p.PoolConfig(max_connections=2, acquire_timeout_ms=1_000),
    )
    session = p.engine_from_url(pooled).session()
    try:
        assert session.execute_scalar("SELECT CAST(1 AS NUMBER(10)) FROM DUAL") == 1
    finally:
        session.close()


def test_oracle_generated_identity_defaults_and_timestamptz_bind() -> None:
    class OracleGeneratedEvent(p.DeclarativeBase):
        __tablename__ = "PLENORA_PY_ORACLE_GENERATED"

        id: p.Mapped[int] = p.mapped_column(primary_key=True, generated=True)
        observed_at: p.Mapped[datetime] = p.mapped_column(
            p.DateTime(timezone=True), nullable=False
        )
        created_by: p.Mapped[str] = p.mapped_column(
            p.String(32),
            nullable=False,
            server_default=p.ServerDefault.literal("database"),
        )

    session = connect_oracle_reference()
    metadata = p.OrmMetadata(models=(OracleGeneratedEvent,))
    observed = datetime(
        2026, 9, 3, 10, 11, 12, 123456, tzinfo=timezone(timedelta(hours=2, minutes=30))
    )
    try:
        _drop_probe(session, OracleGeneratedEvent.__tablename__)
        metadata.create_all(session)
        with p.OrmSession(session) as orm:
            event = OracleGeneratedEvent(observed_at=observed)
            orm.add(event)
        assert event.id is not None
        assert event.created_by == "database"
        with p.OrmSession(session) as orm:
            loaded = orm.get(OracleGeneratedEvent, event.id)
            assert loaded is not None
            assert loaded.observed_at.utcoffset() == timedelta(hours=2, minutes=30)
    finally:
        try:
            _drop_probe(session, OracleGeneratedEvent.__tablename__)
        finally:
            session.close()


# Decimali in (-1, 1) con zeri dopo il punto pari e dispari: il ramo che il
# codec NUMBER del driver salvava per dieci volte se stesso (0.05 come .5).
_NUMBER_PROBES = ("0.05", "0.5", "-0.05", "-0.5", "0.0005", "0.005", "0.07", "1.5", "1E-130")


def test_oracle_decimal_binds_and_reads_are_exact() -> None:
    from decimal import Decimal

    session = connect_oracle_reference()
    name = "PLENORA_PY_ORACLE_NUMBER"
    try:
        _drop_probe(session, name)
        session.execute_ddl(
            f'CREATE TABLE "{name}" ("ID" NUMBER(10) PRIMARY KEY, "D" NUMBER)'
        )
        for identity, text in enumerate(_NUMBER_PROBES, start=1):
            session.execute_sql(
                f'INSERT INTO "{name}" ("ID", "D") VALUES (:1, :2)',
                [identity, p.decimal(text)],
            )
        for identity, text in enumerate(_NUMBER_PROBES, start=1):
            # Il server, senza il decoder del driver.
            server = session.execute_scalar(
                f"SELECT TO_CHAR(\"D\", 'TM9') FROM \"{name}\" WHERE \"ID\" = :1",
                [identity],
            )
            assert Decimal(server) == Decimal(text), (text, server)
            # Uguaglianza con il valore legato: esattamente la propria riga.
            assert session.execute_scalar(
                f'SELECT COUNT(*) FROM "{name}" WHERE "D" = :1',
                [p.decimal(text)],
            ) == 1, text
            # Il decoder del driver.
            read = session.execute_scalar(
                f'SELECT "D" FROM "{name}" WHERE "ID" = :1', [identity]
            )
            assert Decimal(str(read)) == Decimal(text), (text, read)
    finally:
        try:
            _drop_probe(session, name)
        finally:
            session.close()
    # -infinito e +infinito non sono decimali: la lettura fallisce, non rende
    # "" o 1e126. Una sessione per lettura: l'errore la chiude.
    for raw in ("00", "FF65"):
        session = connect_oracle_reference()
        try:
            with pytest.raises(p.PlenoraDataMappingError) as raised:
                session.execute_scalar(
                    f"SELECT UTL_RAW.CAST_TO_NUMBER(HEXTORAW('{raw}')) FROM DUAL"
                )
            assert raised.value.category == "data_mapping"
        finally:
            session.close()
