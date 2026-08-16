"""F3-6c — Test integrazione live per spatial builder (where_spatial)."""
from __future__ import annotations

import os

import pytest

import plenora_database as p

from ._harness import connect_postgres, postgres_dsn_or_skip


# POINT(0 0) con SRID 4326: 1 byte di endianness + 4 di tipo + 4 di SRID +
# due double = 25 byte. Serve un EWKB **valido** perche il costruttore lo
# verifica davvero: i byte nulli usati prima passavano solo finche nessuno
# guardava, e questi test non parlano di validazione EWKB.
VALID_POINT_EWKB = bytes.fromhex("0101000020e610000000000000000000000000000000000000")


def _postgis_or_skip(session) -> None:
    if session.postgis_version is None:
        pytest.skip("live test: PostGIS non installato sul target")


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    _postgis_or_skip(s)
    try:
        yield s
    finally:
        s.close()


# ================================ validation ================================


def test_spatial_reference_rejects_non_bytes() -> None:
    with pytest.raises(TypeError, match="bytes"):
        p.spatial.SpatialReference(ewkb="not bytes", srid=4326)


def test_spatial_reference_rejects_negative_srid() -> None:
    with pytest.raises(ValueError, match="srid"):
        p.spatial.SpatialReference(ewkb=b"\x00", srid=-1)


def test_spatial_reference_rejects_bad_dimensions() -> None:
    with pytest.raises(ValueError, match="dimensions"):
        p.spatial.SpatialReference(ewkb=b"\x00", srid=4326, dimensions="4d")


def test_spatial_reference_rejects_bad_semantics() -> None:
    with pytest.raises(ValueError, match="semantics"):
        p.spatial.SpatialReference(ewkb=b"\x00", srid=4326, semantics="both")


def test_where_spatial_rejects_bad_predicate(session) -> None:
    ref = p.spatial.geometry(ewkb=VALID_POINT_EWKB, srid=4326)
    with pytest.raises(ValueError, match="predicato spaziale non valido"):
        session.select("t").where_spatial("g", "outside", ref)


def test_where_spatial_dwithin_requires_distance(session) -> None:
    ref = p.spatial.geometry(ewkb=VALID_POINT_EWKB, srid=4326)
    with pytest.raises(ValueError, match="distance_meters"):
        session.select("t").where_spatial("g", "d_within", ref)


def test_where_spatial_distance_only_for_dwithin(session) -> None:
    ref = p.spatial.geometry(ewkb=VALID_POINT_EWKB, srid=4326)
    with pytest.raises(ValueError, match="distance_meters non ammesso"):
        session.select("t").where_spatial("g", "intersects", ref, distance_meters=1.0)


# ================================ end-to-end ================================


def _get_ref_ewkb(session, wkt: str, srid: int) -> bytes:
    """Costruisce l'EWKB di un riferimento via PostGIS."""
    return session.execute_scalar(
        f"SELECT ST_AsEWKB(ST_SetSRID(ST_GeomFromText($1), {srid}))",
        [wkt],
    )


def test_spatial_intersects_geometry_polygon(session) -> None:
    session.execute("DROP TABLE IF EXISTS _pyf6c_poi")
    session.execute(
        "CREATE TABLE _pyf6c_poi ("
        " id INT PRIMARY KEY,"
        " name TEXT NOT NULL,"
        " geom geometry(Point, 4326))"
    )
    try:
        session.execute(
            "INSERT INTO _pyf6c_poi (id, name, geom) VALUES "
            "(1, 'inside', ST_SetSRID(ST_MakePoint(5, 45), 4326)),"
            "(2, 'outside', ST_SetSRID(ST_MakePoint(100, 100), 4326))"
        )
        # Bounding polygon (0..10, 40..50).
        ref_ewkb = _get_ref_ewkb(
            session, "POLYGON((0 40, 10 40, 10 50, 0 50, 0 40))", 4326
        )
        ref = p.spatial.geometry(ewkb=ref_ewkb, srid=4326)

        rows = (
            session.select("_pyf6c_poi")
            .columns("name")
            .where_spatial("geom", "intersects", ref)
            .order_by("id")
            .all()
        )
        assert [r["name"] for r in rows] == ["inside"]
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6c_poi")


def test_spatial_bounding_box_uses_index_operator(session) -> None:
    session.execute("DROP TABLE IF EXISTS _pyf6c_bb")
    session.execute(
        "CREATE TABLE _pyf6c_bb (id INT PRIMARY KEY, geom geometry(Point, 4326))"
    )
    try:
        session.execute(
            "INSERT INTO _pyf6c_bb (id, geom) VALUES "
            "(1, ST_SetSRID(ST_MakePoint(5, 45), 4326)),"
            "(2, ST_SetSRID(ST_MakePoint(100, 100), 4326))"
        )
        ref_ewkb = _get_ref_ewkb(session, "POLYGON((0 40, 10 40, 10 50, 0 50, 0 40))", 4326)
        ref = p.spatial.geometry(ewkb=ref_ewkb, srid=4326)
        rows = (
            session.select("_pyf6c_bb")
            .columns("id")
            .where_spatial("geom", "bounding_box", ref)
            .all()
        )
        assert [r["id"] for r in rows] == [1]
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6c_bb")


def test_spatial_contains_within_predicates(session) -> None:
    # Setup: 3 polygons con relazioni note al ref.
    session.execute("DROP TABLE IF EXISTS _pyf6c_regions")
    session.execute(
        "CREATE TABLE _pyf6c_regions ("
        " id INT PRIMARY KEY,"
        " name TEXT NOT NULL,"
        " geom geometry(Polygon, 4326))"
    )
    try:
        session.execute(
            "INSERT INTO _pyf6c_regions (id, name, geom) VALUES "
            "(1, 'R_big',   ST_SetSRID(ST_MakeEnvelope(0, 0, 10, 10), 4326)),"
            "(2, 'R_mid',   ST_SetSRID(ST_MakeEnvelope(2, 2, 4, 4), 4326)),"
            "(3, 'R_far',   ST_SetSRID(ST_MakeEnvelope(100, 100, 101, 101), 4326))"
        )
        # REF = piccolo polygon interno a R_big e R_mid.
        ref_ewkb = _get_ref_ewkb(
            session, "POLYGON((2.5 2.5, 3.5 2.5, 3.5 3.5, 2.5 3.5, 2.5 2.5))", 4326
        )
        ref = p.spatial.geometry(ewkb=ref_ewkb, srid=4326)

        # ST_Contains(geom, REF): quali geom contengono REF? → R_big, R_mid.
        contains = (
            session.select("_pyf6c_regions")
            .columns("name")
            .where_spatial("geom", "contains", ref)
            .order_by("id")
            .all()
        )
        assert [r["name"] for r in contains] == ["R_big", "R_mid"]

        # ST_Within(geom, REF): quali geom stanno dentro REF? → nessuno.
        within = (
            session.select("_pyf6c_regions")
            .columns("name")
            .where_spatial("geom", "within", ref)
            .all()
        )
        assert within == []
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6c_regions")


def test_spatial_dwithin_geography_uses_meters(session) -> None:
    # Colonna geography(Point,4326): DWithin usa metri (geodetico).
    session.execute("DROP TABLE IF EXISTS _pyf6c_geog")
    session.execute(
        "CREATE TABLE _pyf6c_geog ("
        " id INT PRIMARY KEY,"
        " name TEXT NOT NULL,"
        " g geography(Point, 4326))"
    )
    try:
        session.execute(
            "INSERT INTO _pyf6c_geog (id, name, g) VALUES "
            "(1, 'Duomo',  ST_SetSRID(ST_MakePoint(9.190, 45.464), 4326)::geography),"
            "(2, 'Sesto',  ST_SetSRID(ST_MakePoint(9.240, 45.530), 4326)::geography),"
            "(3, 'Torino', ST_SetSRID(ST_MakePoint(7.680, 45.070), 4326)::geography)"
        )
        # REF = Duomo.
        ref_ewkb = _get_ref_ewkb(session, "POINT(9.190 45.464)", 4326)
        # semantics=geography → cast `::geography` server-side (fix v0.2).
        ref = p.spatial.geography(ewkb=ref_ewkb, srid=4326)

        # DWithin 1 km: solo Duomo (~0 m).
        rows = (
            session.select("_pyf6c_geog")
            .columns("name")
            .where_spatial("g", "d_within", ref, distance_meters=1_000.0)
            .order_by("id")
            .all()
        )
        assert [r["name"] for r in rows] == ["Duomo"]

        # DWithin 10 km: Duomo + Sesto (~7 km), esclude Torino.
        rows = (
            session.select("_pyf6c_geog")
            .columns("name")
            .where_spatial("g", "d_within", ref, distance_meters=10_000.0)
            .order_by("id")
            .all()
        )
        assert [r["name"] for r in rows] == ["Duomo", "Sesto"]
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6c_geog")


def test_spatial_predicate_chains_with_other_where(session) -> None:
    session.execute("DROP TABLE IF EXISTS _pyf6c_mixed")
    session.execute(
        "CREATE TABLE _pyf6c_mixed ("
        " id INT PRIMARY KEY,"
        " category TEXT NOT NULL,"
        " geom geometry(Point, 4326))"
    )
    try:
        session.execute(
            "INSERT INTO _pyf6c_mixed (id, category, geom) VALUES "
            "(1, 'A', ST_SetSRID(ST_MakePoint(5, 45), 4326)),"
            "(2, 'B', ST_SetSRID(ST_MakePoint(6, 46), 4326)),"
            "(3, 'A', ST_SetSRID(ST_MakePoint(100, 100), 4326))"
        )
        ref_ewkb = _get_ref_ewkb(session, "POLYGON((0 40, 10 40, 10 50, 0 50, 0 40))", 4326)
        ref = p.spatial.geometry(ewkb=ref_ewkb, srid=4326)
        # Combina spatial + non-spatial predicate (AND chain).
        rows = (
            session.select("_pyf6c_mixed")
            .columns("id", "category")
            .where_spatial("geom", "intersects", ref)
            .where_eq("category", "A")
            .all()
        )
        # Solo id=1 combina category='A' e dentro bbox.
        assert rows == [{"id": 1, "category": "A"}]
    finally:
        session.execute("DROP TABLE IF EXISTS _pyf6c_mixed")


def test_spatial_reference_repr_hides_ewkb_bytes() -> None:
    assert len(VALID_POINT_EWKB) == 25, "l'assert sulla lunghezza dipende da questo"
    ref = p.spatial.geometry(ewkb=VALID_POINT_EWKB, srid=4326)
    r = repr(ref)
    assert "ewkb=<25B>" in r
    assert "srid=4326" in r
    # I bytes non compaiono.
    assert "\\x00" not in r
