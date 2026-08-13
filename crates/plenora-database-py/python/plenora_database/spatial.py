"""Spatial helpers per il portable AST (F3-6c).

Il consumer costruisce un `SpatialReference` (geometria di
riferimento) e lo passa a `where_spatial(...)` di Select/Update/Delete
builder. Il portable AST viene tradotto lato Rust nel dialetto del
provider (PostGIS `ST_Intersects` / `ST_Contains` / `ST_Within` /
`ST_DWithin` con cast condizionale `::geometry` o `::geography` in
base alle semantics — v0.2 fix del driver).

Uso tipico:

    import plenora_database as p

    with p.connect(dsn) as s:
        # 1. Estrai EWKB di riferimento (via query PostGIS o buffer WKB
        #    prodotto client-side)
        ref_ewkb = s.execute_scalar(
            "SELECT ST_AsEWKB(ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))"
        )

        # 2. Costruisci il SpatialReference
        ref = p.spatial.geometry(ewkb=ref_ewkb, srid=4326)

        # 3. Predicato spaziale nel builder
        rows = (
            s.select("poi")
             .columns("id", "name")
             .where_spatial("geom", "intersects", ref)
             .all()
        )

        # DWithin richiede distance_meters (semantica geodetica se
        # geography, unità SRS se geometry — es. gradi per SRID 4326).
        rows = (
            s.select("poi")
             .where_spatial("geom", "d_within", ref, distance_meters=500.0)
             .all()
        )
"""
from __future__ import annotations

from typing import Union

# Dimensioni supportate dal core (Dimensions enum, snake_case).
_VALID_DIMENSIONS = frozenset({"xy", "xyz", "xym", "xyzm", "unknown"})
# Semantics supportate.
_VALID_SEMANTICS = frozenset({"geometry", "geography"})
# Predicati supportati (SpatialPredicate::Kind, snake_case).
_VALID_PREDICATES = frozenset({
    "intersects", "contains", "within", "bounding_box", "d_within",
})


class SpatialReference:
    """Geometria di riferimento per predicati spaziali portable.

    Attributi:
        ewkb (bytes): buffer Extended-WKB (WKB con SRID prefix se serialized
            via `ST_AsEWKB`). Il core Rust lo bind-a al placeholder come
            `bytea` e chiama `ST_GeomFromEWKB` server-side.
        srid (int): SRID dichiarato del riferimento (deve combaciare con
            la colonna target).
        dimensions (str): "xy" / "xyz" / "xym" / "xyzm" / "unknown".
        semantics (str): "geometry" o "geography" — determina il cast
            server-side (`::geometry` vs `::geography`, fix driver v0.2).
    """

    __slots__ = ("ewkb", "srid", "dimensions", "semantics")

    def __init__(
        self,
        ewkb: Union[bytes, bytearray],
        srid: int,
        dimensions: str = "xy",
        semantics: str = "geometry",
    ) -> None:
        if not isinstance(ewkb, (bytes, bytearray)):
            raise TypeError(
                f"SpatialReference.ewkb deve essere bytes/bytearray, non {type(ewkb).__name__}"
            )
        if not isinstance(srid, int) or srid < 0:
            raise ValueError(f"SpatialReference.srid deve essere int >= 0, ricevuto {srid!r}")
        if dimensions not in _VALID_DIMENSIONS:
            raise ValueError(
                f"SpatialReference.dimensions non valida: {dimensions!r}, "
                f"attesi {sorted(_VALID_DIMENSIONS)}"
            )
        if semantics not in _VALID_SEMANTICS:
            raise ValueError(
                f"SpatialReference.semantics non valida: {semantics!r}, "
                f"attesi {sorted(_VALID_SEMANTICS)}"
            )
        self.ewkb = bytes(ewkb)
        self.srid = srid
        self.dimensions = dimensions
        self.semantics = semantics

    def __repr__(self) -> str:
        return (
            f"SpatialReference(ewkb=<{len(self.ewkb)}B>, srid={self.srid}, "
            f"dimensions={self.dimensions!r}, semantics={self.semantics!r})"
        )


def geometry(
    ewkb: Union[bytes, bytearray],
    srid: int,
    dimensions: str = "xy",
) -> SpatialReference:
    """SpatialReference con semantics=geometry (predicati usano il cast
    server-side `::geometry`; distanze in unità SRS)."""
    return SpatialReference(ewkb, srid, dimensions, "geometry")


def geography(
    ewkb: Union[bytes, bytearray],
    srid: int,
    dimensions: str = "xy",
) -> SpatialReference:
    """SpatialReference con semantics=geography (predicati usano il cast
    server-side `::geography`; distanze in metri, calcoli geodetici)."""
    return SpatialReference(ewkb, srid, dimensions, "geography")


def _validate_predicate(predicate: str) -> None:
    if predicate not in _VALID_PREDICATES:
        raise ValueError(
            f"predicato spaziale non valido: {predicate!r}, "
            f"attesi {sorted(_VALID_PREDICATES)}"
        )


def _spatial_predicate_dict(predicate: str, distance_meters: float | None) -> dict:
    """Costruisce il dict `SpatialPredicate` (tag `kind`)."""
    _validate_predicate(predicate)
    if predicate == "d_within":
        if distance_meters is None:
            raise ValueError(
                "where_spatial(predicate='d_within') richiede distance_meters=<float>"
            )
        if not isinstance(distance_meters, (int, float)):
            raise TypeError(
                f"distance_meters deve essere numero, non {type(distance_meters).__name__}"
            )
        return {"kind": "d_within", "distance_meters": float(distance_meters)}
    if distance_meters is not None:
        raise ValueError(
            f"distance_meters non ammesso per predicato {predicate!r} "
            f"(solo d_within lo richiede)"
        )
    return {"kind": predicate}


def _spatial_reference_dict(ref: SpatialReference) -> dict:
    """Costruisce il dict `SpatialReference`. Serde su Rust deserializza
    `ewkb: Vec<u8>` da array JSON di interi (0..255)."""
    return {
        "ewkb": list(ref.ewkb),
        "srid": ref.srid,
        "dimensions": ref.dimensions,
        "semantics": ref.semantics,
    }
