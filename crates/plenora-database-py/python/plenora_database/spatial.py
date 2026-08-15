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

        # DWithin richiede distance_meters. **Attenzione unità**:
        # - Geography + qualsiasi SRID → metri (garantito).
        # - Geometry + SRID planare (3857, 25832) → unità del SRID (metri
        #   per web mercator e UTM).
        # - Geometry + SRID geografico (4326, 4269, 4267, 4258, 4283) →
        #   il compilatore Rust **rifiuta** (silent wrong result: sarebbero
        #   gradi, non metri). Usa `spatial.geography(...)` per WGS84.
        ref_geog = p.spatial.geography(ewkb=ref_ewkb, srid=4326)
        rows = (
            s.select("poi")
             .where_spatial("geom", "d_within", ref_geog, distance_meters=500.0)
             .all()
        )
"""
from __future__ import annotations

from typing import Union

# Dimensioni supportate dal core (Dimensions enum, snake_case).
_VALID_DIMENSIONS = frozenset({"xy", "xyz", "xym", "xyzm", "unknown"})
# Semantics supportate.
_VALID_SEMANTICS = frozenset({"geometry", "geography"})


# SRID geografici (lat/lon in gradi): con semantics=geometry + DWithin
# il compilatore Rust rifiuta (silent wrong result: distanza in gradi).
# Client-side fast-fail per UX migliore.
#
# Fase A1: la lista è caricata **al primo accesso** dal modulo nativo
# `plenora_database._native.geographic_srids()` che a sua volta legge da
# `plenora_database_core::spatial_policy::GEOGRAPHIC_SRIDS`.
# Single-source-of-truth Rust: se il core aggiunge un SRID, Python lo
# vede automaticamente al prossimo import senza toccare questo file.
def _load_geographic_srids() -> frozenset[int]:
    try:
        from . import _native  # type: ignore[attr-defined]
        return frozenset(_native.geographic_srids())
    except (ImportError, AttributeError):
        # Fallback difensivo per contesti in cui il native non è
        # ancora caricato (test unitari puro-Python, doctest). Deve
        # restare in sync col Rust ma è solo un safety net.
        return frozenset({4326, 4269, 4267, 4258, 4283})


_GEOGRAPHIC_SRIDS: frozenset[int] = _load_geographic_srids()
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


def _validate_predicate_reference_combo(
    predicate: str, reference: SpatialReference
) -> None:
    """Fast-fail client-side per combinazioni fuorvianti.

    Reason: il compilatore Rust rifiuta comunque (fix review #5), ma il
    round-trip include serialization + IPC + parsing — errore locale
    dà stacktrace più utile.
    """
    if (
        predicate == "d_within"
        and reference.semantics == "geometry"
        and reference.srid in _GEOGRAPHIC_SRIDS
    ):
        raise ValueError(
            f"where_spatial('d_within', ...) con semantics='geometry' su "
            f"SRID {reference.srid} (geografico lat/lon) produrrebbe distanza "
            f"in gradi (fuorviante rispetto al nome distance_meters). "
            f"Usa spatial.geography(...) per distanze in metri reali, "
            f"oppure riproietta il dato su un SRID planare (es. 3857)."
        )
    if predicate == "bounding_box" and reference.semantics == "geography":
        raise ValueError(
            "where_spatial('bounding_box', ...) non supportato con "
            "semantics='geography' (operator && è solo geometry). "
            "Usa 'intersects'."
        )


def _spatial_reference_dict(ref: SpatialReference) -> dict:
    """Costruisce il dict `SpatialReference`. Serde su Rust deserializza
    `ewkb: Vec<u8>` da array JSON di interi (0..255)."""
    return {
        "ewkb": list(ref.ewkb),
        "srid": ref.srid,
        "dimensions": ref.dimensions,
        "semantics": ref.semantics,
    }
