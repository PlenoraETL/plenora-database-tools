"""Type stubs per il modulo spatial."""
from __future__ import annotations


class SpatialReference:
    ewkb: bytes
    srid: int
    dimensions: str
    semantics: str

    def __init__(
        self,
        ewkb: bytes | bytearray,
        srid: int,
        dimensions: str = "xy",
        semantics: str = "geometry",
    ) -> None: ...
    @classmethod
    def validated(
        cls,
        ewkb: bytes | bytearray,
        srid: int,
        dimensions: str = "xy",
        semantics: str = "geometry",
    ) -> SpatialReference: ...
    def __repr__(self) -> str: ...


def geometry(
    ewkb: bytes | bytearray,
    srid: int,
    dimensions: str = "xy",
) -> SpatialReference: ...


def geography(
    ewkb: bytes | bytearray,
    srid: int,
    dimensions: str = "xy",
) -> SpatialReference: ...
