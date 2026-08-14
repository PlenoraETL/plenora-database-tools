"""Type stubs per il modulo spatial."""


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
