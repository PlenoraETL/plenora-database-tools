"""Ingresso JSON esplicito verso record ORM o batch Arrow.

JSON non diventa un secondo protocollo dati: questo modulo valida il bordo
Python e converte subito ogni record nella rappresentazione gia usata dal
resto dello SDK. Gli input JSON Lines vengono consumati incrementalmente.
"""
from __future__ import annotations

import json
import math
import struct
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Generic, TypeVar

from .orm import DeclarativeBase, Geometry


T = TypeVar("T", bound=DeclarativeBase)

_SCALAR_TYPES = frozenset({bool, int, float, str, bytes, Decimal, date, time, datetime})
_GEOMETRY_TYPES = {
    "point": 1,
    "linestring": 2,
    "polygon": 3,
    "multipoint": 4,
    "multilinestring": 5,
    "multipolygon": 6,
    "geometrycollection": 7,
}


class JsonInputError(ValueError):
    """Input JSON non conforme, senza il valore rifiutato nel messaggio."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        record_index: int | None = None,
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.record_index = record_index
        self.field = field


@dataclass(frozen=True, slots=True)
class JsonGeometry:
    """Contratto GeoJSON dichiarato per una colonna WKB/EWKB.

    ``srid`` e obbligatorio: il modulo non ricava mai il CRS dal payload.
    GeoJSON descrive coordinate X/Y e, opzionalmente, Z; M non e inferibile
    senza una convenzione esterna e viene quindi rifiutato.
    """

    srid: int
    dimensions: str = "xy"
    semantics: str = "geometry"
    geometry_type: str | None = None
    encoding: str = "ewkb"

    def __post_init__(self) -> None:
        if (
            not isinstance(self.srid, int)
            or isinstance(self.srid, bool)
            or self.srid <= 0
            or self.srid > 0x1FFF_FFFF
        ):
            raise ValueError("JsonGeometry.srid deve essere un intero positivo valido")
        if self.dimensions not in {"xy", "xyz"}:
            raise ValueError("JsonGeometry supporta soltanto dimensioni xy o xyz")
        if self.semantics not in {"geometry", "geography"}:
            raise ValueError("JsonGeometry.semantics non valida")
        if self.encoding not in {"wkb", "ewkb"}:
            raise ValueError("JsonGeometry.encoding deve essere wkb o ewkb")
        if self.geometry_type is not None:
            if not isinstance(self.geometry_type, str):
                raise ValueError("JsonGeometry.geometry_type non valido")
            normalized = self.geometry_type.strip().lower()
            if normalized not in _GEOMETRY_TYPES:
                raise ValueError("JsonGeometry.geometry_type non valido")
            object.__setattr__(self, "geometry_type", normalized)

    def orm_type(self) -> Geometry:
        """Tipo ORM equivalente; la validazione nativa resta nel mapper."""

        return Geometry(
            srid=self.srid,
            dimensions=self.dimensions,
            semantics=self.semantics,
            geometry_type=self.geometry_type,
        )


@dataclass(frozen=True, slots=True)
class JsonField:
    """Campo dichiarato dell'input; nessuna inferenza dai valori."""

    name: str
    type_: type[Any] | JsonGeometry | Geometry
    nullable: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name or "\x00" in self.name:
            raise ValueError("JsonField.name non valido")
        type_ = self.type_
        if isinstance(type_, Geometry):
            object.__setattr__(
                self,
                "type_",
                JsonGeometry(
                    srid=type_.srid,
                    dimensions=type_.dimensions,
                    semantics=type_.semantics,
                    geometry_type=type_.geometry_type,
                ),
            )
        elif not isinstance(type_, JsonGeometry) and type_ not in _SCALAR_TYPES:
            raise TypeError("JsonField.type_ non e un tipo JSON qualificato")
        if not isinstance(self.nullable, bool):
            raise TypeError("JsonField.nullable deve essere bool")


@dataclass(frozen=True, slots=True)
class JsonSchema:
    """Schema chiuso: ogni record deve avere esattamente questi campi."""

    fields: tuple[JsonField, ...]

    def __init__(self, fields: Iterable[JsonField]) -> None:
        materialized = tuple(fields)
        if not materialized:
            raise ValueError("JsonSchema richiede almeno un campo")
        if not all(isinstance(field, JsonField) for field in materialized):
            raise TypeError("JsonSchema accetta soltanto JsonField")
        names = tuple(field.name for field in materialized)
        if len(names) != len(set(names)):
            raise ValueError("JsonSchema contiene nomi duplicati")
        object.__setattr__(self, "fields", materialized)

    @classmethod
    def from_model(cls, model: type[DeclarativeBase]) -> JsonSchema:
        """Deriva lo schema esplicito dal mapper, escludendo valori server-side."""

        mapper = getattr(model, "__mapper__", None)
        if mapper is None:
            raise TypeError("JsonSchema.from_model richiede un modello dichiarativo")
        fields = []
        for attribute in mapper.attributes:
            if attribute.generated or attribute.server_default:
                continue
            if attribute.name is None:
                raise TypeError("mapper con attributo privo di nome")
            fields.append(JsonField(attribute.name, attribute.type_, attribute.nullable))
        return cls(fields)


class JsonInput(Generic[T]):
    """Validatore e convertitore riusabile per uno schema JSON chiuso."""

    def __init__(self, schema: JsonSchema) -> None:
        if not isinstance(schema, JsonSchema):
            raise TypeError("JsonInput richiede JsonSchema")
        self.schema = schema
        self._names = frozenset(field.name for field in schema.fields)

    @classmethod
    def for_model(cls, model: type[T]) -> JsonInput[T]:
        return cls(JsonSchema.from_model(model))

    def records(self, source: Any) -> Iterator[dict[str, Any]]:
        """Converte mapping, array JSON, iterabili o stream JSON Lines."""

        for index, record in enumerate(_sync_records(source)):
            yield self._validate(record, index)

    def arecords(self, source: AsyncIterable[Any]) -> AsyncIterator[dict[str, Any]]:
        """Versione incrementale per sorgenti asincrone di mapping/JSON Lines."""

        if not isinstance(source, AsyncIterable):
            raise TypeError("JsonInput.arecords richiede un AsyncIterable")

        async def iterate() -> AsyncIterator[dict[str, Any]]:
            index = 0
            async for item in source:
                record = _line_or_mapping(item, index)
                yield self._validate(record, index)
                index += 1

        return iterate()

    def batches(self, source: Any, *, batch_size: int = 1_024) -> Iterator[Any]:
        """Produce ``pyarrow.RecordBatch`` bounded da ``batch_size``."""

        size = _batch_size(batch_size)
        pa_schema = self.arrow_schema()
        pending: list[dict[str, Any]] = []
        for record in self.records(source):
            pending.append(record)
            if len(pending) == size:
                yield _record_batch(pending, pa_schema)
                pending.clear()
        if pending:
            yield _record_batch(pending, pa_schema)

    def abatches(
        self, source: AsyncIterable[Any], *, batch_size: int = 1_024
    ) -> AsyncIterator[Any]:
        """Produce batch Arrow da JSON Lines asincrono senza leggere tutto."""

        size = _batch_size(batch_size)
        pa_schema = self.arrow_schema()
        async def iterate() -> AsyncIterator[Any]:
            pending: list[dict[str, Any]] = []
            async for record in self.arecords(source):
                pending.append(record)
                if len(pending) == size:
                    yield _record_batch(pending, pa_schema)
                    pending.clear()
            if pending:
                yield _record_batch(pending, pa_schema)

        return iterate()

    def objects(self, source: Any, model: type[T]) -> Iterator[T]:
        """Costruisce istanze transienti ORM dai record validati."""

        _require_model_schema(self.schema, model)
        for index, record in enumerate(self.records(source)):
            try:
                yield model(**record)
            except Exception:
                raise JsonInputError(
                    "orm_construction",
                    "costruzione dell'oggetto ORM fallita",
                    record_index=index,
                ) from None

    def aobjects(
        self, source: AsyncIterable[Any], model: type[T]
    ) -> AsyncIterator[T]:
        """Versione asincrona della costruzione di oggetti ORM."""

        _require_model_schema(self.schema, model)
        async def iterate() -> AsyncIterator[T]:
            index = 0
            async for record in self.arecords(source):
                try:
                    yield model(**record)
                except Exception:
                    raise JsonInputError(
                        "orm_construction",
                        "costruzione dell'oggetto ORM fallita",
                        record_index=index,
                    ) from None
                index += 1

        return iterate()

    def arrow_schema(self) -> Any:
        """Schema Arrow esplicito, inclusi i metadata GeoArrow canonici."""

        try:
            import pyarrow as pa
        except ImportError as error:  # pragma: no cover - dipendenza opzionale
            raise ImportError(
                "JsonInput.batches richiede pyarrow: `pip install pyarrow`"
            ) from error
        fields = [_arrow_field(field, pa) for field in self.schema.fields]
        metadata = None
        if any(isinstance(field.type_, JsonGeometry) for field in self.schema.fields):
            metadata = {b"plenora.contract.version": b"1"}
        return pa.schema(fields, metadata=metadata)

    def _validate(self, record: Mapping[str, Any], index: int) -> dict[str, Any]:
        raw_names = tuple(record)
        if not all(isinstance(name, str) for name in raw_names):
            raise JsonInputError(
                "field_name",
                "il record JSON contiene un nome di campo non valido",
                record_index=index,
            )
        names = set(raw_names)
        if names - self._names:
            raise JsonInputError(
                "undeclared_field",
                "il record JSON contiene campi non dichiarati",
                record_index=index,
            )
        if self._names - names:
            raise JsonInputError(
                "missing_field",
                "il record JSON non contiene tutti i campi dichiarati",
                record_index=index,
            )
        validated = {}
        for field in self.schema.fields:
            value = record[field.name]
            if value is None:
                if not field.nullable:
                    raise JsonInputError(
                        "null_not_allowed",
                        "un campo JSON non nullable contiene null",
                        record_index=index,
                        field=field.name,
                    )
                validated[field.name] = None
                continue
            try:
                validated[field.name] = _validated_value(value, field.type_)
            except JsonInputError as error:
                error.record_index = index
                error.field = field.name
                raise
        return validated


def _sync_records(source: Any) -> Iterator[Mapping[str, Any]]:
    if isinstance(source, Mapping):
        yield source
        return
    if isinstance(source, (str, bytes, bytearray)):
        document = _decode_json(source, None)
        if isinstance(document, Mapping):
            yield document
            return
        if isinstance(document, list):
            for index, record in enumerate(document):
                if not isinstance(record, Mapping):
                    raise JsonInputError(
                        "record_type",
                        "l'array JSON deve contenere soltanto oggetti",
                        record_index=index,
                    )
                yield record
            return
        raise JsonInputError("document_type", "il documento JSON deve essere oggetto o array")
    if not isinstance(source, Iterable):
        raise TypeError("JsonInput.records richiede JSON, mapping o Iterable")
    for index, item in enumerate(source):
        yield _line_or_mapping(item, index)


def _line_or_mapping(item: Any, index: int) -> Mapping[str, Any]:
    if isinstance(item, Mapping):
        return item
    if isinstance(item, (str, bytes, bytearray)):
        document = _decode_json(item, index)
        if isinstance(document, Mapping):
            return document
        raise JsonInputError(
            "jsonl_record_type",
            "ogni elemento JSON Lines deve essere un oggetto",
            record_index=index,
        )
    raise JsonInputError(
        "record_type",
        "la sorgente contiene un elemento che non e un oggetto JSON",
        record_index=index,
    )


def _decode_json(value: str | bytes | bytearray, index: int | None) -> Any:
    try:
        return json.loads(value, object_pairs_hook=_object_without_duplicates)
    except JsonInputError as error:
        error.record_index = index
        raise
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError, RecursionError):
        raise JsonInputError(
            "invalid_json", "JSON non valido", record_index=index
        ) from None


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise JsonInputError(
                "duplicate_field", "un oggetto JSON contiene campi duplicati"
            )
        result[name] = value
    return result


def _validated_value(value: Any, type_: type[Any] | JsonGeometry) -> Any:
    if isinstance(type_, JsonGeometry):
        return _geojson_to_wkb(value, type_)
    if type_ is bool:
        if type(value) is not bool:
            raise JsonInputError("field_type", "un campo JSON non rispetta il tipo dichiarato")
        return value
    if type_ is int:
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise JsonInputError("field_type", "un campo JSON non rispetta int64")
        return value
    if type_ is float:
        if type(value) not in {int, float} or not math.isfinite(value):
            raise JsonInputError("field_type", "un campo JSON non rispetta float64")
        return float(value)
    if type_ is str:
        if type(value) is not str:
            raise JsonInputError("field_type", "un campo JSON non rispetta string")
        return value
    if type_ is bytes:
        if not isinstance(value, (bytes, bytearray)):
            raise JsonInputError("field_type", "un campo JSON non rispetta binary")
        return bytes(value)
    if type_ is Decimal:
        if type(value) is not str:
            raise JsonInputError("field_type", "un campo decimal deve essere una stringa JSON")
        try:
            result = Decimal(value)
        except InvalidOperation:
            raise JsonInputError("field_type", "un campo JSON non rispetta decimal") from None
        if not result.is_finite():
            raise JsonInputError("field_type", "un campo JSON non rispetta decimal")
        return result
    if type_ in {date, time, datetime}:
        if type(value) is not str:
            raise JsonInputError("field_type", "un campo temporale deve essere una stringa JSON")
        try:
            result = type_.fromisoformat(value)
        except ValueError:
            raise JsonInputError("field_type", "un campo JSON non rispetta il formato temporale") from None
        if type_ is datetime and result.tzinfo is not None:
            raise JsonInputError("field_type", "datetime JSON richiede un valore senza timezone")
        if type_ is time and result.tzinfo is not None:
            raise JsonInputError("field_type", "time JSON richiede un valore senza timezone")
        return result
    raise AssertionError("tipo JsonField non raggiungibile")


def _geojson_to_wkb(value: Any, geometry: JsonGeometry) -> bytes:
    if not isinstance(value, Mapping):
        raise JsonInputError("geojson_type", "il campo geometrico non e un oggetto GeoJSON")
    kind = value.get("type")
    if not isinstance(kind, str) or kind.lower() not in _GEOMETRY_TYPES:
        raise JsonInputError("geojson_type", "tipo GeoJSON non supportato")
    normalized = kind.lower()
    if geometry.geometry_type is not None and normalized != geometry.geometry_type:
        raise JsonInputError("geojson_type", "tipo GeoJSON incompatibile con lo schema")
    try:
        return _encode_geometry(value, geometry, include_srid=geometry.encoding == "ewkb")
    except JsonInputError:
        raise
    except (OverflowError, TypeError, ValueError, RecursionError, struct.error):
        raise JsonInputError("geojson_coordinates", "coordinate GeoJSON non valide") from None


def _header(kind: str, geometry: JsonGeometry, include_srid: bool) -> bytes:
    type_code = _GEOMETRY_TYPES[kind]
    if geometry.dimensions == "xyz":
        type_code = type_code | 0x8000_0000 if include_srid else type_code + 1_000
    if include_srid:
        type_code |= 0x2000_0000
    header = struct.pack("<BI", 1, type_code)
    return header + (struct.pack("<I", geometry.srid) if include_srid else b"")


def _position(value: Any, dimensions: str) -> tuple[float, ...]:
    width = 2 if dimensions == "xy" else 3
    if not isinstance(value, (list, tuple)) or len(value) != width:
        raise JsonInputError("geojson_coordinates", "coordinate GeoJSON non valide")
    coordinates = []
    for coordinate in value:
        if type(coordinate) not in {int, float} or not math.isfinite(coordinate):
            raise JsonInputError("geojson_coordinates", "coordinate GeoJSON non valide")
        coordinates.append(float(coordinate))
    return tuple(coordinates)


def _points(values: Any, dimensions: str, *, minimum: int = 0) -> list[tuple[float, ...]]:
    if not isinstance(values, (list, tuple)) or len(values) < minimum:
        raise JsonInputError("geojson_coordinates", "coordinate GeoJSON non valide")
    return [_position(value, dimensions) for value in values]


def _pack_points(points: list[tuple[float, ...]]) -> bytes:
    flat = (coordinate for point in points for coordinate in point)
    return struct.pack(f"<{sum(len(point) for point in points)}d", *flat)


def _encode_geometry(value: Mapping[str, Any], geometry: JsonGeometry, *, include_srid: bool) -> bytes:
    kind_value = value.get("type")
    if not isinstance(kind_value, str) or kind_value.lower() not in _GEOMETRY_TYPES:
        raise JsonInputError("geojson_type", "tipo GeoJSON non supportato")
    kind = kind_value.lower()
    coordinate_member = "geometries" if kind == "geometrycollection" else "coordinates"
    allowed = {"type", "bbox", coordinate_member}
    if set(value) - allowed or coordinate_member not in value:
        raise JsonInputError("geojson_member", "la geometria contiene membri non dichiarati")
    if "bbox" in value:
        _validate_bbox(value["bbox"], geometry.dimensions)
    output = bytearray(_header(kind, geometry, include_srid))
    if kind == "point":
        output.extend(_pack_points([_position(value.get("coordinates"), geometry.dimensions)]))
    elif kind == "linestring":
        points = _points(value.get("coordinates"), geometry.dimensions, minimum=2)
        output.extend(struct.pack("<I", len(points)))
        output.extend(_pack_points(points))
    elif kind == "polygon":
        rings_value = value.get("coordinates")
        if not isinstance(rings_value, (list, tuple)):
            raise JsonInputError("geojson_coordinates", "coordinate GeoJSON non valide")
        output.extend(struct.pack("<I", len(rings_value)))
        for ring_value in rings_value:
            ring = _points(ring_value, geometry.dimensions, minimum=4)
            if ring[0] != ring[-1]:
                raise JsonInputError("geojson_ring", "un anello GeoJSON non e chiuso")
            output.extend(struct.pack("<I", len(ring)))
            output.extend(_pack_points(ring))
    elif kind in {"multipoint", "multilinestring", "multipolygon"}:
        items = value.get("coordinates")
        if not isinstance(items, (list, tuple)):
            raise JsonInputError("geojson_coordinates", "coordinate GeoJSON non valide")
        child_kind = {
            "multipoint": "Point",
            "multilinestring": "LineString",
            "multipolygon": "Polygon",
        }[kind]
        output.extend(struct.pack("<I", len(items)))
        for item in items:
            output.extend(
                _encode_geometry(
                    {"type": child_kind, "coordinates": item},
                    geometry,
                    include_srid=include_srid,
                )
            )
    else:
        items = value.get("geometries")
        if not isinstance(items, (list, tuple)):
            raise JsonInputError("geojson_coordinates", "GeometryCollection non valida")
        output.extend(struct.pack("<I", len(items)))
        for item in items:
            if not isinstance(item, Mapping):
                raise JsonInputError("geojson_coordinates", "GeometryCollection non valida")
            output.extend(_encode_geometry(item, geometry, include_srid=include_srid))
    return bytes(output)


def _validate_bbox(value: Any, dimensions: str) -> None:
    width = 2 if dimensions == "xy" else 3
    if not isinstance(value, (list, tuple)) or len(value) != width * 2:
        raise JsonInputError("geojson_bbox", "bbox GeoJSON non valido")
    if any(
        type(coordinate) not in {int, float} or not math.isfinite(coordinate)
        for coordinate in value
    ):
        raise JsonInputError("geojson_bbox", "bbox GeoJSON non valido")


def _arrow_field(field: JsonField, pa: Any) -> Any:
    type_ = field.type_
    metadata = None
    if isinstance(type_, JsonGeometry):
        arrow_type = pa.binary()
        declaration = "exact" if type_.geometry_type is not None else "unresolved"
        metadata = {
            b"ARROW:extension:name": b"geoarrow.wkb",
            b"plenora.geometry.encoding": type_.encoding.encode("ascii"),
            b"plenora.geometry.dimensions": type_.dimensions.encode("ascii"),
            b"plenora.geometry.types_declaration": declaration.encode("ascii"),
            b"plenora.geometry.srid": str(type_.srid).encode("ascii"),
            b"plenora.geometry.crs_resolution": b"declared_unresolved",
            b"plenora.geometry.spatial_semantics": type_.semantics.encode("ascii"),
            b"plenora.geometry.precision": b"float64",
        }
        if type_.geometry_type is not None:
            metadata[b"plenora.geometry.types"] = type_.geometry_type.encode("ascii")
    else:
        arrow_type = {
            bool: pa.bool_(),
            int: pa.int64(),
            float: pa.float64(),
            str: pa.string(),
            bytes: pa.binary(),
            Decimal: pa.decimal128(38, 10),
            date: pa.date32(),
            time: pa.time64("us"),
            datetime: pa.timestamp("us"),
        }[type_]
    return pa.field(field.name, arrow_type, nullable=field.nullable, metadata=metadata)


def _record_batch(records: list[dict[str, Any]], schema: Any) -> Any:
    try:
        import pyarrow as pa

        return pa.RecordBatch.from_pylist(records, schema=schema)
    except Exception:
        raise JsonInputError("arrow_conversion", "conversione del batch Arrow fallita") from None


def _batch_size(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("batch_size deve essere un intero positivo")
    return value


def _require_model_schema(schema: JsonSchema, model: type[DeclarativeBase]) -> None:
    expected = JsonSchema.from_model(model)
    if schema != expected:
        raise TypeError("lo schema JSON non coincide con il mapper ORM")


__all__ = [
    "JsonField",
    "JsonGeometry",
    "JsonInput",
    "JsonInputError",
    "JsonSchema",
]
