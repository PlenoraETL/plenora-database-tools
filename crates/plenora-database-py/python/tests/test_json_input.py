"""Contratto offline dell'adattatore JSON -> ORM / Arrow / GeoArrow."""
from __future__ import annotations

import io
import os
import random
import struct
from datetime import date, datetime, time
from decimal import Decimal

import plenora_database as p
import pytest

from ._harness import connect_postgres, postgres_dsn_or_skip


class JsonAccount(p.DeclarativeBase):
    __tablename__ = "json_accounts"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)


class JsonGeneratedAccount(p.DeclarativeBase):
    __tablename__ = "json_generated_accounts"

    id: p.Mapped[int] = p.mapped_column(int, primary_key=True, generated=True)
    name: p.Mapped[str] = p.mapped_column(str, nullable=False)
    created_by: p.Mapped[str] = p.mapped_column(
        str, nullable=False, server_default=True
    )


def scalar_input() -> p.JsonInput:
    return p.JsonInput(
        p.JsonSchema(
            [
                p.JsonField("id", int),
                p.JsonField("name", str),
                p.JsonField("note", str, nullable=True),
            ]
        )
    )


def test_mapping_document_array_and_json_lines_have_one_contract() -> None:
    ingress = scalar_input()
    expected = [
        {"id": 1, "name": "Ada", "note": None},
        {"id": 2, "name": "Linus", "note": "kernel"},
    ]

    assert list(ingress.records(expected[0])) == expected[:1]
    assert list(ingress.records('[{"id":1,"name":"Ada","note":null}]')) == expected[:1]
    assert list(ingress.records(expected)) == expected
    lines = io.StringIO(
        '{"id":1,"name":"Ada","note":null}\n'
        '{"id":2,"name":"Linus","note":"kernel"}\n'
    )
    assert list(ingress.records(lines)) == expected


@pytest.mark.parametrize(
    ("source", "code"),
    [
        ({"id": 1, "name": "Ada"}, "missing_field"),
        (
            {"id": 1, "name": "Ada", "note": None, "secret": "TOKEN-42"},
            "undeclared_field",
        ),
        ({"id": True, "name": "Ada", "note": None}, "field_type"),
        ({"id": 1, "name": "Ada", "note": 7}, "field_type"),
        ({"id": 1, "name": None, "note": None}, "null_not_allowed"),
    ],
)
def test_schema_is_closed_and_errors_do_not_echo_values(source, code: str) -> None:
    with pytest.raises(p.JsonInputError) as caught:
        list(scalar_input().records(source))
    assert caught.value.code == code
    assert "TOKEN-42" not in str(caught.value)
    assert "Ada" not in str(caught.value)


def test_invalid_and_duplicate_json_do_not_echo_the_document() -> None:
    ingress = p.JsonInput(p.JsonSchema([p.JsonField("password", str)]))
    for source, code in [
        ('{"password":"SECRET",', "invalid_json"),
        ('{"password":"SECRET","password":"OTHER"}', "duplicate_field"),
    ]:
        with pytest.raises(p.JsonInputError) as caught:
            list(ingress.records(source))
        assert caught.value.code == code
        assert "SECRET" not in str(caught.value)
        assert "OTHER" not in str(caught.value)


def test_scalar_conversion_is_explicit_and_lossless() -> None:
    ingress = p.JsonInput(
        p.JsonSchema(
            [
                p.JsonField("amount", Decimal),
                p.JsonField("day", date),
                p.JsonField("at", time),
                p.JsonField("created", datetime),
            ]
        )
    )
    [record] = ingress.records(
        {
            "amount": "12.3400000000",
            "day": "2026-08-30",
            "at": "12:13:14.123456",
            "created": "2026-08-30T12:13:14.123456",
        }
    )
    assert record == {
        "amount": Decimal("12.3400000000"),
        "day": date(2026, 8, 30),
        "at": time(12, 13, 14, 123456),
        "created": datetime(2026, 8, 30, 12, 13, 14, 123456),
    }


def test_model_schema_excludes_server_generated_values_and_builds_objects() -> None:
    generated = p.JsonSchema.from_model(JsonGeneratedAccount)
    assert [field.name for field in generated.fields] == ["name"]

    ingress = p.JsonInput.for_model(JsonAccount)
    [account] = ingress.objects('{"id":7,"name":"Ada"}', JsonAccount)
    assert isinstance(account, JsonAccount)
    assert account.id == 7
    assert account.name == "Ada"


@pytest.mark.asyncio
async def test_async_json_lines_records_batches_and_objects() -> None:
    async def lines():
        yield '{"id":1,"name":"Ada"}'
        yield {"id": 2, "name": "Linus"}

    ingress = p.JsonInput.for_model(JsonAccount)
    records = [record async for record in ingress.arecords(lines())]
    assert [record["id"] for record in records] == [1, 2]
    batches = [batch async for batch in ingress.abatches(lines(), batch_size=1)]
    assert [batch.num_rows for batch in batches] == [1, 1]
    objects = [item async for item in ingress.aobjects(lines(), JsonAccount)]
    assert [item.name for item in objects] == ["Ada", "Linus"]


def test_batches_are_bounded_and_copy_from_accepts_the_generator() -> None:
    pyarrow = pytest.importorskip("pyarrow")
    import pyarrow.ipc as ipc

    from plenora_database._arrow_io import _to_ipc_bytes

    consumed = 0

    def records():
        nonlocal consumed
        for index in range(5):
            consumed += 1
            yield {"id": index, "name": f"n{index}"}

    batches = p.JsonInput.for_model(JsonAccount).batches(records(), batch_size=2)
    first = next(batches)
    assert first.num_rows == 2
    assert consumed == 2

    stream = _to_ipc_bytes(iter([first, *batches]))
    table = ipc.open_stream(io.BytesIO(stream)).read_all()
    assert table.num_rows == 5
    assert table.column("id").to_pylist() == [0, 1, 2, 3, 4]


def test_arrow_schema_is_explicit_and_geoarrow_is_canonical() -> None:
    pyarrow = pytest.importorskip("pyarrow")
    geometry = p.JsonGeometry(
        srid=4326, geometry_type="Point", encoding="ewkb"
    )
    ingress = p.JsonInput(
        p.JsonSchema([p.JsonField("id", int), p.JsonField("shape", geometry)])
    )
    schema = ingress.arrow_schema()
    shape = schema.field("shape")

    assert schema.metadata == {b"plenora.contract.version": b"1"}
    assert shape.type == pyarrow.binary()
    assert shape.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
    assert shape.metadata[b"plenora.geometry.encoding"] == b"ewkb"
    assert shape.metadata[b"plenora.geometry.srid"] == b"4326"
    assert shape.metadata[b"plenora.geometry.types"] == b"point"
    assert shape.metadata[b"plenora.geometry.types_declaration"] == b"exact"


def test_geojson_point_becomes_wkb_or_ewkb_only_from_declared_crs() -> None:
    point = {"type": "Point", "coordinates": [9.19, 45.46]}
    wkb_input = p.JsonInput(
        p.JsonSchema(
            [p.JsonField("shape", p.JsonGeometry(4326, encoding="wkb"))]
        )
    )
    ewkb_input = p.JsonInput(
        p.JsonSchema(
            [p.JsonField("shape", p.JsonGeometry(4326, encoding="ewkb"))]
        )
    )
    [wkb] = wkb_input.records({"shape": point})
    [ewkb] = ewkb_input.records({"shape": point})

    assert wkb["shape"] == struct.pack("<BIdd", 1, 1, 9.19, 45.46)
    assert ewkb["shape"] == struct.pack(
        "<BII2d", 1, 0x2000_0001, 4326, 9.19, 45.46
    )


def test_geojson_nested_shapes_are_checked_recursively() -> None:
    ingress = p.JsonInput(
        p.JsonSchema([p.JsonField("shape", p.JsonGeometry(4326))])
    )
    polygon = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
    }
    assert list(ingress.records({"shape": polygon}))[0]["shape"]

    invalid_collection = {
        "type": "GeometryCollection",
        "geometries": [{"type": "Point", "coordinates": [1, 2], "crs": "secret"}],
    }
    with pytest.raises(p.JsonInputError) as caught:
        list(ingress.records({"shape": invalid_collection}))
    assert caught.value.code == "geojson_member"
    assert "secret" not in str(caught.value)


def test_every_supported_geojson_shape_produces_native_valid_ewkb() -> None:
    from plenora_database import _native

    samples = {
        "point": {"type": "Point", "coordinates": [1, 2]},
        "linestring": {
            "type": "LineString",
            "coordinates": [[0, 0], [1, 1]],
        },
        "polygon": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
        },
        "multipoint": {
            "type": "MultiPoint",
            "coordinates": [[0, 0], [1, 1]],
        },
        "multilinestring": {
            "type": "MultiLineString",
            "coordinates": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]],
        },
        "multipolygon": {
            "type": "MultiPolygon",
            "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 0]]]],
        },
        "geometrycollection": {
            "type": "GeometryCollection",
            "geometries": [{"type": "Point", "coordinates": [1, 2]}],
        },
    }
    for expected, geojson in samples.items():
        ingress = p.JsonInput(
            p.JsonSchema(
                [
                    p.JsonField(
                        "shape",
                        p.JsonGeometry(4326, geometry_type=expected),
                    )
                ]
            )
        )
        [record] = ingress.records({"shape": geojson})
        payload = record["shape"]
        p.SpatialReference.validated(payload, 4326, "xy")
        assert (
            _native.inspect_ewkb_geometry_type(payload, 4326, "xy").lower()
            == expected
        )

    xyz = p.JsonInput(
        p.JsonSchema(
            [p.JsonField("shape", p.JsonGeometry(4979, dimensions="xyz"))]
        )
    )
    [record] = xyz.records(
        {"shape": {"type": "Point", "coordinates": [9.19, 45.46, 120]}}
    )
    p.SpatialReference.validated(record["shape"], 4979, "xyz")


def test_geojson_rejects_unclosed_rings_wrong_declared_type_and_implicit_m() -> None:
    with pytest.raises(ValueError):
        p.JsonGeometry(4326, dimensions="xym")

    ingress = p.JsonInput(
        p.JsonSchema(
            [
                p.JsonField(
                    "shape", p.JsonGeometry(4326, geometry_type="polygon")
                )
            ]
        )
    )
    with pytest.raises(p.JsonInputError, match="incompatibile"):
        list(
            ingress.records(
                {"shape": {"type": "Point", "coordinates": [1, 2]}}
            )
        )
    with pytest.raises(p.JsonInputError) as caught:
        list(
            ingress.records(
                {
                    "shape": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]],
                    }
                }
            )
        )
    assert caught.value.code == "geojson_ring"


def test_json_input_randomized_campaign() -> None:
    """Campagna ripetibile, estendibile senza cambiare il test del wheel."""

    from plenora_database import _native

    cases = int(os.environ.get("PLENORA_JSON_FUZZ_CASES", "500"))
    assert cases > 0
    generator = random.Random(0x504C_454E_4F52_41)
    ingress = p.JsonInput(
        p.JsonSchema(
            [
                p.JsonField("id", int),
                p.JsonField(
                    "shape", p.JsonGeometry(4326, geometry_type="point")
                ),
            ]
        )
    )
    for index in range(cases):
        x = generator.uniform(-1_000_000, 1_000_000)
        y = generator.uniform(-1_000_000, 1_000_000)
        if index % 2 == 0:
            [record] = ingress.records(
                {
                    "id": index,
                    "shape": {"type": "Point", "coordinates": [x, y]},
                }
            )
            p.SpatialReference.validated(record["shape"], 4326, "xy")
            assert _native.inspect_ewkb_geometry_type(
                record["shape"], 4326, "xy"
            ) == "Point"
        else:
            secret = f"PAYLOAD-{index}"
            with pytest.raises(p.JsonInputError) as caught:
                list(
                    ingress.records(
                        {
                            "id": index,
                            "shape": {
                                "type": "Point",
                                "coordinates": [x, secret],
                            },
                        }
                    )
                )
            assert secret not in str(caught.value)


def test_json_geoarrow_batches_land_in_postgis() -> None:
    """Prova live del bordo completo GeoJSON -> GeoArrow/EWKB -> provider."""

    session = connect_postgres(postgres_dsn_or_skip())
    session.execute("DROP TABLE IF EXISTS _plenora_json_places")
    session.execute(
        "CREATE TABLE _plenora_json_places ("
        "id BIGINT PRIMARY KEY, shape geometry(Point, 4326) NOT NULL)"
    )
    try:
        ingress = p.JsonInput(
            p.JsonSchema(
                [
                    p.JsonField("id", int),
                    p.JsonField(
                        "shape",
                        p.JsonGeometry(
                            4326, geometry_type="point", encoding="ewkb"
                        ),
                    ),
                ]
            )
        )
        outcome = session.copy_from(
            "public",
            "_plenora_json_places",
            ingress.batches(
                [
                    {
                        "id": 1,
                        "shape": {
                            "type": "Point",
                            "coordinates": [9.19, 45.46],
                        },
                    }
                ],
                batch_size=1,
            ),
        )
        assert outcome["status"] == "committed"
        matching = session.execute_scalar(
            "SELECT COUNT(*)::BIGINT FROM _plenora_json_places "
            "WHERE id = 1 AND ST_SRID(shape) = 4326 "
            "AND abs(ST_X(shape) - 9.19) < 0.000000001 "
            "AND abs(ST_Y(shape) - 45.46) < 0.000000001"
        )
        assert matching == 1
    finally:
        try:
            session.execute("DROP TABLE IF EXISTS _plenora_json_places")
        finally:
            session.close()
