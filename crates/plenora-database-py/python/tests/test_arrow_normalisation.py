"""Normalizzazione Arrow verso i tipi che i writer accettano.

Offline: nessun database. `_to_ipc_bytes` e il punto in cui l'input del
chiamante diventa il buffer che il provider legge, e i tipi a offset larghi
sono il caso in cui l'adapter puo fallire in silenzio — non sollevando, ma
producendo un buffer che il writer rifiutera con un messaggio che parla del
provider invece che della conversione.
"""

from __future__ import annotations

import io

import pytest

pyarrow = pytest.importorskip("pyarrow")
import pyarrow.ipc as ipc  # noqa: E402

from plenora_database._arrow_io import _to_ipc_bytes  # noqa: E402


def roundtrip(table):
    """Lo schema che il provider vedrebbe, piu i dati riletti."""

    reader = ipc.open_stream(io.BytesIO(_to_ipc_bytes(table)))
    materialised = reader.read_all()
    return materialised.schema, materialised


def test_large_utf8_becomes_utf8_keeping_values() -> None:
    schema = pyarrow.schema(
        [pyarrow.field("label", pyarrow.large_string(), nullable=False)]
    )
    table = pyarrow.table(
        {"label": pyarrow.array(["a", "b"], pyarrow.large_string())}, schema=schema
    )

    narrowed, data = roundtrip(table)
    assert narrowed.field("label").type == pyarrow.string()
    assert narrowed.field("label").nullable is False
    assert data.column("label").to_pylist() == ["a", "b"]


def test_large_binary_becomes_binary_keeping_values() -> None:
    table = pyarrow.table(
        {"payload": pyarrow.array([b"\x00\x01", b"\xff"], pyarrow.large_binary())}
    )

    narrowed, data = roundtrip(table)
    assert narrowed.field("payload").type == pyarrow.binary()
    assert data.column("payload").to_pylist() == [b"\x00\x01", b"\xff"]


def test_large_list_of_large_utf8_is_narrowed_all_the_way_down() -> None:
    """La ricorsione e il punto: fermarsi al primo livello non basta.

    `large_list<large_string>` ridotto a `list<large_string>` resta un tipo
    che il writer rifiuta, e la conversione sembrerebbe fatta.
    """

    element = pyarrow.field("item", pyarrow.large_string(), nullable=False)
    column = pyarrow.field("tags", pyarrow.large_list(element), nullable=True)
    table = pyarrow.table(
        {
            "tags": pyarrow.array(
                [["x", "y"], ["z"]], pyarrow.large_list(element)
            )
        },
        schema=pyarrow.schema([column]),
    )

    narrowed, data = roundtrip(table)
    field = narrowed.field("tags")
    assert pyarrow.types.is_list(field.type), field.type
    assert field.type.value_field.type == pyarrow.string()
    assert data.column("tags").to_pylist() == [["x", "y"], ["z"]]


def test_the_element_field_keeps_its_name_nullability_and_metadata() -> None:
    """Nome, nullability e metadata dell'elemento sono parte del contratto."""

    element = pyarrow.field(
        "voce",
        pyarrow.large_string(),
        nullable=False,
        metadata={b"plenora.unit": b"codice"},
    )
    column = pyarrow.field(
        "tags",
        pyarrow.large_list(element),
        nullable=False,
        metadata={b"plenora.origin": b"catalogo"},
    )
    table = pyarrow.table(
        {"tags": pyarrow.array([["x"]], pyarrow.large_list(element))},
        schema=pyarrow.schema([column]),
    )

    narrowed, _ = roundtrip(table)
    field = narrowed.field("tags")
    assert field.nullable is False
    assert field.metadata == {b"plenora.origin": b"catalogo"}
    inner = field.type.value_field
    assert inner.name == "voce"
    assert inner.nullable is False
    assert inner.metadata == {b"plenora.unit": b"codice"}


def test_a_list_of_narrow_types_is_left_untouched() -> None:
    """Senza nulla da convertire, nessuna copia e nessun rimaneggiamento."""

    element = pyarrow.field("item", pyarrow.int64(), nullable=True)
    table = pyarrow.table(
        {"values": pyarrow.array([[1, 2]], pyarrow.list_(element))},
        schema=pyarrow.schema([pyarrow.field("values", pyarrow.list_(element))]),
    )

    narrowed, data = roundtrip(table)
    assert narrowed.field("values").type == pyarrow.list_(element)
    assert data.column("values").to_pylist() == [[1, 2]]


def test_struct_is_left_alone_because_no_writer_accepts_it() -> None:
    """Il supporto e limitato ai tipi qualificati, e lo si vede.

    Convertire uno `struct` produrrebbe un tipo comunque rifiutato dal
    writer, in piu con una copia: la scelta e non toccarlo.
    """

    inner = pyarrow.field("nome", pyarrow.large_string())
    table = pyarrow.table(
        {"persona": pyarrow.array([{"nome": "ada"}], pyarrow.struct([inner]))}
    )

    narrowed, _ = roundtrip(table)
    assert pyarrow.types.is_struct(narrowed.field("persona").type)
    assert narrowed.field("persona").type.field(0).type == pyarrow.large_string()


def test_mixed_columns_convert_only_what_needs_it() -> None:
    table = pyarrow.table(
        {
            "id": pyarrow.array([1], pyarrow.int64()),
            "label": pyarrow.array(["a"], pyarrow.large_string()),
        }
    )

    narrowed, data = roundtrip(table)
    assert narrowed.field("id").type == pyarrow.int64()
    assert narrowed.field("label").type == pyarrow.string()
    assert data.num_rows == 1
