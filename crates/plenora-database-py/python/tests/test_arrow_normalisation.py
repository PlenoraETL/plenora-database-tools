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


def test_struct_of_large_utf8_is_narrowed() -> None:
    """Lo struct **e** un tipo qualificato: range e composite lo usano.

    Il test precedente fissava il contrario — "nessun writer accetta struct,
    quindi non si tocca" — ed era falso: `PostgreSQL` scrive i `range` come
    struct di `lower`/`upper` testuali piu i flag, e i `composite` come
    struct di campi testuali. Uno `struct<large_string>` arrivava intatto al
    writer, che lo rifiutava.
    """

    inner = pyarrow.field("nome", pyarrow.large_string(), nullable=False)
    column = pyarrow.field("persona", pyarrow.struct([inner]), nullable=False)
    table = pyarrow.table(
        {"persona": pyarrow.array([{"nome": "ada"}], pyarrow.struct([inner]))},
        schema=pyarrow.schema([column]),
    )

    narrowed, data = roundtrip(table)
    field = narrowed.field("persona")
    assert pyarrow.types.is_struct(field.type)
    assert field.type.field(0).type == pyarrow.string()
    assert field.nullable is False
    assert data.column("persona").to_pylist() == [{"nome": "ada"}]


def test_a_range_shaped_struct_keeps_its_field_names_and_nullability() -> None:
    """La forma dei `range` PostgreSQL attraversa l'adapter intatta.

    Sono **sette** campi, quelli che `range_fields()` costruisce nel lettore
    PostgreSQL: `lower`/`upper` testuali e nullable, e cinque flag booleani
    non nullable. I due `*_unbounded` distinguono un estremo aperto da un
    estremo nullo, che nella struct si presentano identici — `lower` a
    `None` — e senza di loro il test fissava una forma che il writer non
    riceve mai.

    Il campo esterno porta la propria nullability e i propri metadata: e
    dalla dichiarazione nativa nei metadata che il writer sa quale tipo
    `range` sta scrivendo, e ricostruire il campo con i default la
    perderebbe in silenzio.
    """

    declaration = {b"plenora.postgres.native_declaration": b"tstzrange"}
    fields = [
        pyarrow.field("lower", pyarrow.large_string(), nullable=True),
        pyarrow.field("upper", pyarrow.large_string(), nullable=True),
        pyarrow.field("lower_inclusive", pyarrow.bool_(), nullable=False),
        pyarrow.field("upper_inclusive", pyarrow.bool_(), nullable=False),
        pyarrow.field("lower_unbounded", pyarrow.bool_(), nullable=False),
        pyarrow.field("upper_unbounded", pyarrow.bool_(), nullable=False),
        pyarrow.field("empty", pyarrow.bool_(), nullable=False),
    ]
    struct_type = pyarrow.struct(fields)
    column = pyarrow.field(
        "periodo", struct_type, nullable=False, metadata=declaration
    )
    table = pyarrow.table(
        {
            "periodo": pyarrow.array(
                [
                    {
                        "lower": "2026-01-01",
                        "upper": "2026-12-31",
                        "lower_inclusive": True,
                        "upper_inclusive": False,
                        "lower_unbounded": False,
                        "upper_unbounded": False,
                        "empty": False,
                    },
                    {
                        # Estremo superiore aperto: `upper` e nullo **e**
                        # `upper_unbounded` e vero. Solo il flag distingue
                        # questo caso da un valore mancante.
                        "lower": "2026-01-01",
                        "upper": None,
                        "lower_inclusive": True,
                        "upper_inclusive": False,
                        "lower_unbounded": False,
                        "upper_unbounded": True,
                        "empty": False,
                    },
                ],
                struct_type,
            )
        },
        schema=pyarrow.schema([column]),
    )

    narrowed, data = roundtrip(table)
    outer = narrowed.field("periodo")
    assert outer.nullable is False
    assert outer.metadata == declaration

    converted = outer.type
    assert [converted.field(i).name for i in range(converted.num_fields)] == [
        "lower",
        "upper",
        "lower_inclusive",
        "upper_inclusive",
        "lower_unbounded",
        "upper_unbounded",
        "empty",
    ]
    for index in (0, 1):
        assert converted.field(index).type == pyarrow.string()
        assert converted.field(index).nullable is True
    for index in range(2, converted.num_fields):
        assert converted.field(index).type == pyarrow.bool_()
        assert converted.field(index).nullable is False

    rows = data.column("periodo").to_pylist()
    assert rows[0]["lower"] == "2026-01-01"
    assert rows[0]["upper_unbounded"] is False
    assert rows[1]["upper"] is None
    assert rows[1]["upper_unbounded"] is True


def test_a_composite_struct_keeps_the_native_declaration_metadata() -> None:
    """Per i `composite` il metadata del campo **e** il contratto.

    Il writer rilegge `plenora.postgres.native_declaration` per sapere che
    tipo nativo sta scrivendo. Ricostruire il campo con i default lo
    perderebbe in silenzio, e la colonna arriverebbe come testo anonimo.
    """

    declaration = {b"plenora.postgres.native_declaration": b"public.indirizzo"}
    fields = [
        pyarrow.field("via", pyarrow.large_string(), metadata=declaration),
        pyarrow.field("civico", pyarrow.large_string(), metadata=declaration),
    ]
    struct_type = pyarrow.struct(fields)
    table = pyarrow.table(
        {
            "indirizzo": pyarrow.array(
                [{"via": "Roma", "civico": "1"}], struct_type
            )
        },
        schema=pyarrow.schema([pyarrow.field("indirizzo", struct_type)]),
    )

    narrowed, _ = roundtrip(table)
    converted = narrowed.field("indirizzo").type
    for index in range(converted.num_fields):
        assert converted.field(index).type == pyarrow.string()
        assert converted.field(index).metadata == declaration


def test_a_struct_of_narrow_types_is_left_untouched() -> None:
    """Senza nulla da convertire, la struct non viene ricostruita."""

    struct_type = pyarrow.struct(
        [pyarrow.field("id", pyarrow.int64()), pyarrow.field("nome", pyarrow.string())]
    )
    table = pyarrow.table(
        {"riga": pyarrow.array([{"id": 1, "nome": "ada"}], struct_type)},
        schema=pyarrow.schema([pyarrow.field("riga", struct_type)]),
    )

    narrowed, _ = roundtrip(table)
    assert narrowed.field("riga").type == struct_type


def test_a_list_of_structs_of_large_utf8_is_narrowed_at_both_levels() -> None:
    """La ricorsione attraversa i due tipi nested, non uno solo."""

    inner = pyarrow.field("nome", pyarrow.large_string(), nullable=False)
    element = pyarrow.field("item", pyarrow.struct([inner]), nullable=False)
    list_type = pyarrow.large_list(element)
    table = pyarrow.table(
        {"persone": pyarrow.array([[{"nome": "ada"}]], list_type)},
        schema=pyarrow.schema([pyarrow.field("persone", list_type)]),
    )

    narrowed, data = roundtrip(table)
    field = narrowed.field("persone")
    assert pyarrow.types.is_list(field.type)
    inner_struct = field.type.value_field.type
    assert pyarrow.types.is_struct(inner_struct)
    assert inner_struct.field(0).type == pyarrow.string()
    assert inner_struct.field(0).nullable is False
    assert data.column("persone").to_pylist() == [[{"nome": "ada"}]]


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
