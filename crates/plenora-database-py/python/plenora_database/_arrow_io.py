"""Helper Arrow IPC per bulk write.

Converte input Python (pyarrow.Table / RecordBatch / list) in bytes
Arrow IPC stream self-contained per il consumo da parte del binding
Rust (`_native.copy_from` / `_native.acopy_from`).
"""
from __future__ import annotations

import io
from collections.abc import Iterable, Iterator
from itertools import chain
from typing import Any


def _narrowed_type(field_type: Any, pa: Any) -> Any:
    """Restituisce l'equivalente a offset 32 bit di un tipo Arrow, se esiste.

    Ritorna None se non occorre convertire. La conversione attraversa list
    e struct conservando nomi, nullability e metadata dei campi. I writer
    richiedono offset a 32 bit per i tipi qualificati; i cast successivi
    verificano che gli offset siano rappresentabili.
    """

    if pa.types.is_large_string(field_type):
        return pa.string()
    if pa.types.is_large_binary(field_type):
        return pa.binary()
    # I tipi `view` esistono solo da pyarrow 16: chiederli a una versione
    # precedente sarebbe un AttributeError, non un tipo assente.
    for probe, replacement in (
        ("is_string_view", pa.string()),
        ("is_binary_view", pa.binary()),
    ):
        checker = getattr(pa.types, probe, None)
        if checker is not None and checker(field_type):
            return replacement

    is_large_list = pa.types.is_large_list(field_type)
    if is_large_list or pa.types.is_list(field_type):
        element = field_type.value_field
        narrowed_element = _narrowed_type(element.type, pa)
        if narrowed_element is None and not is_large_list:
            return None
        return pa.list_(_narrowed_field(element, narrowed_element, pa))

    if pa.types.is_struct(field_type):
        children = list(field_type)
        narrowed_children = [_narrowed_type(child.type, pa) for child in children]
        if all(narrowed is None for narrowed in narrowed_children):
            return None
        return pa.struct(
            [
                _narrowed_field(child, narrowed, pa)
                for child, narrowed in zip(children, narrowed_children)
            ]
        )
    return None


def _narrowed_field(field: Any, narrowed_type: Any, pa: Any) -> Any:
    """Il campo con il tipo convertito e tutto il resto identico.

    Nome, nullability e metadata sopravvivono alla conversione. Per i campi
    di un `composite` PostgreSQL il metadata porta la dichiarazione nativa
    che il writer rilegge: perderlo qui renderebbe la colonna irriconoscibile
    a valle, con un errore che parlerebbe del provider.
    """

    return pa.field(
        field.name,
        field.type if narrowed_type is None else narrowed_type,
        field.nullable,
        field.metadata,
    )


def _narrow_batches(schema: Any, batches: list, pa: Any) -> tuple:
    """Schema e batch con i tipi a offset larghi riportati a quelli stretti.

    Restituisce gli originali quando non c'e nulla da convertire, cosi il
    percorso comune non paga una copia.
    """

    narrowed_schema, replacements = _narrowed_schema(schema, pa)
    return narrowed_schema, [
        _narrowed_batch(
            batch, schema, narrowed_schema, replacements, pa
        )
        for batch in batches
    ]


def _narrowed_schema(schema: Any, pa: Any) -> tuple[Any, dict[int, Any]]:
    """Calcola una volta lo schema stretto per uno stream di batch."""

    replacements = {
        index: narrowed
        for index, field in enumerate(schema)
        if (narrowed := _narrowed_type(field.type, pa)) is not None
    }
    narrowed_schema = schema
    for index, replacement in replacements.items():
        field = schema.field(index)
        narrowed_schema = narrowed_schema.set(
            index,
            pa.field(field.name, replacement, field.nullable, field.metadata),
        )
    return narrowed_schema, replacements


def _narrowed_batch(
    batch: Any,
    source_schema: Any,
    narrowed_schema: Any,
    replacements: dict[int, Any],
    pa: Any,
) -> Any:
    """Valida e converte un batch senza materializzare l'iterabile."""

    if not isinstance(batch, pa.RecordBatch):
        raise TypeError("copy_from: l'iterabile deve contenere solo RecordBatch")
    if batch.schema != source_schema:
        raise ValueError("copy_from: i RecordBatch devono avere lo stesso schema")
    if not replacements:
        return batch
    return pa.RecordBatch.from_arrays(
        [
            column.cast(narrowed_schema.field(index).type)
            if index in replacements
            else column
            for index, column in enumerate(batch.columns)
        ],
        schema=narrowed_schema,
    )


def _require_pyarrow() -> Any:
    try:
        import pyarrow as pa
        import pyarrow.ipc  # noqa: F401  (`pa.ipc` si carica solo cosi)
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "l'interfaccia Arrow richiede pyarrow installato: `pip install pyarrow`"
        ) from exc
    return pa


def _record_batch_reader(reader: Any) -> Any:
    """Un `pyarrow.RecordBatchReader` lazy sopra un `BatchReader` nativo.

    Lo schema viene dal reader, non dal primo batch: un risultato vuoto ha
    comunque il suo schema. Ogni chunk e uno stream IPC autonomo con un solo
    batch; uno schema diverso da quello dichiarato e un errore, non un batch
    accettato.
    """

    pa = _require_pyarrow()
    schema = pa.ipc.open_stream(pa.py_buffer(reader.schema_bytes())).schema

    def batches() -> Iterator[Any]:
        for chunk in reader:
            stream = pa.ipc.open_stream(pa.py_buffer(chunk))
            if not stream.schema.equals(schema, check_metadata=True):
                raise ValueError("batch con schema diverso da quello del reader")
            yield from stream

    return pa.RecordBatchReader.from_batches(schema, batches())


def c_stream_from_reader(reader: Any, requested_schema: Any = None) -> Any:
    """`__arrow_c_stream__` del `BatchReader` nativo (PyCapsule Interface)."""

    return _record_batch_reader(reader).__arrow_c_stream__(requested_schema)


def table_from_reader(reader: Any) -> Any:
    """Tutti i batch ancora da leggere come `pyarrow.Table`."""

    return _record_batch_reader(reader).read_all()


def _to_ipc_bytes(source: Any) -> bytes:
    """Serializza `source` in bytes Arrow IPC stream self-contained.

    Accetta:
      - `bytes` — passato-through (assunto IPC valido, verificato dal Rust)
      - `pyarrow.Table` — batches iterati e scritti
      - `pyarrow.RecordBatch` — un unico batch
      - iterabile di `pyarrow.RecordBatch` (tutti con stesso schema)
      - `pandas.DataFrame` — convertito via `pyarrow.Table.from_pandas`
      - `list[dict]` — convertito via `pyarrow.Table.from_pylist`
      - qualunque oggetto con `__arrow_c_stream__` (Arrow PyCapsule
        Interface): `pyarrow.RecordBatchReader`, il `BatchReader` di
        `Session.read`, tabelle di altre librerie

    Raises:
      - `TypeError` se il tipo non è supportato
      - `ValueError` se la lista è vuota o gli elementi hanno tipi misti
      - `ImportError` se pyarrow non è installato (a meno di bytes)
    """
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)

    try:
        import pyarrow as pa
        import pyarrow.ipc as ipc
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "copy_from richiede pyarrow installato quando `source` "
            "non è già bytes: `pip install pyarrow`"
        ) from exc

    # pandas DataFrame — richiede pandas installato solo se usato
    if type(source).__name__ == "DataFrame" and hasattr(source, "to_dict"):
        # duck-type: pandas.DataFrame ha to_dict + iloc + columns
        try:
            source = pa.Table.from_pandas(source, preserve_index=False)
        except (pa.ArrowException, ValueError, TypeError, OverflowError):
            raise ValueError("copy_from: conversione DataFrame Arrow non valida") from None

    if isinstance(source, pa.Table):
        schema = source.schema
        batches = source.to_batches()
    elif isinstance(source, pa.RecordBatch):
        schema = source.schema
        batches = [source]
    elif hasattr(source, "__arrow_c_stream__"):
        # PyCapsule Interface: lo schema e quello dichiarato dal produttore,
        # anche per uno stream vuoto.
        try:
            reader = pa.RecordBatchReader.from_stream(source)
        except (pa.ArrowException, ValueError, TypeError):
            raise ValueError("copy_from: stream Arrow non leggibile") from None
        schema = reader.schema
        batches = reader
    elif isinstance(source, list):
        if not source:
            raise ValueError("copy_from: lista vuota")
        first = source[0]
        if isinstance(first, pa.RecordBatch):
            # lista di RecordBatch — tutti devono avere stesso schema
            batches = source
            schema = first.schema
        elif isinstance(first, dict):
            # lista di dict — convertibile via pyarrow.Table.from_pylist
            try:
                tbl = pa.Table.from_pylist(source)
            except (pa.ArrowException, ValueError, TypeError, OverflowError):
                raise ValueError("copy_from: conversione record Arrow non valida") from None
            schema = tbl.schema
            batches = tbl.to_batches()
        else:
            raise TypeError(
                f"copy_from: lista deve contenere pyarrow.RecordBatch o dict, "
                f"trovato {type(first).__name__}"
            )
    elif isinstance(source, Iterable):
        iterator: Iterator[Any] = iter(source)
        try:
            first = next(iterator)
        except StopIteration:
            raise ValueError("copy_from: iterabile vuoto") from None
        if not isinstance(first, pa.RecordBatch):
            raise TypeError(
                "copy_from: l'iterabile deve contenere pyarrow.RecordBatch"
            )
        schema = first.schema
        batches = chain((first,), iterator)
    else:
        raise TypeError(
            f"copy_from: source deve essere bytes, pyarrow.Table/RecordBatch, "
            f"iterabile di RecordBatch, list di dict o pandas.DataFrame — "
            f"trovato {type(source).__name__}"
        )

    try:
        source_schema = schema
        schema, replacements = _narrowed_schema(source_schema, pa)
        buf = io.BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            for batch in batches:
                writer.write_batch(
                    _narrowed_batch(batch, source_schema, schema, replacements, pa)
                )
    except (pa.ArrowException, ValueError, TypeError, OverflowError):
        raise ValueError("copy_from: serializzazione Arrow non valida") from None
    return buf.getvalue()
