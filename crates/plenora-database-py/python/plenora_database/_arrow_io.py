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
    """L'equivalente a offset 32 bit di un tipo a offset larghi, se esiste.

    Ritorna `None` quando non c'e nulla da convertire.

    I writer dei provider accettano `string`, `binary` e `list` di scalari;
    `pandas` non lascia scegliere. Con pandas 3 e pyarrow 25
    `Table.from_pandas` produce `large_string` per ogni colonna di testo, e
    `copy_from` finiva su "tipo Arrow non supportato dal writer PostgreSQL"
    per un DataFrame di tre righe — un limite dell'adapter presentato come
    limite del provider.

    **La conversione e ricorsiva sui tipi nested qualificati**, che sono due:

    * `list` — `PostgreSQL` la scrive come array, con elementi boolean,
      int32, int64, float32, float64 o testo;
    * `struct` — e la forma dei `range` (`lower`/`upper` testuali piu i
      flag di inclusione) e dei tipi `composite`, i cui campi portano nei
      metadata la dichiarazione nativa che il writer rilegge.

    Fermarsi al primo livello lasciava `large_list<large_string>` a
    diventare `list<large_string>`, che il writer rifiuta esattamente come
    prima: la conversione sembrava fatta e non lo era. Ogni campo figlio
    conserva nome, nullability e metadata — per i `composite` il metadata
    **e** il contratto, e ricostruire il campo con i default lo perderebbe
    in silenzio, trasformando una colonna tipizzata in testo anonimo.

    `map` resta intoccato: nessun provider lo scrive, quindi convertirlo
    produrrebbe un tipo comunque rifiutato, con in piu una copia.

    La conversione puo fallire: oltre 2 GiB di offset il cast solleva invece
    di troncare. E il comportamento giusto — l'alternativa sarebbe scrivere
    dati diversi da quelli passati.
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


def _to_ipc_bytes(source: Any) -> bytes:
    """Serializza `source` in bytes Arrow IPC stream self-contained.

    Accetta:
      - `bytes` — passato-through (assunto IPC valido, verificato dal Rust)
      - `pyarrow.Table` — batches iterati e scritti
      - `pyarrow.RecordBatch` — un unico batch
      - iterabile di `pyarrow.RecordBatch` (tutti con stesso schema)
      - `pandas.DataFrame` — convertito via `pyarrow.Table.from_pandas`
      - `list[dict]` — convertito via `pyarrow.Table.from_pylist`

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
        source = pa.Table.from_pandas(source, preserve_index=False)

    if isinstance(source, pa.Table):
        schema = source.schema
        batches = source.to_batches()
    elif isinstance(source, pa.RecordBatch):
        schema = source.schema
        batches = [source]
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
            tbl = pa.Table.from_pylist(source)
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

    source_schema = schema
    schema, replacements = _narrowed_schema(source_schema, pa)

    buf = io.BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        for batch in batches:
            writer.write_batch(
                _narrowed_batch(batch, source_schema, schema, replacements, pa)
            )
    return buf.getvalue()
