"""Helper Arrow IPC per bulk write (P3 v0.1.2).

Converte input Python (pyarrow.Table / RecordBatch / list) in bytes
Arrow IPC stream self-contained per il consumo da parte del binding
Rust (`_native.copy_from` / `_native.acopy_from`).
"""
from __future__ import annotations

import io
from typing import Any


def _to_ipc_bytes(source: Any) -> bytes:
    """Serializza `source` in bytes Arrow IPC stream self-contained.

    Accetta:
      - `bytes` — passato-through (assunto IPC valido, verificato dal Rust)
      - `pyarrow.Table` — batches iterati e scritti
      - `pyarrow.RecordBatch` — un unico batch
      - lista di `pyarrow.RecordBatch` (tutti con stesso schema)
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
    else:
        raise TypeError(
            f"copy_from: source deve essere bytes, pyarrow.Table/RecordBatch, "
            f"list di RecordBatch/dict o pandas.DataFrame — "
            f"trovato {type(source).__name__}"
        )

    buf = io.BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return buf.getvalue()
