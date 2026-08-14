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
      - iterable di `pyarrow.RecordBatch` (tutti con stesso schema)

    Raises:
      - `TypeError` se il tipo non è supportato
      - `ValueError` se la lista di batches è vuota
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

    if isinstance(source, pa.Table):
        schema = source.schema
        batches = source.to_batches()
    elif isinstance(source, pa.RecordBatch):
        schema = source.schema
        batches = [source]
    elif hasattr(source, "__iter__"):
        batches = list(source)
        if not batches:
            raise ValueError("copy_from: lista batches vuota")
        first = batches[0]
        if not isinstance(first, pa.RecordBatch):
            raise TypeError(
                f"copy_from: iterable deve contenere pyarrow.RecordBatch, "
                f"trovato {type(first).__name__}"
            )
        schema = first.schema
    else:
        raise TypeError(
            f"copy_from: source deve essere bytes, pyarrow.Table, "
            f"pyarrow.RecordBatch o iterable di RecordBatch — "
            f"trovato {type(source).__name__}"
        )

    buf = io.BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return buf.getvalue()
