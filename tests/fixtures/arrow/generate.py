"""Generate IPC interoperability and malformed-dictionary fixtures with PyArrow.

Run manually with the repository's pinned SDK test requirements. Only the
DictionaryBatch.data vtable entry is cleared; the payload and framing remain
intact. Tests consume the committed bytes and need no fixture regeneration.
"""
from pathlib import Path
import struct

import pyarrow as pa

ROOT = Path(__file__).resolve().parent
MARKER = "PAYLOAD_MUST_NOT_LEAK"


def slot(data, table, index):
    vtable = table - struct.unpack_from("<i", data, table)[0]
    return vtable + 4 + index * 2


def corrupt_dictionary(raw, start):
    data = bytearray(raw)
    while True:
        assert data[start:start + 4] == b"\xff" * 4
        size = struct.unpack_from("<I", data, start + 4)[0]
        assert size
        message = start + 8
        table = message + struct.unpack_from("<I", data, message)[0]
        type_offset = struct.unpack_from("<H", data, slot(data, table, 1))[0]
        if data[table + type_offset] == 2:  # MessageHeader.DictionaryBatch
            header_offset = struct.unpack_from("<H", data, slot(data, table, 2))[0]
            header = table + header_offset
            dictionary = header + struct.unpack_from("<I", data, header)[0]
            entry = slot(data, dictionary, 1)  # DictionaryBatch.data
            assert struct.unpack_from("<H", data, entry)[0]
            struct.pack_into("<H", data, entry, 0)
            return bytes(data)
        vtable = table - struct.unpack_from("<i", data, table)[0]
        vtable_size = struct.unpack_from("<H", data, vtable)[0]
        body_offset = struct.unpack_from("<H", data, slot(data, table, 3))[0] if vtable_size > 10 else 0
        body_size = struct.unpack_from("<q", data, table + body_offset)[0] if body_offset else 0
        start = message + size + body_size


def main():
    values = pa.array([MARKER, None, "value"]).dictionary_encode()
    field = pa.field("label", values.type, metadata={"test.unit": "label"})
    schema = pa.schema([field], metadata={"plenora.contract.version": "1"})
    batch = pa.record_batch([values], schema=schema)
    for mode, start in (("file", 8), ("stream", 0)):
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_file if mode == "file" else pa.ipc.new_stream
        with writer(sink, schema) as output:
            output.write_batch(batch)
        raw = sink.getvalue().to_pybytes()
        (ROOT / f"dictionary.{mode}").write_bytes(raw)
        (ROOT / f"dictionary-missing-data.{mode}").write_bytes(corrupt_dictionary(raw, start))


if __name__ == "__main__":
    main()
