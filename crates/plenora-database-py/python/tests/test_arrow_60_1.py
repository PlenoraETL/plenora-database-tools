"""Confine Python prima del writer IPC e proprieta Map conservate."""
import pytest

pa = pytest.importorskip("pyarrow")
from plenora_database._arrow_io import _to_ipc_bytes
from plenora_database.errors import PlenoraDataMappingError


@pytest.mark.parametrize("width", [16, 32, 64])
@pytest.mark.parametrize("nesting", ["plain", "list", "struct"])
@pytest.mark.parametrize("source_kind", ["table", "batch", "reader", "iterable"])
def test_invalid_run_ends_rejected_before_serialization(width, nesting, source_kind):
    ends = pa.array([123, 257], getattr(pa, f"int{width}")())
    runs = pa.RunEndEncodedArray.from_arrays(ends, pa.array(["SECRET", "last"]))
    values = {
        "plain": runs,
        "list": pa.ListArray.from_arrays([0, 257], runs),
        "struct": pa.StructArray.from_arrays([runs], names=["child"]),
    }[nesting]
    batch = pa.record_batch([values], names=["v"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    raw = sink.getvalue().to_pybytes()
    size = width // 8
    marker = (123).to_bytes(size, "little") + (257).to_bytes(size, "little")
    assert raw.count(marker) == 1
    raw = raw.replace(marker, marker[:size] + (256).to_bytes(size, "little"))
    # PyArrow materializza senza full validation: non leggere celle.
    broken = pa.ipc.open_stream(raw).read_next_batch()
    source = {
        "table": lambda: pa.Table.from_batches([broken]),
        "batch": lambda: broken,
        "reader": lambda: pa.RecordBatchReader.from_batches(broken.schema, [batch, broken]),
        "iterable": lambda: iter([batch, broken]),
    }[source_kind]()
    with pytest.raises(PlenoraDataMappingError) as error:
        _to_ipc_bytes(source)
    assert "SECRET" not in str(error.value)
    assert error.value.phase == "prepare"


@pytest.mark.parametrize("sorted_keys", [False, True])
def test_map_keys_sorted_survives_python_ipc(sorted_keys):
    kind = pa.map_(pa.string(), pa.int32(), keys_sorted=sorted_keys)
    values = pa.array([[('a', 7), ('b', 9)], None], type=kind)
    batch = pa.record_batch([values], names=["m"])
    result = pa.ipc.open_stream(_to_ipc_bytes([batch, batch])).read_all()
    assert result.schema.field(0).type.keys_sorted is sorted_keys
    assert result.to_pylist() == [{"m": [('a', 7), ('b', 9)]}, {"m": None}] * 2


@pytest.mark.parametrize("mode", ["file", "stream"])
@pytest.mark.parametrize("source_kind", ["iterable", "reader", "batch", "table"])
def test_struct_short_bitmap_rejected_by_sdk_at_prepare(mode, source_kind):
    from pathlib import Path

    raw = (Path(__file__).parent / "fixtures" / f"struct-short-bitmap.{mode}").read_bytes()
    reader = (pa.ipc.open_file if mode == "file" else pa.ipc.open_stream)(raw)
    # Apertura/schema validi; lettura e validazione del batch dentro lo SDK.
    def batches():
        if mode == "file":
            yield reader.get_batch(0)
        else:
            yield reader.read_next_batch()

    source = {
        "iterable": batches,
        "reader": lambda: pa.RecordBatchReader.from_batches(reader.schema, batches()),
        "batch": lambda: next(batches()),
        "table": lambda: pa.Table.from_batches(list(batches()), schema=reader.schema),
    }[source_kind]()
    with pytest.raises(PlenoraDataMappingError) as error:
        _to_ipc_bytes(source)
    assert error.value.category == "data_mapping"
    assert error.value.phase == "prepare"
    assert "PAYLOAD_MUST_NOT_LEAK" not in str(error.value)


@pytest.mark.parametrize("surface", ["postgres", "family", "postgres_async", "family_async"])
@pytest.mark.parametrize("late", [False, True])
def test_public_copy_rejects_original_reader_before_native(surface, late):
    import asyncio
    from pathlib import Path
    import plenora_database as db
    from plenora_database._session import Session
    from plenora_database._async_session import AsyncSession

    raw = (Path(__file__).parent / "fixtures" / "struct-short-bitmap.stream").read_bytes()
    broken = pa.ipc.open_stream(raw).read_next_batch()
    good = pa.record_batch([pa.nulls(9, type=broken.column(0).type)],
                           schema=broken.schema)
    consumed = []

    def batches():
        if late:
            yield good
        consumed.append(True)
        yield broken

    class NativeSpy:
        called = False

        def copy_from(self, *args, **kwargs):
            self.called = True

        async def acopy_from(self, *args, **kwargs):
            self.called = True

    native = NativeSpy()
    cls = {"postgres": Session, "family": db._DatabaseSessionWrapper,
           "postgres_async": AsyncSession, "family_async": db._AsyncDatabaseSessionWrapper}[surface]
    source = pa.RecordBatchReader.from_batches(broken.schema, batches())
    assert consumed == []
    with pytest.raises(PlenoraDataMappingError) as error:
        if surface.endswith("_async"):
            asyncio.run(cls(native).acopy_from("s", "t", source, mapping_policy="strict"))
        else:
            cls(native).copy_from("s", "t", source, mapping_policy="strict")
    assert consumed == [True]
    assert not native.called
    assert error.value.category == "data_mapping"
    assert error.value.phase == "prepare"
    assert "PAYLOAD_MUST_NOT_LEAK" not in str(error.value)


MALFORMED = [
    "struct-short-bitmap", "primitive-short", "list-offset-oob",
    "list-offset-negative", "map-offset-oob", "struct-children-different",
    "dictionary-oob", "struct-child-short-bitmap", "list-struct-short-bitmap", "run-end-32-plain",
]


@pytest.mark.parametrize("case", MALFORMED)
@pytest.mark.parametrize("kind", ["batch", "table", "reader", "iterable", "array", "chunked_array", "c_stream", "c_array"])
@pytest.mark.parametrize("surface", ["postgres", "family", "postgres_async", "family_async"])
def test_original_arrow_matrix(case, kind, surface):
    import asyncio
    from pathlib import Path
    import plenora_database as db
    from plenora_database._session import Session
    from plenora_database._async_session import AsyncSession

    broken = list(pa.ipc.open_stream(
        (Path(__file__).parent / "fixtures" / f"{case}.stream").read_bytes()
    ))[-1]
    with pytest.raises(pa.ArrowInvalid):
        broken.validate(full=True)

    class CapsuleStream:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.RecordBatchReader.from_batches(broken.schema, [broken]).__arrow_c_stream__(requested_schema)

    class CapsuleArray:
        def __arrow_c_array__(self, requested_schema=None):
            return broken.__arrow_c_array__(requested_schema)

    good = pa.record_batch([pa.nulls(len(broken), type=broken.column(0).type)], schema=broken.schema)
    source = {
        "batch": lambda: broken,
        "table": lambda: pa.Table.from_batches([broken]),
        "reader": lambda: pa.RecordBatchReader.from_batches(broken.schema, iter([good, broken])),
        "iterable": lambda: iter([good, broken]),
        "array": broken.to_struct_array,
        "chunked_array": lambda: pa.chunked_array([broken.to_struct_array()]),
        "c_stream": CapsuleStream,
        "c_array": CapsuleArray,
    }[kind]()

    class NativeSpy:
        called = False
        def copy_from(self, *args, **kwargs):
            self.called = True
        async def acopy_from(self, *args, **kwargs):
            self.called = True

    native = NativeSpy()
    cls = {"postgres": Session, "family": db._DatabaseSessionWrapper,
           "postgres_async": AsyncSession, "family_async": db._AsyncDatabaseSessionWrapper}[surface]
    def invoke():
        if surface.endswith("_async"):
            return asyncio.run(cls(native).acopy_from("s", "t", source, mapping_policy="strict"))
        return cls(native).copy_from("s", "t", source, mapping_policy="strict")

    # C-only producers have lost physical buffer lengths: owner-approved limit.
    if kind.startswith("c_") and case in {
        "struct-short-bitmap", "primitive-short", "struct-child-short-bitmap",
        "list-struct-short-bitmap",
    }:
        invoke()
        assert native.called
    else:
        with pytest.raises(PlenoraDataMappingError) as error:
            invoke()
        assert error.value.category == "data_mapping"
        assert error.value.phase == "prepare"
        assert "PAYLOAD_MUST_NOT_LEAK" not in str(error.value)
        assert not native.called


@pytest.mark.parametrize("kind", ["batch", "table", "reader", "array", "chunked_array", "c_stream", "c_array"])
def test_valid_original_and_capsules(kind):
    batch = pa.record_batch([pa.array([1, None, 3])], names=["v"])
    class Stream:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.RecordBatchReader.from_batches(batch.schema, [batch]).__arrow_c_stream__(requested_schema)
    class Array:
        def __arrow_c_array__(self, requested_schema=None):
            return batch.__arrow_c_array__(requested_schema)
    source = {"batch": lambda: batch, "table": lambda: pa.Table.from_batches([batch]),
              "reader": lambda: pa.RecordBatchReader.from_batches(batch.schema, [batch]),
              "array": batch.to_struct_array,
              "chunked_array": lambda: pa.chunked_array([batch.to_struct_array()]),
              "c_stream": Stream, "c_array": Array}[kind]()
    assert pa.ipc.open_stream(_to_ipc_bytes(source)).read_next_batch().equals(batch)


@pytest.mark.parametrize("export", ["table", "stream"])
def test_reader_export_validates_original_batch_before_c(export):
    from pathlib import Path
    from plenora_database._arrow_io import table_from_reader, c_stream_from_reader
    raw = (Path(__file__).parent / "fixtures" / "struct-short-bitmap.stream").read_bytes()
    class NativeReader:
        def schema_bytes(self):
            return raw
        def __iter__(self):
            yield raw
    if export == "table":
        with pytest.raises(PlenoraDataMappingError) as error:
            table_from_reader(NativeReader())
        assert error.value.phase == "read"
    else:
        capsule = c_stream_from_reader(NativeReader())
        with pytest.raises(pa.ArrowException) as error:
            pa.RecordBatchReader._import_from_c_capsule(capsule).read_next_batch()
    assert "PAYLOAD_MUST_NOT_LEAK" not in str(error.value)
