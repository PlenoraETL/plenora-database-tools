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
def test_struct_short_bitmap_rejected_by_sdk_at_prepare(mode):
    from pathlib import Path

    raw = (Path(__file__).parent / "fixtures" / f"struct-short-bitmap.{mode}").read_bytes()
    reader = (pa.ipc.open_file if mode == "file" else pa.ipc.open_stream)(raw)
    # Apertura/schema validi; lettura e validazione del batch dentro lo SDK.
    def batches():
        if mode == "file":
            yield reader.get_batch(0)
        else:
            yield reader.read_next_batch()

    with pytest.raises(PlenoraDataMappingError) as error:
        _to_ipc_bytes(batches())
    assert error.value.category == "data_mapping"
    assert error.value.phase == "prepare"
    assert "PAYLOAD_MUST_NOT_LEAK" not in str(error.value)
