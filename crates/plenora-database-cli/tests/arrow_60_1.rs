#![allow(clippy::unwrap_used)]
//! Contratti osservabili dell'aggiornamento Arrow e confine RUSTSEC-2026-0442.
use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{Array, Int32Array, RecordBatch, RunArray};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};

#[test]
fn public_inspect_rejects_run_ends_and_short_bitmaps_without_payload() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/arrow");
    for file in std::fs::read_dir(root).unwrap() {
        let path = file.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if !name.starts_with("run-end-")
            && !name.starts_with("short-bitmap.")
            && !name.starts_with("struct-short-bitmap.")
        {
            continue;
        }
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_plenora-database"))
            .arg("inspect-dataset")
            .arg(path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(envelope["status"], "error");
        assert_eq!(envelope["error"]["category"], "invalid_plan");
        assert_eq!(envelope["error"]["phase"], "validate");
        assert!(!String::from_utf8_lossy(&output.stdout).contains("PAYLOAD_MUST_NOT_LEAK"));
        assert_eq!(
            envelope["error"]["message"],
            "RecordBatch Arrow IPC non decodificabile"
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    }
}

#[test]
fn map_keys_sorted_survives_rust_ipc() {
    use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow_array::MapArray;
    for sorted in [false, true] {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("a");
        builder.values().append_value(7);
        builder.append(true).unwrap();
        let (field, offsets, entries, nulls, _) = builder.finish().into_parts();
        let map = MapArray::try_new(field, offsets, entries, nulls, sorted).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            map.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(map)]).unwrap();
        let mut writer = FileWriter::try_new(Vec::new(), &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let reader =
            arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        for output in reader {
            let output = output.unwrap();
            let map = output
                .column(0)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap();
            assert_eq!(map.ordered(), sorted);
            assert_eq!(output, batch);
        }
    }
}

#[test]
fn liste_affettate_di_union_conservano_i_valori_attraverso_io() {
    use arrow_array::{ArrayRef, ListArray, UnionArray};
    use arrow_schema::{UnionFields, UnionMode};

    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        let fields = UnionFields::try_new(
            [3, 7],
            [
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ],
        )
        .unwrap();
        let union: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                vec![3_i8, 7, 3].into(),
                (mode == UnionMode::Dense).then(|| vec![2, 0, 1].into()),
                vec![
                    Arc::new(Int32Array::from(vec![Some(11), None, Some(33)])),
                    Arc::new(Int32Array::from(vec![Some(101), Some(202), None])),
                ],
            )
            .unwrap(),
        );
        let list = ListArray::new(
            Arc::new(Field::new("item", union.data_type().clone(), true)),
            ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![Some(0)]),
                Some(vec![Some(0), Some(0)]),
            ])
            .offsets()
            .clone(),
            union,
            None,
        )
        .slice(1, 1);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let mut writer = FileWriter::try_new(Vec::new(), &schema).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let mut reader =
            arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        let output = reader.next().unwrap().unwrap();
        assert_eq!(output.num_rows(), 1);
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let union = values.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.len(), 2);
        // Attesi dalla costruzione, senza usare uguaglianza ArrayData o il
        // decoder upstream come oracolo (anch'essi corretti in questa release).
        for (index, atteso) in [
            Some(if mode == UnionMode::Dense { 101 } else { 202 }),
            if mode == UnionMode::Dense {
                None
            } else {
                Some(33)
            },
        ]
        .into_iter()
        .enumerate()
        {
            let value = union.value(index);
            let value = value.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(value.iter().next().unwrap(), atteso);
        }
        assert!(reader.next().is_none());
    }
}

#[test]
fn array_del_chiamante_non_puo_dichiarare_righe_senza_run() {
    let array = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![2, 3]),
        &Int32Array::from(vec![7, 9]),
    )
    .unwrap();
    // Riproduttore upstream: nessun accesso fuori limite viene eseguito.
    for (offset, len) in [(0, 5), (2, 2), (3, 1)] {
        assert!(array
            .to_data()
            .into_builder()
            .offset(offset)
            .len(len)
            .build()
            .is_err());
    }
    assert!(array
        .to_data()
        .into_builder()
        .offset(1)
        .len(2)
        .build()
        .is_ok());
}

#[test]
fn ipc_senza_null_scrive_bitmap_vuota_e_rilegge_gli_stessi_valori() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![Some(7), Some(9)]))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new(&mut bytes, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let n = bytes.len();
    let footer_len =
        usize::try_from(i32::from_le_bytes(bytes[n - 10..n - 6].try_into().unwrap())).unwrap();
    let footer = arrow_ipc::root_as_footer(&bytes[n - 10 - footer_len..n - 10]).unwrap();
    let offset = usize::try_from(footer.recordBatches().unwrap().get(0).offset()).unwrap();
    let length = usize::try_from(i32::from_le_bytes(
        bytes[offset + 4..offset + 8].try_into().unwrap(),
    ))
    .unwrap();
    let message = arrow_ipc::root_as_message(&bytes[offset + 8..offset + 8 + length]).unwrap();
    assert_eq!(
        message
            .header_as_record_batch()
            .unwrap()
            .buffers()
            .unwrap()
            .get(0)
            .length(),
        0
    );
    let mut reader =
        arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), batch);
}
