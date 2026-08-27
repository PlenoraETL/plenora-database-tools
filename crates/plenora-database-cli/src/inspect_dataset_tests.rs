use super::*;
use arrow_ipc::writer::FileWriter;
use plenora_database_core::arrow::array::{ArrayRef, BinaryArray};
use plenora_database_core::arrow::schema::{Field, Schema};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct TestFile(std::path::PathBuf);

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn point_xy() -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&9.19_f64.to_le_bytes());
    bytes.extend_from_slice(&45.46_f64.to_le_bytes());
    bytes
}

fn schema(crs_id: &str, srid: Option<&str>, axis: Option<&str>) -> SchemaRef {
    let mut metadata = HashMap::from([
        (protocol::FIELD_ID.to_owned(), "7".to_owned()),
        (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
        (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
        (
            protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
            "exact".to_owned(),
        ),
        (protocol::GEOMETRY_TYPES.to_owned(), "point".to_owned()),
        (
            protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
            "resolved".to_owned(),
        ),
        (protocol::GEOMETRY_CRS_ID.to_owned(), crs_id.to_owned()),
    ]);
    if let Some(value) = srid {
        metadata.insert(protocol::GEOMETRY_SRID.to_owned(), value.to_owned());
    }
    if let Some(value) = axis {
        metadata.insert(protocol::GEOMETRY_AXIS_ORDER.to_owned(), value.to_owned());
    }
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("geometry", DataType::Binary, true).with_metadata(metadata)],
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn write_fixture(schema: SchemaRef, values: &[Option<Vec<u8>>]) -> TestFile {
    let path = std::env::temp_dir().join(format!(
        "plenora-database-inspect-{}-{}.arrow",
        std::process::id(),
        FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = File::create(&path).expect("create IPC fixture");
    let mut writer = FileWriter::try_new(&mut file, &schema).expect("IPC writer");
    let borrowed = values.iter().map(Option::as_deref).collect::<Vec<_>>();
    let array: ArrayRef = Arc::new(BinaryArray::from(borrowed));
    let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
    writer.write(&batch).expect("write IPC fixture");
    writer.finish().expect("finish IPC fixture");
    drop(writer);
    drop(file);
    TestFile(path)
}

#[test]
fn valid_ipc_reports_every_geometry_cell() {
    let fixture = write_fixture(
        schema("OGC:CRS84", None, Some("lon_lat")),
        &[Some(point_xy()), None],
    );
    let report = inspect(&fixture.0).expect("inspect valid IPC");
    assert_eq!(report["status"], "ok");
    assert_eq!(report["rows"], 2);
    assert_eq!(report["fields"][0]["field_id"], 7);
    assert_eq!(
        report["fields"][0]["geometry_cells"]
            .as_array()
            .expect("geometry cells")
            .len(),
        2
    );
    assert_eq!(report["fields"][0]["geometry_cells"][0]["dimensions"], "xy");
    assert_eq!(report["fields"][0]["geometry_cells"][1]["status"], "null");
}

#[test]
fn conflicting_crs_fails_before_data_is_accepted() {
    let fixture = write_fixture(
        schema("EPSG:4326", Some("3003"), Some("lat_lon")),
        &[Some(point_xy())],
    );
    let error = inspect(&fixture.0).expect_err("conflicting CRS");
    assert_eq!(
        error.database_error().category,
        plenora_database_core::ErrorCategory::Crs
    );
    assert_eq!(
        error.database_error().phase,
        plenora_database_core::ErrorPhase::Validate
    );
    assert_eq!(
        error.database_error().remote_effect,
        plenora_database_core::RemoteEffect::None
    );
    assert_eq!(
        error.database_error().retry,
        plenora_database_core::RetryDisposition::Never
    );
    assert_eq!(
        error.database_error().message,
        "identificatore CRS e SRID numerico divergenti"
    );
    let envelope: Value =
        serde_json::from_str(&error.to_json().expect("CRS error envelope serialization"))
            .expect("CRS error envelope JSON");
    assert_eq!(envelope["status"], "error");
    assert_eq!(envelope["protocol_version"], 1);
    assert_eq!(envelope["error"]["category"], "crs");
    assert_eq!(envelope["error"]["phase"], "validate");
    assert_eq!(envelope["error"]["remote_effect"], "none");
    assert_eq!(envelope["error"]["retry"]["kind"], "never");
}

#[test]
fn malformed_geometry_fails_with_cell_coordinates() {
    let fixture = write_fixture(
        schema("OGC:CRS84", None, Some("lon_lat")),
        &[Some(vec![1, 2, 3])],
    );
    let error = inspect(&fixture.0).expect_err("malformed WKB");
    // Le coordinate della cella restano — sono posizione, non contenuto —
    // e la causa della libreria non c'e piu: un errore di decodifica WKB
    // nomina volentieri byte e offset del dato.
    let message = &error.database_error().message;
    assert!(message.contains("batch 0"), "{message}");
    assert!(message.contains("riga 0"), "{message}");
}

#[test]
fn geometry_cell_product_is_bounded_before_output_growth() {
    let mut cells = MAX_GEOMETRY_CELLS;
    assert!(reserve_geometry_cells(&mut cells, 0).is_ok());
    assert!(reserve_geometry_cells(&mut cells, 1).is_err());
    let mut overflow = u64::MAX;
    assert!(reserve_geometry_cells(&mut overflow, 1).is_err());
}
