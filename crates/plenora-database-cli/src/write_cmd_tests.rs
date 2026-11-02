use super::*;
use plenora_database_core::arrow::array::RecordBatch;
use plenora_database_core::arrow::schema::{DataType, Field, Schema, SchemaRef};
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::ErrorCategory;
use std::collections::HashMap;
use std::sync::Arc;

struct SchemaOnly(SchemaRef);

impl BatchStream for SchemaOnly {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async { Ok(None) })
    }
}

fn operation() -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: "target".to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

/// REJ-001: il dry run giudica lo schema come la scrittura. Fino al 24/10
/// rispondeva `dry_run` anche a uno schema che la scrittura rifiuta, qui un
/// campo `geoarrow.wkb` senza le chiavi obbligatorie del vocabolario.
#[tokio::test]
async fn dry_run_rejects_a_schema_the_write_would_reject() {
    let field = Field::new("geom", DataType::Binary, true).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_owned(),
        "geoarrow.wkb".to_owned(),
    )]));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![field],
        HashMap::from([("plenora.contract.version".to_owned(), "1".to_owned())]),
    ));
    let error = execute_bulk_write(
        "UNUSED_DSN",
        operation(),
        Box::new(SchemaOnly(schema)),
        DryRun::Offline,
    )
    .await
    .expect_err("schema non conforme in dry run");
    assert_eq!(error.database_error().category, ErrorCategory::Schema);
}

/// Un campo geometria conforme del vettore `crs-declared-unresolved`: un CRS
/// dichiarato senza `plenora.geometry.srid` e con un `crs_id` non `EPSG`.
fn esri_without_srid() -> SchemaRef {
    let metadata = HashMap::from(
        [
            ("ARROW:extension:name", "geoarrow.wkb"),
            ("plenora.field_id", "0"),
            ("plenora.geometry.encoding", "wkb"),
            ("plenora.geometry.dimensions", "xy"),
            ("plenora.geometry.types_declaration", "exact"),
            ("plenora.geometry.types", "point"),
            ("plenora.geometry.spatial_semantics", "geometry"),
            ("plenora.geometry.precision", "float64"),
            ("plenora.geometry.crs_resolution", "declared_unresolved"),
            ("plenora.geometry.crs_id", "ESRI:102100"),
            ("plenora.geometry.axis_order", "easting_northing"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned())),
    );
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("geom", DataType::Binary, true).with_metadata(metadata)],
        HashMap::from([("plenora.contract.version".to_owned(), "1".to_owned())]),
    ))
}

/// Il dry run compila il piano come la scrittura (`compile_write_plan`). Fino
/// al 24/10 giudicava soltanto lo schema: uno SRID non derivabile da
/// `ESRI:102100` dava `dry_run` (exit 0) e la scrittura `crs` (exit 3).
#[tokio::test]
async fn dry_run_compiles_the_plan_like_the_write() {
    let schema = esri_without_srid();
    let error = execute_bulk_write(
        "UNUSED_DSN",
        operation(),
        Box::new(SchemaOnly(Arc::clone(&schema))),
        DryRun::Offline,
    )
    .await
    .expect_err("SRID non derivabile in dry run");
    assert_eq!(error.database_error().category, ErrorCategory::Crs);
    // La stessa funzione, la stessa risposta, senza server.
    let direct = plenora_db_postgres::compile_write_plan(&schema, &operation())
        .expect_err("SRID non derivabile in compilazione");
    assert_eq!(direct.category, ErrorCategory::Crs);
}

/// Uno stream con i batch dati e, se serve, un totale dichiarato.
struct Fixed {
    schema: SchemaRef,
    batches: std::vec::IntoIter<RecordBatch>,
    declared: Option<u64>,
}

impl BatchStream for Fixed {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        let next = self.batches.next();
        Box::pin(async move { Ok(next) })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        self.declared
    }
}

/// Lo schema del vettore `invalid-wkb-truncated`, con l'encoding dato.
fn point_4326(encoding: &str, srid: Option<&str>) -> SchemaRef {
    let mut metadata = HashMap::from(
        [
            ("plenora.field_id", "1"),
            ("ARROW:extension:name", "geoarrow.wkb"),
            ("plenora.geometry.encoding", encoding),
            ("plenora.geometry.dimensions", "xy"),
            ("plenora.geometry.spatial_semantics", "geometry"),
            ("plenora.geometry.precision", "float64"),
            ("plenora.geometry.types_declaration", "exact"),
            ("plenora.geometry.types", "point"),
            ("plenora.geometry.crs_resolution", "resolved"),
            ("plenora.geometry.crs_id", "EPSG:4326"),
            ("plenora.geometry.axis_order", "lon_lat"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned())),
    );
    if let Some(srid) = srid {
        metadata.insert("plenora.geometry.srid".to_owned(), srid.to_owned());
    }
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                "plenora.field_id".to_owned(),
                "0".to_owned(),
            )])),
            Field::new("geometry", DataType::Binary, true).with_metadata(metadata),
        ],
        HashMap::from([("plenora.contract.version".to_owned(), "1".to_owned())]),
    ))
}

fn stream(schema: &SchemaRef, values: &[&[u8]], declared: Option<u64>) -> Box<dyn BatchStream> {
    use plenora_database_core::arrow::array::{BinaryArray, Int64Array};
    let ids = (1..=values.len())
        .map(|id| i64::try_from(id).expect("id"))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(BinaryArray::from(values.to_vec())),
        ],
    )
    .expect("batch");
    Box::new(Fixed {
        schema: Arc::clone(schema),
        batches: vec![batch].into_iter(),
        declared,
    })
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

fn create() -> WriteOperation {
    WriteOperation {
        mode: WriteMode::Create,
        ..operation()
    }
}

const UNSET_DSN: &str = "PLENORA_DRY_RUN_TEST_UNSET_DSN";
const TRUNCATED: &str = "01010000000000000000205ec0";
const POINT: &str = "0101000000000000000000f03f0000000000000040";

/// Il dry run giudica ogni valore come la scrittura (GEO-011). Fino al 24/10
/// leggeva soltanto lo schema: il punto senza l'ultima ordinata del vettore
/// `invalid-wkb-truncated` dava `dry_run` (exit 0), la scrittura
/// `data_mapping` (exit 3) in fase `write`.
#[tokio::test]
async fn dry_run_judges_every_value_like_the_write() {
    let schema = point_4326("wkb", None);
    let truncated = hex(TRUNCATED);
    let error = execute_bulk_write(
        UNSET_DSN,
        create(),
        stream(&schema, &[&truncated], None),
        DryRun::Offline,
    )
    .await
    .expect_err("valore troncato in dry run");
    let error = error.database_error();
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Write);
    // Niente e stato scritto, niente e stato annullato.
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::None
    );
}

/// Lo SRID di un valore EWKB contro quello del campo (GEO-008), come nel
/// vettore `invalid-ewkb-srid-differs`: `crs` in fase `write`.
#[tokio::test]
async fn dry_run_compares_the_value_srid_like_the_write() {
    let schema = point_4326("ewkb", Some("4326"));
    // POINT(1 2) con SRID 3857.
    let other_srid = hex("0101000020110f0000000000000000f03f0000000000000040");
    let error = execute_bulk_write(
        UNSET_DSN,
        create(),
        stream(&schema, &[&other_srid], None),
        DryRun::Offline,
    )
    .await
    .expect_err("SRID del valore diverso in dry run");
    assert_eq!(error.database_error().category, ErrorCategory::Crs);
    assert_eq!(
        error.database_error().phase,
        plenora_database_core::ErrorPhase::Write
    );
}

/// Il dry run con il catalogo non ricade in offline: una variabile DSN
/// nominata ma assente o vuota e un errore di configurazione, prima
/// dell'ingresso. Fino al 25/10 diventava un dry run offline con exit 0.
#[tokio::test]
async fn catalog_dry_run_without_the_dsn_is_a_configuration_error() {
    let schema = point_4326("wkb", None);
    let truncated = hex(TRUNCATED);
    // Il valore troncato non si legge: la configurazione si rifiuta prima.
    let error = execute_bulk_write(
        UNSET_DSN,
        create(),
        stream(&schema, &[&truncated], None),
        DryRun::Catalog,
    )
    .await
    .expect_err("DSN assente");
    let error = error.database_error();
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Validate);
    let empty = require_dsn(Some(std::ffi::OsStr::new(""))).expect_err("DSN vuota");
    assert_eq!(
        empty.database_error().category,
        ErrorCategory::InvalidConfiguration
    );
    require_dsn(Some(std::ffi::OsStr::new("host=x"))).expect("DSN presente");
}

/// Offline solo se chiesto, e `--offline` senza `--dry-run` non vale.
#[test]
fn offline_is_an_explicit_flag_of_the_dry_run() {
    let parse = |args: &[&str]| parse_dry_run(&mut args.iter().map(|arg| (*arg).to_owned()));
    assert_eq!(parse(&[]).expect("nessuno"), DryRun::No);
    assert_eq!(parse(&["--dry-run"]).expect("catalogo"), DryRun::Catalog);
    assert_eq!(
        parse(&["--dry-run", "--offline"]).expect("offline"),
        DryRun::Offline
    );
    assert_eq!(
        parse(&["--offline", "--dry-run"]).expect("offline"),
        DryRun::Offline
    );
    assert!(parse(&["--offline"]).is_err());
    assert!(parse(&["--dry-run", "--altro"]).is_err());
}

/// Un valore ben formato passa; il dry run offline lo dichiara (il catalogo
/// non e letto), non lo tace.
#[tokio::test]
async fn offline_dry_run_accepts_well_formed_values_without_the_catalog() {
    let schema = point_4326("wkb", None);
    let point = hex(POINT);
    let provider = crate::pfm::postgres_provider_for_pfm().expect("provider");
    let decided = provider
        .dry_run_write(
            None,
            &create(),
            stream(&schema, &[&point, &point], None),
            &crate::pfm::pfm_budget().expect("budget"),
            &CancellationToken::new(),
        )
        .await
        .expect("valori ben formati");
    assert_eq!((decided.batches, decided.rows), (1, 2));
    assert!(decided.loss_report.is_none());
}

/// Append in una transazione con un totale dichiarato passa dalla
/// diagnostica row-scoped, che rifiuta un ingresso piu corto del dichiarato
/// e giudica i valori riga per riga.
#[tokio::test]
async fn dry_run_follows_the_row_scoped_path() {
    let schema = point_4326("wkb", None);
    let point = hex(POINT);
    let error = execute_bulk_write(
        UNSET_DSN,
        operation(),
        stream(&schema, &[&point], Some(2)),
        DryRun::Offline,
    )
    .await
    .expect_err("ingresso piu corto del dichiarato");
    assert_eq!(error.database_error().category, ErrorCategory::InvalidPlan);
    let truncated = hex(TRUNCATED);
    let error = execute_bulk_write(
        UNSET_DSN,
        operation(),
        stream(&schema, &[&point, &truncated], Some(2)),
        DryRun::Offline,
    )
    .await
    .expect_err("valore troncato sul percorso row-scoped");
    assert_eq!(error.database_error().category, ErrorCategory::DataMapping);
}

/// N6 della rilettura del 25/10: il commento d'uso diceva `--mode=append`,
/// che il parser rifiuta; la forma e `--mode append`. Il rifiuto arriva
/// prima di ogni connessione.
#[tokio::test]
async fn postgres_write_ipc_vuole_il_valore_dopo_mode() {
    let args = |mode: &[&str]| {
        ["PLENORA_DSN_ASSENTE", "public", "t", "assente.arrow"]
            .iter()
            .chain(mode)
            .map(|item| (*item).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    };
    let refused = postgres_write_ipc(&mut args(&["--mode=append"]))
        .await
        .expect_err("forma con =");
    assert_eq!(
        refused.database_error().message,
        "opzione postgres-write-ipc sconosciuta"
    );
    let accepted = postgres_write_ipc(&mut args(&["--mode", "append"]))
        .await
        .expect_err("ingresso assente");
    assert_ne!(
        accepted.database_error().message,
        "opzione postgres-write-ipc sconosciuta"
    );
}
