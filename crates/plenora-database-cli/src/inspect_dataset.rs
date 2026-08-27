use crate::CliResult;
use arrow_ipc::reader::FileReader;
use plenora_database_core::arrow::array::{Array, BinaryArray, LargeBinaryArray, RecordBatch};
use plenora_database_core::arrow::schema::{DataType, SchemaRef};
use plenora_database_core::ewkb::{inspect_ewkb_detailed, EwkbInspection};
use plenora_database_core::field_contract::{validate_schema_contract, FieldContract};
use plenora_database_core::protocol;
use serde_json::{json, Map, Value};
use std::fs::{self, File};
use std::path::Path;

const MAX_IPC_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_BATCHES: u64 = 65_536;
const MAX_ROWS: u64 = 1_000_000;
const MAX_COLUMNS: usize = 65_536;
const MAX_BATCH_MEMORY_BYTES: usize = 512 * 1024 * 1024;
const MAX_WKB_CELL_BYTES: usize = 64 * 1024 * 1024;
const MAX_GEOMETRY_CELLS: u64 = 1_000_000;
const MAX_GEOMETRY_COMPONENTS: u64 = 16_777_216;
const MAX_GEOMETRY_DEPTH: u64 = 64;

pub fn inspect(path: impl AsRef<Path>) -> CliResult<Value> {
    let path = path.as_ref();
    let file_size = fs::metadata(path)
        .map_err(|_| "dataset Arrow IPC non leggibile".to_owned())?
        .len();
    if file_size == 0 || file_size > MAX_IPC_FILE_BYTES {
        return Err("dataset Arrow IPC vuoto o oltre il limite di 512 MiB".into());
    }
    let file = File::open(path).map_err(|_| "dataset Arrow IPC non apribile".to_owned())?;
    let reader = FileReader::try_new(file, None)
        .map_err(|_| "file Arrow IPC non valido o non supportato".to_owned())?;
    let schema = reader.schema();
    if schema.fields().len() > MAX_COLUMNS {
        return Err("schema Arrow IPC oltre il limite di colonne".into());
    }
    validate_schema_contract(&schema)?;

    let spatial_fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| FieldContract::parse(field).map(|contract| (index, contract)))
        .collect::<plenora_database_core::Result<Vec<_>>>()?;

    let mut field_reports = schema
        .fields()
        .iter()
        .map(|field| {
            let contract = FieldContract::parse(field)?;
            let mut report = Map::from_iter([
                ("name".to_owned(), json!(field.name())),
                ("data_type".to_owned(), json!(field.data_type().to_string())),
                ("nullable".to_owned(), json!(field.is_nullable())),
                ("field_id".to_owned(), json!(contract.field_id)),
                ("metadata".to_owned(), json!(field.metadata())),
            ]);
            if contract.spatial {
                report.insert("geometry_cells".to_owned(), Value::Array(Vec::new()));
            }
            Ok(report)
        })
        .collect::<Result<Vec<_>, crate::CliError>>()?;

    let mut batches = 0_u64;
    let mut rows = 0_u64;
    let mut geometry_cells = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|_| "RecordBatch Arrow IPC non decodificabile".to_owned())?;
        batches = batches
            .checked_add(1)
            .ok_or_else(|| "overflow nel conteggio batch".to_owned())?;
        if batches > MAX_BATCHES {
            return Err("dataset Arrow IPC oltre il limite di batch".into());
        }
        if batch.get_array_memory_size() > MAX_BATCH_MEMORY_BYTES {
            return Err("RecordBatch Arrow IPC oltre il limite di memoria".into());
        }
        let batch_rows = u64::try_from(batch.num_rows())
            .map_err(|_| "conteggio righe Arrow oltre u64".to_owned())?;
        rows = rows
            .checked_add(batch_rows)
            .ok_or_else(|| "overflow nel conteggio righe".to_owned())?;
        if rows > MAX_ROWS {
            return Err("dataset Arrow IPC oltre il limite di righe ispezionabili".into());
        }
        inspect_batch(
            &schema,
            &batch,
            batches - 1,
            &spatial_fields,
            &mut field_reports,
            &mut geometry_cells,
        )?;
    }

    Ok(json!({
        "schema_version": 1,
        "status": "ok",
        "contract_version": schema.metadata().get(protocol::CONTRACT_VERSION_KEY),
        "schema_metadata": schema.metadata(),
        "batches": batches,
        "rows": rows,
        "fields": field_reports
    }))
}

fn inspect_batch(
    schema: &SchemaRef,
    batch: &RecordBatch,
    batch_index: u64,
    contracts: &[(usize, FieldContract<'_>)],
    field_reports: &mut [Map<String, Value>],
    geometry_cells: &mut u64,
) -> Result<(), String> {
    if batch.schema() != *schema {
        return Err("schema del RecordBatch divergente dallo schema IPC".to_owned());
    }
    for (field_index, contract) in contracts.iter().filter(|(_, contract)| contract.spatial) {
        reserve_geometry_cells(geometry_cells, batch.num_rows())?;
        let array = batch
            .columns()
            .get(*field_index)
            .ok_or_else(|| "colonna geometrica assente dal RecordBatch".to_owned())?;
        let cells = field_reports
            .get_mut(*field_index)
            .and_then(|report| report.get_mut("geometry_cells"))
            .and_then(Value::as_array_mut)
            .ok_or_else(|| "report geometrico interno incoerente".to_owned())?;
        match contract.field.data_type() {
            DataType::Binary => {
                let values = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "array geometry Binary incoerente".to_owned())?;
                inspect_binary_values(values, batch_index, contract, cells)?;
            }
            DataType::LargeBinary => {
                let values = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| "array geometry LargeBinary incoerente".to_owned())?;
                inspect_large_binary_values(values, batch_index, contract, cells)?;
            }
            _ => return Err("tipo Arrow geometrico non binario".to_owned()),
        }
    }
    Ok(())
}

fn reserve_geometry_cells(total: &mut u64, rows: usize) -> Result<(), String> {
    *total = total
        .checked_add(
            u64::try_from(rows).map_err(|_| "conteggio celle geometry oltre u64".to_owned())?,
        )
        .ok_or_else(|| "overflow nel conteggio celle geometry".to_owned())?;
    if *total > MAX_GEOMETRY_CELLS {
        Err("dataset Arrow IPC oltre il limite di celle geometry".to_owned())
    } else {
        Ok(())
    }
}

fn inspect_binary_values(
    values: &BinaryArray,
    batch_index: u64,
    contract: &FieldContract<'_>,
    cells: &mut Vec<Value>,
) -> Result<(), String> {
    for row in 0..values.len() {
        if values.is_null(row) {
            cells.push(json!({"batch": batch_index, "row": row, "status": "null"}));
        } else {
            cells.push(inspect_cell(values.value(row), batch_index, row, contract)?);
        }
    }
    Ok(())
}

fn inspect_large_binary_values(
    values: &LargeBinaryArray,
    batch_index: u64,
    contract: &FieldContract<'_>,
    cells: &mut Vec<Value>,
) -> Result<(), String> {
    for row in 0..values.len() {
        if values.is_null(row) {
            cells.push(json!({"batch": batch_index, "row": row, "status": "null"}));
        } else {
            cells.push(inspect_cell(values.value(row), batch_index, row, contract)?);
        }
    }
    Ok(())
}

fn inspect_cell(
    bytes: &[u8],
    batch: u64,
    row: usize,
    contract: &FieldContract<'_>,
) -> Result<Value, String> {
    if bytes.len() > MAX_WKB_CELL_BYTES {
        return Err(format!(
            "cella geometry batch {batch} riga {row} oltre 64 MiB"
        ));
    }
    let inspection = inspect_ewkb_detailed(bytes, MAX_GEOMETRY_COMPONENTS, MAX_GEOMETRY_DEPTH)
        .map_err(|_| format!("geometry non decodificabile: batch {batch}, riga {row}"))?;
    validate_observed_contract(contract, &inspection, batch, row)?;
    Ok(json!({
        "batch": batch,
        "row": row,
        "status": "ok",
        "bytes": bytes.len(),
        "components": inspection.stats.components,
        "max_depth": inspection.stats.max_depth,
        "geometry_type": inspection.root.geometry_type_name(),
        "dimensions": inspection.root.dimensions_label(),
        "embedded_srid": inspection.root.srid
    }))
}

fn validate_observed_contract(
    contract: &FieldContract<'_>,
    inspection: &EwkbInspection,
    batch: u64,
    row: usize,
) -> Result<(), String> {
    if contract
        .dimensions
        .is_some_and(|value| value != "unknown" && value != inspection.root.dimensions_label())
    {
        return Err(format!(
            "geometry batch {batch} riga {row}: dimensioni del payload divergenti dai metadati"
        ));
    }
    if let Some(declared) = contract.geometry_types {
        let observed = inspection
            .root
            .geometry_type_name()
            .ok_or_else(|| format!("geometry batch {batch} riga {row}: tipo non canonico"))?;
        if !declared
            .split(',')
            .any(|candidate| candidate.eq_ignore_ascii_case(observed))
        {
            return Err(format!(
                "geometry batch {batch} riga {row}: tipo del payload divergente dai metadati"
            ));
        }
    }
    if contract.encoding == Some("ewkb") {
        if let Some(embedded) = inspection.root.srid {
            if contract.srid != Some(embedded) {
                return Err(format!(
                    "geometry batch {batch} riga {row}: SRID EWKB divergente dai metadati"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "inspect_dataset_tests.rs"]
mod tests;
