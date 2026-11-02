//! Bind della misura: il tipo SQL viene dal valore, mai dal target descritto.
use super::*;
use odbc_api::IntoParameter;
use plenora_database_testkit::write_probe::{direct_error, direct_value, DirectObservation};

fn bind(value: ParameterValue) -> crate::parameter::Bound {
    match value {
        ParameterValue::Null { .. } => Box::new(Option::<String>::None.into_parameter()),
        ParameterValue::Bool(value) => Box::new(i16::from(value)),
        ParameterValue::I32(value) => Box::new(value),
        ParameterValue::I64(value) => Box::new(value),
        ParameterValue::F64(value) => Box::new(value),
        ParameterValue::String(value)
        | ParameterValue::Decimal(value)
        | ParameterValue::Date(value)
        | ParameterValue::Timestamp(value) => Box::new(value.into_parameter()),
        _ => panic!("tipo non previsto dalla batteria Db2"),
    }
}

pub fn observe(
    config: &Db2Config,
    secret: &SecretString,
    value: ParameterValue,
    native: &str,
) -> Result<DirectObservation> {
    let (connection, timeout) = open_connection(config, secret)?;
    connection
        .set_autocommit(false)
        .map_err(|e| driver_error(&e, ErrorPhase::Write))?;
    // Il CAST e eseguito dal server; il driver riceve il tipo sorgente del bind.
    let sql = format!(
        "INSERT INTO DB2INST1.{} (ID, V0) VALUES (0, CAST(? AS {native}))",
        plenora_database_testkit::write_probe::PROBE_TABLE,
    );
    let written = (|| {
        let mut statement = connection
            .prepare(&sql)
            .map_err(|e| driver_error(&e, ErrorPhase::Prepare))?;
        statement
            .set_query_timeout_sec(timeout)
            .map_err(|e| driver_error(&e, ErrorPhase::Write))?;
        let parameters = [bind(value)];
        if let Err(error) = statement.execute_strict(parameters.as_slice()) {
            // Un avviso di conversione puo seguire una scrittura riuscita:
            // la sonda deve rileggere la riga, non chiamarlo rifiuto.
            if !matches!(&error, odbc_api::Error::Diagnostics { record, .. }
                if record.state.as_str().starts_with("01"))
            {
                return Err(driver_error(&error, ErrorPhase::Write));
            }
        }
        Ok::<_, DatabaseError>(())
    })();
    let (observed, reading) = match written {
        Err(error) => (Err(error), false),
        Ok(()) => (read_value(&connection, timeout), true),
    };
    connection
        .rollback()
        .map_err(|e| driver_error(&e, ErrorPhase::Rollback))?;
    Ok(match observed {
        Err(error) => direct_error(&error, reading),
        Ok(value) => DirectObservation::Read(direct_value(&value)),
    })
}

// La lettura mantiene i float binari e tratta il CLOB come testo, senza
// passare dall'elenco dei tipi ammessi dall'API transazionale.
fn read_value(connection: &Connection<'_>, timeout: usize) -> Result<ParameterValue> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT V0 FROM DB2INST1.{} WHERE ID = 0",
            plenora_database_testkit::write_probe::PROBE_TABLE,
        ))
        .map_err(|e| driver_error(&e, ErrorPhase::Read))?;
    statement
        .set_query_timeout_sec(timeout)
        .map_err(|e| driver_error(&e, ErrorPhase::Read))?;
    let mut cursor = statement
        .execute_strict(())
        .map_err(|e| driver_error(&e, ErrorPhase::Read))?
        .expect("cursore della sonda");
    let kind = cursor
        .col_data_type_strict(1)
        .map_err(|e| driver_error(&e, ErrorPhase::Read))?;
    let descriptor = match floating(kind) {
        Some(Floating::Real) => BufferDesc::F32 { nullable: true },
        Some(Floating::Double) => BufferDesc::F64 { nullable: true },
        None => BufferDesc::Text {
            max_str_len: QUERY_CELL_BYTES,
        },
    };
    let buffer = ColumnarDynBuffer::try_from_descs(1, [descriptor])
        .map_err(|e| driver_error(&e, ErrorPhase::Read))?;
    let mut rows = cursor
        .bind_buffer(buffer)
        .map_err(|e| driver_error(&e, ErrorPhase::Read))?;
    let batch = crate::exact_read::fetch(&mut rows, ErrorPhase::Read)?.expect("riga della sonda");
    let kind = match kind {
        DataType::Other {
            data_type: SqlDataType(-99),
            ..
        } => DataType::Varchar { length: None },
        kind => kind,
    };
    let column = batch.column(0);
    let value = match floating(kind) {
        Some(Floating::Real) => column
            .as_nullable_slice::<f32>()
            .expect("buffer REAL")
            .get(0)
            .map(|v| f64::from(*v)),
        Some(Floating::Double) => column
            .as_nullable_slice::<f64>()
            .expect("buffer DOUBLE")
            .get(0)
            .copied(),
        None => return decode_cell(column, 0, kind),
    };
    Ok(value.map_or_else(
        || ParameterValue::Null {
            type_name: "float".to_owned(),
        },
        ParameterValue::F64,
    ))
}

#[test]
fn measurement_binds_values_refused_by_application_admission() {
    for value in [
        ParameterValue::F64(-0.0),
        ParameterValue::F64(f64::MIN_POSITIVE / 2.0),
    ] {
        assert!(crate::parameter::validate(std::slice::from_ref(&value)).is_err());
        let _ = bind(value);
    }
}
