//! Facade OLTP: helper ergonomici sopra `TransactionScope`.
//!
//! I metodi qui sono funzioni libere e non espongono tipi del driver: leggono
//! righe via `TransactionScope::query` (canonicamente `Vec<ParameterValue>`)
//! e le convertono nel tipo scalare richiesto. Restano provider-neutral.

use crate::portable::{compile_portable, PortableStatement};
use crate::provider::ParameterValue;
use crate::row::Row;
use crate::transaction::{Statement, TransactionScope};
use crate::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};

fn expect_single_row(rows: Vec<Row>) -> Result<Row> {
    if rows.is_empty() {
        return Err(not_found("query attesa a 1 riga, ricevute 0"));
    }
    if rows.len() > 1 {
        return Err(too_many_rows(rows.len()));
    }
    Ok(rows.into_iter().next().expect("controllato lunghezza"))
}

fn expect_at_most_one_row(rows: Vec<Row>) -> Result<Option<Row>> {
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() > 1 {
        return Err(too_many_rows(rows.len()));
    }
    Ok(Some(
        rows.into_iter().next().expect("controllato lunghezza"),
    ))
}

fn expect_single_column(row: Row) -> Result<ParameterValue> {
    if row.len() != 1 {
        return Err(malformed(&format!(
            "scalar attesa a 1 colonna, ricevute {}",
            row.len()
        )));
    }
    Ok(row
        .into_values()
        .into_iter()
        .next()
        .expect("controllato lunghezza"))
}

/// Estrae lo scalare da un result set: al piu **una** riga, esattamente
/// **una** colonna. `None` quando la query non ha restituito righe.
///
/// E' la stessa cardinalita che i costruttori tipizzati di questo modulo
/// applicano da sempre, resa pubblica perche le altre superfici scalar —
/// binding Python e CLI — la applichino invece di reimplementarla. Prendevano
/// la prima riga e la prima colonna e buttavano via il resto: una query che
/// restituiva due righe, o due colonne, dava un risultato plausibile e
/// arbitrario invece di un errore.
///
/// # Errors
///
/// `Conflict` se le righe sono piu di una, `DataMapping` se le colonne non
/// sono esattamente una.
pub fn scalar_opt(rows: Vec<Row>) -> Result<Option<ParameterValue>> {
    expect_at_most_one_row(rows)?.map_or(Ok(None), |row| expect_single_column(row).map(Some))
}

fn not_found(message: &str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::NotFound,
        phase: ErrorPhase::Read,
        remote_effect: crate::RemoteEffect::None,
        retry: crate::RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

fn too_many_rows(n: usize) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::Conflict,
        phase: ErrorPhase::Read,
        remote_effect: crate::RemoteEffect::None,
        retry: crate::RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: format!("query attesa a 1 riga, ricevute {n}"),
        diagnostics: None,
    }
}

fn malformed(message: &str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Read,
        remote_effect: crate::RemoteEffect::None,
        retry: crate::RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

fn scalar_type_mismatch(expected: &str, got: &ParameterValue) -> DatabaseError {
    let got_name = match got {
        ParameterValue::Bool(_) => "bool",
        ParameterValue::I32(_) => "i32",
        ParameterValue::I64(_) => "i64",
        ParameterValue::F64(_) => "f64",
        ParameterValue::String(_) => "string",
        ParameterValue::Bytes(_) => "bytes",
        ParameterValue::Date(_) => "date",
        ParameterValue::Timestamp(_) => "timestamp",
        ParameterValue::TimestampTz(_) => "timestamptz",
        ParameterValue::Decimal(_) => "decimal",
        ParameterValue::Uuid(_) => "uuid",
        ParameterValue::Json(_) => "json",
        ParameterValue::Wkb { .. } => "wkb",
        ParameterValue::Enum { .. } => "enum",
        ParameterValue::Null { .. } => "null",
    };
    malformed(&format!(
        "scalar attesa di tipo {expected}, ricevuto {got_name}"
    ))
}

/// Esegue una query che deve ritornare esattamente una riga.
///
/// # Errors
///
/// - `NotFound` se 0 righe
/// - `Conflict` se >1 righe
/// - errori tecnici propagati dal driver
pub async fn query_one(
    tx: &mut dyn TransactionScope,
    statement: &Statement,
    cancellation: &CancellationToken,
) -> Result<Row> {
    let rows = tx.query(statement, cancellation).await?;
    expect_single_row(rows)
}

/// Esegue una query che può ritornare zero o una riga.
///
/// # Errors
///
/// - `Conflict` se ritorna piu di una riga
/// - errori tecnici propagati dal driver
pub async fn query_optional(
    tx: &mut dyn TransactionScope,
    statement: &Statement,
    cancellation: &CancellationToken,
) -> Result<Option<Row>> {
    let rows = tx.query(statement, cancellation).await?;
    expect_at_most_one_row(rows)
}

macro_rules! scalar_getter {
    ($func:ident, $ty:ty, $expected:literal, $pattern:pat => $extract:expr) => {
        #[doc = concat!("Legge una singola cella scalare `", $expected, "` da una query 1x1.")]
        ///
        /// # Errors
        ///
        /// - `NotFound` se 0 righe, `Conflict` se >1 righe o >1 colonne
        /// - `DataMapping` se il tipo della cella non corrisponde
        /// - errori tecnici propagati dal driver
        pub async fn $func(
            tx: &mut dyn TransactionScope,
            statement: &Statement,
            cancellation: &CancellationToken,
        ) -> Result<$ty> {
            let row = query_one(tx, statement, cancellation).await?;
            let value = expect_single_column(row)?;
            match value {
                $pattern => Ok($extract),
                other => Err(scalar_type_mismatch($expected, &other)),
            }
        }
    };
}

// ============================================================================
//  Facade portable: compila l'AST + esegue in un colpo solo.
// ============================================================================

/// Se lo statement rende righe, e non un conteggio.
///
/// Pubblica perche la domanda la deve porre anche chi **sceglie** quale delle
/// due meta della facade chiamare — il CLI, per esempio, che riceve un AST da
/// un file e non sa cosa contenga. La risposta e una proprieta dell'AST, e
/// averne due copie sarebbe stato il difetto solito: quella del chiamante
/// smetterebbe di seguire questa senza che nessuno se ne accorga, e la
/// conseguenza sarebbe un `execute_portable` chiamato su uno statement che
/// rende righe — rifiutato con un `InvalidPlan` che sembrerebbe colpa del
/// piano.
///
/// Una `SELECT` rende righe sempre, anche senza `returning`: e cio che una
/// SELECT e.
#[must_use]
pub const fn statement_returns_rows(stmt: &PortableStatement) -> bool {
    match stmt {
        PortableStatement::Select(_) => true, // SELECT ritorna sempre righe
        PortableStatement::Insert(s) => !s.returning.is_empty(),
        PortableStatement::Update(s) => !s.returning.is_empty(),
        PortableStatement::Delete(s) => !s.returning.is_empty(),
        PortableStatement::Upsert(s) => !s.returning.is_empty(),
    }
}

/// Compila ed esegue un `PortableStatement` come DML (INSERT/UPDATE/DELETE/
/// UPSERT senza RETURNING). Ritorna il numero di righe modificate.
///
/// Per statement con RETURNING o SELECT usa `execute_portable_returning`.
///
/// # Errors
///
/// - `InvalidPlan` se l'AST ha vincoli violati o se contiene RETURNING
///   (usa `execute_portable_returning`)
/// - errori tecnici propagati dal driver
pub async fn execute_portable(
    tx: &mut dyn TransactionScope,
    statement: &PortableStatement,
    cancellation: &CancellationToken,
) -> Result<u64> {
    if matches!(statement, PortableStatement::Select(_)) {
        return Err(DatabaseError::invalid_plan(
            "execute_portable non accetta SELECT: usa execute_portable_returning",
        ));
    }
    if statement_returns_rows(statement) {
        return Err(DatabaseError::invalid_plan(
            "execute_portable non accetta statement con RETURNING: usa execute_portable_returning",
        ));
    }
    let compiled = compile_portable(tx.provider_kind(), statement)?;
    tx.execute(&compiled, cancellation).await
}

/// Compila ed esegue un `PortableStatement` che restituisce righe:
/// SELECT o INSERT/UPDATE/DELETE/UPSERT con RETURNING.
///
/// # Errors
///
/// - `InvalidPlan` se lo statement non ha RETURNING (o non è SELECT)
/// - errori tecnici propagati dal driver
///
/// # Note su portabilità
///
/// `PostgreSQL` produce `RETURNING`; `SQL Server` produce `OUTPUT` per insert,
/// update e delete. `MySQL` non qualifica nessuna forma. `MariaDB` qualifica
/// insert, delete e upsert, ma non update. La matrice autorevole e
/// `compile_returning`.
pub async fn execute_portable_returning(
    tx: &mut dyn TransactionScope,
    statement: &PortableStatement,
    cancellation: &CancellationToken,
) -> Result<Vec<Row>> {
    if !statement_returns_rows(statement) {
        return Err(DatabaseError::invalid_plan(
            "execute_portable_returning richiede RETURNING (o SELECT)",
        ));
    }
    let compiled = compile_portable(tx.provider_kind(), statement)?;
    tx.query(&compiled, cancellation).await
}

/// Come `execute_portable_returning` ma esige esattamente una riga.
///
/// # Errors
///
/// - `NotFound` se 0 righe
/// - `Conflict` se >1 righe
/// - `InvalidPlan` se lo statement non ha RETURNING
pub async fn execute_portable_returning_one(
    tx: &mut dyn TransactionScope,
    statement: &PortableStatement,
    cancellation: &CancellationToken,
) -> Result<Row> {
    let rows = execute_portable_returning(tx, statement, cancellation).await?;
    expect_single_row(rows)
}

// ============================================================================
//  Facade scalar (tipizzata per valore scalare 1x1)
// ============================================================================

scalar_getter!(execute_scalar_bool, bool, "bool", ParameterValue::Bool(v) => v);
scalar_getter!(execute_scalar_i32, i32, "i32", ParameterValue::I32(v) => v);
scalar_getter!(execute_scalar_i64, i64, "i64", ParameterValue::I64(v) => v);
scalar_getter!(execute_scalar_f64, f64, "f64", ParameterValue::F64(v) => v);
scalar_getter!(execute_scalar_string, String, "string", ParameterValue::String(v) => v);
scalar_getter!(execute_scalar_bytes, Vec<u8>, "bytes", ParameterValue::Bytes(v) => v);
scalar_getter!(execute_scalar_uuid, String, "uuid", ParameterValue::Uuid(v) => v);
scalar_getter!(execute_scalar_json, serde_json::Value, "json", ParameterValue::Json(v) => v);
scalar_getter!(execute_scalar_date, String, "date", ParameterValue::Date(v) => v);
scalar_getter!(execute_scalar_timestamp, String, "timestamp", ParameterValue::Timestamp(v) => v);
scalar_getter!(execute_scalar_timestamptz, String, "timestamptz", ParameterValue::TimestampTz(v) => v);

/// Legge una cella `ENUM` — ritorna `(type_name, label)`.
///
/// # Errors
///
/// - `NotFound` / `Conflict` sui vincoli di shape della facade
/// - `DataMapping` se la cella non è un enum (es. text puro)
pub async fn execute_scalar_enum(
    tx: &mut dyn TransactionScope,
    statement: &Statement,
    cancellation: &CancellationToken,
) -> Result<(String, String)> {
    let row = query_one(tx, statement, cancellation).await?;
    let value = expect_single_column(row)?;
    match value {
        ParameterValue::Enum { type_name, label } => Ok((type_name, label)),
        other => Err(scalar_type_mismatch("enum", &other)),
    }
}

/// Legge un valore `Decimal` canonico (rappresentazione stringa).
///
/// Il valore resta una stringa canonica per non introdurre arrotondamenti. Il
/// data plane Arrow rappresenta invece i decimali come `Decimal128` o
/// `Decimal256`.
///
/// # Errors
///
/// `DataMapping` se cardinalità o tipo non corrispondono; propaga inoltre gli
/// errori tecnici della query.
pub async fn execute_scalar_decimal(
    tx: &mut dyn TransactionScope,
    statement: &Statement,
    cancellation: &CancellationToken,
) -> Result<String> {
    let row = query_one(tx, statement, cancellation).await?;
    let value = expect_single_column(row)?;
    match value {
        ParameterValue::Decimal(v) => Ok(v),
        other => Err(scalar_type_mismatch("decimal", &other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expect_single_row_rejects_empty() {
        let err = expect_single_row(vec![]).unwrap_err();
        assert_eq!(err.category, ErrorCategory::NotFound);
    }

    fn row_i32(name: &str, v: i32) -> Row {
        Row::try_new(
            std::sync::Arc::from(vec![name.to_owned()]),
            vec![ParameterValue::I32(v)],
        )
        .expect("fixture coerente")
    }

    #[test]
    fn expect_single_row_rejects_multiple() {
        let err = expect_single_row(vec![row_i32("id", 1), row_i32("id", 2)]).unwrap_err();
        assert_eq!(err.category, ErrorCategory::Conflict);
    }

    #[test]
    fn expect_at_most_one_row_returns_none_for_empty() {
        let out = expect_at_most_one_row(vec![]).expect("ok");
        assert!(out.is_none());
    }

    #[test]
    fn expect_single_column_rejects_wrong_width() {
        let row = Row::try_new(
            std::sync::Arc::from(vec!["a".to_owned(), "b".to_owned()]),
            vec![ParameterValue::I32(1), ParameterValue::I32(2)],
        )
        .expect("fixture coerente");
        let err = expect_single_column(row).unwrap_err();
        assert_eq!(err.category, ErrorCategory::DataMapping);
    }

    #[test]
    fn scalar_type_mismatch_reports_actual_type() {
        let err = scalar_type_mismatch("i64", &ParameterValue::String("nope".into()));
        assert!(err.message.contains("i64"));
        assert!(err.message.contains("string"));
    }
}
