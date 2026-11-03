//! Conferma NUL per superficie: non accetta rifiuti generici come fedelta.
use crate::parameter_fidelity::{
    begin, describe, insert_statement, report, run, Case, Expect, NativeSql, Target,
};
use plenora_database_core::arrow::array::{Int32Array, StringArray};
use plenora_database_core::arrow::schema::{DataType, Field, Schema, SchemaRef};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::facade::execute_portable;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteStatus;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, ProviderKind as P, ReadOperation, TransactionProfile, WriteMode,
    WriteOperation,
};
use plenora_database_core::portable::{
    eq, Expression, PortableStatement, TableRef, UpdateStatement,
};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue as V, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::Statement;
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase};
use std::fmt::Write as _;
use std::sync::Arc;

struct Input(Option<RecordBatch>, SchemaRef);
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.1.clone()
    }
    fn declared_input_rows(&self) -> Option<u64> {
        Some(1)
    }
    fn next_batch<'a>(
        &'a mut self,
        cancel: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(plenora_database_core::DatabaseError::cancelled(
                    None,
                    ErrorPhase::Write,
                    "sonda cancellata",
                ));
            }
            Ok(self.0.take())
        })
    }
}

fn types(kind: P) -> Vec<&'static str> {
    match kind {
        P::Sqlite | P::Duckdb => vec![],
        P::Postgres => vec!["VARCHAR(64)", "CHAR(3)", "TEXT"],
        P::Mysql | P::Mariadb => vec![
            "VARCHAR(64)",
            "CHAR(3)",
            "TINYTEXT",
            "TEXT",
            "MEDIUMTEXT",
            "LONGTEXT",
        ],
        P::Sqlserver => vec![
            "VARCHAR(64)",
            "CHAR(3)",
            "NVARCHAR(64)",
            "NCHAR(3)",
            "VARCHAR(MAX)",
            "NVARCHAR(MAX)",
        ],
        P::Oracle => vec![
            "VARCHAR2(64 CHAR)",
            "CHAR(3 CHAR)",
            "NVARCHAR2(64)",
            "NCHAR(3)",
            "CLOB",
            "NCLOB",
        ],
        P::Db2 => vec![
            "VARCHAR(64 OCTETS)",
            "CHAR(3 OCTETS)",
            "CLOB(64 OCTETS)",
            "VARCHAR(64 CODEUNITS32)",
            "CHAR(3 CODEUNITS32)",
            "CLOB(64 CODEUNITS32)",
        ],
    }
}

fn raw(kind: P, declaration: &str, col: &str) -> String {
    match kind {
        P::Sqlite | P::Duckdb => String::new(),
        P::Postgres => format!("upper(encode(convert_to({col}::text, 'UTF8'), 'hex'))"),
        P::Mysql | P::Mariadb => format!("HEX({col})"),
        P::Db2 => format!("VARCHAR(HEX({col}), 256)"),
        P::Sqlserver => format!("CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), {col}), 2)"),
        P::Oracle => {
            let value = if declaration.contains("CLOB") {
                format!("DBMS_LOB.SUBSTR({col}, 64, 1)")
            } else {
                col.into()
            };
            format!("RAWTOHEX(UTL_I18N.STRING_TO_RAW({value}, 'AL32UTF8'))")
        }
    }
}

fn hex(kind: P, declaration: &str, text: &str) -> String {
    let bytes = if kind == P::Sqlserver && declaration.starts_with('N') {
        text.encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>()
    } else {
        text.as_bytes().to_vec()
    };
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02X}");
        out
    })
}

/// Esegue una matrice indipendente dalle attese della vecchia suite A900.
///
/// CHAR riceve tre caratteri: il padding non puo mascherare il trasporto NUL.
/// CLOB/NCLOB Oracle sono operandi non qualificati nel portable/read:
/// si verifica lo stesso rifiuto con controllo e NUL, distinto dal bind nativo.
/// NCLOB Oracle e CHAR MySQL/MariaDB restano chiusi anche nel writer.
///
/// # Errors
/// Restituisce tutte le discrepanze, senza fermarsi al primo tipo o superficie.
#[allow(clippy::too_many_lines)]
pub async fn check(
    provider: &dyn Provider,
    secret: &SecretString,
    schema: &str,
    only: Option<usize>,
) -> Result<(), String> {
    let kind = provider.kind();
    let cancel = CancellationToken::new();
    let mut failures = Vec::new();
    let declarations = types(kind);
    if declarations.is_empty() || only.is_some_and(|index| index >= declarations.len()) {
        return Err("matrice NUL senza tipi da misurare".into());
    }
    for (index, declaration) in declarations.into_iter().enumerate() {
        if only.is_some_and(|selected| selected != index) {
            continue;
        }
        let name = format!("A900_NUL_{index}");
        let quote = if matches!(kind, P::Mysql | P::Mariadb) {
            '`'
        } else {
            '"'
        };
        let table = format!("{quote}{schema}{quote}.{quote}{name}{quote}");
        let col = format!("{quote}V{quote}");
        let id = format!("{quote}ID{quote}");
        let _ = provider
            .execute_ddl(secret, &format!("DROP TABLE {table}"), &cancel)
            .await;
        provider
            .execute_ddl(
                secret,
                &format!(
                    "CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {col} {declaration})"
                ),
                &cancel,
            )
            .await
            .map_err(|e| describe(&e))?;
        let (p1, p2) = match kind {
            P::Postgres => ("$1", "$2"),
            P::Sqlserver => ("@P1", "@P2"),
            P::Oracle => (":1", ":2"),
            _ => ("?", "?"),
        };
        let native = |operation, _: &str| match operation {
            NativeSql::Insert => format!("INSERT INTO {table} ({id}, {col}) VALUES ({p1}, {p2})"),
            NativeSql::Select => format!("SELECT {col} FROM {table} WHERE {id} = {p1}"),
            NativeSql::Count => {
                format!("SELECT COUNT(*) FROM {table} WHERE {id} = {p1} AND {col} = {p2}")
            }
            NativeSql::Raw => format!(
                "SELECT {} FROM {table} WHERE {id} = {p1}",
                raw(kind, declaration, &col)
            ),
            NativeSql::Delete => format!("DELETE FROM {table} WHERE {id} = {p1}"),
        };
        let target = Target {
            provider,
            secret,
            limits: ResourceLimits::default(),
            table: ObjectRef {
                catalog: None,
                schema: Some(schema.into()),
                object: name,
            },
            id_column: "ID",
            native: &native,
            native_cross_type_declared: false,
        };
        let fixed = declaration.starts_with("CHAR(") || declaration.starts_with("NCHAR(");
        let lob = kind == P::Oracle && declaration.contains("CLOB");
        let values = if fixed {
            vec!["abc", "a\0b", "\0ab", "ab\0", "\0\0\0"]
        } else {
            vec!["abc", "a\0b", "\0ab", "ab\0", "\0"]
        };
        let cases = values
            .iter()
            .map(|text| {
                let value = V::String((*text).into());
                let expected = if kind == P::Postgres && text.contains('\0') {
                    Expect::Refused(ErrorCategory::DataMapping)
                } else if lob {
                    Expect::Assigned {
                        read: value.clone(),
                        raw: hex(kind, declaration, text),
                    }
                } else {
                    Expect::exact(value.clone(), hex(kind, declaration, text))
                };
                let case = Case::new("nul/control", "V", value, expected);
                if lob {
                    case.only(&[crate::parameter_fidelity::FidelityPath::Native])
                } else {
                    case
                }
            })
            .collect::<Vec<_>>();
        let (summary, errors) = report(&run(&target, &cases).await);
        println!("NUL {kind:?} {declaration}:\n{summary}");
        failures.extend(errors.into_iter().map(|e| format!("{declaration}: {e}")));
        for (case_index, text) in values.into_iter().enumerate() {
            if let Err(error) = update(&target, text, lob).await {
                failures.push(format!(
                    "{declaration} caso {case_index} portable INSERT/UPDATE: {error}"
                ));
            }
            if lob {
                if let Err(error) = closed_read(&target, text).await {
                    failures.push(format!(
                        "{declaration} caso {case_index} filtro read: {error}"
                    ));
                }
            }
            // Le chiusure del tipo valgono anche per il controllo senza NUL.
            let closed = (fixed && matches!(kind, P::Mysql | P::Mariadb))
                || (kind == P::Oracle && declaration == "NCLOB");
            if let Err(error) = bulk(&target, text, closed).await {
                failures.push(format!("{declaration} caso {case_index} Arrow: {error}"));
            }
        }
        provider
            .execute_ddl(secret, &format!("DROP TABLE {table}"), &cancel)
            .await
            .map_err(|e| describe(&e))?;
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

async fn update(target: &Target<'_>, text: &str, closed: bool) -> Result<(), String> {
    let cancel = CancellationToken::new();
    let mut tx = begin(target).await?;
    tx.execute(
        &Statement::new((target.native)(NativeSql::Insert, "V"))
            .with_params(vec![V::I32(900), V::String("abc".into())]),
        &cancel,
    )
    .await
    .map_err(|e| describe(&e))?;
    let statement = PortableStatement::Update(UpdateStatement {
        table: TableRef {
            schema: target.table.schema.clone(),
            name: target.table.object.clone(),
        },
        assignments: vec![("V".into(), Expression::literal(V::String(text.into())))],
        filter: Some(eq("ID", V::I32(900))),
        returning: vec![],
    });
    let result = execute_portable(tx.as_mut(), &statement, &cancel).await;
    let refused = closed || (target.provider.kind() == P::Postgres && text.contains('\0'));
    if refused {
        match result {
            Err(e)
                if e.category == ErrorCategory::DataMapping
                    && e.phase == ErrorPhase::Prepare
                    && e.remote_effect == plenora_database_core::RemoteEffect::None => {}
            _ => return Err("rifiuto NUL pre-invio assente".into()),
        }
    } else {
        result.map_err(|e| describe(&e))?;
    }
    let rows = tx
        .query(
            &Statement::new((target.native)(NativeSql::Select, "V")).with_params(vec![V::I32(900)]),
            &cancel,
        )
        .await
        .map_err(|e| describe(&e))?;
    let expected = if refused { "abc" } else { text };
    if rows.len() != 1 || rows[0].values() != vec![V::String(expected.into())] {
        return Err("valore UPDATE non conservato".into());
    }
    let inserted = execute_portable(
        tx.as_mut(),
        &insert_statement(target, "V", 902, &V::String(text.into())),
        &cancel,
    )
    .await;
    if refused {
        match inserted {
            Err(e)
                if e.category == ErrorCategory::DataMapping
                    && e.phase == ErrorPhase::Prepare
                    && e.remote_effect == plenora_database_core::RemoteEffect::None => {}
            _ => return Err("INSERT NUL senza rifiuto pre-invio".into()),
        }
    } else {
        inserted.map_err(|e| describe(&e))?;
        let rows = tx
            .query(
                &Statement::new((target.native)(NativeSql::Select, "V"))
                    .with_params(vec![V::I32(902)]),
                &cancel,
            )
            .await
            .map_err(|e| describe(&e))?;
        if rows.len() != 1 || rows[0].values() != vec![V::String(text.into())] {
            return Err("INSERT portable non conserva il testo".into());
        }
    }
    tx.rollback(&cancel).await.map_err(|e| describe(&e))?;
    println!("NUL portable INSERT/UPDATE: closed_type={closed}, righe verificate");
    Ok(())
}

async fn bulk(target: &Target<'_>, text: &str, closed: bool) -> Result<(), String> {
    let cancel = CancellationToken::new();
    let budget = ResourceBudget::new(target.limits.clone()).map_err(|e| describe(&e))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("V", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![901])),
            Arc::new(StringArray::from(vec![text])),
        ],
    )
    .map_err(|_| "batch NUL non valido")?;
    let operation = WriteOperation {
        target: target.table.clone(),
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec![],
        update_columns: vec![],
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let prepared = target
        .provider
        .prepare_write(target.secret, &operation, schema.clone(), &budget, &cancel)
        .await;
    if closed {
        return match prepared {
            Err(e)
                if e.category == ErrorCategory::Unsupported
                    && matches!(e.phase, ErrorPhase::Prepare | ErrorPhase::Validate) =>
            {
                println!("NUL Arrow: tipo non qualificato, stesso rifiuto con controllo e NUL");
                Ok(())
            }
            Err(e) => Err(describe(&e)),
            Ok(_) => Err("tipo writer non qualificato aperto".into()),
        };
    }
    let prepared = prepared.map_err(|e| describe(&e))?;
    let result = target
        .provider
        .write(
            target.secret,
            prepared,
            Box::new(Input(Some(batch), schema)),
            &budget,
            &cancel,
        )
        .await;
    let refused = target.provider.kind() == P::Postgres && text.contains('\0');
    if refused {
        match result {
            Err(e) if e.category == ErrorCategory::DataMapping => {}
            _ => return Err("writer non rifiuta NUL".into()),
        }
    } else {
        let outcome = result.map_err(|e| describe(&e))?;
        if outcome.status != WriteStatus::Committed {
            return Err("writer non committed".into());
        }
    }
    let mut tx = begin(target).await?;
    let rows = tx
        .query(
            &Statement::new((target.native)(NativeSql::Select, "V")).with_params(vec![V::I32(901)]),
            &cancel,
        )
        .await
        .map_err(|e| describe(&e))?;
    if refused {
        if !rows.is_empty() {
            return Err("writer rifiutato ha lasciato una riga".into());
        }
    } else if rows.len() != 1 || rows[0].values() != vec![V::String(text.into())] {
        return Err("writer non conserva il testo".into());
    }
    tx.execute(
        &Statement::new((target.native)(NativeSql::Delete, "V")).with_params(vec![V::I32(901)]),
        &cancel,
    )
    .await
    .map_err(|e| describe(&e))?;
    tx.commit(&cancel).await.map_err(|e| describe(&e))?;
    println!("NUL Arrow: esito esatto, riga verificata");
    Ok(())
}

async fn closed_read(target: &Target<'_>, text: &str) -> Result<(), String> {
    let cancel = CancellationToken::new();
    let budget = ResourceBudget::new(target.limits.clone()).map_err(|e| describe(&e))?;
    let query = ReadOperation {
        source: target.table.clone(),
        projection: vec!["V".into()],
        filter: Some(FilterExpression::Eq {
            field: "V".into(),
            parameter: "value".into(),
        }),
        order_by: vec![],
        row_limit: None,
        row_offset: None,
        declared_crs: vec![],
    };
    let parameters = ParameterBag::new([("value".into(), V::String(text.into()))].into());
    match target
        .provider
        .read(target.secret, &query, &parameters, &budget, &cancel)
        .await
    {
        Err(error)
            if error.category == ErrorCategory::DataMapping
                && matches!(error.phase, ErrorPhase::Prepare | ErrorPhase::Validate)
                && error.remote_effect == plenora_database_core::RemoteEffect::None =>
        {
            println!("NUL read: operando non qualificato, stesso rifiuto con controllo e NUL");
            Ok(())
        }
        Err(error) => Err(describe(&error)),
        Ok(_) => Err("operando read non qualificato aperto".into()),
    }
}
