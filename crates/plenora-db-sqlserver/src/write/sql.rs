use super::plan::{plan_error, sql_identifier, WriteColumnPlan};
use crate::SqlServerColumnKind;
use plenora_database_core::plan::{WriteMode, WriteOperation};
use plenora_database_core::{ErrorCategory, Result};
use plenora_database_sql::Renderer;

pub(super) struct RowStatement {
    pub(super) sql: String,
    pub(super) key_input_indices: Vec<usize>,
}

/// Compila una sola forma DML parametrica per record Arrow.
///
/// Questa unità non apre connessioni e non decide se un target sia sicuro:
/// riceve un piano colonna già validato e può soltanto produrre SQL e ordine
/// dei bind. L'upsert evita deliberatamente `MERGE`.
// Tenere affiancate tutte le forme DML rende revisionabili placeholder, lock e
// codici OUTPUT come un unico protocollo; dividerle nasconderebbe le simmetrie.
#[allow(clippy::too_many_lines)]
pub(super) fn compile_row_statement(
    operation: &WriteOperation,
    columns: &[WriteColumnPlan],
    renderer: &Renderer,
    quoted_object: &str,
) -> Result<RowStatement> {
    let quoted_columns: Vec<String> = columns
        .iter()
        .map(|column| {
            let name = sql_identifier(&column.name)?;
            renderer.quote_identifier(&name)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut next_ordinal = 1_usize;
    let ordinals = columns
        .iter()
        .map(|column| {
            let ordinal = next_ordinal;
            next_ordinal =
                next_ordinal.saturating_add(if column.spatial_srid.is_some() { 2 } else { 1 });
            ordinal
        })
        .collect::<Vec<_>>();
    let expression_for = |name: &str| -> Result<String> {
        let index = columns
            .iter()
            .position(|column| column.name == name)
            .ok_or_else(|| {
                plan_error(
                    ErrorCategory::InvalidPlan,
                    format!("campo write assente dallo schema Arrow: {name}"),
                )
            })?;
        placeholder_expression(&columns[index], ordinals[index])
    };
    let quoted_for = |name: &str| -> Result<String> {
        let identifier = sql_identifier(name)?;
        renderer.quote_identifier(&identifier)
    };
    let key_input_indices = operation
        .keys
        .iter()
        .map(|key| {
            columns
                .iter()
                .position(|column| column.name == *key)
                .ok_or_else(|| {
                    plan_error(
                        ErrorCategory::InvalidPlan,
                        format!("chiave assente dallo schema Arrow: {key}"),
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let insert_expressions = columns
        .iter()
        .zip(&ordinals)
        .map(|(column, ordinal)| placeholder_expression(column, *ordinal))
        .collect::<Result<Vec<_>>>()?;
    // `OUTPUT` senza `INTO` e rifiutato da SQL Server (334) su una tabella
    // con un trigger abilitato: fino al 25/10 Append, `TruncateInsert`,
    // Update e `DeleteByKeys` fallivano con un trigger AFTER qualunque.
    // L'azione va in una variabile tabella, che `OUTPUT ... INTO` ammette
    // sempre, e si rilegge con un `SELECT`: la stessa forma di Upsert.
    let actions = |statement: String| {
        format!(
            "DECLARE @plenora_actions TABLE ([action] int NOT NULL); {statement} \
             SELECT [action] AS [plenora_action] FROM @plenora_actions;"
        )
    };
    let insert_sql = || {
        actions(format!(
            "INSERT INTO {quoted_object} ({}) OUTPUT 1 INTO @plenora_actions ([action]) \
             VALUES ({});",
            quoted_columns.join(", "),
            insert_expressions.join(", ")
        ))
    };
    let predicates = || -> Result<String> {
        operation
            .keys
            .iter()
            .map(|key| -> Result<String> {
                Ok(format!("{} = {}", quoted_for(key)?, expression_for(key)?))
            })
            .collect::<Result<Vec<_>>>()
            .map(|parts| parts.join(" AND "))
    };
    let update_names = || {
        if operation.update_columns.is_empty() {
            columns
                .iter()
                .filter(|column| !operation.keys.contains(&column.name))
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        } else {
            operation.update_columns.clone()
        }
    };
    let assignments = || {
        update_names()
            .iter()
            .map(|name| -> Result<String> {
                Ok(format!("{} = {}", quoted_for(name)?, expression_for(name)?))
            })
            .collect::<Result<Vec<_>>>()
    };
    let sql = match operation.mode {
        WriteMode::Create | WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert => {
            insert_sql()
        }
        WriteMode::Update => actions(format!(
            "UPDATE {quoted_object} WITH (UPDLOCK, HOLDLOCK) SET {} \
             OUTPUT 2 INTO @plenora_actions ([action]) WHERE {};",
            assignments()?.join(", "),
            predicates()?
        )),
        WriteMode::DeleteByKeys => actions(format!(
            "DELETE FROM {quoted_object} WITH (HOLDLOCK) \
             OUTPUT 3 INTO @plenora_actions ([action]) WHERE {};",
            predicates()?
        )),
        WriteMode::Upsert => {
            let assignments = assignments()?;
            // Ogni riga dell'ingresso lascia un'azione: 2 aggiornata, 1
            // inserita, 0 (soltanto con le sole chiavi) gia presente. Una
            // riga senza azione e un INSERT che il server ha scartato senza
            // errore (un indice unico `IGNORE_DUP_KEY = ON` su altre colonne)
            // e si rifiuta come in Append (`row_mutation`): fino al 25/10
            // usciva `skipped` con il COMMIT.
            let (update, otherwise) = if assignments.is_empty() {
                (
                    format!(
                        "IF NOT EXISTS (SELECT 1 FROM {quoted_object} WITH \
                         (UPDLOCK, HOLDLOCK) WHERE {}) BEGIN ",
                        predicates()?
                    ),
                    " ELSE INSERT INTO @plenora_actions ([action]) VALUES (0);",
                )
            } else {
                (
                    format!(
                        "UPDATE {quoted_object} WITH (UPDLOCK, HOLDLOCK) SET {} \
                         OUTPUT 2 INTO @plenora_actions ([action]) WHERE {}; \
                         IF @@ROWCOUNT = 0 BEGIN ",
                        assignments.join(", "),
                        predicates()?
                    ),
                    "",
                )
            };
            format!(
                "DECLARE @plenora_actions TABLE ([action] int NOT NULL); \
                 {update}INSERT INTO {quoted_object} ({}) \
                 OUTPUT 1 INTO @plenora_actions ([action]) VALUES ({}); END;{otherwise} \
                 SELECT [action] AS [plenora_action] FROM @plenora_actions;",
                quoted_columns.join(", "),
                insert_expressions.join(", ")
            )
        }
    };
    Ok(RowStatement {
        sql,
        key_input_indices,
    })
}

/// Tipo intermedio del target: unica fonte per parametri e tabella delle chiavi.
pub(super) fn intermediate_declaration(column: &WriteColumnPlan) -> Result<String> {
    if matches!(column.native_type.as_str(), "text" | "ntext" | "image") {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "tipo legacy SQL Server non rappresentabile in un parametro del target",
        ));
    }
    let collation = match &column.collation {
        Some(name) => {
            super::plan::validate_collation_name(name)?;
            format!(" COLLATE {name}")
        }
        None if matches!(column.native_type.as_str(), "char" | "varchar" | "text") => {
            return Err(plan_error(
                ErrorCategory::Unsupported,
                "collation del tipo intermedio SQL Server non provata",
            ));
        }
        None => String::new(),
    };
    Ok(format!("{}{collation}", column.native_declaration))
}

pub(super) fn placeholder_expression(column: &WriteColumnPlan, ordinal: usize) -> Result<String> {
    let placeholder = format!("@P{ordinal}");
    let declaration = intermediate_declaration(column)?;
    let expression = match column.kind {
        SqlServerColumnKind::Geometry => format!(
            "geometry::STGeomFromWKB({placeholder}, @P{})",
            ordinal.saturating_add(1)
        ),
        SqlServerColumnKind::Geography => format!(
            "geography::STGeomFromWKB({placeholder}, @P{})",
            ordinal.saturating_add(1)
        ),
        _ => {
            // La collation prima della conversione seleziona anche la code page:
            // convertirla dopo soltanto userebbe quella del database corrente.
            let source = if matches!(column.native_type.as_str(), "char" | "varchar") {
                format!(
                    "{placeholder} COLLATE {}",
                    column.collation.as_deref().unwrap_or_default()
                )
            } else {
                placeholder
            };
            let style = if column.native_type == "xml" {
                ", 1"
            } else {
                ""
            };
            let converted = format!("CONVERT({}, {source}{style})", column.native_declaration);
            match declaration.strip_prefix(&column.native_declaration) {
                Some(suffix) => format!("{converted}{suffix}"),
                None => converted,
            }
        }
    };
    Ok(expression)
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;
