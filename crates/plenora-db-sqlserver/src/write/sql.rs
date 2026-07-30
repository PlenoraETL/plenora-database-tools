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
    let quoted_columns = columns
        .iter()
        .map(|column| sql_identifier(&column.name).map(|name| renderer.quote_identifier(&name)))
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
        Ok(placeholder_expression(&columns[index], ordinals[index]))
    };
    let quoted_for = |name: &str| -> Result<String> {
        sql_identifier(name).map(|identifier| renderer.quote_identifier(&identifier))
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
        .collect::<Vec<_>>();
    let insert_sql = || {
        format!(
            "INSERT INTO {quoted_object} ({}) OUTPUT 1 AS [plenora_action] VALUES ({});",
            quoted_columns.join(", "),
            insert_expressions.join(", ")
        )
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
        WriteMode::Update => format!(
            "UPDATE {quoted_object} WITH (UPDLOCK, HOLDLOCK) SET {} \
             OUTPUT 2 AS [plenora_action] WHERE {};",
            assignments()?.join(", "),
            predicates()?
        ),
        WriteMode::DeleteByKeys => format!(
            "DELETE FROM {quoted_object} WITH (HOLDLOCK) \
             OUTPUT 3 AS [plenora_action] WHERE {};",
            predicates()?
        ),
        WriteMode::Upsert => {
            let assignments = assignments()?;
            let update = if assignments.is_empty() {
                format!(
                    "IF NOT EXISTS (SELECT 1 FROM {quoted_object} WITH \
                     (UPDLOCK, HOLDLOCK) WHERE {}) BEGIN ",
                    predicates()?
                )
            } else {
                format!(
                    "UPDATE {quoted_object} WITH (UPDLOCK, HOLDLOCK) SET {} \
                     OUTPUT 2 INTO @plenora_actions ([action]) WHERE {}; \
                     IF @@ROWCOUNT = 0 BEGIN ",
                    assignments.join(", "),
                    predicates()?
                )
            };
            format!(
                "DECLARE @plenora_actions TABLE ([action] int NOT NULL); \
                 {update}INSERT INTO {quoted_object} ({}) \
                 OUTPUT 1 INTO @plenora_actions ([action]) VALUES ({}); END; \
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

fn placeholder_expression(column: &WriteColumnPlan, ordinal: usize) -> String {
    let placeholder = format!("@P{ordinal}");
    match column.kind {
        SqlServerColumnKind::Decimal { .. } | SqlServerColumnKind::TimestampTz => {
            format!("CONVERT({}, {placeholder})", column.native_declaration)
        }
        SqlServerColumnKind::Utf8 if column.native_type == "uniqueidentifier" => {
            format!("CONVERT(uniqueidentifier, {placeholder})")
        }
        SqlServerColumnKind::Utf8 if column.native_type == "xml" => {
            format!("CONVERT(xml, {placeholder})")
        }
        SqlServerColumnKind::Geometry => format!(
            "geometry::STGeomFromWKB({placeholder}, @P{})",
            ordinal.saturating_add(1)
        ),
        SqlServerColumnKind::Geography => format!(
            "geography::STGeomFromWKB({placeholder}, @P{})",
            ordinal.saturating_add(1)
        ),
        _ => placeholder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::loss::MappingPolicy;
    use plenora_database_core::plan::{ObjectRef, SridPolicy, TransactionProfile, WriteOperation};
    use plenora_database_sql::{Dialect, DialectCapabilities};

    fn operation(mode: WriteMode) -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("dbo".to_owned()),
                object: "target".to_owned(),
                layer_id: None,
            },
            mode,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: vec!["id".to_owned()],
            update_columns: Vec::new(),
            srid_policy: Some(SridPolicy::RequireMatch),
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    fn columns() -> Vec<WriteColumnPlan> {
        vec![
            WriteColumnPlan {
                input_index: 0,
                name: "id".to_owned(),
                kind: SqlServerColumnKind::I32,
                native_type: "int".to_owned(),
                native_declaration: "int".to_owned(),
                nullable: false,
                collation: None,
                spatial_srid: None,
            },
            WriteColumnPlan {
                input_index: 1,
                name: "label".to_owned(),
                kind: SqlServerColumnKind::Utf8,
                native_type: "nvarchar".to_owned(),
                native_declaration: "nvarchar(100)".to_owned(),
                nullable: false,
                collation: None,
                spatial_srid: None,
            },
        ]
    }

    fn renderer() -> Renderer {
        Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
    }

    #[test]
    fn keyed_statements_are_bound_and_never_use_merge() {
        let renderer = renderer();
        let mut update = operation(WriteMode::Update);
        update.update_columns = vec!["label".to_owned()];
        let compiled = compile_row_statement(&update, &columns(), &renderer, "[dbo].[target]")
            .expect("update SQL");
        assert_eq!(compiled.key_input_indices, vec![0]);
        assert!(compiled.sql.contains("SET [label] = @P2"));
        assert!(compiled.sql.contains("WHERE [id] = @P1"));

        let upsert =
            compile_row_statement(&operation(WriteMode::Upsert), &columns(), &renderer, "[t]")
                .expect("upsert SQL");
        assert!(upsert.sql.contains("UPDLOCK, HOLDLOCK"));
        assert!(upsert.sql.contains("IF @@ROWCOUNT = 0"));
        assert!(!upsert.sql.to_uppercase().contains("MERGE"));

        let delete = compile_row_statement(
            &operation(WriteMode::DeleteByKeys),
            &columns()[..1],
            &renderer,
            "[t]",
        )
        .expect("delete SQL");
        assert!(delete.sql.contains("OUTPUT 3"));
        assert!(delete.sql.contains("WHERE [id] = @P1"));
    }
}
