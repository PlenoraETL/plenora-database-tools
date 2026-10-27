//! Unita esatte dei segnaposto direttamente associati a colonne.
//!
//! `SQLDescribeParam` perde la distinzione fra VARCHAR(12 OCTETS) e
//! VARCHAR(3 CODEUNITS32). Solo una colonna risolta nel catalogo autorizza
//! a sostituire quel limite ambiguo. Espressioni, join e CTE non vengono
//! indovinati: conservano l'ammissione del descrittore.

use odbc_api::Connection;
use plenora_database_core::operand_admission::{operand_type, OperandType};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::Result;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, SetExpr, Statement, TableFactor, TableObject,
    TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

pub fn text_operands(
    connection: &Connection<'_>,
    timeout: usize,
    sql: &str,
    count: usize,
) -> Result<Vec<Option<OperandType>>> {
    let mut result = vec![None; count];
    if count == 0 {
        return Ok(result);
    }
    let Some((table, columns)) = bindings(sql, count) else {
        return Ok(result);
    };
    if columns.iter().all(Option::is_none) {
        return Ok(result);
    }
    let parts = names(&table);
    let Some(parts) = parts else {
        return Ok(result);
    };
    let (schema, name) = match parts.as_slice() {
        [name] => (None, name.as_str()),
        [schema, name] => (Some(schema.as_str()), name.as_str()),
        _ => return Ok(result),
    };
    let Some(types) =
        crate::operand_catalog::OdbcCatalog::new(connection, timeout).column_types(schema, name)?
    else {
        return Ok(result);
    };
    for (index, column) in columns.into_iter().enumerate() {
        if let Some(kind) = column.and_then(|column| types.get(&column)) {
            let kind = operand_type(ProviderKind::Db2, kind);
            if matches!(kind, OperandType::Text { .. }) {
                result[index] = Some(kind);
            }
        }
    }
    Ok(result)
}

fn name(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_uppercase()
    }
}

fn names(object: &ObjectName) -> Option<Vec<String>> {
    object
        .0
        .iter()
        .map(|part| part.as_ident().map(name))
        .collect()
}

fn column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(name(ident)),
        Expr::Nested(expr) => column(expr),
        // Qualificatori e scope non dimostrati non autorizzano un override.
        _ => None,
    }
}

fn assign(expr: &Expr, column: Option<String>, bindings: &mut [Option<String>]) {
    match expr {
        Expr::Value(value) => {
            if let Value::Placeholder(marker) = &value.value {
                if let Some(index) = marker
                    .strip_prefix('?')
                    .and_then(|index| index.parse::<usize>().ok())
                    .and_then(|index| index.checked_sub(1))
                {
                    if let Some(slot) = bindings.get_mut(index) {
                        *slot = column;
                    }
                }
            }
        }
        Expr::Nested(expr) => assign(expr, column, bindings),
        _ => (),
    }
}

fn predicate(expr: &Expr, bindings: &mut [Option<String>]) {
    match expr {
        Expr::Nested(expr) => predicate(expr, bindings),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                predicate(left, bindings);
                predicate(right, bindings);
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                assign(left, column(right), bindings);
                assign(right, column(left), bindings);
            }
            _ => (),
        },
        Expr::InList { expr, list, .. } => {
            for value in list {
                assign(value, column(expr), bindings);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            assign(low, column(expr), bindings);
            assign(high, column(expr), bindings);
        }
        _ => (),
    }
}

fn relation(tables: &[TableWithJoins]) -> Option<ObjectName> {
    let [table] = tables else {
        return None;
    };
    if !table.joins.is_empty() {
        return None;
    }
    match &table.relation {
        TableFactor::Table {
            name,
            args: None,
            alias,
            ..
        } if alias.as_ref().is_none_or(|alias| alias.columns.is_empty()) => Some(name.clone()),
        _ => None,
    }
}

/// Numera i token, non i caratteri: '?' in stringhe e commenti non e un bind.
/// Il SQL numerato serve soltanto all'analisi; non viene mai eseguito.
pub fn bindings(sql: &str, count: usize) -> Option<(ObjectName, Vec<Option<String>>)> {
    let dialect = GenericDialect;
    let mut tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut index = 0;
    for token in &mut tokens {
        if let Token::Placeholder(marker) = token {
            if marker != "?" {
                return None;
            }
            index += 1;
            *marker = format!("?{index}");
        }
    }
    if index != count {
        return None;
    }
    let statements = Parser::new(&dialect)
        .with_tokens(tokens)
        .parse_statements()
        .ok()?;
    let [statement] = statements.as_slice() else {
        return None;
    };
    let mut bindings = vec![None; count];
    let table = match statement {
        Statement::Insert(insert) => {
            let TableObject::TableName(table) = &insert.table else {
                return None;
            };
            let source = insert.source.as_ref()?;
            if source.with.is_some() || insert.on.is_some() {
                return None;
            }
            let SetExpr::Values(values) = source.body.as_ref() else {
                return None;
            };
            for row in &values.rows {
                if row.len() != insert.columns.len() {
                    return None;
                }
                for (value, col) in row.iter().zip(&insert.columns) {
                    let parts = names(col)?;
                    let [col] = parts.as_slice() else {
                        return None;
                    };
                    assign(value, Some(col.clone()), &mut bindings);
                }
            }
            table.clone()
        }
        Statement::Query(query) => {
            if query.with.is_some() {
                return None;
            }
            let SetExpr::Select(select) = query.body.as_ref() else {
                return None;
            };
            let table = relation(&select.from)?;
            if let Some(expr) = &select.selection {
                predicate(expr, &mut bindings);
            }
            table
        }
        Statement::Update(update) => {
            if update.from.is_some() {
                return None;
            }
            let table = relation(std::slice::from_ref(&update.table))?;
            for assignment in &update.assignments {
                if let sqlparser::ast::AssignmentTarget::ColumnName(col) = &assignment.target {
                    let parts = names(col)?;
                    if let [col] = parts.as_slice() {
                        assign(&assignment.value, Some(col.clone()), &mut bindings);
                    }
                }
            }
            if let Some(expr) = &update.selection {
                predicate(expr, &mut bindings);
            }
            table
        }
        Statement::Delete(delete) => {
            if delete.using.is_some() {
                return None;
            }
            let (sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables)) = &delete.from;
            let table = relation(tables)?;
            if let Some(expr) = &delete.selection {
                predicate(expr, &mut bindings);
            }
            table
        }
        _ => return None,
    };
    Some((table, bindings))
}
