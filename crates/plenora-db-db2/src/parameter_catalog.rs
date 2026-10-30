//! Unita esatte dei segnaposto direttamente associati a colonne.
//!
//! `SQLDescribeParam` perde la distinzione fra VARCHAR(12 OCTETS) e
//! VARCHAR(3 CODEUNITS32). Solo una colonna risolta nel catalogo autorizza
//! a sostituire quel limite ambiguo. Espressioni, join e CTE non vengono
//! indovinati: conservano l'ammissione del descrittore.

use crate::operand_catalog::Db2Catalog;
use odbc_api::Connection;
use plenora_database_core::operand_admission::{operand_type, OperandType};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::Result;
use sqlparser::ast::{
    BinaryOperator, CastKind, DataType, Expr, Ident, ObjectName, SetExpr, Statement, TableFactor,
    TableObject, TableWithJoins, Value,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
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
    let schema = match schema {
        Some(schema) => schema.to_owned(),
        None => crate::operand_catalog::OdbcCatalog::new(connection, timeout).current_schema()?,
    };
    let facts = crate::write_admission::column_facts(connection, timeout, &schema, name)?;
    for (index, binding) in columns.into_iter().enumerate() {
        if let Some(binding) = binding {
            result[index] = binding.text_operand(&facts)?;
        }
    }
    Ok(result)
}

/// Il candidato conserva ogni CAST: nessuno autorizza l'override prima del catalogo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub column: String,
    casts: Vec<DataType>,
}

impl Binding {
    pub fn matches(&self, facts: &[crate::write_admission::ColumnFacts]) -> Result<bool> {
        let mut matching = facts
            .iter()
            .filter(|fact| fact.name.trim_end_matches(' ') == self.column.trim_end_matches(' '));
        let fact = matching.next();
        if matching.next().is_some() || fact.is_some_and(|fact| fact.name != self.column) {
            return Err(crate::write_admission::identity_error());
        }
        if self.casts.is_empty() {
            return Ok(true);
        }
        let Some(target) =
            fact.and_then(|fact| crate::write::intermediate::TargetType::from_catalog(fact).ok())
        else {
            return Ok(false);
        };
        let Ok(Expr::Cast { data_type, .. }) = Parser::new(&ParameterDialect)
            .try_with_sql(&target.operand())
            .and_then(|mut parser| parser.parse_expr())
        else {
            return Ok(false);
        };
        Ok(self.casts.iter().all(|cast| *cast == data_type))
    }

    /// CAST e descrittore provengono dalla medesima riga, con nome esatto.
    pub fn text_operand(
        &self,
        facts: &[crate::write_admission::ColumnFacts],
    ) -> Result<Option<OperandType>> {
        if !self.matches(facts)? {
            return Ok(None);
        }
        let Some(fact) = facts.iter().find(|fact| fact.name == self.column) else {
            return Ok(None);
        };
        if fact.type_schema != "SYSIBM" {
            return Ok(None);
        }
        let kind = crate::operand_catalog::catalog_type(
            &fact.type_name,
            if fact.string_units_length > 0 {
                fact.string_units_length
            } else {
                fact.length
            },
            fact.scale,
            fact.codepage,
            &fact.string_units,
        );
        let kind = operand_type(ProviderKind::Db2, &kind);
        Ok(matches!(kind, OperandType::Text { .. }).then_some(kind))
    }
}

/// Conserva i token del tipo Db2, incluse le unita CODEUNITS32 e quelle dei
/// LOB che il dialetto generico non legge. Il tipo non viene reinterpretato:
/// deve uguagliare quello prodotto da `TargetType`, token per token.
#[derive(Debug)]
struct ParameterDialect;

impl Dialect for ParameterDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        GenericDialect.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        GenericDialect.is_identifier_part(ch)
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<std::result::Result<Expr, ParserError>> {
        if !parser.parse_keyword(Keyword::CAST) {
            return None;
        }
        Some((|| {
            parser.expect_token(&Token::LParen)?;
            let expr = Box::new(parser.parse_expr()?);
            parser.expect_keyword(Keyword::AS)?;
            let mut depth = 0;
            let mut tokens = Vec::new();
            loop {
                let token = parser.next_token().token;
                match &token {
                    Token::EOF => {
                        return Err(ParserError::ParserError("CAST Db2 incompleto".into()))
                    }
                    Token::RParen if depth == 0 => break,
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => (),
                }
                tokens.push(match token {
                    Token::Word(word) if word.quote_style.is_none() => word.value.to_uppercase(),
                    token => token.to_string(),
                });
            }
            Ok(Expr::Cast {
                kind: CastKind::Cast,
                expr,
                data_type: DataType::Custom(
                    ObjectName::from(Ident::with_quote('"', tokens.join(" "))),
                    vec![],
                ),
                format: None,
            })
        })())
    }
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

fn assign(expr: &Expr, column: Option<String>, bindings: &mut [Option<Binding>]) {
    match expr {
        Expr::Value(value) => {
            if let Value::Placeholder(marker) = &value.value {
                if let Some(index) = marker
                    .strip_prefix('?')
                    .and_then(|index| index.parse::<usize>().ok())
                    .and_then(|index| index.checked_sub(1))
                {
                    if let Some(slot) = bindings.get_mut(index) {
                        *slot = column.map(|column| Binding {
                            column,
                            casts: Vec::new(),
                        });
                    }
                }
            }
        }
        Expr::Nested(expr) => assign(expr, column, bindings),
        Expr::Cast {
            kind: CastKind::Cast,
            expr,
            data_type,
            format: None,
        } => {
            // Ogni livello deve essere identico: un CAST esterno non nasconde
            // la conversione di un CAST interno diverso.
            let mut inner = vec![None; bindings.len()];
            assign(expr, column, &mut inner);
            for (slot, candidate) in bindings.iter_mut().zip(inner) {
                if let Some(mut candidate) = candidate {
                    candidate.casts.push(data_type.clone());
                    *slot = Some(candidate);
                }
            }
        }
        _ => (),
    }
}

fn predicate(expr: &Expr, bindings: &mut [Option<Binding>]) {
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
pub fn bindings(sql: &str, count: usize) -> Option<(ObjectName, Vec<Option<Binding>>)> {
    let dialect = ParameterDialect;
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
