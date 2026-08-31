//! Adapter Apache AGE 1.7.0 per PostgreSQL 18.

use crate::error::classify_error;
use bytes::BytesMut;
use plenora_database_core::graph::{
    AgeCapabilities, GraphEdge, GraphStatement, GraphValue, GraphVertex, MAX_CYPHER_BYTES,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;
use std::fmt;
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};
use tokio_postgres::Client;

const MAX_AGTYPE_CELL_BYTES: usize = 8 * 1_048_576;

pub async fn probe_age_capabilities(client: &Client) -> Result<AgeCapabilities> {
    let row = client
        .query_one(
            r"
            SELECT
                current_setting('server_version_num')::int / 10000,
                (SELECT extversion FROM pg_extension WHERE extname = 'age'),
                EXISTS (
                    SELECT 1
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = 'ag_catalog' AND p.proname = 'cypher'
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = 'ag_catalog' AND p.proname = 'agtype_out'
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_type t
                    JOIN pg_namespace n ON n.oid = t.typnamespace
                    WHERE n.nspname = 'ag_catalog' AND t.typname = 'agtype'
                )
            ",
            &[],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let postgres_major_i32: i32 = row.get(0);
    let postgres_major = u16::try_from(postgres_major_i32).unwrap_or(0);
    let extension_version: Option<String> = row.get(1);
    let mut runtime_objects_present: bool = row.get(2);

    let candidate = AgeCapabilities::from_probe(
        postgres_major,
        extension_version.clone(),
        runtime_objects_present,
    );
    if candidate.qualified() {
        // AGE documenta LOAD per ogni nuova sessione. Un'installazione che
        // espone il catalogo ma non consente di caricare il modulo non apre le
        // capability operative.
        runtime_objects_present = client.batch_execute("LOAD 'age'").await.is_ok();
    }
    Ok(AgeCapabilities::from_probe(
        postgres_major,
        extension_version,
        runtime_objects_present,
    ))
}

pub fn build_cypher_sql(statement: &GraphStatement) -> Result<String> {
    statement.validate()?;
    let delimiter = dollar_delimiter(&statement.cypher)?;
    let output_columns = statement
        .columns
        .iter()
        .map(|column| {
            let quoted = quote_identifier(column);
            format!("ag_catalog.agtype_out(q.{quoted})::text AS {quoted}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let record_columns = statement
        .columns
        .iter()
        .map(|column| format!("{} ag_catalog.agtype", quote_identifier(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let parameters = if statement.params.is_empty() {
        String::new()
    } else {
        ", $1".to_owned()
    };
    Ok(format!(
        "SELECT {output_columns} FROM ag_catalog.cypher('{}', {delimiter}{}{delimiter}{parameters}) AS q({record_columns})",
        statement.graph, statement.cypher
    ))
}

/// Parametro testuale per il tipo custom `ag_catalog.agtype`.
///
/// AGE richiede che il terzo argomento di `cypher` sia un parametro diretto:
/// un cast attorno a `$1` viene rifiutato dal parser dell'estensione. Il codec
/// usa quindi il formato testo del protocollo PostgreSQL per legare il JSON
/// senza interpolarlo nello statement.
pub struct AgeParameter(String);

impl AgeParameter {
    pub const fn new(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Debug for AgeParameter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AgeParameter([REDACTED])")
    }
}

impl ToSql for AgeParameter {
    fn to_sql(
        &self,
        _target_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        output.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(target_type: &Type) -> bool {
        target_type.name() == "agtype" && target_type.schema() == "ag_catalog"
    }

    to_sql_checked!();

    fn encode_format(&self, _target_type: &Type) -> Format {
        Format::Text
    }
}

fn dollar_delimiter(cypher: &str) -> Result<String> {
    for index in 0..=MAX_CYPHER_BYTES {
        let candidate = if index == 0 {
            "$plenora_age$".to_owned()
        } else {
            format!("$plenora_age_{index}$")
        };
        if !cypher.contains(&candidate) {
            return Ok(candidate);
        }
    }
    Err(DatabaseError::invalid_plan(
        "impossibile delimitare in sicurezza la query Cypher",
    ))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{value}\"")
}

/// Decodifica la rappresentazione testuale prodotta da `agtype_out`.
///
/// # Errors
///
/// Ritorna `DataMapping` per valori malformati, non supportati o oltre il
/// limite per cella, senza includere il valore nel messaggio.
pub fn parse_agtype(input: &str) -> Result<GraphValue> {
    if input.len() > MAX_AGTYPE_CELL_BYTES {
        return Err(mapping_error("cella agtype oltre il limite di 8 MiB"));
    }
    let mut parser = AgtypeParser::new(input);
    let value = parser.parse_value()?;
    parser.skip_whitespace();
    if parser.position != input.len() {
        return Err(mapping_error("valore agtype non riconosciuto"));
    }
    Ok(value)
}

fn mapping_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        Some(ProviderKind::Postgres),
        message,
    )
}

struct AgtypeParser<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> AgtypeParser<'a> {
    const fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    fn parse_value(&mut self) -> Result<GraphValue> {
        self.skip_whitespace();
        let value_start = self.position;
        let mut value = match self.peek() {
            Some(b'\"') => GraphValue::String(self.parse_string()?),
            Some(b'{') => GraphValue::Map(self.parse_map()?),
            Some(b'[') => GraphValue::List(self.parse_list()?),
            Some(_) => self.parse_atom()?,
            None => return Err(mapping_error("valore agtype vuoto")),
        };
        self.skip_whitespace();
        let suffix_start = self.position;
        if self.remaining().starts_with("::") {
            self.position += 2;
            let suffix = self.parse_suffix();
            value = match suffix {
                "numeric" => {
                    GraphValue::Numeric(self.input[value_start..suffix_start].trim().to_owned())
                }
                "vertex" => GraphValue::Vertex(vertex_from_value(value)?),
                "edge" => GraphValue::Edge(edge_from_value(value)?),
                "path" => match value {
                    GraphValue::List(items) => GraphValue::Path(items),
                    _ => return Err(mapping_error("path agtype non valido")),
                },
                _ => return Err(mapping_error("annotazione agtype non supportata")),
            };
        }
        Ok(value)
    }

    fn parse_map(&mut self) -> Result<BTreeMap<String, GraphValue>> {
        self.position += 1;
        let mut values = BTreeMap::new();
        self.skip_whitespace();
        if self.consume(b'}') {
            return Ok(values);
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'\"') {
                return Err(mapping_error("chiave mappa agtype non valida"));
            }
            let key = self.parse_string()?;
            self.skip_whitespace();
            if !self.consume(b':') {
                return Err(mapping_error("mappa agtype non valida"));
            }
            let value = self.parse_value()?;
            values.insert(key, value);
            self.skip_whitespace();
            if self.consume(b'}') {
                break;
            }
            if !self.consume(b',') {
                return Err(mapping_error("mappa agtype non valida"));
            }
        }
        Ok(values)
    }

    fn parse_list(&mut self) -> Result<Vec<GraphValue>> {
        self.position += 1;
        let mut values = Vec::new();
        self.skip_whitespace();
        if self.consume(b']') {
            return Ok(values);
        }
        loop {
            values.push(self.parse_value()?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            if !self.consume(b',') {
                return Err(mapping_error("lista agtype non valida"));
            }
        }
        Ok(values)
    }

    fn parse_string(&mut self) -> Result<String> {
        let start = self.position;
        self.position += 1;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            self.position += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'\"' {
                return serde_json::from_str(&self.input[start..self.position])
                    .map_err(|_| mapping_error("stringa agtype non valida"));
            }
        }
        Err(mapping_error("stringa agtype non terminata"))
    }

    fn parse_atom(&mut self) -> Result<GraphValue> {
        let start = self.position;
        while let Some(byte) = self.peek() {
            if byte.is_ascii_whitespace()
                || matches!(byte, b',' | b']' | b'}')
                || self.remaining().starts_with("::")
            {
                break;
            }
            self.position += 1;
        }
        let token = &self.input[start..self.position];
        match token {
            "null" => Ok(GraphValue::Null),
            "true" => Ok(GraphValue::Bool(true)),
            "false" => Ok(GraphValue::Bool(false)),
            "NaN" | "Infinity" | "-Infinity" => Ok(GraphValue::Numeric(token.to_owned())),
            _ => token.parse::<i64>().map(GraphValue::Integer).or_else(|_| {
                token
                    .parse::<f64>()
                    .map(GraphValue::Float)
                    .map_err(|_| mapping_error("numero agtype non valido"))
            }),
        }
    }

    fn parse_suffix(&mut self) -> &str {
        let start = self.position;
        while self.peek().is_some_and(|byte| byte.is_ascii_alphabetic()) {
            self.position += 1;
        }
        &self.input[start..self.position]
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.position += 1;
        }
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.position).copied()
    }

    fn remaining(&self) -> &str {
        &self.input[self.position..]
    }
}

fn vertex_from_value(value: GraphValue) -> Result<GraphVertex> {
    let GraphValue::Map(mut fields) = value else {
        return Err(mapping_error("vertex agtype non valido"));
    };
    Ok(GraphVertex {
        id: take_integer(&mut fields, "id", "vertex agtype non valido")?,
        label: take_string(&mut fields, "label", "vertex agtype non valido")?,
        properties: take_map(&mut fields, "properties", "vertex agtype non valido")?,
    })
}

fn edge_from_value(value: GraphValue) -> Result<GraphEdge> {
    let GraphValue::Map(mut fields) = value else {
        return Err(mapping_error("edge agtype non valido"));
    };
    Ok(GraphEdge {
        id: take_integer(&mut fields, "id", "edge agtype non valido")?,
        label: take_string(&mut fields, "label", "edge agtype non valido")?,
        start_id: take_integer(&mut fields, "start_id", "edge agtype non valido")?,
        end_id: take_integer(&mut fields, "end_id", "edge agtype non valido")?,
        properties: take_map(&mut fields, "properties", "edge agtype non valido")?,
    })
}

fn take_integer(
    fields: &mut BTreeMap<String, GraphValue>,
    name: &str,
    message: &'static str,
) -> Result<i64> {
    match fields.remove(name) {
        Some(GraphValue::Integer(value)) => Ok(value),
        _ => Err(mapping_error(message)),
    }
}

fn take_string(
    fields: &mut BTreeMap<String, GraphValue>,
    name: &str,
    message: &'static str,
) -> Result<String> {
    match fields.remove(name) {
        Some(GraphValue::String(value)) => Ok(value),
        _ => Err(mapping_error(message)),
    }
}

fn take_map(
    fields: &mut BTreeMap<String, GraphValue>,
    name: &str,
    message: &'static str,
) -> Result<BTreeMap<String, GraphValue>> {
    match fields.remove(name) {
        Some(GraphValue::Map(value)) => Ok(value),
        _ => Err(mapping_error(message)),
    }
}

#[cfg(test)]
#[path = "age_tests.rs"]
mod tests;
