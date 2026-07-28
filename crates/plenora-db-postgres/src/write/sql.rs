use super::{
    mapping_error,
    plan::{ColumnSemantics, WriteColumnPlan},
};
use crate::field_contract::FieldContract;
use arrow_schema::{DataType, IntervalUnit, SchemaRef, TimeUnit};
use plenora_database_core::plan::{ObjectRef, WriteMode, WriteOperation};
use plenora_database_core::{DatabaseError, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};

// Keeping all statement shapes together makes placeholder ordering and field
// indexing reviewable across the seven supported write modes.
#[allow(clippy::too_many_lines)]
pub(super) fn statement(
    operation: &WriteOperation,
    target: &ObjectRef,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
) -> Result<(String, Vec<usize>)> {
    let renderer = renderer();
    let field_index = |name: &str| {
        schema
            .index_of(name)
            .map_err(|_| DatabaseError::invalid_plan("campo write assente nello schema Arrow"))
    };
    match operation.mode {
        WriteMode::Update => {
            let mut indexes = operation
                .update_columns
                .iter()
                .map(|name| field_index(name))
                .collect::<Result<Vec<_>>>()?;
            indexes.extend(
                operation
                    .keys
                    .iter()
                    .map(|name| field_index(name))
                    .collect::<Result<Vec<_>>>()?,
            );
            let sets = operation
                .update_columns
                .iter()
                .enumerate()
                .map(|(position, name)| {
                    let index = field_index(name)?;
                    let value = placeholder_expression(&plans[index], position + 1);
                    let identifier = Identifier::new(name.clone())?;
                    Ok(format!(
                        "{} = {value}",
                        renderer.quote_identifier(&identifier)
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let predicates = key_predicates(
                &renderer,
                &operation.keys,
                operation.update_columns.len() + 1,
            )?;
            Ok((
                format!(
                    "UPDATE {} SET {} WHERE {}",
                    quote_object(target)?,
                    sets.join(", "),
                    predicates
                ),
                indexes,
            ))
        }
        WriteMode::DeleteByKeys => Ok((
            format!(
                "DELETE FROM {} WHERE {}",
                quote_object(target)?,
                key_predicates(&renderer, &operation.keys, 1)?
            ),
            operation
                .keys
                .iter()
                .map(|name| field_index(name))
                .collect::<Result<Vec<_>>>()?,
        )),
        _ => {
            let columns = schema
                .fields()
                .iter()
                .map(|field| {
                    Identifier::new(field.name().clone()).map(|id| renderer.quote_identifier(&id))
                })
                .collect::<Result<Vec<_>>>()?;
            let values = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, _)| placeholder_expression(&plans[index], index + 1))
                .collect::<Vec<_>>();
            let conflict = if operation.mode == WriteMode::Upsert {
                let keys = operation
                    .keys
                    .iter()
                    .map(|key| {
                        Identifier::new(key.clone()).map(|id| renderer.quote_identifier(&id))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let updates = schema
                    .fields()
                    .iter()
                    .filter(|field| !operation.keys.contains(field.name()))
                    .map(|field| {
                        Identifier::new(field.name().clone()).map(|identifier| {
                            let name = renderer.quote_identifier(&identifier);
                            format!("{name} = EXCLUDED.{name}")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if updates.is_empty() {
                    format!(" ON CONFLICT ({}) DO NOTHING", keys.join(", "))
                } else {
                    format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        keys.join(", "),
                        updates.join(", ")
                    )
                }
            } else {
                String::new()
            };
            Ok((
                format!(
                    "INSERT INTO {} ({}) VALUES ({}){conflict}",
                    quote_object(target)?,
                    columns.join(", "),
                    values.join(", ")
                ),
                (0..schema.fields().len()).collect(),
            ))
        }
    }
}

pub(super) fn placeholder_expression(plan: &WriteColumnPlan, ordinal: usize) -> String {
    if plan.semantics == ColumnSemantics::Geometry {
        format!("ST_GeomFromEWKB(${ordinal})")
    } else if plan.semantics == ColumnSemantics::Geography {
        format!("ST_GeomFromEWKB(${ordinal})::geography")
    } else if matches!(plan.data_type, DataType::Decimal128(_, _)) {
        format!("${ordinal}::text::numeric")
    } else if matches!(plan.data_type, DataType::Utf8)
        && matches!(plan.native_type.as_deref(), Some("json" | "jsonb" | "uuid"))
    {
        format!(
            "${ordinal}::text::{}",
            plan.native_type.as_deref().unwrap_or("text")
        )
    } else if matches!(
        plan.data_type,
        DataType::Utf8 | DataType::List(_) | DataType::Struct(_)
    ) {
        plan.native_declaration_sql.as_deref().map_or_else(
            || format!("${ordinal}"),
            |declaration| {
                if declaration == "text" {
                    format!("${ordinal}")
                } else {
                    format!("${ordinal}::text::{declaration}")
                }
            },
        )
    } else {
        format!("${ordinal}")
    }
}

fn key_predicates(renderer: &Renderer, keys: &[String], first: usize) -> Result<String> {
    keys.iter()
        .enumerate()
        .map(|(offset, key)| {
            Identifier::new(key.clone())
                .map(|id| format!("{} = ${}", renderer.quote_identifier(&id), first + offset))
        })
        .collect::<Result<Vec<_>>>()
        .map(|items| items.join(" AND "))
}

pub(super) fn pg_type(contract: FieldContract<'_>) -> Result<String> {
    if contract.is_geometry() || contract.is_geography() {
        let base = if contract.is_geography() {
            "geography"
        } else {
            "geometry"
        };
        let geometry_type = contract.geometry_type.unwrap_or("Geometry");
        if !geometry_type
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
        {
            return Err(DatabaseError::invalid_plan("geometry type non valido"));
        }
        let dimensions = match contract.dimensions {
            None | Some("xy") => "",
            Some("xyz") => "Z",
            Some("xym") => "M",
            Some("xyzm") => "ZM",
            Some(_) => {
                return Err(DatabaseError::invalid_plan(
                    "dimensioni geometry non valide o ambigue",
                ));
            }
        };
        let srid = contract.srid.unwrap_or(0);
        return Ok(format!("{base}({geometry_type}{dimensions},{srid})"));
    }
    Ok(match contract.field.data_type() {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int32 => "integer".to_owned(),
        DataType::Int64 => "bigint".to_owned(),
        DataType::Float32 => "real".to_owned(),
        DataType::Float64 => "double precision".to_owned(),
        DataType::Utf8 => native_declaration_sql(contract).unwrap_or_else(|| "text".to_owned()),
        DataType::Binary => "bytea".to_owned(),
        DataType::Date32 => "date".to_owned(),
        DataType::Time64(TimeUnit::Microsecond) => "time".to_owned(),
        DataType::Interval(IntervalUnit::MonthDayNano) => "interval".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            if timezone.is_some() {
                "timestamptz".to_owned()
            } else {
                "timestamp".to_owned()
            }
        }
        DataType::Decimal128(precision, scale) => {
            format!("numeric({precision},{scale})")
        }
        DataType::List(_) => native_declaration_sql(contract).ok_or_else(mapping_error)?,
        DataType::Struct(_) if contract.is_range() => {
            native_declaration_sql(contract).ok_or_else(mapping_error)?
        }
        DataType::Struct(_) if contract.is_composite() => {
            native_declaration_sql(contract).ok_or_else(mapping_error)?
        }
        _ => return Err(mapping_error()),
    })
}

pub(super) fn native_declaration_sql(contract: FieldContract<'_>) -> Option<String> {
    let declaration = contract.native_declaration.or(contract.native_type)?;
    let base = declaration.strip_suffix("[]").unwrap_or(declaration);
    let supported = matches!(
        base,
        "text"
            | "character varying"
            | "boolean"
            | "smallint"
            | "integer"
            | "bigint"
            | "real"
            | "double precision"
            | "date"
            | "time without time zone"
            | "time with time zone"
            | "timestamp without time zone"
            | "timestamp with time zone"
            | "interval"
            | "uuid"
            | "json"
            | "jsonb"
            | "inet"
            | "cidr"
            | "int4range"
            | "int8range"
            | "numrange"
            | "tsrange"
            | "tstzrange"
            | "daterange"
    ) || (base.starts_with("numeric(")
        && base.ends_with(')')
        && base[8..base.len() - 1]
            .chars()
            .all(|character| character.is_ascii_digit() || character == ','));
    if supported {
        return Some(declaration.to_owned());
    }
    if matches!(contract.type_kind, Some("e" | "d" | "c")) {
        let (base, array_suffix) = declaration
            .strip_suffix("[]")
            .map_or((declaration, ""), |base| (base, "[]"));
        if base.split('.').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        }) {
            let renderer = renderer();
            let quoted = base
                .split('.')
                .map(|part| {
                    Identifier::new(part.to_owned())
                        .map(|identifier| renderer.quote_identifier(&identifier))
                })
                .collect::<Result<Vec<_>>>()
                .ok()?;
            return Some(format!("{}{array_suffix}", quoted.join(".")));
        }
    }
    None
}

pub(super) fn quote_object(target: &ObjectRef) -> Result<String> {
    Ok(renderer().quote_object(&ObjectName {
        catalog: None,
        schema: target
            .schema
            .as_ref()
            .map(|value| Identifier::new(value.clone()))
            .transpose()?,
        object: Identifier::new(target.object.clone())?,
    }))
}

pub(super) const fn renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}
