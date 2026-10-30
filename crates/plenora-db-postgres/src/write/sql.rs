use super::{mapping_error, plan::WriteColumnPlan};
use crate::field_contract::FieldContract;
use arrow_schema::{DataType, IntervalUnit, SchemaRef, TimeUnit};
use plenora_database_core::plan::{ObjectRef, WriteMode, WriteOperation};
use plenora_database_core::{DatabaseError, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};

// Keeping all statement shapes together makes placeholder ordering and field
// indexing auditable across the supported write modes.
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
                        renderer.quote_identifier(&identifier)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let predicates = key_predicates(
                &renderer,
                &operation.keys,
                schema,
                plans,
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
                key_predicates(&renderer, &operation.keys, schema, plans, 1)?
            ),
            operation
                .keys
                .iter()
                .map(|name| field_index(name))
                .collect::<Result<Vec<_>>>()?,
        )),
        _ => {
            let columns: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| {
                    let id = Identifier::new(field.name().clone())?;
                    renderer.quote_identifier(&id)
                })
                .collect::<Result<Vec<_>>>()?;
            let values = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, _)| placeholder_expression(&plans[index], index + 1))
                .collect::<Vec<_>>();
            let conflict = if operation.mode == WriteMode::Upsert {
                let keys: Vec<String> = operation
                    .keys
                    .iter()
                    .map(|key| {
                        let id = Identifier::new(key.clone())?;
                        renderer.quote_identifier(&id)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let updates: Vec<String> = schema
                    .fields()
                    .iter()
                    .filter(|field| !operation.keys.contains(field.name()))
                    .map(|field| {
                        let identifier = Identifier::new(field.name().clone())?;
                        let name = renderer.quote_identifier(&identifier)?;
                        Ok(format!("{name} = EXCLUDED.{name}"))
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

/// Il server tipizza il parametro con la colonna, non con i metadata Arrow.
/// Testo, decimali, liste e letterali usano la funzione d'ingresso del target;
/// i valori tipizzati e `PostGIS` usano il suo formato binario. L'OID inferito
/// viene verificato prima dei valori anche nel percorso row-scoped.
/// Le regole di canonicalita e perdita si applicano al catalogo del target.
pub(super) fn placeholder_expression(_plan: &WriteColumnPlan, ordinal: usize) -> String {
    // Anche PostGIS riceve direttamente EWKB nel tipo del target, come COPY.
    // Un costruttore SQL darebbe invece al parametro il tipo bytea.
    format!("${ordinal}")
}

/// Il confronto delle chiavi di Update e `DeleteByKeys`, con l'uguaglianza
/// dell'indice che ne prova l'unicita (regola K2): la colonna resta intatta,
/// il parametro prende tipo, typmod e collation del target. L'operatore
/// qualificato deve accettare lo stesso tipo (guardia nel catalogo). Il testo non
/// dipende dal `search_path` (regola Q2). Una chiave senza l'uguaglianza
/// letta dal catalogo si rifiuta: nessun ripiego su un `=` risolto dalla
/// sessione.
fn key_predicates(
    renderer: &Renderer,
    keys: &[String],
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
    first: usize,
) -> Result<String> {
    let items: Result<Vec<String>> = keys
        .iter()
        .enumerate()
        .map(|(offset, key)| {
            let id = Identifier::new(key.clone())?;
            let quoted = renderer.quote_identifier(&id)?;
            let plan = schema
                .index_of(key)
                .ok()
                .and_then(|index| plans.get(index))
                .ok_or_else(super::intermediate::unsupported)?;
            let equality = plan
                .key_equality
                .as_ref()
                .ok_or_else(super::intermediate::unsupported)?;
            let ordinal = first + offset;
            let parameter = plan
                .target_type
                .as_ref()
                .map_or_else(|| format!("${ordinal}"), |target| target.parameter(ordinal));
            Ok(format!(
                "{quoted} OPERATOR({operator}) {parameter}",
                operator = equality.operator,
            ))
        })
        .collect();
    Ok(items?.join(" AND "))
}

/// I tipi di `pg_catalog` che lo SQL della scrittura nomina e che non sono
/// parole chiave della grammatica (`integer`, `boolean`, `numeric(p,s)`,
/// `timestamp ...`, `time ...`, `interval`, `character varying` il parser li
/// risolve in `pg_catalog` da se).
const UNRESERVED_BUILTINS: &[&str] = &[
    "text",
    "bytea",
    "date",
    "timestamptz",
    "uuid",
    "json",
    "jsonb",
    "inet",
    "cidr",
    "int4range",
    "int8range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
];

/// La regola Q2 negli statement sul target, che girano con il `search_path`
/// della sessione: un tipo di `pg_catalog` che non e una parola chiave si
/// qualifica (`pg_catalog.date[]`), cosi uno schema della sessione con un
/// tipo omonimo non lo sostituisce. Gli altri nomi restano come sono.
pub(super) fn qualify_builtin(declaration: &str) -> String {
    let base = declaration.trim_end_matches("[]");
    let suffix = &declaration[base.len()..];
    if UNRESERVED_BUILTINS.contains(&base) {
        format!("pg_catalog.{base}{suffix}")
    } else {
        declaration.to_owned()
    }
}

/// Il tipo con cui `CREATE TABLE` e l'evoluzione dello schema dichiarano la
/// colonna, con i tipi di `pg_catalog` qualificati ([`qualify_builtin`]).
pub(super) fn pg_type(contract: &FieldContract<'_>) -> Result<String> {
    let declared = pg_type_unqualified(contract)?;
    Ok(if contract.is_geometry() || contract.is_geography() {
        declared
    } else {
        qualify_builtin(&declared)
    })
}

fn pg_type_unqualified(contract: &FieldContract<'_>) -> Result<String> {
    if contract.is_geometry() || contract.is_geography() {
        let base = if contract.is_geography() {
            "geography"
        } else {
            "geometry"
        };
        // Piu tipi dichiarati (ARROW-VOCABULARY §3) chiedono una colonna
        // generica: il controllo per valore sta in
        // `plenora_database_core::geometry_value`.
        let geometry_type = contract
            .geometry_type
            .filter(|types| !types.contains(','))
            .unwrap_or("Geometry");
        if !geometry_type
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
        {
            return Err(DatabaseError::invalid_plan("geometry type non valido"));
        }
        // Dimensioni non note in lettura (colonna senza typmod): la colonna
        // si ricrea generica, perche un typmod imporrebbe XY.
        if contract.dimensions == Some("unknown") {
            return Ok(base.to_owned());
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
        let srid = contract.target_srid.unwrap_or(0);
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

pub(super) fn native_declaration_sql(contract: &FieldContract<'_>) -> Option<String> {
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
            let quoted: Result<Vec<String>> = base
                .split('.')
                .map(|part| {
                    let identifier = Identifier::new(part.to_owned())?;
                    renderer.quote_identifier(&identifier)
                })
                .collect();
            return Some(format!("{}{array_suffix}", quoted.ok()?.join(".")));
        }
    }
    None
}

pub(super) fn quote_object(target: &ObjectRef) -> Result<String> {
    renderer().quote_object(&ObjectName {
        catalog: None,
        schema: target
            .schema
            .as_ref()
            .map(|value| Identifier::new(value.clone()))
            .transpose()?,
        object: Identifier::new(target.object.clone())?,
    })
}

pub(super) const fn renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}
