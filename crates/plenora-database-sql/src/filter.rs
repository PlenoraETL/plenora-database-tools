//! Rendering condiviso dei filtri scalari del piano portabile.

use super::{Expression, Identifier};
use plenora_database_core::plan::{ComparisonOperator, FilterExpression, ProviderKind};
use plenora_database_core::{DatabaseError, ErrorPhase, Result};

/// Superficie `FilterExpression` ammessa dal dialetto corrente.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterLowering {
    pub provider: ProviderKind,
    pub case_insensitive_like: bool,
    pub spatial: bool,
}

/// Converte il filtro del piano nell'AST del renderer senza perdere le
/// restrizioni del provider.
///
/// # Errors
///
/// Propaga la validazione dell'identificatore e restituisce `Unsupported`
/// quando il filtro domanda una forma che la policy non apre.
pub fn lower_filter<F>(
    expression: &FilterExpression,
    policy: FilterLowering,
    identifier: F,
) -> Result<Expression>
where
    F: Copy + Fn(&str) -> Result<Identifier>,
{
    fn comparison<F>(
        field: &str,
        operator: ComparisonOperator,
        parameter: &str,
        identifier: F,
    ) -> Result<Expression>
    where
        F: Copy + Fn(&str) -> Result<Identifier>,
    {
        Ok(Expression::Compare {
            field: identifier(field)?,
            operator,
            parameter: parameter.to_owned(),
        })
    }

    match expression {
        FilterExpression::And { args } => Ok(Expression::And(
            args.iter()
                .map(|argument| lower_filter(argument, policy, identifier))
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Or { args } => Ok(Expression::Or(
            args.iter()
                .map(|argument| lower_filter(argument, policy, identifier))
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Eq { field, parameter } => {
            comparison(field, ComparisonOperator::Eq, parameter, identifier)
        }
        FilterExpression::Ne { field, parameter } => {
            comparison(field, ComparisonOperator::Ne, parameter, identifier)
        }
        FilterExpression::Lt { field, parameter } => {
            comparison(field, ComparisonOperator::Lt, parameter, identifier)
        }
        FilterExpression::Lte { field, parameter } => {
            comparison(field, ComparisonOperator::Lte, parameter, identifier)
        }
        FilterExpression::Gt { field, parameter } => {
            comparison(field, ComparisonOperator::Gt, parameter, identifier)
        }
        FilterExpression::Gte { field, parameter } => {
            comparison(field, ComparisonOperator::Gte, parameter, identifier)
        }
        FilterExpression::IsNull { field } => Ok(Expression::IsNull(identifier(field)?)),
        FilterExpression::IsNotNull { field } => Ok(Expression::IsNotNull(identifier(field)?)),
        FilterExpression::In { field, parameters } => Ok(Expression::In {
            field: identifier(field)?,
            parameters: parameters.clone(),
        }),
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => Ok(Expression::Between {
            field: identifier(field)?,
            lower_parameter: lower_parameter.clone(),
            upper_parameter: upper_parameter.clone(),
        }),
        FilterExpression::Like {
            field,
            parameter,
            case_insensitive,
        } => {
            if *case_insensitive && !policy.case_insensitive_like {
                return Err(DatabaseError::unsupported(
                    policy.provider,
                    ErrorPhase::Prepare,
                    "LIKE case-insensitive richiede una collation esplicita",
                ));
            }
            Ok(Expression::Like {
                field: identifier(field)?,
                parameter: parameter.clone(),
                case_insensitive: *case_insensitive,
            })
        }
        FilterExpression::Spatial {
            function,
            field,
            geometry_parameter,
            distance_parameter,
        } => {
            if !policy.spatial {
                return Err(DatabaseError::unsupported(
                    policy.provider,
                    ErrorPhase::Prepare,
                    "filtro spatial richiede tipo, WKB e SRID risolti",
                ));
            }
            Ok(Expression::SpatialPredicate {
                function: *function,
                field: identifier(field)?,
                geometry_parameter: geometry_parameter.clone(),
                distance_parameter: distance_parameter.clone(),
            })
        }
    }
}

/// Seleziona e ordina colonne per nome preservandone il tipo concreto.
///
/// # Errors
///
/// Usa l'errore costruito dal chiamante quando una colonna non esiste.
pub fn select_columns_by_name<T, N, E>(
    available: &[T],
    projection: &[String],
    name_of: N,
    missing: E,
) -> Result<Vec<T>>
where
    T: Clone,
    N: for<'a> Fn(&'a T) -> &'a str,
    E: Fn() -> DatabaseError,
{
    if projection.is_empty() {
        return Ok(available.to_vec());
    }
    projection
        .iter()
        .map(|name| {
            available
                .iter()
                .find(|column| name_of(column) == name)
                .cloned()
                .ok_or_else(&missing)
        })
        .collect()
}

#[cfg(test)]
#[path = "filter_tests.rs"]
mod tests;
