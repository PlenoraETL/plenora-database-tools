//! Adapter dal DML relazionale canonico al lowering `portable`.

use crate::plan::{ComparisonOperator, ProviderKind};
use crate::portable::{
    DeleteStatement, Expression, InsertStatement, PortableStatement, Predicate, TableRef,
    UpdateStatement, UpsertStatement,
};
use crate::provider::ParameterValue;
use crate::relational::{
    DeleteOperation, InsertOperation, MutationOperation, QueryExpression, QueryParameterType,
    UpdateOperation, UpsertOperation,
};
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};

/// Statement DML abbassato, con layout dei bind ma senza valori applicativi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredMutation {
    pub sql: String,
    pub bind_names: Vec<String>,
    pub returns_rows: bool,
}

/// Compila il DML canonico attraverso il lowering `portable` qualificato.
///
/// I `ParameterValue` costruiti qui sono sentinelle che servono soltanto a far
/// emettere i placeholder. Non provengono dal chiamante e vengono scartati;
/// l'esecuzione ordina i valori veri tramite `bind_names`.
///
/// # Errors
///
/// Rifiuta forme non ancora rappresentabili dall'adapter o dal provider.
pub fn compile_relational_mutation(
    provider: ProviderKind,
    operation: &MutationOperation,
) -> Result<LoweredMutation> {
    let mut binds = Binds::sentinels();
    let (statement, returns_rows) = lower(operation, &mut binds)?;
    let bind_names = binds.names;
    let lowered = crate::portable::compile_portable(provider, &statement)?;
    if lowered.params.len() != bind_names.len() {
        return Err(DatabaseError::new(
            ErrorCategory::Internal,
            ErrorPhase::Prepare,
            Some(provider),
            "layout bind DML incoerente dopo il lowering",
        ));
    }
    Ok(LoweredMutation {
        sql: lowered.sql,
        bind_names,
        returns_rows,
    })
}

/// I segnaposto del lowering: sentinelle per lo SQL, oppure i valori veri del
/// chiamante nell'ordine dei bind, per l'ammissione
/// ([`crate::admission_gate::mutation`]).
pub(crate) struct Binds<'v> {
    pub(crate) names: Vec<String>,
    values: Option<&'v [ParameterValue]>,
    /// Le posizioni dei valori spatial: restano sentinelle nel lowering (il
    /// compilatore giudicherebbe lo SRID del letterale) e il cancello rimette
    /// il valore del chiamante dopo la compilazione.
    pub(crate) spatial: Vec<usize>,
    /// Le posizioni dei parametri tipizzati confrontati, con il loro tipo:
    /// il compilatore li rende `CAST(? AS tipo)` con una sentinella (regola
    /// T), e il cancello vi lega il valore del chiamante ammesso sul tipo
    /// reso ([`crate::cast_table::rendered_target`]).
    pub(crate) typed: Vec<(usize, QueryParameterType)>,
}

impl<'v> Binds<'v> {
    const fn sentinels() -> Self {
        Self {
            names: Vec::new(),
            values: None,
            spatial: Vec::new(),
            typed: Vec::new(),
        }
    }

    pub(crate) const fn values(values: &'v [ParameterValue]) -> Self {
        Self {
            names: Vec::new(),
            values: Some(values),
            spatial: Vec::new(),
            typed: Vec::new(),
        }
    }

    fn scalar(
        &mut self,
        name: &str,
        parameter_type: Option<QueryParameterType>,
    ) -> Result<Expression> {
        let position = self.names.len();
        self.names.push(name.to_owned());
        let value = match self.values {
            Some(values) => values.get(position).cloned().ok_or_else(|| {
                DatabaseError::invalid_plan("valori DML meno dei segnaposto del piano")
            })?,
            None if parameter_type == Some(QueryParameterType::TimestampTz) => {
                ParameterValue::TimestampTz(String::new())
            }
            None => ParameterValue::I64(0),
        };
        Ok(Expression::Literal(value))
    }

    /// Un parametro tipizzato in una posizione confrontata: il segnaposto
    /// del `CAST` reso, senza letterale.
    fn typed(&mut self, name: &str, parameter_type: QueryParameterType) -> Expression {
        self.typed.push((self.names.len(), parameter_type));
        self.names.push(name.to_owned());
        Expression::TypedParameter(parameter_type)
    }

    fn spatial(&mut self, name: &str) {
        self.spatial.push(self.names.len());
        self.names.push(name.to_owned());
    }
}

/// Il DML abbassato nell'AST portable, con i segnaposto di `binds`.
pub(crate) fn lower(
    operation: &MutationOperation,
    binds: &mut Binds<'_>,
) -> Result<(PortableStatement, bool)> {
    Ok(match operation {
        MutationOperation::Insert(insert) => (
            PortableStatement::Insert(insert_statement(insert, binds)?),
            !insert.returning.is_empty(),
        ),
        MutationOperation::Update(update) => (
            PortableStatement::Update(update_statement(update, binds)?),
            !update.returning.is_empty(),
        ),
        MutationOperation::Delete(delete) => (
            PortableStatement::Delete(delete_statement(delete, binds)?),
            !delete.returning.is_empty(),
        ),
        MutationOperation::Upsert(upsert) => (
            PortableStatement::Upsert(upsert_statement(upsert, binds)?),
            !upsert.returning.is_empty(),
        ),
    })
}

fn upsert_statement(operation: &UpsertOperation, binds: &mut Binds<'_>) -> Result<UpsertStatement> {
    let values = operation
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|expression| insert_value(expression, binds))
                .collect()
        })
        .collect::<Result<Vec<_>>>()?;
    let update_on_conflict = operation
        .update_on_conflict
        .iter()
        .map(|assignment| {
            Ok((
                assignment.column.clone(),
                value(&assignment.value, &operation.target, binds, false)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(UpsertStatement {
        table: table(&operation.target)?,
        columns: operation.columns.clone(),
        values,
        conflict_target: operation.conflict_target.clone(),
        update_on_conflict,
        returning: operation.returning.clone(),
    })
}

fn table(target: &crate::plan::ObjectRef) -> Result<TableRef> {
    if target.catalog.is_some() {
        return Err(DatabaseError::invalid_plan(
            "target DML cross-catalog non supportato",
        ));
    }
    Ok(TableRef {
        schema: target.schema.clone(),
        name: target.object.clone(),
    })
}

fn insert_statement(operation: &InsertOperation, binds: &mut Binds<'_>) -> Result<InsertStatement> {
    let values = operation
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|expression| insert_value(expression, binds))
                .collect()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(InsertStatement {
        table: table(&operation.target)?,
        columns: operation.columns.clone(),
        values,
        returning: operation.returning.clone(),
    })
}

fn update_statement(operation: &UpdateOperation, binds: &mut Binds<'_>) -> Result<UpdateStatement> {
    let assignments = operation
        .assignments
        .iter()
        .map(|assignment| {
            Ok((
                assignment.column.clone(),
                value(&assignment.value, &operation.target, binds, false)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = operation
        .filter
        .as_ref()
        .map(|expression| predicate(expression, &operation.target, binds))
        .transpose()?;
    Ok(UpdateStatement {
        table: table(&operation.target)?,
        assignments,
        filter,
        returning: operation.returning.clone(),
    })
}

fn delete_statement(operation: &DeleteOperation, binds: &mut Binds<'_>) -> Result<DeleteStatement> {
    let filter = operation
        .filter
        .as_ref()
        .map(|expression| predicate(expression, &operation.target, binds))
        .transpose()?;
    Ok(DeleteStatement {
        table: table(&operation.target)?,
        filter,
        returning: operation.returning.clone(),
    })
}

fn column(expression: &QueryExpression, target: &crate::plan::ObjectRef) -> Result<String> {
    let QueryExpression::Column { column } = expression else {
        return Err(DatabaseError::invalid_plan(
            "predicato DML richiede una colonna a sinistra",
        ));
    };
    if column
        .relation
        .as_ref()
        .is_some_and(|relation| relation != &target.object)
    {
        return Err(DatabaseError::invalid_plan(
            "predicato DML riferito a una relazione diversa dal target",
        ));
    }
    Ok(column.field.clone())
}

fn insert_value(expression: &QueryExpression, binds: &mut Binds<'_>) -> Result<Expression> {
    match expression {
        QueryExpression::Parameter { name } => parameter(name, None, binds),
        QueryExpression::TypedParameter {
            name,
            parameter_type,
        } => parameter(name, Some(*parameter_type), binds),
        QueryExpression::SpatialValue {
            expression,
            srid,
            semantics,
        } => spatial_value(expression, *srid, *semantics, binds),
        _ => Err(DatabaseError::invalid_plan("valore INSERT richiede bind()")),
    }
}

/// Un valore di un'assegnazione (`compared` falso) o di un predicato
/// (`compared` vero). Un parametro tipizzato confrontato si rende come lo
/// rende il renderer delle query (`CastDialect::render`, regola T); scritto,
/// con il solo segnaposto. Fino alla quattordicesima lettura del 25/10 il
/// predicato rendeva il segnaposto nudo, e il server convertiva il valore
/// legato verso la colonna.
fn value(
    expression: &QueryExpression,
    target: &crate::plan::ObjectRef,
    binds: &mut Binds<'_>,
    compared: bool,
) -> Result<Expression> {
    match expression {
        QueryExpression::Parameter { name } => parameter(name, None, binds),
        QueryExpression::TypedParameter {
            name,
            parameter_type,
        } if compared => Ok(binds.typed(name, *parameter_type)),
        QueryExpression::TypedParameter {
            name,
            parameter_type,
        } => parameter(name, Some(*parameter_type), binds),
        QueryExpression::Column { .. } => Ok(Expression::Column(column(expression, target)?)),
        QueryExpression::SpatialValue {
            expression,
            srid,
            semantics,
        } => spatial_value(expression, *srid, *semantics, binds),
        _ => Err(DatabaseError::invalid_plan(
            "valore DML richiede bind() o Column",
        )),
    }
}

fn parameter(
    name: &str,
    parameter_type: Option<QueryParameterType>,
    binds: &mut Binds<'_>,
) -> Result<Expression> {
    binds.scalar(name, parameter_type)
}

fn spatial_value(
    expression: &QueryExpression,
    srid: u32,
    semantics: crate::geometry::SpatialSemantics,
    binds: &mut Binds<'_>,
) -> Result<Expression> {
    if srid == 0 {
        return Err(DatabaseError::invalid_plan(
            "valore spatial richiede SRID positivo",
        ));
    }
    let (QueryExpression::Parameter { name } | QueryExpression::TypedParameter { name, .. }) =
        expression
    else {
        return Err(DatabaseError::invalid_plan(
            "valore spatial richiede un bind",
        ));
    };
    binds.spatial(name);
    // Sentinella non geometrica: il compilatore portable giudica lo SRID di
    // un letterale geometrico, e questo non e un valore del chiamante.
    Ok(Expression::SpatialValue {
        expression: Box::new(Expression::Literal(ParameterValue::Null {
            type_name: "bytea".to_owned(),
        })),
        srid,
        semantics,
    })
}

fn predicate(
    expression: &QueryExpression,
    target: &crate::plan::ObjectRef,
    binds: &mut Binds<'_>,
) -> Result<Predicate> {
    match expression {
        QueryExpression::Compare {
            left,
            operator,
            right,
        } => {
            let column = column(left, target)?;
            let value = value(right, target, binds, true)?;
            Ok(match operator {
                ComparisonOperator::Eq => Predicate::Eq { column, value },
                ComparisonOperator::Ne => Predicate::Ne { column, value },
                ComparisonOperator::Lt => Predicate::Lt { column, value },
                ComparisonOperator::Lte => Predicate::Lte { column, value },
                ComparisonOperator::Gt => Predicate::Gt { column, value },
                ComparisonOperator::Gte => Predicate::Gte { column, value },
            })
        }
        QueryExpression::InList {
            expression,
            values,
            negated,
        } => {
            let predicate = Predicate::In {
                column: column(expression, target)?,
                values: values
                    .iter()
                    .map(|expression| value(expression, target, binds, true))
                    .collect::<Result<Vec<_>>>()?,
            };
            Ok(negate(predicate, *negated))
        }
        QueryExpression::Between {
            expression,
            lower,
            upper,
            negated,
        } => {
            let predicate = Predicate::Between {
                column: column(expression, target)?,
                low: value(lower, target, binds, true)?,
                high: value(upper, target, binds, true)?,
            };
            Ok(negate(predicate, *negated))
        }
        QueryExpression::Like {
            expression,
            pattern,
            case_insensitive,
            negated,
        } => {
            if *case_insensitive {
                return Err(DatabaseError::invalid_plan(
                    "ILIKE DML non ancora portabile",
                ));
            }
            let predicate = Predicate::Like {
                column: column(expression, target)?,
                pattern: value(pattern, target, binds, true)?,
            };
            Ok(negate(predicate, *negated))
        }
        QueryExpression::IsNull {
            expression,
            negated,
        } => Ok(if *negated {
            Predicate::IsNotNull {
                column: column(expression, target)?,
            }
        } else {
            Predicate::IsNull {
                column: column(expression, target)?,
            }
        }),
        QueryExpression::And { arguments } => Ok(Predicate::And {
            predicates: arguments
                .iter()
                .map(|argument| predicate(argument, target, binds))
                .collect::<Result<Vec<_>>>()?,
        }),
        QueryExpression::Or { arguments } => Ok(Predicate::Or {
            predicates: arguments
                .iter()
                .map(|argument| predicate(argument, target, binds))
                .collect::<Result<Vec<_>>>()?,
        }),
        QueryExpression::Not { expression } => Ok(Predicate::Not {
            predicate: Box::new(predicate(expression, target, binds)?),
        }),
        _ => Err(DatabaseError::invalid_plan(
            "predicato DML non ancora supportato",
        )),
    }
}

fn negate(predicate: Predicate, negated: bool) -> Predicate {
    if negated {
        Predicate::Not {
            predicate: Box::new(predicate),
        }
    } else {
        predicate
    }
}

#[cfg(test)]
#[path = "relational_mutation_tests.rs"]
mod tests;
