//! Adapter temporaneo dal DML relazionale canonico al lowering `portable`.

use crate::plan::{ComparisonOperator, ProviderKind};
use crate::portable::{
    DeleteStatement, Expression, InsertStatement, PortableStatement, Predicate, TableRef,
    UpdateStatement, UpsertStatement,
};
use crate::provider::ParameterValue;
use crate::relational::{
    DeleteOperation, InsertOperation, MutationOperation, QueryExpression, UpdateOperation,
    UpsertOperation,
};
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};

/// Statement DML abbassato, con layout dei bind ma senza valori applicativi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredMutation {
    pub sql: String,
    pub bind_names: Vec<String>,
    pub returns_rows: bool,
}

/// Compila il DML canonico riusando temporaneamente il lowering qualificato.
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
    let mut bind_names = Vec::new();
    let (statement, returns_rows) = match operation {
        MutationOperation::Insert(insert) => (
            PortableStatement::Insert(insert_statement(insert, &mut bind_names)?),
            !insert.returning.is_empty(),
        ),
        MutationOperation::Update(update) => (
            PortableStatement::Update(update_statement(update, &mut bind_names)?),
            !update.returning.is_empty(),
        ),
        MutationOperation::Delete(delete) => (
            PortableStatement::Delete(delete_statement(delete, &mut bind_names)?),
            !delete.returning.is_empty(),
        ),
        MutationOperation::Upsert(upsert) => (
            PortableStatement::Upsert(upsert_statement(upsert, &mut bind_names)?),
            !upsert.returning.is_empty(),
        ),
    };
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

fn upsert_statement(
    operation: &UpsertOperation,
    bind_names: &mut Vec<String>,
) -> Result<UpsertStatement> {
    let values = operation
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|expression| insert_value(expression, bind_names))
                .collect()
        })
        .collect::<Result<Vec<_>>>()?;
    let update_on_conflict = operation
        .update_on_conflict
        .iter()
        .map(|assignment| {
            Ok((
                assignment.column.clone(),
                value(&assignment.value, &operation.target, bind_names)?,
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

fn insert_statement(
    operation: &InsertOperation,
    bind_names: &mut Vec<String>,
) -> Result<InsertStatement> {
    let values = operation
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|expression| insert_value(expression, bind_names))
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

fn update_statement(
    operation: &UpdateOperation,
    bind_names: &mut Vec<String>,
) -> Result<UpdateStatement> {
    let assignments = operation
        .assignments
        .iter()
        .map(|assignment| {
            Ok((
                assignment.column.clone(),
                value(&assignment.value, &operation.target, bind_names)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = operation
        .filter
        .as_ref()
        .map(|expression| predicate(expression, &operation.target, bind_names))
        .transpose()?;
    Ok(UpdateStatement {
        table: table(&operation.target)?,
        assignments,
        filter,
        returning: operation.returning.clone(),
    })
}

fn delete_statement(
    operation: &DeleteOperation,
    bind_names: &mut Vec<String>,
) -> Result<DeleteStatement> {
    let filter = operation
        .filter
        .as_ref()
        .map(|expression| predicate(expression, &operation.target, bind_names))
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

fn insert_value(expression: &QueryExpression, bind_names: &mut Vec<String>) -> Result<Expression> {
    let (QueryExpression::Parameter { name } | QueryExpression::TypedParameter { name, .. }) =
        expression
    else {
        return Err(DatabaseError::invalid_plan("valore INSERT richiede bind()"));
    };
    bind_names.push(name.clone());
    Ok(Expression::Literal(ParameterValue::I64(0)))
}

fn value(
    expression: &QueryExpression,
    target: &crate::plan::ObjectRef,
    bind_names: &mut Vec<String>,
) -> Result<Expression> {
    match expression {
        QueryExpression::Parameter { name } | QueryExpression::TypedParameter { name, .. } => {
            bind_names.push(name.clone());
            Ok(Expression::Literal(ParameterValue::I64(0)))
        }
        QueryExpression::Column { .. } => Ok(Expression::Column(column(expression, target)?)),
        _ => Err(DatabaseError::invalid_plan(
            "valore DML richiede bind() o Column",
        )),
    }
}

fn predicate(
    expression: &QueryExpression,
    target: &crate::plan::ObjectRef,
    bind_names: &mut Vec<String>,
) -> Result<Predicate> {
    match expression {
        QueryExpression::Compare {
            left,
            operator,
            right,
        } => {
            let column = column(left, target)?;
            let value = value(right, target, bind_names)?;
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
                    .map(|expression| value(expression, target, bind_names))
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
                low: value(lower, target, bind_names)?,
                high: value(upper, target, bind_names)?,
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
                pattern: value(pattern, target, bind_names)?,
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
                .map(|argument| predicate(argument, target, bind_names))
                .collect::<Result<Vec<_>>>()?,
        }),
        QueryExpression::Or { arguments } => Ok(Predicate::Or {
            predicates: arguments
                .iter()
                .map(|argument| predicate(argument, target, bind_names))
                .collect::<Result<Vec<_>>>()?,
        }),
        QueryExpression::Not { expression } => Ok(Predicate::Not {
            predicate: Box::new(predicate(expression, target, bind_names)?),
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
