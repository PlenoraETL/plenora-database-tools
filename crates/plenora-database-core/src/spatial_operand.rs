//! Un operando geometrico sta soltanto dentro un nodo spatial.
//!
//! # La regola
//!
//! Un operando di tipo geometrico — una colonna che il catalogo del provider
//! (o il mapping ORM) dice geometrica, una funzione spatial che rende una
//! geometria, un `SpatialValue` — puo comparire soltanto come argomento di un
//! nodo spatial. Sotto un operatore che non e spatial si rifiuta prima
//! dell'esecuzione, `unsupported` con
//! [`GEOMETRY_SCALAR_OPERAND`](crate::spatial_probe::GEOMETRY_SCALAR_OPERAND):
//!
//! - confronti (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN` (lista e subquery,
//!   anche la projection della subquery), `BETWEEN`, `LIKE`;
//! - aritmetica, `MIN`/`MAX`/`SUM`/`AVG`, `LAG`/`LEAD`;
//! - `ORDER BY`, `GROUP BY`, `DISTINCT`, `DISTINCT ON`, `PARTITION BY` e
//!   `ORDER BY` delle finestre, le set operation che confrontano righe
//!   (tutte tranne `UNION ALL`);
//! - attraverso cio che conserva il valore: `CAST`, `LOWER`/`UPPER`,
//!   `COALESCE`, i rami `THEN`/`ELSE` di un `CASE`, una subquery scalare, una
//!   colonna di sorgente derivata o di CTE seguita fino alla sua definizione,
//!   un alias della projection nominato da `ORDER BY` o `GROUP BY`.
//!
//! `IS NULL`, `COUNT` e gli argomenti dei nodi spatial restano ammessi.
//!
//! # Perche
//!
//! Il server confronta il proprio formato interno con il valore legato.
//! Misurato su `MySQL` 8 e `MariaDB` 11.4 (25/10, prima di questa regola):
//! `t.g = CAST(? AS BINARY)` con un WKB rendeva zero righe, `<>` una, `IN`
//! zero, `ORDER BY t.g` ordinava per byte, senza errore, con byte WKB, testo
//! WKT o il formato interno `SRID + WKB`. Nessuna forma di quei confronti ha
//! la semantica di un'uguaglianza fra geometrie.
//!
//! # Chi decide
//!
//! La parte che non dipende dal catalogo — una funzione spatial che rende
//! una geometria, un `SpatialValue` — la decide [`scalar_operands`] da sola,
//! e la chiede il renderer relazionale per ogni dialetto. Le colonne non
//! portano un tipo nel piano: [`scalar_operands`] le raccoglie, risolte fino
//! alla tabella di base dove la FROM lo permette, e il provider le giudica
//! sul catalogo ([`ScalarOperands::judge`]). Una lettura (`ReadOperation`)
//! nomina campi di una sola relazione, gia descritta dal provider:
//! [`ensure_scalar_filter_fields`].
//!
//! # Limite dichiarato
//!
//! - **Regola.** Una colonna che non si risolve in una tabella di base (senza
//!   qualificazione in un blocco annidato o in una FROM con piu relazioni, un
//!   riferimento correlato, un `SELECT *` di una relazione non di base) si
//!   giudica per **nome**: e geometrica se una relazione di base nominata dal
//!   piano ha una colonna geometrica con quel nome. Puo rifiutare una colonna
//!   scalare omonima di una geometrica di un'altra tabella dello stesso
//!   piano; non puo lasciarne passare una geometrica.
//! - **Ambito.** Il compilatore senza catalogo (SDK con tabelle non tipizzate,
//!   facade, CLI portable) non sa quali colonne sono geometriche: rifiuta le
//!   funzioni spatial e i `SpatialValue`, e le colonne soltanto dove il
//!   mapping ORM o la reflection le dichiarano `Geometry`.
//! - **Hazard.** Su quel percorso `t.g = :p` su una colonna geometrica non
//!   dichiarata arriva al server come prima: `MySQL` e `MariaDB` rendono un
//!   risultato vuoto o pieno senza errore; `PostgreSQL`, SQL Server, Oracle
//!   e Db2 rifiutano il confronto fra un tipo geometrico e un binario o un
//!   testo, ma non e una garanzia di questo repository.
//! - **Rientro.** Quando il compilatore riceve i tipi delle colonne (un
//!   catalogo o un mapping per ogni relazione nominata), la stessa
//!   [`ScalarOperands::judge`] li giudica anche li.

use crate::plan::{FilterExpression, OrderBy, ProviderKind};
use crate::relational::{
    ColumnRef, CommonTableExpression, QueryExpression, QueryOperation, QuerySetOperator,
    ScalarFunction,
};
use crate::spatial_probe::{named_relation, BaseColumn, RelationScope, GEOMETRY_SCALAR_OPERAND};
use crate::{DatabaseError, ErrorPhase, Result};
use std::collections::BTreeSet;

/// Le colonne che un piano usa come operandi di operatori non spatial.
#[derive(Debug, Clone, Default)]
pub struct ScalarOperands {
    columns: Vec<BaseColumn>,
    names: BTreeSet<String>,
    relations: Vec<(Option<String>, String)>,
}

impl ScalarOperands {
    /// Le colonne di base risolte senza ambiguita.
    #[must_use]
    pub fn columns(&self) -> &[BaseColumn] {
        &self.columns
    }

    /// I nomi delle colonne che non si risolvono in una tabella di base: si
    /// giudicano per nome contro tutte le [`Self::relations`].
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }

    /// Le relazioni di base (schema, oggetto) nominate dal piano, CTE escluse:
    /// quelle di cui il provider legge le colonne geometriche.
    #[must_use]
    pub fn relations(&self) -> &[(Option<String>, String)] {
        &self.relations
    }

    /// Nessuna colonna da giudicare: il provider non ha niente da leggere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.names.is_empty()
    }

    /// Il giudizio sul catalogo: `geometric` dice se una colonna di base e
    /// geometrica, `geometric_name` se una relazione di [`Self::relations`]
    /// ha una colonna geometrica con quel nome.
    ///
    /// # Errors
    ///
    /// `unsupported` ([`GEOMETRY_SCALAR_OPERAND`]) per la prima colonna
    /// geometrica.
    pub fn judge(
        &self,
        provider: ProviderKind,
        geometric: impl Fn(&BaseColumn) -> bool,
        geometric_name: impl Fn(&str) -> bool,
    ) -> Result<()> {
        if self.columns.iter().any(geometric) || self.names().any(geometric_name) {
            return Err(refusal(provider));
        }
        Ok(())
    }
}

fn refusal(provider: ProviderKind) -> DatabaseError {
    DatabaseError::unsupported(provider, ErrorPhase::Prepare, GEOMETRY_SCALAR_OPERAND)
}

/// Un blocco: lo scope della FROM, il blocco che lo contiene, l'operazione e
/// le CTE visibili (comprese le sue).
struct Block<'a> {
    scope: RelationScope,
    parent: Option<usize>,
    operation: &'a QueryOperation,
    visible: Vec<&'a CommonTableExpression>,
}

enum Item<'a> {
    /// Un blocco da visitare per intero; `bool`: la sua projection e un
    /// operando (subquery scalare o `IN`, `DISTINCT`, set operation).
    Operation(
        &'a QueryOperation,
        Vec<&'a CommonTableExpression>,
        Option<usize>,
        bool,
    ),
    /// Un'espressione del blocco `usize`; `bool`: e un operando.
    Expression(&'a QueryExpression, bool, usize),
    /// Una colonna di uscita di una sorgente derivata o di una CTE, seguita
    /// fino alla sua definizione.
    Output(&'a QueryOperation, Vec<&'a CommonTableExpression>, String),
    /// La colonna in posizione `usize` della projection di un ramo di set
    /// operation, come operando nel blocco del ramo.
    Projection(&'a QueryOperation, Vec<&'a CommonTableExpression>, usize),
    /// Una colonna nominata da un `SELECT *`, come operando del blocco.
    Column(ColumnRef, usize),
}

/// Il piano visitato: la parte statica decisa, le colonne raccolte.
///
/// # Errors
///
/// `unsupported` ([`GEOMETRY_SCALAR_OPERAND`]) per una funzione spatial che
/// rende una geometria o un `SpatialValue` in posizione di operando: non
/// serve un catalogo per saperle geometriche.
#[allow(clippy::too_many_lines)]
pub fn scalar_operands(
    provider: ProviderKind,
    operation: &QueryOperation,
) -> Result<ScalarOperands> {
    let mut found = ScalarOperands::default();
    let mut blocks: Vec<Block<'_>> = Vec::new();
    let mut followed: BTreeSet<(usize, String)> = BTreeSet::new();
    let mut aliased: BTreeSet<(usize, String)> = BTreeSet::new();
    let mut stack = vec![Item::Operation(operation, Vec::new(), None, false)];
    while let Some(item) = stack.pop() {
        match item {
            Item::Operation(operation, visible, parent, operand) => {
                let index = open_block(&mut blocks, operation, visible, parent);
                let inner = blocks[index].visible.clone();
                let names = inner
                    .iter()
                    .map(|cte| cte.name.as_str())
                    .collect::<BTreeSet<_>>();
                for source in operation.source.iter().chain(
                    operation
                        .joins
                        .iter()
                        .filter_map(|join| join.source.as_ref()),
                ) {
                    let common = source.object.schema.is_none()
                        && names.contains(source.object.object.as_str());
                    let relation = (source.object.schema.clone(), source.object.object.clone());
                    if !common && !found.relations.contains(&relation) {
                        found.relations.push(relation);
                    }
                }
                for cte in &operation.common_table_expressions {
                    stack.push(Item::Operation(&cte.query, inner.clone(), None, false));
                }
                if let Some(source) = &operation.derived_source {
                    stack.push(Item::Operation(&source.query, inner.clone(), None, false));
                }
                for join in &operation.joins {
                    if let Some(source) = &join.derived_source {
                        let parent = join.lateral.then_some(index);
                        stack.push(Item::Operation(&source.query, inner.clone(), parent, false));
                    }
                }
                // Una set operation che confronta righe confronta ogni colonna
                // di ogni ramo; `UNION ALL` le accoda soltanto.
                let comparing = operation
                    .set_operations
                    .iter()
                    .any(|set| set.operator != QuerySetOperator::Union || !set.all);
                let projection_operand = operand || operation.distinct || comparing;
                for set in &operation.set_operations {
                    stack.push(Item::Operation(
                        &set.query,
                        inner.clone(),
                        parent,
                        projection_operand,
                    ));
                }
                for projection in &operation.projection {
                    stack.push(Item::Expression(
                        &projection.expression,
                        projection_operand,
                        index,
                    ));
                }
                let ordering = operation
                    .distinct_on
                    .iter()
                    .chain(&operation.group_by)
                    .chain(operation.order_by.iter().map(|order| &order.expression));
                for expression in ordering {
                    stack.push(Item::Expression(expression, true, index));
                }
                for predicate in operation
                    .filter
                    .iter()
                    .chain(operation.joins.iter().filter_map(|join| join.on.as_ref()))
                    .chain(operation.having.iter())
                {
                    stack.push(Item::Expression(predicate, false, index));
                }
            }
            Item::Output(query, visible, field) => {
                let index = open_block(&mut blocks, query, visible, None);
                follow_output(&blocks, index, &field, &mut followed, &mut stack);
            }
            Item::Projection(branch, visible, position) => {
                let index = open_block(&mut blocks, branch, visible, None);
                if let Some(item) = branch.projection.get(position) {
                    stack.push(Item::Expression(&item.expression, true, index));
                }
            }
            Item::Column(column, index) => {
                // Un alias della projection nominato senza qualificazione
                // (`ORDER BY a`, `GROUP BY a`, `HAVING a = ?`) e l'espressione
                // che nomina; una volta sola per blocco, perche due alias
                // possono nominarsi a vicenda.
                let operation = blocks[index].operation;
                let key = (std::ptr::from_ref(operation) as usize, column.field.clone());
                if column.relation.is_none() && aliased.insert(key) {
                    for projection in &operation.projection {
                        if projection.alias.as_deref() == Some(column.field.as_str()) {
                            stack.push(Item::Expression(&projection.expression, true, index));
                        }
                    }
                }
                resolve(&column, index, &blocks, &mut found, &mut stack);
            }
            Item::Expression(expression, operand, index) => {
                visit_expression(provider, expression, operand, index, &blocks, &mut stack)?;
            }
        }
    }
    Ok(found)
}

/// La stessa visita per il predicato di una mutazione (`UPDATE ... WHERE`,
/// `DELETE ... WHERE`): ogni colonna e del target.
///
/// # Errors
///
/// Quelli di [`scalar_operands`].
pub fn scalar_operands_of_predicate(
    provider: ProviderKind,
    target: &crate::plan::ObjectRef,
    predicate: &QueryExpression,
) -> Result<ScalarOperands> {
    let operation = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(crate::relational::QuerySource {
            object: target.clone(),
            alias: None,
        }),
        derived_source: None,
        projection: Vec::new(),
        joins: Vec::new(),
        filter: Some(predicate.clone()),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    };
    scalar_operands(provider, &operation)
}

fn open_block<'a>(
    blocks: &mut Vec<Block<'a>>,
    operation: &'a QueryOperation,
    visible: Vec<&'a CommonTableExpression>,
    parent: Option<usize>,
) -> usize {
    let names = visible
        .iter()
        .map(|cte| cte.name.clone())
        .collect::<BTreeSet<_>>();
    let scope = RelationScope::of(operation, &names);
    let mut inner = visible;
    inner.extend(operation.common_table_expressions.iter());
    blocks.push(Block {
        scope,
        parent,
        operation,
        visible: inner,
    });
    blocks.len() - 1
}

/// La colonna di uscita `field` del blocco `index`, in ogni ramo di set
/// operation: la sua espressione e un operando. Un `SELECT *` la nomina come
/// colonna del blocco.
fn follow_output<'a>(
    blocks: &[Block<'a>],
    index: usize,
    field: &str,
    followed: &mut BTreeSet<(usize, String)>,
    stack: &mut Vec<Item<'a>>,
) {
    let operation = blocks[index].operation;
    // Una CTE ricorsiva si nomina da se: la stessa colonna dello stesso
    // blocco si segue una volta sola.
    let key = (std::ptr::from_ref(operation) as usize, field.to_owned());
    if !followed.insert(key) {
        return;
    }
    let position = operation.projection.iter().position(|item| {
        item.alias.as_ref().map_or_else(
            || matches!(&item.expression, QueryExpression::Column { column } if column.field == field),
            |alias| alias == field,
        )
    });
    let Some(position) = position else {
        for item in &operation.projection {
            if let QueryExpression::Wildcard { relation } = &item.expression {
                stack.push(Item::Column(
                    ColumnRef {
                        relation: relation.clone(),
                        field: field.to_owned(),
                    },
                    index,
                ));
            }
        }
        return;
    };
    let mut branches = vec![operation];
    let mut cursor = 0;
    while let Some(branch) = branches.get(cursor).copied() {
        branches.extend(branch.set_operations.iter().map(|set| set.query.as_ref()));
        cursor += 1;
    }
    for branch in branches {
        if std::ptr::eq(branch, operation) {
            if let Some(item) = branch.projection.get(position) {
                stack.push(Item::Expression(&item.expression, true, index));
            }
        } else {
            stack.push(Item::Projection(
                branch,
                blocks[index].visible.clone(),
                position,
            ));
        }
    }
}

#[allow(clippy::too_many_lines)]
fn visit_expression<'a>(
    provider: ProviderKind,
    expression: &'a QueryExpression,
    operand: bool,
    index: usize,
    blocks: &[Block<'a>],
    stack: &mut Vec<Item<'a>>,
) -> Result<()> {
    let mut push = |child: &'a QueryExpression, operand: bool| {
        stack.push(Item::Expression(child, operand, index));
    };
    match expression {
        QueryExpression::Column { column } => {
            if operand {
                stack.push(Item::Column(column.clone(), index));
            }
        }
        QueryExpression::Wildcard { .. }
        | QueryExpression::Parameter { .. }
        | QueryExpression::TypedParameter { .. } => {}
        QueryExpression::SpatialValue { expression, .. } => {
            if operand {
                return Err(refusal(provider));
            }
            push(expression, false);
        }
        QueryExpression::Spatial {
            function,
            arguments,
        } => {
            if operand && function.returns_geometry() {
                return Err(refusal(provider));
            }
            for argument in arguments {
                push(argument, false);
            }
        }
        QueryExpression::SpatialWindow {
            function,
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            if operand && function.returns_geometry() {
                return Err(refusal(provider));
            }
            for argument in arguments {
                push(argument, false);
            }
            for child in partition_by
                .iter()
                .chain(order_by.iter().map(|order| &order.expression))
            {
                push(child, true);
            }
        }
        QueryExpression::SpatialOperator { left, right, .. } => {
            push(left, false);
            push(right, false);
        }
        QueryExpression::SpatialOutput { expression, .. }
        | QueryExpression::Cast { expression, .. }
        | QueryExpression::NullsOrder { expression, .. } => push(expression, operand),
        QueryExpression::Not { expression } | QueryExpression::IsNull { expression, .. } => {
            push(expression, false);
        }
        QueryExpression::Arithmetic { left, right, .. }
        | QueryExpression::Compare { left, right, .. } => {
            push(left, true);
            push(right, true);
        }
        QueryExpression::Case {
            branches,
            else_expression,
        } => {
            for branch in branches {
                push(&branch.when, false);
                push(&branch.then, operand);
            }
            if let Some(otherwise) = else_expression {
                push(otherwise, operand);
            }
        }
        QueryExpression::Scalar {
            function,
            arguments,
        } => {
            let carried = argument_operand(*function, operand);
            for argument in arguments {
                push(argument, carried);
            }
        }
        QueryExpression::Window {
            function,
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            let carried = argument_operand(*function, operand);
            for argument in arguments {
                push(argument, carried);
            }
            for child in partition_by
                .iter()
                .chain(order_by.iter().map(|order| &order.expression))
            {
                push(child, true);
            }
        }
        QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
            for argument in arguments {
                push(argument, false);
            }
        }
        QueryExpression::InList {
            expression, values, ..
        } => {
            push(expression, true);
            for value in values {
                push(value, true);
            }
        }
        QueryExpression::Between {
            expression,
            lower,
            upper,
            ..
        } => {
            for child in [expression, lower, upper] {
                push(child, true);
            }
        }
        QueryExpression::Like {
            expression,
            pattern,
            ..
        } => {
            push(expression, true);
            push(pattern, true);
        }
        QueryExpression::ScalarSubquery { query } => {
            stack.push(Item::Operation(
                query,
                blocks[index].visible.clone(),
                Some(index),
                operand,
            ));
        }
        QueryExpression::Exists { query, .. } => {
            stack.push(Item::Operation(
                query,
                blocks[index].visible.clone(),
                Some(index),
                false,
            ));
        }
        QueryExpression::InSubquery {
            expression, query, ..
        } => {
            push(expression, true);
            stack.push(Item::Operation(
                query,
                blocks[index].visible.clone(),
                Some(index),
                true,
            ));
        }
    }
    Ok(())
}

/// Gli argomenti di una funzione scalare sono operandi se la funzione li
/// confronta o li calcola (`MIN`, `MAX`, `SUM`, `AVG`), o se ne rende il
/// valore a un operando (`LOWER`, `UPPER`, `COALESCE`, `LAG`, `LEAD`).
/// `COUNT` li conta soltanto.
const fn argument_operand(function: ScalarFunction, operand: bool) -> bool {
    match function {
        ScalarFunction::Minimum
        | ScalarFunction::Maximum
        | ScalarFunction::Sum
        | ScalarFunction::Average => true,
        ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::Coalesce
        | ScalarFunction::Lag
        | ScalarFunction::Lead => operand,
        ScalarFunction::Count
        | ScalarFunction::RowNumber
        | ScalarFunction::Rank
        | ScalarFunction::DenseRank => false,
    }
}

/// Una colonna in posizione di operando: la colonna di base, se si risolve
/// senza ambiguita; la definizione, se e di una sorgente derivata o di una
/// CTE; il nome, altrimenti.
fn resolve<'a>(
    column: &ColumnRef,
    index: usize,
    blocks: &[Block<'a>],
    found: &mut ScalarOperands,
    stack: &mut Vec<Item<'a>>,
) {
    let field = column.field.clone();
    let Some(relation) = &column.relation else {
        // Senza qualificazione la colonna puo essere di ogni relazione della
        // FROM, e in un blocco annidato di ogni blocco esterno: si seguono
        // tutte le sorgenti derivate e le CTE della catena, e si giudica il
        // nome; la colonna di base, soltanto nel blocco radice con una sola
        // tabella.
        let mut current = Some(index);
        while let Some(block) = current {
            for (query, visible) in relations_to_follow(&blocks[block], None) {
                stack.push(Item::Output(query, visible, field.clone()));
            }
            current = blocks[block].parent;
        }
        match blocks[index].scope.base_column(column) {
            Some(base) if blocks[index].parent.is_none() => push_column(found, base),
            _ => {
                found.names.insert(field);
            }
        }
        return;
    };
    let mut current = Some(index);
    while let Some(block) = current {
        if blocks[block].scope.names(relation) {
            if let Some(base) = blocks[block].scope.base_column(column) {
                push_column(found, base);
            } else {
                let follow = relations_to_follow(&blocks[block], Some(relation));
                if follow.is_empty() {
                    found.names.insert(field.clone());
                }
                for (query, visible) in follow {
                    stack.push(Item::Output(query, visible, field.clone()));
                }
            }
            return;
        }
        current = blocks[block].parent;
    }
    found.names.insert(field);
}

fn push_column(found: &mut ScalarOperands, column: BaseColumn) {
    if !found.columns.contains(&column) {
        found.columns.push(column);
    }
}

/// Le sorgenti derivate e le CTE della FROM di `block` che `relation` nomina
/// (tutte, senza qualificazione), con le CTE visibili da chi le definisce.
fn relations_to_follow<'a>(
    block: &Block<'a>,
    relation: Option<&str>,
) -> Vec<(&'a QueryOperation, Vec<&'a CommonTableExpression>)> {
    let operation = block.operation;
    if let Some(relation) = relation {
        return named_relation(operation, &block.visible, Some(relation))
            .map(|(query, _)| (query, block.visible.clone()))
            .into_iter()
            .collect();
    }
    let mut follow = Vec::new();
    for source in operation.derived_source.iter().chain(
        operation
            .joins
            .iter()
            .filter_map(|join| join.derived_source.as_ref()),
    ) {
        follow.push((source.query.as_ref(), block.visible.clone()));
    }
    for source in operation.source.iter().chain(
        operation
            .joins
            .iter()
            .filter_map(|join| join.source.as_ref()),
    ) {
        if source.object.schema.is_some() {
            continue;
        }
        if let Some(cte) = block
            .visible
            .iter()
            .rev()
            .find(|cte| cte.name == source.object.object)
        {
            follow.push((cte.query.as_ref(), block.visible.clone()));
        }
    }
    follow
}

/// La stessa regola per una lettura (`ReadOperation`).
///
/// I campi di `filter` sotto un operatore non spatial e quelli di `order_by`
/// non sono geometrici. `geometric` dice se un campo della relazione letta lo
/// e, dalla descrizione che il provider ha gia letto.
///
/// # Errors
///
/// `unsupported` ([`GEOMETRY_SCALAR_OPERAND`]) per il primo campo
/// geometrico.
pub fn ensure_scalar_filter_fields(
    provider: ProviderKind,
    filter: Option<&FilterExpression>,
    order_by: &[OrderBy],
    geometric: impl Fn(&str) -> bool,
) -> Result<()> {
    if order_by.iter().any(|order| geometric(&order.field)) {
        return Err(refusal(provider));
    }
    let mut stack = filter.into_iter().collect::<Vec<_>>();
    while let Some(expression) = stack.pop() {
        let fields: Vec<&str> = match expression {
            FilterExpression::And { args } | FilterExpression::Or { args } => {
                stack.extend(args);
                continue;
            }
            FilterExpression::IsNull { .. }
            | FilterExpression::IsNotNull { .. }
            | FilterExpression::Spatial { .. } => continue,
            FilterExpression::Eq { field, .. }
            | FilterExpression::Ne { field, .. }
            | FilterExpression::Lt { field, .. }
            | FilterExpression::Lte { field, .. }
            | FilterExpression::Gt { field, .. }
            | FilterExpression::Gte { field, .. }
            | FilterExpression::In { field, .. }
            | FilterExpression::Between { field, .. }
            | FilterExpression::Like { field, .. } => vec![field],
        };
        if fields.into_iter().any(&geometric) {
            return Err(refusal(provider));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "spatial_operand_tests.rs"]
mod tests;
