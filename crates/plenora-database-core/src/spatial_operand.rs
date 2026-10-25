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
//!   colonna di sorgente derivata o di CTE seguita fino alla sua definizione
//!   in **ogni** ramo di set operation, un `*` espanso colonna per colonna, un
//!   nome di uscita (alias o nome di colonna) nominato da `ORDER BY`,
//!   `GROUP BY` o `HAVING`, in ogni ramo.
//!
//! `IS NULL`, `COUNT` e gli argomenti dei nodi spatial restano ammessi.
//!
//! # Mondo chiuso
//!
//! Con il catalogo ([`OperandCatalog`]) ogni riferimento in posizione di
//! operando — colonna, alias, colonna di uscita di una sorgente derivata o di
//! una CTE, nome generato dal server, colonna di un `*`, posizione di un ramo
//! di set operation — deve essere **provato** non geometrico: si risolve con
//! le regole di nome del dialetto ([`NameRule`]) fino a una colonna di base di
//! cui il catalogo dice il tipo, o fino a un'espressione che si giudica a sua
//! volta. Cio che non si risolve si rifiuta, `unsupported` con
//! [`GEOMETRY_OPERAND_UNRESOLVED`], quando il piano puo contenere una
//! geometria: una relazione nominata ha una colonna geometrica (o il catalogo
//! non ne legge le colonne), oppure il piano contiene un nodo spatial. Su un
//! piano senza geometrie un riferimento non risolto non puo essere
//! geometrico, e arriva al server come prima.
//!
//! Un'espressione senza alias che non e una colonna, nella projection di una
//! sorgente derivata o di una CTE, prende dal server un nome che il piano non
//! dice (`st_centroid`, `coalesce`, `case`, la colonna dentro un `CAST` su
//! `PostgreSQL`; il testo dell'espressione su `MySQL`). Un riferimento che non
//! trova un nome esplicito si giudica contro tutte queste espressioni.
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
//! Una visita sola, [`scalar_operands`]: senza catalogo decide la parte che
//! non ne ha bisogno (una funzione spatial che rende una geometria, un
//! `SpatialValue`), ed e quella che chiede il renderer relazionale per ogni
//! dialetto; elenca le relazioni di base nominate. Il provider ne legge le
//! colonne ([`RelationColumns`]) e la stessa visita le giudica
//! ([`ScalarOperands::judge`]). Una lettura (`ReadOperation`) nomina campi di
//! una sola relazione, gia descritta dal provider:
//! [`ensure_scalar_filter_fields`].
//!
//! # Limite dichiarato
//!
//! - **Regola.** Mondo chiuso sui riferimenti: senza la lista delle colonne di
//!   una relazione non si prova niente.
//! - **Ambito.** Il compilatore senza catalogo (SDK con tabelle non tipizzate,
//!   facade, CLI portable) non sa quali colonne sono geometriche: rifiuta le
//!   funzioni spatial e i `SpatialValue`, e le colonne soltanto dove il
//!   mapping ORM o la reflection le dichiarano `Geometry`
//!   ([`RelationColumns::Declared`]): una colonna non dichiarata di una
//!   relazione dichiarata si assume scalare, e un `*` di quella relazione in
//!   posizione di operando non si espande.
//! - **Hazard.** Su quel percorso `t.g = :p` su una colonna geometrica non
//!   dichiarata arriva al server come prima: `MySQL` e `MariaDB` rendono un
//!   risultato vuoto o pieno senza errore; `PostgreSQL`, SQL Server, Oracle
//!   e Db2 rifiutano il confronto fra un tipo geometrico e un binario o un
//!   testo, ma non e una garanzia di questo repository.
//! - **Rientro.** Quando il compilatore riceve i tipi delle colonne (un
//!   catalogo o un mapping per ogni relazione nominata), la stessa
//!   [`ScalarOperands::judge`] li giudica anche li.
//!
//! - **Regola.** Le regole di nome di [`NameRule::Folded`] confrontano senza
//!   maiuscole e senza spazi finali; le collation di SQL Server insensibili
//!   agli accenti o che espandono (`ß`/`ss`) non sono modellate.
//! - **Ambito.** SQL Server, `MySQL`, `MariaDB`.
//! - **Hazard.** Nessun passaggio silenzioso: un nome che la piegatura non
//!   trova non si risolve, e in un piano con geometrie si rifiuta; un nome
//!   senza qualificazione che esce da un blocco con colonne non ASCII per
//!   cercare in un blocco esterno non si risolve. Il costo e un rifiuto in
//!   piu.
//! - **Rientro.** Quando la piegatura segue la collation letta dal catalogo.

use crate::plan::{FilterExpression, OrderBy, ProviderKind};
use crate::relational::{
    ColumnRef, CommonTableExpression, QueryExpression, QueryOperation, QuerySetOperator,
    ScalarFunction,
};
use crate::spatial_probe::GEOMETRY_SCALAR_OPERAND;
use crate::{DatabaseError, ErrorPhase, Result};
use std::collections::{BTreeMap, BTreeSet};

/// Motivo stabile del rifiuto di un riferimento che non si prova non
/// geometrico, in un piano che puo contenere una geometria.
pub const GEOMETRY_OPERAND_UNRESOLVED: &str =
    "riferimento di colonna non risolto in un piano con geometrie: sotto un operatore non spatial ogni colonna, alias, colonna di sorgente derivata o di CTE, nome generato dal server, colonna di un * o di un ramo di set operation deve risolversi in una colonna non geometrica del catalogo; si nomini la colonna con un alias esplicito";

/// Profondita massima di un'espansione di `*` attraverso sorgenti derivate:
/// oltre, l'espansione non si conosce e il riferimento non si risolve.
const MAX_EXPANSION_DEPTH: usize = 64;

/// Come il dialetto confronta i nomi di colonne, alias e relazioni.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameRule {
    /// Il renderer quota ogni identificatore: il nome e quello esatto
    /// (`PostgreSQL`, Oracle, Db2).
    Exact,
    /// Senza maiuscole e senza spazi finali: SQL Server con le collation
    /// insensibili alle maiuscole, i nomi di colonna di `MySQL` e `MariaDB`.
    /// Due nomi uguali per la piegatura si giudicano entrambi.
    Folded,
}

impl NameRule {
    /// La regola del provider.
    #[must_use]
    pub const fn of(provider: ProviderKind) -> Self {
        match provider {
            ProviderKind::Sqlserver | ProviderKind::Mysql | ProviderKind::Mariadb => Self::Folded,
            _ => Self::Exact,
        }
    }

    /// I due nomi possono nominare la stessa colonna.
    #[must_use]
    pub fn matches(self, left: &str, right: &str) -> bool {
        match self {
            Self::Exact => left == right,
            Self::Folded => fold(left) == fold(right),
        }
    }
}

fn fold(name: &str) -> String {
    name.trim_end_matches(' ').to_lowercase()
}

/// Le colonne di una relazione di base, come le dice il catalogo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationColumns {
    /// Il catalogo non conosce la relazione: la query fallira sul server.
    Absent,
    /// La relazione esiste ma le sue colonne non si leggono (un sinonimo
    /// verso un altro database, un oggetto che il catalogo non descrive):
    /// nessun riferimento si risolve.
    Unknown,
    /// Tutte le colonne, nell'ordine del catalogo: nome, geometrica.
    Known(Vec<(String, bool)>),
    /// Soltanto le colonne dichiarate geometriche (mapping ORM, reflection):
    /// le altre si assumono scalari.
    Declared(Vec<String>),
}

impl RelationColumns {
    fn may_hold_geometry(&self) -> bool {
        match self {
            Self::Absent => false,
            Self::Unknown => true,
            Self::Known(columns) => columns.iter().any(|(_, geometric)| *geometric),
            Self::Declared(columns) => !columns.is_empty(),
        }
    }
}

/// Il catalogo delle relazioni di [`ScalarOperands::relations`].
#[derive(Debug, Clone)]
pub struct OperandCatalog {
    rule: NameRule,
    relations: BTreeMap<(Option<String>, String), RelationColumns>,
}

impl OperandCatalog {
    /// Un catalogo vuoto con le regole di nome del provider.
    #[must_use]
    pub const fn new(provider: ProviderKind) -> Self {
        Self {
            rule: NameRule::of(provider),
            relations: BTreeMap::new(),
        }
    }

    /// Le colonne della relazione (schema, oggetto) come il piano la nomina.
    pub fn insert(&mut self, schema: Option<String>, object: String, columns: RelationColumns) {
        self.relations.insert((schema, object), columns);
    }

    /// Una relazione che nessuno ha letto non si conosce.
    fn columns(&self, schema: Option<&String>, object: &str) -> &RelationColumns {
        self.relations
            .get(&(schema.cloned(), object.to_owned()))
            .unwrap_or(&RelationColumns::Unknown)
    }
}

/// Le relazioni di base di un piano e i suoi riferimenti in posizione di
/// operando, da giudicare sul catalogo.
#[derive(Debug, Clone)]
pub struct ScalarOperands {
    provider: ProviderKind,
    operation: QueryOperation,
    relations: Vec<(Option<String>, String)>,
    references: bool,
}

impl ScalarOperands {
    /// Le relazioni di base (schema, oggetto) nominate dal piano, CTE escluse:
    /// quelle di cui il provider legge le colonne.
    #[must_use]
    pub fn relations(&self) -> &[(Option<String>, String)] {
        &self.relations
    }

    /// Nessun riferimento in posizione di operando: il provider non ha
    /// niente da leggere.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        !self.references
    }

    /// Il giudizio sul catalogo, a mondo chiuso.
    ///
    /// # Errors
    ///
    /// `unsupported` ([`GEOMETRY_SCALAR_OPERAND`]) per un operando
    /// geometrico; `unsupported` ([`GEOMETRY_OPERAND_UNRESOLVED`]) per un
    /// riferimento non risolto in un piano che puo contenere una geometria.
    pub fn judge(&self, catalog: &OperandCatalog) -> Result<()> {
        walk(self.provider, &self.operation, Some(catalog)).map(|_| ())
    }
}

fn refusal(provider: ProviderKind) -> DatabaseError {
    DatabaseError::unsupported(provider, ErrorPhase::Prepare, GEOMETRY_SCALAR_OPERAND)
}

fn unresolved_refusal(provider: ProviderKind) -> DatabaseError {
    DatabaseError::unsupported(provider, ErrorPhase::Prepare, GEOMETRY_OPERAND_UNRESOLVED)
}

/// Il piano visitato senza catalogo: la parte statica decisa, le relazioni
/// e i riferimenti raccolti.
///
/// # Errors
///
/// `unsupported` ([`GEOMETRY_SCALAR_OPERAND`]) per una funzione spatial che
/// rende una geometria o un `SpatialValue` in posizione di operando: non
/// serve un catalogo per saperle geometriche.
pub fn scalar_operands(
    provider: ProviderKind,
    operation: &QueryOperation,
) -> Result<ScalarOperands> {
    let outcome = walk(provider, operation, None)?;
    Ok(ScalarOperands {
        provider,
        operation: operation.clone(),
        relations: outcome.relations,
        references: outcome.references,
    })
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

/// Che cosa una relazione della FROM puo essere.
#[derive(Clone)]
enum RelationKind<'a> {
    /// Una tabella o una vista del catalogo.
    Base(Option<String>, String),
    /// Una sorgente derivata o una CTE: la sua query, le CTE che vede, il
    /// blocco che la contiene se e `LATERAL`.
    Query(
        &'a QueryOperation,
        Vec<&'a CommonTableExpression>,
        Option<usize>,
    ),
}

/// Una relazione della FROM: il nome con cui il blocco la nomina e cio che
/// puo essere (una CTE omonima per la sola piegatura e anche la tabella).
struct FromRelation<'a> {
    name: String,
    kinds: Vec<RelationKind<'a>>,
}

/// Un blocco: la sua operazione, il blocco che lo contiene, le CTE visibili
/// (comprese le sue) e la FROM.
struct Block<'a> {
    operation: &'a QueryOperation,
    parent: Option<usize>,
    visible: Vec<&'a CommonTableExpression>,
    from: Vec<FromRelation<'a>>,
}

/// Da dove viene una colonna di uscita di un blocco.
#[derive(Clone)]
enum EntrySource<'a> {
    /// La projection in posizione `usize`.
    Projection(usize),
    /// Una colonna di una tabella di base nominata da un `*`, da risolvere
    /// nel blocco.
    Column(ColumnRef),
    /// La colonna di uscita in posizione `usize` di una sorgente derivata o
    /// di una CTE nominata da un `*`.
    Position(
        &'a QueryOperation,
        Vec<&'a CommonTableExpression>,
        Option<usize>,
        usize,
    ),
}

/// Una colonna di uscita: il nome (assente se lo genera il server) e
/// l'origine.
#[derive(Clone)]
struct Entry<'a> {
    name: Option<String>,
    source: EntrySource<'a>,
}

/// Le colonne di uscita di un blocco; `bool`: l'elenco e completo (ogni `*`
/// espanso).
type Entries<'a> = (Vec<Entry<'a>>, bool);

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
    /// Un riferimento di colonna in posizione di operando nel blocco.
    Reference(ColumnRef, usize),
    /// La colonna di uscita in posizione `usize` di una query, in ogni ramo
    /// di set operation, come operando.
    Position(
        &'a QueryOperation,
        Vec<&'a CommonTableExpression>,
        Option<usize>,
        usize,
    ),
}

/// Il giudizio di un riferimento.
enum Candidate<'a> {
    Geometry,
    Scalar,
    Unresolved,
    /// Si giudica l'item.
    Follow(Item<'a>),
}

struct Outcome {
    relations: Vec<(Option<String>, String)>,
    references: bool,
}

struct Walk<'a, 'c> {
    provider: ProviderKind,
    rule: NameRule,
    catalog: Option<&'c OperandCatalog>,
    blocks: Vec<Block<'a>>,
    opened: BTreeMap<(usize, Option<usize>), usize>,
    entries: BTreeMap<usize, Entries<'a>>,
    expanding: BTreeSet<usize>,
    followed: BTreeSet<(usize, Option<usize>, usize)>,
    aliased: BTreeSet<(usize, String)>,
    stack: Vec<Item<'a>>,
    relations: Vec<(Option<String>, String)>,
    references: bool,
    unresolved: bool,
    spatial: bool,
}

fn walk(
    provider: ProviderKind,
    operation: &QueryOperation,
    catalog: Option<&OperandCatalog>,
) -> Result<Outcome> {
    let mut state = Walk {
        provider,
        rule: catalog.map_or_else(|| NameRule::of(provider), |catalog| catalog.rule),
        catalog,
        blocks: Vec::new(),
        opened: BTreeMap::new(),
        entries: BTreeMap::new(),
        expanding: BTreeSet::new(),
        followed: BTreeSet::new(),
        aliased: BTreeSet::new(),
        stack: vec![Item::Operation(operation, Vec::new(), None, false)],
        relations: Vec::new(),
        references: false,
        unresolved: false,
        spatial: false,
    };
    while let Some(item) = state.stack.pop() {
        state.step(item)?;
    }
    if let Some(catalog) = catalog {
        let geometric = state.spatial
            || state.relations.iter().any(|(schema, object)| {
                catalog.columns(schema.as_ref(), object).may_hold_geometry()
            });
        if state.unresolved && geometric {
            return Err(unresolved_refusal(provider));
        }
    }
    Ok(Outcome {
        relations: state.relations,
        references: state.references,
    })
}

fn address(operation: &QueryOperation) -> usize {
    std::ptr::from_ref(operation) as usize
}

impl<'a> Walk<'a, '_> {
    fn step(&mut self, item: Item<'a>) -> Result<()> {
        match item {
            Item::Operation(operation, visible, parent, operand) => {
                self.visit_operation(operation, visible, parent, operand);
            }
            Item::Expression(expression, operand, index) => {
                self.visit_expression(expression, operand, index)?;
            }
            Item::Reference(column, index) => self.reference(&column, index)?,
            Item::Position(query, visible, parent, position) => {
                self.position(query, visible, parent, position)?;
            }
        }
        Ok(())
    }

    fn visit_operation(
        &mut self,
        operation: &'a QueryOperation,
        visible: Vec<&'a CommonTableExpression>,
        parent: Option<usize>,
        operand: bool,
    ) {
        let index = self.open_block(operation, visible, parent);
        let inner = self.blocks[index].visible.clone();
        for relation in &self.blocks[index].from {
            for kind in &relation.kinds {
                if let RelationKind::Base(schema, object) = kind {
                    let key = (schema.clone(), object.clone());
                    if !self.relations.contains(&key) {
                        self.relations.push(key);
                    }
                }
            }
        }
        for cte in &operation.common_table_expressions {
            self.stack
                .push(Item::Operation(&cte.query, inner.clone(), None, false));
        }
        if let Some(source) = &operation.derived_source {
            self.stack
                .push(Item::Operation(&source.query, inner.clone(), None, false));
        }
        for join in &operation.joins {
            if let Some(source) = &join.derived_source {
                let parent = join.lateral.then_some(index);
                self.stack
                    .push(Item::Operation(&source.query, inner.clone(), parent, false));
            }
        }
        // Una set operation che confronta righe confronta ogni colonna di
        // ogni ramo; `UNION ALL` le accoda soltanto.
        let comparing = operation
            .set_operations
            .iter()
            .any(|set| set.operator != QuerySetOperator::Union || !set.all);
        let projection_operand = operand || operation.distinct || comparing;
        for set in &operation.set_operations {
            self.stack.push(Item::Operation(
                &set.query,
                inner.clone(),
                parent,
                projection_operand,
            ));
        }
        for projection in &operation.projection {
            self.stack.push(Item::Expression(
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
            self.stack.push(Item::Expression(expression, true, index));
        }
        for predicate in operation
            .filter
            .iter()
            .chain(operation.joins.iter().filter_map(|join| join.on.as_ref()))
            .chain(operation.having.iter())
        {
            self.stack.push(Item::Expression(predicate, false, index));
        }
    }

    /// Il blocco di `operation` con `parent`, aperto una volta sola.
    fn open_block(
        &mut self,
        operation: &'a QueryOperation,
        visible: Vec<&'a CommonTableExpression>,
        parent: Option<usize>,
    ) -> usize {
        let key = (address(operation), parent);
        if let Some(index) = self.opened.get(&key) {
            return *index;
        }
        let mut inner = visible;
        inner.extend(operation.common_table_expressions.iter());
        let index = self.blocks.len();
        let mut from = Vec::new();
        let sources = operation
            .source
            .iter()
            .map(|source| (Some(source), None, false));
        let derived = operation
            .derived_source
            .iter()
            .map(|source| (None, Some(source), false));
        let joined = operation.joins.iter().map(|join| {
            (
                join.source.as_ref(),
                join.derived_source.as_ref(),
                join.lateral,
            )
        });
        for (source, derived, lateral) in sources.chain(derived).chain(joined) {
            if let Some(source) = source {
                from.push(self.named_source(source, &inner));
            }
            if let Some(derived) = derived {
                from.push(FromRelation {
                    name: derived.alias.clone(),
                    kinds: vec![RelationKind::Query(
                        &derived.query,
                        inner.clone(),
                        lateral.then_some(index),
                    )],
                });
            }
        }
        self.blocks.push(Block {
            operation,
            parent,
            visible: inner,
            from,
        });
        self.opened.insert(key, index);
        index
    }

    /// Una relazione nominata: una CTE visibile con lo stesso nome (la piu
    /// interna), la tabella del catalogo altrimenti; per la sola piegatura,
    /// entrambe.
    fn named_source(
        &self,
        source: &crate::relational::QuerySource,
        visible: &[&'a CommonTableExpression],
    ) -> FromRelation<'a> {
        let object = &source.object;
        let name = source
            .alias
            .clone()
            .unwrap_or_else(|| object.object.clone());
        let base = RelationKind::Base(object.schema.clone(), object.object.clone());
        if object.schema.is_some() {
            return FromRelation {
                name,
                kinds: vec![base],
            };
        }
        let cte = visible
            .iter()
            .rev()
            .find(|cte| self.rule.matches(&cte.name, &object.object));
        let kinds = match cte {
            Some(cte) => {
                let query = RelationKind::Query(&cte.query, visible.to_vec(), None);
                if cte.name == object.object {
                    vec![query]
                } else {
                    vec![query, base]
                }
            }
            None => vec![base],
        };
        FromRelation { name, kinds }
    }

    #[allow(clippy::too_many_lines)]
    fn visit_expression(
        &mut self,
        expression: &'a QueryExpression,
        operand: bool,
        index: usize,
    ) -> Result<()> {
        let provider = self.provider;
        let mut children: Vec<(&'a QueryExpression, bool)> = Vec::new();
        match expression {
            QueryExpression::Column { column } => {
                if operand {
                    self.stack.push(Item::Reference(column.clone(), index));
                }
            }
            QueryExpression::Wildcard { relation } => {
                if operand {
                    self.wildcard(relation.as_deref(), index)?;
                }
            }
            QueryExpression::Parameter { .. } | QueryExpression::TypedParameter { .. } => {}
            QueryExpression::SpatialValue { expression, .. } => {
                self.spatial = true;
                if operand {
                    return Err(refusal(provider));
                }
                children.push((expression, false));
            }
            QueryExpression::Spatial {
                function,
                arguments,
            } => {
                self.spatial = true;
                if operand && function.returns_geometry() {
                    return Err(refusal(provider));
                }
                children.extend(arguments.iter().map(|argument| (argument, false)));
            }
            QueryExpression::SpatialWindow {
                function,
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                self.spatial = true;
                if operand && function.returns_geometry() {
                    return Err(refusal(provider));
                }
                children.extend(arguments.iter().map(|argument| (argument, false)));
                children.extend(
                    partition_by
                        .iter()
                        .chain(order_by.iter().map(|order| &order.expression))
                        .map(|child| (child, true)),
                );
            }
            QueryExpression::SpatialOperator { left, right, .. } => {
                self.spatial = true;
                children.push((left, false));
                children.push((right, false));
            }
            QueryExpression::SpatialOutput { expression, .. } => {
                self.spatial = true;
                children.push((expression, operand));
            }
            QueryExpression::Cast { expression, .. }
            | QueryExpression::NullsOrder { expression, .. } => {
                children.push((expression, operand));
            }
            QueryExpression::Not { expression } | QueryExpression::IsNull { expression, .. } => {
                children.push((expression, false));
            }
            QueryExpression::Arithmetic { left, right, .. }
            | QueryExpression::Compare { left, right, .. } => {
                children.push((left, true));
                children.push((right, true));
            }
            QueryExpression::Case {
                branches,
                else_expression,
            } => {
                for branch in branches {
                    children.push((&branch.when, false));
                    children.push((&branch.then, operand));
                }
                if let Some(otherwise) = else_expression {
                    children.push((otherwise, operand));
                }
            }
            QueryExpression::Scalar {
                function,
                arguments,
            } => {
                let carried = argument_operand(*function, operand);
                children.extend(arguments.iter().map(|argument| (argument, carried)));
            }
            QueryExpression::Window {
                function,
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                let carried = argument_operand(*function, operand);
                children.extend(arguments.iter().map(|argument| (argument, carried)));
                children.extend(
                    partition_by
                        .iter()
                        .chain(order_by.iter().map(|order| &order.expression))
                        .map(|child| (child, true)),
                );
            }
            QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
                children.extend(arguments.iter().map(|argument| (argument, false)));
            }
            QueryExpression::InList {
                expression, values, ..
            } => {
                children.push((expression, true));
                children.extend(values.iter().map(|value| (value, true)));
            }
            QueryExpression::Between {
                expression,
                lower,
                upper,
                ..
            } => {
                for child in [expression, lower, upper] {
                    children.push((child, true));
                }
            }
            QueryExpression::Like {
                expression,
                pattern,
                ..
            } => {
                children.push((expression, true));
                children.push((pattern, true));
            }
            QueryExpression::ScalarSubquery { query } => {
                let visible = self.blocks[index].visible.clone();
                self.stack
                    .push(Item::Operation(query, visible, Some(index), operand));
            }
            QueryExpression::Exists { query, .. } => {
                let visible = self.blocks[index].visible.clone();
                self.stack
                    .push(Item::Operation(query, visible, Some(index), false));
            }
            QueryExpression::InSubquery {
                expression, query, ..
            } => {
                children.push((expression, true));
                let visible = self.blocks[index].visible.clone();
                self.stack
                    .push(Item::Operation(query, visible, Some(index), true));
            }
        }
        for (child, operand) in children {
            self.stack.push(Item::Expression(child, operand, index));
        }
        Ok(())
    }

    fn apply(&mut self, candidates: Vec<Candidate<'a>>) -> Result<()> {
        if candidates.is_empty() {
            self.unresolved = true;
        }
        for candidate in candidates {
            match candidate {
                Candidate::Geometry => return Err(refusal(self.provider)),
                Candidate::Scalar => {}
                Candidate::Unresolved => self.unresolved = true,
                Candidate::Follow(item) => self.stack.push(item),
            }
        }
        Ok(())
    }

    /// Un riferimento di colonna in posizione di operando: le colonne che
    /// puo nominare nella FROM del blocco (o, correlato, di un blocco
    /// esterno), e i nomi di uscita del blocco stesso (`ORDER BY`,
    /// `GROUP BY`, `HAVING` per alias o per nome), in ogni ramo di set
    /// operation.
    fn reference(&mut self, column: &ColumnRef, index: usize) -> Result<()> {
        self.references = true;
        let mut candidates = match &column.relation {
            Some(relation) => self.qualified(relation, &column.field, index),
            None => self.unqualified(&column.field, index),
        };
        let own = self.output_positions(index, column);
        let key = (index, fold(&column.field));
        if !own.is_empty() && self.aliased.insert(key) {
            let block = &self.blocks[index];
            let (operation, visible, parent) =
                (block.operation, block.visible.clone(), block.parent);
            for position in own {
                candidates.push(Candidate::Follow(Item::Position(
                    operation,
                    visible.clone(),
                    parent,
                    position,
                )));
            }
        } else if !own.is_empty() {
            candidates.push(Candidate::Scalar);
        }
        self.apply(candidates)
    }

    /// Le posizioni delle colonne di uscita del blocco che `column` nomina:
    /// per nome se non e qualificata, per la stessa colonna della projection
    /// se lo e.
    fn output_positions(&mut self, index: usize, column: &ColumnRef) -> Vec<usize> {
        let (entries, _) = self.entries_of(index, 0);
        let operation = self.blocks[index].operation;
        let rule = self.rule;
        entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                column.relation.as_deref().map_or_else(
                    || {
                        entry
                            .name
                            .as_deref()
                            .is_some_and(|name| rule.matches(name, &column.field))
                    },
                    |relation| {
                        !operation.set_operations.is_empty()
                            && matches!(
                                entry.source,
                                EntrySource::Projection(position)
                                    if matches!(
                                        &operation.projection[position].expression,
                                        QueryExpression::Column { column: own }
                                            if own.relation.as_deref().is_some_and(|own| rule.matches(own, relation))
                                                && rule.matches(&own.field, &column.field)
                                    )
                            )
                    },
                )
            })
            .map(|(position, _)| position)
            .collect()
    }

    /// `relation.field`: la relazione piu interna con quel nome.
    fn qualified(&mut self, relation: &str, field: &str, index: usize) -> Vec<Candidate<'a>> {
        let mut current = Some(index);
        while let Some(block) = current {
            let named = self.blocks[block]
                .from
                .iter()
                .filter(|from| self.rule.matches(&from.name, relation))
                .flat_map(|from| from.kinds.clone())
                .collect::<Vec<_>>();
            if !named.is_empty() {
                let mut candidates = Vec::new();
                for kind in named {
                    candidates.extend(self.in_relation(&kind, field).0);
                }
                if candidates.is_empty() {
                    candidates.push(Candidate::Unresolved);
                }
                return candidates;
            }
            current = self.blocks[block].parent;
        }
        vec![Candidate::Unresolved]
    }

    /// `field` senza qualificazione: ogni relazione della FROM che la puo
    /// avere, nel blocco piu interno che ne ha almeno una.
    fn unqualified(&mut self, field: &str, index: usize) -> Vec<Candidate<'a>> {
        let mut candidates = Vec::new();
        let mut current = Some(index);
        while let Some(block) = current {
            let kinds = self.blocks[block]
                .from
                .iter()
                .flat_map(|from| from.kinds.clone())
                .collect::<Vec<_>>();
            let mut stops = false;
            let mut non_ascii = false;
            for kind in kinds {
                let (found, definite, foreign) = self.in_relation(&kind, field);
                stops |= definite;
                non_ascii |= foreign;
                candidates.extend(found);
            }
            if stops {
                return candidates;
            }
            if non_ascii && self.rule == NameRule::Folded {
                // La collation puo uguagliare `field` a una colonna non ASCII
                // di questo blocco che la piegatura non uguaglia.
                candidates.push(Candidate::Unresolved);
                return candidates;
            }
            current = self.blocks[block].parent;
        }
        candidates
    }

    /// Le colonne di `kind` che `field` puo nominare; `bool`: la relazione ha
    /// la colonna (o non si sa, e il riferimento non si risolve), e la
    /// ricerca si ferma; `bool`: la relazione ha colonne con nomi non ASCII.
    fn in_relation(
        &mut self,
        kind: &RelationKind<'a>,
        field: &str,
    ) -> (Vec<Candidate<'a>>, bool, bool) {
        match kind {
            RelationKind::Base(schema, object) => {
                let Some(catalog) = self.catalog else {
                    return (vec![Candidate::Scalar], true, false);
                };
                match catalog.columns(schema.as_ref(), object) {
                    RelationColumns::Absent => (Vec::new(), false, false),
                    RelationColumns::Unknown => (vec![Candidate::Unresolved], true, false),
                    RelationColumns::Declared(geometric) => {
                        if geometric.iter().any(|name| self.rule.matches(name, field)) {
                            (vec![Candidate::Geometry], true, false)
                        } else {
                            (vec![Candidate::Scalar], false, false)
                        }
                    }
                    RelationColumns::Known(columns) => {
                        let foreign = columns.iter().any(|(name, _)| !name.is_ascii());
                        let matching = columns
                            .iter()
                            .filter(|(name, _)| self.rule.matches(name, field))
                            .collect::<Vec<_>>();
                        if matching.is_empty() {
                            (Vec::new(), false, foreign)
                        } else if matching.iter().any(|(_, geometric)| *geometric) {
                            (vec![Candidate::Geometry], true, foreign)
                        } else {
                            (vec![Candidate::Scalar], true, foreign)
                        }
                    }
                }
            }
            RelationKind::Query(query, visible, parent) => {
                let candidates = self.output_by_name(query, visible, *parent, field);
                let definite = !candidates.is_empty();
                (candidates, definite, false)
            }
        }
    }

    /// Le colonne di uscita di `query` che `field` nomina: per nome
    /// esplicito; senza un nome esplicito, ogni colonna il cui nome lo
    /// genera il server; con un `*` non espanso, nessuna prova.
    fn output_by_name(
        &mut self,
        query: &'a QueryOperation,
        visible: &[&'a CommonTableExpression],
        parent: Option<usize>,
        field: &str,
    ) -> Vec<Candidate<'a>> {
        let index = self.open_block(query, visible.to_vec(), parent);
        let (entries, complete) = self.entries_of(index, 0);
        let rule = self.rule;
        let mut positions = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                entry
                    .name
                    .as_deref()
                    .is_some_and(|name| rule.matches(name, field))
            })
            .map(|(position, _)| position)
            .collect::<Vec<_>>();
        if positions.is_empty() {
            positions = entries
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.name.is_none())
                .map(|(position, _)| position)
                .collect();
        }
        let mut candidates = positions
            .into_iter()
            .map(|position| {
                Candidate::Follow(Item::Position(query, visible.to_vec(), parent, position))
            })
            .collect::<Vec<_>>();
        if !complete {
            candidates.push(Candidate::Unresolved);
        }
        candidates
    }

    /// Le colonne di uscita del blocco, con ogni `*` espanso dove il
    /// catalogo lo permette.
    fn entries_of(&mut self, index: usize, depth: usize) -> Entries<'a> {
        if let Some(entries) = self.entries.get(&index) {
            return entries.clone();
        }
        if depth > MAX_EXPANSION_DEPTH || self.expanding.contains(&index) {
            // Una CTE ricorsiva che espande se stessa, o una catena troppo
            // lunga: l'elenco non si conosce.
            return (Vec::new(), false);
        }
        self.expanding.insert(index);
        let operation = self.blocks[index].operation;
        let mut entries = Vec::new();
        let mut complete = true;
        'projection: for (position, item) in operation.projection.iter().enumerate() {
            let QueryExpression::Wildcard { relation } = &item.expression else {
                let name = item.alias.clone().or_else(|| match &item.expression {
                    QueryExpression::Column { column } => Some(column.field.clone()),
                    _ => None,
                });
                entries.push(Entry {
                    name,
                    source: EntrySource::Projection(position),
                });
                continue;
            };
            let from = self.relations_named(index, relation.as_deref());
            if from.is_empty() {
                complete = false;
                break;
            }
            for (name, kinds) in from {
                let [kind] = kinds.as_slice() else {
                    complete = false;
                    break 'projection;
                };
                match kind {
                    RelationKind::Base(schema, object) => {
                        let columns = self
                            .catalog
                            .map(|catalog| catalog.columns(schema.as_ref(), object));
                        let Some(RelationColumns::Known(columns)) = columns else {
                            complete = false;
                            break 'projection;
                        };
                        for (field, _) in columns {
                            entries.push(Entry {
                                name: Some(field.clone()),
                                source: EntrySource::Column(ColumnRef {
                                    relation: Some(name.clone()),
                                    field: field.clone(),
                                }),
                            });
                        }
                    }
                    RelationKind::Query(query, visible, parent) => {
                        let inner = self.open_block(query, visible.clone(), *parent);
                        let (nested, nested_complete) = self.entries_of(inner, depth + 1);
                        for (position, entry) in nested.into_iter().enumerate() {
                            entries.push(Entry {
                                name: entry.name,
                                source: EntrySource::Position(
                                    query,
                                    visible.clone(),
                                    *parent,
                                    position,
                                ),
                            });
                        }
                        if !nested_complete {
                            complete = false;
                            break 'projection;
                        }
                    }
                }
            }
        }
        self.expanding.remove(&index);
        let result = (entries, complete);
        self.entries.insert(index, result.clone());
        result
    }

    /// Le relazioni della FROM del blocco che un `*` nomina.
    fn relations_named(
        &self,
        index: usize,
        relation: Option<&str>,
    ) -> Vec<(String, Vec<RelationKind<'a>>)> {
        self.blocks[index]
            .from
            .iter()
            .filter(|from| relation.is_none_or(|relation| self.rule.matches(&from.name, relation)))
            .map(|from| (from.name.clone(), from.kinds.clone()))
            .collect()
    }

    /// Un `*` in posizione di operando (`DISTINCT *`, la projection di una
    /// subquery `IN` o di una set operation che confronta righe): ogni
    /// colonna che espande e un operando.
    fn wildcard(&mut self, relation: Option<&str>, index: usize) -> Result<()> {
        self.references = true;
        let from = self.relations_named(index, relation);
        let mut candidates = Vec::new();
        if from.is_empty() {
            candidates.push(Candidate::Unresolved);
        }
        for (name, kinds) in from {
            for kind in kinds {
                match kind {
                    RelationKind::Base(schema, object) => {
                        let Some(catalog) = self.catalog else {
                            continue;
                        };
                        match catalog.columns(schema.as_ref(), &object) {
                            RelationColumns::Known(columns) => {
                                for (field, _) in columns {
                                    candidates.push(Candidate::Follow(Item::Reference(
                                        ColumnRef {
                                            relation: Some(name.clone()),
                                            field: field.clone(),
                                        },
                                        index,
                                    )));
                                }
                            }
                            RelationColumns::Declared(geometric) if geometric.is_empty() => {
                                candidates.push(Candidate::Scalar);
                            }
                            RelationColumns::Declared(_) => candidates.push(Candidate::Geometry),
                            RelationColumns::Absent | RelationColumns::Unknown => {
                                candidates.push(Candidate::Unresolved);
                            }
                        }
                    }
                    RelationKind::Query(query, visible, parent) => {
                        let inner = self.open_block(query, visible.clone(), parent);
                        let (entries, complete) = self.entries_of(inner, 0);
                        for position in 0..entries.len() {
                            candidates.push(Candidate::Follow(Item::Position(
                                query,
                                visible.clone(),
                                parent,
                                position,
                            )));
                        }
                        if !complete {
                            candidates.push(Candidate::Unresolved);
                        }
                    }
                }
            }
        }
        if candidates.is_empty() {
            candidates.push(Candidate::Scalar);
        }
        self.apply(candidates)
    }

    /// La colonna di uscita in posizione `position` di `query`, in ogni ramo
    /// di set operation: la sua espressione e un operando.
    fn position(
        &mut self,
        query: &'a QueryOperation,
        visible: Vec<&'a CommonTableExpression>,
        parent: Option<usize>,
        position: usize,
    ) -> Result<()> {
        // Una CTE ricorsiva si nomina da se: la stessa colonna dello stesso
        // blocco si segue una volta sola.
        if !self.followed.insert((address(query), parent, position)) {
            return Ok(());
        }
        let first = self.open_block(query, visible, parent);
        let mut branches = vec![first];
        let mut cursor = 0;
        while let Some(branch) = branches.get(cursor).copied() {
            let block = &self.blocks[branch];
            let (operation, inner) = (block.operation, block.visible.clone());
            for set in &operation.set_operations {
                let index = self.open_block(&set.query, inner.clone(), parent);
                branches.push(index);
            }
            cursor += 1;
        }
        let mut candidates = Vec::new();
        for branch in branches {
            let (entries, _) = self.entries_of(branch, 0);
            let Some(entry) = entries.get(position) else {
                // Un `*` non espanso prima della posizione, o un ramo con meno
                // colonne: la colonna del ramo non si conosce.
                candidates.push(Candidate::Unresolved);
                continue;
            };
            let operation = self.blocks[branch].operation;
            candidates.push(Candidate::Follow(match &entry.source {
                EntrySource::Projection(item) => {
                    Item::Expression(&operation.projection[*item].expression, true, branch)
                }
                EntrySource::Column(column) => Item::Reference(column.clone(), branch),
                EntrySource::Position(query, visible, parent, position) => {
                    Item::Position(query, visible.clone(), *parent, *position)
                }
            }));
        }
        self.apply(candidates)
    }
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

/// La stessa regola per una lettura (`ReadOperation`).
///
/// I campi di `filter` sotto un operatore non spatial e quelli di `order_by`
/// non sono geometrici. `geometric` dice se un campo della relazione letta lo
/// e, dalla descrizione che il provider ha gia letto, con le regole di nome
/// del dialetto ([`NameRule`]).
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
