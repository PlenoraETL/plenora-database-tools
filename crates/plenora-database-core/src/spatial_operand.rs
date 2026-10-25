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
//! di set operation — deve essere **provato** non geometrico: si risolve fino
//! a una colonna di base di cui il catalogo dice il tipo, o fino a
//! un'espressione che si giudica a sua volta. Cio che non si risolve si
//! rifiuta, `unsupported` con [`GEOMETRY_OPERAND_UNRESOLVED`], quando il piano
//! puo contenere una geometria: una relazione nominata ha una colonna
//! geometrica (o il catalogo non ne legge le colonne), oppure il piano
//! contiene un nodo spatial. Su un piano senza geometrie un riferimento non
//! risolto non puo essere geometrico, e arriva al server come prima.
//!
//! Un'espressione senza alias che non e una colonna, nella projection di una
//! sorgente derivata o di una CTE, prende dal server un nome che il piano non
//! dice (`st_centroid`, `coalesce`, `case`, la colonna dentro un `CAST` su
//! `PostgreSQL`; il testo dell'espressione su `MySQL`). Un riferimento che non
//! trova un nome esplicito si giudica contro tutte queste espressioni.
//!
//! # Regola N: forme di nomi chiuse
//!
//! Il giudizio non emula il legame dei nomi di ogni dialetto: rifiuta le forme
//! in cui il legame dipende dal dialetto, `unsupported` con
//! [`AMBIGUOUS_NAME_SHAPE`], su ogni dialetto, **soltanto in un piano che puo
//! contenere una geometria** (lo stesso criterio del mondo chiuso: una
//! relazione con una colonna geometrica o con colonne che il catalogo non
//! legge, un nodo spatial; rendendo, senza catalogo, soltanto il nodo
//! spatial). Su un piano senza geometrie il legame di un nome non puo far
//! confrontare una geometria, e le forme arrivano al server come prima:
//!
//! - (a) due nomi di relazione (alias o nome) visibili insieme — nello stesso
//!   blocco o in blocchi annidati — uguali senza maiuscole e senza spazi
//!   finali, anche uguali per byte; due CTE visibili uguali allo stesso modo;
//!   un qualificatore o una relazione senza schema che incontra un nome
//!   soltanto senza maiuscole; un corpo di CTE che nomina una CTE successiva
//!   dello stesso `WITH`, o se stessa senza `recursive` (il dialetto la lega
//!   alla tabella o la rifiuta);
//! - (b) due nomi di uscita della stessa projection uguali senza maiuscole
//!   (alias, o il nome di una colonna senza alias);
//! - (c) un riferimento di colonna senza qualificatore in un blocco annidato
//!   (subquery, `LATERAL`) quando un blocco esterno ha relazioni;
//! - (d) sul catalogo, un nome di colonna o di uscita che incontra il nome
//!   nominato soltanto senza maiuscole dove il dialetto le distingue o puo
//!   distinguerle ([`NameRule::Exact`]: `PostgreSQL`, Oracle, Db2, SQL Server
//!   con qualunque collation);
//! - (e) sul catalogo, in un piano con geometrie su SQL Server, `MySQL` e
//!   `MariaDB`, un nome non ASCII (relazione, CTE, uscita, colonna): la
//!   collation puo uguagliare nomi che la piegatura distingue.
//!
//! Con queste forme rifiutate il legame di ogni nome e lo stesso sotto ogni
//! regola, e il giudizio risolve una volta sola: relazioni e alias per byte,
//! una CTE con le CTE che vede dove e definita, un riferimento senza
//! qualificatore nel solo blocco che lo contiene, i nomi di colonna con la
//! regola del dialetto ([`NameRule`]) soltanto contro il catalogo e i nomi di
//! uscita. Una relazione nominata con il catalogo (un altro database) non si
//! conosce: i suoi riferimenti non si risolvono, e il piano puo contenere una
//! geometria.
//!
//! # Regola S: l'esecuzione giudica sul catalogo
//!
//! Lo SDK, l'ORM, la facade portable e la CLI portable eseguono SQL
//! compilato. Prima di eseguirlo passano il piano ([`OperandPlan`]) alla
//! transazione, che legge dal catalogo del provider le colonne di ogni
//! relazione nominata (`TransactionScope::operand_catalog`) e lo giudica con
//! la stessa [`ScalarOperands::judge`] dei provider
//! (`facade::verify_operands`): le colonne non dichiarate o non mappate si
//! giudicano dal catalogo, non dal mapping del chiamante. Un provider che non
//! legge il catalogo rende ogni relazione sconosciuta: un riferimento in
//! posizione di operando su una relazione si rifiuta; un piano che non nomina
//! relazioni resta. Il nativo dello SDK rende lo SQL dal piano e lo confronta
//! con quello ricevuto: uno SQL diverso si rifiuta.
//!
//! Oracle non legge il catalogo: la guardia del server
//! ([`operand_server_guard`], misurata) rifiuta ogni operatore scalare su
//! `MDSYS.SDO_GEOMETRY`, e l'esecuzione giudica soltanto la parte senza
//! catalogo, che rifiuta cio che la guardia non protegge. Db2 non legge il
//! catalogo e non ha guardia (misurato: `SYSIBM.ST_GEOMETRY` e un tipo
//! distinto su `BLOB`, e il server ne confronta i byte): mondo chiuso.
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
//! non ne ha bisogno (la regola N in un piano con un nodo spatial, una
//! funzione spatial che rende una geometria, un `SpatialValue`, il binario
//! canonico di una geometria, `SpatialOutput`, sotto un operatore), ed e quella che chiede il renderer
//! relazionale per ogni dialetto; elenca le relazioni di base nominate. Il
//! provider, o la transazione per lo SQL compilato, ne legge le colonne
//! ([`RelationColumns`]) e la stessa visita le giudica
//! ([`ScalarOperands::judge`]). Una lettura (`ReadOperation`) nomina campi di
//! una sola relazione, gia descritta dal provider:
//! [`ensure_scalar_filter_fields`].
//!
//! # Limiti dichiarati
//!
//! - **Regola.** La guardia del server sugli operandi scalari di Oracle
//!   ([`operand_server_guard`]) vale per le forme misurate
//!   (`tests/fixtures/operandi_server_guard.json`, misurate da
//!   `scripts/sonde_operandi_server.py` e dal live test Oracle), non per
//!   ogni forma che il piano puo esprimere.
//! - **Ambito.** Oracle, sullo SDK, la facade e la CLI portable: le colonne
//!   non si giudicano sul catalogo.
//! - **Hazard.** Una forma non misurata in cui il server convertisse la
//!   geometria prima dell'operatore confronterebbe senza errore; su
//!   un'immagine diversa da quella qualificata il tipo potrebbe avere un
//!   `MAP`/`ORDER`. Un operatore su una colonna di una sorgente derivata che
//!   la query non legge mai passa senza errore (misurato: il server elimina
//!   la colonna prima di controllarne il tipo); nessuna riga ne dipende.
//! - **Rientro.** Quando Oracle legge il catalogo nella transazione; fino ad
//!   allora ogni forma nuova del renderer entra nel fixture e si misura.
//!
//! - **Regola.** Mondo chiuso: senza le colonne di una relazione non si prova
//!   niente.
//! - **Ambito.** Un provider senza `operand_catalog` e senza guardia: Db2
//!   (misurato senza guardia: `=`, `<>`, `<`, `IN`, `BETWEEN`, `CAST`,
//!   `COALESCE` su `SYSIBM.ST_GEOMETRY` passano e confrontano i byte del
//!   `BLOB`), sullo SDK, la facade e la CLI portable; i finti di prova.
//! - **Hazard.** Nessun passaggio silenzioso: un riferimento in posizione di
//!   operando su una relazione che puo contenere una geometria si rifiuta,
//!   anche su colonne scalari (`WHERE id = ?`, `ORDER BY id`). Il costo e il
//!   rifiuto di query ordinarie su Db2.
//! - **Rientro.** Quando Db2 legge il catalogo nella transazione
//!   (`TransactionScope::operand_catalog`), con le sue prove live.
//!
//! - **Regola.** La compilazione senza esecuzione non legge il catalogo.
//! - **Ambito.** `compile_relational_query` e `compile_relational_mutation`
//!   dello SDK, `portable-compile` della CLI, il renderer relazionale.
//! - **Hazard.** Lo SQL reso ha avuto il rifiuto delle geometrie che il
//!   piano dice da se (funzioni spatial, `SpatialValue`, `SpatialOutput`, la
//!   regola N con un nodo spatial, le colonne dichiarate `Geometry` dallo
//!   SDK), non il giudizio sulle colonne:
//!   chi lo esegue fuori da questo repository esegue SQL del chiamante.
//! - **Rientro.** Nessuno: chi esegue passa dall'esecuzione, che giudica.
//!
//! - **Regola.** Il catalogo si legge prima di eseguire, con un'istruzione
//!   separata.
//! - **Ambito.** Ogni provider, nel path query e nella transazione.
//! - **Hazard.** Un `ALTER TABLE` concorrente fra la lettura e l'esecuzione
//!   che rende geometrica una colonna giudicata scalare: la lettura non
//!   prende lock sulla relazione.
//! - **Rientro.** Quando il giudizio e l'esecuzione condividono un lock o una
//!   versione del catalogo.
//!
//! - **Regola.** La regola N (d) giudica soltanto i riferimenti in posizione
//!   di operando; un nome di colonna in projection o in un predicato `IS NULL`
//!   non si confronta con il catalogo.
//! - **Ambito.** Ogni dialetto.
//! - **Hazard.** Nessuno geometrico: quei nomi non sono operandi; un nome
//!   sbagliato arriva al server, che lo rifiuta o lo lega con le proprie
//!   regole.
//! - **Rientro.** Non serve.
//!
//! - **Regola.** SQL Server confronta per byte ([`NameRule::Exact`]) senza
//!   leggere la collation.
//! - **Ambito.** SQL Server, anche con collation `CI`.
//! - **Hazard.** Nessun passaggio silenzioso: un nome che incontra una
//!   colonna o un alias soltanto senza maiuscole si rifiuta anche dove il
//!   server lo legherebbe. Il costo e un rifiuto in piu.
//! - **Rientro.** Quando il giudizio legge la collation del database.

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

/// Motivo stabile del rifiuto di una forma di nomi il cui legame dipende
/// dalle regole di nome del dialetto (regola N).
pub const AMBIGUOUS_NAME_SHAPE: &str =
    "forma di nomi ambigua: gli alias e i nomi di relazione visibili insieme devono essere distinti anche senza maiuscole, i nomi di uscita di una projection distinti anche senza maiuscole, ogni colonna di una subquery con relazioni esterne qualificata, ogni identificatore uguale per byte al nome che nomina nel piano o nel catalogo";

/// Profondita massima di un'espansione di `*` attraverso sorgenti derivate:
/// oltre, l'espansione non si conosce e il riferimento non si risolve.
const MAX_EXPANSION_DEPTH: usize = 64;

/// La guardia del server sugli operandi geometrici scalari: dove il server
/// rifiuta ogni operatore scalare su una colonna geometrica, misurato.
///
/// Oracle, `MDSYS.SDO_GEOMETRY` (tipo oggetto senza `MAP` o `ORDER`, con
/// attributi `VARRAY`), misurato su Oracle Database Free
/// (`docker-compose.oracle.yml`) con lo SQL del renderer per ogni forma di
/// `tests/fixtures/operandi_server_guard.json`: confronto con un bind
/// binario o testo, `IN` e `BETWEEN` con bind, `CAST` a `VARCHAR2` o `RAW`,
/// `LOWER`/`UPPER`, `COALESCE`, aritmetica, `SUM`/`AVG`, una colonna di CTE
/// sotto `=`: ORA-00932; `=` fra geometrie, `IN` subquery, `ORDER BY` (anche
/// di `CASE`, di subquery scalare, per alias, di una colonna derivata),
/// `DISTINCT`, `MIN`/`MAX`, `HAVING`, `PARTITION BY` e `ORDER BY` delle
/// finestre, `LAG`/`LEAD`, `UNION`/`INTERSECT`/`EXCEPT`: ORA-22901;
/// `GROUP BY`: ORA-22950. La stessa forma su una colonna non geometrica
/// passa (controllo).
///
/// Db2 non ha guardia: su Db2 12.1 `ST_GEOMETRY` e `SYSIBM.ST_GEOMETRY`, un
/// tipo distinto su `BLOB`, e `=`, `<>`, `<`, `>=`, `IN`, `NOT IN`,
/// `BETWEEN` con un bind binario, `=` fra geometrie, `IN` subquery, `CAST` a
/// testo o binario, `COALESCE`, una colonna di CTE sotto `=` passano senza
/// errore, confrontando i byte del formato interno (misurato con lo stesso
/// fixture). Db2 resta a mondo chiuso.
///
/// Il renderer applica l'operatore direttamente alla colonna (provato
/// offline in `operandi_geometrici.rs` sullo stesso fixture): nessuna
/// conversione implicita sta fra la colonna e l'operatore. Le forme che il
/// server non protegge restano rifiutate senza catalogo: una funzione
/// spatial che rende una geometria, un `SpatialValue`, il binario canonico
/// (`SpatialOutput`, un `BLOB` WKB) sotto un operatore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperandServerGuard {
    /// Oracle, `MDSYS.SDO_GEOMETRY`.
    OracleObjectType,
}

/// La guardia del server sugli operandi scalari del prodotto, se c'e.
#[must_use]
pub const fn operand_server_guard(provider: ProviderKind) -> Option<OperandServerGuard> {
    match provider {
        ProviderKind::Oracle => Some(OperandServerGuard::OracleObjectType),
        _ => None,
    }
}

/// Come il dialetto confronta un nome di colonna del piano con il nome del
/// catalogo o con un nome di uscita.
///
/// I nomi di relazione e gli alias si confrontano sempre per byte: la regola
/// N rifiuta le forme in cui la differenza conterebbe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameRule {
    /// Il nome e quello esatto; un nome che differisce soltanto per le
    /// maiuscole si rifiuta ([`AMBIGUOUS_NAME_SHAPE`]): `PostgreSQL`, Oracle e
    /// Db2 quotano ogni identificatore, SQL Server dipende dalla collation
    /// del database (`CS` o `CI`), che il giudizio non legge.
    Exact,
    /// Senza maiuscole e senza spazi finali: i nomi di colonna e gli alias di
    /// colonna di `MySQL` e `MariaDB`, che non distinguono le maiuscole su
    /// nessuna piattaforma.
    Folded,
}

/// Come un nome del piano incontra un nome nominato.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NameMatch {
    Same,
    CaseOnly,
    Different,
}

impl NameRule {
    /// La regola del provider.
    #[must_use]
    pub const fn of(provider: ProviderKind) -> Self {
        match provider {
            ProviderKind::Mysql | ProviderKind::Mariadb => Self::Folded,
            _ => Self::Exact,
        }
    }

    /// I due nomi possono nominare la stessa colonna: per byte con
    /// [`Self::Exact`], senza maiuscole con [`Self::Folded`].
    #[must_use]
    pub fn matches(self, left: &str, right: &str) -> bool {
        match self {
            Self::Exact => left == right,
            Self::Folded => fold(left) == fold(right),
        }
    }

    fn compare(self, plan: &str, named: &str) -> NameMatch {
        if plan == named {
            NameMatch::Same
        } else if fold(plan) != fold(named) {
            NameMatch::Different
        } else if self == Self::Folded {
            NameMatch::Same
        } else {
            NameMatch::CaseOnly
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
    /// le altre si assumono scalari. Lo usa soltanto la compilazione dello
    /// SDK, come rifiuto anticipato; l'esecuzione giudica sul catalogo
    /// (regola S).
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

fn ambiguous_refusal(provider: ProviderKind) -> DatabaseError {
    DatabaseError::unsupported(provider, ErrorPhase::Prepare, AMBIGUOUS_NAME_SHAPE)
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

/// La stessa visita per uno statement portable (`PortableStatement`).
///
/// Vale anche per le mutazioni relazionali che il core abbassa in uno
/// statement portable: le colonne confrontate dal `WHERE` e quelle di
/// `ORDER BY` sono operandi della tabella dello statement; le assegnazioni,
/// `IS NULL` e i predicati spatial no.
///
/// # Errors
///
/// Quelli di [`scalar_operands`].
pub fn scalar_operands_of_portable(
    provider: ProviderKind,
    statement: &crate::portable::PortableStatement,
) -> Result<ScalarOperands> {
    use crate::portable::PortableStatement;
    let (table, filter, order_by) = match statement {
        PortableStatement::Select(select) => (
            &select.table,
            select.filter.as_ref(),
            select.order_by.as_slice(),
        ),
        PortableStatement::Update(update) => (&update.table, update.filter.as_ref(), &[][..]),
        PortableStatement::Delete(delete) => (&delete.table, delete.filter.as_ref(), &[][..]),
        PortableStatement::Insert(insert) => (&insert.table, None, &[][..]),
        PortableStatement::Upsert(upsert) => (&upsert.table, None, &[][..]),
    };
    // La chiave di conflitto di un upsert si confronta (`ON CONFLICT`, il
    // `ON` di un `MERGE`).
    let conflict = match statement {
        PortableStatement::Upsert(upsert) => upsert.conflict_target.as_slice(),
        _ => &[],
    };
    let operands = portable_operands(conflict, filter);
    let column = unqualified_column;
    let operation = QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(crate::relational::QuerySource {
            object: crate::plan::ObjectRef {
                catalog: None,
                schema: table.schema.clone(),
                object: table.name.clone(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: Vec::new(),
        joins: Vec::new(),
        filter: (!operands.is_empty()).then_some(QueryExpression::And {
            arguments: operands,
        }),
        group_by: Vec::new(),
        having: None,
        order_by: order_by
            .iter()
            .map(|order| crate::relational::QueryOrdering {
                expression: column(&order.column),
                direction: crate::plan::SortDirection::Asc,
            })
            .collect(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    };
    scalar_operands(provider, &operation)
}

/// Una colonna senza qualificatore, della sola relazione dello statement.
fn unqualified_column(field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    }
}

/// Le colonne confrontate da uno statement portable, ciascuna come operando
/// (`col IN (valori)`): la chiave di conflitto e ogni colonna di un
/// confronto, di `IN`, `BETWEEN` o `LIKE`, anche quella a destra.
fn portable_operands(
    conflict: &[String],
    filter: Option<&crate::portable::Predicate>,
) -> Vec<QueryExpression> {
    use crate::portable::Predicate;
    let column = unqualified_column;
    let value = |expression: &crate::portable::Expression| match expression {
        crate::portable::Expression::Column(field) => column(field),
        _ => QueryExpression::Parameter {
            name: "p".to_owned(),
        },
    };
    let mut operands = conflict
        .iter()
        .map(|field| QueryExpression::InList {
            expression: Box::new(column(field)),
            values: Vec::new(),
            negated: false,
        })
        .collect::<Vec<_>>();
    let mut stack = filter.into_iter().collect::<Vec<_>>();
    while let Some(predicate) = stack.pop() {
        let (field, values): (&str, Vec<&crate::portable::Expression>) = match predicate {
            Predicate::And { predicates } | Predicate::Or { predicates } => {
                stack.extend(predicates);
                continue;
            }
            Predicate::Not { predicate } => {
                stack.push(predicate);
                continue;
            }
            Predicate::IsNull { .. } | Predicate::IsNotNull { .. } | Predicate::Spatial { .. } => {
                continue;
            }
            Predicate::Eq { column, value }
            | Predicate::Ne { column, value }
            | Predicate::Lt { column, value }
            | Predicate::Lte { column, value }
            | Predicate::Gt { column, value }
            | Predicate::Gte { column, value }
            | Predicate::Like {
                column,
                pattern: value,
            } => (column, vec![value]),
            Predicate::In { column, values } => (column, values.iter().collect()),
            Predicate::Between { column, low, high } => (column, vec![low, high]),
        };
        operands.push(QueryExpression::InList {
            expression: Box::new(column(field)),
            values: values.into_iter().map(value).collect(),
            negated: false,
        });
    }
    operands
}

/// Il piano che un percorso di esecuzione giudica sul catalogo del provider.
///
/// Lo SDK, l'ORM, la facade portable e la CLI portable eseguono SQL
/// compilato, e prima di eseguirlo ne passano il piano alla transazione
/// (`TransactionScope::operand_catalog`).
#[derive(Debug, Clone, Copy)]
pub enum OperandPlan<'a> {
    /// Una query relazionale (`QueryOperation`).
    Query(&'a QueryOperation),
    /// Una mutazione relazionale (`MutationOperation`).
    Mutation(&'a crate::relational::MutationOperation),
    /// Uno statement portable.
    Portable(&'a crate::portable::PortableStatement),
}

impl OperandPlan<'_> {
    /// La visita senza catalogo del piano.
    ///
    /// # Errors
    ///
    /// Quelli di [`scalar_operands`].
    pub fn operands(&self, provider: ProviderKind) -> Result<ScalarOperands> {
        use crate::relational::MutationOperation;
        match *self {
            Self::Query(query) => scalar_operands(provider, query),
            Self::Portable(statement) => scalar_operands_of_portable(provider, statement),
            Self::Mutation(mutation) => {
                let (target, filter) = match mutation {
                    MutationOperation::Update(update) => (&update.target, update.filter.clone()),
                    MutationOperation::Delete(delete) => (&delete.target, delete.filter.clone()),
                    MutationOperation::Insert(insert) => (&insert.target, None),
                    // La chiave di conflitto si confronta.
                    MutationOperation::Upsert(upsert) => (
                        &upsert.target,
                        Some(QueryExpression::And {
                            arguments: upsert
                                .conflict_target
                                .iter()
                                .map(|field| QueryExpression::InList {
                                    expression: Box::new(QueryExpression::Column {
                                        column: ColumnRef {
                                            relation: None,
                                            field: field.clone(),
                                        },
                                    }),
                                    values: Vec::new(),
                                    negated: false,
                                })
                                .collect(),
                        }),
                    ),
                };
                let always = QueryExpression::And {
                    arguments: Vec::new(),
                };
                scalar_operands_of_predicate(provider, target, filter.as_ref().unwrap_or(&always))
            }
        }
    }
}

/// Che cosa una relazione della FROM puo essere.
#[derive(Clone)]
enum RelationKind<'a> {
    /// Una tabella o una vista del catalogo.
    Base(Option<String>, String),
    /// Una relazione nominata con il catalogo (un altro database): il
    /// provider legge le colonne del proprio, e queste non si conoscono.
    Foreign,
    /// Una sorgente derivata o una CTE: la sua query, le CTE che vede, il
    /// blocco che la contiene se e `LATERAL`.
    Query(
        &'a QueryOperation,
        Vec<&'a CommonTableExpression>,
        Option<usize>,
    ),
}

/// Una relazione della FROM: il nome con cui il blocco la nomina e che cosa
/// e. Il nome di una CTE si incontra per byte: un nome che la incontra
/// soltanto senza maiuscole e la forma che la regola N rifiuta.
struct FromRelation<'a> {
    name: String,
    kind: RelationKind<'a>,
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
    /// Il nome incontra il nome nominato soltanto senza maiuscole, dove il
    /// dialetto le distingue o puo distinguerle.
    Ambiguous,
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
    aliased: BTreeSet<(usize, usize)>,
    cte_scopes: BTreeMap<usize, Vec<&'a CommonTableExpression>>,
    stack: Vec<Item<'a>>,
    relations: Vec<(Option<String>, String)>,
    references: bool,
    seen: Seen,
}

/// Cio che la visita ha incontrato e che decide il giudizio finale: quattro
/// fatti indipendenti, non uno stato.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct Seen {
    /// Un riferimento che non si risolve.
    unresolved: bool,
    /// Una forma della regola N.
    ambiguous: bool,
    /// Un nodo spatial, o una relazione di un altro catalogo.
    geometric: bool,
    /// Un nome di relazione, di CTE, di uscita o un riferimento di colonna
    /// con caratteri non ASCII.
    foreign: bool,
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
        cte_scopes: BTreeMap::new(),
        stack: vec![Item::Operation(operation, Vec::new(), None, false)],
        relations: Vec::new(),
        references: false,
        seen: Seen::default(),
    };
    while let Some(item) = state.stack.pop() {
        state.step(item)?;
    }
    if let Some(catalog) = catalog {
        let geometric = state.seen.geometric
            || state.relations.iter().any(|(schema, object)| {
                catalog.columns(schema.as_ref(), object).may_hold_geometry()
            });
        // Le collation di SQL Server, `MySQL` e `MariaDB` possono uguagliare
        // nomi non ASCII che la piegatura distingue (accenti, `ß`/`ss`): in
        // un piano con geometrie un nome non ASCII non si confronta.
        let collated = matches!(
            provider,
            ProviderKind::Sqlserver | ProviderKind::Mysql | ProviderKind::Mariadb
        );
        if (state.seen.ambiguous || (state.seen.foreign && collated)) && geometric {
            return Err(ambiguous_refusal(provider));
        }
        if state.seen.unresolved && geometric {
            return Err(unresolved_refusal(provider));
        }
    } else if state.seen.ambiguous && state.seen.geometric {
        // Senza catalogo si sa geometrico soltanto un piano con un nodo
        // spatial (o una relazione di un altro catalogo): il resto lo decide
        // il giudizio sul catalogo.
        return Err(ambiguous_refusal(provider));
    }
    Ok(Outcome {
        relations: state.relations,
        references: state.references,
    })
}

fn address(operation: &QueryOperation) -> usize {
    std::ptr::from_ref(operation) as usize
}

fn identity(cte: &CommonTableExpression) -> usize {
    std::ptr::from_ref(cte) as usize
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
        self.check_names(index);
        let inner = self.blocks[index].visible.clone();
        for relation in &self.blocks[index].from {
            match &relation.kind {
                RelationKind::Base(schema, object) => {
                    let key = (schema.clone(), object.clone());
                    if !self.relations.contains(&key) {
                        self.relations.push(key);
                    }
                }
                // Le colonne di un altro database non si leggono: il piano
                // puo contenere una geometria.
                RelationKind::Foreign => self.seen.geometric = true,
                RelationKind::Query(..) => {}
            }
        }
        for cte in &operation.common_table_expressions {
            let scope = self.scope_of(cte, &inner);
            self.stack
                .push(Item::Operation(&cte.query, scope, None, false));
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

    /// La regola N sul blocco, senza catalogo: (a) i nomi di relazione del
    /// blocco e dei blocchi che lo contengono sono distinti anche senza
    /// maiuscole (due nomi uguali per byte in ambiti annidati compresi); le
    /// CTE visibili altrettanto, e una relazione senza schema che incontra
    /// una CTE soltanto senza maiuscole si rifiuta; (b) i nomi di uscita della
    /// projection sono distinti anche senza maiuscole.
    fn check_names(&mut self, index: usize) {
        let foreign = self.blocks[index]
            .from
            .iter()
            .map(|from| from.name.as_str())
            .chain(
                self.blocks[index]
                    .visible
                    .iter()
                    .map(|cte| cte.name.as_str()),
            )
            .chain(
                self.blocks[index]
                    .operation
                    .projection
                    .iter()
                    .filter_map(|item| item.alias.as_deref()),
            )
            .any(|name| !name.is_ascii());
        self.seen.foreign |= foreign;
        if self.ambiguous_names(index) {
            self.seen.ambiguous = true;
        }
    }

    /// Le forme (a) e (b) della regola N nel blocco.
    fn ambiguous_names(&self, index: usize) -> bool {
        let refused = || true;
        let mut names = BTreeSet::new();
        let mut current = Some(index);
        while let Some(block) = current {
            for from in &self.blocks[block].from {
                if !names.insert(fold(&from.name)) {
                    return refused();
                }
            }
            current = self.blocks[block].parent;
        }
        let block = &self.blocks[index];
        let mut ctes: BTreeMap<String, usize> = BTreeMap::new();
        for cte in &block.visible {
            let own = identity(cte);
            if ctes
                .insert(fold(&cte.name), own)
                .is_some_and(|other| other != own)
            {
                return refused();
            }
        }
        let operation = block.operation;
        if refers_forward(&operation.common_table_expressions) {
            return refused();
        }
        let sources = operation.source.iter().chain(
            operation
                .joins
                .iter()
                .filter_map(|join| join.source.as_ref()),
        );
        for source in sources {
            let object = &source.object;
            if object.schema.is_none()
                && block.visible.iter().any(|cte| {
                    NameRule::Exact.compare(&object.object, &cte.name) == NameMatch::CaseOnly
                })
            {
                return refused();
            }
        }
        let mut outputs = BTreeSet::new();
        for item in &operation.projection {
            let name = item.alias.as_deref().or(match &item.expression {
                QueryExpression::Column { column } => Some(column.field.as_str()),
                _ => None,
            });
            if name.is_some_and(|name| !outputs.insert(fold(name))) {
                return refused();
            }
        }
        false
    }

    /// La regola N su un riferimento di colonna, in ogni posizione: (a) il
    /// qualificatore e uguale per byte alla relazione che incontra senza
    /// maiuscole; (c) senza qualificatore, il blocco non ha blocchi esterni
    /// con relazioni (subquery, `LATERAL`).
    fn check_reference(&mut self, relation: Option<&str>, index: usize) {
        self.seen.foreign |= relation.is_some_and(|relation| !relation.is_ascii());
        // Con il qualificatore si guarda il blocco e quelli esterni; senza,
        // soltanto quelli esterni.
        let mut current = if relation.is_some() {
            Some(index)
        } else {
            self.blocks[index].parent
        };
        while let Some(block) = current {
            let from = &self.blocks[block].from;
            let refused = relation.map_or(!from.is_empty(), |relation| {
                from.iter().any(|from| {
                    NameRule::Exact.compare(relation, &from.name) == NameMatch::CaseOnly
                })
            });
            if refused {
                self.seen.ambiguous = true;
                break;
            }
            current = self.blocks[block].parent;
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
        for (position, cte) in operation.common_table_expressions.iter().enumerate() {
            let mut scope = visible.clone();
            scope.extend(operation.common_table_expressions[..position].iter());
            if cte.recursive {
                scope.push(cte);
            }
            self.cte_scopes.entry(identity(cte)).or_insert(scope);
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
                    kind: RelationKind::Query(
                        &derived.query,
                        inner.clone(),
                        lateral.then_some(index),
                    ),
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

    /// Una relazione nominata: la CTE visibile con lo stesso nome per byte,
    /// con le CTE che vede dove e definita; la tabella del catalogo
    /// altrimenti; una relazione di un altro catalogo non si conosce.
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
        if object.catalog.is_some() {
            return FromRelation {
                name,
                kind: RelationKind::Foreign,
            };
        }
        let cte = visible
            .iter()
            .rev()
            .find(|cte| object.schema.is_none() && cte.name == object.object);
        let kind = cte.map_or_else(
            || RelationKind::Base(object.schema.clone(), object.object.clone()),
            |cte| RelationKind::Query(&cte.query, self.scope_of(cte, visible), None),
        );
        FromRelation { name, kind }
    }

    /// Le CTE che il corpo di `cte` vede: quelle visibili dove e definita e
    /// le precedenti dello stesso `WITH`, se stessa soltanto se ricorsiva.
    fn scope_of(
        &self,
        cte: &'a CommonTableExpression,
        fallback: &[&'a CommonTableExpression],
    ) -> Vec<&'a CommonTableExpression> {
        self.cte_scopes
            .get(&identity(cte))
            .cloned()
            .unwrap_or_else(|| fallback.to_vec())
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
                self.seen.foreign |= !column.field.is_ascii();
                self.check_reference(column.relation.as_deref(), index);
                if operand {
                    self.stack.push(Item::Reference(column.clone(), index));
                }
            }
            QueryExpression::Wildcard { relation } => {
                if let Some(relation) = relation {
                    self.check_reference(Some(relation), index);
                }
                if operand {
                    self.wildcard(relation.as_deref(), index)?;
                }
            }
            QueryExpression::Parameter { .. } | QueryExpression::TypedParameter { .. } => {}
            QueryExpression::SpatialValue { expression, .. } => {
                self.seen.geometric = true;
                if operand {
                    return Err(refusal(provider));
                }
                children.push((expression, false));
            }
            QueryExpression::Spatial {
                function,
                arguments,
            } => {
                self.seen.geometric = true;
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
                self.seen.geometric = true;
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
                self.seen.geometric = true;
                children.push((left, false));
                children.push((right, false));
            }
            QueryExpression::SpatialOutput { expression, .. } => {
                // Il binario canonico di una geometria: sotto un operatore
                // confronta byte, non geometrie, su ogni dialetto.
                self.seen.geometric = true;
                if operand {
                    return Err(refusal(provider));
                }
                children.push((expression, false));
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
            self.seen.unresolved = true;
        }
        for candidate in candidates {
            match candidate {
                Candidate::Geometry => return Err(refusal(self.provider)),
                Candidate::Ambiguous => self.seen.ambiguous = true,
                Candidate::Scalar => {}
                Candidate::Unresolved => self.seen.unresolved = true,
                Candidate::Follow(item) => self.stack.push(item),
            }
        }
        Ok(())
    }

    /// Un riferimento di colonna in posizione di operando: la colonna che
    /// nomina nella FROM del blocco (qualificato, anche di un blocco esterno),
    /// e i nomi di uscita del blocco stesso (`ORDER BY`, `GROUP BY`, `HAVING`
    /// per alias o per nome), in ogni ramo di set operation.
    fn reference(&mut self, column: &ColumnRef, index: usize) -> Result<()> {
        self.references = true;
        let mut candidates = match &column.relation {
            Some(relation) => self.qualified(relation, &column.field, index),
            None => self.unqualified(&column.field, index),
        };
        let own = self.output_positions(index, column).unwrap_or_else(|()| {
            self.seen.ambiguous = true;
            Vec::new()
        });
        // Ogni colonna di uscita si segue una volta sola (due alias che si
        // nominano a vicenda), per posizione: un riferimento qualificato e
        // uno senza qualificatore possono nominarne di diverse.
        let block = &self.blocks[index];
        let (operation, visible, parent) = (block.operation, block.visible.clone(), block.parent);
        for position in own {
            if self.aliased.insert((index, position)) {
                candidates.push(Candidate::Follow(Item::Position(
                    operation,
                    visible.clone(),
                    parent,
                    position,
                )));
            } else {
                candidates.push(Candidate::Scalar);
            }
        }
        self.apply(candidates)
    }

    /// Le posizioni delle colonne di uscita del blocco che `column` nomina:
    /// per nome se non e qualificata, per la stessa colonna della projection
    /// se lo e. `Err`: un nome di uscita la incontra soltanto senza
    /// maiuscole.
    fn output_positions(
        &mut self,
        index: usize,
        column: &ColumnRef,
    ) -> std::result::Result<Vec<usize>, ()> {
        let (entries, _) = self.entries_of(index, 0);
        let operation = self.blocks[index].operation;
        let rule = self.rule;
        let mut positions = Vec::new();
        for (position, entry) in entries.iter().enumerate() {
            let found = column.relation.as_deref().map_or_else(
                || {
                    entry.name.as_deref().map_or(NameMatch::Different, |name| {
                        rule.compare(&column.field, name)
                    })
                },
                |relation| match entry.source {
                    EntrySource::Projection(item) if !operation.set_operations.is_empty() => {
                        match &operation.projection[item].expression {
                            QueryExpression::Column { column: own }
                                if own.relation.as_deref() == Some(relation) =>
                            {
                                rule.compare(&column.field, &own.field)
                            }
                            _ => NameMatch::Different,
                        }
                    }
                    _ => NameMatch::Different,
                },
            );
            match found {
                NameMatch::Same => positions.push(position),
                NameMatch::CaseOnly => return Err(()),
                NameMatch::Different => {}
            }
        }
        Ok(positions)
    }

    /// `relation.field`: la relazione con quel nome per byte, nel blocco o
    /// in un blocco esterno (la regola N ne lascia una sola).
    fn qualified(&mut self, relation: &str, field: &str, index: usize) -> Vec<Candidate<'a>> {
        let mut current = Some(index);
        while let Some(block) = current {
            let named = self.blocks[block]
                .from
                .iter()
                .find(|from| from.name == relation)
                .map(|from| from.kind.clone());
            if let Some(kind) = named {
                let candidates = self.in_relation(&kind, field);
                if candidates.is_empty() {
                    return vec![Candidate::Unresolved];
                }
                return candidates;
            }
            current = self.blocks[block].parent;
        }
        vec![Candidate::Unresolved]
    }

    /// `field` senza qualificazione: le relazioni della FROM del blocco. La
    /// regola N rifiuta un riferimento senza qualificazione in un blocco con
    /// blocchi esterni che hanno relazioni: il dialetto lo cercherebbe fuori,
    /// con le proprie regole di nome.
    fn unqualified(&mut self, field: &str, index: usize) -> Vec<Candidate<'a>> {
        let kinds = self.blocks[index]
            .from
            .iter()
            .map(|from| from.kind.clone())
            .collect::<Vec<_>>();
        let mut candidates = Vec::new();
        for kind in kinds {
            candidates.extend(self.in_relation(&kind, field));
        }
        candidates
    }

    /// Le colonne di `kind` che `field` nomina, con la regola del dialetto;
    /// vuoto se nessuna.
    fn in_relation(&mut self, kind: &RelationKind<'a>, field: &str) -> Vec<Candidate<'a>> {
        match kind {
            RelationKind::Base(schema, object) => {
                let Some(catalog) = self.catalog else {
                    return vec![Candidate::Scalar];
                };
                match catalog.columns(schema.as_ref(), object) {
                    RelationColumns::Absent => Vec::new(),
                    RelationColumns::Unknown => vec![Candidate::Unresolved],
                    RelationColumns::Declared(geometric) => {
                        // Dichiarate dal chiamante: un nome che incontra una
                        // colonna geometrica anche soltanto senza maiuscole
                        // si giudica geometrico.
                        if geometric.iter().any(|name| fold(name) == fold(field)) {
                            vec![Candidate::Geometry]
                        } else {
                            vec![Candidate::Scalar]
                        }
                    }
                    RelationColumns::Known(columns) => {
                        let mut found: Option<bool> = None;
                        let mut case_only = false;
                        for (name, geometric) in columns {
                            match self.rule.compare(field, name) {
                                NameMatch::Same => {
                                    found = Some(found.unwrap_or(false) || *geometric);
                                }
                                NameMatch::CaseOnly => case_only = true,
                                NameMatch::Different => {}
                            }
                        }
                        match found {
                            Some(true) => vec![Candidate::Geometry],
                            Some(false) => vec![Candidate::Scalar],
                            None if case_only => vec![Candidate::Ambiguous],
                            None => Vec::new(),
                        }
                    }
                }
            }
            RelationKind::Query(query, visible, parent) => {
                self.output_by_name(query, visible, *parent, field)
            }
            RelationKind::Foreign => vec![Candidate::Unresolved],
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
        let mut positions = Vec::new();
        for (position, entry) in entries.iter().enumerate() {
            match entry
                .name
                .as_deref()
                .map_or(NameMatch::Different, |name| rule.compare(field, name))
            {
                NameMatch::Same => positions.push(position),
                NameMatch::CaseOnly => return vec![Candidate::Ambiguous],
                NameMatch::Different => {}
            }
        }
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
            for (name, kind) in from {
                match kind {
                    RelationKind::Base(schema, object) => {
                        let columns = self
                            .catalog
                            .map(|catalog| catalog.columns(schema.as_ref(), &object));
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
                    RelationKind::Foreign => {
                        complete = false;
                        break 'projection;
                    }
                    RelationKind::Query(query, visible, parent) => {
                        let inner = self.open_block(query, visible.clone(), parent);
                        let (nested, nested_complete) = self.entries_of(inner, depth + 1);
                        for (position, entry) in nested.into_iter().enumerate() {
                            entries.push(Entry {
                                name: entry.name,
                                source: EntrySource::Position(
                                    query,
                                    visible.clone(),
                                    parent,
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

    /// Le relazioni della FROM del blocco che un `*` nomina, per byte.
    fn relations_named(
        &self,
        index: usize,
        relation: Option<&str>,
    ) -> Vec<(String, RelationKind<'a>)> {
        self.blocks[index]
            .from
            .iter()
            .filter(|from| relation.is_none_or(|relation| from.name == relation))
            .map(|from| (from.name.clone(), from.kind.clone()))
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
        for (name, kind) in from {
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
                RelationKind::Foreign => candidates.push(Candidate::Unresolved),
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

/// Un corpo di CTE che nomina senza schema, anche soltanto senza maiuscole,
/// una CTE dello stesso `WITH` che non vede: una successiva, o se stessa non
/// ricorsiva. Il legame dipende dal dialetto (`PostgreSQL` lo lega alla
/// tabella senza `RECURSIVE` e alla CTE con, SQL Server rifiuta il nome in
/// avanti): la regola N lo rifiuta.
fn refers_forward(ctes: &[CommonTableExpression]) -> bool {
    use crate::relational::{walk_query, QueryWalkControl, QueryWalkNode};
    ctes.iter().enumerate().any(|(position, cte)| {
        let hidden = ctes
            .iter()
            .enumerate()
            .filter(|(other, sibling)| {
                *other > position || (*other == position && !sibling.recursive)
            })
            .map(|(_, sibling)| fold(&sibling.name))
            .collect::<BTreeSet<_>>();
        if hidden.is_empty() {
            return false;
        }
        !walk_query(&cte.query, |node| match node {
            QueryWalkNode::Source(source)
                if source.object.schema.is_none()
                    && source.object.catalog.is_none()
                    && hidden.contains(&fold(&source.object.object)) =>
            {
                QueryWalkControl::Break
            }
            _ => QueryWalkControl::Continue,
        })
    })
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
