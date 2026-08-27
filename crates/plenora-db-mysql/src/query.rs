//! Path `QueryOperation` scalare a sorgente singola per `MySQL`.
//!
//! L'AST portabile viene renderizzato dal dialect `MySQL` condiviso e lo
//! schema di output arriva dai metadati di colonna di `COM_STMT_PREPARE`:
//! `MySQL` non ha un equivalente di `sys.dm_exec_describe_first_result_set`,
//! quindi la descrizione del prepared statement e l'unica fonte autoritativa.
//!
//! Il sottoinsieme qualificato comprende proiezioni scalari, DISTINCT, aggregazione
//! (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX` con GROUP BY e HAVING), i join
//! fisici INNER, LEFT, RIGHT e CROSS fra tabelle del solo database
//! configurato e, nella sola lista di selezione di una query non raggruppata,
//! le window stabili fra righe pari: `RANK` e `DENSE_RANK` con ORDER BY non
//! vuoto e gli aggregati di finestra, senza frame o con un frame RANGE senza
//! offset. `ROW_NUMBER`, `LAG`, `LEAD` e il frame ROWS restano invece chiusi:
//! numerano le righe pari una per una e sarebbero riproducibili solo su una
//! chiave d'ordine dimostrata univoca, proprieta che l'AST portabile non
//! esprime. CTE, set operation, window spatial, derived source (di base o di
//! join), LATERAL, subquery e locking restano fail-closed per lo stesso
//! motivo di fondo: sono sottoinsiemi dell'AST non dimostrati su
//! `MySQL`, non costrutti necessariamente assenti dal motore. Le funzioni di
//! `VERIFIED_SPATIAL_FUNCTIONS` si renderizzano e si eseguono; resta chiusa la
//! *window* spatial. FULL JOIN e invece un'assenza reale: `MySQL` non ha una forma nativa equivalente,
//! cosi come non ha la clausola di frame GROUPS.
//!
//! Il `sql_mode` deterministico applicato dal bootstrap di sessione non
//! include `ONLY_FULL_GROUP_BY`: `MySQL` accetterebbe in silenzio un gruppo
//! non determinato restituendo un valore arbitrario per riga. La verifica di
//! determinismo del gruppo e quindi a carico del provider e avviene prima di
//! qualsiasi contatto con il server.

use crate::types::mysql_renderer;
use crate::{MysqlColumnSpec, MAX_IDENTIFIER_CHARACTERS};
use mysql_async::Column;
use plenora_database_core::limits::Limits;
use plenora_database_core::query::{
    validate_query_operation, walk_query_expression, JoinKind, QueryExpression, QueryOperation,
    QueryOrdering, QuerySource, QueryWalkControl, QueryWalkNode, ScalarFunction, SpatialFunction,
    WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::RenderedSql;
use std::collections::BTreeSet;

/// Sottoinsieme delle funzioni spatial `MySQL` qualificate per l'AST
/// portabile — pubblicate in `probe_capabilities` e utilizzabili via
/// `Provider::query` sia come proiezioni scalari sia (per i predicati) come
/// filtri WHERE.
///
/// La lista e il cancello condiviso da capability e renderer: ogni voce e
/// attraversata da `live_v12_every_verified_spatial_function_executes`, mentre
/// le funzioni assenti restano `Unsupported`. La sonda prova almeno una forma
/// di argomento valida; le funzioni che restituiscono geometrie richiedono
/// anche l'evidenza CRS di `raw.crs_rule_check`.
pub const VERIFIED_SPATIAL_FUNCTIONS: &[SpatialFunction] = &[
    SpatialFunction::GeometryType,
    SpatialFunction::Srid,
    SpatialFunction::Dimensions,
    SpatialFunction::NPoints,
    SpatialFunction::IsEmpty,
    SpatialFunction::IsValid,
    SpatialFunction::IsClosed,
    SpatialFunction::Intersects,
    SpatialFunction::Contains,
    SpatialFunction::Within,
    SpatialFunction::Disjoint,
    SpatialFunction::Equals,
    SpatialFunction::Distance,
    SpatialFunction::Area,
    SpatialFunction::Length,
    // Funzioni scalari qualificate sia dalla sonda delle forme sia dalla sonda
    // end-to-end. Le liste MySQL e MariaDB restano indipendenti per prodotto.
    SpatialFunction::X,
    SpatialFunction::Y,
    SpatialFunction::IsSimple,
    SpatialFunction::Touches,
    SpatialFunction::Crosses,
    SpatialFunction::Overlaps,
    SpatialFunction::HausdorffDistance,
    SpatialFunction::FrechetDistance,
    SpatialFunction::AsGeoJson,
    // Queste funzioni geometriche sono qualificate sui CRS proiettati; MySQL
    // risponde 3618 su 4326. Il piano dichiara il CRS d'ingresso e il provider
    // ne applica e verifica la regola di propagazione.
    SpatialFunction::Envelope,
    SpatialFunction::Centroid,
    SpatialFunction::Buffer,
    // Funzioni geometriche di cui il provider sa propagare il CRS. Restano
    // escluse le forme con regole non rappresentabili e gli aggregati.
    SpatialFunction::StartPoint,
    SpatialFunction::EndPoint,
    SpatialFunction::PointN,
    SpatialFunction::ConvexHull,
    // MariaDB non qualifica `ST_Simplify`: i riferimenti rispondono 1305 o
    // 4212, quindi la differenza e intenzionale.
    SpatialFunction::Simplify,
];

/// Le funzioni spatial qualificate su `MariaDB`.
///
/// E l'intersezione delle versioni qualificate: il profilo non conosce in
/// anticipo la minor del server e puo promettere soltanto le forme provate su
/// tutti i riferimenti. `live_v12_every_verified_spatial_function_executes`
/// attraversa ogni voce con il profilo `MariaDB`.
pub const MARIADB_VERIFIED_SPATIAL_FUNCTIONS: &[SpatialFunction] = &[
    SpatialFunction::GeometryType,
    SpatialFunction::Srid,
    SpatialFunction::Dimensions,
    SpatialFunction::NPoints,
    SpatialFunction::IsEmpty,
    SpatialFunction::IsClosed,
    SpatialFunction::Intersects,
    SpatialFunction::Contains,
    SpatialFunction::Within,
    SpatialFunction::Disjoint,
    SpatialFunction::Equals,
    SpatialFunction::Distance,
    SpatialFunction::Area,
    SpatialFunction::Length,
    // `CoveredBy` e `IsValid` non appartengono all'intersezione delle versioni.
    // `Relate` richiede tre argomenti su MariaDB, mentre il contratto ne
    // ammette anche due, quindi non e qualificabile sull'intera arieta.
    SpatialFunction::X,
    SpatialFunction::Y,
    SpatialFunction::IsSimple,
    SpatialFunction::Touches,
    SpatialFunction::Crosses,
    SpatialFunction::Overlaps,
    SpatialFunction::AsGeoJson,
    // Funzioni geometriche provate da `raw.crs_rule_check`:
    // `raw.crs_rule_check` le ha attraversate su tutti e tre i sistemi di
    // riferimento — geografico, e i due proiettati — e su tutte e tre le major.
    // Nessun 3618: questo prodotto le implementa ovunque.
    //
    // `ST_Buffer` rende SRID 0 partendo da 3003, e non e un motivo per
    // chiuderlo: `ST_Contains(ST_Buffer(area, 1), area)` risponde 1, cioe le
    // coordinate sono rimaste dove erano e il motore ha lasciato cadere
    // l'etichetta invece di riproiettare. Cio che il provider pubblica non e
    // quell'etichetta ma il CRS dichiarato per l'ingresso, confermato riga per
    // riga sull'ingresso stesso.
    SpatialFunction::Envelope,
    SpatialFunction::Centroid,
    SpatialFunction::Buffer,
    // Funzioni presenti su tutti i riferimenti e con una regola CRS
    // rappresentabile. `Simplify` e `Collect` non appartengono
    // all'intersezione; gli aggregati restano inoltre fuori dall'AST.
    SpatialFunction::StartPoint,
    SpatialFunction::EndPoint,
    SpatialFunction::PointN,
    SpatialFunction::ConvexHull,
    SpatialFunction::PointOnSurface,
    SpatialFunction::Boundary,
];

/// Renderizza una `QueryOperation` scalare a sorgente singola.
///
/// La validazione portabile del core viene applicata per prima, cosi la
/// successiva ispezione lavora su un albero gia limitato in profondita e
/// numero di nodi.
///
/// # Errors
///
/// Restituisce `InvalidPlan` per strutture incomplete o identificatori oltre i
/// 64 caratteri di `MySQL`, e `Unsupported` per ogni sottoinsieme dell'AST non
/// ancora qualificato su `MySQL`.
pub fn render_query(operation: &QueryOperation, database: &str) -> Result<RenderedSql> {
    render_query_with_profile(operation, database, &crate::profile::MYSQL_PROFILE)
}

/// La resa di una query, con il profilo che dice quali funzioni spatial sono
/// qualificate su **questo** prodotto.
///
/// Il cancello del renderer e la capability pubblicata leggono la stessa
/// lista per evitare che un piano ammesso contraddica il profilo del prodotto.
///
/// # Errors
///
/// Come `render_query`.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn render_query_with_profile(
    operation: &QueryOperation,
    database: &str,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<RenderedSql> {
    Ok(render_query_plan(operation, database, profile)?.rendered)
}

/// Una geometria calcolata, e il sistema di riferimento in cui cade.
///
/// Il tipo wire non basta a descriverla: il renderer la incapsula in
/// `ST_AsBinary`, quindi arriva come BLOB e nessuna ispezione dei metadati la
/// distingue da un blob qualunque. Cio che la distingue e il **piano**, ed e da
/// li che questa struttura viene.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct MysqlQueryGeometry {
    /// La posizione della projection nel result set.
    pub result_index: usize,
    /// Il CRS del risultato, dedotto dalla regola della funzione e dal CRS
    /// dichiarato per la geometria d'ingresso.
    pub srid: u32,
}

/// Cio che il path query deve sapere oltre al testo SQL.
#[derive(Debug, Clone)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct MysqlRenderedQuery {
    pub rendered: RenderedSql,
    /// Le geometrie calcolate, per posizione.
    pub geometries: Vec<MysqlQueryGeometry>,
    /// Le colonne `ST_SRID` accodate, da confermare riga per riga.
    pub crs_checks: Vec<crate::types::MysqlCrsCheck>,
    /// Quante colonne appartengono al chiamante. Cio che sta oltre e il
    /// controllo, e non compare in nessuno schema pubblicato.
    pub visible_columns: usize,
}

/// Renderizza la query e prepara la verifica dei CRS dichiarati.
///
/// # Il problema che risolve
///
/// Una query puo **calcolare** una geometria, e cio che il motore rende non
/// porta il sistema di riferimento: `raw.geometry_result_forms` ha misurato
/// SRID 0 per `ST_Buffer` su una colonna in 4326, su entrambi i prodotti.
/// Pubblicare quello 0 come CRS direbbe una cosa che nessuno ha dichiarato, e
/// per questo la superficie era chiusa.
///
/// Il frame pero e noto: e quello dell'ingresso, che il piano dichiara e che
/// [`SpatialFunction::crs_rule`] dice se il risultato eredita. Restava solo da
/// **dimostrare** che l'ingresso stia davvero dove il piano dice.
///
/// # Come lo dimostra
///
/// Con lo stesso meccanismo del path di lettura, che qui costa ancora meno
/// perche la colonna di controllo e essa stessa una projection: per ogni
/// colonna geometrica sorgente si accoda un `ST_SRID` in fondo alla lista di
/// selezione, e il decoder lo confronta con la dichiarazione **a ogni riga**.
/// Le colonne accodate stanno dopo tutte le visibili, e cio le rende invisibili
/// a tutto il resto: nessun indice cambia, e uno schema pubblicato resta
/// identico a quello di una query senza geometrie.
///
/// Riga per riga e non una volta sola per la ragione di sempre: la colonna che
/// richiede una dichiarazione e quella che nessuna DDL vincola, e due righe
/// possono portare SRID diversi.
///
/// # Errors
///
/// Come `render_query`, e in piu `Crs` quando una geometria calcolata non ha un
/// sistema di riferimento dimostrabile — che resta la risposta onesta, e ora e
/// l'eccezione invece della regola.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn render_query_plan(
    operation: &QueryOperation,
    database: &str,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<MysqlRenderedQuery> {
    validate_query_operation(operation, &Limits::default())?;
    ensure_qualified_shape(operation, database, profile)?;
    let declared = declared_query_crs(operation, profile)?;
    let geometries = resolve_query_geometries(operation, &declared, profile)?;
    let visible_columns = operation.projection.len();
    if geometries.is_empty() {
        return Ok(MysqlRenderedQuery {
            rendered: mysql_renderer().render_query(operation)?,
            geometries: Vec::new(),
            crs_checks: Vec::new(),
            visible_columns,
        });
    }
    let (checked, crs_checks) = append_crs_checks(operation, &geometries, profile)?;
    Ok(MysqlRenderedQuery {
        rendered: mysql_renderer().render_query(&checked)?,
        geometries: geometries
            .into_iter()
            .map(|geometry| MysqlQueryGeometry {
                result_index: geometry.result_index,
                srid: geometry.srid,
            })
            .collect(),
        crs_checks,
        visible_columns,
    })
}

/// Una geometria calcolata, con la colonna che le ha dato il frame.
#[derive(Debug)]
struct ResolvedGeometry {
    result_index: usize,
    source_column: String,
    srid: u32,
}

/// I CRS dichiarati dal piano, indicizzati per colonna.
fn declared_query_crs(
    operation: &QueryOperation,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<std::collections::BTreeMap<String, u32>> {
    let product = profile.product();
    let mut declared = std::collections::BTreeMap::new();
    for entry in &operation.declared_crs {
        ensure_identifier(&entry.column)?;
        if entry.srid == 0 {
            // Zero e l'indefinito OGC: dichiararlo non aggiunge niente a cio
            // che il motore gia risponde da solo, e darebbe l'aria di averlo
            // fatto.
            return Err(prepare_error(
                ErrorCategory::Crs,
                format!("CRS dichiarato {product} pari a zero: e l'indefinito OGC, non un sistema"),
            ));
        }
        if declared.insert(entry.column.clone(), entry.srid).is_some() {
            return Err(prepare_error(
                ErrorCategory::Crs,
                format!("colonna {product} con due CRS dichiarati: la fonte sarebbe ambigua"),
            ));
        }
    }
    Ok(declared)
}

/// Attribuisce un sistema di riferimento a ogni geometria calcolata, o rifiuta.
fn resolve_query_geometries(
    operation: &QueryOperation,
    declared: &std::collections::BTreeMap<String, u32>,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<Vec<ResolvedGeometry>> {
    let product = profile.product();
    let mut resolved = Vec::new();
    for (index, projection) in operation.projection.iter().enumerate() {
        let QueryExpression::Spatial {
            function,
            arguments,
        } = &projection.expression
        else {
            continue;
        };
        if !function.returns_geometry() {
            continue;
        }
        // Il gruppo cambierebbe di significato: la colonna di controllo e una
        // projection, e in una query raggruppata una projection non aggregata
        // deve appartenere al gruppo. Rifiutare qui costa una forma che
        // nessuno ha chiesto; ammetterla costerebbe un risultato arbitrario.
        if !operation.group_by.is_empty() || operation.distinct {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                format!(
                    "geometria calcolata {product} in una query raggruppata o DISTINCT: la \
                     conferma del CRS non e esprimibile in questa forma"
                ),
            ));
        }
        let rule = function.crs_rule();
        if rule != Some(plenora_database_core::spatial_catalog::CrsRule::Preserves) {
            // `argument` e `undefined` non sono impossibili in linea di
            // principio: sono non misurate. La differenza fra le due sta nel
            // catalogo, e questo messaggio non la nasconde dietro un «non
            // supportato» generico.
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                format!(
                    "geometria calcolata {product}: la regola di CRS di questa funzione non e \
                     «preserves», e nessuna misura ha ancora attraversato le altre"
                ),
            ));
        }
        // Gli argomenti **geometrici**, che sono quelli da cui un frame puo
        // arrivare. `ST_Buffer` ne ha uno solo e prende in piu una distanza:
        // guardare l'arieta invece del ruolo lo avrebbe rifiutato per un
        // argomento che con il sistema di riferimento non c'entra.
        let geometry_arguments = arguments
            .iter()
            .enumerate()
            .filter(|(index, _)| function.takes_geometry_at(*index))
            .map(|(_, argument)| argument)
            .collect::<Vec<_>>();
        let [QueryExpression::Column { column }] = geometry_arguments.as_slice() else {
            // La regola dice «il risultato sta dov'e l'ingresso», e questo ha
            // senso solo se l'ingresso e una colonna di cui qualcuno sa dire
            // dove sta. Un'espressione annidata sposterebbe la domanda, non la
            // risolverebbe.
            return Err(prepare_error(
                ErrorCategory::Crs,
                format!(
                    "geometria calcolata {product} su un argomento che non e una colonna: non \
                     c'e un CRS dichiarato da ereditare"
                ),
            ));
        };
        let srid = declared.get(&column.field).copied().ok_or_else(|| {
            prepare_error(
                ErrorCategory::Crs,
                format!(
                    "geometria calcolata {product} senza CRS dichiarato per la colonna \
                     d'ingresso: il contratto GeoArrow ne pubblica uno, e inventarlo sarebbe \
                     l'unico esito peggiore del rifiuto"
                ),
            )
        })?;
        resolved.push(ResolvedGeometry {
            result_index: index,
            source_column: column.field.clone(),
            srid,
        });
    }
    Ok(resolved)
}

/// Accoda una colonna `ST_SRID` per ogni colonna geometrica sorgente.
fn append_crs_checks(
    operation: &QueryOperation,
    geometries: &[ResolvedGeometry],
    profile: &dyn crate::profile::ProductProfile,
) -> Result<(QueryOperation, Vec<crate::types::MysqlCrsCheck>)> {
    let product = profile.product();
    if !profile
        .verified_spatial_functions()
        .contains(&SpatialFunction::Srid)
    {
        // La conferma si appoggia a una funzione che deve essere qualificata
        // quanto quella che apre: un controllo reso con una funzione non
        // dimostrata non e un controllo.
        return Err(unsupported(format!(
            "conferma del CRS {product} impossibile: ST_SRID non e fra le funzioni qualificate"
        )));
    }
    let mut checked = operation.clone();
    let mut checks = Vec::new();
    let mut seen = BTreeSet::new();
    for geometry in geometries {
        if !seen.insert(geometry.source_column.clone()) {
            continue;
        }
        checks.push(crate::types::MysqlCrsCheck {
            result_index: checked.projection.len(),
            column: geometry.source_column.clone(),
            expected: geometry.srid,
        });
        checked
            .projection
            .push(plenora_database_core::query::QueryProjection {
                // Senza alias, come sul path di lettura: un nome la farebbe
                // sembrare una colonna del risultato.
                alias: None,
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::Srid,
                    arguments: vec![QueryExpression::Column {
                        column: plenora_database_core::query::ColumnRef {
                            relation: None,
                            field: geometry.source_column.clone(),
                        },
                    }],
                },
            });
    }
    // Il piano cresciuto passa dallo stesso cancello dell'originale: le colonne
    // accodate sono SQL come le altre, e non hanno una corsia riservata.
    validate_query_operation(&checked, &Limits::default())?;
    Ok((checked, checks))
}

/// Traduce i metadati di colonna del prepared statement nel contratto Arrow.
///
/// # Errors
///
/// Restituisce `Schema` per un result set privo di colonne o con nomi non
/// utilizzabili, e `Unsupported` per tipi wire non ancora qualificati.
pub fn query_result_columns(columns: &[Column]) -> Result<Vec<MysqlColumnSpec>> {
    query_result_columns_with_profile(columns, &crate::profile::MYSQL_PROFILE)
}

/// I metadati di colonna, con il profilo che li interpreta.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn query_result_columns_with_profile(
    columns: &[Column],
    profile: &dyn crate::profile::ProductProfile,
) -> Result<Vec<MysqlColumnSpec>> {
    let product = profile.product();
    if columns.is_empty() {
        return Err(prepare_error(
            ErrorCategory::Schema,
            format!("QueryOperation {product} senza colonne risultanti"),
        ));
    }
    let mut names = BTreeSet::new();
    columns
        .iter()
        .map(|column| {
            let specification = profile.wire_column_spec(column)?;
            if !names.insert(specification.name.clone()) {
                return Err(prepare_error(
                    ErrorCategory::Schema,
                    format!("QueryOperation {product} con nomi colonna duplicati"),
                ));
            }
            Ok(specification)
        })
        .collect()
}

/// Posizione sintattica in cui un'espressione viene ispezionata.
///
/// `MySQL` ammette un aggregato soltanto nella lista di selezione, in HAVING e
/// in ORDER BY. Altrove il motore lo rifiuta, e dentro un altro aggregato la
/// forma non ha alcuna semantica definita. Una window function e ammessa
/// dallo stesso motore anche in ORDER BY, ma il provider la qualifica solo
/// nella lista di selezione.
#[derive(Clone, Copy)]
enum Scope {
    /// SELECT: aggregati e window function sono ammessi.
    Projection,
    /// HAVING e ORDER BY: gli aggregati sono ammessi, le window no.
    Aggregable,
    /// WHERE, GROUP BY, ON e operandi di una window: la clausola compare nel
    /// messaggio d'errore.
    RowOnly(&'static str),
    /// Argomento di un aggregato: nessun aggregato annidato.
    AggregateArgument,
}

/// Scope degli argomenti, di PARTITION BY e dell'ORDER BY di una window.
///
/// La finestra e definita riga per riga sopra il risultato gia filtrato: un
/// aggregato, un'altra window o una subquery al suo interno non avrebbero qui
/// alcun insieme su cui essere valutati.
const WINDOW_OPERAND: Scope = Scope::RowOnly("dentro una window function");

/// Sottoinsiemi non qualificati dell'AST, riconoscibili
/// dalla sola struttura della `QueryOperation`.
fn ensure_qualified_subset(query: &QueryOperation) -> Result<()> {
    if !query.common_table_expressions.is_empty() {
        return Err(unsupported("CTE non ancora qualificate nel path query"));
    }
    if !query.set_operations.is_empty() {
        return Err(unsupported(
            "set operation non ancora qualificate nel path query",
        ));
    }
    if query.derived_source.is_some() {
        return Err(unsupported(
            "subquery come sorgente non ancora qualificata nel path query",
        ));
    }
    if query.locking.is_some() {
        return Err(unsupported(
            "locking esplicito non ancora qualificato nel path query",
        ));
    }
    // DISTINCT ON e una estensione PostgreSQL: qui la forma qualificata e
    // DISTINCT, l'equivalente si esprime con GROUP BY.
    if !query.distinct_on.is_empty() {
        return Err(unsupported(
            "DISTINCT ON non esiste in questo dialetto: la forma qualificata e DISTINCT",
        ));
    }
    Ok(())
}

/// Una window e valutata dopo il raggruppamento e prima di DISTINCT.
///
/// Entrambe le combinazioni hanno in `MySQL` una semantica precisa, ma
/// dimostrarla richiede prove non ancora disponibili: restano chiuse
/// invece di essere renderizzate su una semantica assunta.
fn ensure_window_interactions(query: &QueryOperation, grouped: bool) -> Result<()> {
    if !query
        .projection
        .iter()
        .any(|projection| contains_window(&projection.expression))
    {
        return Ok(());
    }
    if grouped {
        return Err(unsupported(
            "window insieme a GROUP BY o a un aggregato di gruppo \
 non ancora qualificata nel path query",
        ));
    }
    if query.distinct {
        return Err(unsupported(
            "window insieme a DISTINCT non ancora qualificata nel path query",
        ));
    }
    Ok(())
}

fn ensure_qualified_shape(
    query: &QueryOperation,
    database: &str,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<()> {
    ensure_qualified_subset(query)?;
    if query.row_limit.is_some() && query.order_by.is_empty() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "LIMIT richiede ORDER BY esplicito per un risultato deterministico",
        ));
    }
    let relations = ensure_relations(query, database, profile)?;

    let aggregated = query
        .projection
        .iter()
        .any(|projection| contains_aggregate(&projection.expression))
        || query.having.as_ref().is_some_and(contains_aggregate)
        || query
            .order_by
            .iter()
            .any(|ordering| contains_aggregate(&ordering.expression));
    let grouped = aggregated || !query.group_by.is_empty();
    if query.having.is_some() && !grouped {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "HAVING senza aggregazione: il filtro per riga appartiene a WHERE",
        ));
    }

    ensure_window_interactions(query, grouped)?;

    for key in &query.group_by {
        // Un intero letterale in GROUP BY e una posizione ordinale per MySQL:
        // un placeholder nella stessa posizione renderebbe la chiave ambigua.
        if matches!(key, QueryExpression::Parameter { .. }) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "parametro in GROUP BY: la chiave sarebbe ambigua",
            ));
        }
        ensure_expression(key, Scope::RowOnly("in GROUP BY"), &relations, profile)?;
    }
    for projection in &query.projection {
        if let Some(alias) = &projection.alias {
            ensure_identifier(alias)?;
        }
        match &projection.expression {
            // Il wildcard e una voce di proiezione valida solo fuori da una
            // query aggregata: dentro un gruppo non e determinato.
            QueryExpression::Wildcard { relation } => {
                if let Some(relation) = relation {
                    ensure_known_relation(relation, &relations)?;
                }
                if grouped {
                    return Err(prepare_error(
                        ErrorCategory::InvalidPlan,
                        "wildcard in una query aggregata non e determinato dal gruppo",
                    ));
                }
            }
            expression => ensure_expression(expression, Scope::Projection, &relations, profile)?,
        }
    }
    if let Some(filter) = &query.filter {
        ensure_expression(filter, Scope::RowOnly("in WHERE"), &relations, profile)?;
    }
    if let Some(having) = &query.having {
        ensure_expression(having, Scope::Aggregable, &relations, profile)?;
    }
    for ordering in &query.order_by {
        ensure_expression(&ordering.expression, Scope::Aggregable, &relations, profile)?;
    }
    if grouped {
        ensure_group_determinism(query)?;
    }
    if query.distinct {
        ensure_distinct_determinism(query)?;
    }
    Ok(())
}

/// Nome con cui una relazione e visibile alle colonne qualificate.
///
/// Senza alias `MySQL` espone la relazione con il nome della tabella, mai con
/// il nome qualificato dal database.
fn relation_name(source: &QuerySource) -> &str {
    source.alias.as_deref().unwrap_or(&source.object.object)
}

/// Qualifica la sorgente di base e ogni join fisico, restituendo l'insieme
/// dei nomi di relazione visibili alla query.
///
/// Il controllo di unicita precede la rete: due relazioni con lo stesso nome
/// renderebbero ambigua ogni colonna qualificata e `MySQL` se ne accorgerebbe
/// solo al prepare, con un messaggio che non identifica il piano.
fn ensure_relations<'a>(
    query: &'a QueryOperation,
    database: &str,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<BTreeSet<&'a str>> {
    let source = query
        .source
        .as_ref()
        .ok_or_else(|| prepare_error(ErrorCategory::InvalidPlan, "query senza sorgente fisica"))?;
    ensure_source(source, database)?;
    let mut relations = BTreeSet::from([relation_name(source)]);
    for join in &query.joins {
        // `MySQL` 8.0.14 supporta LATERAL, ma il renderer condiviso lo emette
        // solo per PostgreSQL: la forma resta non dimostrata, non assente.
        if join.lateral {
            return Err(unsupported(
                "join LATERAL non ancora qualificati nel path query",
            ));
        }
        if join.derived_source.is_some() {
            return Err(unsupported(
                "join su subquery non ancora qualificati nel path query",
            ));
        }
        match join.kind {
            JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Cross => {}
            // Il renderer condiviso emetterebbe `FULL JOIN`, che MySQL
            // rifiuta: qui il limite e del motore, non della qualificazione.
            JoinKind::Full => {
                return Err(unsupported(
                    "FULL JOIN non esiste in questo dialetto: la forma equivalente e l'unione \
 di un LEFT JOIN e di un RIGHT JOIN",
                ));
            }
        }
        let joined = join.source.as_ref().ok_or_else(|| {
            prepare_error(ErrorCategory::InvalidPlan, "join senza sorgente fisica")
        })?;
        ensure_source(joined, database)?;
        if !relations.insert(relation_name(joined)) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "alias di relazione duplicato fra sorgente e join",
            ));
        }

        // Una clausola ON vede la relazione di base, i join precedenti e la
        // relazione appena introdotta, ma non i join che seguono.
        match (&join.kind, &join.on) {
            (JoinKind::Cross, None) => {}
            (JoinKind::Cross, Some(_)) => {
                return Err(prepare_error(
                    ErrorCategory::InvalidPlan,
                    "CROSS JOIN con clausola ON",
                ));
            }
            (_, None) => {
                return Err(prepare_error(
                    ErrorCategory::InvalidPlan,
                    "JOIN senza clausola ON",
                ));
            }
            // ON e valutata per riga durante la costruzione del join: un
            // aggregato o una window function non hanno qui alcun gruppo su
            // cui essere definiti.
            (_, Some(on)) => {
                ensure_expression(on, Scope::RowOnly("in ON"), &relations, profile)?;
            }
        }
    }
    Ok(relations)
}

fn ensure_known_relation(relation: &str, relations: &BTreeSet<&str>) -> Result<()> {
    ensure_identifier(relation)?;
    if !relations.contains(relation) {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "riferimento a una relazione assente da FROM e dai join",
        ));
    }
    Ok(())
}

/// `MySQL` ammette un aggregato solo dove esiste un gruppo su cui valutarlo.
fn ensure_aggregable(scope: Scope) -> Result<()> {
    match scope {
        Scope::Projection | Scope::Aggregable => Ok(()),
        Scope::RowOnly(clause) => Err(prepare_error(
            ErrorCategory::InvalidPlan,
            format!("funzione aggregata non ammessa {clause}"),
        )),
        Scope::AggregateArgument => Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "aggregato annidato in un altro aggregato",
        )),
    }
}

#[allow(clippy::too_many_lines)]
fn ensure_expression(
    expression: &QueryExpression,
    scope: Scope,
    relations: &BTreeSet<&str>,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<()> {
    let mut stack = vec![(expression, scope)];
    while let Some((item, scope)) = stack.pop() {
        match item {
            QueryExpression::Wildcard { .. } => {
                return Err(prepare_error(
                    ErrorCategory::InvalidPlan,
                    "wildcard ammesso solo come voce di proiezione o come COUNT(*)",
                ));
            }
            QueryExpression::Column { column } => {
                if let Some(relation) = &column.relation {
                    ensure_known_relation(relation, relations)?;
                }
                ensure_identifier(&column.field)?;
            }
            QueryExpression::Parameter { .. } => {}
            QueryExpression::Scalar {
                function,
                arguments,
            } => match function {
                ScalarFunction::Lower | ScalarFunction::Upper | ScalarFunction::Coalesce => {
                    stack.extend(arguments.iter().map(|argument| (argument, scope)));
                }
                ScalarFunction::Count
                | ScalarFunction::Sum
                | ScalarFunction::Average
                | ScalarFunction::Minimum
                | ScalarFunction::Maximum => {
                    ensure_aggregable(scope)?;
                    if is_count_star(*function, arguments) {
                        continue;
                    }
                    stack.extend(
                        arguments
                            .iter()
                            .map(|argument| (argument, Scope::AggregateArgument)),
                    );
                }
                ScalarFunction::RowNumber
                | ScalarFunction::Rank
                | ScalarFunction::DenseRank
                | ScalarFunction::Lag
                | ScalarFunction::Lead => {
                    return Err(prepare_error(
                        ErrorCategory::InvalidPlan,
                        "funzione window usata senza clausola OVER",
                    ));
                }
            },
            QueryExpression::Compare { left, right, .. } => {
                stack.push((left, scope));
                stack.push((right, scope));
            }
            QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
                stack.extend(arguments.iter().map(|argument| (argument, scope)));
            }
            QueryExpression::IsNull { expression, .. } => stack.push((expression, scope)),
            QueryExpression::Window {
                function,
                arguments,
                partition_by,
                order_by,
                frame,
            } if matches!(scope, Scope::Projection) => {
                ensure_window(
                    *function,
                    arguments,
                    partition_by,
                    order_by,
                    frame.as_ref(),
                    relations,
                    profile,
                )?;
            }
            QueryExpression::Window { .. } | QueryExpression::SpatialWindow { .. } => {
                return Err(unsupported(
                    "window function non ancora qualificata nel path query",
                ));
            }
            QueryExpression::Spatial {
                function,
                arguments,
            } => {
                // Accetta soltanto le funzioni in VERIFIED_SPATIAL_FUNCTIONS.
                // Le altre restano Unsupported senza una prova live dedicata.
                if !profile.verified_spatial_functions().contains(function) {
                    return Err(unsupported(format!(
                        "funzione spatial '{function:?}' non qualificata su {}",
                        profile.product()
                    )));
                }
                stack.extend(arguments.iter().map(|argument| (argument, scope)));
            }
            QueryExpression::SpatialOperator { .. } => {
                return Err(unsupported(
                    "spatial operator non ancora qualificato nel path query",
                ));
            }
            QueryExpression::ScalarSubquery { .. }
            | QueryExpression::Exists { .. }
            | QueryExpression::InSubquery { .. } => {
                return Err(unsupported(
                    "subquery non ancora qualificata nel path query",
                ));
            }
        }
    }
    Ok(())
}

/// Qualifica una window function scalare della lista di selezione.
///
/// La chiamata di testa e l'unico punto in cui un aggregato puo comparire:
/// argomenti, PARTITION BY e ORDER BY della finestra sono valutati per riga e
/// devono restare scalari e riferiti alle sole relazioni presenti in FROM e
/// nei join.
fn ensure_window(
    function: ScalarFunction,
    arguments: &[QueryExpression],
    partition_by: &[QueryExpression],
    order_by: &[QueryOrdering],
    frame: Option<&WindowFrame>,
    relations: &BTreeSet<&str>,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<()> {
    match function {
        ScalarFunction::Rank | ScalarFunction::DenseRank => {
            // Il rango e una posizione, non una funzione dei valori di riga:
            // MySQL rifiuta qualunque argomento.
            if !arguments.is_empty() {
                return Err(prepare_error(
                    ErrorCategory::InvalidPlan,
                    "funzione di rango con argomenti: la posizione non ne accetta",
                ));
            }
            ensure_peer_stable_window(order_by, frame)?;
        }
        ScalarFunction::RowNumber | ScalarFunction::Lag | ScalarFunction::Lead => {
            return Err(unsupported(TOTAL_ORDER_NOT_PROVABLE));
        }
        ScalarFunction::Count
        | ScalarFunction::Sum
        | ScalarFunction::Average
        | ScalarFunction::Minimum
        | ScalarFunction::Maximum => {
            if let Some(frame) = frame {
                ensure_window_frame(frame, order_by)?;
            }
        }
        // Il core rifiuta gia questa forma in fase Validate; il provider
        // ripete il controllo per non dipendere dall'ordine dei validatori.
        ScalarFunction::Lower | ScalarFunction::Upper | ScalarFunction::Coalesce => {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "funzione scalare usata come window function",
            ));
        }
    }
    if !is_count_star(function, arguments) {
        for argument in arguments {
            ensure_expression(argument, WINDOW_OPERAND, relations, profile)?;
        }
    }
    for partition in partition_by {
        ensure_expression(partition, WINDOW_OPERAND, relations, profile)?;
    }
    for ordering in order_by {
        ensure_expression(&ordering.expression, WINDOW_OPERAND, relations, profile)?;
    }
    Ok(())
}

/// Motivo per cui una window che legge la posizione della riga dentro le
/// righe pari resta chiusa.
///
/// `ROW_NUMBER`, `LAG`, `LEAD` e ogni frame `ROWS` numerano le righe una per
/// una: fra righe con la stessa chiave d'ordine la numerazione dipende
/// dall'ordine fisico scelto dal motore e cambia da un'esecuzione all'altra.
/// Sarebbero riproducibili solo su una chiave che e un ordine totale univoco,
/// ma l'unicita e una proprieta dei dati e dei vincoli della tabella: l'AST
/// portabile non la esprime e il provider non puo dedurla prima della rete.
/// Assumerla dalla sola presenza di un ORDER BY non vuoto pubblicherebbe come
/// deterministico un valore che non lo e.
const TOTAL_ORDER_NOT_PROVABLE: &str =
    "window che numera le righe pari (ROW_NUMBER, LAG, LEAD, frame ROWS) \
 non ancora qualificata nel path query: l'AST portabile non dimostra che la \
 chiave d'ordine sia un ordine totale univoco e con chiavi duplicate il \
 valore non sarebbe riproducibile";

/// Vincoli comuni alle window di rango stabili fra pari: `RANK` e `DENSE_RANK`.
///
/// Le righe con la stessa chiave d'ordine ricevono lo stesso rango, quindi un
/// ORDER BY non vuoto basta a renderle riproducibili senza dimostrare che la
/// chiave sia univoca. `MySQL` 8.4 accetta invece la sintassi del frame anche
/// su queste funzioni e poi la ignora: renderizzarla pubblicherebbe un piano
/// che il motore non esegue.
fn ensure_peer_stable_window(
    order_by: &[QueryOrdering],
    frame: Option<&WindowFrame>,
) -> Result<()> {
    if order_by.is_empty() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "window di rango senza ORDER BY: \
 la posizione della riga non sarebbe riproducibile",
        ));
    }
    if frame.is_some() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "frame esplicito su una window di rango: \
 il motore lo ignora e il piano reso non sarebbe quello richiesto",
        ));
    }
    Ok(())
}

/// Qualifica il frame di una window aggregata.
fn ensure_window_frame(frame: &WindowFrame, order_by: &[QueryOrdering]) -> Result<()> {
    match frame.units {
        // ROWS conta le posizioni dentro la partizione: fra righe pari la
        // porzione dipende dall'ordine fisico scelto dal motore.
        WindowFrameUnits::Rows => return Err(unsupported(TOTAL_ORDER_NOT_PROVABLE)),
        // RANGE con offset confronta i valori della chiave d'ordine, non le
        // posizioni: su una chiave temporale MySQL pretende un INTERVAL, che
        // l'AST portabile non sa esprimere. Senza il tipo della chiave, noto
        // solo al server, l'offset nudo non e dimostrabile prima della rete.
        WindowFrameUnits::Range => {
            if matches!(&frame.start, WindowFrameBound::UnboundedFollowing)
                || matches!(
                    frame.end.as_ref(),
                    Some(WindowFrameBound::UnboundedPreceding)
                )
            {
                return Err(prepare_error(
                    ErrorCategory::InvalidPlan,
                    "frame RANGE con limite iniziale o finale non valido",
                ));
            }
            if frame_offset(&frame.start) || frame.end.as_ref().is_some_and(frame_offset) {
                return Err(unsupported(
                    "frame RANGE con offset non ancora qualificato nel path query: \
 l'AST portabile non esprime l'INTERVAL richiesto da una chiave temporale",
                ));
            }
        }
        WindowFrameUnits::Groups => {
            return Err(unsupported(
                "la clausola GROUPS non esiste in questo dialetto: \
 le unita di frame disponibili sono ROWS e RANGE",
            ));
        }
    }
    if order_by.is_empty() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "frame di window senza ORDER BY: \
 la porzione di partizione non sarebbe determinata",
        ));
    }
    Ok(())
}

const fn frame_offset(bound: &WindowFrameBound) -> bool {
    matches!(
        bound,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    )
}

/// `COUNT(*)` e l'unica forma in cui il wildcard sopravvive dentro una
/// chiamata: `COUNT(t.*)` e ogni altro aggregato con `*` non sono validi.
fn is_count_star(function: ScalarFunction, arguments: &[QueryExpression]) -> bool {
    matches!(function, ScalarFunction::Count)
        && matches!(arguments, [QueryExpression::Wildcard { relation: None }])
}

const fn is_aggregate(function: ScalarFunction) -> bool {
    matches!(
        function,
        ScalarFunction::Count
            | ScalarFunction::Sum
            | ScalarFunction::Average
            | ScalarFunction::Minimum
            | ScalarFunction::Maximum
    )
}

fn contains_aggregate(expression: &QueryExpression) -> bool {
    !walk_query_expression(expression, |node| match node {
        QueryWalkNode::Expression(QueryExpression::Scalar { function, .. })
            if is_aggregate(*function) =>
        {
            QueryWalkControl::Break
        }
        QueryWalkNode::Expression(
            QueryExpression::Scalar { .. }
            | QueryExpression::Compare { .. }
            | QueryExpression::And { .. }
            | QueryExpression::Or { .. }
            | QueryExpression::IsNull { .. },
        ) => QueryWalkControl::Continue,
        QueryWalkNode::Operation(_) | QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => {
            QueryWalkControl::Skip
        }
    })
}

/// Segnala una window scalare ovunque compaia nell'espressione.
///
/// La ricerca non entra nella finestra: argomenti, PARTITION BY e ORDER BY
/// sono gia vincolati a restare per riga da `ensure_window`.
fn contains_window(expression: &QueryExpression) -> bool {
    !walk_query_expression(expression, |node| match node {
        QueryWalkNode::Expression(QueryExpression::Window { .. }) => QueryWalkControl::Break,
        QueryWalkNode::Expression(
            QueryExpression::Scalar { .. }
            | QueryExpression::Compare { .. }
            | QueryExpression::And { .. }
            | QueryExpression::Or { .. }
            | QueryExpression::IsNull { .. },
        ) => QueryWalkControl::Continue,
        QueryWalkNode::Operation(_) | QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => {
            QueryWalkControl::Skip
        }
    })
}

/// Verifica che ogni espressione pubblicata da una query aggregata sia
/// determinata dal gruppo, cioe sia un aggregato, una costante o una chiave di
/// GROUP BY.
fn ensure_group_determinism(query: &QueryOperation) -> Result<()> {
    for projection in &query.projection {
        if !is_determined_by_group(&projection.expression, &query.group_by) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "proiezione non aggregata e assente da GROUP BY",
            ));
        }
    }
    if let Some(having) = &query.having {
        if !is_determined_by_group(having, &query.group_by) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "HAVING su un'espressione assente da GROUP BY",
            ));
        }
    }
    for ordering in &query.order_by {
        if !is_determined_by_group(&ordering.expression, &query.group_by)
            && !matches_projection(&ordering.expression, query)
        {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "ORDER BY su un'espressione assente da GROUP BY",
            ));
        }
    }
    Ok(())
}

/// Con DISTINCT le righe sono l'insieme dei valori proiettati: ordinare per
/// un'espressione esterna alla proiezione produrrebbe un ordine arbitrario.
fn ensure_distinct_determinism(query: &QueryOperation) -> Result<()> {
    for ordering in &query.order_by {
        if !matches_projection(&ordering.expression, query) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "ORDER BY fuori dalla proiezione DISTINCT: l'ordine non sarebbe riproducibile",
            ));
        }
    }
    Ok(())
}

fn is_determined_by_group(expression: &QueryExpression, keys: &[QueryExpression]) -> bool {
    if keys.contains(expression) {
        return true;
    }
    match expression {
        QueryExpression::Parameter { .. } => true,
        QueryExpression::Scalar {
            function,
            arguments,
        } => {
            is_aggregate(*function)
                || arguments
                    .iter()
                    .all(|argument| is_determined_by_group(argument, keys))
        }
        QueryExpression::Compare { left, right, .. } => {
            is_determined_by_group(left, keys) && is_determined_by_group(right, keys)
        }
        QueryExpression::And { arguments } | QueryExpression::Or { arguments } => arguments
            .iter()
            .all(|argument| is_determined_by_group(argument, keys)),
        QueryExpression::IsNull { expression, .. } => is_determined_by_group(expression, keys),
        _ => false,
    }
}

fn matches_projection(expression: &QueryExpression, query: &QueryOperation) -> bool {
    query.projection.iter().any(|projection| {
        if &projection.expression == expression {
            return true;
        }
        match (&projection.expression, expression) {
            // Un wildcard non qualificato proietta ogni colonna disponibile,
            // di base e di ogni join.
            (QueryExpression::Wildcard { relation: None }, _) => true,
            // `t`.* copre soltanto le colonne della relazione `t`: con un
            // join le altre relazioni restano fuori dalla proiezione.
            (
                QueryExpression::Wildcard {
                    relation: Some(relation),
                },
                QueryExpression::Column { column },
            ) => column.relation.as_deref() == Some(relation.as_str()),
            (QueryExpression::Wildcard { .. }, _) => false,
            _ => match (expression, &projection.alias) {
                (QueryExpression::Column { column }, Some(alias)) => {
                    column.relation.is_none() && &column.field == alias
                }
                _ => false,
            },
        }
    })
}

fn ensure_source(source: &QuerySource, database: &str) -> Result<()> {
    if source
        .object
        .catalog
        .as_deref()
        .is_some_and(|catalog| catalog != database)
        || source
            .object
            .schema
            .as_deref()
            .is_some_and(|schema| schema != database)
    {
        return Err(unsupported(
            "accesso cross-database non supportato dal provider",
        ));
    }
    // MySQL usa nomi a due componenti: `database`.`tabella`. Un AST con
    // catalog e schema insieme renderizzerebbe tre componenti.
    if source.object.catalog.is_some() && source.object.schema.is_some() {
        return Err(unsupported(
            "il dialetto non ammette nomi qualificati a tre componenti",
        ));
    }
    for part in source
        .object
        .catalog
        .iter()
        .chain(source.object.schema.iter())
        .chain(std::iter::once(&source.object.object))
        .chain(source.alias.iter())
    {
        ensure_identifier(part)?;
    }
    Ok(())
}

fn ensure_identifier(value: &str) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.chars().count() > MAX_IDENTIFIER_CHARACTERS
    {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "identificatore vuoto, con NUL o oltre 64 caratteri",
        ));
    }
    Ok(())
}

#[allow(clippy::redundant_pub_crate)]
pub(crate) fn unsupported(message: impl Into<String>) -> DatabaseError {
    DatabaseError::unsupported(
        crate::profile::PROVISIONAL_KIND,
        ErrorPhase::Prepare,
        message,
    )
}

#[allow(clippy::redundant_pub_crate)]
pub(crate) fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

#[cfg(test)]
#[path = "query_tests.rs"]
mod tests;
