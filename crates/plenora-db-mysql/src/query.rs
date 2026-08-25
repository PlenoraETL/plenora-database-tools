//! Path `QueryOperation` scalare a sorgente singola per `MySQL`.
//!
//! L'AST portabile viene renderizzato dal dialect `MySQL` condiviso e lo
//! schema di output arriva dai metadati di colonna di `COM_STMT_PREPARE`:
//! `MySQL` non ha un equivalente di `sys.dm_exec_describe_first_result_set`,
//! quindi la descrizione del prepared statement e l'unica fonte autoritativa.
//!
//! Questa tranche qualifica proiezioni scalari, DISTINCT, aggregazione
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
//! motivo di fondo: sono sottoinsiemi dell'AST non ancora dimostrati su
//! `MySQL`, non costrutti necessariamente assenti dal motore. Lo spatial no:
//! era in quell'elenco e non c'e piu, perche le funzioni di
//! `VERIFIED_SPATIAL_FUNCTIONS` si renderizzano e si eseguono — cio che resta
//! chiuso e la *window* spatial, non lo spatial. FULL JOIN e
//! invece un'assenza reale: `MySQL` 8.4 non ha una forma nativa equivalente,
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
    validate_query_operation, JoinKind, QueryExpression, QueryOperation, QueryOrdering,
    QuerySource, ScalarFunction, SpatialFunction, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use plenora_database_sql::RenderedSql;
use std::collections::BTreeSet;

/// Sottoinsieme delle funzioni spatial `MySQL` qualificate per l'AST
/// portabile — pubblicate in `probe_capabilities` e utilizzabili via
/// `Provider::query` sia come proiezioni scalari sia (per i predicati) come
/// filtri WHERE.
///
/// Fonte: rendering dialect condiviso in `plenora-database-sql` (unificato
/// col dialect `Postgres` per il subset `ST_*`). Le funzioni escluse
/// (`X`/`Y`/`Z`/`M`, `AsGeoJson`, `DWithin`, `Transform`, ecc.) restano
/// `Unsupported` finché non hanno una prova live su `MySQL`.
///
/// # Cosa prova questa lista, e da quando
///
/// Per un po' ha promesso piu di quanto avesse. Il nome diceva "verified" e le
/// prove live ne attraversavano **due** — `Area` e `Intersects`; il test di
/// capability contava la lista e ci cercava dentro cinque nomi, che e una
/// verifica sulla costante, non sul motore. Le altre ventiquattro erano una
/// deduzione dal dialect condiviso con `PostgreSQL`, cioe esattamente il tipo
/// di affermazione che la regola 1 vieta.
///
/// La lista non e descrittiva: `render_query` **rifiuta** ogni funzione che
/// non vi compare. Pubblicarne una che poi fallisce non e un'imprecisione in
/// un documento, e una promessa che il provider fa al chiamante e non
/// mantiene.
///
/// Ora la sonda `live_v12_every_verified_spatial_function_executes` le
/// attraversa **tutte**, una per una, contro il riferimento. Interrogata sulle
/// ventisei di allora ne ha bocciate dodici in un colpo solo, e la lista si e
/// accorciata con la misura in mano.
///
/// # Le undici che sono uscite, e perche
///
/// Non undici ragioni: **una**. `StartPoint`, `EndPoint`, `PointN`, `Buffer`,
/// `Envelope`, `Intersection`, `Union`, `Difference`, `SymDifference`,
/// `ConvexHull` e `Centroid` restituiscono una geometria, e il mapper del
/// result set di questo provider rifiuta `MYSQL_TYPE_GEOMETRY`: una geometria
/// in uscita da una query non porta SRID ne profilo dimensionale dimostrati, e
/// il renderer non incapsula la colonna in `ST_AsBinary`. Sono bloccate sul
/// preflight SRID, non sul nome: il giorno che quel preflight esiste tornano
/// tutte insieme, e la sonda lo dira.
///
/// Due cose misurate lungo la strada, che vale la pena non riscoprire:
/// `ST_NDims` e `ST_NPoints` non esistono su `MySQL` — li rende
/// `dialect_spatial_name` come `ST_Dimension` e `ST_NumPoints` — e `ST_Union`
/// su `MySQL` e **binario soltanto**, mentre il contratto ne ammette da uno a
/// tre argomenti. Quando `Union` rientrera, la forma unaria restera comunque
/// senza controparte.
///
/// # Cosa dimostra, e cosa no
///
/// La sonda gira su due geometrie — una lineare e una areale — e chiede a
/// ciascuna funzione di eseguire su almeno una delle due: `ST_Area` su una
/// `LINESTRING` risponde `3516`, e sarebbe stata una falsa assenza. Quello che
/// dimostra e che il renderer produce SQL che `MySQL` esegue su un SRID
/// cartesiano, non che ogni funzione valga su ogni sistema di riferimento —
/// che e una domanda diversa, e non e questa lista a rispondervi.
///
/// **15 funzioni**:
/// - metadata (7): `GeometryType`, `Srid`, `Dimensions`, `NPoints`,
///   `IsEmpty`, `IsValid`, `IsClosed`
/// - predicate binary (5): `Intersects`, `Contains`, `Within`, `Disjoint`,
///   `Equals`
/// - metriche (3): `Distance`, `Area`, `Length`
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
    // Le nove che `raw.spatial_candidate_functions` ha trovato presenti fra le
    // ventisei mai chieste. Presenti non vuol dire qualificate: la sonda
    // `live_v12_every_verified_spatial_function_executes` le attraversa una per
    // una, e cio che non esegue esce da questa lista con la misura in mano —
    // come ne sono uscite undici quando scese da ventisei a quindici.
    //
    // `Relate` e `CoveredBy` non ci sono, e su `MariaDB` si: le due liste non
    // sono piu una il sottoinsieme dell'altra, e non c'e ragione perche lo
    // siano.
    SpatialFunction::X,
    SpatialFunction::Y,
    SpatialFunction::IsSimple,
    SpatialFunction::Touches,
    SpatialFunction::Crosses,
    SpatialFunction::Overlaps,
    SpatialFunction::HausdorffDistance,
    SpatialFunction::FrechetDistance,
    SpatialFunction::AsGeoJson,
];

/// Le funzioni spatial qualificate su `MariaDB`.
///
/// **Quattordici**, e la differenza con le quindici di `MySQL` e una sola:
/// `IsValid`. Non e una scelta di prudenza — e la prima divergenza misurata
/// **fra le due major di `MariaDB`** in tutto il documento di evidenza, e vale
/// pena dire come si presenta:
///
/// * `MariaDB 12.3` esegue tutte e quindici;
/// * `MariaDB 11.8 LTS` risponde `1305` a `ST_IsValid` — la funzione non
///   esiste.
///
/// Pubblicarne quindici funzionerebbe sulla 12.3 e romperebbe sulla LTS, che e
/// la versione con piu installazioni. La lista e percio l'**intersezione** delle
/// major supportate, che e la sola forma onesta quando il profilo e uno solo
/// per il prodotto: una capability e una promessa a chi non sa su quale minor
/// atterrera.
///
/// Il giorno in cui il profilo si sdoppiasse per major, `IsValid` tornerebbe
/// sulla 12 — e allora questa lista sarebbe il posto in cui dirlo.
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
    // Le otto presenti su **entrambe** le major, dalle ventisei mai chieste.
    //
    // `CoveredBy` resta fuori, ed e la seconda divergenza fra le due major di
    // questo prodotto: la 12.3 ce l'ha, la 11.8 LTS no. Stessa regola di
    // `IsValid` — la lista e l'intersezione, perche una capability e una
    // promessa a chi non sa su quale minor atterrera.
    //
    // `HausdorffDistance` e `FrechetDistance` sono su `MySQL` e non qui.
    //
    // `Relate` ha fatto il percorso inverso e vale la pena raccontarlo: la
    // sonda delle candidate lo aveva trovato **presente** — il server ha la
    // funzione — ed e entrato in questa lista. Poi il gate lo ha bocciato con
    // 1582, numero di parametri sbagliato: `MariaDB` lo vuole a tre argomenti
    // — le due geometrie e il pattern DE-9IM — e il contratto ne ammette anche
    // due, che e l'arieta con cui un chiamante puo scriverlo.
    //
    // Esiste, quindi, e non e utilizzabile nella forma che il contratto
    // permette. E' la stessa regola che tolse `Union` dalla lista di `MySQL`:
    // una funzione e qualificata quando lo e a **ogni** arieta che il piano
    // ammette, non quando ne esiste una che funziona.
    SpatialFunction::X,
    SpatialFunction::Y,
    SpatialFunction::IsSimple,
    SpatialFunction::Touches,
    SpatialFunction::Crosses,
    SpatialFunction::Overlaps,
    SpatialFunction::AsGeoJson,
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
/// La lista era una sola per entrambi, e la conseguenza era che il renderer
/// ammetteva cio che `MySQL` aveva verificato anche dove non valeva: su
/// `MariaDB 11.8` un piano con `ST_IsValid` sarebbe passato di qui e sarebbe
/// morto sul server con 1305, mentre la capability pubblicata diceva —
/// giustamente — che quella funzione non c'e. Il cancello e la promessa devono
/// leggere la stessa lista, altrimenti una delle due mente.
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
    validate_query_operation(operation, &Limits::default())?;
    ensure_qualified_shape(operation, database, profile)?;
    mysql_renderer().render_query(operation)
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
/// dallo stesso motore anche in ORDER BY, ma questa tranche la qualifica solo
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

/// Sottoinsiemi dell'AST che questa tranche non tocca affatto, riconoscibili
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
/// dimostrarla richiede prove che questa tranche non porta: restano chiuse
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
            // Il renderer condiviso emetterebbe `FULL JOIN`, che MySQL 8.4
            // rifiuta: qui il limite e del motore, non della tranche.
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
                // v1.2: accetta funzioni ∈ VERIFIED_SPATIAL_FUNCTIONS.
                // Le altre restano Unsupported finché non hanno un test
                // live dedicato su MySQL 8.4.
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
    let mut stack = vec![expression];
    while let Some(item) = stack.pop() {
        match item {
            QueryExpression::Scalar {
                function,
                arguments,
            } => {
                if is_aggregate(*function) {
                    return true;
                }
                stack.extend(arguments);
            }
            QueryExpression::Compare { left, right, .. } => {
                stack.push(left);
                stack.push(right);
            }
            QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
                stack.extend(arguments);
            }
            QueryExpression::IsNull { expression, .. } => stack.push(expression),
            QueryExpression::Wildcard { .. }
            | QueryExpression::Column { .. }
            | QueryExpression::Parameter { .. }
            | QueryExpression::Spatial { .. }
            | QueryExpression::SpatialOperator { .. }
            | QueryExpression::Window { .. }
            | QueryExpression::SpatialWindow { .. }
            | QueryExpression::ScalarSubquery { .. }
            | QueryExpression::Exists { .. }
            | QueryExpression::InSubquery { .. } => {}
        }
    }
    false
}

/// Segnala una window scalare ovunque compaia nell'espressione.
///
/// La ricerca non entra nella finestra: argomenti, PARTITION BY e ORDER BY
/// sono gia vincolati a restare per riga da `ensure_window`.
fn contains_window(expression: &QueryExpression) -> bool {
    let mut stack = vec![expression];
    while let Some(item) = stack.pop() {
        match item {
            QueryExpression::Window { .. } => return true,
            QueryExpression::Scalar { arguments, .. }
            | QueryExpression::And { arguments }
            | QueryExpression::Or { arguments } => stack.extend(arguments),
            QueryExpression::Compare { left, right, .. } => {
                stack.push(left);
                stack.push(right);
            }
            QueryExpression::IsNull { expression, .. } => stack.push(expression),
            QueryExpression::Wildcard { .. }
            | QueryExpression::Column { .. }
            | QueryExpression::Parameter { .. }
            | QueryExpression::Spatial { .. }
            | QueryExpression::SpatialOperator { .. }
            | QueryExpression::SpatialWindow { .. }
            | QueryExpression::ScalarSubquery { .. }
            | QueryExpression::Exists { .. }
            | QueryExpression::InSubquery { .. } => {}
        }
    }
    false
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
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(crate::profile::PROVISIONAL_KIND),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MysqlColumnKind;
    use mysql_async::consts::{ColumnFlags, ColumnType};
    use plenora_database_core::arrow::schema::DataType;
    use plenora_database_core::plan::ProviderKind;
    use plenora_database_core::plan::{ComparisonOperator, ObjectRef, SortDirection};
    use plenora_database_core::protocol;
    use plenora_database_core::query::{
        ColumnRef, CommonTableExpression, JoinKind, QueryDerivedSource, QueryJoin, QueryLock,
        QueryLockStrength, QueryLockWait, QueryProjection, QuerySetOperation, QuerySetOperator,
        SpatialFunction,
    };

    fn source(object: &str) -> QuerySource {
        QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: object.to_owned(),
            },
            alias: None,
        }
    }

    fn column(field: &str) -> QueryExpression {
        QueryExpression::Column {
            column: ColumnRef {
                relation: None,
                field: field.to_owned(),
            },
        }
    }

    fn base_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(source("events")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: column("event_id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        }
    }

    #[test]
    fn scalar_single_source_renders_with_backticks_and_positional_binds() {
        let mut query = base_query();
        query.projection.push(QueryProjection {
            expression: QueryExpression::Scalar {
                function: ScalarFunction::Lower,
                arguments: vec![column("label")],
            },
            alias: Some("lowered".to_owned()),
        });
        query.filter = Some(QueryExpression::Compare {
            left: Box::new(column("event_id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        query.order_by = vec![QueryOrdering {
            expression: column("event_id"),
            direction: SortDirection::Asc,
        }];
        query.row_limit = Some(10);
        let rendered = render_query(&query, "warehouse").expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT `event_id`, LOWER(`label`) AS `lowered` FROM `warehouse`.`events` \
             WHERE `event_id` >= ? ORDER BY `event_id` ASC LIMIT 10"
        );
        assert_eq!(rendered.binds.len(), 1);
        assert_eq!(rendered.binds[0].name, "floor");
        assert_eq!(rendered.binds[0].ordinal, 1);

        let mut unordered_limit = base_query();
        unordered_limit.row_limit = Some(1);
        let error = render_query(&unordered_limit, "warehouse")
            .expect_err("LIMIT senza ORDER BY deve restare fail-closed");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert!(error.message.contains("LIMIT"), "{}", error.message);
        assert!(error.message.contains("ORDER BY"), "{}", error.message);
    }

    #[test]
    fn unqualified_ast_subsets_stay_unsupported() {
        let mut cases: Vec<(&str, QueryOperation)> = Vec::new();

        let mut cte = base_query();
        cte.common_table_expressions = vec![CommonTableExpression {
            name: "walk".to_owned(),
            recursive: false,
            query: Box::new(base_query()),
        }];
        cases.push(("cte", cte));

        let mut union = base_query();
        union.set_operations = vec![QuerySetOperation {
            operator: QuerySetOperator::Union,
            all: true,
            query: Box::new(base_query()),
        }];
        cases.push(("set operation", union));

        // Anche la sorgente di base derivata resta fuori: la tranche qualifica
        // solo relazioni fisiche, in FROM come in ogni join.
        let mut derived_base = base_query();
        derived_base.source = None;
        derived_base.derived_source = Some(QueryDerivedSource {
            query: Box::new(base_query()),
            alias: "recent".to_owned(),
        });
        cases.push(("derived source di base", derived_base));

        // La window scalare e qualificata da questa tranche; la window
        // spatial resta fuori. Non insieme al resto dell'AST spatial — le
        // funzioni di `VERIFIED_SPATIAL_FUNCTIONS` sono accettate, e poche
        // righe piu in basso questo stesso test lo mostra — ma per la sola
        // forma window, che nessuna prova attraversa.
        let mut spatial_window = base_query();
        spatial_window.projection = vec![QueryProjection {
            expression: QueryExpression::SpatialWindow {
                function: SpatialFunction::ClusterDbscan,
                arguments: vec![
                    column("geom"),
                    QueryExpression::Parameter {
                        name: "eps".to_owned(),
                    },
                    QueryExpression::Parameter {
                        name: "minimum".to_owned(),
                    },
                ],
                partition_by: vec![column("actor_id")],
                order_by: Vec::new(),
                frame: None,
            },
            alias: Some("cluster".to_owned()),
        }];
        cases.push(("spatial window", spatial_window));

        // v1.2: SpatialFunction verified ora accettato (vedi
        // VERIFIED_SPATIAL_FUNCTIONS). Il test di fail-closed usa una
        // funzione NON verified (AsGeoJson) per verificare che il subset
        // resti conservativo.
        let mut spatial = base_query();
        spatial.projection = vec![QueryProjection {
            expression: QueryExpression::Spatial {
                function: SpatialFunction::NRings,
                arguments: vec![column("geom")],
            },
            alias: None,
        }];
        cases.push(("spatial non verified", spatial));

        let mut subquery = base_query();
        subquery.filter = Some(QueryExpression::Exists {
            query: Box::new(base_query()),
            negated: false,
        });
        cases.push(("subquery", subquery));

        let mut locking = base_query();
        locking.locking = Some(QueryLock {
            strength: QueryLockStrength::Update,
            relations: Vec::new(),
            wait: QueryLockWait::NoWait,
        });
        cases.push(("locking", locking));

        for (label, query) in cases {
            let error = match render_query(&query, "warehouse") {
                Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
                Err(error) => error,
            };
            assert_eq!(error.category, ErrorCategory::Unsupported, "{label}");
            assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
            // «non ancora qualificat…» oppure «non qualificata su <prodotto>»:
            // il secondo e il messaggio del cancello spatial, che da quando
            // legge la lista del profilo nomina il prodotto invece di rinviare
            // a una costante — su due prodotti quella costante non e piu una.
            assert!(
                error.message.contains("non ancora qualificat")
                    || error.message.contains("non qualificata su"),
                "{label}: {}",
                error.message
            );
        }
    }

    fn aliased_source(object: &str, alias: &str) -> QuerySource {
        QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: object.to_owned(),
            },
            alias: Some(alias.to_owned()),
        }
    }

    fn qualified(relation: &str, field: &str) -> QueryExpression {
        QueryExpression::Column {
            column: ColumnRef {
                relation: Some(relation.to_owned()),
                field: field.to_owned(),
            },
        }
    }

    fn equality(left: QueryExpression, right: QueryExpression) -> QueryExpression {
        QueryExpression::Compare {
            left: Box::new(left),
            operator: ComparisonOperator::Eq,
            right: Box::new(right),
        }
    }

    fn physical_join(
        kind: JoinKind,
        source: QuerySource,
        on: Option<QueryExpression>,
    ) -> QueryJoin {
        QueryJoin {
            kind,
            source: Some(source),
            derived_source: None,
            lateral: false,
            on,
        }
    }

    /// Base `events` AS `e` con un solo INNER JOIN su `actors` AS `a`.
    fn joined_query() -> QueryOperation {
        let mut query = base_query();
        query.source = Some(aliased_source("events", "e"));
        query.projection = vec![QueryProjection {
            expression: qualified("e", "event_id"),
            alias: None,
        }];
        query.joins = vec![physical_join(
            JoinKind::Inner,
            aliased_source("actors", "a"),
            Some(equality(
                qualified("e", "actor_id"),
                qualified("a", "actor_id"),
            )),
        )];
        query
    }

    #[test]
    fn physical_joins_render_with_relation_qualified_columns_and_ordered_binds() {
        let mut query = base_query();
        query.source = Some(aliased_source("events", "e"));
        query.projection = vec![
            QueryProjection {
                expression: qualified("e", "event_id"),
                alias: Some("event_id".to_owned()),
            },
            QueryProjection {
                expression: qualified("a", "name"),
                alias: Some("actor".to_owned()),
            },
            QueryProjection {
                expression: qualified("r", "name"),
                alias: Some("region".to_owned()),
            },
        ];
        query.joins = vec![
            physical_join(
                JoinKind::Inner,
                aliased_source("actors", "a"),
                Some(QueryExpression::And {
                    arguments: vec![
                        equality(qualified("e", "actor_id"), qualified("a", "actor_id")),
                        QueryExpression::Compare {
                            left: Box::new(qualified("a", "tier")),
                            operator: ComparisonOperator::Gte,
                            right: Box::new(QueryExpression::Parameter {
                                name: "tier".to_owned(),
                            }),
                        },
                    ],
                }),
            ),
            physical_join(
                JoinKind::Left,
                aliased_source("regions", "r"),
                Some(equality(
                    qualified("a", "region_id"),
                    qualified("r", "region_id"),
                )),
            ),
            physical_join(JoinKind::Cross, aliased_source("calendar", "c"), None),
        ];
        query.filter = Some(QueryExpression::Compare {
            left: Box::new(qualified("e", "event_id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        query.order_by = vec![QueryOrdering {
            expression: qualified("e", "event_id"),
            direction: SortDirection::Asc,
        }];
        query.row_limit = Some(10);
        let rendered = render_query(&query, "warehouse").expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT `e`.`event_id` AS `event_id`, `a`.`name` AS `actor`, \
             `r`.`name` AS `region` FROM `warehouse`.`events` AS `e` \
             INNER JOIN `warehouse`.`actors` AS `a` \
             ON (`e`.`actor_id` = `a`.`actor_id` AND `a`.`tier` >= ?) \
             LEFT JOIN `warehouse`.`regions` AS `r` \
             ON `a`.`region_id` = `r`.`region_id` \
             CROSS JOIN `warehouse`.`calendar` AS `c` \
             WHERE `e`.`event_id` >= ? ORDER BY `e`.`event_id` ASC LIMIT 10"
        );
        // Il bind di ON precede quello di WHERE: l'ordine posizionale segue
        // la posizione sintattica, non l'ordine di dichiarazione.
        assert_eq!(
            rendered
                .binds
                .iter()
                .map(|bind| (bind.name.as_str(), bind.ordinal))
                .collect::<Vec<_>>(),
            vec![("tier", 1), ("floor", 2)]
        );
    }

    #[test]
    fn join_on_cannot_reference_a_relation_introduced_later() {
        let mut query = base_query();
        query.source = Some(aliased_source("events", "e"));
        query.projection = vec![QueryProjection {
            expression: qualified("e", "event_id"),
            alias: Some("event_id".to_owned()),
        }];
        query.joins = vec![
            physical_join(
                JoinKind::Inner,
                aliased_source("actors", "a"),
                Some(equality(
                    qualified("a", "region_id"),
                    qualified("r", "region_id"),
                )),
            ),
            physical_join(
                JoinKind::Left,
                aliased_source("regions", "r"),
                Some(equality(
                    qualified("a", "region_id"),
                    qualified("r", "region_id"),
                )),
            ),
        ];

        let error = render_query(&query, "warehouse")
            .expect_err("ON non puo vedere una relazione introdotta da un join successivo");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert!(error.message.contains("assente da FROM e dai join"));
    }

    /// `MySQL` non ha FULL JOIN nativo: il rifiuto descrive un'assenza del
    /// motore, non una qualificazione mancante di questa tranche.
    #[test]
    fn right_join_renders_while_full_join_is_reported_as_absent_from_mysql() {
        let mut right = joined_query();
        right.joins[0].kind = JoinKind::Right;
        right.projection = vec![QueryProjection {
            expression: qualified("a", "name"),
            alias: Some("actor".to_owned()),
        }];
        assert_eq!(
            render_query(&right, "warehouse").expect("render").sql,
            "SELECT `a`.`name` AS `actor` FROM `warehouse`.`events` AS `e` \
             RIGHT JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id`"
        );

        let mut full = joined_query();
        full.joins[0].kind = JoinKind::Full;
        let error = render_query(&full, "warehouse").expect_err("FULL JOIN");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert!(
            error.message.contains("non esiste in questo dialetto"),
            "{}",
            error.message
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn join_shapes_outside_the_qualified_subset_fail_closed() {
        let mut cases: Vec<(&str, QueryOperation, ErrorCategory)> = Vec::new();

        let mut derived = joined_query();
        derived.joins[0].source = None;
        derived.joins[0].derived_source = Some(QueryDerivedSource {
            query: Box::new(base_query()),
            alias: "recent".to_owned(),
        });
        derived.joins[0].on = Some(equality(
            qualified("e", "actor_id"),
            qualified("recent", "event_id"),
        ));
        cases.push(("join su subquery", derived, ErrorCategory::Unsupported));

        let mut cross_database = joined_query();
        cross_database.joins[0].source = Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("other".to_owned()),
                object: "actors".to_owned(),
            },
            alias: Some("a".to_owned()),
        });
        cases.push((
            "join fuori dal database configurato",
            cross_database,
            ErrorCategory::Unsupported,
        ));

        let mut three_part = joined_query();
        three_part.joins[0].source = Some(QuerySource {
            object: ObjectRef {
                catalog: Some("warehouse".to_owned()),
                schema: Some("warehouse".to_owned()),
                object: "actors".to_owned(),
            },
            alias: Some("a".to_owned()),
        });
        cases.push((
            "join a tre componenti",
            three_part,
            ErrorCategory::Unsupported,
        ));

        let mut duplicate_alias = joined_query();
        duplicate_alias.joins[0].source = Some(aliased_source("actors", "e"));
        duplicate_alias.joins[0].on = Some(equality(
            qualified("e", "actor_id"),
            qualified("e", "actor_id"),
        ));
        cases.push((
            "alias duplicato tra base e join",
            duplicate_alias,
            ErrorCategory::InvalidPlan,
        ));

        let mut duplicate_between_joins = joined_query();
        duplicate_between_joins.joins.push(physical_join(
            JoinKind::Left,
            aliased_source("regions", "a"),
            Some(equality(
                qualified("e", "region_id"),
                qualified("a", "region_id"),
            )),
        ));
        cases.push((
            "alias duplicato tra due join",
            duplicate_between_joins,
            ErrorCategory::InvalidPlan,
        ));

        // Senza alias la relazione e visibile con il nome della tabella: un
        // join sulla stessa tabella e ambiguo esattamente come un alias ripetuto.
        let mut duplicate_object = base_query();
        duplicate_object.joins = vec![physical_join(
            JoinKind::Inner,
            source("events"),
            Some(equality(
                QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("events".to_owned()),
                        field: "event_id".to_owned(),
                    },
                },
                QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("events".to_owned()),
                        field: "event_id".to_owned(),
                    },
                },
            )),
        )];
        cases.push((
            "self join senza alias",
            duplicate_object,
            ErrorCategory::InvalidPlan,
        ));

        let mut aggregate_on = joined_query();
        aggregate_on.joins[0].on = Some(QueryExpression::Compare {
            left: Box::new(aggregate(
                ScalarFunction::Count,
                vec![qualified("a", "actor_id")],
            )),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        cases.push(("aggregato in ON", aggregate_on, ErrorCategory::InvalidPlan));

        let mut window_on = joined_query();
        window_on.joins[0].on = Some(equality(
            QueryExpression::Window {
                function: ScalarFunction::Rank,
                arguments: Vec::new(),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            },
            qualified("a", "actor_id"),
        ));
        cases.push(("window in ON", window_on, ErrorCategory::InvalidPlan));

        // v1.2: Centroid è ora verified. Il test JOIN spatial usa una
        // funzione non-verified per verificare che JOIN-on-spatial resti
        // conservativo — ma qualsiasi spatial in ON è già rifiutato dalle
        // regole di join (spatial expression non ammessa come predicato di JOIN).
        let mut spatial_on = joined_query();
        spatial_on.joins[0].on = Some(equality(
            QueryExpression::Spatial {
                function: SpatialFunction::NRings,
                arguments: vec![qualified("a", "geom")],
            },
            qualified("e", "geom"),
        ));
        cases.push(("spatial in ON", spatial_on, ErrorCategory::Unsupported));

        let mut subquery_on = joined_query();
        subquery_on.joins[0].on = Some(QueryExpression::Exists {
            query: Box::new(base_query()),
            negated: false,
        });
        cases.push(("subquery in ON", subquery_on, ErrorCategory::Unsupported));

        let mut oversized_alias = joined_query();
        oversized_alias.joins[0].source = Some(aliased_source(
            "actors",
            &"a".repeat(MAX_IDENTIFIER_CHARACTERS + 1),
        ));
        cases.push((
            "alias join oltre 64 caratteri",
            oversized_alias,
            ErrorCategory::InvalidPlan,
        ));

        let mut unknown_relation = joined_query();
        unknown_relation.projection = vec![QueryProjection {
            expression: qualified("missing", "event_id"),
            alias: None,
        }];
        cases.push((
            "colonna su relazione inesistente",
            unknown_relation,
            ErrorCategory::InvalidPlan,
        ));

        let mut unknown_wildcard = joined_query();
        unknown_wildcard.projection = vec![QueryProjection {
            expression: QueryExpression::Wildcard {
                relation: Some("missing".to_owned()),
            },
            alias: None,
        }];
        cases.push((
            "wildcard su relazione inesistente",
            unknown_wildcard,
            ErrorCategory::InvalidPlan,
        ));

        // Alcuni casi non sono limiti di `MySQL`: sono SQL invalido per
        // qualunque motore, e da quando `validate_query_operation` conosce la
        // clausola di provenienza di una window li rifiuta prima del
        // renderer. Rispondono percio in fase `Validate` e senza provider —
        // il confine giusto, e va misurato dove cade davvero.
        for (label, query, category) in cases {
            let error = match render_query(&query, "warehouse") {
                Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
                Err(error) => error,
            };
            assert_eq!(error.category, category, "{label}: {}", error.message);
            if PORTABLE_REJECTIONS.contains(&label) {
                assert_eq!(error.phase, ErrorPhase::Validate, "{label}");
                assert_eq!(error.provider, None, "{label}");
            } else {
                assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
                assert_eq!(error.provider, Some(ProviderKind::Mysql), "{label}");
            }
        }
    }

    /// La presenza della clausola ON e gia rifiutata dalla validazione
    /// portabile, che risponde in fase `Validate`. Il provider ripete il
    /// controllo per conto proprio: la copertura di quel confine appartiene
    /// al path `MySQL` e non deve dipendere dall'ordine dei validatori.
    #[test]
    fn the_on_clause_boundary_is_covered_by_the_core_and_by_the_provider() {
        let mut cross_with_on = joined_query();
        cross_with_on.joins[0].kind = JoinKind::Cross;
        let mut inner_without_on = joined_query();
        inner_without_on.joins[0].on = None;

        for (label, query) in [
            ("CROSS JOIN con ON", cross_with_on),
            ("INNER JOIN senza ON", inner_without_on),
        ] {
            let core = render_query(&query, "warehouse").expect_err(label);
            assert_eq!(core.category, ErrorCategory::InvalidPlan, "{label}");
            assert_eq!(core.phase, ErrorPhase::Validate, "{label}");

            let provider =
                ensure_qualified_shape(&query, "warehouse", &crate::profile::MYSQL_PROFILE)
                    .expect_err(label);
            assert_eq!(provider.category, ErrorCategory::InvalidPlan, "{label}");
            assert_eq!(provider.phase, ErrorPhase::Prepare, "{label}");
            assert_eq!(provider.provider, Some(ProviderKind::Mysql), "{label}");
            assert!(provider.message.contains("JOIN"), "{}", provider.message);
        }
    }

    #[test]
    fn distinct_stays_deterministic_across_joins() {
        let mut distinct = joined_query();
        distinct.projection = vec![
            QueryProjection {
                expression: qualified("e", "event_id"),
                alias: Some("event".to_owned()),
            },
            QueryProjection {
                expression: QueryExpression::Scalar {
                    function: ScalarFunction::Lower,
                    arguments: vec![qualified("a", "name")],
                },
                alias: Some("actor".to_owned()),
            },
        ];
        distinct.distinct = true;
        distinct.order_by = vec![
            QueryOrdering {
                expression: qualified("e", "event_id"),
                direction: SortDirection::Asc,
            },
            QueryOrdering {
                expression: column("actor"),
                direction: SortDirection::Desc,
            },
        ];
        assert_eq!(
            render_query(&distinct, "warehouse").expect("render").sql,
            "SELECT DISTINCT `e`.`event_id` AS `event`, LOWER(`a`.`name`) AS `actor` \
             FROM `warehouse`.`events` AS `e` \
             INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
             ORDER BY `e`.`event_id` ASC, `actor` DESC"
        );

        // Una colonna qualificata su un'altra relazione non appartiene alla
        // proiezione DISTINCT: l'ordine non sarebbe riproducibile.
        distinct.order_by = vec![QueryOrdering {
            expression: qualified("a", "tier"),
            direction: SortDirection::Asc,
        }];
        assert_eq!(
            render_query(&distinct, "warehouse")
                .expect_err("ordine fuori dalla proiezione DISTINCT")
                .category,
            ErrorCategory::InvalidPlan
        );

        // Un wildcard qualificato copre solo la sua relazione: con DISTINCT
        // l'ordine su un'altra relazione resta non riproducibile.
        let mut qualified_wildcard = joined_query();
        qualified_wildcard.projection = vec![QueryProjection {
            expression: QueryExpression::Wildcard {
                relation: Some("e".to_owned()),
            },
            alias: None,
        }];
        qualified_wildcard.distinct = true;
        qualified_wildcard.order_by = vec![QueryOrdering {
            expression: qualified("a", "name"),
            direction: SortDirection::Asc,
        }];
        assert_eq!(
            render_query(&qualified_wildcard, "warehouse")
                .expect_err("wildcard qualificato non copre l'altra relazione")
                .category,
            ErrorCategory::InvalidPlan
        );
        qualified_wildcard.order_by = vec![QueryOrdering {
            expression: qualified("e", "event_id"),
            direction: SortDirection::Asc,
        }];
        assert_eq!(
            render_query(&qualified_wildcard, "warehouse")
                .expect("render")
                .sql,
            "SELECT DISTINCT `e`.* FROM `warehouse`.`events` AS `e` \
             INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
             ORDER BY `e`.`event_id` ASC"
        );
    }

    #[test]
    fn group_determinism_reads_the_relation_qualifier_as_part_of_the_key() {
        let mut grouped = joined_query();
        grouped.projection = vec![
            QueryProjection {
                expression: qualified("a", "name"),
                alias: Some("actor".to_owned()),
            },
            QueryProjection {
                expression: count_qualified("e", "event_id"),
                alias: Some("events".to_owned()),
            },
        ];
        grouped.group_by = vec![qualified("a", "name")];
        grouped.having = Some(QueryExpression::Compare {
            left: Box::new(count_qualified("e", "event_id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        grouped.order_by = vec![QueryOrdering {
            expression: qualified("a", "name"),
            direction: SortDirection::Asc,
        }];
        let rendered = render_query(&grouped, "warehouse").expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT `a`.`name` AS `actor`, COUNT(`e`.`event_id`) AS `events` \
             FROM `warehouse`.`events` AS `e` \
             INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
             GROUP BY `a`.`name` HAVING COUNT(`e`.`event_id`) >= ? ORDER BY `a`.`name` ASC"
        );
        assert_eq!(rendered.binds.len(), 1);
        assert_eq!(rendered.binds[0].ordinal, 1);

        // Con GROUP BY su `a`.`name` la stessa colonna non qualificata non e
        // la stessa chiave: il gruppo resta non dimostrato.
        grouped.group_by = vec![column("name")];
        assert_eq!(
            render_query(&grouped, "warehouse")
                .expect_err("chiave di gruppo non qualificata")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    fn aggregate(function: ScalarFunction, arguments: Vec<QueryExpression>) -> QueryExpression {
        QueryExpression::Scalar {
            function,
            arguments,
        }
    }

    fn count_qualified(relation: &str, field: &str) -> QueryExpression {
        aggregate(ScalarFunction::Count, vec![qualified(relation, field)])
    }

    fn count(field: &str) -> QueryExpression {
        aggregate(ScalarFunction::Count, vec![column(field)])
    }

    #[test]
    fn distinct_projection_renders_and_orders_inside_the_projection() {
        let mut query = base_query();
        query.projection = vec![
            QueryProjection {
                expression: column("actor_id"),
                alias: None,
            },
            QueryProjection {
                expression: QueryExpression::Scalar {
                    function: ScalarFunction::Lower,
                    arguments: vec![column("label")],
                },
                alias: Some("lowered".to_owned()),
            },
        ];
        query.distinct = true;
        query.order_by = vec![
            QueryOrdering {
                expression: column("actor_id"),
                direction: SortDirection::Asc,
            },
            QueryOrdering {
                expression: column("lowered"),
                direction: SortDirection::Desc,
            },
        ];
        let rendered = render_query(&query, "warehouse").expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT DISTINCT `actor_id`, LOWER(`label`) AS `lowered` \
             FROM `warehouse`.`events` ORDER BY `actor_id` ASC, `lowered` DESC"
        );
        assert!(rendered.binds.is_empty());
    }

    #[test]
    fn grouped_aggregates_render_with_binds_ordered_by_clause() {
        let mut query = base_query();
        query.projection = vec![
            QueryProjection {
                expression: column("actor_id"),
                alias: None,
            },
            QueryProjection {
                expression: count("event_id"),
                alias: Some("events".to_owned()),
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Sum, vec![column("amount")]),
                alias: Some("total".to_owned()),
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Average, vec![column("amount")]),
                alias: Some("mean".to_owned()),
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Minimum, vec![column("event_id")]),
                alias: Some("first_event".to_owned()),
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Maximum, vec![column("event_id")]),
                alias: Some("last_event".to_owned()),
            },
        ];
        query.filter = Some(QueryExpression::Compare {
            left: Box::new(column("event_id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "since".to_owned(),
            }),
        });
        query.group_by = vec![column("actor_id")];
        query.having = Some(QueryExpression::Compare {
            left: Box::new(count("event_id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        query.order_by = vec![QueryOrdering {
            expression: column("actor_id"),
            direction: SortDirection::Asc,
        }];
        query.row_limit = Some(25);
        let rendered = render_query(&query, "warehouse").expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT `actor_id`, COUNT(`event_id`) AS `events`, SUM(`amount`) AS `total`, \
             AVG(`amount`) AS `mean`, MIN(`event_id`) AS `first_event`, \
             MAX(`event_id`) AS `last_event` FROM `warehouse`.`events` \
             WHERE `event_id` >= ? GROUP BY `actor_id` HAVING COUNT(`event_id`) >= ? \
             ORDER BY `actor_id` ASC LIMIT 25"
        );
        assert_eq!(rendered.binds.len(), 2);
        assert_eq!(rendered.binds[0].name, "since");
        assert_eq!(rendered.binds[0].ordinal, 1);
        assert_eq!(rendered.binds[1].name, "floor");
        assert_eq!(rendered.binds[1].ordinal, 2);
    }

    #[test]
    fn count_star_is_the_only_wildcard_accepted_inside_an_aggregate() {
        let mut query = base_query();
        query.projection = vec![QueryProjection {
            expression: aggregate(
                ScalarFunction::Count,
                vec![QueryExpression::Wildcard { relation: None }],
            ),
            alias: Some("rows".to_owned()),
        }];
        let rendered = render_query(&query, "warehouse").expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT COUNT(*) AS `rows` FROM `warehouse`.`events`"
        );

        let mut aliased_star = base_query();
        aliased_star.projection = vec![QueryProjection {
            expression: aggregate(
                ScalarFunction::Count,
                vec![QueryExpression::Wildcard {
                    relation: Some("events".to_owned()),
                }],
            ),
            alias: None,
        }];
        assert_eq!(
            render_query(&aliased_star, "warehouse")
                .expect_err("COUNT(t.*) non e valido in MySQL")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn grouped_shapes_without_a_deterministic_group_fail_closed() {
        let mut cases: Vec<(&str, QueryOperation)> = Vec::new();

        let mut ungrouped_column = base_query();
        ungrouped_column.projection = vec![
            QueryProjection {
                expression: column("label"),
                alias: None,
            },
            QueryProjection {
                expression: count("event_id"),
                alias: None,
            },
        ];
        ungrouped_column.group_by = vec![column("actor_id")];
        cases.push(("colonna fuori da GROUP BY", ungrouped_column));

        let mut grouped_wildcard = base_query();
        grouped_wildcard.projection = vec![
            QueryProjection {
                expression: QueryExpression::Wildcard { relation: None },
                alias: None,
            },
            QueryProjection {
                expression: count("event_id"),
                alias: None,
            },
        ];
        grouped_wildcard.group_by = vec![column("actor_id")];
        cases.push(("wildcard in query aggregata", grouped_wildcard));

        let mut aggregate_in_where = base_query();
        aggregate_in_where.filter = Some(QueryExpression::Compare {
            left: Box::new(count("event_id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        cases.push(("aggregato in WHERE", aggregate_in_where));

        let mut aggregate_in_group = base_query();
        aggregate_in_group.projection = vec![QueryProjection {
            expression: count("event_id"),
            alias: None,
        }];
        aggregate_in_group.group_by = vec![count("event_id")];
        cases.push(("aggregato in GROUP BY", aggregate_in_group));

        let mut nested_aggregate = base_query();
        nested_aggregate.projection = vec![QueryProjection {
            expression: aggregate(ScalarFunction::Sum, vec![count("event_id")]),
            alias: None,
        }];
        cases.push(("aggregato annidato", nested_aggregate));

        let mut having_without_aggregation = base_query();
        having_without_aggregation.having = Some(QueryExpression::Compare {
            left: Box::new(column("event_id")),
            operator: ComparisonOperator::Gt,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        cases.push(("HAVING senza aggregazione", having_without_aggregation));

        let mut wildcard_in_sum = base_query();
        wildcard_in_sum.projection = vec![QueryProjection {
            expression: aggregate(
                ScalarFunction::Sum,
                vec![QueryExpression::Wildcard { relation: None }],
            ),
            alias: None,
        }];
        cases.push(("wildcard dentro SUM", wildcard_in_sum));

        let mut parameter_group = base_query();
        parameter_group.projection = vec![QueryProjection {
            expression: count("event_id"),
            alias: None,
        }];
        parameter_group.group_by = vec![QueryExpression::Parameter {
            name: "key".to_owned(),
        }];
        cases.push(("parametro in GROUP BY", parameter_group));

        let mut ordering_outside_group = base_query();
        ordering_outside_group.projection = vec![
            QueryProjection {
                expression: column("actor_id"),
                alias: None,
            },
            QueryProjection {
                expression: count("event_id"),
                alias: None,
            },
        ];
        ordering_outside_group.group_by = vec![column("actor_id")];
        ordering_outside_group.order_by = vec![QueryOrdering {
            expression: column("label"),
            direction: SortDirection::Asc,
        }];
        cases.push(("ORDER BY fuori dal gruppo", ordering_outside_group));

        let mut having_outside_group = base_query();
        having_outside_group.projection = vec![QueryProjection {
            expression: column("actor_id"),
            alias: None,
        }];
        having_outside_group.group_by = vec![column("actor_id")];
        having_outside_group.having = Some(QueryExpression::Compare {
            left: Box::new(column("label")),
            operator: ComparisonOperator::Gt,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        cases.push(("HAVING fuori dal gruppo", having_outside_group));

        for (label, query) in cases {
            let error = match render_query(&query, "warehouse") {
                Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
                Err(error) => error,
            };
            assert_eq!(error.category, ErrorCategory::InvalidPlan, "{label}");
            assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
            assert_eq!(error.provider, Some(ProviderKind::Mysql), "{label}");
        }
    }

    #[test]
    fn distinct_ordering_must_belong_to_the_projection() {
        let mut outside = base_query();
        outside.distinct = true;
        outside.order_by = vec![QueryOrdering {
            expression: column("label"),
            direction: SortDirection::Asc,
        }];
        let error = render_query(&outside, "warehouse").expect_err("ordine non riproducibile");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);

        let mut wildcard = base_query();
        wildcard.projection = vec![QueryProjection {
            expression: QueryExpression::Wildcard { relation: None },
            alias: None,
        }];
        wildcard.distinct = true;
        wildcard.order_by = vec![QueryOrdering {
            expression: column("label"),
            direction: SortDirection::Asc,
        }];
        assert_eq!(
            render_query(&wildcard, "warehouse").expect("render").sql,
            "SELECT DISTINCT * FROM `warehouse`.`events` ORDER BY `label` ASC"
        );
    }

    /// `MySQL` non ha DISTINCT ON: il rifiuto descrive un'assenza del motore,
    /// non una qualificazione mancante di questa tranche.
    #[test]
    fn distinct_on_is_reported_as_absent_from_mysql() {
        let mut query = base_query();
        query.distinct_on = vec![column("event_id")];
        query.order_by = vec![QueryOrdering {
            expression: column("event_id"),
            direction: SortDirection::Asc,
        }];
        let error = render_query(&query, "warehouse").expect_err("DISTINCT ON");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert!(
            error.message.contains("non esiste in questo dialetto"),
            "{}",
            error.message
        );
    }

    #[test]
    fn window_only_functions_and_argument_counts_fail_before_the_network() {
        let mut bare_window = base_query();
        bare_window.projection = vec![QueryProjection {
            expression: aggregate(ScalarFunction::Rank, Vec::new()),
            alias: None,
        }];
        let error = render_query(&bare_window, "warehouse").expect_err("RANK senza OVER");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);

        let mut wrong_arity = base_query();
        wrong_arity.projection = vec![QueryProjection {
            expression: aggregate(
                ScalarFunction::Count,
                vec![column("event_id"), column("actor_id")],
            ),
            alias: None,
        }];
        assert_eq!(
            render_query(&wrong_arity, "warehouse")
                .expect_err("COUNT con due argomenti")
                .category,
            ErrorCategory::InvalidPlan
        );

        let mut empty_coalesce = base_query();
        empty_coalesce.projection = vec![QueryProjection {
            expression: aggregate(ScalarFunction::Coalesce, Vec::new()),
            alias: None,
        }];
        assert_eq!(
            render_query(&empty_coalesce, "warehouse")
                .expect_err("COALESCE senza argomenti")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    /// `MySQL` 8.0.14 supporta LATERAL: il rifiuto e una mancata
    /// qualificazione di questa tranche, non un'assenza del motore.
    #[test]
    fn lateral_is_reported_as_not_yet_qualified_instead_of_absent() {
        let mut lateral = base_query();
        lateral.joins = vec![QueryJoin {
            kind: JoinKind::Inner,
            source: None,
            derived_source: Some(QueryDerivedSource {
                query: Box::new(base_query()),
                alias: "recent".to_owned(),
            }),
            lateral: true,
            on: Some(QueryExpression::Compare {
                left: Box::new(column("event_id")),
                operator: ComparisonOperator::Eq,
                right: Box::new(column("event_id")),
            }),
        }];
        let error = render_query(&lateral, "warehouse").expect_err("lateral join");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert!(!error.message.contains("non esiste"));
        assert!(error.message.contains("non ancora qualificati"));
    }

    #[test]
    fn cross_database_and_oversized_identifiers_are_rejected_before_rendering() {
        let mut cross = base_query();
        cross.source = Some(QuerySource {
            object: ObjectRef {
                catalog: Some("other".to_owned()),
                schema: None,
                object: "events".to_owned(),
            },
            alias: None,
        });
        assert_eq!(
            render_query(&cross, "warehouse")
                .expect_err("cross database")
                .category,
            ErrorCategory::Unsupported
        );

        let mut cross_schema = base_query();
        cross_schema.source = Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("other".to_owned()),
                object: "events".to_owned(),
            },
            alias: None,
        });
        assert_eq!(
            render_query(&cross_schema, "warehouse")
                .expect_err("schema MySQL diverso dal database configurato")
                .category,
            ErrorCategory::Unsupported
        );

        let mut three_part = base_query();
        three_part.source = Some(QuerySource {
            object: ObjectRef {
                catalog: Some("warehouse".to_owned()),
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
            },
            alias: None,
        });
        assert_eq!(
            render_query(&three_part, "warehouse")
                .expect_err("nome a tre componenti")
                .category,
            ErrorCategory::Unsupported
        );

        let mut long_identifier = base_query();
        long_identifier.projection = vec![QueryProjection {
            expression: column(&"a".repeat(MAX_IDENTIFIER_CHARACTERS + 1)),
            alias: None,
        }];
        assert_eq!(
            render_query(&long_identifier, "warehouse")
                .expect_err("identificatore oltre 64 caratteri")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    fn ascending(expression: QueryExpression) -> QueryOrdering {
        QueryOrdering {
            expression,
            direction: SortDirection::Asc,
        }
    }

    fn scalar_window(
        function: ScalarFunction,
        arguments: Vec<QueryExpression>,
        partition_by: Vec<QueryExpression>,
        order_by: Vec<QueryOrdering>,
        frame: Option<WindowFrame>,
    ) -> QueryExpression {
        QueryExpression::Window {
            function,
            arguments,
            partition_by,
            order_by,
            frame,
        }
    }

    const fn frame(
        units: WindowFrameUnits,
        start: WindowFrameBound,
        end: WindowFrameBound,
    ) -> WindowFrame {
        WindowFrame {
            units,
            start,
            end: Some(end),
        }
    }

    /// `joined_query()` con una sola proiezione window, sempre con alias
    /// `value`: i test sulle forme rifiutate non devono ripetere il piano.
    fn windowed_query(expression: QueryExpression) -> QueryOperation {
        let mut query = joined_query();
        query.projection = vec![QueryProjection {
            expression,
            alias: Some("value".to_owned()),
        }];
        query
    }

    /// Prefisso FROM di `windowed_query`, comune a ogni SQL atteso.
    const JOINED_FROM: &str = "FROM `warehouse`.`events` AS `e` \
         INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id`";

    /// Una window in SELECT vede l'insieme finale delle relazioni, quindi
    /// anche quelle introdotte da un join successivo, mentre i bind della
    /// proiezione precedono quelli di WHERE.
    #[test]
    fn scalar_windows_render_over_the_final_relation_set_with_projection_binds_first() {
        let mut query = joined_query();
        query.projection = vec![
            QueryProjection {
                expression: qualified("e", "event_id"),
                alias: Some("event_id".to_owned()),
            },
            QueryProjection {
                expression: scalar_window(
                    ScalarFunction::Rank,
                    Vec::new(),
                    vec![qualified("a", "region_id")],
                    vec![ascending(qualified("e", "event_id"))],
                    None,
                ),
                alias: Some("ranked".to_owned()),
            },
            QueryProjection {
                expression: scalar_window(
                    ScalarFunction::DenseRank,
                    Vec::new(),
                    Vec::new(),
                    vec![ascending(qualified("e", "event_id"))],
                    None,
                ),
                alias: Some("dense".to_owned()),
            },
            QueryProjection {
                expression: scalar_window(
                    ScalarFunction::Sum,
                    vec![qualified("e", "amount")],
                    vec![qualified("a", "region_id")],
                    vec![ascending(qualified("e", "event_id"))],
                    Some(frame(
                        WindowFrameUnits::Range,
                        WindowFrameBound::UnboundedPreceding,
                        WindowFrameBound::CurrentRow,
                    )),
                ),
                alias: Some("running".to_owned()),
            },
            QueryProjection {
                expression: scalar_window(
                    ScalarFunction::Count,
                    vec![QueryExpression::Wildcard { relation: None }],
                    vec![qualified("a", "region_id")],
                    Vec::new(),
                    None,
                ),
                alias: Some("peers".to_owned()),
            },
            // Il bind della proiezione non appartiene piu a una window: la
            // prova sull'ordine posizionale resta, senza dipendere da una
            // funzione che questa tranche non pubblica.
            QueryProjection {
                expression: QueryExpression::Scalar {
                    function: ScalarFunction::Coalesce,
                    arguments: vec![
                        qualified("e", "amount"),
                        QueryExpression::Parameter {
                            name: "fallback".to_owned(),
                        },
                    ],
                },
                alias: Some("amount".to_owned()),
            },
        ];
        query.filter = Some(QueryExpression::Compare {
            left: Box::new(qualified("e", "event_id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        query.order_by = vec![ascending(qualified("e", "event_id"))];
        query.row_limit = Some(10);

        let rendered = render_query(&query, "warehouse").expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT `e`.`event_id` AS `event_id`, \
             RANK() OVER (PARTITION BY `a`.`region_id` ORDER BY `e`.`event_id` ASC) \
             AS `ranked`, \
             DENSE_RANK() OVER (ORDER BY `e`.`event_id` ASC) AS `dense`, \
             SUM(`e`.`amount`) OVER (PARTITION BY `a`.`region_id` \
             ORDER BY `e`.`event_id` ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             AS `running`, \
             COUNT(*) OVER (PARTITION BY `a`.`region_id`) AS `peers`, \
             COALESCE(`e`.`amount`, ?) AS `amount` \
             FROM `warehouse`.`events` AS `e` \
             INNER JOIN `warehouse`.`actors` AS `a` ON `e`.`actor_id` = `a`.`actor_id` \
             WHERE `e`.`event_id` >= ? ORDER BY `e`.`event_id` ASC LIMIT 10"
        );
        assert_eq!(
            rendered
                .binds
                .iter()
                .map(|bind| (bind.name.as_str(), bind.ordinal))
                .collect::<Vec<_>>(),
            vec![("fallback", 1), ("floor", 2)]
        );
    }

    /// Un aggregato di finestra senza ORDER BY copre l'intera partizione ed e
    /// gia deterministico: il frame invece taglia la partizione per posizione
    /// e senza un ordine dichiarato la porzione non sarebbe determinata.
    #[test]
    fn aggregate_windows_render_without_an_order_but_a_frame_requires_one() {
        let whole_partition = windowed_query(scalar_window(
            ScalarFunction::Average,
            vec![qualified("e", "amount")],
            vec![qualified("a", "region_id")],
            Vec::new(),
            None,
        ));
        assert_eq!(
            render_query(&whole_partition, "warehouse")
                .expect("render")
                .sql,
            format!(
                "SELECT AVG(`e`.`amount`) OVER (PARTITION BY `a`.`region_id`) AS `value` \
                 {JOINED_FROM}"
            )
        );

        let unordered_frame = windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![qualified("e", "amount")],
            Vec::new(),
            Vec::new(),
            Some(frame(
                WindowFrameUnits::Range,
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
        ));
        let error = render_query(&unordered_frame, "warehouse").expect_err("frame senza ORDER BY");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert!(
            error.message.contains("senza ORDER BY"),
            "{}",
            error.message
        );
    }

    /// `MySQL` 8.4 non ha GROUPS nel parser, mentre un offset RANGE confronta
    /// valori e pretende un INTERVAL quando la chiave d'ordine e temporale:
    /// l'AST portabile esprime solo un offset numerico nudo. Un frame ROWS
    /// conta invece le posizioni, che fra righe pari sono arbitrarie finche
    /// l'unicita dell'ordine non e dimostrata.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn window_frames_are_limited_to_the_units_mysql_can_represent() {
        let range = windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![qualified("e", "amount")],
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            Some(frame(
                WindowFrameUnits::Range,
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
        ));
        assert_eq!(
            render_query(&range, "warehouse").expect("render").sql,
            format!(
                "SELECT SUM(`e`.`amount`) OVER (ORDER BY `e`.`event_id` ASC \
                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `value` {JOINED_FROM}"
            )
        );

        let range_offset = windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![qualified("e", "amount")],
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            Some(frame(
                WindowFrameUnits::Range,
                WindowFrameBound::Preceding(2),
                WindowFrameBound::CurrentRow,
            )),
        ));
        let offset = render_query(&range_offset, "warehouse").expect_err("RANGE con offset");
        assert_eq!(offset.category, ErrorCategory::Unsupported);
        assert_eq!(offset.phase, ErrorPhase::Prepare);
        assert!(
            offset.message.contains("non ancora qualificat"),
            "{}",
            offset.message
        );

        let rows = windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![qualified("e", "amount")],
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            Some(frame(
                WindowFrameUnits::Rows,
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
        ));
        let positional = render_query(&rows, "warehouse").expect_err("frame ROWS");
        assert_eq!(positional.category, ErrorCategory::Unsupported);
        assert_eq!(positional.phase, ErrorPhase::Prepare);
        assert!(
            positional.message.contains("ordine totale"),
            "{}",
            positional.message
        );
        // ROWS esiste nel parser di MySQL 8.4: il rifiuto e una qualificazione
        // mancante di questa tranche, non un'assenza del motore.
        assert!(
            !positional.message.contains("non esiste in questo dialetto"),
            "{}",
            positional.message
        );

        let groups = windowed_query(scalar_window(
            ScalarFunction::Sum,
            vec![qualified("e", "amount")],
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            Some(frame(
                WindowFrameUnits::Groups,
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::CurrentRow,
            )),
        ));
        let absent = render_query(&groups, "warehouse").expect_err("GROUPS");
        assert_eq!(absent.category, ErrorCategory::Unsupported);
        assert_eq!(absent.phase, ErrorPhase::Prepare);
        assert!(
            absent.message.contains("non esiste in questo dialetto"),
            "{}",
            absent.message
        );

        for (label, invalid_frame) in [
            (
                "start UNBOUNDED FOLLOWING abbreviato",
                WindowFrame {
                    units: WindowFrameUnits::Range,
                    start: WindowFrameBound::UnboundedFollowing,
                    end: None,
                },
            ),
            (
                "end UNBOUNDED PRECEDING",
                WindowFrame {
                    units: WindowFrameUnits::Range,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: Some(WindowFrameBound::UnboundedPreceding),
                },
            ),
        ] {
            let invalid = windowed_query(scalar_window(
                ScalarFunction::Sum,
                vec![qualified("e", "amount")],
                Vec::new(),
                vec![ascending(qualified("e", "event_id"))],
                Some(invalid_frame),
            ));
            let error = render_query(&invalid, "warehouse").expect_err(label);
            assert_eq!(error.category, ErrorCategory::InvalidPlan, "{label}");
            assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
            assert!(
                error.message.contains("limite"),
                "{label}: {}",
                error.message
            );
        }
    }

    /// `RANK` e `DENSE_RANK` sono stabili fra pari: due righe con la stessa
    /// chiave d'ordine ricevono lo stesso valore, quindi un ORDER BY non vuoto
    /// basta a renderle riproducibili anche quando la chiave ha duplicati.
    /// `ROW_NUMBER`, `LAG` e `LEAD` leggono invece una posizione dentro i pari
    /// e sono riproducibili solo se la chiave e un ordine totale univoco: una
    /// proprieta che l'AST portabile non esprime e che il provider non puo
    /// dedurre prima della rete. Il frame esplicito e infine accettato dal
    /// parser di `MySQL` 8.4 e poi ignorato, quindi renderizzarlo
    /// pubblicherebbe un piano che il motore non esegue.
    #[test]
    fn peer_stable_ranking_renders_while_total_order_windows_stay_closed() {
        for (function, call) in [
            (ScalarFunction::Rank, "RANK()"),
            (ScalarFunction::DenseRank, "DENSE_RANK()"),
        ] {
            let ordered = windowed_query(scalar_window(
                function,
                Vec::new(),
                Vec::new(),
                vec![ascending(qualified("e", "event_id"))],
                None,
            ));
            assert_eq!(
                render_query(&ordered, "warehouse")
                    .unwrap_or_else(|error| panic!("{call}: {}", error.message))
                    .sql,
                format!(
                    "SELECT {call} OVER (ORDER BY `e`.`event_id` ASC) AS `value` {JOINED_FROM}"
                )
            );

            let unordered = windowed_query(scalar_window(
                function,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
            ));
            let missing = render_query(&unordered, "warehouse").expect_err(call);
            assert_eq!(missing.category, ErrorCategory::InvalidPlan, "{call}");
            assert_eq!(missing.phase, ErrorPhase::Prepare, "{call}");
            assert!(missing.message.contains("senza ORDER BY"), "{call}");

            let framed = windowed_query(scalar_window(
                function,
                Vec::new(),
                Vec::new(),
                vec![ascending(qualified("e", "event_id"))],
                Some(frame(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::UnboundedPreceding,
                    WindowFrameBound::CurrentRow,
                )),
            ));
            let ignored = render_query(&framed, "warehouse").expect_err(call);
            assert_eq!(ignored.category, ErrorCategory::InvalidPlan, "{call}");
            assert_eq!(ignored.phase, ErrorPhase::Prepare, "{call}");
            assert!(
                ignored.message.contains("frame"),
                "{call}: {}",
                ignored.message
            );
        }

        for (function, arguments, call) in [
            (ScalarFunction::RowNumber, Vec::new(), "ROW_NUMBER"),
            (ScalarFunction::Lag, vec![qualified("e", "amount")], "LAG"),
            (ScalarFunction::Lead, vec![qualified("e", "amount")], "LEAD"),
        ] {
            let ordered = windowed_query(scalar_window(
                function,
                arguments,
                Vec::new(),
                vec![ascending(qualified("e", "event_id"))],
                None,
            ));
            let error = match render_query(&ordered, "warehouse") {
                Ok(rendered) => panic!("{call} deve restare fail-closed, reso: {}", rendered.sql),
                Err(error) => error,
            };
            assert_eq!(error.category, ErrorCategory::Unsupported, "{call}");
            assert_eq!(error.phase, ErrorPhase::Prepare, "{call}");
            assert_eq!(error.provider, Some(ProviderKind::Mysql), "{call}");
            assert!(
                error.message.contains("ordine totale"),
                "{call}: {}",
                error.message
            );
            assert!(
                error.message.contains("non ancora qualificat"),
                "{call}: {}",
                error.message
            );
            // MySQL 8.4 ha ROW_NUMBER, LAG e LEAD: il rifiuto e una
            // qualificazione mancante di questa tranche, non un'assenza.
            assert!(
                !error.message.contains("non esiste in questo dialetto"),
                "{call}: {}",
                error.message
            );
        }
    }

    /// I casi che la validazione portabile rifiuta prima del renderer.
    ///
    /// Non sono limiti di `MySQL`: nessun motore ammette una window in una
    /// clausola `ON` o dentro gli argomenti di un'altra window.
    const PORTABLE_REJECTIONS: &[&str] = &["window in ON", "window annidata in una window"];

    #[test]
    #[allow(clippy::too_many_lines)]
    fn window_operands_stay_row_only_scalar_and_relation_valid() {
        let ordering = || vec![ascending(qualified("e", "event_id"))];
        let mut cases: Vec<(&str, QueryOperation, ErrorCategory)> = Vec::new();

        cases.push((
            "window annidata in una window",
            windowed_query(scalar_window(
                ScalarFunction::Sum,
                vec![scalar_window(
                    ScalarFunction::Rank,
                    Vec::new(),
                    Vec::new(),
                    ordering(),
                    None,
                )],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        cases.push((
            "aggregato dentro una window",
            windowed_query(scalar_window(
                ScalarFunction::Sum,
                vec![count_qualified("e", "event_id")],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        cases.push((
            "aggregato in PARTITION BY",
            windowed_query(scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                vec![count_qualified("e", "event_id")],
                ordering(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        cases.push((
            "aggregato nell'ORDER BY della window",
            windowed_query(scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                Vec::new(),
                vec![ascending(count_qualified("e", "event_id"))],
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        // v1.2: SpatialFunction::Area è ora verified — deve essere accettata
        // anche dentro una window. Uso funzione non-verified per il fail-closed test.
        cases.push((
            "spatial non-verified dentro una window",
            windowed_query(scalar_window(
                ScalarFunction::Sum,
                vec![QueryExpression::Spatial {
                    function: SpatialFunction::NRings,
                    arguments: vec![qualified("e", "geom")],
                }],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ErrorCategory::Unsupported,
        ));

        cases.push((
            "subquery dentro una window",
            windowed_query(scalar_window(
                ScalarFunction::Sum,
                vec![QueryExpression::ScalarSubquery {
                    query: Box::new(base_query()),
                }],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ErrorCategory::Unsupported,
        ));

        cases.push((
            "relazione assente nell'argomento",
            windowed_query(scalar_window(
                ScalarFunction::Sum,
                vec![qualified("missing", "amount")],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        cases.push((
            "relazione assente in PARTITION BY",
            windowed_query(scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                vec![qualified("missing", "region_id")],
                ordering(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        cases.push((
            "relazione assente nell'ORDER BY della window",
            windowed_query(scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                Vec::new(),
                vec![ascending(qualified("missing", "event_id"))],
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        // `COUNT(*)` sopravvive come unica forma di wildcard: `COUNT(t.*)` e
        // ogni altro aggregato con `*` restano fuori anche sotto OVER.
        cases.push((
            "COUNT qualificato con wildcard",
            windowed_query(scalar_window(
                ScalarFunction::Count,
                vec![QueryExpression::Wildcard {
                    relation: Some("e".to_owned()),
                }],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        cases.push((
            "wildcard dentro SUM OVER",
            windowed_query(scalar_window(
                ScalarFunction::Sum,
                vec![QueryExpression::Wildcard { relation: None }],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        cases.push((
            "identificatore oltre 64 caratteri in PARTITION BY",
            windowed_query(scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                vec![column(&"a".repeat(MAX_IDENTIFIER_CHARACTERS + 1))],
                ordering(),
                None,
            )),
            ErrorCategory::InvalidPlan,
        ));

        // Alcuni casi non sono limiti di `MySQL`: sono SQL invalido per
        // qualunque motore, e da quando `validate_query_operation` conosce la
        // clausola di provenienza di una window li rifiuta prima del
        // renderer. Rispondono percio in fase `Validate` e senza provider —
        // il confine giusto, e va misurato dove cade davvero.
        for (label, query, category) in cases {
            let error = match render_query(&query, "warehouse") {
                Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
                Err(error) => error,
            };
            assert_eq!(error.category, category, "{label}: {}", error.message);
            if PORTABLE_REJECTIONS.contains(&label) {
                assert_eq!(error.phase, ErrorPhase::Validate, "{label}");
                assert_eq!(error.provider, None, "{label}");
            } else {
                assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
                assert_eq!(error.provider, Some(ProviderKind::Mysql), "{label}");
            }
        }
    }

    /// `MySQL` valuta una window dopo WHERE e GROUP BY e prima di ORDER BY:
    /// fuori dalla lista di selezione la forma non e dimostrata qui.
    #[test]
    fn windows_outside_the_projection_stay_closed() {
        let position = || {
            scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                Vec::new(),
                vec![ascending(qualified("e", "event_id"))],
                None,
            )
        };
        let threshold = || QueryExpression::Parameter {
            name: "floor".to_owned(),
        };

        let mut filtered = joined_query();
        filtered.filter = Some(QueryExpression::Compare {
            left: Box::new(position()),
            operator: ComparisonOperator::Lte,
            right: Box::new(threshold()),
        });

        let mut ordered = joined_query();
        ordered.order_by = vec![ascending(position())];

        let mut keyed = joined_query();
        keyed.projection = vec![QueryProjection {
            expression: count_qualified("e", "event_id"),
            alias: Some("events".to_owned()),
        }];
        keyed.group_by = vec![position()];

        let mut filtered_group = joined_query();
        filtered_group.projection = vec![QueryProjection {
            expression: count_qualified("e", "event_id"),
            alias: Some("events".to_owned()),
        }];
        filtered_group.group_by = vec![qualified("a", "region_id")];
        filtered_group.having = Some(QueryExpression::Compare {
            left: Box::new(position()),
            operator: ComparisonOperator::Lte,
            right: Box::new(threshold()),
        });

        // `WHERE`, `GROUP BY` e `HAVING` non sono clausole in cui una window
        // sia SQL valido: le rifiuta la validazione portabile, per tutti i
        // provider e prima del renderer. `ORDER BY` invece **e** valido — solo
        // non ancora qualificato qui — e resta il caso che misura la chiusura
        // di questo provider.
        for (label, query) in [
            ("window in WHERE", filtered),
            ("window in GROUP BY", keyed),
            ("window in HAVING", filtered_group),
        ] {
            let error = match render_query(&query, "warehouse") {
                Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
                Err(error) => error,
            };
            assert_eq!(error.category, ErrorCategory::InvalidPlan, "{label}");
            assert_eq!(error.phase, ErrorPhase::Validate, "{label}");
            assert!(
                error.message.contains("fuori da projection"),
                "{label}: {}",
                error.message
            );
        }

        let error = match render_query(&ordered, "warehouse") {
            Ok(rendered) => panic!(
                "window in ORDER BY deve restare fail-closed: {}",
                rendered.sql
            ),
            Err(error) => error,
        };
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.provider, Some(ProviderKind::Mysql));
        assert!(
            error.message.contains("non ancora qualificat"),
            "{}",
            error.message
        );
    }

    /// Una window e valutata dopo il raggruppamento e prima di DISTINCT: le
    /// due combinazioni hanno semantiche precise che questa tranche non
    /// dimostra, quindi restano chiuse invece di essere renderizzate.
    #[test]
    fn windows_combined_with_grouping_or_distinct_stay_closed() {
        let position = || {
            scalar_window(
                ScalarFunction::Rank,
                Vec::new(),
                Vec::new(),
                vec![ascending(qualified("a", "region_id"))],
                None,
            )
        };

        let mut grouped = windowed_query(position());
        grouped.group_by = vec![qualified("a", "region_id")];

        let mut with_aggregate = joined_query();
        with_aggregate.projection = vec![
            QueryProjection {
                expression: count_qualified("e", "event_id"),
                alias: Some("events".to_owned()),
            },
            QueryProjection {
                expression: position(),
                alias: Some("value".to_owned()),
            },
        ];

        let mut distinct = windowed_query(position());
        distinct.distinct = true;

        for (label, query) in [
            ("window con GROUP BY", grouped),
            ("window con aggregato di gruppo", with_aggregate),
            ("window con DISTINCT", distinct),
        ] {
            let error = match render_query(&query, "warehouse") {
                Ok(rendered) => panic!("{label} deve restare fail-closed, reso: {}", rendered.sql),
                Err(error) => error,
            };
            assert_eq!(error.category, ErrorCategory::Unsupported, "{label}");
            assert_eq!(error.phase, ErrorPhase::Prepare, "{label}");
            // «non ancora qualificat…» oppure «non qualificata su <prodotto>»:
            // il secondo e il messaggio del cancello spatial, che da quando
            // legge la lista del profilo nomina il prodotto invece di rinviare
            // a una costante — su due prodotti quella costante non e piu una.
            assert!(
                error.message.contains("non ancora qualificat")
                    || error.message.contains("non qualificata su"),
                "{label}: {}",
                error.message
            );
        }
    }

    /// Funzione non ammessa sotto OVER e rango con argomenti sono gia
    /// rifiutati dalla validazione portabile in fase `Validate`. Il provider
    /// ripete il controllo per conto proprio: quel confine appartiene al path
    /// `MySQL` e non deve dipendere dall'ordine dei validatori.
    #[test]
    fn the_window_function_boundary_is_covered_by_the_core_and_by_the_provider() {
        let scalar_over = windowed_query(scalar_window(
            ScalarFunction::Lower,
            vec![qualified("e", "label")],
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            None,
        ));
        let ranked_with_arguments = windowed_query(scalar_window(
            ScalarFunction::Rank,
            vec![qualified("e", "event_id")],
            Vec::new(),
            vec![ascending(qualified("e", "event_id"))],
            None,
        ));

        for (label, query) in [
            ("funzione scalare sotto OVER", scalar_over),
            ("rango con argomenti", ranked_with_arguments),
        ] {
            let core = render_query(&query, "warehouse").expect_err(label);
            assert_eq!(core.category, ErrorCategory::InvalidPlan, "{label}");
            assert_eq!(core.phase, ErrorPhase::Validate, "{label}");

            let provider =
                ensure_qualified_shape(&query, "warehouse", &crate::profile::MYSQL_PROFILE)
                    .expect_err(label);
            assert_eq!(provider.category, ErrorCategory::InvalidPlan, "{label}");
            assert_eq!(provider.phase, ErrorPhase::Prepare, "{label}");
            assert_eq!(provider.provider, Some(ProviderKind::Mysql), "{label}");
        }
    }

    fn wire_column(name: &str, column_type: ColumnType) -> Column {
        Column::new(column_type)
            .with_name(name.as_bytes())
            .with_character_set(255)
    }

    #[test]
    fn statement_metadata_maps_to_the_arrow_contract() {
        let columns = vec![
            wire_column("id", ColumnType::MYSQL_TYPE_LONGLONG)
                .with_flags(ColumnFlags::NOT_NULL_FLAG | ColumnFlags::UNSIGNED_FLAG),
            wire_column("label", ColumnType::MYSQL_TYPE_VAR_STRING),
            wire_column("payload", ColumnType::MYSQL_TYPE_BLOB).with_character_set(63),
            wire_column("moment", ColumnType::MYSQL_TYPE_DATETIME),
        ];
        let specs = query_result_columns(&columns).expect("mapping");
        assert_eq!(specs[0].kind, MysqlColumnKind::U64);
        assert!(!specs[0].nullable);
        assert!(specs
            .iter()
            .all(|specification| specification.native_declaration.is_empty()));
        assert!(specs.iter().all(|specification| !specification
            .arrow_field()
            .metadata()
            .contains_key(protocol::MYSQL_NATIVE_DECLARATION)));
        assert_eq!(specs[1].kind, MysqlColumnKind::Utf8);
        assert!(specs[1].nullable);
        assert_eq!(specs[2].kind, MysqlColumnKind::Binary);
        assert_eq!(specs[2].native_type, "blob");
        assert_eq!(specs[3].kind, MysqlColumnKind::Timestamp);
        assert_eq!(
            specs[3]
                .arrow_field()
                .metadata()
                .get(protocol::MYSQL_NATIVE_TYPE),
            Some(&"datetime".to_owned())
        );
        assert_eq!(specs[1].arrow_field().data_type(), &DataType::Utf8);
    }

    #[test]
    fn boolean_width_matches_the_catalog_read_path() {
        let boolean = wire_column("flag", ColumnType::MYSQL_TYPE_TINY).with_column_length(1);
        assert_eq!(
            query_result_columns(std::slice::from_ref(&boolean)).expect("bool")[0].kind,
            MysqlColumnKind::Bool
        );
        let signed = wire_column("small", ColumnType::MYSQL_TYPE_TINY).with_column_length(4);
        assert_eq!(
            query_result_columns(std::slice::from_ref(&signed)).expect("tinyint")[0].kind,
            MysqlColumnKind::I8
        );
    }

    #[test]
    fn decimal_precision_is_reconstructed_and_bounded() {
        let signed = wire_column("amount", ColumnType::MYSQL_TYPE_NEWDECIMAL)
            .with_column_length(12)
            .with_decimals(2);
        assert_eq!(
            query_result_columns(std::slice::from_ref(&signed)).expect("decimal")[0].kind,
            MysqlColumnKind::Decimal {
                precision: 10,
                scale: 2,
            }
        );

        let unsigned = wire_column("total", ColumnType::MYSQL_TYPE_NEWDECIMAL)
            .with_column_length(10)
            .with_decimals(0)
            .with_flags(ColumnFlags::UNSIGNED_FLAG);
        assert_eq!(
            query_result_columns(std::slice::from_ref(&unsigned)).expect("decimal")[0].kind,
            MysqlColumnKind::Decimal {
                precision: 10,
                scale: 0,
            }
        );

        let wide = wire_column("wide", ColumnType::MYSQL_TYPE_NEWDECIMAL)
            .with_column_length(67)
            .with_decimals(0);
        assert_eq!(
            query_result_columns(std::slice::from_ref(&wide))
                .expect_err("oltre Decimal128")
                .category,
            ErrorCategory::Unsupported
        );
    }

    #[test]
    fn geometry_and_empty_result_sets_fail_closed() {
        let geometry = wire_column("geom", ColumnType::MYSQL_TYPE_GEOMETRY).with_character_set(63);
        assert_eq!(
            query_result_columns(std::slice::from_ref(&geometry))
                .expect_err("geometria")
                .category,
            ErrorCategory::Unsupported
        );
        assert_eq!(
            query_result_columns(&[])
                .expect_err("nessuna colonna")
                .category,
            ErrorCategory::Schema
        );
        let anonymous = Column::new(ColumnType::MYSQL_TYPE_LONG);
        assert_eq!(
            query_result_columns(std::slice::from_ref(&anonymous))
                .expect_err("nome vuoto")
                .category,
            ErrorCategory::Schema
        );
        let duplicate = vec![
            wire_column("same", ColumnType::MYSQL_TYPE_LONG),
            wire_column("same", ColumnType::MYSQL_TYPE_VAR_STRING),
        ];
        assert_eq!(
            query_result_columns(&duplicate)
                .expect_err("nomi output duplicati")
                .category,
            ErrorCategory::Schema
        );
    }

    #[test]
    fn renderer_is_the_shared_mysql_dialect() {
        let identifier = plenora_database_sql::Identifier::new("we`ird").expect("identifier");
        assert_eq!(
            mysql_renderer().quote_identifier(&identifier).unwrap(),
            "`we``ird`"
        );
    }

    /// Nessuna funzione verified restituisce una geometria.
    ///
    /// Undici delle ventisei che questa lista pubblicava sono state bocciate
    /// dal riferimento tutte per la stessa ragione: il mapper del result set
    /// rifiuta `MYSQL_TYPE_GEOMETRY`, quindi una funzione che restituisce una
    /// geometria non arriva mai a consegnare una riga. Nessuna delle undici
    /// aveva bisogno di un server per essere scoperta — il tipo di ritorno sta
    /// nel catalogo versionato, e il rifiuto sta in `profile.rs`. Servivano
    /// due gate live e una fixture in piedi per vedere una cosa che qui si
    /// vede in trenta millisecondi.
    ///
    /// Questa guardia non e una riscrittura della lista in un'altra forma: la
    /// lista dice **quali** funzioni, il catalogo dice **cosa restituiscono**,
    /// e le due cose si contraddicono senza che nessuno se ne accorga. Il
    /// giorno che il preflight SRID esiste, questo test cade — ed e il segnale
    /// giusto: e il momento di riaprirle, non di allentare il test.
    #[test]
    fn no_verified_function_returns_a_geometry() {
        let catalog = plenora_database_core::spatial_catalog::spatial_function_catalog()
            .expect("catalogo spatial incorporato");
        let returns = |function: &SpatialFunction| -> String {
            let id = serde_json::to_value(function)
                .expect("serializza la funzione")
                .as_str()
                .expect("l'id di wire e una stringa")
                .to_owned();
            catalog
                .functions
                .iter()
                .find(|specification| specification.id == id)
                .unwrap_or_else(|| panic!("{id} non e nel catalogo"))
                .returns
                .clone()
        };
        let geometries = VERIFIED_SPATIAL_FUNCTIONS
            .iter()
            .filter(|function| returns(function) == "geometry")
            .map(|function| format!("{function:?}"))
            .collect::<Vec<_>>();
        assert!(
            geometries.is_empty(),
            "pubblicate come verified ma il result set non le sa mappare: {}",
            geometries.join(", ")
        );
    }
}
