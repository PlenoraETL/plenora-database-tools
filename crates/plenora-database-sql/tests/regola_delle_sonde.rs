//! La regola delle sonde spatial, provata per dialetto, percorso, sonda e
//! operatore.
//!
//! Una sonda (o un valore spatial scritto) e eseguibile soltanto se il suo
//! SRID e noto e provato uguale a quello della colonna prima
//! dell'esecuzione: dal provider sul catalogo, o da una guardia del server
//! misurata live (`spatial_probe::server_guard`). Altrimenti si rifiuta
//! compilando. La decisione e una sola (`spatial_probe::admit_probe`); qui si
//! prova che **ogni** percorso che compila uno statement la chiede, con la
//! tabella attesa scritta per esteso e non ricavata dalla regola.
//!
//! I percorsi:
//!
//! - `Portable`: il compilatore portable, come lo usano `where_spatial` dello
//!   SDK (Select, Update, Delete), la facade e la CLI portable;
//! - `Mutazione`: `compile_relational_mutation`, l'ORM dello SDK e la facade
//!   in scrittura (`INSERT ... VALUES (SpatialValue)`);
//! - `Sdk`: il renderer relazionale senza catalogo, come lo costruisce il
//!   binding Python (`compile_relational_query`);
//! - `Provider`: il renderer come lo costruisce ciascun provider nel proprio
//!   path read o query. Soltanto `PostgreSQL`, SQL Server e Oracle
//!   confrontano le sonde col catalogo prima di rendere; gli altri tre non
//!   lo dichiarano e restano nella regola senza catalogo.
//!
//! Le sonde: senza SRID, con SRID 4326 (quello della colonna dei fixture
//! live), con SRID 3857, un bind nudo. Senza catalogo «4326» e «3857» danno
//! lo stesso verdetto, ed e il punto: chi compila non sa quale sia giusto, e
//! decide la guardia del server (le prove live: `scripts/sonde_srid_server.py`
//! e la suite delle sonde del provider `PostgreSQL`) o il rifiuto.

use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::{ObjectRef, ProviderKind, SortDirection};
use plenora_database_core::portable::{compile_portable, select, spatial};
use plenora_database_core::relational::{
    ColumnRef, CommonTableExpression, InsertOperation, JoinKind, MutationOperation,
    QueryDerivedSource, QueryExpression, QueryJoin, QueryOperation, QueryOrdering,
    QueryParameterType, QueryProjection, QuerySetOperation, QuerySetOperator, QuerySource,
    SpatialFunction, SpatialOperator,
};
use plenora_database_core::{
    compile_relational_mutation, ErrorCategory, SpatialPredicate, SpatialReference,
};
use plenora_database_sql::{Dialect, DialectCapabilities, Renderer, SqlServerSpatialParameter};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Percorso {
    Portable,
    Mutazione,
    Sdk,
    Provider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sonda {
    SenzaSrid,
    Srid4326,
    Srid3857,
    BindNudo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operatore {
    /// `ST_Intersects` e i suoi corrispondenti.
    Funzione,
    /// `&&`, `MBRIntersects`, `STEnvelope().STIntersects`.
    BoundingBox,
    /// Un valore scritto in una colonna.
    Scrittura,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Atteso {
    /// Compila; con la guardia `PostgreSQL` attorno alla sonda se `true`.
    Eseguibile {
        guardia: bool,
    },
    Rifiutato(ErrorCategory),
}

use Atteso::{Eseguibile, Rifiutato};
use ErrorCategory::{Crs, InvalidPlan, Unsupported};
use Operatore::{BoundingBox, Funzione, Scrittura};
use Percorso::{Mutazione, Portable, Provider, Sdk};
use ProviderKind::{Db2, Mariadb, Mysql, Oracle, Postgres, Sqlserver};
use Sonda::{BindNudo, SenzaSrid, Srid3857, Srid4326};

const ESEGUIBILE: Atteso = Eseguibile { guardia: false };
const GUARDATA: Atteso = Eseguibile { guardia: true };

/// La tabella, per esteso: percorso, sonda, operatore, poi il verdetto per
/// `PostgreSQL`, `MySQL`, `MariaDB`, SQL Server, Oracle, Db2.
#[rustfmt::skip]
const TABELLA: &[(Percorso, Sonda, Operatore, [Atteso; 6])] = &[
    // Il compilatore portable non ha il catalogo: passano soltanto
    // PostgreSQL (guardia sul typmod, gia nella forma portable) e MySQL
    // (3033/3643). Senza SRID e `crs` ovunque.
    (Portable, SenzaSrid, Funzione,    [Rifiutato(Crs); 6]),
    (Portable, Srid4326,  Funzione,    [ESEGUIBILE, ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    (Portable, Srid3857,  Funzione,    [ESEGUIBILE, ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    (Portable, SenzaSrid, BoundingBox, [Rifiutato(Crs); 6]),
    (Portable, Srid4326,  BoundingBox, [ESEGUIBILE, ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    (Portable, Srid3857,  BoundingBox, [ESEGUIBILE, ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    // Scrittura: PostgreSQL e MySQL vincolano lo SRID della colonna dal
    // server; SQL Server non ha SRID di colonna (limite dichiarato);
    // MariaDB accetta un valore in un altro sistema, Oracle e Db2 non sono
    // misurati.
    (Mutazione, SenzaSrid, Scrittura,  [Rifiutato(InvalidPlan); 6]),
    (Mutazione, Srid4326,  Scrittura,  [ESEGUIBILE, ESEGUIBILE, Rifiutato(Unsupported), ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    (Mutazione, Srid3857,  Scrittura,  [ESEGUIBILE, ESEGUIBILE, Rifiutato(Unsupported), ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    // Lo SDK relazionale: un bind nudo non ha SRID noto a chi compila; un
    // `SpatialValue` passa dove il server ha la guardia, e su PostgreSQL con
    // la guardia attorno alla sonda.
    (Sdk, BindNudo,  Funzione,         [Rifiutato(Unsupported); 6]),
    (Sdk, SenzaSrid, Funzione,         [Rifiutato(InvalidPlan); 6]),
    (Sdk, Srid4326,  Funzione,         [GUARDATA, ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    (Sdk, Srid3857,  Funzione,         [GUARDATA, ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    // Gli operatori di bounding box esistono soltanto su PostgreSQL.
    (Sdk, BindNudo,  BoundingBox,      [Rifiutato(Unsupported); 6]),
    (Sdk, Srid4326,  BoundingBox,      [GUARDATA, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    (Sdk, Srid3857,  BoundingBox,      [GUARDATA, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
    // I provider che confrontano col catalogo rendono il bind com'e; MySQL,
    // MariaDB e Db2 non hanno una forma per un bind geometrico nudo e non
    // dichiarano il confronto.
    (Provider, BindNudo, Funzione,     [ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), ESEGUIBILE, ESEGUIBILE, Rifiutato(Unsupported)]),
    (Provider, BindNudo, BoundingBox,  [ESEGUIBILE, Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported), Rifiutato(Unsupported)]),
];

const PRODOTTI: [ProviderKind; 6] = [Postgres, Mysql, Mariadb, Sqlserver, Oracle, Db2];

fn wkb_point(srid: Option<u32>) -> Vec<u8> {
    let mut bytes = vec![0x01];
    match srid {
        Some(srid) => {
            bytes.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
            bytes.extend_from_slice(&srid.to_le_bytes());
        }
        None => bytes.extend_from_slice(&1_u32.to_le_bytes()),
    }
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes
}

const fn srid_di(sonda: Sonda) -> u32 {
    match sonda {
        Sonda::SenzaSrid | Sonda::BindNudo => 0,
        Sonda::Srid4326 => 4_326,
        Sonda::Srid3857 => 3_857,
    }
}

fn portable(
    prodotto: ProviderKind,
    sonda: Sonda,
    operatore: Operatore,
) -> Result<String, ErrorCategory> {
    let srid = srid_di(sonda);
    let reference = SpatialReference {
        ewkb: wkb_point((srid != 0).then_some(srid)),
        srid,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    };
    let predicate = match operatore {
        Operatore::BoundingBox => SpatialPredicate::BoundingBox,
        _ => SpatialPredicate::Intersects,
    };
    let statement = select("features", vec!["id"])
        .where_(spatial("shape", predicate, reference))
        .into_statement();
    compile_portable(prodotto, &statement)
        .map(|compiled| compiled.sql)
        .map_err(|error| error.category)
}

fn target() -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: None,
        object: "features".to_owned(),
    }
}

fn spatial_value(srid: u32) -> QueryExpression {
    QueryExpression::SpatialValue {
        expression: Box::new(QueryExpression::TypedParameter {
            name: "sonda".to_owned(),
            parameter_type: QueryParameterType::Binary,
        }),
        srid,
        semantics: SpatialSemantics::Geometry,
    }
}

fn mutazione(prodotto: ProviderKind, sonda: Sonda) -> Result<String, ErrorCategory> {
    let operation = MutationOperation::Insert(InsertOperation {
        target: target(),
        columns: vec!["shape".to_owned()],
        rows: vec![vec![spatial_value(srid_di(sonda))]],
        returning: Vec::new(),
    });
    compile_relational_mutation(prodotto, &operation)
        .map(|lowered| lowered.sql)
        .map_err(|error| error.category)
}

fn query(probe: QueryExpression, operatore: Operatore) -> QueryOperation {
    let column = QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: "shape".to_owned(),
        },
    };
    let filter = match operatore {
        Operatore::BoundingBox => QueryExpression::SpatialOperator {
            operator: SpatialOperator::BoundingBoxIntersects,
            left: Box::new(column),
            right: Box::new(probe),
        },
        _ => QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![column, probe],
        },
    };
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("public".to_owned()),
                object: "features".to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "id".to_owned(),
                },
            },
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(filter),
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

/// Il renderer come lo costruisce il binding Python (`Sdk`) o il provider
/// nel proprio path (`Provider`).
fn renderer(prodotto: ProviderKind, percorso: Percorso) -> Renderer {
    let dialect = match prodotto {
        Postgres => Dialect::Postgres,
        Mysql | Mariadb => Dialect::Mysql,
        Sqlserver => Dialect::SqlServer,
        Oracle => Dialect::Oracle,
        _ => Dialect::Db2,
    };
    let renderer = Renderer::new(
        dialect,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_wkb_axis_option(if prodotto == Mysql {
        Some(plenora_database_sql::MYSQL_WKB_AXIS_OPTION)
    } else {
        None
    })
    .with_provider(prodotto);
    match (percorso, prodotto) {
        (Percorso::Provider, Postgres | Oracle) => renderer.with_spatial_probes_verified(),
        // SQL Server rende un bind geometrico soltanto con il profilo che il
        // preflight ha letto (semantica e SRID del valore).
        (Percorso::Provider, Sqlserver) => renderer
            .with_spatial_probes_verified()
            .with_sql_server_spatial_parameters(BTreeMap::from([(
                "sonda".to_owned(),
                SqlServerSpatialParameter {
                    semantics: SpatialSemantics::Geometry,
                    srid: 4_326,
                },
            )])),
        _ => renderer,
    }
}

fn relazionale(
    prodotto: ProviderKind,
    percorso: Percorso,
    sonda: Sonda,
    operatore: Operatore,
) -> Result<String, ErrorCategory> {
    let probe = match sonda {
        Sonda::BindNudo => QueryExpression::Parameter {
            name: "sonda".to_owned(),
        },
        other => spatial_value(srid_di(other)),
    };
    renderer(prodotto, percorso)
        .render_query(&query(probe, operatore))
        .map(|rendered| rendered.sql)
        .map_err(|error| error.category)
}

/// La guardia `PostgreSQL`: il typmod della colonna letto dal server prima
/// di ogni riga, con la tabella di base risolta dalla query.
fn ha_la_guardia(sql: &str) -> bool {
    sql.contains("postgis_typmod_srid") && sql.contains("ST_Equals(") && sql.contains("E'features'")
}

#[test]
fn ogni_percorso_chiede_la_regola_delle_sonde() {
    let mut difformi = Vec::new();
    for (percorso, sonda, operatore, attesi) in TABELLA {
        for (prodotto, atteso) in PRODOTTI.iter().zip(attesi) {
            let osservato = match percorso {
                Percorso::Portable => portable(*prodotto, *sonda, *operatore),
                Percorso::Mutazione => mutazione(*prodotto, *sonda),
                Percorso::Sdk | Percorso::Provider => {
                    relazionale(*prodotto, *percorso, *sonda, *operatore)
                }
            };
            let conforme = match (atteso, &osservato) {
                (Atteso::Eseguibile { guardia }, Ok(sql)) => {
                    *prodotto != Postgres
                        || *percorso == Percorso::Portable
                        || ha_la_guardia(sql) == *guardia
                }
                (Atteso::Rifiutato(categoria), Err(categoria_osservata)) => {
                    categoria == categoria_osservata
                }
                _ => false,
            };
            if !conforme {
                difformi.push(format!(
                    "{percorso:?} {sonda:?} {operatore:?} {prodotto:?}: atteso {atteso:?}, osservato {osservato:?}"
                ));
            }
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La forma portable `PostgreSQL` porta gia la guardia sul typmod
/// (`postgres_guarded_probe`), con relazione e colonna legate come testo.
#[test]
fn la_forma_portable_postgresql_e_quella_guardata() {
    let sql = portable(Postgres, Srid3857, Funzione).expect("PostgreSQL");
    assert!(
        sql.contains("postgis_typmod_srid") && sql.contains("ST_Equals("),
        "{sql}"
    );
}

/// Due valori dichiarati si confrontano compilando: lo stesso SRID passa
/// senza guardia, due SRID diversi sono `crs` prima di ogni server.
#[test]
fn due_valori_dichiarati_si_confrontano_compilando() {
    let query = |left: u32, right: u32| {
        let mut operation = query(spatial_value(right), Funzione);
        operation.filter = Some(QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![spatial_value(left), spatial_value(right)],
        });
        operation
    };
    let sdk = renderer(Postgres, Sdk);
    let stesso = sdk.render_query(&query(4_326, 4_326)).expect("stesso SRID");
    assert!(!ha_la_guardia(&stesso.sql), "{}", stesso.sql);
    let diversi = sdk
        .render_query(&query(4_326, 3_857))
        .expect_err("SRID diversi");
    assert_eq!(diversi.category, Crs);
}

/// Una colonna che non si risolve in una tabella di base (una sorgente
/// derivata) non ha un typmod da confrontare: senza catalogo la sonda si
/// rifiuta, invece di una guardia che leggerebbe la tabella sbagliata.
#[test]
fn una_colonna_senza_tabella_di_base_non_si_guarda() {
    let mut operation = query(spatial_value(4_326), Funzione);
    operation.filter = Some(QueryExpression::Spatial {
        function: SpatialFunction::Intersects,
        arguments: vec![
            QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("altrove".to_owned()),
                    field: "shape".to_owned(),
                },
            },
            spatial_value(4_326),
        ],
    });
    let error = renderer(Postgres, Sdk)
        .render_query(&operation)
        .expect_err("relazione non risolta");
    assert_eq!(error.category, Unsupported);
}

/// Una sonda dentro un termine che cambia frame (`ST_Transform`) non si
/// confronta con niente: senza catalogo si rifiuta.
#[test]
fn una_sonda_dentro_un_termine_senza_frame_si_rifiuta() {
    let mut operation = query(spatial_value(4_326), Funzione);
    operation.filter = Some(QueryExpression::Spatial {
        function: SpatialFunction::Intersects,
        arguments: vec![
            QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "shape".to_owned(),
                },
            },
            QueryExpression::Spatial {
                function: SpatialFunction::Transform,
                arguments: vec![
                    spatial_value(3_857),
                    QueryExpression::Parameter {
                        name: "srid".to_owned(),
                    },
                ],
            },
        ],
    });
    for prodotto in [Postgres, Mysql] {
        let error = renderer(prodotto, Sdk)
            .render_query(&operation)
            .expect_err("sonda in un termine senza frame");
        assert_eq!(error.category, Unsupported, "{prodotto:?}");
    }
}

/// Un valore spatial confrontato con `=`: nessuno confronta gli SRID, su
/// ogni percorso e con ogni prova.
#[test]
fn un_valore_spatial_fuori_da_un_nodo_spatial_si_rifiuta() {
    let mut operation = query(spatial_value(4_326), Funzione);
    operation.filter = Some(QueryExpression::Compare {
        left: Box::new(QueryExpression::Column {
            column: ColumnRef {
                relation: None,
                field: "shape".to_owned(),
            },
        }),
        operator: plenora_database_core::plan::ComparisonOperator::Eq,
        right: Box::new(spatial_value(4_326)),
    });
    for prodotto in PRODOTTI {
        for percorso in [Sdk, Provider] {
            let error = renderer(prodotto, percorso)
                .render_query(&operation)
                .expect_err("valore spatial in un confronto");
            assert_eq!(error.category, Unsupported, "{prodotto:?} {percorso:?}");
        }
    }
}

fn colonna() -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: "shape".to_owned(),
        },
    }
}

fn parametro(name: &str) -> QueryExpression {
    QueryExpression::Parameter {
        name: name.to_owned(),
    }
}

const fn funzione(function: SpatialFunction, arguments: Vec<QueryExpression>) -> QueryExpression {
    QueryExpression::Spatial {
        function,
        arguments,
    }
}

fn confronto(
    left: QueryExpression,
    operator: plenora_database_core::plan::ComparisonOperator,
    right: QueryExpression,
) -> QueryExpression {
    QueryExpression::Compare {
        left: Box::new(left),
        operator,
        right: Box::new(right),
    }
}

/// Le forme in cui una sonda arriva sotto un operatore che non e spatial: la
/// sonda la costruisce `sonda` (un `SpatialValue` 3857, o un bind nudo in
/// posizione geometrica).
#[allow(clippy::too_many_lines)]
fn sonde_annidate(sonda: &dyn Fn() -> QueryExpression) -> Vec<(&'static str, QueryExpression)> {
    use plenora_database_core::plan::ComparisonOperator::{Eq, Gt, Ne};
    use SpatialFunction::{Area, Buffer, Centroid, Transform};
    vec![
        // L'ingresso della rilettura: `g = ST_Centroid(SpatialValue 3857)` su
        // una colonna 4326 rendeva zero righe senza errore.
        (
            "g = ST_Centroid(sonda)",
            confronto(colonna(), Eq, funzione(Centroid, vec![sonda()])),
        ),
        (
            "ST_Centroid(sonda) = g",
            confronto(funzione(Centroid, vec![sonda()]), Eq, colonna()),
        ),
        (
            "g diverso da ST_Buffer(ST_Centroid(sonda), d)",
            confronto(
                colonna(),
                Ne,
                funzione(
                    Buffer,
                    vec![funzione(Centroid, vec![sonda()]), parametro("d")],
                ),
            ),
        ),
        (
            "ST_Area(sonda) > a",
            confronto(funzione(Area, vec![sonda()]), Gt, parametro("a")),
        ),
        (
            "ST_Area(sonda) + 1 > a",
            confronto(
                QueryExpression::Arithmetic {
                    left: Box::new(funzione(Area, vec![sonda()])),
                    operator: plenora_database_core::relational::ArithmeticOperator::Add,
                    right: Box::new(parametro("uno")),
                },
                Gt,
                parametro("a"),
            ),
        ),
        (
            "ST_Area(ST_Transform(sonda, s)) > a",
            confronto(
                funzione(
                    Area,
                    vec![funzione(Transform, vec![sonda(), parametro("s")])],
                ),
                Gt,
                parametro("a"),
            ),
        ),
        (
            "g IN (ST_Centroid(sonda))",
            QueryExpression::InList {
                expression: Box::new(colonna()),
                values: vec![funzione(Centroid, vec![sonda()])],
                negated: false,
            },
        ),
        (
            "ST_Area(sonda) BETWEEN a AND b",
            QueryExpression::Between {
                expression: Box::new(funzione(Area, vec![sonda()])),
                lower: Box::new(parametro("a")),
                upper: Box::new(parametro("b")),
                negated: false,
            },
        ),
        (
            "ST_Centroid(sonda) IS NULL",
            QueryExpression::IsNull {
                expression: Box::new(funzione(Centroid, vec![sonda()])),
                negated: false,
            },
        ),
        (
            "NOT (g = ST_Centroid(sonda))",
            QueryExpression::Not {
                expression: Box::new(confronto(colonna(), Eq, funzione(Centroid, vec![sonda()]))),
            },
        ),
        (
            "CASE WHEN ST_Centroid(sonda) = g THEN 1 END = 1",
            confronto(
                QueryExpression::Case {
                    branches: vec![plenora_database_core::relational::QueryCaseBranch {
                        when: confronto(funzione(Centroid, vec![sonda()]), Eq, colonna()),
                        then: parametro("uno"),
                    }],
                    else_expression: None,
                },
                Eq,
                parametro("uno"),
            ),
        ),
    ]
}

/// La regola P: una sonda sotto un operatore che non e spatial si rifiuta su
/// ogni dialetto e ogni percorso, a qualunque profondita e attraverso
/// qualunque funzione. La decisione e `admit_probe` con
/// `ProbeUse::ScalarOperand`, quindi il motivo e lo stesso ovunque.
#[test]
fn una_sonda_sotto_un_operatore_non_spatial_si_rifiuta_a_ogni_profondita() {
    let sv = || QueryExpression::SpatialValue {
        expression: Box::new(QueryExpression::TypedParameter {
            name: "sonda".to_owned(),
            parameter_type: QueryParameterType::Binary,
        }),
        srid: 3_857,
        semantics: SpatialSemantics::Geometry,
    };
    let bind = || parametro("sonda");
    let forme: [(&str, &dyn Fn() -> QueryExpression); 2] = [("sv", &sv), ("bind", &bind)];
    let mut difformi = Vec::new();
    for (sonda, forma) in forme {
        for (nome, filtro) in sonde_annidate(forma) {
            let mut operation = query(spatial_value(4_326), Funzione);
            operation.filter = Some(filtro);
            for prodotto in PRODOTTI {
                for percorso in [Sdk, Provider] {
                    let osservato = renderer(prodotto, percorso).render_query(&operation);
                    let conforme = osservato.as_ref().is_err_and(|error| {
                        error.category == Unsupported
                            && error.message
                                == plenora_database_core::spatial_probe::SPATIAL_VALUE_OUTSIDE_SPATIAL
                    });
                    if !conforme {
                        difformi.push(format!(
                            "{sonda} {nome} {prodotto:?} {percorso:?}: {:?}",
                            osservato.map(|rendered| rendered.sql)
                        ));
                    }
                }
            }
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// I controlli positivi della regola P: una sonda dentro un nodo spatial con
/// due geometrie e giudicata li, anche attraverso una funzione che conserva
/// il frame e anche quando il risultato del nodo e confrontato con `<`.
#[test]
fn una_sonda_giudicata_da_un_nodo_spatial_resta_ammessa() {
    use plenora_database_core::plan::ComparisonOperator::Lt;
    let filtri = [
        (
            "ST_Distance(g, sonda) < d",
            confronto(
                funzione(
                    SpatialFunction::Distance,
                    vec![colonna(), spatial_value(3_857)],
                ),
                Lt,
                parametro("d"),
            ),
        ),
        (
            "ST_Intersects(g, ST_Centroid(sonda))",
            funzione(
                SpatialFunction::Intersects,
                vec![
                    colonna(),
                    funzione(SpatialFunction::Centroid, vec![spatial_value(3_857)]),
                ],
            ),
        ),
    ];
    for (nome, filtro) in filtri {
        let mut operation = query(spatial_value(4_326), Funzione);
        operation.filter = Some(filtro);
        let sdk = renderer(Postgres, Sdk)
            .render_query(&operation)
            .unwrap_or_else(|error| panic!("{nome}: {error:?}"));
        assert!(ha_la_guardia(&sdk.sql), "{nome}: {}", sdk.sql);
        renderer(Postgres, Provider)
            .render_query(&operation)
            .unwrap_or_else(|error| panic!("{nome}: {error:?}"));
    }
}

// ---------------------------------------------------------------------------
// La regola P strutturale: dove una sonda puo stare.
// ---------------------------------------------------------------------------

fn colonna_di(relation: &str, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    }
}

fn sorgente(object: &str, alias: &str) -> QuerySource {
    QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: object.to_owned(),
        },
        alias: Some(alias.to_owned()),
    }
}

fn proiezione(expression: QueryExpression, alias: &str) -> QueryProjection {
    QueryProjection {
        expression,
        alias: Some(alias.to_owned()),
    }
}

/// Un blocco `SELECT projection [FROM source] [WHERE filter]`.
const fn blocco(
    source: Option<QuerySource>,
    projection: Vec<QueryProjection>,
    filter: Option<QueryExpression>,
) -> QueryOperation {
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source,
        derived_source: None,
        projection,
        joins: Vec::new(),
        filter,
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

/// `SELECT t.id FROM public.features t`.
fn base() -> QueryOperation {
    blocco(
        Some(sorgente("features", "t")),
        vec![proiezione(colonna_di("t", "id"), "id")],
        None,
    )
}

/// `(SELECT sonda AS s)`, senza FROM.
fn sonda_proiettata(sonda: QueryExpression) -> QueryOperation {
    blocco(None, vec![proiezione(sonda, "s")], None)
}

fn interseca(left: QueryExpression, right: QueryExpression) -> QueryExpression {
    funzione(SpatialFunction::Intersects, vec![left, right])
}

fn join_derivata(query: QueryOperation, alias: &str, on: QueryExpression) -> QueryJoin {
    QueryJoin {
        kind: JoinKind::Inner,
        source: None,
        derived_source: Some(QueryDerivedSource {
            query: Box::new(query),
            alias: alias.to_owned(),
        }),
        lateral: false,
        on: Some(on),
    }
}

const fn ordinamento(expression: QueryExpression) -> QueryOrdering {
    QueryOrdering {
        expression,
        direction: SortDirection::Asc,
    }
}

/// Dove la sonda sta: fuori da ogni predicato (`Fuori`), dentro un predicato
/// ma non argomento di un nodo spatial che la giudica (`NonGiudicata`), o
/// argomento di un nodo che la confronta con un termine che non e una
/// colonna della FROM dello stesso blocco (`FuoriDalBlocco`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Posizione {
    Fuori,
    NonGiudicata,
    FuoriDalBlocco,
}

/// Le forme della regola P strutturale, per una sonda costruita da `sonda`.
#[allow(clippy::too_many_lines)]
fn sonde_fuori_posto(
    sonda: &dyn Fn() -> QueryExpression,
) -> Vec<(&'static str, QueryOperation, Posizione)> {
    use plenora_database_core::plan::ComparisonOperator::{Eq, Lt};
    use Posizione::{Fuori, FuoriDalBlocco, NonGiudicata};
    let tg = || colonna_di("t", "shape");
    let mut forme = Vec::new();

    // P1: la sonda nella projection di una subquery `IN`. Su d30d1d2 0 righe
    // senza errore su PostGIS.
    let mut operation = base();
    operation.filter = Some(QueryExpression::InSubquery {
        expression: Box::new(tg()),
        query: Box::new(sonda_proiettata(sonda())),
        negated: false,
    });
    forme.push(("g IN (SELECT sonda)", operation, Fuori));

    // P2: la sonda attraverso un alias.
    let mut operation = base();
    operation.joins = vec![join_derivata(
        sonda_proiettata(sonda()),
        "d",
        confronto(tg(), Eq, colonna_di("d", "s")),
    )];
    forme.push(("JOIN (SELECT sonda AS s) d ON t.g = d.s", operation, Fuori));

    let mut operation = base();
    operation.joins = vec![join_derivata(
        sonda_proiettata(sonda()),
        "d",
        interseca(tg(), colonna_di("d", "s")),
    )];
    forme.push((
        "JOIN (SELECT sonda AS s) d ON ST_Intersects(t.g, d.s)",
        operation,
        Fuori,
    ));

    let mut operation = base();
    operation.common_table_expressions = vec![CommonTableExpression {
        name: "p".to_owned(),
        recursive: false,
        query: Box::new(sonda_proiettata(sonda())),
    }];
    operation.joins = vec![QueryJoin {
        kind: JoinKind::Inner,
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: None,
                object: "p".to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        lateral: false,
        on: Some(confronto(tg(), Eq, colonna_di("p", "s"))),
    }];
    forme.push((
        "WITH p AS (SELECT sonda AS s) JOIN p ON t.g = p.s",
        operation,
        Fuori,
    ));

    let mut operation = base();
    let mut esiste = blocco(
        None,
        vec![proiezione(colonna_di("d", "s"), "s")],
        Some(confronto(tg(), Eq, colonna_di("d", "s"))),
    );
    esiste.derived_source = Some(QueryDerivedSource {
        query: Box::new(sonda_proiettata(sonda())),
        alias: "d".to_owned(),
    });
    operation.filter = Some(QueryExpression::Exists {
        query: Box::new(esiste),
        negated: false,
    });
    forme.push((
        "EXISTS (SELECT d.s FROM (SELECT sonda AS s) d WHERE t.g = d.s)",
        operation,
        Fuori,
    ));

    // Una subquery scalare come argomento di un nodo spatial.
    let mut operation = base();
    operation.filter = Some(interseca(
        tg(),
        QueryExpression::ScalarSubquery {
            query: Box::new(sonda_proiettata(sonda())),
        },
    ));
    forme.push(("ST_Intersects(t.g, (SELECT sonda))", operation, Fuori));

    // Le clausole che non sono predicati.
    let mut operation = base();
    operation.projection.push(proiezione(
        funzione(SpatialFunction::Distance, vec![tg(), sonda()]),
        "m",
    ));
    forme.push(("SELECT ST_Distance(t.g, sonda)", operation, Fuori));

    let mut operation = base();
    operation.projection.push(proiezione(sonda(), "s"));
    forme.push(("SELECT sonda", operation, Fuori));

    let mut operation = base();
    operation.order_by = vec![ordinamento(funzione(
        SpatialFunction::Distance,
        vec![tg(), sonda()],
    ))];
    forme.push(("ORDER BY ST_Distance(t.g, sonda)", operation, Fuori));

    let mut operation = base();
    operation.order_by = vec![ordinamento(QueryExpression::SpatialOperator {
        operator: SpatialOperator::KnnDistance,
        left: Box::new(tg()),
        right: Box::new(sonda()),
    })];
    forme.push(("ORDER BY t.g <-> sonda", operation, Fuori));

    let mut operation = base();
    operation.group_by = vec![colonna_di("t", "id"), interseca(tg(), sonda())];
    forme.push(("GROUP BY ST_Intersects(t.g, sonda)", operation, Fuori));

    let mut operation = base();
    operation.projection.push(proiezione(
        QueryExpression::Window {
            function: plenora_database_core::relational::ScalarFunction::RowNumber,
            arguments: Vec::new(),
            partition_by: vec![interseca(tg(), sonda())],
            order_by: Vec::new(),
            frame: None,
        },
        "n",
    ));
    forme.push((
        "ROW_NUMBER() OVER (PARTITION BY ST_Intersects(t.g, sonda))",
        operation,
        Fuori,
    ));

    let mut operation = base();
    operation.set_operations = vec![QuerySetOperation {
        operator: QuerySetOperator::Union,
        all: true,
        query: Box::new(blocco(
            Some(sorgente("features", "u")),
            vec![proiezione(sonda(), "id")],
            None,
        )),
    }];
    forme.push(("UNION ALL (SELECT sonda FROM features u)", operation, Fuori));

    let mut operation = base();
    operation.joins = vec![join_derivata(
        blocco(
            Some(sorgente("features", "u")),
            vec![
                proiezione(colonna_di("u", "id"), "id"),
                proiezione(
                    funzione(
                        SpatialFunction::Distance,
                        vec![colonna_di("u", "shape"), sonda()],
                    ),
                    "m",
                ),
            ],
            None,
        ),
        "d",
        confronto(colonna_di("d", "m"), Lt, parametro("soglia")),
    )];
    forme.push((
        "JOIN (SELECT ST_Distance(u.g, sonda) AS m FROM features u) d ON d.m < soglia",
        operation,
        Fuori,
    ));

    // Gli involucri che nascondono la sonda al nodo spatial, nel WHERE.
    let mut operation = base();
    operation.filter = Some(interseca(
        tg(),
        QueryExpression::Case {
            branches: vec![plenora_database_core::relational::QueryCaseBranch {
                when: confronto(parametro("a"), Eq, parametro("b")),
                then: sonda(),
            }],
            else_expression: None,
        },
    ));
    forme.push((
        "ST_Intersects(t.g, CASE WHEN a = b THEN sonda END)",
        operation,
        NonGiudicata,
    ));

    let mut operation = base();
    operation.filter = Some(interseca(
        tg(),
        QueryExpression::Scalar {
            function: plenora_database_core::relational::ScalarFunction::Coalesce,
            arguments: vec![sonda(), tg()],
        },
    ));
    forme.push((
        "ST_Intersects(t.g, COALESCE(sonda, t.g))",
        operation,
        NonGiudicata,
    ));

    let mut operation = base();
    operation.filter = Some(interseca(
        tg(),
        QueryExpression::Cast {
            expression: Box::new(sonda()),
            target_type: QueryParameterType::Binary,
        },
    ));
    forme.push(("ST_Intersects(t.g, CAST(sonda))", operation, NonGiudicata));

    // Un riferimento correlato: la colonna non e nella FROM del blocco che
    // contiene il predicato.
    let mut operation = base();
    operation.filter = Some(QueryExpression::Exists {
        query: Box::new(blocco(
            Some(sorgente("altre", "u")),
            vec![proiezione(colonna_di("u", "id"), "id")],
            Some(interseca(tg(), sonda())),
        )),
        negated: false,
    });
    forme.push((
        "EXISTS (SELECT u.id FROM altre u WHERE ST_Intersects(t.g, sonda))",
        operation,
        FuoriDalBlocco,
    ));

    // Una colonna di una sorgente derivata, nello stesso blocco.
    let mut operation = base();
    operation.joins = vec![join_derivata(
        blocco(
            Some(sorgente("altre", "u")),
            vec![proiezione(colonna_di("u", "shape"), "s")],
            None,
        ),
        "d",
        interseca(colonna_di("d", "s"), sonda()),
    )];
    forme.push((
        "JOIN (SELECT u.g AS s FROM altre u) d ON ST_Intersects(d.s, sonda)",
        operation,
        FuoriDalBlocco,
    ));
    forme
}

type CostruisciSonda = Box<dyn Fn() -> QueryExpression>;

/// Le sonde della regola P strutturale: un `SpatialValue` 3857, la stessa
/// dentro `ST_Centroid`, e un bind nudo in posizione geometrica.
fn tre_sonde() -> [(&'static str, CostruisciSonda); 3] {
    [
        ("sv", Box::new(|| spatial_value(3_857))),
        (
            "ST_Centroid(sv)",
            Box::new(|| funzione(SpatialFunction::Centroid, vec![spatial_value(3_857)])),
        ),
        (
            "ST_Centroid(bind)",
            Box::new(|| funzione(SpatialFunction::Centroid, vec![parametro("sonda")])),
        ),
    ]
}

const fn motivo(posizione: Posizione) -> &'static str {
    use plenora_database_core::spatial_probe::{
        PROBE_OUTSIDE_PREDICATE, PROBE_PEER_OUTSIDE_BLOCK, SPATIAL_VALUE_OUTSIDE_SPATIAL,
    };
    match posizione {
        Posizione::Fuori => PROBE_OUTSIDE_PREDICATE,
        Posizione::NonGiudicata => SPATIAL_VALUE_OUTSIDE_SPATIAL,
        Posizione::FuoriDalBlocco => PROBE_PEER_OUTSIDE_BLOCK,
    }
}

/// La regola P strutturale: una sonda e ammessa soltanto come argomento (o
/// dentro funzioni che ne conservano il frame) di un nodo spatial che la
/// giudica, in un `WHERE`, `ON` o `HAVING` del blocco la cui FROM contiene
/// la colonna confrontata. Ogni altra posizione si rifiuta compilando, su
/// ogni dialetto e su ogni percorso, con il motivo della posizione. Su
/// d30d1d2 le forme P1 e P2 rendevano su tutti e sei i dialetti, e su `PostGIS`
/// davano 0 righe senza errore.
#[test]
fn una_sonda_fuori_da_un_predicato_spatial_del_proprio_blocco_si_rifiuta() {
    let mut difformi = Vec::new();
    let mut casi = 0;
    for (sonda, forma) in tre_sonde() {
        for (nome, operation, posizione) in sonde_fuori_posto(forma.as_ref()) {
            for prodotto in PRODOTTI {
                for percorso in [Sdk, Provider] {
                    casi += 1;
                    let osservato = renderer(prodotto, percorso).render_query(&operation);
                    let conforme = osservato.as_ref().is_err_and(|error| {
                        error.category == Unsupported && error.message == motivo(posizione)
                    });
                    if !conforme {
                        difformi.push(format!(
                            "{sonda} | {nome} | {prodotto:?} {percorso:?}: {:?}",
                            osservato
                                .map(|rendered| rendered.sql)
                                .map_err(|error| (error.category, error.message))
                        ));
                    }
                }
            }
        }
    }
    assert!(
        difformi.is_empty(),
        "{} difformi su {casi}:\n{}",
        difformi.len(),
        difformi.join("\n")
    );
}

/// I controlli positivi della regola P strutturale: le forme ammesse restano
/// ammesse, anche nel `WHERE` di una sorgente derivata, di una CTE, di un
/// `EXISTS`, nell'`ON` e nell'`HAVING`, con la colonna nella FROM dello
/// stesso blocco. Su PostgreSQL senza catalogo con la guardia sul typmod, col
/// catalogo del provider; su `MySQL` dove passa da 3033/3643.
#[test]
#[allow(clippy::too_many_lines)]
fn le_forme_ammesse_della_regola_p_restano_ammesse() {
    use plenora_database_core::plan::ComparisonOperator::{Eq, Lt};
    let sv = || spatial_value(3_857);
    let ug = || colonna_di("u", "shape");
    let dentro = |filtro: QueryExpression| {
        blocco(
            Some(sorgente("features", "u")),
            vec![proiezione(colonna_di("u", "id"), "id")],
            Some(filtro),
        )
    };
    let mut forme: Vec<(&str, QueryOperation, bool)> = vec![
        (
            "ST_Intersects(g, sonda)",
            dentro(interseca(ug(), sv())),
            true,
        ),
        (
            "ST_Intersects(g, ST_Centroid(sonda))",
            dentro(interseca(
                ug(),
                funzione(SpatialFunction::Centroid, vec![sv()]),
            )),
            true,
        ),
        (
            "ST_Distance(g, sonda) < d",
            dentro(confronto(
                funzione(SpatialFunction::Distance, vec![ug(), sv()]),
                Lt,
                parametro("d"),
            )),
            true,
        ),
        (
            "g && sonda",
            dentro(QueryExpression::SpatialOperator {
                operator: SpatialOperator::BoundingBoxIntersects,
                left: Box::new(ug()),
                right: Box::new(sv()),
            }),
            false,
        ),
    ];
    let mut operation = base();
    operation.joins = vec![join_derivata(
        dentro(interseca(ug(), sv())),
        "d",
        confronto(colonna_di("d", "id"), Eq, colonna_di("t", "id")),
    )];
    forme.push((
        "JOIN (SELECT u.id FROM features u WHERE ST_Intersects(u.g, sonda)) d",
        operation,
        true,
    ));
    let mut operation = base();
    operation.common_table_expressions = vec![CommonTableExpression {
        name: "vicine".to_owned(),
        recursive: false,
        query: Box::new(dentro(interseca(ug(), sv()))),
    }];
    forme.push((
        "WITH vicine AS (SELECT u.id FROM features u WHERE ST_Intersects(u.g, sonda))",
        operation,
        true,
    ));
    let mut operation = base();
    operation.filter = Some(QueryExpression::Exists {
        query: Box::new(dentro(interseca(ug(), sv()))),
        negated: false,
    });
    forme.push((
        "EXISTS (SELECT u.id FROM features u WHERE ST_Intersects(u.g, sonda))",
        operation,
        true,
    ));
    let mut operation = base();
    operation.joins = vec![QueryJoin {
        kind: JoinKind::Inner,
        source: Some(sorgente("features", "u")),
        derived_source: None,
        lateral: false,
        on: Some(QueryExpression::And {
            arguments: vec![
                confronto(colonna_di("u", "id"), Eq, colonna_di("t", "id")),
                interseca(ug(), sv()),
            ],
        }),
    }];
    forme.push((
        "JOIN features u ON u.id = t.id AND ST_Intersects(u.g, sonda)",
        operation,
        true,
    ));
    let mut operation = dentro(interseca(ug(), sv()));
    operation.filter = None;
    operation.group_by = vec![colonna_di("u", "id"), ug()];
    operation.having = Some(interseca(ug(), sv()));
    forme.push((
        "GROUP BY u.id, u.g HAVING ST_Intersects(u.g, sonda)",
        operation,
        true,
    ));

    let mut difformi = Vec::new();
    for (nome, operation, mysql) in &forme {
        match renderer(Postgres, Sdk).render_query(operation) {
            Ok(rendered) if rendered.sql.contains("postgis_typmod_srid") => {}
            other => difformi.push(format!("{nome} Postgres Sdk: {other:?}")),
        }
        if let Err(error) = renderer(Postgres, Provider).render_query(operation) {
            difformi.push(format!("{nome} Postgres Provider: {error:?}"));
        }
        if *mysql {
            if let Err(error) = renderer(Mysql, Sdk).render_query(operation) {
                difformi.push(format!("{nome} Mysql Sdk: {error:?}"));
            }
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

// ---------------------------------------------------------------------------
// Il mondo chiuso: ogni argomento geometrico e una colonna, una funzione
// spatial o una sonda diretta.
// ---------------------------------------------------------------------------

type Involucro = Box<dyn Fn(QueryExpression) -> QueryExpression>;

fn cast(target_type: QueryParameterType) -> Involucro {
    Box::new(move |inner| QueryExpression::Cast {
        expression: Box::new(inner),
        target_type,
    })
}

/// Gli involucri che nascondono una sonda al nodo spatial: nessuno conserva
/// il frame in modo che chi compila lo veda.
#[allow(clippy::too_many_lines)]
fn involucri() -> Vec<(&'static str, Involucro)> {
    use plenora_database_core::plan::ComparisonOperator::Eq;
    use plenora_database_core::relational::{ArithmeticOperator, QueryCaseBranch, ScalarFunction};
    vec![
        ("CAST(x AS BINARY)", cast(QueryParameterType::Binary)),
        ("CAST(x AS STRING)", cast(QueryParameterType::String)),
        (
            "COALESCE(x)",
            Box::new(|inner| QueryExpression::Scalar {
                function: ScalarFunction::Coalesce,
                arguments: vec![inner],
            }),
        ),
        (
            "COALESCE(x, t.g)",
            Box::new(|inner| QueryExpression::Scalar {
                function: ScalarFunction::Coalesce,
                arguments: vec![inner, colonna_di("t", "shape")],
            }),
        ),
        (
            "UPPER(x)",
            Box::new(|inner| QueryExpression::Scalar {
                function: ScalarFunction::Upper,
                arguments: vec![inner],
            }),
        ),
        (
            "CASE WHEN a = b THEN x END",
            Box::new(|inner| QueryExpression::Case {
                branches: vec![QueryCaseBranch {
                    when: confronto(parametro("a"), Eq, parametro("b")),
                    then: inner,
                }],
                else_expression: None,
            }),
        ),
        (
            "CASE WHEN a = b THEN t.g ELSE x END",
            Box::new(|inner| QueryExpression::Case {
                branches: vec![QueryCaseBranch {
                    when: confronto(parametro("a"), Eq, parametro("b")),
                    then: colonna_di("t", "shape"),
                }],
                else_expression: Some(Box::new(inner)),
            }),
        ),
        (
            "x + 0",
            Box::new(|inner| QueryExpression::Arithmetic {
                left: Box::new(inner),
                operator: ArithmeticOperator::Add,
                right: Box::new(parametro("zero")),
            }),
        ),
        (
            "(SELECT x)",
            Box::new(|inner| QueryExpression::ScalarSubquery {
                query: Box::new(sonda_proiettata(inner)),
            }),
        ),
        (
            "ST_Buffer(CAST(x AS BINARY), d)",
            Box::new(|inner| {
                funzione(
                    SpatialFunction::Buffer,
                    vec![
                        QueryExpression::Cast {
                            expression: Box::new(inner),
                            target_type: QueryParameterType::Binary,
                        },
                        parametro("d"),
                    ],
                )
            }),
        ),
        (
            "ST_Transform(COALESCE(x), s)",
            Box::new(|inner| {
                funzione(
                    SpatialFunction::Transform,
                    vec![
                        QueryExpression::Scalar {
                            function: ScalarFunction::Coalesce,
                            arguments: vec![inner],
                        },
                        parametro("s"),
                    ],
                )
            }),
        ),
    ]
}

/// Le sonde che un involucro puo nascondere: il bind nudo e quello tipizzato
/// (la forma dello SDK `p.bind("p", BindType.BINARY)`), un `SpatialValue`,
/// un bind sotto `ST_Centroid`.
fn quattro_sonde() -> [(&'static str, CostruisciSonda); 4] {
    [
        ("bind", Box::new(|| parametro("sonda"))),
        (
            "bind BINARY",
            Box::new(|| QueryExpression::TypedParameter {
                name: "sonda".to_owned(),
                parameter_type: QueryParameterType::Binary,
            }),
        ),
        ("sv 3857", Box::new(|| spatial_value(3_857))),
        (
            "ST_Centroid(bind)",
            Box::new(|| funzione(SpatialFunction::Centroid, vec![parametro("sonda")])),
        ),
    ]
}

/// I nodi spatial che confrontano due geometrie, con `x` in una posizione
/// geometrica e la colonna del blocco nell'altra.
fn nodi_che_confrontano(x: &QueryExpression) -> Vec<(&'static str, QueryExpression)> {
    use plenora_database_core::plan::ComparisonOperator::Lt;
    let tg = || colonna_di("t", "shape");
    vec![
        ("ST_Intersects(t.g, x)", interseca(tg(), x.clone())),
        ("ST_Intersects(x, t.g)", interseca(x.clone(), tg())),
        (
            "ST_DWithin(t.g, x, d)",
            funzione(
                SpatialFunction::DWithin,
                vec![tg(), x.clone(), parametro("d")],
            ),
        ),
        (
            "ST_Distance(t.g, x) < d",
            confronto(
                funzione(SpatialFunction::Distance, vec![tg(), x.clone()]),
                Lt,
                parametro("d"),
            ),
        ),
        (
            "NOT ST_Intersects(t.g, x)",
            QueryExpression::Not {
                expression: Box::new(interseca(tg(), x.clone())),
            },
        ),
        (
            "t.g && x",
            QueryExpression::SpatialOperator {
                operator: SpatialOperator::BoundingBoxIntersects,
                left: Box::new(tg()),
                right: Box::new(x.clone()),
            },
        ),
        (
            "x ~ t.g",
            QueryExpression::SpatialOperator {
                operator: SpatialOperator::BoundingBoxContains,
                left: Box::new(x.clone()),
                right: Box::new(tg()),
            },
        ),
    ]
}

/// La settima rilettura: una sonda nascosta da un involucro non spatial in
/// posizione geometrica non era riconosciuta come sonda da nessun renderer e
/// da nessun provider, e arrivava al server senza confronto di SRID
/// (`PostGIS` `t.g && CAST(:p AS BINARY)` con EWKB 3857: 1 riga su una
/// colonna 4326; `ST_Intersects` con una sonda 3857 lontana: 0 righe; SQL
/// Server 0 righe anche sotto `NOT`; `MariaDB` 1 riga). Ora ogni involucro,
/// per ogni sonda, nodo, dialetto e percorso, si rifiuta compilando. Su
/// 9fb015b le forme con un bind rendevano.
#[test]
fn una_sonda_nascosta_da_un_involucro_si_rifiuta_ovunque() {
    use plenora_database_core::spatial_probe::{
        GEOMETRY_ARGUMENT_UNCLASSIFIED, PROBE_OUTSIDE_PREDICATE, SPATIAL_VALUE_OUTSIDE_SPATIAL,
    };
    let mut difformi = Vec::new();
    let mut casi = 0;
    for (sonda, forma) in quattro_sonde() {
        for (involucro, avvolgi) in involucri() {
            for (nodo, filtro) in nodi_che_confrontano(&avvolgi(forma())) {
                let mut operation = base();
                operation.filter = Some(filtro);
                for prodotto in PRODOTTI {
                    for percorso in [Sdk, Provider] {
                        casi += 1;
                        let osservato = renderer(prodotto, percorso).render_query(&operation);
                        // Un bind nudo nell'involucro e l'argomento non
                        // classificato; una sonda riconoscibile dentro
                        // l'involucro porta il motivo della propria posizione.
                        let conforme = osservato.as_ref().is_err_and(|error| {
                            error.category == Unsupported
                                && if sonda.starts_with("bind") {
                                    error.message == GEOMETRY_ARGUMENT_UNCLASSIFIED
                                } else {
                                    [
                                        GEOMETRY_ARGUMENT_UNCLASSIFIED,
                                        SPATIAL_VALUE_OUTSIDE_SPATIAL,
                                        PROBE_OUTSIDE_PREDICATE,
                                    ]
                                    .contains(&error.message.as_str())
                                }
                        });
                        if !conforme {
                            difformi.push(format!(
                                "{sonda} | {involucro} | {nodo} | {prodotto:?} {percorso:?}: {:?}",
                                osservato
                                    .map(|rendered| rendered.sql)
                                    .map_err(|error| (error.category, error.message))
                            ));
                        }
                    }
                }
            }
        }
    }
    assert!(
        difformi.is_empty(),
        "{} difformi su {casi}:\n{}",
        difformi.len(),
        difformi.join("\n")
    );
}

/// Il mondo chiuso anche senza sonde riconoscibili: un valore che arriva in
/// posizione geometrica da una colonna di una sorgente derivata o di una
/// CTE definita da un bind (a qualunque livello, in qualunque ramo), o da
/// una colonna senza qualificazione che la FROM esterna potrebbe fornire,
/// non e una colonna di relazione. Su 9fb015b `JOIN (SELECT
/// CAST(:p AS BINARY) AS s) d ON ST_Intersects(t.g, d.s)` rendeva.
#[test]
#[allow(clippy::too_many_lines)]
fn un_argomento_geometrico_non_classificato_si_rifiuta() {
    use plenora_database_core::spatial_probe::GEOMETRY_ARGUMENT_UNCLASSIFIED;
    let tg = || colonna_di("t", "shape");
    let bind_in_cast = || QueryExpression::Cast {
        expression: Box::new(parametro("sonda")),
        target_type: QueryParameterType::Binary,
    };
    let mut forme: Vec<(&str, QueryOperation)> = Vec::new();

    let mut operation = base();
    operation.joins = vec![join_derivata(
        sonda_proiettata(bind_in_cast()),
        "d",
        interseca(tg(), colonna_di("d", "s")),
    )];
    forme.push((
        "JOIN (SELECT CAST(:p) AS s) d ON ST_Intersects(t.g, d.s)",
        operation,
    ));

    let mut operation = base();
    operation.joins = vec![join_derivata(
        sonda_proiettata(parametro("sonda")),
        "d",
        QueryExpression::SpatialOperator {
            operator: SpatialOperator::BoundingBoxIntersects,
            left: Box::new(tg()),
            right: Box::new(colonna_di("d", "s")),
        },
    )];
    forme.push(("JOIN (SELECT :p AS s) d ON t.g && d.s", operation));

    let mut operation = base();
    operation.common_table_expressions = vec![CommonTableExpression {
        name: "p".to_owned(),
        recursive: false,
        query: Box::new(sonda_proiettata(bind_in_cast())),
    }];
    operation.joins = vec![QueryJoin {
        kind: JoinKind::Inner,
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: None,
                object: "p".to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        lateral: false,
        on: Some(interseca(tg(), colonna_di("p", "s"))),
    }];
    forme.push((
        "WITH p AS (SELECT CAST(:p) AS s) JOIN p ON ST_Intersects(t.g, p.s)",
        operation,
    ));

    // Una colonna senza qualificazione in un blocco annidato: SQL la
    // risolve nella sorgente derivata esterna se la tabella interna non ha
    // quella colonna, e chi compila non lo sa.
    let mut operation = blocco(None, vec![proiezione(colonna_di("d", "s"), "s")], None);
    operation.derived_source = Some(QueryDerivedSource {
        query: Box::new(sonda_proiettata(bind_in_cast())),
        alias: "d".to_owned(),
    });
    operation.filter = Some(QueryExpression::Exists {
        query: Box::new(blocco(
            Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("public".to_owned()),
                    object: "features".to_owned(),
                },
                alias: None,
            }),
            vec![proiezione(colonna(), "g")],
            Some(interseca(
                colonna(),
                QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "s".to_owned(),
                    },
                },
            )),
        )),
        negated: false,
    });
    forme.push((
        "FROM (SELECT CAST(:p) AS s) d WHERE EXISTS (SELECT g FROM features WHERE ST_Intersects(g, s))",
        operation,
    ));

    // Un argomento geometrico non classificato fuori da un predicato.
    let mut operation = base();
    operation.projection.push(proiezione(
        funzione(SpatialFunction::Area, vec![bind_in_cast()]),
        "a",
    ));
    forme.push(("SELECT ST_Area(CAST(:p))", operation));

    let mut operation = base();
    operation.order_by = vec![ordinamento(QueryExpression::SpatialOperator {
        operator: SpatialOperator::KnnDistance,
        left: Box::new(tg()),
        right: Box::new(bind_in_cast()),
    })];
    forme.push(("ORDER BY t.g <-> CAST(:p)", operation));

    // La colonna di una sorgente derivata si segue fino alla projection che
    // la definisce, a ogni livello e in ogni ramo di set operation.
    let mut operation = base();
    let mut esterna = blocco(None, vec![proiezione(colonna_di("i", "s"), "s")], None);
    esterna.derived_source = Some(QueryDerivedSource {
        query: Box::new(sonda_proiettata(bind_in_cast())),
        alias: "i".to_owned(),
    });
    operation.joins = vec![join_derivata(
        esterna,
        "d",
        interseca(tg(), colonna_di("d", "s")),
    )];
    forme.push((
        "JOIN (SELECT i.s FROM (SELECT CAST(:p) AS s) i) d ON ST_Intersects(t.g, d.s)",
        operation,
    ));

    let mut operation = base();
    let mut rami = blocco(
        Some(sorgente("altre", "u")),
        vec![proiezione(colonna_di("u", "shape"), "s")],
        None,
    );
    rami.set_operations = vec![QuerySetOperation {
        operator: QuerySetOperator::Union,
        all: true,
        query: Box::new(sonda_proiettata(bind_in_cast())),
    }];
    operation.joins = vec![join_derivata(
        rami,
        "d",
        interseca(tg(), colonna_di("d", "s")),
    )];
    forme.push((
        "JOIN (SELECT u.g AS s FROM altre u UNION ALL SELECT CAST(:p)) d ON ST_Intersects(t.g, d.s)",
        operation,
    ));

    let mut difformi = Vec::new();
    for (nome, operation) in &forme {
        for prodotto in PRODOTTI {
            for percorso in [Sdk, Provider] {
                let osservato = renderer(prodotto, percorso).render_query(operation);
                let conforme = osservato.as_ref().is_err_and(|error| {
                    error.category == Unsupported && error.message == GEOMETRY_ARGUMENT_UNCLASSIFIED
                });
                if !conforme {
                    difformi.push(format!(
                        "{nome} | {prodotto:?} {percorso:?}: {:?}",
                        osservato
                            .map(|rendered| rendered.sql)
                            .map_err(|error| (error.category, error.message))
                    ));
                }
            }
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Il mondo chiuso non rifiuta le forme che classifica: colonna contro
/// colonna (anche correlata e qualificata, anche sotto `ST_Transform`), una
/// funzione spatial sulla colonna nella projection, una colonna di una
/// sorgente derivata o di una CTE definita da colonne di tabella (anche in
/// una set operation), il bind diretto dei provider col catalogo,
/// `ST_DWithin` con un valore dichiarato.
#[test]
#[allow(clippy::too_many_lines)]
fn le_forme_classificate_restano_ammesse() {
    use plenora_database_core::spatial_probe::GEOMETRY_ARGUMENT_UNCLASSIFIED;
    let tg = || colonna_di("t", "shape");
    let ug = || colonna_di("u", "shape");
    let con_u = |on: QueryExpression| {
        let mut operation = base();
        operation.joins = vec![QueryJoin {
            kind: JoinKind::Inner,
            source: Some(sorgente("altre", "u")),
            derived_source: None,
            lateral: false,
            on: Some(on),
        }];
        operation
    };
    // Senza sonde.
    let mut senza_sonde: Vec<(&str, QueryOperation)> = vec![
        (
            "JOIN altre u ON ST_Intersects(t.g, u.g)",
            con_u(interseca(tg(), ug())),
        ),
        (
            "JOIN altre u ON ST_Intersects(ST_Centroid(t.g), u.g)",
            con_u(interseca(
                funzione(SpatialFunction::Centroid, vec![tg()]),
                ug(),
            )),
        ),
        (
            "JOIN altre u ON ST_Intersects(ST_Transform(t.g, s), u.g)",
            con_u(interseca(
                funzione(SpatialFunction::Transform, vec![tg(), parametro("s")]),
                ug(),
            )),
        ),
    ];
    let mut operation = base();
    operation.filter = Some(QueryExpression::Exists {
        query: Box::new(blocco(
            Some(sorgente("altre", "u")),
            vec![proiezione(colonna_di("u", "id"), "id")],
            Some(interseca(ug(), tg())),
        )),
        negated: false,
    });
    senza_sonde.push((
        "EXISTS (SELECT u.id FROM altre u WHERE ST_Intersects(u.g, t.g))",
        operation,
    ));
    let mut operation = base();
    operation
        .projection
        .push(proiezione(funzione(SpatialFunction::Area, vec![tg()]), "a"));
    senza_sonde.push(("SELECT ST_Area(t.g)", operation));

    // Le colonne di sorgenti derivate e di CTE definite da colonne di
    // tabella: la classifica segue l'alias fino alla projection.
    let derivata_da = |inner: QueryOperation| {
        let mut operation = blocco(
            None,
            vec![proiezione(
                funzione(SpatialFunction::ConvexHull, vec![colonna_di("d", "g")]),
                "j",
            )],
            None,
        );
        operation.derived_source = Some(QueryDerivedSource {
            query: Box::new(inner),
            alias: "d".to_owned(),
        });
        operation
    };
    let colonna_u = || {
        blocco(
            Some(sorgente("features", "u")),
            vec![proiezione(colonna_di("u", "shape"), "g")],
            None,
        )
    };
    senza_sonde.push((
        "SELECT ST_ConvexHull(d.g) FROM (SELECT u.g FROM features u) d",
        derivata_da(colonna_u()),
    ));
    let mut rami = colonna_u();
    rami.set_operations = vec![QuerySetOperation {
        operator: QuerySetOperator::Union,
        all: true,
        query: Box::new(blocco(
            Some(sorgente("altre", "v")),
            vec![proiezione(
                funzione(SpatialFunction::Centroid, vec![colonna_di("v", "shape")]),
                "g",
            )],
            None,
        )),
    }];
    senza_sonde.push((
        "SELECT ST_ConvexHull(d.g) FROM (SELECT u.g ... UNION ALL SELECT ST_Centroid(v.g) ...) d",
        derivata_da(rami),
    ));
    let mut cte = blocco(
        Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: None,
                object: "valori".to_owned(),
            },
            alias: Some("scope".to_owned()),
        }),
        vec![proiezione(
            funzione(SpatialFunction::ConvexHull, vec![colonna_di("scope", "g")]),
            "j",
        )],
        None,
    );
    cte.common_table_expressions = vec![CommonTableExpression {
        name: "valori".to_owned(),
        recursive: false,
        query: Box::new(colonna_u()),
    }];
    senza_sonde.push((
        "WITH valori AS (SELECT u.g FROM features u) SELECT ST_ConvexHull(scope.g) FROM valori scope",
        cte,
    ));
    let mut join_derivata_colonna = base();
    join_derivata_colonna.joins = vec![join_derivata(
        colonna_u(),
        "d",
        interseca(tg(), colonna_di("d", "g")),
    )];
    senza_sonde.push((
        "JOIN (SELECT u.g FROM features u) d ON ST_Intersects(t.g, d.g)",
        join_derivata_colonna,
    ));

    let mut difformi = Vec::new();
    for (nome, operation) in &senza_sonde {
        for prodotto in PRODOTTI {
            for percorso in [Sdk, Provider] {
                // Un dialetto che non ha la funzione la rifiuta per conto
                // proprio: qui conta che il mondo chiuso non lo faccia.
                if let Err(error) = renderer(prodotto, percorso).render_query(operation) {
                    if error.message == GEOMETRY_ARGUMENT_UNCLASSIFIED {
                        difformi.push(format!("{nome} | {prodotto:?} {percorso:?}: {error:?}"));
                    }
                }
            }
        }
        if let Err(error) = renderer(Postgres, Sdk).render_query(operation) {
            difformi.push(format!("{nome} | Postgres Sdk: {error:?}"));
        }
    }

    // Il bind diretto del provider che confronta col catalogo, e il valore
    // dichiarato con la guardia senza catalogo, in `ST_DWithin` (soltanto
    // PostGIS; il bind diretto degli altri provider e nella tabella).
    let mut operation = base();
    operation.filter = Some(funzione(
        SpatialFunction::DWithin,
        vec![tg(), parametro("sonda"), parametro("d")],
    ));
    if let Err(error) = renderer(Postgres, Provider).render_query(&operation) {
        difformi.push(format!(
            "ST_DWithin(t.g, :p, d) | Postgres Provider: {error:?}"
        ));
    }
    let mut operation = base();
    operation.filter = Some(funzione(
        SpatialFunction::DWithin,
        vec![tg(), spatial_value(3_857), parametro("d")],
    ));
    match renderer(Postgres, Sdk).render_query(&operation) {
        Ok(rendered) if rendered.sql.contains("postgis_typmod_srid") => {}
        other => difformi.push(format!("ST_DWithin(t.g, sv, d) Postgres Sdk: {other:?}")),
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// `render_filter` passa dalla stessa regola: il suo predicato isolato si
/// giudica con la colonna della relazione filtrata.
#[test]
fn render_filter_giudica_le_sonde_del_predicato() {
    use plenora_database_sql::{Expression, Identifier};
    let filtro = Expression::SpatialPredicate {
        function: SpatialFunction::Intersects,
        field: Identifier::new("shape").expect("identificatore"),
        geometry_parameter: Some("sonda".to_owned()),
        distance_parameter: None,
    };
    // Senza catalogo un bind nudo non ha SRID noto; col catalogo passa.
    let error = renderer(Postgres, Sdk)
        .render_filter(&filtro)
        .expect_err("bind nudo senza catalogo");
    assert_eq!(error.category, Unsupported);
    renderer(Postgres, Provider)
        .render_filter(&filtro)
        .expect("bind col catalogo");
    renderer(Oracle, Provider)
        .render_filter_for_relation(&filtro, &Identifier::new("T").expect("identificatore"))
        .expect("bind col catalogo, relazione qualificata");
}
