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
use plenora_database_core::plan::{ObjectRef, ProviderKind};
use plenora_database_core::portable::{compile_portable, select, spatial};
use plenora_database_core::relational::{
    ColumnRef, InsertOperation, MutationOperation, QueryExpression, QueryOperation,
    QueryParameterType, QueryProjection, QuerySource, SpatialFunction, SpatialOperator,
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
