use crate::error::classify_error;
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::relational::SpatialFunction;
use plenora_database_core::{ErrorPhase, Result};
use std::collections::BTreeMap;
use tokio_postgres::Client;

pub async fn capability_document(client: &Client) -> Result<ProviderCapabilities> {
    let row = client
        .query_one(
            r"
            SELECT
                current_setting('server_version'),
                COALESCE(
                  (SELECT extversion FROM pg_extension WHERE extname = 'postgis'),
                  ''
                )
            ",
            &[],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let server_version: String = row.get(0);
    let postgis_version: String = row.get(1);
    let spatial = !postgis_version.is_empty();
    let mut extensions = BTreeMap::new();
    if spatial {
        extensions.insert("postgis".to_owned(), postgis_version);
    }
    Ok(ProviderCapabilities {
        schema_version: 2,
        provider: ProviderKind::Postgres,
        provider_version: server_version,
        extension_versions: extensions,
        reads: ReadCapabilities {
            streaming: true,
            // Il data path usa RowStream con backpressure, ma non espone un
            // cursore server nominato o riprendibile.
            server_cursor: false,
            // Il piano rende `OFFSET n` e l'engine lega la bandiera al campo.
            pagination: true,
            projection: true,
            filter: true,
            ordering: true,
            resumable: false,
        },
        writes: WriteCapabilities {
            create: true,
            append: true,
            // Su PostgreSQL `TRUNCATE` e transazionale: le righe tornano
            // indietro insieme a tutto il resto se qualcosa fallisce dopo.
            truncate_insert: true,
            update: true,
            upsert: true,
            replace: true,
            delete_by_keys: true,
            bulk: true,
            array_binding: false,
            // WriteOutcome non trasporta ancora righe restituite.
            returning: false,
            rollback_on_failure: true,
        },
        transactions: TransactionCapabilities {
            single_transaction: true,
            // `live_savepoint_rollback_preserves_prior_statements` prova
            // SAVEPOINT, ROLLBACK TO e RELEASE attraverso lo scope pubblico.
            savepoints: true,
            transactional_ddl: true,
            staged_swap: true,
            scope: TransactionScope::Transaction,
        },
        spatial: if spatial {
            probe_spatial(client).await?
        } else {
            SpatialCapabilities {
                read_wkb: false,
                write_wkb: false,
                geometry: false,
                // Questo ramo e «PostGIS assente»: nessuno dei due tipi
                // esiste, quindi non c'e nulla da pubblicare. Il ramo sopra li
                // scopre dal catalogo quando l'estensione c'e.
                geography: false,
                spatial_index: false,
                mixed_geometry_types: false,
                // Nessuna semantica dichiarata, nessuna voce: una chiave qui
                // sarebbe una promessa su un tipo che il prodotto dice di non
                // avere.
                functions_by_semantics: BTreeMap::new(),
                dimensions: Vec::new(),
                functions: Vec::new(),
                // Senza PostGIS non c'e geometria da leggere, quindi non c'e
                // niente per cui pretendere un CRS dichiarato. `false` qui
                // dice «nessuna condizione», non «la condizione e soddisfatta».
                requires_declared_crs: false,
            }
        },
        limits: ProviderLimits {
            max_identifier_bytes: Some(63),
            max_bind_parameters: Some(65_535),
            max_statement_bytes: None,
            max_batch_rows: None,
            max_payload_bytes: None,
        },
    })
}

/// Una forma invocabile di una funzione `PostGIS`.
///
/// `pronargs` conta i parametri **dichiarati**, `pronargdefaults` quelli con un
/// default: la funzione e invocabile con qualunque arita fra i due. Il solo
/// `pronargs` avrebbe scartato undici funzioni del catalogo che `PostGIS` 3.4
/// dichiara con parametri opzionali (`ST_Difference`, `ST_Force3D`,
/// `ST_Subdivide`, ...).
struct Overload {
    minimum_arity: i32,
    maximum_arity: i32,
    /// Nomi dei tipi degli argomenti dichiarati, in ordine.
    argument_types: Vec<String>,
}

impl Overload {
    /// La forma dichiarata dal catalogo e invocabile su questo overload.
    ///
    /// L'arita da sola non basta: `ST_AsMVTGeom` esiste con due argomenti, ma
    /// il secondo e un `box2d`, non una geometria — e il catalogo lo dichiara
    /// geometrico. Una capability aperta su quel confronto avrebbe promesso una
    /// chiamata che il renderer non puo formare.
    ///
    /// Le posizioni non geometriche non vengono vincolate: il catalogo le
    /// descrive con categorie portabili (`integer`, `number`, `text`, `row`)
    /// che non hanno un solo tipo `PostgreSQL` corrispondente, e pretendere una
    /// corrispondenza esatta chiuderebbe capability valide.
    ///
    /// `semantics` e il tipo con cui la chiamata deve essere formabile:
    /// `geometry` oppure `geography`. Non sono alternative — vedi
    /// `probe_spatial`.
    ///
    /// `arity` e il numero di argomenti da verificare, non quello canonico del
    /// catalogo: il core accetta piu forme per lo stesso identificatore, e una
    /// capability e indivisibile — se una sola forma non e invocabile, la
    /// funzione non puo essere pubblicata.
    fn accepts(&self, geometry_positions: &[usize], arity: usize, semantics: &str) -> bool {
        let Ok(arity) = i32::try_from(arity) else {
            return false;
        };
        if arity < self.minimum_arity || arity > self.maximum_arity {
            return false;
        }
        geometry_positions.iter().all(|position| {
            self.argument_types
                .get(*position)
                .is_some_and(|actual| actual == semantics)
        })
    }
}

/// Le funzioni che l'estensione `PostGIS` fornisce, con le forme invocabili.
///
/// `prokind` include le aggregate (`a`) e le window (`w`): `ST_Extent` e
/// un'aggregata, `ST_ClusterDBSCAN` e `ST_ClusterKMeans` sono window function,
/// e filtrarle avrebbe chiuso tre capability che il riferimento fornisce.
async fn postgis_overloads(client: &Client) -> Result<BTreeMap<String, Vec<Overload>>> {
    let rows = client
        .query(
            r"
            SELECT
                lower(p.proname),
                (p.pronargs - p.pronargdefaults)::int,
                p.pronargs::int,
                ARRAY(
                    SELECT format_type(argument, NULL)
                    FROM unnest(p.proargtypes) AS argument
                )
            FROM pg_proc p
            JOIN pg_depend d
              ON d.classid = 'pg_proc'::regclass
             AND d.objid = p.oid
             AND d.refclassid = 'pg_extension'::regclass
             AND d.deptype = 'e'
            JOIN pg_extension e ON e.oid = d.refobjid
            WHERE e.extname = 'postgis'
              AND p.prokind IN ('f', 'a', 'w')
              -- Il renderer emette i nomi **non qualificati**: una funzione
              -- che appartiene all'estensione ma sta in uno schema fuori dal
              -- `search_path` esiste e non e invocabile dal SQL generato.
              AND pg_function_is_visible(p.oid)
            ",
            &[],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;

    let mut overloads: BTreeMap<String, Vec<Overload>> = BTreeMap::new();
    for row in &rows {
        let name: String = row.get(0);
        let minimum_arity: i32 = row.get(1);
        let maximum_arity: i32 = row.get(2);
        let argument_types: Vec<String> = row.get(3);
        overloads.entry(name).or_default().push(Overload {
            minimum_arity,
            maximum_arity,
            argument_types,
        });
    }
    Ok(overloads)
}

/// I tipi e l'opclass che `PostGIS` mette a disposizione **qui**.
///
/// Ogni oggetto e cercato per appartenenza all'estensione e per visibilita nel
/// `search_path`: il renderer emette nomi non qualificati, quindi un oggetto
/// che esiste in uno schema non visibile non e invocabile dal SQL generato, e
/// aprirci una capability sarebbe un falso positivo riproducibile.
///
/// Restituisce `(geometry, geography, gist_geometry, gist_geography)`.
async fn postgis_shapes(client: &Client) -> Result<(bool, bool, bool, bool)> {
    let shapes = client
        .query_one(
            r"
            WITH gist_default AS (
                SELECT t.typname
                FROM pg_opclass oc
                JOIN pg_am am ON am.oid = oc.opcmethod
                JOIN pg_type t ON t.oid = oc.opcintype
                -- L'opclass **stessa** deve appartenere all'estensione: legare
                -- solo il tipo lasciava passare qualunque opclass GiST definita
                -- da chiunque.
                JOIN pg_depend doc
                  ON doc.classid = 'pg_opclass'::regclass AND doc.objid = oc.oid
                 AND doc.refclassid = 'pg_extension'::regclass AND doc.deptype = 'e'
                JOIN pg_extension e ON e.oid = doc.refobjid
                JOIN pg_depend dt
                  ON dt.classid = 'pg_type'::regclass AND dt.objid = t.oid
                 AND dt.refclassid = 'pg_extension'::regclass AND dt.deptype = 'e'
                WHERE e.extname = 'postgis'
                  AND am.amname = 'gist'
                  -- Il renderer emette `USING GIST (campo)` senza nominare
                  -- un'opclass, quindi dipende da quella predefinita e visibile.
                  AND oc.opcdefault
                  AND pg_opclass_is_visible(oc.oid)
                  AND pg_type_is_visible(t.oid)
            )
            SELECT
                EXISTS(SELECT 1 FROM pg_type t
                       JOIN pg_depend d
                         ON d.classid = 'pg_type'::regclass AND d.objid = t.oid
                        AND d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e'
                       JOIN pg_extension e ON e.oid = d.refobjid
                       WHERE e.extname = 'postgis' AND t.typname = 'geometry'
                         AND pg_type_is_visible(t.oid)),
                EXISTS(SELECT 1 FROM pg_type t
                       JOIN pg_depend d
                         ON d.classid = 'pg_type'::regclass AND d.objid = t.oid
                        AND d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e'
                       JOIN pg_extension e ON e.oid = d.refobjid
                       WHERE e.extname = 'postgis' AND t.typname = 'geography'
                         AND pg_type_is_visible(t.oid)),
                -- Un'opclass GiST predefinita **per ciascuna semantica**:
                -- `create_spatial_indexes` costruisce un indice su ogni colonna
                -- spatial, incluse le `geography`, e una sola prova sul tipo
                -- `geometry` avrebbe promesso l'indice anche dove non c'e.
                EXISTS(SELECT 1 FROM gist_default WHERE typname = 'geometry'),
                EXISTS(SELECT 1 FROM gist_default WHERE typname = 'geography')
            ",
            &[],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    Ok((shapes.get(0), shapes.get(1), shapes.get(2), shapes.get(3)))
}

/// Cosa `PostGIS` mette davvero a disposizione, chiesto a `PostGIS`.
///
/// La presenza dell'estensione non basta: ogni capability deriva dagli
/// oggetti e dagli overload osservati nel catalogo del server.
///
/// Le funzioni si intersecano con il catalogo versionato
/// (`catalog/spatial-functions.v1.json`), che per ciascun id porta il nome
/// `PostGIS` e gli argomenti: si pubblica cio che esiste **con un overload
/// invocabile a quell'arita**, nell'ordine canonico di `SpatialFunction::ALL`.
///
/// Ogni oggetto e cercato per **appartenenza all'estensione**, non per nome:
/// `pg_depend` viene vincolato sia sulla classe dell'oggetto sia su quella del
/// referente. Senza quei vincoli un OID di un'altra catalog table poteva
/// coincidere, e un omonimo definito fuori da `PostGIS` sarebbe bastato ad
/// aprire una capability.
async fn probe_spatial(client: &Client) -> Result<SpatialCapabilities> {
    let overloads = postgis_overloads(client).await?;
    let callable = |name: &str, geometry_positions: &[usize], arity: usize, semantics: &str| {
        overloads.get(name).is_some_and(|forms| {
            forms
                .iter()
                .any(|form| form.accepts(geometry_positions, arity, semantics))
        })
    };
    let (geometry, geography, gist_geometry, gist_geography) = postgis_shapes(client).await?;

    // Le semantiche che il documento sta per pubblicare. La lista di funzioni
    // e **una sola** per entrambe, e il contratto lo dice esplicitamente:
    // «sottoinsieme garantito per ogni semantica spatial pubblicizzata... il
    // contratto non consente di attribuire una funzione soltanto a geometry o
    // soltanto a geography».
    //
    // Pubblicare l'unione violava quella regola in silenzio: su `PostGIS` 3.4
    // sono 65 le funzioni invocabili su `geometry` in **tutte** le forme che il
    // core accetta, e 11 quelle invocabili su entrambe le semantiche. Un
    // documento con geometry e geography ne prometteva 72.
    let advertised: Vec<&str> = [("geometry", geometry), ("geography", geography)]
        .into_iter()
        .filter_map(|(name, enabled)| enabled.then_some(name))
        .collect();

    // Il trasporto che il renderer usa per **formare** una chiamata spatial.
    // Un predicato binario lega la geometria di confronto con
    // `ST_GeomFromEWKB`, e una funzione che restituisce geometria la riporta
    // con `ST_AsEWKB`: senza entrambe, la funzione non è componibile anche se
    // esiste.
    let read_wkb = geometry && callable("st_asewkb", &[0], 1, "geometry");
    let write_wkb = geometry && callable("st_geomfromewkb", &[], 1, "geometry");

    let catalog = plenora_database_core::spatial_catalog::spatial_function_catalog()?;
    // La stessa domanda viene posta una semantica per volta; un'intersezione
    // anticipata perderebbe le funzioni invocabili su una sola semantica.
    let callable_on = |wanted: &str| -> Vec<SpatialFunction> {
        SpatialFunction::ALL
            .iter()
            .copied()
            .filter(|function| {
                if advertised.is_empty() {
                    return false;
                }
                let Some(id) = wire_name(*function) else {
                    return false;
                };
                let Some(spec) = catalog.functions.iter().find(|spec| spec.id == id) else {
                    return false;
                };
                let name = spec.postgres.to_lowercase();
                // Le posizioni geometriche vengono da `takes_geometry_at`, cioe
                // dallo **stesso predicato che usa il renderer**, non dal record
                // canonico del catalogo: i due possono divergere — `Collect` ha una
                // seconda posizione geometrica nel renderer e non nel catalogo — e
                // una capability provata su posizioni diverse da quelle che il SQL
                // occupera non prova la chiamata che verra emessa.
                //
                // Vanno pero ritagliate sull'arita che si sta sondando: una lista
                // sola per tutte le arita chiedeva a `ST_Collect` unaria una
                // geometria in posizione 1, che quell'overload non ha, e nessun
                // `PostGIS` avrebbe potuto soddisfare la richiesta.
                let geometry_positions = |count: usize| -> Vec<usize> {
                    (0..count)
                        .filter(|position| function.takes_geometry_at(*position))
                        .collect()
                };
                // **Ogni** arita che il core accetta, non solo quella canonica del
                // catalogo. Una capability e indivisibile: se il core sa comporre
                // `ST_Intersection` con tre argomenti e `PostGIS` la offre a tre
                // solo per `geometry`, pubblicarla mentre si dichiara anche
                // `geography` autorizza una chiamata che il server non ha.
                let accepted: Vec<usize> = (1..=MAX_SPATIAL_ARGUMENTS)
                    .filter(|count| function.accepts_argument_count(*count))
                    .collect();
                // Qualunque posizione geometrica puo ricevere un
                // `QueryExpression::Parameter`, che il renderer lega con
                // `ST_GeomFromEWKB` — **anche la prima**. Legare la richiesta al
                // solo caso "piu di una posizione" lasciava pubblicata una funzione
                // unaria come `ST_SRID` mentre `Spatial(Srid, [Parameter])` non era
                // formabile. Senza un piano concreto da ispezionare, la risposta
                // conservativa e richiedere il trasporto per ogni funzione che
                // accetta una geometria da qualche parte.
                let takes_a_geometry = accepted
                    .iter()
                    .any(|count| !geometry_positions(*count).is_empty());
                if takes_a_geometry && !write_wkb {
                    return false;
                }
                if spec.returns == "geometry" && !read_wkb {
                    return false;
                }
                // Una funzione senza **nessuna** posizione geometrica non e
                // sondabile per semantica: `Overload::accepts` confronta i tipi
                // degli argomenti nelle posizioni geometriche, e con la lista vuota
                // il confronto e vacuo — la stessa funzione risulta invocabile su
                // `geometry` e su `geography` senza che nulla lo abbia provato.
                //
                // Sono `ST_AsMVT` e `ST_AsGeobuf`, che prendono una riga intera:
                // `PostGIS` richiede che quella riga contenga una colonna
                // `geometry`, e nessun overload dimostra il caso `geography`. Il
                // contratto v2 pubblica **una sola** lista di funzioni per tutte le
                // semantiche dichiarate, quindi pubblicarle accanto a `geography`
                // sarebbe una promessa senza prova: restano solo dove la sola
                // semantica dichiarata e quella per cui esiste la prova.
                // `ST_AsMVT` e `ST_AsGeobuf` restano quindi limitate a geometry.
                if !takes_a_geometry && wanted != "geometry" {
                    return false;
                }
                !accepted.is_empty()
                    && accepted
                        .iter()
                        .all(|count| callable(&name, &geometry_positions(*count), *count, wanted))
            })
            .collect()
    };

    // Una lista **per semantica**, completa. L'intersezione la calcola il core
    // da queste, invece di essere una terza cosa scritta a mano: su `PostGIS`
    // la differenza fra le due e larga — sessantacinque funzioni invocabili su
    // `geometry` contro undici su entrambe — e per anni il contratto ha
    // pubblicato solo le undici, dicendo il vero e dicendo molto meno del vero.
    let functions_by_semantics: BTreeMap<SpatialSemantics, Vec<SpatialFunction>> = advertised
        .iter()
        .map(|semantics| {
            let key = match *semantics {
                "geography" => SpatialSemantics::Geography,
                _ => SpatialSemantics::Geometry,
            };
            (key, callable_on(semantics))
        })
        .collect();
    let functions =
        plenora_database_core::capabilities::intersect_spatial_functions(&functions_by_semantics);

    Ok(SpatialCapabilities {
        // Il CRS lo sa il catalogo. `geometry_columns` porta l'SRID di ogni
        // colonna, e `AddGeometryColumn` lo vincola: una dichiarazione del
        // chiamante sarebbe una seconda fonte per lo stesso fatto, e due fonti
        // per un fatto solo sono una fonte di troppo.
        requires_declared_crs: false,
        // Le funzioni **davvero emesse** dal provider, non quelle equivalenti:
        // il renderer scrive `ST_GeomFromEWKB` (vedi `crate::spatial`) e la
        // lettura chiede `ST_AsEWKB`. Sondare `ST_AsBinary`/`ST_GeomFromWKB`
        // provava l'esistenza di due funzioni che questo codice non usa mai, e
        // avrebbe lasciato la capability aperta con le vere assenti.
        read_wkb,
        write_wkb,
        geometry,
        geography,
        // Un indice per **ogni** semantica pubblicizzata:
        // `create_spatial_indexes` ne costruisce uno su ogni colonna spatial,
        // incluse le `geography`, e una sola prova su `geometry` avrebbe
        // promesso l'indice anche dove l'opclass non c'e.
        // `all()` su una lista vuota e **vero**: senza il controllo esplicito
        // un target senza semantiche spatial pubblicava `spatial_index = true`
        // con `geometry` e `geography` a false, cioe un documento che
        // `ProviderCapabilities::validate` rifiuta.
        spatial_index: !advertised.is_empty()
            && advertised.iter().all(|semantics| {
                if *semantics == "geometry" {
                    gist_geometry
                } else {
                    gist_geography
                }
            }),
        // Una colonna `geometry` senza type modifier accetta tipi diversi: e
        // una proprieta del tipo, quindi vale esattamente quando il tipo c'e.
        mixed_geometry_types: geometry,
        // I profili dimensionali sono provati dalle funzioni che il server
        // offre per **produrre** ciascuna forma: `ST_Force3D` per la Z,
        // `ST_Force3DM` per la M, `ST_Force4D` per entrambe. `Xy` e la forma
        // base di qualunque geometria e non ha una funzione dedicata.
        //
        // Quelle funzioni hanno overload su `geometry` e non su `geography`:
        // la prova copre solo la semantica su cui la sonda le cerca.
        //
        // La lista resta una sola perche il contratto ne prevede una sola:
        // `spatial.dimensions` non e articolata per semantica, quindi non c'e
        // modo di dire "XYZM su geometry, XY su geography". Intersecare su
        // `advertised` — cioe chiudere Z e M appena il target pubblica
        // `geography`, che su un PostGIS normale e sempre — pubblicherebbe un
        // documento **meno** vero di questo: PostGIS costruisce geography con
        // Z e M. La risposta onesta non e cambiare il valore, e dire cosa la
        // sonda copre: la costruibilita delle forme lato `geometry`. Una
        // dimensionalita per semantica e materia di `contracts/v3`.
        dimensions: dimensional_profiles(advertised.is_empty(), &callable),
        functions,
        functions_by_semantics,
    })
}

/// Numero massimo di argomenti che una forma spatial del core puo avere.
///
/// Il margine sopra le forme correnti evita che una funzione più larga passi
/// inosservata restringendo
/// in silenzio cio che viene verificato.
const MAX_SPATIAL_ARGUMENTS: usize = 8;

/// I profili dimensionali che il server sa costruire.
///
/// Non e una deduzione dall'esistenza del tipo: ogni profilo oltre `Xy` e
/// legato alla funzione `PostGIS` che lo produce, e quelle funzioni vengono
/// dalla stessa tabella di overload usata per le capability spatial. Un
/// `PostGIS` compilato senza una di esse smette di dichiarare quel profilo
/// invece di prometterlo.
fn dimensional_profiles(
    without_semantics: bool,
    callable: &impl Fn(&str, &[usize], usize, &str) -> bool,
) -> Vec<Dimensions> {
    if without_semantics {
        return Vec::new();
    }
    let mut profiles = vec![Dimensions::Xy];
    for (name, profile) in [
        ("st_force3d", Dimensions::Xyz),
        ("st_force3dm", Dimensions::Xym),
        ("st_force4d", Dimensions::Xyzm),
    ] {
        if callable(name, &[0], 1, "geometry") {
            profiles.push(profile);
        }
    }
    profiles
}

/// Il nome wire di una funzione spatial, cioe l'id con cui compare nel
/// catalogo e nel contratto.
fn wire_name(function: SpatialFunction) -> Option<String> {
    serde_json::to_value(function)
        .ok()?
        .as_str()
        .map(std::borrow::ToOwned::to_owned)
}
