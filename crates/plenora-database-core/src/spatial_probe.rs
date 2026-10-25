//! Lo SRID di una sonda spatial e di un valore spatial, prima dell'esecuzione.
//!
//! Una **sonda** e la geometria che il chiamante lega a un predicato o a un
//! operatore spatial per confrontarla con una colonna: un `ParameterValue`
//! `Wkb` o `Bytes` in posizione geometrica. Il suo sistema di riferimento non
//! si indovina: un WKB senza SRID arriva al server con SRID 0, e `PostGIS` lo
//! confronta con la colonna senza errore negli operatori di bounding box
//! (`&&`, `~`, `@`), lo converte in 4326 per cast implicito verso
//! `geography`, e anche nei predicati `ST_*` un indice `GiST` che non trova
//! candidati restituisce zero righe prima che il controllo degli SRID misti
//! parta (misurato su `PostGIS` 3: `ST_Intersects` fra una colonna 4326
//! indicizzata e una sonda 3857 lontana rende 0 righe, non un errore).
//!
//! Qui vivono le due regole comuni ai provider:
//!
//! - [`probe_srid`]: lo SRID esplicito della sonda, da `ParameterValue::Wkb`
//!   (`srid`) o dall'EWKB stesso; se ci sono entrambi devono coincidere. Una
//!   sonda senza SRID, o con SRID 0, si rifiuta con `crs`;
//! - [`check_spatial_value`]: un valore che porta uno SRID proprio (EWKB, o il
//!   `srid` di `ParameterValue::Wkb`) deve avere quello dichiarato dal
//!   `SpatialValue` che lo costruisce. Il costruttore `PostGIS`
//!   `ST_SetSRID(ST_GeomFromEWKB(..), srid)` lo sovrascriverebbe in silenzio.
//!
//! E qui vive **la regola** ([`admit_probe`]): una sonda o un valore spatial
//! e eseguibile soltanto se il suo SRID e noto e provato uguale a quello
//! della colonna prima dell'esecuzione, dal provider sul catalogo o da una
//! guardia del server misurata live ([`server_guard`]); altrimenti si
//! rifiuta compilando. Il compilatore portable e il renderer relazionale la
//! chiedono per ogni sonda. Fino al 25/10 ogni dialetto decideva da se, e
//! quattro riletture indipendenti di fila ne hanno trovati di silenziosi:
//! SQL Server rende `NULL` con SRID diversi (righe escluse, `UPDATE` che non
//! tocca niente con successo), `MariaDB` non confronta gli SRID e accetta in
//! scrittura un valore in un altro sistema, `PostGIS` non li confronta negli
//! operatori di bounding box e verso `geography` per cast implicito.
//!
//! Il confronto con lo SRID della colonna, dove c'e un catalogo, resta del
//! provider, che sa leggerlo; [`term_frame`] risolve senza catalogo la
//! colonna con cui una sonda si confronta, ed e la stessa per il provider
//! `PostgreSQL` e per il renderer.

use crate::ewkb::inspect_ewkb_detailed;
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::relational::{
    ColumnRef, CommonTableExpression, QueryExpression, QueryOperation, QuerySource,
};
use crate::spatial_catalog::CrsRule;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeSet;

/// Motivo stabile del rifiuto di una sonda senza SRID.
pub const PROBE_WITHOUT_SRID: &str =
    "sonda spatial senza SRID esplicito: dichiararlo nel valore wkb o nell'EWKB";

/// Motivo stabile del rifiuto di una sonda in un altro sistema di riferimento.
pub const PROBE_SRID_MISMATCH: &str =
    "sonda spatial con SRID diverso da quello del termine con cui si confronta";

/// Motivo stabile del rifiuto di una sonda il cui termine non ha SRID noto.
pub const PROBE_PEER_WITHOUT_SRID: &str =
    "termine confrontato con la sonda spatial senza SRID determinabile prima dell'esecuzione";

/// Motivo stabile del rifiuto di una sonda con due SRID diversi.
pub const PROBE_EMBEDDED_MISMATCH: &str =
    "sonda spatial con SRID dichiarato diverso da quello incorporato nell'EWKB";

/// Motivo stabile del rifiuto di un valore spatial con SRID divergente.
pub const SPATIAL_VALUE_SRID_MISMATCH: &str =
    "valore spatial con SRID incorporato diverso da quello dichiarato";

fn crs(provider: ProviderKind, message: &str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Crs,
        ErrorPhase::Prepare,
        Some(provider),
        message,
    )
}

/// I byte e lo SRID dichiarato di un valore geometrico, o `None` se il valore
/// non e una geometria (il provider lo rifiuta con le proprie regole).
fn geometry_value(value: &ParameterValue) -> Option<(&[u8], Option<u32>)> {
    match value {
        ParameterValue::Wkb { bytes, srid, .. } => Some((bytes, *srid)),
        ParameterValue::Bytes(bytes) => Some((bytes, None)),
        _ => None,
    }
}

/// Lo SRID incorporato nella radice dell'EWKB, o `None` per un WKB puro.
///
/// # Errors
///
/// `DataMapping` per un valore non decodificabile, `ResourceLimit` oltre la
/// profondita ammessa.
fn embedded_srid(bytes: &[u8], max_depth: u64) -> Result<Option<u32>> {
    Ok(inspect_ewkb_detailed(bytes, u64::MAX, max_depth)?.root.srid)
}

/// Lo SRID esplicito di una sonda spatial.
///
/// Restituisce `Ok(None)` se il valore non e geometrico: non e una sonda, e il
/// provider lo tratta come ogni altro bind fuori posto.
///
/// # Errors
///
/// `crs` se la sonda non dichiara uno SRID, dichiara 0, o porta nell'EWKB uno
/// SRID diverso da quello di `ParameterValue::Wkb`; gli errori di decodifica
/// dell'EWKB.
pub fn probe_srid(
    provider: ProviderKind,
    value: &ParameterValue,
    max_depth: u64,
) -> Result<Option<u32>> {
    let Some((bytes, declared)) = geometry_value(value) else {
        return Ok(None);
    };
    let embedded = embedded_srid(bytes, max_depth)?;
    let srid = match (declared, embedded) {
        (Some(declared), Some(embedded)) if declared != embedded => {
            return Err(crs(provider, PROBE_EMBEDDED_MISMATCH));
        }
        (Some(srid), _) | (None, Some(srid)) => srid,
        (None, None) => return Err(crs(provider, PROBE_WITHOUT_SRID)),
    };
    if srid == 0 {
        return Err(crs(provider, PROBE_WITHOUT_SRID));
    }
    Ok(Some(srid))
}

/// I byte che il provider lega per la sonda: l'EWKB con lo SRID esplicito
/// nella radice. Un valore che lo porta gia resta com'e.
///
/// # Errors
///
/// Quelli di [`probe_srid`] e di [`crate::ewkb::with_root_srid`].
pub fn stamped_probe(
    provider: ProviderKind,
    value: &ParameterValue,
    max_depth: u64,
) -> Result<Option<(u32, Vec<u8>)>> {
    let Some(srid) = probe_srid(provider, value, max_depth)? else {
        return Ok(None);
    };
    let Some((bytes, _)) = geometry_value(value) else {
        return Ok(None);
    };
    let bytes = if embedded_srid(bytes, max_depth)?.is_some() {
        bytes.to_vec()
    } else {
        crate::ewkb::with_root_srid(bytes, srid, max_depth)?
    };
    Ok(Some((srid, bytes)))
}

/// Un valore costruito da un `SpatialValue` con SRID `declared`: se porta uno
/// SRID proprio, deve essere quello.
///
/// Un valore senza SRID (WKB puro, `srid: None`) prende quello dichiarato:
/// e la ragione d'essere del costruttore. Un valore non geometrico non si
/// giudica qui.
///
/// # Errors
///
/// `crs` se lo SRID del valore e diverso da quello dichiarato; gli errori di
/// decodifica dell'EWKB.
pub fn check_spatial_value(
    provider: ProviderKind,
    value: &ParameterValue,
    declared: u32,
    max_depth: u64,
) -> Result<()> {
    let Some((bytes, srid)) = geometry_value(value) else {
        return Ok(());
    };
    if srid.is_some_and(|srid| srid != declared) {
        return Err(crs(provider, SPATIAL_VALUE_SRID_MISMATCH));
    }
    if embedded_srid(bytes, max_depth)?.is_some_and(|srid| srid != declared) {
        return Err(crs(provider, SPATIAL_VALUE_SRID_MISMATCH));
    }
    Ok(())
}

/// Lo SRID della sonda contro quello del termine con cui si confronta.
///
/// # Errors
///
/// `crs` se il termine non ha SRID noto o ne ha un altro.
pub fn compare_frames(provider: ProviderKind, probe: u32, peer: Option<u32>) -> Result<()> {
    match peer {
        None | Some(0) => Err(crs(provider, PROBE_PEER_WITHOUT_SRID)),
        Some(peer) if peer != probe => Err(crs(provider, PROBE_SRID_MISMATCH)),
        Some(_) => Ok(()),
    }
}

/// Una geometria vuota `PostGIS` con lo SRID `srid` (SQL): il termine con cui
/// `ST_Equals` confronta due SRID senza confrontare coordinate.
fn postgis_empty(srid: &str) -> String {
    format!("ST_SetSRID('POINT EMPTY'::geometry, {srid})")
}

/// La sonda `PostGIS` di un predicato su una colonna di tabella, con lo SRID
/// dichiarato confrontato con quello della colonna nel catalogo **dal server,
/// prima di ogni riga**.
///
/// Serve dove chi compila non ha il catalogo: il compilatore portable e
/// `build_spatial_select`, che producono uno statement eseguito poi dal
/// chiamante. `geometry` e il valore legato (EWKB o WKB), `srid` lo SRID
/// dichiarato, `relation` e `column` i nomi gia quotati e legati come testo.
///
/// La forma e
/// `CASE WHEN ST_Equals(vuota(srid), vuota(typmod della colonna)) THEN
/// ST_SetSRID(ST_GeomFromEWKB(geometry), srid) END`
/// ([`postgres_column_guard`]). Il confronto non ha variabili di riga: con
/// due SRID diversi `ST_Equals` fallisce con «Operation on mixed SRID
/// geometries», che il provider pubblica `crs`; una colonna senza typmod ha
/// SRID 0 e fallisce allo stesso modo. Con SRID uguali vale la sonda.
///
/// Quando il server la valuta (misurato su `PostGIS` 3.4, 25/10): come
/// chiave di un indice `GiST` una volta, prima di ogni riga e anche su una
/// tabella vuota; in una scansione sequenziale per ogni riga il cui esito
/// dipende dal predicato. Una riga il cui esito non ne dipende — una tabella
/// vuota letta in seqscan, un `OR` il cui altro ramo e gia vero — non la
/// valuta, e passa (o resta fuori) senza errore: e lo stesso risultato che
/// darebbe qualunque sonda, perche quella riga la sonda non la guarda. Una
/// riga che la guarda non arriva al chiamante senza che la guardia abbia
/// risposto.
#[must_use]
pub fn postgres_guarded_probe(geometry: &str, srid: &str, relation: &str, column: &str) -> String {
    postgres_column_guard(
        &format!("ST_SetSRID(ST_GeomFromEWKB({geometry}), {srid})"),
        srid,
        relation,
        column,
    )
}

/// La guardia `PostGIS` attorno a un termine geometrico.
///
/// `value` e dichiarato nello SRID `srid`: il termine se lo SRID e quello del
/// typmod della colonna
/// `column` della relazione `relation`, un errore del server («mixed SRID»,
/// `crs`) altrimenti. `relation` e `column` sono espressioni SQL di tipo
/// testo: segnaposto legati, o letterali di [`postgres_text_literal`].
///
/// E l'unica guardia del server che la regola delle sonde
/// ([`admit_probe`], [`ServerGuard::PostgresColumnTypmod`]) ammette su
/// `PostgreSQL`: le funzioni `ST_*` senza guardia non lo sono, perche un
/// indice `GiST` le risolve con `&&`, che non confronta gli SRID, e una sonda
/// lontana in un altro sistema rende zero righe senza errore.
#[must_use]
pub fn postgres_column_guard(value: &str, srid: &str, relation: &str, column: &str) -> String {
    format!(
        "(CASE WHEN ST_Equals({}, {}) THEN {value} END)",
        postgis_empty(srid),
        postgis_empty(&format!(
            "COALESCE((SELECT postgis_typmod_srid(a.atttypmod) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = to_regclass({relation}) AND a.attname = {column} \
             AND a.attnum > 0 AND NOT a.attisdropped), 0)"
        )),
    )
}

/// Un letterale di testo `PostgreSQL` che vale `text` qualunque sia
/// `standard_conforming_strings`: la forma `E'...'` interpreta sempre le
/// barre rovesciate, e qui ogni barra e ogni apice si raddoppia.
///
/// Serve dove chi compila non puo aggiungere segnaposto (il renderer
/// relazionale: i bind sono nomi del chiamante) e deve nominare una relazione
/// o una colonna gia validate come identificatori.
#[must_use]
pub fn postgres_text_literal(text: &str) -> String {
    format!("E'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}

/// Il testo che `to_regclass` risolve come il nome della relazione nel piano:
/// qualificato con lo schema se il piano lo nomina, altrimenti attraverso il
/// `search_path`, come lo statement stesso.
#[must_use]
pub fn postgres_relation_text(schema: Option<&str>, object: &str) -> String {
    let object = format!("quote_ident({})", postgres_text_literal(object));
    match schema {
        Some(schema) => format!(
            "(quote_ident({}) || '.' || {object})",
            postgres_text_literal(schema)
        ),
        None => object,
    }
}

// ---------------------------------------------------------------------------
// La regola delle sonde: quando uno SRID e provato prima dell'esecuzione.
// ---------------------------------------------------------------------------

/// Chi ha confrontato lo SRID della sonda con quello della colonna prima che
/// lo statement si compilasse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeEvidence {
    /// Il provider, sul catalogo del target (o sui valori della colonna, per
    /// SQL Server che non ha SRID di colonna), prima di rendere lo statement:
    /// il path read e query di `PostgreSQL`, il path query di SQL Server, il
    /// path read di Oracle.
    Catalog,
    /// Nessuno: lo statement si compila senza il catalogo e lo esegue il
    /// chiamante. Il compilatore portable (SDK `where_spatial`, facade,
    /// mutazioni relazionali, CLI portable) e il renderer relazionale dello
    /// SDK.
    CompileOnly,
}

/// Che cosa riceve la sonda.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeUse {
    /// Una funzione che il server valuta su due geometrie: predicati
    /// (`ST_Intersects`, `SDO_RELATE`, `STIntersects`, ...) e misure
    /// (`ST_Distance`).
    Function,
    /// Un operatore o una funzione di bounding box: `&&`, `~`, `@`, `<->`,
    /// `MBRIntersects`, `STEnvelope().STIntersects`.
    BoundingBox,
    /// Un valore scritto in una colonna: `INSERT ... VALUES`, `UPDATE ...
    /// SET`.
    WriteValue,
    /// L'operando di un operatore che non e spatial (`=`, `<>`, `<`, `IN`,
    /// `BETWEEN`, `LIKE`, `IS NULL`, `IN (subquery)`), a qualunque
    /// profondita e attraverso qualunque funzione: nessuno confronta lo SRID
    /// della sonda con quello della colonna (un `PostGIS` `=` fra sistemi
    /// diversi e falso senza errore, anche come `g = ST_Centroid(sonda)`).
    /// Nessun prodotto ha una guardia, e nessun catalogo la sostituisce: si
    /// rifiuta sempre ([`SPATIAL_VALUE_OUTSIDE_SPATIAL`]).
    ScalarOperand,
}

/// Lo SRID della sonda come lo vede chi compila.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeFrame {
    /// Un bind nudo: lo SRID arriva con il valore, dopo la compilazione, e
    /// chi compila non lo conosce.
    Unbound,
    /// Dichiarato nel piano (`SpatialReference`, `SpatialValue`).
    Declared(u32),
    /// Un valore senza SRID, o con SRID 0.
    Missing,
}

/// Una guardia del server, misurata live.
///
/// Rifiuta una sonda in un altro sistema prima che una riga che dipende dal
/// confronto sia restituita o toccata; la misura e
/// `scripts/sonde_srid_server.py`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerGuard {
    /// `PostgreSQL`: la forma [`postgres_column_guard`] contro il typmod della
    /// colonna confrontata. Chi compila deve sapere la tabella di base della
    /// colonna; se non la sa, la sonda si rifiuta.
    PostgresColumnTypmod,
    /// `PostgreSQL`, scrittura: una colonna con typmod rifiuta un valore con
    /// un altro SRID («Geometry SRID does not match column SRID», 22023).
    PostgresTypmodWrite,
    /// `MySQL` 8.0 e 9.7: 3033 («given two geometries of different srids»)
    /// nelle funzioni e negli `MBR*`, 3643 («The SRID of the geometry does not
    /// match the SRID of the column») sul confronto con una colonna vincolata
    /// e indicizzata, anche su una tabella vuota, e in scrittura.
    MysqlSrid,
}

/// Perche una sonda e eseguibile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Il provider ha gia confrontato gli SRID: la forma semplice.
    Catalog,
    /// La guardia del server: il compilatore rende la sua forma.
    Server(ServerGuard),
    /// Un valore scritto su SQL Server, che non ha SRID di colonna: lo SRID
    /// viaggia con il valore e non c'e un vincolo da violare. Il limite e
    /// dichiarato su [`admit_probe`].
    ValueCarriesFrame,
}

/// Le guardie del server provate live, per prodotto e uso: l'unica tabella
/// della regola.
///
/// Fuori da qui nessuna guardia e ammessa: `SQL Server` rende `NULL` con
/// SRID diversi (il predicato `= 1` esclude la riga, anche sotto `NOT` e con
/// `STEnvelope`, anche su `geography` 4326 contro 4269), `MariaDB` non
/// confronta gli SRID (`ST_Intersects`, `MBRIntersects`, `ST_Within` fra una
/// sonda 3857 e una colonna `REF_SYSTEM_ID=4326` rendono righe) e accetta un
/// valore 3857 in quella colonna, gli operatori `PostGIS` senza guardia non
/// confrontano gli SRID; Oracle e Db2 non sono stati misurati.
#[must_use]
pub const fn server_guard(provider: ProviderKind, probe_use: ProbeUse) -> Option<ServerGuard> {
    match (provider, probe_use) {
        (_, ProbeUse::ScalarOperand) => None,
        (ProviderKind::Postgres, ProbeUse::Function | ProbeUse::BoundingBox) => {
            Some(ServerGuard::PostgresColumnTypmod)
        }
        (ProviderKind::Postgres, ProbeUse::WriteValue) => Some(ServerGuard::PostgresTypmodWrite),
        (ProviderKind::Mysql, _) => Some(ServerGuard::MysqlSrid),
        _ => None,
    }
}

/// Motivo stabile del rifiuto di un bind nudo in posizione geometrica dove
/// nessuno confronta lo SRID prima dell'esecuzione.
pub const PROBE_UNBOUND_UNPROVEN: &str =
    "sonda spatial senza SRID noto prima dell'esecuzione: usare un valore spatial con SRID dichiarato";

/// Motivo stabile del rifiuto di una sonda il cui SRID nessuno puo
/// confrontare con quello della colonna prima dell'esecuzione.
pub const PROBE_SRID_UNPROVEN: &str =
    "confronto spatial con una sonda: il provider non verifica lo SRID della colonna prima dell'esecuzione";

/// Motivo stabile del rifiuto di un valore spatial scritto senza che lo SRID
/// della colonna sia verificato.
pub const VALUE_SRID_UNPROVEN: &str =
    "valore spatial scritto senza verifica dello SRID della colonna: il server non lo vincola";

/// Motivo stabile del rifiuto di un valore spatial confrontato fuori da una
/// funzione o da un operatore spatial.
pub const SPATIAL_VALUE_OUTSIDE_SPATIAL: &str =
    "valore spatial confrontato fuori da una funzione o da un operatore spatial";

/// La regola delle sonde.
///
/// Una sonda spatial (o un valore spatial scritto) e eseguibile soltanto se
/// il suo SRID e noto e provato uguale a quello della
/// colonna **prima dell'esecuzione**: dal provider sul catalogo
/// ([`ProbeEvidence::Catalog`]), o da una guardia del server che rifiuta prima
/// di ogni riga, misurata live per il prodotto ([`server_guard`]). Altrimenti
/// si rifiuta compilando: `crs` per una sonda senza SRID, `unsupported` dove
/// la prova manca.
///
/// E l'unica decisione: il compilatore portable e il renderer relazionale la
/// chiedono qui per ogni sonda, e un provider che la salta non ha un'altra
/// strada.
///
/// # Limite dichiarato
///
/// - **Regola.** Su SQL Server un valore scritto non si confronta con uno
///   SRID di colonna ([`Admission::ValueCarriesFrame`]).
/// - **Ambito.** `INSERT`/`UPDATE` del compilatore portable verso SQL Server
///   (mutazioni relazionali dello SDK, facade).
/// - **Hazard.** Una colonna che il chiamante usa in un solo sistema puo
///   ricevere un valore in un altro: le letture del provider la rifiutano poi
///   (`SRID misti`), ma un `SELECT` compilato senza provider non lo vede.
/// - **Rientro.** Un vincolo `CHECK (col.STSrid = n)` letto dal catalogo, o
///   la lettura degli SRID della colonna prima della scrittura come fa il
///   path query.
///
/// # Errors
///
/// `crs` ([`PROBE_WITHOUT_SRID`]) per [`ProbeFrame::Missing`]; `unsupported`
/// se la prova manca: un bind nudo compilato senza catalogo, o un prodotto
/// senza guardia misurata.
pub fn admit_probe(
    provider: ProviderKind,
    evidence: ProbeEvidence,
    probe_use: ProbeUse,
    frame: ProbeFrame,
) -> Result<Admission> {
    // Un operatore che non confronta gli SRID non li confronta con nessuna
    // prova: ne il catalogo ne una guardia del server stanno fra la sonda e
    // il risultato.
    if probe_use == ProbeUse::ScalarOperand {
        return Err(DatabaseError::unsupported(
            provider,
            ErrorPhase::Prepare,
            SPATIAL_VALUE_OUTSIDE_SPATIAL,
        ));
    }
    if frame == ProbeFrame::Missing {
        return Err(crs(provider, PROBE_WITHOUT_SRID));
    }
    if evidence == ProbeEvidence::Catalog {
        return Ok(Admission::Catalog);
    }
    let refused = |message: &str| {
        Err(DatabaseError::unsupported(
            provider,
            ErrorPhase::Prepare,
            message,
        ))
    };
    if frame == ProbeFrame::Unbound {
        return refused(PROBE_UNBOUND_UNPROVEN);
    }
    match (server_guard(provider, probe_use), provider, probe_use) {
        (Some(guard), _, _) => Ok(Admission::Server(guard)),
        (None, ProviderKind::Sqlserver, ProbeUse::WriteValue) => Ok(Admission::ValueCarriesFrame),
        (None, _, ProbeUse::WriteValue) => refused(VALUE_SRID_UNPROVEN),
        (None, _, _) => refused(PROBE_SRID_UNPROVEN),
    }
}

/// Il costruttore `PostGIS` di un `SpatialValue`, che non sovrascrive.
///
/// Lo SRID dichiarato a un valore che non ne porta, lo stesso valore se porta
/// proprio quello, un rifiuto del server («mixed SRID», pubblicato `crs`) se
/// ne porta un altro.
///
/// Era `ST_SetSRID(ST_GeomFromEWKB(value), srid)`, che sovrascriveva in
/// silenzio uno SRID incorporato diverso: un EWKB 3857 dichiarato 4326 si
/// memorizzava `SRID=4326` con le coordinate in metri. Il controllo prima
/// dell'esecuzione resta dove i valori sono noti
/// ([`check_spatial_value`]); questa forma vale anche per chi lega i valori
/// dopo aver compilato, come lo SDK.
///
/// `CASE` valuta i rami nell'ordine, anche quando il server li semplifica
/// prima dell'esecuzione, quindi `ST_Equals` non vede mai uno SRID 0.
#[must_use]
pub fn postgres_spatial_value(value: &str, srid: u32) -> String {
    let geometry = format!("ST_GeomFromEWKB({value})");
    format!(
        "(CASE WHEN ST_SRID({geometry}) = 0 THEN ST_SetSRID({geometry}, {srid}) \
         WHEN ST_Equals({}, {}) THEN {geometry} END)",
        postgis_empty(&format!("ST_SRID({geometry})")),
        postgis_empty(&srid.to_string()),
    )
}

// ---------------------------------------------------------------------------
// Il termine con cui una sonda si confronta, nel piano relazionale.
// ---------------------------------------------------------------------------

/// Una colonna di una relazione di base nominata dal piano.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseColumn {
    pub schema: Option<String>,
    pub object: String,
    pub column: String,
}

/// Le relazioni con cui un'operazione risolve le proprie colonne, senza
/// catalogo: le sorgenti nominate (con alias), gli alias delle sorgenti
/// derivate e le CTE visibili.
#[derive(Debug, Clone, Default)]
pub struct RelationScope {
    /// Nome con cui la query la nomina (alias, o nome dell'oggetto), schema,
    /// oggetto.
    sources: Vec<(String, Option<String>, String)>,
    derived: BTreeSet<String>,
    common_tables: BTreeSet<String>,
}

impl RelationScope {
    /// Lo scope di `operation`, con le CTE `common_tables` visibili (quelle
    /// dell'operazione e delle operazioni che la contengono).
    #[must_use]
    pub fn of(operation: &QueryOperation, common_tables: &BTreeSet<String>) -> Self {
        let sources = operation
            .source
            .iter()
            .chain(
                operation
                    .joins
                    .iter()
                    .filter_map(|join| join.source.as_ref()),
            )
            .map(|source| {
                (
                    source
                        .alias
                        .clone()
                        .unwrap_or_else(|| source.object.object.clone()),
                    source.object.schema.clone(),
                    source.object.object.clone(),
                )
            })
            .collect();
        let derived = operation
            .derived_source
            .iter()
            .chain(
                operation
                    .joins
                    .iter()
                    .filter_map(|join| join.derived_source.as_ref()),
            )
            .map(|source| source.alias.clone())
            .collect();
        let mut common_tables = common_tables.clone();
        common_tables.extend(
            operation
                .common_table_expressions
                .iter()
                .map(|cte| cte.name.clone()),
        );
        Self {
            sources,
            derived,
            common_tables,
        }
    }

    /// Lo scope di un predicato isolato su una sola relazione di base, come
    /// lo rende `render_filter`: ogni colonna, qualificata con `relation` o
    /// senza qualificazione, e una colonna di quella relazione.
    #[must_use]
    pub fn single(relation: &str) -> Self {
        Self {
            sources: vec![(relation.to_owned(), None, relation.to_owned())],
            derived: BTreeSet::new(),
            common_tables: BTreeSet::new(),
        }
    }

    /// Le CTE visibili da qui, per lo scope di una query annidata.
    #[must_use]
    pub const fn common_tables(&self) -> &BTreeSet<String> {
        &self.common_tables
    }

    /// Questo blocco nomina una relazione `relation` nella propria FROM:
    /// una sorgente (tabella o CTE) o una sorgente derivata.
    #[must_use]
    pub fn names(&self, relation: &str) -> bool {
        self.derived.contains(relation) || self.sources.iter().any(|(name, _, _)| name == relation)
    }

    /// La FROM di questo blocco contiene una relazione che non e una tabella
    /// di base: una sorgente derivata o una CTE. Una colonna senza
    /// qualificazione di un blocco annidato puo allora essere sua, e nessuno
    /// lo sa senza catalogo.
    #[must_use]
    pub fn has_non_base_relation(&self) -> bool {
        !self.derived.is_empty()
            || self
                .sources
                .iter()
                .any(|(_, schema, object)| schema.is_none() && self.common_tables.contains(object))
    }

    /// La colonna di base che `column` nomina, se si risolve senza
    /// ambiguita: una sola sorgente nominata con quel nome (o, senza
    /// qualificazione, una sola sorgente e nessuna derivata), e non una CTE.
    /// Una sorgente derivata, una CTE, un riferimento correlato a una query
    /// esterna non si risolvono: `None`.
    #[must_use]
    pub fn base_column(&self, column: &ColumnRef) -> Option<BaseColumn> {
        let named = match &column.relation {
            Some(relation) => {
                if self.derived.contains(relation) {
                    return None;
                }
                self.sources
                    .iter()
                    .filter(|(name, _, _)| name == relation)
                    .collect::<Vec<_>>()
            }
            None if self.derived.is_empty() => self.sources.iter().collect(),
            None => return None,
        };
        let [(_, schema, object)] = named.as_slice() else {
            return None;
        };
        // Un nome senza schema puo essere una CTE della query: non e la
        // tabella del catalogo con lo stesso nome.
        if schema.is_none() && self.common_tables.contains(object) {
            return None;
        }
        Some(BaseColumn {
            schema: schema.clone(),
            object: object.clone(),
            column: column.field.clone(),
        })
    }
}

/// Il frame di un termine geometrico, senza catalogo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermFrame {
    /// Il frame di una colonna di base: lo SRID e nel catalogo.
    Column(BaseColumn),
    /// Uno SRID dichiarato nel piano: un `SpatialValue`.
    Declared(u32),
    /// Un bind: lo SRID arriva con il valore.
    Parameter(String),
    /// Un termine il cui frame non si risolve senza server.
    Unresolved,
}

/// Il frame di `expression`, senza catalogo.
///
/// Una colonna di base, un valore dichiarato, un bind, o una funzione che
/// conserva il frame del suo unico argomento geometrico
/// (`SpatialFunction::crs_rule`), seguita fino al termine.
#[must_use]
pub fn term_frame(expression: &QueryExpression, scope: &RelationScope) -> TermFrame {
    let mut current = expression;
    loop {
        match current {
            QueryExpression::Column { column } => {
                return scope
                    .base_column(column)
                    .map_or(TermFrame::Unresolved, TermFrame::Column);
            }
            QueryExpression::Parameter { name } | QueryExpression::TypedParameter { name, .. } => {
                return TermFrame::Parameter(name.clone());
            }
            QueryExpression::SpatialValue { srid, .. } => return TermFrame::Declared(*srid),
            QueryExpression::Spatial {
                function,
                arguments,
            } if function.returns_geometry() && function.crs_rule() == Some(CrsRule::Preserves) => {
                let geometric = arguments
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| function.takes_geometry_at(*index))
                    .map(|(_, argument)| argument)
                    .collect::<Vec<_>>();
                let [argument] = geometric.as_slice() else {
                    return TermFrame::Unresolved;
                };
                current = argument;
            }
            _ => return TermFrame::Unresolved,
        }
    }
}

/// Un'espressione contiene una sonda: un `SpatialValue`, o un bind in una
/// posizione geometrica di un nodo spatial (anche annidato).
#[must_use]
pub fn contains_geometry_probe(expression: &QueryExpression) -> bool {
    let mut found = matches!(
        expression,
        QueryExpression::Parameter { .. }
            | QueryExpression::TypedParameter { .. }
            | QueryExpression::SpatialValue { .. }
    );
    crate::relational::walk_query_expression(expression, |node| {
        use crate::relational::{QueryWalkControl, QueryWalkNode};
        let QueryWalkNode::Expression(node) = node else {
            return QueryWalkControl::Skip;
        };
        let geometric = match node {
            QueryExpression::SpatialValue { .. } => {
                found = true;
                return QueryWalkControl::Break;
            }
            QueryExpression::Spatial {
                function,
                arguments,
            }
            | QueryExpression::SpatialWindow {
                function,
                arguments,
                ..
            } => arguments.iter().enumerate().any(|(index, argument)| {
                function.takes_geometry_at(index)
                    && matches!(
                        argument,
                        QueryExpression::Parameter { .. } | QueryExpression::TypedParameter { .. }
                    )
            }),
            QueryExpression::SpatialOperator { left, right, .. } => {
                [left, right].iter().any(|side| {
                    matches!(
                        side.as_ref(),
                        QueryExpression::Parameter { .. } | QueryExpression::TypedParameter { .. }
                    )
                })
            }
            _ => false,
        };
        if geometric {
            found = true;
            QueryWalkControl::Break
        } else {
            QueryWalkControl::Continue
        }
    });
    found
}

// ---------------------------------------------------------------------------
// La regola P: dove una sonda puo stare nel piano.
// ---------------------------------------------------------------------------

/// Motivo stabile del rifiuto di una sonda fuori da un predicato.
pub const PROBE_OUTSIDE_PREDICATE: &str =
    "sonda spatial fuori da un predicato WHERE, ON o HAVING: e ammessa soltanto come argomento di una funzione o di un operatore spatial";

/// Motivo stabile del rifiuto di una sonda confrontata con un termine che non
/// e una colonna del proprio blocco.
pub const PROBE_PEER_OUTSIDE_BLOCK: &str =
    "sonda spatial confrontata con un termine che non e una colonna di una tabella nella FROM dello stesso blocco";

/// Motivo stabile del rifiuto di un argomento geometrico che non e una
/// colonna di relazione, una funzione spatial o una sonda.
pub const GEOMETRY_ARGUMENT_UNCLASSIFIED: &str =
    "argomento geometrico di un nodo spatial che non e una colonna di una relazione nella FROM, una funzione spatial o una sonda diretta: CAST, COALESCE, CASE, subquery e altre espressioni non sono ammesse in posizione geometrica";

/// La clausola in cui sta un'espressione.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Clause {
    /// `WHERE`, `ON`, `HAVING`.
    Predicate,
    /// Projection, `ORDER BY`, `GROUP BY`, `DISTINCT ON`, finestre.
    Elsewhere,
}

enum ProbeItem<'a> {
    /// Un blocco, con le CTE visibili da li e il blocco a cui un suo
    /// riferimento correlato puo appartenere.
    Operation(
        &'a QueryOperation,
        Vec<&'a CommonTableExpression>,
        Option<usize>,
    ),
    /// Un'espressione nella clausola indicata, nel blocco `usize`.
    Expression(&'a QueryExpression, Clause, usize),
}

/// Un blocco visitato: lo scope della sua FROM, il blocco che lo contiene
/// per i riferimenti correlati (`None` per una CTE o una sorgente derivata
/// non `LATERAL`, che non vedono la FROM esterna), l'operazione e le CTE
/// visibili, per seguire una colonna di una sorgente derivata o di una CTE
/// fino all'espressione che la definisce.
struct Block<'a> {
    scope: RelationScope,
    parent: Option<usize>,
    operation: Option<&'a QueryOperation>,
    common_tables: Vec<&'a CommonTableExpression>,
}

/// Oltre questa profondita di sorgenti derivate e CTE annidate una colonna
/// non si classifica: il rifiuto e esplicito, non una visita senza fondo.
const MAX_RELATION_DEPTH: usize = 64;

/// Che cosa e un argomento in posizione geometrica di un nodo spatial.
///
/// Il mondo e chiuso: un argomento e una di queste classi **in positivo**, e
/// ogni altra espressione e [`GeometryClass::Unclassified`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeometryClass {
    /// Una sonda: un `SpatialValue` o un bind direttamente in quella
    /// posizione, anche sotto funzioni che ne conservano il frame
    /// (`ST_Centroid(sonda)`). La giudica [`admit_probe`].
    Probe,
    /// Una colonna di una tabella di base nominata dalla FROM del blocco, o
    /// di un blocco esterno per un riferimento qualificato correlato; o una
    /// colonna di una sorgente derivata o di una CTE la cui espressione di
    /// projection, in ogni ramo, e a sua volta una di queste colonne o una
    /// funzione spatial su di esse ([`relation_output_is_column`]).
    Column,
    /// Una funzione spatial che rende una geometria: e a sua volta un nodo
    /// spatial, e i suoi argomenti geometrici si classificano allo stesso
    /// modo.
    Produced,
    /// Tutto il resto: `CAST`, `COALESCE`, `CASE`, subquery, aritmetica,
    /// funzioni scalari, colonne di sorgenti derivate o di CTE definite da
    /// una di queste espressioni (`(SELECT CAST(:p AS BINARY) AS s) d`),
    /// colonne senza qualificazione che un blocco esterno potrebbe fornire.
    Unclassified,
}

const fn is_bind(expression: &QueryExpression) -> bool {
    matches!(
        expression,
        QueryExpression::Parameter { .. } | QueryExpression::TypedParameter { .. }
    )
}

/// L'unico argomento geometrico di una funzione che restituisce una geometria
/// nel frame di quell'argomento (`CrsRule::Preserves`): `ST_Centroid`,
/// `ST_Buffer`. E il passo che [`term_frame`] attraversa.
fn preserved_argument(expression: &QueryExpression) -> Option<(usize, &QueryExpression)> {
    let QueryExpression::Spatial {
        function,
        arguments,
    } = expression
    else {
        return None;
    };
    if !function.returns_geometry() || function.crs_rule() != Some(CrsRule::Preserves) {
        return None;
    }
    let geometric = arguments
        .iter()
        .enumerate()
        .filter(|(index, _)| function.takes_geometry_at(*index))
        .collect::<Vec<_>>();
    match geometric.as_slice() {
        [(index, argument)] => Some((*index, *argument)),
        _ => None,
    }
}

/// Un termine che e una sonda: un `SpatialValue`, un bind, o una funzione che
/// conserva il frame di una sonda.
fn is_probe_term(expression: &QueryExpression) -> bool {
    let mut current = expression;
    loop {
        if is_bind(current) || matches!(current, QueryExpression::SpatialValue { .. }) {
            return true;
        }
        match preserved_argument(current) {
            Some((_, argument)) => current = argument,
            None => return false,
        }
    }
}

/// La colonna `column` del blocco `index` e una colonna di relazione
/// ([`GeometryClass::Column`]).
///
/// Un riferimento qualificato si risolve nel blocco piu interno che nomina
/// la relazione, come in SQL; un riferimento senza qualificazione soltanto
/// nel proprio blocco, e soltanto se nessun blocco esterno ha una sorgente
/// derivata o una CTE che potrebbe fornirlo (senza catalogo non si sa quali
/// colonne abbia la tabella del blocco). Nel blocco trovato e una colonna
/// di una tabella di base, o di una sorgente derivata o di una CTE la cui
/// projection la definisce come colonna ([`relation_output_is_column`]).
fn is_relation_column(column: &ColumnRef, blocks: &[Block<'_>], index: usize) -> bool {
    let block = match &column.relation {
        None => {
            let mut outer = blocks[index].parent;
            while let Some(block) = outer {
                if blocks[block].scope.has_non_base_relation() {
                    return false;
                }
                outer = blocks[block].parent;
            }
            index
        }
        Some(relation) => {
            let mut current = Some(index);
            loop {
                let Some(block) = current else {
                    return false;
                };
                if blocks[block].scope.names(relation) {
                    break block;
                }
                current = blocks[block].parent;
            }
        }
    };
    let block = &blocks[block];
    if block.scope.base_column(column).is_some() {
        return true;
    }
    block.operation.is_some_and(|operation| {
        relation_output_is_column(operation, &block.common_tables, column, &mut Vec::new(), 0)
    })
}

/// La colonna `column`, nominata nella FROM di `operation`, viene da una
/// sorgente derivata o da una CTE la cui projection, in ogni ramo di set
/// operation e nella stessa posizione, e una colonna di relazione o una
/// funzione spatial che rende una geometria dai soli argomenti geometrici
/// di quel tipo.
///
/// Una CTE ricorsiva che si nomina da se si ammette per induzione: ogni ramo
/// deve portare colonne in colonne, e il ramo base deve farlo senza di lei.
/// Un `SELECT *`, una colonna senza alias che non e una colonna, un
/// riferimento correlato a un blocco esterno dentro la sorgente: `false`.
fn relation_output_is_column<'a>(
    operation: &'a QueryOperation,
    common_tables: &[&'a CommonTableExpression],
    column: &ColumnRef,
    visiting: &mut Vec<String>,
    depth: usize,
) -> bool {
    if depth > MAX_RELATION_DEPTH {
        return false;
    }
    let mut visible: Vec<&CommonTableExpression> = common_tables.to_vec();
    visible.extend(operation.common_table_expressions.iter());
    let Some((query, name)) = named_relation(operation, &visible, column.relation.as_deref())
    else {
        return false;
    };
    if let Some(name) = &name {
        if visiting.contains(name) {
            return true;
        }
        visiting.push(name.clone());
    }
    let defined = output_is_column(query, &visible, &column.field, visiting, depth + 1);
    if name.is_some() {
        visiting.pop();
    }
    defined
}

/// La sorgente derivata o la CTE che `relation` nomina nella FROM di
/// `operation`, con il nome della CTE; senza qualificazione, soltanto se la
/// FROM ha una relazione sola. Una tabella di base, o un nome che non si
/// risolve: `None`.
fn named_relation<'a>(
    operation: &'a QueryOperation,
    visible: &[&'a CommonTableExpression],
    relation: Option<&str>,
) -> Option<(&'a QueryOperation, Option<String>)> {
    let derived = operation
        .derived_source
        .iter()
        .chain(
            operation
                .joins
                .iter()
                .filter_map(|join| join.derived_source.as_ref()),
        )
        .collect::<Vec<_>>();
    let sources = operation
        .source
        .iter()
        .chain(
            operation
                .joins
                .iter()
                .filter_map(|join| join.source.as_ref()),
        )
        .collect::<Vec<_>>();
    let common_table = |source: &QuerySource| {
        if source.object.schema.is_some() {
            return None;
        }
        visible
            .iter()
            .rev()
            .find(|cte| cte.name == source.object.object)
            .map(|cte| (cte.query.as_ref(), Some(cte.name.clone())))
    };
    let Some(relation) = relation else {
        return match (derived.as_slice(), sources.as_slice()) {
            ([source], []) => Some((source.query.as_ref(), None)),
            ([], [source]) => common_table(source),
            _ => None,
        };
    };
    if let Some(source) = derived.iter().find(|source| source.alias == relation) {
        return Some((source.query.as_ref(), None));
    }
    sources
        .iter()
        .find(|source| source.alias.as_ref().unwrap_or(&source.object.object) == relation)
        .and_then(|source| common_table(source))
}

/// La colonna di uscita `field` di `query` e, in ogni ramo, una colonna di
/// relazione o una funzione spatial su colonne di relazione.
fn output_is_column<'a>(
    query: &'a QueryOperation,
    common_tables: &[&'a CommonTableExpression],
    field: &str,
    visiting: &mut Vec<String>,
    depth: usize,
) -> bool {
    let Some(position) = query.projection.iter().position(|item| {
        item.alias.as_ref().map_or_else(
            || matches!(&item.expression, QueryExpression::Column { column } if column.field == field),
            |alias| alias == field,
        )
    }) else {
        return false;
    };
    let mut branches = vec![query];
    let mut index = 0;
    while let Some(branch) = branches.get(index).copied() {
        branches.extend(branch.set_operations.iter().map(|set| set.query.as_ref()));
        index += 1;
    }
    branches.into_iter().all(|branch| {
        let Some(item) = branch.projection.get(position) else {
            return false;
        };
        let names = common_tables
            .iter()
            .map(|cte| cte.name.clone())
            .collect::<BTreeSet<_>>();
        let scope = RelationScope::of(branch, &names);
        expression_is_column(
            &item.expression,
            branch,
            &scope,
            common_tables,
            visiting,
            depth,
        )
    })
}

fn expression_is_column<'a>(
    expression: &'a QueryExpression,
    operation: &'a QueryOperation,
    scope: &RelationScope,
    common_tables: &[&'a CommonTableExpression],
    visiting: &mut Vec<String>,
    depth: usize,
) -> bool {
    match expression {
        QueryExpression::Column { column } => {
            scope.base_column(column).is_some()
                || relation_output_is_column(operation, common_tables, column, visiting, depth)
        }
        QueryExpression::Spatial {
            function,
            arguments,
        } if function.returns_geometry() => arguments
            .iter()
            .enumerate()
            .filter(|(position, _)| function.takes_geometry_at(*position))
            .all(|(_, argument)| {
                expression_is_column(argument, operation, scope, common_tables, visiting, depth)
            }),
        _ => false,
    }
}

fn classify_geometry(
    argument: &QueryExpression,
    blocks: &[Block<'_>],
    index: usize,
) -> GeometryClass {
    if is_probe_term(argument) {
        return GeometryClass::Probe;
    }
    match argument {
        QueryExpression::Column { column } if is_relation_column(column, blocks, index) => {
            GeometryClass::Column
        }
        QueryExpression::Spatial { function, .. } if function.returns_geometry() => {
            GeometryClass::Produced
        }
        _ => GeometryClass::Unclassified,
    }
}

/// Gli argomenti di un nodo spatial, ciascuno con `true` se sta in posizione
/// geometrica, e se il nodo confronta due geometrie (una funzione con due
/// argomenti geometrici, un operatore spatial). `None` per ogni altro nodo.
fn spatial_arguments(
    expression: &QueryExpression,
) -> Option<(Vec<(&QueryExpression, bool)>, bool)> {
    let (function, arguments) = match expression {
        QueryExpression::SpatialOperator { left, right, .. } => {
            return Some((vec![(left.as_ref(), true), (right.as_ref(), true)], true));
        }
        QueryExpression::Spatial {
            function,
            arguments,
        }
        | QueryExpression::SpatialWindow {
            function,
            arguments,
            ..
        } => (function, arguments),
        _ => return None,
    };
    let arguments = arguments
        .iter()
        .enumerate()
        .map(|(position, argument)| (argument, function.takes_geometry_at(position)))
        .collect::<Vec<_>>();
    let comparing = arguments.iter().filter(|(_, geometric)| *geometric).count() >= 2;
    Some((arguments, comparing))
}

/// La regola P, per un piano intero.
///
/// **Il mondo e chiuso.** In ogni nodo spatial — una funzione `ST_*`, un
/// operatore spatial, una finestra spatial — di ogni clausola, ogni
/// argomento in posizione geometrica si classifica in positivo come una sola
/// di queste cose ([`GeometryClass`]):
///
/// - una **colonna** di una tabella di base nominata dalla FROM del blocco
///   (o, per un riferimento qualificato correlato, del blocco esterno che la
///   nomina), o di una sorgente derivata o di una CTE la cui projection la
///   definisce, in ogni ramo di set operation, come una di queste colonne o
///   una funzione spatial su di esse (l'alias si segue fino alla tabella; una
///   CTE ricorsiva per induzione);
/// - una **funzione spatial che rende una geometria**, i cui argomenti
///   geometrici si classificano allo stesso modo (`ST_Transform(t.g, n)`,
///   `ST_Centroid(t.g)`);
/// - una **sonda**: un `SpatialValue`, o un bind direttamente in quella
///   posizione, anche sotto funzioni che ne conservano il frame
///   (`ST_Centroid(sonda)`).
///
/// Ogni altra espressione in posizione geometrica — `CAST`, `COALESCE`,
/// `CASE`, subquery, aritmetica, funzioni scalari, una colonna di una
/// sorgente derivata o di una CTE definita da una di queste espressioni (o
/// da un `SELECT *`), una colonna senza qualificazione che un blocco esterno
/// potrebbe fornire — si rifiuta compilando
/// ([`GEOMETRY_ARGUMENT_UNCLASSIFIED`]): un bind nascosto da un involucro non
/// e riconosciuto come sonda da nessun renderer e da nessun provider, e
/// arrivava al server senza che lo SRID fosse confrontato (fino al 25/10
/// `t.g && CAST(:p AS BINARY)` con un EWKB 3857 rendeva una riga su una
/// colonna 4326, `ST_Intersects(t.g, CAST(:p AS BINARY))` con una sonda
/// 3857 lontana zero righe, su `PostGIS`, SQL Server e `MariaDB`).
///
/// Una sonda e poi ammessa soltanto in un nodo che confronta due geometrie
/// (una funzione con due argomenti geometrici, un operatore spatial), in un
/// `WHERE`, `ON` o `HAVING`, e confrontata con una colonna di una tabella
/// nella FROM dello stesso blocco (o con un'altra sonda). Li, e soltanto li,
/// [`admit_probe`] la giudica contro la colonna. Le altre posizioni si
/// rifiutano con `unsupported`:
///
/// - fuori da un predicato — ogni projection, anche di una sorgente derivata,
///   di una CTE, di una subquery o di un ramo di set operation; `ORDER BY`,
///   `GROUP BY`, `DISTINCT ON`, finestre: [`PROBE_OUTSIDE_PREDICATE`]. La
///   colonna della projection porta il frame della sonda a chi la nomina per
///   alias, e chi la nomina non la vede (`JOIN (SELECT sonda AS s) d ON t.g =
///   d.s`, `g IN (SELECT sonda)`: 0 righe senza errore su d30d1d2);
/// - in un predicato, sotto un nodo che non la giudica — un operatore non
///   spatial (`=`, `IN`, `BETWEEN`, ...), una funzione con una sola geometria
///   (`ST_Area(sonda)`, `ST_Transform(sonda, n)`):
///   [`admit_probe`] con [`ProbeUse::ScalarOperand`],
///   [`SPATIAL_VALUE_OUTSIDE_SPATIAL`];
/// - confrontata con un termine che non si risolve in una colonna della FROM
///   del blocco — un riferimento correlato a una query esterna, una colonna
///   di una sorgente derivata o di una CTE, una funzione che cambia frame:
///   [`PROBE_PEER_OUTSIDE_BLOCK`].
///
/// Un argomento non classificato si visita comunque come espressione: se
/// contiene una sonda fuori posto, il motivo e quello della sonda; se no,
/// alla fine della visita e [`GEOMETRY_ARGUMENT_UNCLASSIFIED`].
///
/// La visita e iterativa e copre ogni blocco: CTE, sorgenti derivate, join,
/// subquery di ogni tipo, set operation. E l'unica: la chiedono il renderer
/// relazionale per ogni dialetto, con o senza catalogo, anche per il
/// predicato isolato di `render_filter` ([`judge_probe_predicate`]), e il
/// provider `PostgreSQL` sul piano che giudica.
///
/// # Limite dichiarato
///
/// - **Regola.** Una sonda nell'`ORDER BY` (`ORDER BY g <-> sonda`, la
///   ricerca KNN indicizzata) o nella projection (`SELECT ST_Distance(g,
///   sonda)`) non e ammessa, anche dove il catalogo o la guardia del server
///   la proverebbero. In posizione geometrica di un nodo spatial non sono
///   ammesse, anche senza sonde: una colonna senza qualificazione in un
///   blocco annidato sotto una FROM con sorgenti derivate o CTE; una colonna
///   di una sorgente derivata o di una CTE nominata con `SELECT *`, o
///   definita da un riferimento correlato, o oltre 64 livelli; una funzione
///   scalare o un `CASE` su colonne di tabella (`COALESCE(t.g, u.g)`).
/// - **Ambito.** Ogni query relazionale, su ogni dialetto e percorso.
/// - **Hazard.** Nessuno di correttezza: e una forma rifiutata, non un
///   risultato sbagliato. Il chiamante calcola la distanza o l'ordinamento
///   dopo la lettura, filtra con un predicato, qualifica la colonna o la
///   legge dalla tabella di base.
/// - **Rientro.** Una posizione si riammette quando il valore che la sonda
///   produce non lascia il blocco (non e una colonna di uscita nominabile per
///   alias) e la prova live su `PostGIS` e `MySQL` mostra che una sonda in un
///   altro sistema e rifiutata prima di ogni riga anche li. Una colonna
///   senza qualificazione o un `SELECT *` si riammettono con il catalogo
///   delle colonne delle relazioni nominate; un `COALESCE` o un `CASE` su
///   colonne quando la classifica li attraversa ramo per ramo, senza bind.
///
/// # Errors
///
/// `unsupported` con uno dei motivi sopra: per la prima sonda fuori posto in
/// ordine di visita, o, se non ce ne sono, per il primo argomento non
/// classificato.
pub fn judge_probe_positions(
    provider: ProviderKind,
    evidence: ProbeEvidence,
    operation: &QueryOperation,
) -> Result<()> {
    judge(
        provider,
        evidence,
        Vec::new(),
        vec![ProbeItem::Operation(operation, Vec::new(), None)],
    )
}

/// La regola P per un predicato isolato su una sola relazione di base, come
/// lo rende `render_filter`: la stessa visita di [`judge_probe_positions`],
/// con ogni colonna del predicato colonna di `relation`.
///
/// # Errors
///
/// Quelli di [`judge_probe_positions`].
pub fn judge_probe_predicate(
    provider: ProviderKind,
    evidence: ProbeEvidence,
    predicate: &QueryExpression,
    relation: &str,
) -> Result<()> {
    judge(
        provider,
        evidence,
        vec![Block {
            scope: RelationScope::single(relation),
            parent: None,
            operation: None,
            common_tables: Vec::new(),
        }],
        vec![ProbeItem::Expression(predicate, Clause::Predicate, 0)],
    )
}

#[allow(clippy::too_many_lines)]
fn judge<'a>(
    provider: ProviderKind,
    evidence: ProbeEvidence,
    mut blocks: Vec<Block<'a>>,
    mut stack: Vec<ProbeItem<'a>>,
) -> Result<()> {
    let unsupported =
        |message: &str| DatabaseError::unsupported(provider, ErrorPhase::Prepare, message);
    let refuse = |clause: Clause| match clause {
        Clause::Predicate => admit_probe(
            provider,
            evidence,
            ProbeUse::ScalarOperand,
            ProbeFrame::Unbound,
        )
        .err()
        .unwrap_or_else(|| unsupported(SPATIAL_VALUE_OUTSIDE_SPATIAL)),
        Clause::Elsewhere => unsupported(PROBE_OUTSIDE_PREDICATE),
    };
    let mut unclassified = false;
    while let Some(item) = stack.pop() {
        match item {
            ProbeItem::Operation(operation, visible, parent) => {
                let names = visible
                    .iter()
                    .map(|cte| cte.name.clone())
                    .collect::<BTreeSet<_>>();
                let scope = RelationScope::of(operation, &names);
                let mut inner = visible;
                inner.extend(operation.common_table_expressions.iter());
                let index = blocks.len();
                blocks.push(Block {
                    scope,
                    parent,
                    operation: Some(operation),
                    common_tables: inner.clone(),
                });
                // Una CTE e una sorgente derivata non vedono la FROM di chi
                // le nomina; una sorgente `LATERAL` si, e un ramo di set
                // operation vede quello che vede il blocco.
                let blocks_inside = operation
                    .common_table_expressions
                    .iter()
                    .map(|cte| (cte.query.as_ref(), None))
                    .chain(
                        operation
                            .derived_source
                            .iter()
                            .map(|source| (source.query.as_ref(), None)),
                    )
                    .chain(operation.joins.iter().filter_map(|join| {
                        join.derived_source
                            .as_ref()
                            .map(|source| (source.query.as_ref(), join.lateral.then_some(index)))
                    }))
                    .chain(
                        operation
                            .set_operations
                            .iter()
                            .map(|set| (set.query.as_ref(), parent)),
                    );
                for (block, block_parent) in blocks_inside {
                    stack.push(ProbeItem::Operation(block, inner.clone(), block_parent));
                }
                let elsewhere = operation
                    .projection
                    .iter()
                    .map(|item| &item.expression)
                    .chain(&operation.distinct_on)
                    .chain(&operation.group_by)
                    .chain(operation.order_by.iter().map(|order| &order.expression));
                for expression in elsewhere {
                    stack.push(ProbeItem::Expression(expression, Clause::Elsewhere, index));
                }
                let predicates = operation
                    .filter
                    .iter()
                    .chain(operation.joins.iter().filter_map(|join| join.on.as_ref()))
                    .chain(operation.having.iter());
                for expression in predicates {
                    stack.push(ProbeItem::Expression(expression, Clause::Predicate, index));
                }
            }
            ProbeItem::Expression(expression, clause, index) => {
                if let Some((arguments, comparing)) = spatial_arguments(expression) {
                    // Gli argomenti di una finestra spatial stanno fuori da
                    // ogni predicato, come la finestra stessa.
                    let clause = if matches!(expression, QueryExpression::SpatialWindow { .. }) {
                        Clause::Elsewhere
                    } else {
                        clause
                    };
                    let judging = comparing && clause == Clause::Predicate;
                    let probes = judging
                        && arguments
                            .iter()
                            .any(|(argument, geometric)| *geometric && is_probe_term(argument));
                    for (argument, geometric) in arguments {
                        if !geometric {
                            stack.push(ProbeItem::Expression(argument, clause, index));
                            continue;
                        }
                        if probes
                            && !is_probe_term(argument)
                            && !matches!(
                                term_frame(argument, &blocks[index].scope),
                                TermFrame::Column(_)
                            )
                        {
                            return Err(unsupported(PROBE_PEER_OUTSIDE_BLOCK));
                        }
                        match classify_geometry(argument, &blocks, index) {
                            GeometryClass::Probe if judging => {
                                push_probe(argument, index, &mut stack)?;
                            }
                            GeometryClass::Probe => return Err(refuse(clause)),
                            GeometryClass::Column => {}
                            GeometryClass::Produced => {
                                stack.push(ProbeItem::Expression(argument, clause, index));
                            }
                            GeometryClass::Unclassified => {
                                unclassified = true;
                                stack.push(ProbeItem::Expression(argument, clause, index));
                            }
                        }
                    }
                    if let QueryExpression::SpatialWindow {
                        partition_by,
                        order_by,
                        ..
                    } = expression
                    {
                        for child in partition_by
                            .iter()
                            .chain(order_by.iter().map(|order| &order.expression))
                        {
                            stack.push(ProbeItem::Expression(child, Clause::Elsewhere, index));
                        }
                    }
                    continue;
                }
                let visible = || blocks[index].common_tables.clone();
                match expression {
                    QueryExpression::Wildcard { .. }
                    | QueryExpression::Column { .. }
                    | QueryExpression::Parameter { .. }
                    | QueryExpression::TypedParameter { .. } => {}
                    QueryExpression::SpatialValue { .. } => return Err(refuse(clause)),
                    // I nodi spatial sono tutti sopra.
                    QueryExpression::Spatial { .. }
                    | QueryExpression::SpatialOperator { .. }
                    | QueryExpression::SpatialWindow { .. } => {
                        return Err(unsupported(GEOMETRY_ARGUMENT_UNCLASSIFIED));
                    }
                    QueryExpression::Window {
                        arguments,
                        partition_by,
                        order_by,
                        ..
                    } => {
                        for child in arguments
                            .iter()
                            .chain(partition_by)
                            .chain(order_by.iter().map(|order| &order.expression))
                        {
                            stack.push(ProbeItem::Expression(child, Clause::Elsewhere, index));
                        }
                    }
                    QueryExpression::ScalarSubquery { query }
                    | QueryExpression::Exists { query, .. } => {
                        stack.push(ProbeItem::Operation(query, visible(), Some(index)));
                    }
                    QueryExpression::InSubquery {
                        expression, query, ..
                    } => {
                        stack.push(ProbeItem::Expression(expression, clause, index));
                        stack.push(ProbeItem::Operation(query, visible(), Some(index)));
                    }
                    QueryExpression::Arithmetic { left, right, .. }
                    | QueryExpression::Compare { left, right, .. } => {
                        stack.push(ProbeItem::Expression(left, clause, index));
                        stack.push(ProbeItem::Expression(right, clause, index));
                    }
                    QueryExpression::Cast { expression, .. }
                    | QueryExpression::NullsOrder { expression, .. }
                    | QueryExpression::SpatialOutput { expression, .. }
                    | QueryExpression::Not { expression }
                    | QueryExpression::IsNull { expression, .. } => {
                        stack.push(ProbeItem::Expression(expression, clause, index));
                    }
                    QueryExpression::Case {
                        branches,
                        else_expression,
                    } => {
                        for branch in branches {
                            stack.push(ProbeItem::Expression(&branch.when, clause, index));
                            stack.push(ProbeItem::Expression(&branch.then, clause, index));
                        }
                        if let Some(otherwise) = else_expression {
                            stack.push(ProbeItem::Expression(otherwise, clause, index));
                        }
                    }
                    QueryExpression::Scalar { arguments, .. }
                    | QueryExpression::And { arguments }
                    | QueryExpression::Or { arguments } => {
                        for argument in arguments {
                            stack.push(ProbeItem::Expression(argument, clause, index));
                        }
                    }
                    QueryExpression::InList {
                        expression, values, ..
                    } => {
                        stack.push(ProbeItem::Expression(expression, clause, index));
                        for value in values {
                            stack.push(ProbeItem::Expression(value, clause, index));
                        }
                    }
                    QueryExpression::Between {
                        expression,
                        lower,
                        upper,
                        ..
                    } => {
                        for child in [expression, lower, upper] {
                            stack.push(ProbeItem::Expression(child, clause, index));
                        }
                    }
                    QueryExpression::Like {
                        expression,
                        pattern,
                        ..
                    } => {
                        stack.push(ProbeItem::Expression(expression, clause, index));
                        stack.push(ProbeItem::Expression(pattern, clause, index));
                    }
                }
            }
        }
    }
    if unclassified {
        return Err(unsupported(GEOMETRY_ARGUMENT_UNCLASSIFIED));
    }
    Ok(())
}

/// Una sonda ammessa da un nodo che la giudica: si scende lungo le funzioni
/// che ne conservano il frame, e gli altri argomenti di quelle funzioni (una
/// distanza, una tolleranza) si visitano come espressioni del predicato.
fn push_probe<'a>(
    argument: &'a QueryExpression,
    index: usize,
    stack: &mut Vec<ProbeItem<'a>>,
) -> Result<()> {
    let mut current = argument;
    loop {
        match current {
            QueryExpression::SpatialValue { expression, .. } => {
                stack.push(ProbeItem::Expression(expression, Clause::Predicate, index));
                return Ok(());
            }
            QueryExpression::Parameter { .. } | QueryExpression::TypedParameter { .. } => {
                return Ok(());
            }
            QueryExpression::Spatial { arguments, .. } => {
                // `is_probe_term` ha gia provato la catena: un passo che non
                // la segue e un errore, non un argomento accettato.
                let Some((preserved, next)) = preserved_argument(current) else {
                    return Err(DatabaseError::new(
                        ErrorCategory::Internal,
                        ErrorPhase::Prepare,
                        None,
                        GEOMETRY_ARGUMENT_UNCLASSIFIED,
                    ));
                };
                for (position, child) in arguments.iter().enumerate() {
                    if position != preserved {
                        stack.push(ProbeItem::Expression(child, Clause::Predicate, index));
                    }
                }
                current = next;
            }
            _ => {
                return Err(DatabaseError::new(
                    ErrorCategory::Internal,
                    ErrorPhase::Prepare,
                    None,
                    GEOMETRY_ARGUMENT_UNCLASSIFIED,
                ));
            }
        }
    }
}

#[cfg(test)]
#[path = "spatial_probe_tests.rs"]
mod tests;
