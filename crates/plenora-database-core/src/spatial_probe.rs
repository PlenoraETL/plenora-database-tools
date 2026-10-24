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
//! Il confronto con lo SRID della colonna resta del provider, che sa leggerlo
//! dal proprio catalogo.

use crate::ewkb::inspect_ewkb_detailed;
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};

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
/// ST_SetSRID(ST_GeomFromEWKB(geometry), srid) END`. Il confronto non ha
/// variabili di riga: il server lo valuta una volta, anche come chiave di un
/// indice `GiST`, e anche su una tabella vuota (misurato su `PostGIS` 3 con
/// `enable_seqscan=off`). Con due SRID diversi `ST_Equals` fallisce con
/// «Operation on mixed SRID geometries», che il provider pubblica `crs`; una
/// colonna senza typmod ha SRID 0 e fallisce allo stesso modo. Con SRID
/// uguali vale la sonda.
#[must_use]
pub fn postgres_guarded_probe(geometry: &str, srid: &str, relation: &str, column: &str) -> String {
    format!(
        "(CASE WHEN ST_Equals({}, {}) THEN ST_SetSRID(ST_GeomFromEWKB({geometry}), {srid}) END)",
        postgis_empty(srid),
        postgis_empty(&format!(
            "COALESCE((SELECT postgis_typmod_srid(a.atttypmod) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = to_regclass({relation}) AND a.attname = {column} \
             AND a.attnum > 0 AND NOT a.attisdropped), 0)"
        )),
    )
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

#[cfg(test)]
#[path = "spatial_probe_tests.rs"]
mod tests;
