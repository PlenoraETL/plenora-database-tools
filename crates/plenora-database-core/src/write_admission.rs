//! La regola A: una scrittura si ammette per misura, a mondo chiuso.
//!
//! # Perche
//!
//! Fino al 25/10 ogni provider ammetteva una colonna del target per **nome**
//! del tipo (`native_type_matches`: `starts_with("TIMESTAMP")`, un elenco di
//! `DATA_TYPE`) e dichiarava poi a mano, caso per caso, le riscritture della
//! regola S (`crate::store_rewrite`). Quattro letture indipendenti hanno
//! trovato 7, 4, 1 e 5 difetti silenziosi, senza convergere: la settima, tutti
//! con `Strict`, `committed` e il `LossReport` vuoto, `TIMESTAMP(p<6)` di
//! Oracle che arrotonda e di Db2 che tronca, il tipo `JSON` di Oracle che
//! normalizza i numeri (`1.0` in `1`, 50 cifre in 38, `1e400` in `"Inf"`),
//! `BINARY_FLOAT` di Oracle che salva `-0.0` come `+0`, `DOUBLE(M,D)` e
//! `FLOAT(M,D)` di `MySQL` che arrotondano e perdono `-0`. Un parametro del tipo
//! che nessuno aveva guardato bastava ogni volta.
//!
//! # La regola
//!
//! La **chiave d'ammissione** di una colonna e la coppia fra il tipo Arrow
//! dell'ingresso (con unita, zona e precisione: [`arrow_key`]) e il tipo
//! nativo del target con **tutti** i parametri che il catalogo ne dice e che
//! decidono che cosa si salva: precisione, scala, semantica della lunghezza
//! (`BYTE`/`CHAR`), charset o code page, `ANSI_PADDING`, `varchar2_compat`,
//! il genere del documento JSON. La costruisce il provider, dallo stesso
//! catalogo che legge il preflight, nella forma che la sonda registra.
//!
//! Le misure stanno in un fixture per provider
//! (`tests/fixtures/scrittura_server_<provider>.json`, [`AdmissionFixture`]),
//! scritto da `scripts/sonde_scritture_server.py`: per ogni chiave la sonda
//! scrive, con il **percorso di scrittura della libreria**, una batteria fissa
//! di valori limite della famiglia Arrow, li rilegge con la lettura della
//! libreria e li confronta valore per valore. Nel preflight ogni provider
//! cerca la chiave della colonna nel fixture della major del server:
//!
//! - **misurata esatta**: ammessa con ogni politica;
//! - **misurata con riscritture**: le riscritture misurate diventano le
//!   [`StoreRewrite`] della regola S, con i parametri del target ([`TargetDimensions`]):
//!   con `Strict` il valore riscritto si rifiuta, con le altre politiche la
//!   perdita si dichiara nel `LossReport` con il motivo preciso, e in una
//!   chiave si rifiuta con ogni politica. Una riscrittura che nessuna classe
//!   per valore spiega e [`StoreRewrite::Every`]: ogni valore non nullo;
//! - **misurata come rifiutata** (ogni valore della batteria rifiutato dal
//!   server o dalla libreria) o **non rileggibile**: rifiutata nel preflight,
//!   invece che al primo valore;
//! - **non misurata**: rifiutata con **ogni** politica, `unsupported`, fase
//!   `prepare`, [`UNPROVEN_PAIR`]: non e provata. Lo stesso per una major del
//!   server che il fixture non misura ([`UNPROVEN_SERVER`]).
//!
//! Il messaggio non porta ne valori ne nomi: la chiave e un fatto del target,
//! non un dato, ma la regola 4 di `AGENTS.md` vuole messaggi senza payload, e
//! il tipo del target compare gia nel `LossReport` quando c'e una perdita.
//!
//! # Limite dichiarato: le dimensioni su rappresentanti
//!
//! - **Regola**: la lunghezza di un testo o di un binario (`VARCHAR2(n)`,
//!   `CHAR(n)`, `RAW(n)`, `varchar(n)`) non fa parte della chiave come valore:
//!   la chiave porta `n` in forma simbolica e il fixture le lunghezze misurate
//!   ([`MeasuredKey::dimensione`]). Una colonna si ammette se la sua lunghezza
//!   sta fra la minima e la massima misurata della stessa chiave e nessuna
//!   misura della chiave e rifiutata o non rileggibile; le classi di
//!   riscrittura sono l'**unione** di quelle misurate alle varie lunghezze
//!   (`CHAR(1)` non mostra il riempimento che `CHAR(10)` mostra).
//! - **Ambito**: i parametri di dimensione; precisione e scala dei numeri e
//!   dei tempi, semantica della lunghezza e charset sono parte della chiave.
//! - **Hazard**: un comportamento che cambia a una lunghezza intermedia non
//!   misurata (nessuno noto) non si vedrebbe.
//! - **Rientro**: una misura in piu per lunghezza, o la lunghezza nella chiave.
//!
//! # Limite dichiarato: la batteria
//!
//! - **Regola**: una chiave e esatta se nessun valore della batteria della sua
//!   famiglia si riscrive.
//! - **Ambito**: la batteria e fissa e versionata
//!   ([`ServerMeasurement::batteria`]); un valore fuori dalla batteria che il
//!   server riscrivesse in un modo che nessuna classe misurata predice non si
//!   vedrebbe.
//! - **Hazard**: una riscrittura di un valore non misurato.
//! - **Rientro**: un valore in piu nella batteria, e una nuova misura.
//!
//! # Limite dichiarato: i documenti JSON
//!
//! - **Regola**: un documento rientra esatto se e lo stesso documento JSON:
//!   spazi fuori dalle stringhe, ordine dei membri ed escape delle stringhe non
//!   distinguono; i numeri si confrontano per forma, cifra per cifra; una
//!   chiave ripetuta conta.
//! - **Ambito**: i tipi documento (`json`, `jsonb`, `JSON` di Oracle e di
//!   `MySQL`).
//! - **Hazard**: chi legge il testo byte per byte vede spazi e ordine diversi
//!   da quelli scritti.
//! - **Rientro**: il confronto byte per byte anche per i documenti.
//!
//! # Limite dichiarato: la major del server
//!
//! - **Regola**: il fixture vale per le major misurate; un'altra major si
//!   rifiuta come non provata, con ogni politica (si e scelto il rifiuto, non
//!   la dichiarazione: una perdita dichiarata su un server mai misurato
//!   sarebbe una supposizione).
//! - **Ambito**: ogni provider; la major e quella che il provider legge dal
//!   server nella sessione del preflight.
//! - **Hazard**: piu stretto: un server nuovo non scrive finche non e misurato;
//!   una minor che cambiasse comportamento dentro una major misurata non si
//!   vedrebbe (la sonda di regressione gira sulle immagini fissate).
//! - **Rientro**: la misura della major, con la sonda.

use crate::loss::MappingPolicy;
use crate::plan::ProviderKind;
use crate::store_rewrite::StoreRewrite;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use arrow_schema::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};

/// La versione della batteria della sonda. Un fixture misurato con un'altra
/// batteria non ammette niente: la batteria e parte della prova.
pub const BATTERY_VERSION: &str = "1";

/// Il motivo pubblico del rifiuto di una coppia non misurata.
pub const UNPROVEN_PAIR: &str = "scrittura rifiutata: la coppia fra il tipo Arrow e il tipo \
     del target non e misurata su questo server (regola A): non e provata, con nessuna politica";

/// Il motivo pubblico del rifiuto di una major non misurata.
pub const UNPROVEN_SERVER: &str = "scrittura rifiutata: la versione del server non e misurata \
     (regola A): nessuna coppia di tipi e provata, con nessuna politica";

/// Il motivo pubblico del rifiuto di una coppia misurata come rifiutata o non
/// rileggibile.
pub const REFUSED_PAIR: &str = "scrittura rifiutata: la misura della coppia fra il tipo Arrow e \
     il tipo del target (regola A) non ha scritto e riletto alcun valore";

/// Il motivo pubblico di un fixture incorporato non valido.
pub const INVALID_FIXTURE: &str =
    "fixture delle misure di scrittura (regola A) incorporato non valido: nessuna scrittura e provata";

/// Le misure di un provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionFixture {
    pub schema_version: u32,
    /// Il provider, nella forma di `ProviderKind` (`oracle`, `mysql`, ...).
    pub provider: String,
    pub misure: Vec<ServerMeasurement>,
}

/// Le misure su un server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerMeasurement {
    /// La major nella forma che il provider legge dal server.
    pub server_major: String,
    /// La versione completa misurata, per il registro.
    pub server_version: String,
    /// L'immagine misurata, per digest.
    pub immagine: String,
    /// La versione della batteria ([`BATTERY_VERSION`]).
    pub batteria: String,
    pub chiavi: Vec<MeasuredKey>,
}

/// L'esito della misura di una chiave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasuredOutcome {
    /// Ogni valore scritto si rilegge uguale (o e rifiutato con un errore).
    Esatto,
    /// Almeno un valore si rilegge diverso.
    Riscrive,
    /// Ogni valore non nullo e rifiutato con un errore.
    Rifiuta,
    /// La scrittura riesce ma la lettura della libreria non rilegge i valori.
    Illeggibile,
}

/// Una classe di riscrittura misurata, senza i parametri del target.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "classe", rename_all = "snake_case", deny_unknown_fields)]
pub enum MeasuredRewrite {
    VuotoANull,
    RiempieTesto,
    RiempieBinario,
    ToglieFinali,
    SpaziOltre,
    /// Il minimo assoluto si scrive come testo (`"1e-130"`): un numero JSON
    /// letto da `serde_json` senza `float_roundtrip` puo cambiare l'ultima
    /// cifra (`1e-130` diventava `9.999999999999999e-131`), e la soglia della
    /// misura deve essere quella misurata.
    Underflow {
        #[serde(with = "exact_float")]
        min_abs: f64,
    },
    ZeroNegativo,
    FrazioneSecondi {
        cifre: u8,
    },
    NonAscii,
    JsonNumeri,
    Ogni,
}

/// Un `f64` come testo, letto con `str::parse` (esatto) e scritto con la forma
/// piu corta che si rilegge uguale.
mod exact_float {
    use serde::{Deserialize, Deserializer, Serializer};

    // La firma per riferimento la impone `serde(with)`.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{value:e}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        let text = String::deserialize(deserializer)?;
        let value = text
            .parse::<f64>()
            .map_err(|_| serde::de::Error::custom("soglia non numerica"))?;
        if value.is_finite() && value > 0.0 {
            Ok(value)
        } else {
            Err(serde::de::Error::custom("soglia non positiva o non finita"))
        }
    }
}

/// La misura di una chiave d'ammissione.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasuredKey {
    /// Il tipo Arrow ([`arrow_key`]).
    pub arrow: String,
    /// Il tipo nativo con i parametri, nella forma del provider.
    pub nativo: String,
    /// La lunghezza misurata, per le chiavi con un parametro di dimensione
    /// simbolico; `None` altrimenti.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensione: Option<u64>,
    pub esito: MeasuredOutcome,
    /// Le classi che spiegano i valori riscritti.
    pub riscritture: Vec<MeasuredRewrite>,
    /// I valori della batteria riscritti, per nome.
    pub riscritti: Vec<String>,
    /// I valori della batteria rifiutati con un errore, per nome.
    pub rifiutati: Vec<String>,
    /// I valori che le classi trattano come riscritti e che la misura ha
    /// riletto uguali (la verifica per eccesso), per nome.
    pub per_eccesso: Vec<String>,
}

/// I parametri di dimensione della colonna del target, per istanziare le
/// classi che ne dipendono.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TargetDimensions {
    /// La lunghezza in caratteri (`CHAR(n)`, `NCHAR(n)`).
    pub chars: Option<u64>,
    /// La lunghezza in byte (`RAW(n)`, `binary(n)`, il troncamento di Db2).
    pub bytes: Option<u64>,
}

/// La chiave d'ammissione di una colonna.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionKey {
    pub arrow: String,
    pub nativo: String,
    /// La lunghezza della colonna se `nativo` la porta in forma simbolica.
    pub dimensione: Option<u64>,
}

/// Il fixture di un provider, letto e validato.
#[derive(Debug, Clone)]
pub struct AdmissionTable {
    provider: ProviderKind,
    fixture: AdmissionFixture,
}

/// Le misure di una major.
#[derive(Debug, Clone, Copy)]
pub struct ServerAdmission<'a> {
    provider: ProviderKind,
    measurement: &'a ServerMeasurement,
}

impl AdmissionTable {
    /// Legge il fixture incorporato di `provider`.
    ///
    /// # Errors
    ///
    /// `internal` con [`INVALID_FIXTURE`] se il documento non e valido, ripete
    /// una chiave JSON o una chiave d'ammissione, dichiara un altro provider o
    /// un'altra batteria: nessuna scrittura e provata.
    pub fn parse(provider: ProviderKind, bytes: &[u8]) -> Result<Self> {
        let fixture: AdmissionFixture =
            crate::strict_json::from_slice(bytes).map_err(|_| invalid_fixture(provider))?;
        if fixture.schema_version != 1 || fixture.provider != provider_name(provider) {
            return Err(invalid_fixture(provider));
        }
        let mut majors = std::collections::BTreeSet::new();
        for measurement in &fixture.misure {
            if measurement.batteria != BATTERY_VERSION
                || !majors.insert(measurement.server_major.as_str())
            {
                return Err(invalid_fixture(provider));
            }
            let mut keys = std::collections::BTreeSet::new();
            for key in &measurement.chiavi {
                if !keys.insert((key.arrow.as_str(), key.nativo.as_str(), key.dimensione)) {
                    return Err(invalid_fixture(provider));
                }
                let coherent = match key.esito {
                    MeasuredOutcome::Esatto => {
                        key.riscritture.is_empty() && key.riscritti.is_empty()
                    }
                    MeasuredOutcome::Riscrive => {
                        !key.riscritture.is_empty() && !key.riscritti.is_empty()
                    }
                    MeasuredOutcome::Rifiuta | MeasuredOutcome::Illeggibile => {
                        key.riscritture.is_empty()
                    }
                };
                if !coherent {
                    return Err(invalid_fixture(provider));
                }
            }
        }
        Ok(Self { provider, fixture })
    }

    /// Le misure della major `server_major`.
    ///
    /// # Errors
    ///
    /// `unsupported` con [`UNPROVEN_SERVER`] se il fixture non la misura.
    pub fn server(&self, server_major: &str) -> Result<ServerAdmission<'_>> {
        self.fixture
            .misure
            .iter()
            .find(|measurement| measurement.server_major == server_major)
            .map(|measurement| ServerAdmission {
                provider: self.provider,
                measurement,
            })
            .ok_or_else(|| unproven(self.provider, UNPROVEN_SERVER))
    }

    /// Il fixture letto.
    #[must_use]
    pub const fn fixture(&self) -> &AdmissionFixture {
        &self.fixture
    }
}

impl ServerAdmission<'_> {
    /// Le riscritture della regola S per la colonna con la chiave `key`, con
    /// i parametri di dimensione del target.
    ///
    /// # Errors
    ///
    /// `unsupported` con [`UNPROVEN_PAIR`] se la chiave non e misurata (o la
    /// sua lunghezza e fuori da quelle misurate, o le misure della chiave non
    /// danno le stesse classi), con [`REFUSED_PAIR`] se e misurata come
    /// rifiutata o non rileggibile; `internal` se una classe misurata chiede
    /// un parametro che il target non da.
    pub fn admit(
        &self,
        key: &AdmissionKey,
        dimensions: TargetDimensions,
    ) -> Result<Vec<StoreRewrite>> {
        let measured = self
            .measurement
            .chiavi
            .iter()
            .filter(|candidate| candidate.arrow == key.arrow && candidate.nativo == key.nativo)
            .collect::<Vec<_>>();
        let Some(first) = measured.first() else {
            return Err(unproven(self.provider, UNPROVEN_PAIR));
        };
        match key.dimensione {
            None => {
                if measured.len() != 1 || first.dimensione.is_some() {
                    return Err(unproven(self.provider, UNPROVEN_PAIR));
                }
            }
            Some(length) => {
                let lengths = measured
                    .iter()
                    .map(|candidate| candidate.dimensione)
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| unproven(self.provider, UNPROVEN_PAIR))?;
                let (Some(min), Some(max)) = (lengths.iter().min(), lengths.iter().max()) else {
                    return Err(unproven(self.provider, UNPROVEN_PAIR));
                };
                if length < *min || length > *max {
                    return Err(unproven(self.provider, UNPROVEN_PAIR));
                }
            }
        }
        // Le classi della chiave sono l'unione delle misure delle sue
        // lunghezze (una lunghezza corta non mostra il riempimento che una
        // lunga mostra); una lunghezza rifiutata o non rileggibile non prova
        // la chiave.
        if measured.iter().any(|candidate| {
            matches!(
                candidate.esito,
                MeasuredOutcome::Rifiuta | MeasuredOutcome::Illeggibile
            )
        }) {
            return Err(unproven(self.provider, REFUSED_PAIR));
        }
        let mut classes: Vec<MeasuredRewrite> = Vec::new();
        for rewrite in measured.iter().flat_map(|candidate| &candidate.riscritture) {
            if !classes.contains(rewrite) {
                classes.push(*rewrite);
            }
        }
        classes
            .into_iter()
            .map(|rewrite| instantiate(self.provider, rewrite, dimensions))
            .collect()
    }

    /// La major misurata.
    #[must_use]
    pub fn server_major(&self) -> &str {
        &self.measurement.server_major
    }
}

/// La classe misurata con i parametri del target.
///
/// # Errors
///
/// `internal` se la classe chiede una lunghezza che il target non da: la
/// regola non si verificherebbe.
pub fn instantiate(
    provider: ProviderKind,
    rewrite: MeasuredRewrite,
    dimensions: TargetDimensions,
) -> Result<StoreRewrite> {
    let missing = || {
        DatabaseError::new(
            ErrorCategory::Internal,
            ErrorPhase::Prepare,
            Some(provider),
            "regola A: classe di riscrittura misurata senza la lunghezza del target",
        )
    };
    Ok(match rewrite {
        MeasuredRewrite::VuotoANull => StoreRewrite::EmptyToNull,
        MeasuredRewrite::RiempieTesto => StoreRewrite::PadText {
            chars: dimensions.chars.ok_or_else(missing)?,
        },
        MeasuredRewrite::RiempieBinario => StoreRewrite::PadBinary {
            bytes: dimensions.bytes.ok_or_else(missing)?,
        },
        MeasuredRewrite::ToglieFinali => StoreRewrite::StripTrailing,
        MeasuredRewrite::SpaziOltre => StoreRewrite::ExcessSpaces {
            bytes: dimensions.bytes.ok_or_else(missing)?,
        },
        MeasuredRewrite::Underflow { min_abs } => StoreRewrite::Underflow { min_abs },
        MeasuredRewrite::ZeroNegativo => StoreRewrite::NegativeZero,
        MeasuredRewrite::FrazioneSecondi { cifre } => {
            StoreRewrite::FractionalSeconds { digits: cifre }
        }
        MeasuredRewrite::NonAscii => StoreRewrite::NonAscii,
        MeasuredRewrite::JsonNumeri => StoreRewrite::JsonNumbers,
        MeasuredRewrite::Ogni => StoreRewrite::Every,
    })
}

/// Se una politica ammette le perdite dichiarate (tutte tranne `Strict`).
#[must_use]
pub fn declares_losses(policy: MappingPolicy) -> bool {
    policy != MappingPolicy::Strict
}

/// La forma canonica del tipo Arrow nella chiave d'ammissione.
#[must_use]
pub fn arrow_key(data_type: &DataType) -> String {
    let unit = |unit: &TimeUnit| match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    };
    match data_type {
        DataType::Boolean => "bool".to_owned(),
        DataType::Int8 => "int8".to_owned(),
        DataType::Int16 => "int16".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::UInt8 => "uint8".to_owned(),
        DataType::UInt16 => "uint16".to_owned(),
        DataType::UInt32 => "uint32".to_owned(),
        DataType::UInt64 => "uint64".to_owned(),
        DataType::Float16 => "float16".to_owned(),
        DataType::Float32 => "float32".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Decimal128(precision, scale) => format!("decimal128({precision},{scale})"),
        DataType::Decimal256(precision, scale) => format!("decimal256({precision},{scale})"),
        DataType::Utf8 => "utf8".to_owned(),
        DataType::LargeUtf8 => "large_utf8".to_owned(),
        DataType::Binary => "binary".to_owned(),
        DataType::LargeBinary => "large_binary".to_owned(),
        DataType::FixedSizeBinary(size) => format!("fixed_size_binary({size})"),
        DataType::Date32 => "date32".to_owned(),
        DataType::Date64 => "date64".to_owned(),
        DataType::Time32(time) => format!("time32[{}]", unit(time)),
        DataType::Time64(time) => format!("time64[{}]", unit(time)),
        DataType::Timestamp(time, None) => format!("timestamp[{}]", unit(time)),
        DataType::Timestamp(time, Some(zone)) => format!("timestamp[{},{zone}]", unit(time)),
        // Ogni altro tipo ha la forma di debug: una chiave che nessun fixture
        // misura, quindi rifiutata.
        other => format!("{other:?}"),
    }
}

/// Il nome del provider nel fixture.
#[must_use]
pub const fn provider_name(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Postgres => "postgres",
        ProviderKind::Mysql => "mysql",
        ProviderKind::Mariadb => "mariadb",
        ProviderKind::Sqlserver => "sqlserver",
        ProviderKind::Oracle => "oracle",
        ProviderKind::Db2 => "db2",
        ProviderKind::Sqlite => "sqlite",
        ProviderKind::Duckdb => "duckdb",
    }
}

fn unproven(provider: ProviderKind, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        Some(provider),
        message,
    )
}

fn invalid_fixture(provider: ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Internal,
        ErrorPhase::Prepare,
        Some(provider),
        INVALID_FIXTURE,
    )
}

#[cfg(test)]
#[path = "write_admission_tests.rs"]
mod tests;
