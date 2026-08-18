//! Profilo di prodotto: dove il crate decide cosa dipende dal server.
//!
//! ADR 0014 ha deciso un solo crate con `MysqlProvider` e (in seguito)
//! `MariadbProvider` pubblici e distinti, sopra un profilo **interno**
//! condiviso. Questo modulo e quel profilo. Non e API pubblica, e non deve
//! diventarlo: cio che il consumatore sceglie e il provider, non il profilo.
//!
//! Il profilo raccoglie le decisioni che l'evidenza (`docs/mariadb/EVIDENCE.md`)
//! ha misurato come divergenti fra i due prodotti — riconoscimento, timeout,
//! catalogo, metadata nativi, spatial. Finche esiste una sola
//! implementazione il trait sembra sovradimensionato; e proprio quello il
//! punto dell'estrazione: rendere visibili, in un posto solo, le scelte che
//! oggi sono sparse e implicite, prima che un secondo prodotto le duplichi.
//!
//! Due vincoli di forma, entrambi deliberati:
//!
//! * **`&'static dyn`, non un parametro generico.** `MysqlProvider<P>`
//!   cambierebbe un tipo pubblico e si propagherebbe a CLI e SDK. Il profilo
//!   e senza stato, quindi il costo del dispatch dinamico e una chiamata
//!   indiretta su decisioni che si prendono una volta per statement.
//! * **Il bypass `MariaDB` di test non si sposta.** Vive in `catalog.rs`,
//!   accanto al punto in cui il rifiuto scatta, ed e li che va letto. Il
//!   profilo dice *se* un prodotto e estraneo; resta al chiamante decidere
//!   se in quel preciso test lo si attraversa.

// `pub(crate)` in un modulo privato e ridondante per il compilatore, non per
// chi legge: dice che questi item sono condivisi dentro il crate e che non
// devono uscirne. Il profilo non e API, e la visibilita e il posto in cui
// quella decisione resta scritta.
#![allow(clippy::redundant_pub_crate)]

use crate::query::{prepare_error, unsupported};
use crate::types::MysqlColumnKind;
use crate::MysqlColumnSpec;
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::Column;
use plenora_database_core::{
    plan::ProviderKind, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

/// Collation id riservato di `MySQL` per i tipi binari.
const BINARY_CHARACTER_SET: u16 = 63;

/// Le decisioni che cambiano con il prodotto servito dalla connessione.
///
/// Ogni metodo qui e una domanda a cui `MySQL` e `MariaDB` rispondono, o
/// potrebbero rispondere, in modo diverso. Una domanda a cui rispondono
/// sempre allo stesso modo non appartiene al profilo: appartiene al codice.
pub(crate) trait ProductProfile: Send + Sync {
    /// Nome del prodotto servito, per i messaggi diagnostici.
    fn product(&self) -> &'static str;

    /// Il `ProviderKind` con cui il profilo firma errori e capability.
    fn kind(&self) -> ProviderKind;

    /// Rifiuto del prodotto sbagliato, a partire da cio che la probe legge.
    ///
    /// `None` significa "il server e quello che questo profilo serve".
    /// Non significa "compatibile": la qualifica di una capability resta
    /// una prova live, non una stringa di versione.
    fn foreign_product_rejection(
        &self,
        product_version: &str,
        version_comment: &str,
    ) -> Option<DatabaseError>;

    /// Lo statement che impone il timeout di statement sulla sessione.
    ///
    /// Il contratto del core esprime il timeout in millisecondi; il server
    /// no, necessariamente. Fra i due c'e una conversione, e il punto di
    /// questo metodo e che la conversione stia dove sta anche il nome della
    /// variabile: separarli e il modo in cui un timeout di cinque secondi
    /// diventa uno di cinque millisecondi senza che nulla fallisca.
    ///
    /// ADR 0014 ha misurato che `MAX_EXECUTION_TIME` non esiste su `MariaDB`
    /// (errore 1193), dove la variabile analoga si chiama diversamente **e**
    /// si misura in secondi. Un secondo profilo cambiera entrambe le cose
    /// insieme, in questo metodo, o non le cambiera in modo coerente.
    fn statement_timeout_statement(&self, timeout_ms: u64) -> String;

    /// Le interrogazioni del catalogo.
    ///
    /// Non sono qui per gusto di simmetria: ADR 0014 ha misurato che due
    /// colonne che questo provider legge — `SRS_ID` in `columns`,
    /// `EXPRESSION` in `statistics` — su `MariaDB` non esistono (errore 1054).
    /// La query e la sola cosa che decide quali metadati arrivano, quindi e
    /// la sola cosa che un secondo profilo deve poter cambiare.
    /// Gli schemi visibili, esclusi quelli di sistema del prodotto.
    fn schemas_query(&self) -> &'static str;
    /// Tabelle e viste di uno schema.
    fn objects_query(&self) -> &'static str;
    /// L'oggetto singolo, per schema e nome.
    fn object_query(&self) -> &'static str;
    /// Le colonne di un oggetto, con i metadati che il mapping richiede.
    fn object_columns_query(&self) -> &'static str;
    /// Le parti di indice, ordinate per indice e posizione.
    fn object_indexes_query(&self) -> &'static str;

    /// Se il prodotto pubblica le parti funzionali di un indice.
    ///
    /// Un indice funzionale ha parti senza nome di colonna: `columns` non lo
    /// descrive per intero e non e confrontabile con le keys di un upsert.
    /// Un prodotto che non le pubblicasse non renderebbe l'indice
    /// confrontabile — renderebbe invisibile che non lo e, ed e la ragione
    /// per cui la parte senza colonna ne espressione resta un errore.
    fn reports_functional_index_parts(&self) -> bool;

    /// I metadati nativi di una colonna, letti dal wire.
    ///
    /// Il prepare descrive il tipo del protocollo, non la dichiarazione SQL:
    /// e da qui che esce `MYSQL_NATIVE_TYPE` sul path query, e qui che ADR
    /// 0014 ha misurato la divergenza piu concreta — dalla stessa DDL
    /// `document JSON` `MySQL` manda `MYSQL_TYPE_JSON` e `MariaDB`
    /// `MYSQL_TYPE_BLOB`, quindi lo schema Arrow pubblicato porta
    /// `native_type=json` sull'uno e `text` sull'altro.
    ///
    /// Il profilo possiede la **produzione** del valore: da quale tipo wire
    /// nasce quale nome. Non possiede la **semantica** — se il campo debba
    /// annotare il wire o la DDL — che e una domanda del contratto e va
    /// decisa dove il campo e definito.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per colonne senza nome utilizzabile, decimal oltre
    /// `Decimal128` e tipi wire non ancora qualificati.
    fn wire_column_spec(&self, column: &Column) -> Result<MysqlColumnSpec>;

    /// Se un tipo dichiarato nel catalogo e una geometria.
    fn is_spatial_native_type(&self, native_type: &str) -> bool;

    /// Se una colonna spatial senza SRID dichiarato va rifiutata.
    ///
    /// Su `MySQL` l'SRID arriva da `SRS_ID`, che il catalogo seleziona. Un
    /// prodotto che non lo pubblicasse non renderebbe la colonna priva di
    /// CRS: renderebbe impossibile saperlo, ed e una differenza che la
    /// decisione deve poter esprimere invece di ereditarla.
    fn spatial_requires_declared_srid(&self) -> bool;

    /// Come si proietta una colonna geometrica per ottenerne il WKB.
    fn geometry_projection(&self, quoted: &str) -> String;

    /// Se il WKB uscito da quella proiezione non e quello atteso.
    ///
    /// Sta accanto alla proiezione perche ne e la conseguenza: chi sceglie la
    /// funzione sceglie anche quale dialetto ne esce. Separarli lascerebbe un
    /// controllo che valida l'output di una funzione che non conosce.
    fn geometry_output_is_unexpected(&self, srid: Option<u32>, dimensions: &str) -> bool;
}

/// Il profilo di `MySQL`, l'unico prodotto che il crate serve oggi.
///
/// La versione di riferimento non e scritta qui: la fissa
/// `docker/mysql/references.json`, per digest, ed e il gate a verificarla.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MysqlProfile;

/// Il `ProviderKind` con cui firma chi non ha ancora un prodotto sotto.
///
/// Rendering dell'AST, mappatura dei tipi, binding dei parametri e
/// validazione della configurazione avvengono prima di qualunque connessione,
/// e spesso dentro funzioni pure a cui un profilo non arriva se non
/// attraversando decine di firme che non lo userebbero per altro.
///
/// Questa costante e percio un **segnaposto, non un'attribuzione**: ogni
/// errore che lascia il crate passa da un bordo che lo ristampa con il kind
/// del profilo effettivo (`attributed`). Il valore che il chiamante osserva
/// e sempre quello del provider che ha ricevuto la chiamata, mai questo.
///
/// Cio che rende sicura la scorciatoia e il bordo, non la costante: una
/// guardia verifica che i metodi di `Provider` timbrino tutti.
pub(crate) const PROVISIONAL_KIND: ProviderKind = ProviderKind::Mysql;

/// Ristampa l'attribuzione di un esito con il prodotto che lo ha davvero
/// generato.
///
/// Si applica al bordo, dove il profilo c'e: da li in poi nessun segnaposto
/// sopravvive.
pub(crate) fn attributed<T>(
    profile: &dyn ProductProfile,
    result: plenora_database_core::Result<T>,
) -> plenora_database_core::Result<T> {
    result.map_err(|mut error| {
        error.provider = Some(profile.kind());
        error
    })
}

/// L'unica istanza: il profilo e senza stato, costruirne altre non ha senso.
pub(crate) static MYSQL_PROFILE: MysqlProfile = MysqlProfile;

impl ProductProfile for MysqlProfile {
    fn product(&self) -> &'static str {
        "MySQL"
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::Mysql
    }

    fn foreign_product_rejection(
        &self,
        product_version: &str,
        version_comment: &str,
    ) -> Option<DatabaseError> {
        // Fix P1 review MySQL 2026-08-15: fail-closed su MariaDB.
        // MariaDB non è testato né qualificato: differenze rilevanti su
        // sequenze, INSERT ... ON DUPLICATE KEY, MERGE syntax, spatial
        // (GEOMETRYCOLLECTION), pool prepared statement cache, e
        // isolation semantics. Il consumer che dichiara MariaDB usa il
        // fork sbagliato — meglio errore chiaro alla probe che silenti
        // divergenze in produzione.
        //
        // Il riconoscimento e per stringa perche e cio che il server
        // espone: `VERSION()` e `@@version_comment`. ADR 0014 ha misurato
        // che entrambe portano "mariadb" su tutti e tre i riferimenti.
        let looks_like_mariadb = product_version.to_ascii_lowercase().contains("mariadb")
            || version_comment.to_ascii_lowercase().contains("mariadb");
        if !looks_like_mariadb {
            return None;
        }
        Some(DatabaseError {
            category: ErrorCategory::Unsupported,
            phase: ErrorPhase::Probe,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(self.kind()),
            execution_id: None,
            message: format!(
                "MariaDB rilevato (product_version={product_version:?}, \
                 version_comment={version_comment:?}) — provider `{}` non \
                 qualificato per MariaDB. Un provider dedicato è in roadmap.",
                self.product().to_ascii_lowercase()
            ),
            diagnostics: None,
        })
    }

    fn statement_timeout_statement(&self, timeout_ms: u64) -> String {
        // `MAX_EXECUTION_TIME` e session-scoped e si misura in millisecondi,
        // la stessa unita del contratto: qui la conversione e l'identita, e
        // dichiararlo serve a non farla sparire quando smettera di esserlo.
        format!("SET SESSION MAX_EXECUTION_TIME = {timeout_ms}")
    }

    fn schemas_query(&self) -> &'static str {
        "SELECT SCHEMA_NAME AS schema_name \
        FROM information_schema.schemata \
        WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
        ORDER BY SCHEMA_NAME"
    }

    fn objects_query(&self) -> &'static str {
        "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, \
        TABLE_TYPE AS table_type, ENGINE AS engine \
        FROM information_schema.tables WHERE TABLE_SCHEMA = ? \
        ORDER BY TABLE_NAME"
    }

    fn object_query(&self) -> &'static str {
        "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, \
        TABLE_TYPE AS table_type, ENGINE AS engine \
        FROM information_schema.tables \
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    }

    fn object_columns_query(&self) -> &'static str {
        "SELECT COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position, \
        DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, \
        IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, \
        CHARACTER_SET_NAME AS character_set_name, COLLATION_NAME AS collation_name, \
        NUMERIC_PRECISION AS numeric_precision, NUMERIC_SCALE AS numeric_scale, \
        DATETIME_PRECISION AS datetime_precision, SRS_ID AS srs_id, \
        EXTRA AS extra, GENERATION_EXPRESSION AS generation_expression \
        FROM information_schema.columns \
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
    }

    fn object_indexes_query(&self) -> &'static str {
        "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, \
        SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, \
        EXPRESSION AS expression \
        FROM information_schema.statistics \
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
        ORDER BY INDEX_NAME, SEQ_IN_INDEX"
    }

    #[allow(clippy::too_many_lines)]
    fn wire_column_spec(&self, column: &Column) -> Result<MysqlColumnSpec> {
        let name = column.name_str().into_owned();
        if name.is_empty() || name.contains('\0') {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "colonna di output MySQL senza nome utilizzabile",
            ));
        }
        let flags = column.flags();
        let unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
        let binary = column.character_set() == BINARY_CHARACTER_SET;
        let (kind, native_type) = match column.column_type() {
            ColumnType::MYSQL_TYPE_TINY => (tiny_kind(column, unsigned), "tinyint"),
            ColumnType::MYSQL_TYPE_SHORT => (
                if unsigned {
                    MysqlColumnKind::U16
                } else {
                    MysqlColumnKind::I16
                },
                "smallint",
            ),
            ColumnType::MYSQL_TYPE_YEAR => (MysqlColumnKind::I16, "year"),
            ColumnType::MYSQL_TYPE_INT24 => (
                if unsigned {
                    MysqlColumnKind::U32
                } else {
                    MysqlColumnKind::I32
                },
                "mediumint",
            ),
            ColumnType::MYSQL_TYPE_LONG => (
                if unsigned {
                    MysqlColumnKind::U32
                } else {
                    MysqlColumnKind::I32
                },
                "int",
            ),
            ColumnType::MYSQL_TYPE_LONGLONG => (
                if unsigned {
                    MysqlColumnKind::U64
                } else {
                    MysqlColumnKind::I64
                },
                "bigint",
            ),
            ColumnType::MYSQL_TYPE_FLOAT => (MysqlColumnKind::F32, "float"),
            ColumnType::MYSQL_TYPE_DOUBLE => (MysqlColumnKind::F64, "double"),
            ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                (decimal_kind(column, unsigned)?, "decimal")
            }
            ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
                (MysqlColumnKind::Date, "date")
            }
            ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
                (MysqlColumnKind::Time, "time")
            }
            ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => {
                (MysqlColumnKind::Timestamp, "datetime")
            }
            ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
                (MysqlColumnKind::Timestamp, "timestamp")
            }
            ColumnType::MYSQL_TYPE_JSON => (MysqlColumnKind::Utf8, "json"),
            ColumnType::MYSQL_TYPE_BIT => (MysqlColumnKind::Binary, "bit"),
            ColumnType::MYSQL_TYPE_ENUM => (MysqlColumnKind::Utf8, "enum"),
            ColumnType::MYSQL_TYPE_SET => (MysqlColumnKind::Utf8, "set"),
            ColumnType::MYSQL_TYPE_STRING => text_kind(binary, flags, "char", "binary"),
            ColumnType::MYSQL_TYPE_VARCHAR | ColumnType::MYSQL_TYPE_VAR_STRING => {
                text_kind(binary, flags, "varchar", "varbinary")
            }
            ColumnType::MYSQL_TYPE_TINY_BLOB => text_kind(binary, flags, "tinytext", "tinyblob"),
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB => {
                text_kind(binary, flags, "mediumtext", "mediumblob")
            }
            ColumnType::MYSQL_TYPE_LONG_BLOB => text_kind(binary, flags, "longtext", "longblob"),
            ColumnType::MYSQL_TYPE_BLOB => text_kind(binary, flags, "text", "blob"),
            // Una geometria in uscita da una query non porta SRID ne profilo
            // dimensionale dimostrati e il renderer non incapsula la colonna in
            // ST_AsBinary: senza quel preflight il contratto GeoArrow sarebbe una
            // dichiarazione non verificata.
            ColumnType::MYSQL_TYPE_GEOMETRY => {
                return Err(unsupported(
                    "geometria MySQL nel path query richiede il preflight SRID non ancora qualificato",
                ));
            }
            other => {
                return Err(unsupported(format!(
                    "tipo wire MySQL non qualificato nel result set: {other:?}"
                )));
            }
        };
        Ok(MysqlColumnSpec {
            name,
            // COM_STMT_PREPARE descrive il tipo wire, ma non conserva sempre la
            // dichiarazione SQL originale (lunghezza caratteri, FSP, collation o
            // tipo dell'espressione). Una stringa vuota fa omettere il metadato
            // dichiarativo invece di pubblicarne uno ricostruito e non fedele.
            native_declaration: String::new(),
            native_type: native_type.to_owned(),
            nullable: !flags.contains(ColumnFlags::NOT_NULL_FLAG),
            collation: None,
            kind,
            spatial_srid: None,
        })
    }

    fn is_spatial_native_type(&self, native_type: &str) -> bool {
        matches!(
            native_type,
            "geometry"
                | "point"
                | "linestring"
                | "polygon"
                | "multipoint"
                | "multilinestring"
                | "multipolygon"
                | "geometrycollection"
                | "geomcollection"
        )
    }

    fn spatial_requires_declared_srid(&self) -> bool {
        true
    }

    fn geometry_projection(&self, quoted: &str) -> String {
        format!("ST_AsBinary({quoted}) AS {quoted}")
    }

    fn geometry_output_is_unexpected(&self, srid: Option<u32>, dimensions: &str) -> bool {
        // `ST_AsBinary` di `MySQL` produce WKB senza SRID incapsulato e solo
        // XY: entrambe le cose sono cio che il contratto GeoArrow pubblicato
        // dichiara, e una violazione e un difetto, non una variante.
        srid.is_some() || dimensions != "xy"
    }

    fn reports_functional_index_parts(&self) -> bool {
        // MySQL 8.0+ popola `EXPRESSION` per le parti funzionali, e la query
        // sopra la seleziona: le due affermazioni devono restare vere
        // insieme, ed e cio che una guardia verifica.
        true
    }
}

/// `MySQL` non distingue `tinyint(1)` da `tinyint` nel tipo wire: l'unico
/// segnale e la larghezza dichiarata, la stessa usata dal path catalogo.
fn tiny_kind(column: &Column, unsigned: bool) -> MysqlColumnKind {
    if unsigned {
        MysqlColumnKind::U8
    } else if column.column_length() == 1 {
        MysqlColumnKind::Bool
    } else {
        MysqlColumnKind::I8
    }
}

const fn text_kind(
    binary: bool,
    flags: ColumnFlags,
    text: &'static str,
    blob: &'static str,
) -> (MysqlColumnKind, &'static str) {
    if binary {
        (MysqlColumnKind::Binary, blob)
    } else if flags.contains(ColumnFlags::ENUM_FLAG) {
        (MysqlColumnKind::Utf8, "enum")
    } else if flags.contains(ColumnFlags::SET_FLAG) {
        (MysqlColumnKind::Utf8, "set")
    } else {
        (MysqlColumnKind::Utf8, text)
    }
}

/// Ricostruisce precisione e scala dal solo `column_length` del protocollo.
///
/// `MySQL` pubblica la lunghezza di visualizzazione, cioe la precisione piu un
/// carattere per il segno quando la colonna e signed e uno per il separatore
/// decimale quando la scala e maggiore di zero.
fn decimal_kind(column: &Column, unsigned: bool) -> Result<MysqlColumnKind> {
    let scale = i8::try_from(column.decimals()).map_err(|_| {
        prepare_error(
            ErrorCategory::Unsupported,
            "scala decimal MySQL non rappresentabile",
        )
    })?;
    let separators = u32::from(!unsigned) + u32::from(scale > 0);
    let precision = column
        .column_length()
        .checked_sub(separators)
        .and_then(|value| u8::try_from(value).ok())
        .ok_or_else(|| {
            prepare_error(
                ErrorCategory::Unsupported,
                "precisione decimal MySQL non ricostruibile dai metadati",
            )
        })?;
    if precision == 0 || precision > 38 || scale < 0 || scale > precision.cast_signed() {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "decimal MySQL oltre Decimal128 Arrow",
        ));
    }
    Ok(MysqlColumnKind::Decimal { precision, scale })
}

#[cfg(test)]
mod tests {
    use super::{ProductProfile, MYSQL_PROFILE};
    use crate::types::MysqlColumnKind;
    use mysql_async::consts::ColumnType;
    use mysql_async::Column;
    use plenora_database_core::{plan::ProviderKind, ErrorCategory, ErrorPhase};

    #[test]
    fn the_profile_accepts_mysql_and_rejects_mariadb_from_either_string() {
        assert!(MYSQL_PROFILE
            .foreign_product_rejection("9.7.2", "MySQL Community Server - GPL")
            .is_none());
        // Il riferimento MariaDB 12.3.2 porta il marchio in `VERSION()`; la
        // 11.8.8 lo porta anche nel commento. Entrambe le letture bastano da
        // sole, e nessuna delle due si puo dare per scontata.
        for (version, comment) in [
            ("12.3.2-MariaDB", "MySQL Community Server - GPL"),
            ("11.8.8", "mariadb.org binary distribution"),
            ("12.3.2-MariaDB", "mariadb.org binary distribution"),
        ] {
            let rejection = MYSQL_PROFILE
                .foreign_product_rejection(version, comment)
                .unwrap_or_else(|| panic!("{version} / {comment} doveva essere rifiutato"));
            assert_eq!(rejection.category, ErrorCategory::Unsupported);
            assert_eq!(rejection.phase, ErrorPhase::Probe);
            assert_eq!(rejection.provider, Some(ProviderKind::Mysql));
            assert!(rejection.message.contains("non qualificato per MariaDB"));
            assert!(rejection.message.contains(version));
        }
    }

    #[test]
    fn the_statement_timeout_keeps_the_contract_unit() {
        // Il contratto parla in millisecondi e MySQL li accetta tali quali:
        // il numero che finisce nello statement e lo stesso che entra. Un
        // profilo che convertisse in secondi produrrebbe `5`, e questa
        // asserzione e cio che lo distingue.
        assert_eq!(
            MYSQL_PROFILE.statement_timeout_statement(5_000),
            "SET SESSION MAX_EXECUTION_TIME = 5000"
        );
        assert_eq!(
            MYSQL_PROFILE.statement_timeout_statement(1),
            "SET SESSION MAX_EXECUTION_TIME = 1"
        );
    }

    #[test]
    fn no_other_module_writes_the_timeout_statement() {
        // La transazione emette il timeout ma non lo compone piu. Se il nome
        // della variabile tornasse a comparire li, un secondo profilo lo
        // cambierebbe in un posto solo e l'altro resterebbe MySQL.
        let variable = format!("MAX_EXECUTION{}TIME", "_");
        assert!(!include_str!("transaction.rs").contains(variable.as_str()));
    }

    #[test]
    fn the_catalog_is_queried_only_through_the_profile() {
        // Una query rimasta nel catalogo sarebbe una query che un secondo
        // profilo non puo cambiare: verrebbe eseguita comunque, e fallirebbe
        // sul prodotto sbagliato invece di essere sostituita.
        let source = format!("FROM information{}schema", "_");
        assert!(!include_str!("catalog.rs").contains(source.as_str()));
    }

    #[test]
    fn the_functional_index_flag_matches_the_query_that_supports_it() {
        // Il flag promette che le parti funzionali si vedono; a mostrarle e
        // la colonna selezionata dalla query. Separati, uno dei due mente.
        assert_eq!(
            MYSQL_PROFILE.reports_functional_index_parts(),
            MYSQL_PROFILE.object_indexes_query().contains("EXPRESSION")
        );
    }

    #[test]
    fn the_query_module_no_longer_maps_wire_types() {
        // La mappatura vive nel profilo. Se `query.rs` tornasse a nominare i
        // tipi del protocollo fuori dai propri test, esisterebbero due
        // produzioni di `MYSQL_NATIVE_TYPE` e un secondo profilo ne
        // cambierebbe una sola.
        let source = include_str!("query.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("query.rs ha un modulo di test")
            .0;
        let wire = format!("ColumnType::MYSQL{}TYPE", "_");
        assert!(!production.contains(wire.as_str()));
    }

    #[test]
    fn the_wire_produces_the_native_type_that_diverged() {
        // Il caso che ADR 0014 ha misurato: dalla stessa DDL `document JSON`
        // MySQL manda MYSQL_TYPE_JSON e MariaDB MYSQL_TYPE_BLOB. Il nome che
        // ne esce e cio che finisce nei metadata Arrow, ed e questo il valore
        // che un secondo profilo dovra decidere di nuovo.
        let json = Column::new(ColumnType::MYSQL_TYPE_JSON)
            .with_name(b"document")
            .with_character_set(255);
        let spec = MYSQL_PROFILE.wire_column_spec(&json).expect("json");
        assert_eq!(spec.native_type, "json");
        assert_eq!(spec.kind, MysqlColumnKind::Utf8);

        let blob = Column::new(ColumnType::MYSQL_TYPE_BLOB)
            .with_name(b"document")
            .with_character_set(255);
        let spec = MYSQL_PROFILE.wire_column_spec(&blob).expect("blob");
        assert_eq!(spec.native_type, "text");
    }

    #[test]
    fn the_spatial_types_carry_the_srid_rule_that_qualifies_them() {
        for spatial in [
            "geometry",
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
            "geomcollection",
        ] {
            assert!(MYSQL_PROFILE.is_spatial_native_type(spatial), "{spatial}");
        }
        for scalar in ["blob", "json", "text", "geo", "geometryx", ""] {
            assert!(!MYSQL_PROFILE.is_spatial_native_type(scalar), "{scalar}");
        }
        // La regola che rende qualificata una colonna spatial: senza SRID
        // dichiarato si rifiuta. Un profilo che la spegnesse pubblicherebbe
        // geometrie con CRS ignoto, ed e la ragione per cui e una decisione
        // e non una costante.
        assert!(MYSQL_PROFILE.spatial_requires_declared_srid());
    }

    #[test]
    fn the_expected_wkb_matches_the_projection_that_produces_it() {
        assert_eq!(
            MYSQL_PROFILE.geometry_projection("`geom`"),
            "ST_AsBinary(`geom`) AS `geom`"
        );
        // Cio che quella funzione produce: XY, nessun SRID incapsulato.
        assert!(!MYSQL_PROFILE.geometry_output_is_unexpected(None, "xy"));
        assert!(MYSQL_PROFILE.geometry_output_is_unexpected(Some(4_326), "xy"));
        assert!(MYSQL_PROFILE.geometry_output_is_unexpected(None, "xyz"));
    }

    #[test]
    fn no_other_module_writes_the_geometry_projection() {
        // La proiezione e l'attesa sul suo output sono due meta della stessa
        // decisione. Se `types.rs` tornasse a scrivere la funzione, un
        // secondo profilo ne cambierebbe una sola, e il controllo in lettura
        // validerebbe l'output di una funzione che non ha scelto.
        let production = include_str!("types.rs")
            .split_once("#[cfg(test)]")
            .expect("types.rs ha un modulo di test")
            .0;
        let function = format!("ST{}AsBinary", "_");
        assert!(!production.contains(function.as_str()));
    }

    #[test]
    fn the_catalog_derived_specs_always_carry_a_profile() {
        // La forma pubblica di `from_catalog` ricade sul profilo statico. In
        // produzione deve restare solo la sua definizione: un consumatore che
        // la chiamasse validerebbe il target di un secondo prodotto con le
        // regole di questo, ed e esattamente cio che il preflight di
        // scrittura faceva.
        let needle = format!("from{}catalog(", "_");
        for (module, source, allowed) in [
            ("write.rs", include_str!("write.rs"), 0),
            ("read.rs", include_str!("read.rs"), 0),
            ("types.rs", include_str!("types.rs"), 1),
        ] {
            let production = source
                .split_once("#[cfg(test)]")
                .map_or(source, |(head, _)| head);
            assert_eq!(
                production.matches(needle.as_str()).count(),
                allowed,
                "{module} usa la forma senza profilo"
            );
        }
    }

    #[test]
    fn no_module_signs_an_error_with_a_hardcoded_product() {
        // Il segnaposto ha un nome perche sia greppabile e perche il bordo lo
        // ristampi. Un literal scritto a mano non lo sarebbe: sopravvivrebbe
        // al bordo solo se qualcuno lo mettesse dove il bordo non passa, ed e
        // la ragione per cui qui non ne deve restare nessuno.
        let literal = format!("ProviderKind::{}sql", "My");
        // Il confine e il modulo di test. Composto a runtime perche un
        // literal multilinea qui dentro e fragile da leggere e da mantenere.
        // Il confine e l'apertura del modulo di test. Il marker comincia
        // con un a capo e non con cio che lo precede: cosi aggancia sia
        // un file LF sia uno CRLF, e le guardie non dipendono da come il
        // working tree e stato scritto.
        let marker = format!("{}mod tests {{", '\n');
        for (module, source) in [
            ("arrow.rs", include_str!("arrow.rs")),
            ("catalog.rs", include_str!("catalog.rs")),
            ("config.rs", include_str!("config.rs")),
            ("error.rs", include_str!("error.rs")),
            ("parameter.rs", include_str!("parameter.rs")),
            ("pool.rs", include_str!("pool.rs")),
            ("provider.rs", include_str!("provider.rs")),
            ("query.rs", include_str!("query.rs")),
            ("read.rs", include_str!("read.rs")),
            ("row_diagnostics.rs", include_str!("row_diagnostics.rs")),
            ("session.rs", include_str!("session.rs")),
            ("transaction.rs", include_str!("transaction.rs")),
            ("types.rs", include_str!("types.rs")),
            ("write.rs", include_str!("write.rs")),
        ] {
            let production = source
                .split_once(marker.as_str())
                .map_or(source, |(head, _)| head);
            assert_eq!(
                production.matches(literal.as_str()).count(),
                0,
                "{module} firma un errore con il prodotto cablato"
            );
        }
    }

    #[test]
    fn every_boundary_that_returns_a_future_restamps_the_attribution() {
        // Il segnaposto e sicuro solo perche il bordo lo copre. Un metodo di
        // `Provider` che restituisse un futuro senza timbrare lascerebbe
        // uscire l'attribuzione con cui l'errore e nato, e nessuna delle
        // guardie sopra se ne accorgerebbe.
        let source = include_str!("provider.rs");
        let start = source
            .find("impl Provider for MysqlProvider {")
            .expect("l'impl del trait deve esistere");
        let end = source[start..]
            .find(format!("{}}}", '\n').as_str())
            .map_or(source.len(), |at| start + at);
        let block = &source[start..end];
        let boxed = format!("Box::{}(", "pin");
        let stamped = format!("crate::profile::{}(", "attributed");
        assert_eq!(
            block.matches(boxed.as_str()).count(),
            block.matches(stamped.as_str()).count(),
            "ogni futuro restituito da Provider deve passare dal bordo"
        );
        assert!(block.matches(boxed.as_str()).count() >= 8);
    }

    #[test]
    fn the_profile_names_the_product_it_serves() {
        assert_eq!(MYSQL_PROFILE.product(), "MySQL");
        assert_eq!(MYSQL_PROFILE.kind(), ProviderKind::Mysql);
    }
}
