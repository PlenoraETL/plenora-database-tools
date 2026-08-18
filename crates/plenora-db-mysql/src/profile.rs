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
//!   e senza stato, quindi il costo e una chiamata indiretta non inlineabile.
//!   Quasi tutte le decisioni si prendono una volta per statement, ma non
//!   tutte: `geometry_output_is_unexpected` viene interrogato per **ogni
//!   cella spatial** letta. Li la chiamata indiretta e comunque trascurabile
//!   accanto all'ispezione EWKB che la precede, che percorre la geometria —
//!   ed e la ragione per cui la forma resta questa, non il fatto che la
//!   frequenza sia bassa.
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
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::{
    plan::ProviderKind, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use std::collections::BTreeMap;

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

    /// Le capability e i limiti che il prodotto pubblica.
    ///
    /// Non e una tabella di comodo: e il contratto su cui il consumatore
    /// decide cosa puo chiedere. Cablata nel provider, un secondo prodotto la
    /// ereditava intera — dichiarando qualificate spatial e write mode che
    /// nessuna evidenza aveva provato su di lui. ADR 0010 e 0014 dicono il
    /// contrario: una capability si dichiara dopo la prova, per prodotto.
    ///
    /// `provider_version` arriva dalla probe: e l'unica parte che il profilo
    /// non decide, perche la dice il server.
    fn capabilities(&self, provider_version: String) -> ProviderCapabilities;

    /// Se il prodotto qualifica la **scrittura** di geometrie.
    ///
    /// Separata dalla lettura perche sono due prove diverse: leggere un WKB
    /// prodotto dal server non dice nulla su cosa il server accetti in
    /// ingresso. Un profilo senza evidenza di scrittura spatial risponde
    /// `false`, e il piano si chiude in compilazione invece di scoprirlo al
    /// primo INSERT.
    fn write_spatial_is_qualified(&self) -> bool;

    /// Se un tipo geometrico e scrivibile come dichiarazione `exact`.
    ///
    /// E un insieme piu stretto di quello letto: `geometry` e
    /// `geomcollection` non compaiono, perche una dichiarazione `exact` che
    /// nomina il tipo generico non e esatta.
    fn writable_geometry_type(&self, name: &str) -> bool;

    /// Cosa significa un codice di errore del server.
    ///
    /// I codici sono superficie di prodotto: ADR 0014 ne ha misurati due che
    /// divergono gia oggi (1193 e 1054 su `MariaDB`). Ereditare questa tabella
    /// significherebbe classificare come "colonna non valida" un errore che
    /// sull'altro prodotto vuol dire altro — e la classificazione decide se
    /// il chiamante puo ritentare.
    fn classify_server_code(&self, code: u16) -> (ErrorCategory, RetryDisposition, String);

    /// Le cause di rifiuto per riga che il prodotto riconosce.
    ///
    /// La classificazione legge **solo il codice**. Il testo del messaggio e
    /// vendor, localizzato e trasporta valori di riga: interpretarlo
    /// significherebbe pubblicare come certo cio che e una congettura, e il
    /// contratto `plenora-row-diagnostics-v1` lo vieta.
    fn row_rejection_cause(&self, code: u16) -> Option<&'static str>;
}

/// Il profilo di `MySQL`, l'unico prodotto che il crate serve oggi.
///
/// La versione di riferimento non e scritta qui: la fissa
/// `docker/mysql/references.json`, per digest, ed e il gate a verificarla.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MysqlProfile;

/// Gli alias che le interrogazioni del catalogo devono esporre.
///
/// Il profilo restituisce SQL libero, ma `catalog.rs` legge le righe **per
/// nome**: una query che non esponesse `srs_id` compilerebbe e fallirebbe al
/// primo oggetto descritto, a runtime, su un errore che parla di una colonna
/// mancante invece che di un profilo incompleto. Il contratto appartiene
/// percio al profilo, dove le query vivono.
///
/// Un prodotto che la colonna non ce l'ha — ADR 0014 ne ha misurati due,
/// `SRS_ID` ed `EXPRESSION` su `MariaDB` — deve scrivere `NULL AS srs_id`, non
/// ometterla. Non e una formalita: assente significherebbe "non misurato", e
/// il lettore non avrebbe modo di distinguerlo da "nessun SRID dichiarato".
/// Sono le due cose che l'evidenza tiene separate ovunque, e qui e il punto
/// in cui si confonderebbero.
///
/// Le quattro liste esistono solo nei test, ed e una scelta: in produzione
/// duplicherebbero le stringhe che le query gia contengono, e due copie della
/// stessa verita divergono. A tenerle vere sono le guardie, che confrontano
/// il contratto con le query da una parte e con cio che il catalogo legge
/// dall'altra — le uniche due direzioni in cui puo rompersi.
#[cfg(test)]
pub(crate) const SCHEMA_ALIASES: &[&str] = &["schema_name"];

/// Vedi [`SCHEMA_ALIASES`]. Vale per gli oggetti di uno schema e per il
/// singolo oggetto: le due query rispondono alla stessa domanda con filtri
/// diversi, e chi le legge non distingue.
#[cfg(test)]
pub(crate) const OBJECT_ALIASES: &[&str] = &["table_schema", "table_name", "table_type", "engine"];

/// Vedi [`SCHEMA_ALIASES`]. `srs_id` e qui: e la colonna su cui poggia la
/// strategia spatial, ed e la prima che un secondo profilo dovra dichiarare
/// nulla invece che assente.
#[cfg(test)]
pub(crate) const COLUMN_ALIASES: &[&str] = &[
    "column_name",
    "ordinal_position",
    "data_type",
    "column_type",
    "is_nullable",
    "column_default",
    "character_set_name",
    "collation_name",
    "numeric_precision",
    "numeric_scale",
    "datetime_precision",
    "srs_id",
    "extra",
    "generation_expression",
];

/// Vedi [`SCHEMA_ALIASES`]. `seq_in_index` non viene letto ma ordina le
/// parti, e `expression` e l'altra colonna che su `MariaDB` non esiste.
#[cfg(test)]
pub(crate) const INDEX_PART_ALIASES: &[&str] = &[
    "index_name",
    "non_unique",
    "seq_in_index",
    "column_name",
    "expression",
];

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
pub(crate) fn attributed_kind<T>(
    kind: ProviderKind,
    result: plenora_database_core::Result<T>,
) -> plenora_database_core::Result<T> {
    result.map_err(|mut error| {
        error.provider = Some(kind);
        error
    })
}

/// Come [`attributed_kind`], quando il profilo e a portata di mano.
pub(crate) fn attributed<T>(
    profile: &dyn ProductProfile,
    result: plenora_database_core::Result<T>,
) -> plenora_database_core::Result<T> {
    attributed_kind(profile.kind(), result)
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
        // Fail-closed su MariaDB, dal fix P1 del 2026-08-15. La lista di
        // divergenze che accompagnava quel fix era una review, non una
        // misura, e ADR 0014 l'ha smentita su piu punti di quanti ne abbia
        // confermati. Non la si ripete qui: le divergenze misurate stanno in
        // `docs/mariadb/EVIDENCE.md`, con il server e il digest su cui sono
        // state osservate.
        //
        // Cio che regge il rifiuto non e la lista: e che il provider non e
        // qualificato per MariaDB, e una capability non qualificata si
        // dichiara chiusa. Meglio un errore alla probe che divergenze
        // silenziose in produzione — a maggior ragione ora che sappiamo
        // quali sono, e che non sono quelle che si credeva.
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

    fn capabilities(&self, provider_version: String) -> ProviderCapabilities {
        ProviderCapabilities {
            schema_version: 1,
            provider: self.kind(),
            provider_version,
            extension_versions: BTreeMap::new(),
            reads: ReadCapabilities {
                streaming: true,
                server_cursor: false,
                pagination: false,
                object_id_windows: false,
                projection: true,
                filter: true,
                ordering: true,
                resumable: false,
            },
            // Sei mode qualificate su sette. `TruncateInsert` resta
            // fail-closed e non ha un flag proprio nel contratto: il
            // consumer che la chiede riceve `Unsupported` in prepare, con
            // il rinvio a `Replace` nel messaggio.
            //
            // `rollback_on_failure = true` con `transactional_ddl =
            // false` e la combinazione documentata in
            // `WriteCapabilities::rollback_on_failure`: le righe tornano
            // sempre indietro, la tabella creata da `Create` no. Le altre
            // cinque mode non emettono DDL, quindi per loro il rollback e
            // pieno in entrambi i sensi.
            writes: WriteCapabilities {
                create: true,
                append: true,
                update: true,
                upsert: true,
                replace: true,
                delete_by_keys: true,
                bulk: true,
                array_binding: false,
                returning: false,
                apply_edits: false,
                rollback_on_failure: true,
                use_global_ids: false,
            },
            transactions: TransactionCapabilities {
                single_transaction: true,
                savepoints: true,
                transactional_ddl: false,
                staged_swap: false,
                scope: TransactionScope::Transaction,
            },
            spatial: mysql_spatial_capabilities(),
            limits: ProviderLimits {
                // Il contratto esprime il limite in **byte**, il prodotto in
                // caratteri: `MAX_IDENTIFIER_CHARACTERS` sono 64 caratteri
                // Unicode, che in utf8mb4 arrivano a 256 byte. Pubblicare 64
                // dichiarerebbe un limite quattro volte piu stretto del vero
                // e farebbe rifiutare al consumatore identificatori che il
                // server accetta. Finche il contratto non esprime i
                // caratteri, l'unica risposta onesta e "non dichiarato".
                max_identifier_bytes: None,
                max_bind_parameters: Some(crate::MAX_BIND_PARAMETERS as u64),
                max_statement_bytes: None,
                max_batch_rows: Some(crate::MAX_BATCH_ROWS as u64),
                max_payload_bytes: None,
                max_record_count: None,
            },
        }
    }

    fn classify_server_code(&self, code: u16) -> (ErrorCategory, RetryDisposition, String) {
        let product = self.product();
        match code {
            1_045 => (
                ErrorCategory::Authentication,
                RetryDisposition::Never,
                format!("autenticazione {product} rifiutata (codice 1045)"),
            ),
            1_044 => (
                ErrorCategory::Authorization,
                RetryDisposition::Never,
                format!("autorizzazione {product} negata (codice 1044)"),
            ),
            1_049 | 1_146 => (
                ErrorCategory::NotFound,
                RetryDisposition::Never,
                format!("database o oggetto {product} non trovato (codice {code})"),
            ),
            1_054 => (
                ErrorCategory::Schema,
                RetryDisposition::Never,
                format!("colonna {product} non valida (codice 1054)"),
            ),
            1_062 => (
                ErrorCategory::Conflict,
                RetryDisposition::Never,
                format!("vincolo univoco {product} violato (codice 1062)"),
            ),
            1_213 => (
                ErrorCategory::Transient,
                RetryDisposition::Safe,
                format!("deadlock {product}; transazione vittima annullata"),
            ),
            1_205 | 3_024 => (
                ErrorCategory::Timeout,
                RetryDisposition::Never,
                format!("timeout {product} (codice {code})"),
            ),
            native => (
                ErrorCategory::Execution,
                RetryDisposition::Never,
                format!("errore server {product} redatto (codice {native})"),
            ),
        }
    }

    fn row_rejection_cause(&self, code: u16) -> Option<&'static str> {
        match code {
            // 1048 NULL in colonna non nullable, 1062 chiave duplicata,
            // 1452 vincolo di integrità referenziale, 3819 CHECK violato,
            // 4025 CHECK violato sulla colonna (MySQL 8.0.16+).
            1_048 | 1_062 | 1_452 | 3_819 | 4_025 => {
                Some(plenora_database_core::row_diagnostics::CAUSE_CONSTRAINT_VIOLATION)
            }
            _ => None,
        }
    }

    fn write_spatial_is_qualified(&self) -> bool {
        true
    }

    fn writable_geometry_type(&self, name: &str) -> bool {
        crate::write::geometry_type_is_writable(name)
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

fn mysql_spatial_capabilities() -> SpatialCapabilities {
    SpatialCapabilities {
        read_wkb: true,
        write_wkb: true,
        geometry: true,
        geography: false,
        spatial_index: false,
        mixed_geometry_types: true,
        dimensions: vec![plenora_database_core::geometry::Dimensions::Xy],
        // v1.2: 20 funzioni spatial MySQL 8+ dichiarate verified via il
        // dialect condiviso `plenora-database-sql`. Vedi
        // `crate::query::VERIFIED_SPATIAL_FUNCTIONS` per la lista + rationale.
        functions: crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec(),
    }
}

/// Un secondo prodotto, esistente solo nei test.
///
/// Non e `MariadbProfile`: non decide nulla di `MariaDB` e non ne anticipa il
/// comportamento. Serve a una cosa sola — rendere **osservabile** cio che
/// altrimenti sarebbe indistinguibile, perche con un profilo solo ogni
/// attribuzione e `Mysql` e nessun test puo dire se viene dal profilo o da
/// un literal sopravvissuto. Delega tutto a `MYSQL_PROFILE` tranne
/// l'identita, che e esattamente cio che si vuole vedere cambiare.
#[cfg(test)]
pub(crate) struct SecondProductProfile;

#[cfg(test)]
pub(crate) static SECOND_PRODUCT_PROFILE: SecondProductProfile = SecondProductProfile;

#[cfg(test)]
impl ProductProfile for SecondProductProfile {
    fn product(&self) -> &'static str {
        "SecondProduct"
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::Mariadb
    }

    fn foreign_product_rejection(
        &self,
        product_version: &str,
        version_comment: &str,
    ) -> Option<DatabaseError> {
        MYSQL_PROFILE.foreign_product_rejection(product_version, version_comment)
    }

    fn statement_timeout_statement(&self, timeout_ms: u64) -> String {
        MYSQL_PROFILE.statement_timeout_statement(timeout_ms)
    }

    fn schemas_query(&self) -> &'static str {
        MYSQL_PROFILE.schemas_query()
    }

    fn objects_query(&self) -> &'static str {
        MYSQL_PROFILE.objects_query()
    }

    fn object_query(&self) -> &'static str {
        MYSQL_PROFILE.object_query()
    }

    fn object_columns_query(&self) -> &'static str {
        MYSQL_PROFILE.object_columns_query()
    }

    fn object_indexes_query(&self) -> &'static str {
        MYSQL_PROFILE.object_indexes_query()
    }

    fn reports_functional_index_parts(&self) -> bool {
        MYSQL_PROFILE.reports_functional_index_parts()
    }

    fn wire_column_spec(&self, column: &Column) -> Result<MysqlColumnSpec> {
        MYSQL_PROFILE.wire_column_spec(column)
    }

    fn is_spatial_native_type(&self, native_type: &str) -> bool {
        MYSQL_PROFILE.is_spatial_native_type(native_type)
    }

    fn spatial_requires_declared_srid(&self) -> bool {
        MYSQL_PROFILE.spatial_requires_declared_srid()
    }

    fn geometry_projection(&self, quoted: &str) -> String {
        MYSQL_PROFILE.geometry_projection(quoted)
    }

    fn geometry_output_is_unexpected(&self, srid: Option<u32>, dimensions: &str) -> bool {
        MYSQL_PROFILE.geometry_output_is_unexpected(srid, dimensions)
    }

    fn capabilities(&self, provider_version: String) -> ProviderCapabilities {
        ProviderCapabilities {
            provider: self.kind(),
            ..MYSQL_PROFILE.capabilities(provider_version)
        }
    }

    fn write_spatial_is_qualified(&self) -> bool {
        MYSQL_PROFILE.write_spatial_is_qualified()
    }

    fn writable_geometry_type(&self, name: &str) -> bool {
        MYSQL_PROFILE.writable_geometry_type(name)
    }

    fn classify_server_code(&self, code: u16) -> (ErrorCategory, RetryDisposition, String) {
        MYSQL_PROFILE.classify_server_code(code)
    }

    fn row_rejection_cause(&self, code: u16) -> Option<&'static str> {
        MYSQL_PROFILE.row_rejection_cause(code)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProductProfile, COLUMN_ALIASES, INDEX_PART_ALIASES, MYSQL_PROFILE, OBJECT_ALIASES,
        SCHEMA_ALIASES, SECOND_PRODUCT_PROFILE,
    };
    use crate::types::MysqlColumnKind;
    use crate::MysqlConfig;
    use mysql_async::consts::ColumnType;
    use mysql_async::Column;
    use plenora_database_core::plan::ObjectRef;
    use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource,
    };
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use plenora_database_core::CancellationToken;
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
    fn every_method_that_returns_a_future_restamps_the_attribution() {
        // Non un conteggio complessivo: quello lascerebbe compensare due
        // sbilanciamenti in metodi diversi. La verifica e per metodo, sui due
        // trait che restituiscono futuri — `Provider` e `TransactionScope` —
        // perche il segnaposto e sicuro solo dove il bordo lo copre.
        let boxed = format!("Box::{}(", "pin");
        let stamped = format!("crate::profile::{}", "attributed");
        for (module, source, header) in [
            (
                "provider.rs",
                include_str!("provider.rs"),
                "impl Provider for MysqlProvider {",
            ),
            (
                "transaction.rs",
                include_str!("transaction.rs"),
                "impl TransactionScope for MysqlTransaction {",
            ),
        ] {
            let start = source.find(header).expect("l'impl del trait deve esistere");
            let end = source[start..]
                .find(format!("{}}}", '\n').as_str())
                .map_or(source.len(), |at| start + at);
            let block = &source[start..end];
            let mut methods = 0;
            for method in block.split(format!("{}    fn ", '\n').as_str()).skip(1) {
                if !method.contains(boxed.as_str()) {
                    continue;
                }
                methods += 1;
                let name = method.split(['(', '<']).next().unwrap_or("?");
                assert!(
                    method.contains(stamped.as_str()),
                    "{module}::{name} restituisce un futuro senza ristampare l'attribuzione"
                );
            }
            assert!(methods >= 8, "{module}: solo {methods} metodi ispezionati");
        }
    }

    #[tokio::test]
    async fn a_second_profile_changes_what_the_caller_observes() {
        // La prova che le guardie strutturali non possono dare: con un solo
        // profilo ogni attribuzione e `Mysql`, e nessun test distingue il
        // profilo da un literal sopravvissuto. Con due, la differenza si
        // vede — e questo errore nasce nel renderer, con il segnaposto, e
        // arriva al chiamante con l'identita del provider.
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = crate::MysqlProvider::with_profile(config, 2, &SECOND_PRODUCT_PROFILE)
            .expect("provider sul secondo profilo");
        assert_eq!(provider.kind(), ProviderKind::Mariadb);

        // Un identificatore oltre il limite fallisce nel rendering, prima di
        // qualunque connessione: e il percorso che usa `PROVISIONAL_KIND`.
        // Un identificatore oltre il limite fallisce nel rendering, prima
        // di qualunque connessione: e il percorso che usa
        // `PROVISIONAL_KIND`.
        let mut operation = oversized_identifier_query();
        operation.source = Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "x".repeat(crate::MAX_IDENTIFIER_CHARACTERS + 1),
                layer_id: None,
            },
            alias: None,
        });

        let error = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &ParameterBag::default(),
                &ResourceBudget::new(ResourceLimits::default()).expect("budget"),
                &CancellationToken::new(),
            )
            .await
            .err()
            .expect("identificatore oltre il limite");
        assert_eq!(
            error.provider,
            Some(ProviderKind::Mariadb),
            "l'errore esce con l'attribuzione del provider, non con il segnaposto"
        );
    }

    #[test]
    fn the_capability_table_is_built_only_by_the_profile() {
        // Cablata nel provider, la tabella veniva ereditata intera da
        // qualunque profilo: un secondo prodotto avrebbe dichiarato
        // qualificate spatial e write mode che nessuna evidenza aveva provato
        // su di lui. Qui il provider puo solo chiederla.
        let source = include_str!("provider.rs");
        let production = source
            .split_once(format!("{}mod tests {{", '\n').as_str())
            .map_or(source, |(head, _)| head);
        for built in [
            "ProviderCapabilities",
            "SpatialCapabilities",
            "ProviderLimits",
        ] {
            let literal = format!("{built} {{");
            assert_eq!(
                production.matches(literal.as_str()).count(),
                0,
                "provider.rs costruisce {built} invece di chiederla al profilo"
            );
        }
        // E cio che il profilo pubblica resta quello di prima: sei mode su
        // sette, `TruncateInsert` fail-closed, spatial senza indice.
        let published = MYSQL_PROFILE.capabilities("9.7.2".to_owned());
        assert_eq!(published.provider_version, "9.7.2");
        assert_eq!(published.provider, MYSQL_PROFILE.kind());
        assert!(published.spatial.read_wkb && published.spatial.write_wkb);
        assert!(!published.spatial.spatial_index);
        assert_eq!(
            published.limits.max_bind_parameters,
            Some(crate::MAX_BIND_PARAMETERS as u64)
        );
    }

    #[test]
    fn every_catalog_query_exposes_the_aliases_its_reader_requires() {
        for (label, sql, aliases) in [
            ("schemi", MYSQL_PROFILE.schemas_query(), SCHEMA_ALIASES),
            ("oggetti", MYSQL_PROFILE.objects_query(), OBJECT_ALIASES),
            ("oggetto", MYSQL_PROFILE.object_query(), OBJECT_ALIASES),
            (
                "colonne",
                MYSQL_PROFILE.object_columns_query(),
                COLUMN_ALIASES,
            ),
            (
                "indici",
                MYSQL_PROFILE.object_indexes_query(),
                INDEX_PART_ALIASES,
            ),
        ] {
            for alias in aliases {
                assert!(
                    sql.contains(format!("AS {alias}").as_str()),
                    "la query {label} non espone {alias}"
                );
            }
        }
    }

    #[test]
    fn the_catalog_reads_no_alias_the_contract_does_not_declare() {
        // L'altra direzione: un alias letto e non dichiarato sarebbe un
        // requisito invisibile, che un secondo profilo scoprirebbe solo
        // fallendo. La probe resta fuori — legge variabili di sessione, non
        // il catalogo — e il taglio parte da dove il catalogo comincia.
        let source = include_str!("catalog.rs");
        let catalog = source
            .split_once("pub async fn list_schemas")
            .expect("il catalogo comincia da list_schemas")
            .1;
        let declared: Vec<&str> = SCHEMA_ALIASES
            .iter()
            .chain(OBJECT_ALIASES)
            .chain(COLUMN_ALIASES)
            .chain(INDEX_PART_ALIASES)
            .copied()
            .collect();
        let mut rest = catalog;
        while let Some((_, tail)) = rest.split_once("required(row, \"") {
            let alias = tail.split('"').next().unwrap_or_default();
            assert!(declared.contains(&alias), "alias non dichiarato: {alias}");
            rest = tail;
        }
        let mut rest = catalog;
        while let Some((_, tail)) = rest.split_once("optional(row, \"") {
            let alias = tail.split('"').next().unwrap_or_default();
            assert!(declared.contains(&alias), "alias non dichiarato: {alias}");
            rest = tail;
        }
    }

    fn oversized_identifier_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("warehouse".to_owned()),
                    object: "events".to_owned(),
                    layer_id: None,
                },
                alias: None,
            }),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                },
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
    fn no_production_path_uses_a_profileless_entry_point() {
        // Ogni forma esportata ha un gemello `_with_profile`, e la forma
        // senza esiste solo per chi il profilo non ce l'ha: chiamandola dal
        // provider si perde silenziosamente il prodotto, ed e successo due
        // volte — con il pool e con la compilazione della scrittura.
        let source = include_str!("provider.rs");
        let production = source
            .split_once(format!("{}mod tests {{", '\n').as_str())
            .map_or(source, |(head, _)| head);
        for entry in [
            "MysqlPool::new",
            "probe_server",
            "list_schemas",
            "list_objects",
            "describe_object",
            "read_operation",
            "query_operation",
            "MysqlReadPlan::compile",
            "MysqlWritePlan::compile",
            "from_catalog",
            "query_result_columns",
        ] {
            // L'ago e il nome seguito dalla parentesi: il gemello con il
            // profilo ha altro in mezzo e non aggancia.
            let literal = format!("{entry}(");
            assert_eq!(
                production.matches(literal.as_str()).count(),
                0,
                "il provider usa {entry} senza profilo"
            );
        }
    }

    #[test]
    fn the_profile_names_the_product_it_serves() {
        assert_eq!(MYSQL_PROFILE.product(), "MySQL");
        assert_eq!(MYSQL_PROFILE.kind(), ProviderKind::Mysql);
    }
}
