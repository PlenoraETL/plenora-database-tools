//! Profilo di prodotto: dove il crate decide cosa dipende dal server.
//!
//! ADR 0014 ha deciso un solo crate con `MysqlProvider` e (in seguito)
//! `MariadbProvider` pubblici e distinti, sopra un profilo **interno**
//! condiviso. Questo modulo e quel profilo. Non e API pubblica, e non deve
//! diventarlo: cio che il consumatore sceglie e il provider, non il profilo.
//!
//! Il profilo raccoglie le decisioni che l'evidenza (`docs/mariadb/EVIDENCE.md`)
//! ha misurato come divergenti fra i due prodotti — riconoscimento, timeout,
//! catalogo, metadata nativi e spatial. Il trait mantiene queste scelte
//! visibili in un solo punto per entrambi i prodotti.
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
use plenora_database_core::protocol;
use plenora_database_core::{
    plan::ProviderKind, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use std::collections::BTreeMap;

/// Collation id riservato di `MySQL` per i tipi binari.
///
/// E' l'unico segnale che distingue `BLOB` da `TEXT` e `VARBINARY` da
/// `VARCHAR`: sul filo entrambi arrivano come `Value::Bytes`, e il tipo di
/// colonna e lo stesso. Visibile al crate perche anche il decoder delle
/// transazioni deve poterlo consultare senza tentare una decodifica UTF-8.
pub(crate) const BINARY_CHARACTER_SET: u16 = 63;

/// Le tre chiavi con cui il prodotto annota una colonna nello schema Arrow.
///
/// Stanno insieme perche sono un namespace solo: pubblicarne due di un
/// prodotto e una dell'altro darebbe uno schema che dichiara due origini per
/// la stessa colonna. Il consumatore legge queste chiavi per sapere cosa
/// fosse la colonna sul server, e quale tabella di tipi applicare per
/// interpretarne il nome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataKeys {
    pub(crate) native_type: &'static str,
    pub(crate) native_declaration: &'static str,
    pub(crate) collation: &'static str,
}

/// Classificazione completa di un codice di errore del server.
///
/// L'effetto remoto fa parte del verdetto perche determina anche se il
/// chiamante debba ripulire o riconciliare lo stato.
pub(crate) struct ServerCodeVerdict {
    pub(crate) category: ErrorCategory,
    pub(crate) retry: RetryDisposition,
    pub(crate) message: String,
    /// L'effetto che il codice implica di per se. `None` significa "il
    /// codice non lo determina", non "nessun effetto".
    pub(crate) remote_effect: Option<RemoteEffect>,
}

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

    /// Le versioni del prodotto su cui il profilo e qualificato, se il profilo
    /// dichiara un limite.
    ///
    /// `foreign_product_rejection` identifica il prodotto; questo metodo
    /// stabilisce separatamente se la versione e stata misurata. Un solo
    /// riconoscimento accetterebbe qualunque server con lo stesso nome.
    ///
    /// `None` significa "nessun limite dichiarato", e non "tutte qualificate": è
    /// la policy del profilo `MySQL`, e cambiarla
    /// rifiuterebbe server che il provider serve da sempre. Dichiararlo qui,
    /// invece di lasciarlo implicito, e cio che rende visibile l'asimmetria.
    fn qualified_versions(&self) -> Option<&'static [(u32, u32)]>;

    /// Il nome della variabile che porta il livello di isolamento della
    /// sessione.
    ///
    /// I due prodotti non la chiamano allo stesso modo, e non e una sinonimia
    /// che si possa scegliere a piacere: `tx_isolation` e stata **rimossa** da
    /// `MySQL` 8.0, e `transaction_isolation` non esiste su `MariaDB` prima
    /// della 11.1. Non c'e un nome che vada bene per entrambi i prodotti, e
    /// dentro `MariaDB` ce n'e uno solo che vada bene per tutte le versioni
    /// misurate.
    ///
    /// Il nome appartiene quindi al profilo, come quello del timeout: una
    /// costante condivisa sarebbe valida per un solo prodotto.
    fn session_isolation_variable(&self) -> &'static str;

    /// Lo statement che impone il timeout di statement sulla sessione.
    ///
    /// Il contratto del core esprime il timeout in millisecondi; il server
    /// no, necessariamente. Fra i due c'e una conversione, e il punto di
    /// questo metodo e che la conversione stia dove sta anche il nome della
    /// variabile: separarli e il modo in cui un timeout di cinque secondi
    /// diventa uno di cinque millisecondi senza che nulla fallisca.
    ///
    /// ADR 0014 misura che `MAX_EXECUTION_TIME` non esiste su `MariaDB`
    /// (errore 1193), dove la variabile analoga ha un nome e un'unita diversi.
    /// Il profilo deve cambiare insieme entrambi gli aspetti.
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
    /// e da qui che esce il `native_type` sul path query — sotto la chiave
    /// che il profilo dichiara in [`ProductProfile::metadata_keys`], non
    /// sotto una fissa — e qui che ADR
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

    /// Il namespace dei metadata che il prodotto pubblica nello schema Arrow.
    ///
    /// E contratto pubblico, non decorazione: un consumer che trova
    /// `plenora.mysql.native_type` su un batch letto da `MariaDB` dovrebbe
    /// dedurre da un metadato che non lo dice quale tabella di tipi
    /// applicare — e le due tabelle divergono, `json` contro `text` dalla
    /// stessa DDL. Il profilo lo decide perche e la sola cosa che sa **chi**
    /// ha risposto.
    fn metadata_keys(&self) -> MetadataKeys;

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

    /// Le funzioni spatial che **questo** prodotto ha attraversato.
    ///
    /// Non e una comodita per la tabella delle capability: e il cancello
    /// condiviso con il renderer, cosi promessa e piano ammesso coincidono.
    fn verified_spatial_functions(
        &self,
    ) -> &'static [plenora_database_core::query::SpatialFunction];

    /// La dichiarazione DDL di una colonna geometrica con il CRS dichiarato.
    ///
    /// `MySQL` ammette `GEOMETRY SRID <n>`, che vincola la colonna: ogni valore
    /// che ci entra deve appartenere a quel sistema di riferimento, e il
    /// catalogo lo pubblica. `MariaDB` rifiuta quella forma con 1064 — misurato
    /// su entrambe le major, e non solo per `SRID`: anche `REF_SYSTEM_ID`, che
    /// la sua documentazione indica al posto suo.
    ///
    /// Il CRS non sparisce: si sposta. `ST_GeomFromWKB(?, <n>)` e accettato da
    /// entrambi i prodotti e su entrambi il valore memorizzato conserva l'SRID
    /// — anche su `MariaDB`, dove la colonna non lo porta. E' la meta che rende
    /// praticabile l'altra: la lettura verifica il CRS **valore per valore**,
    /// quindi una colonna non vincolata resta descrivibile con onesta.
    fn geometry_column_ddl(&self, srid: u32) -> String;

    /// Se l'SRID che il catalogo descrive e compatibile con quello dichiarato.
    ///
    /// Sono due domande diverse a seconda del prodotto. Dove la colonna e vincolata — `MySQL` —
    /// il catalogo porta l'SRID e deve essere **quello**: scrivere geometrie
    /// 3003 in una colonna dichiarata 4326 e un errore che il server
    /// rifiuterebbe comunque, ed e meglio dirlo in preflight.
    ///
    /// Dove la colonna non puo essere vincolata — `MariaDB` — il catalogo tace
    /// per costruzione e `None` significa "non confrontabile", non "diverso".
    fn geometry_target_srid_is_compatible(&self, catalog: Option<u32>, declared: u32) -> bool;

    /// La proiezione che rende l'SRID di **ogni valore** geometrico.
    ///
    /// Sta accanto a `geometry_projection` perche e la sua controparte: quella
    /// decide cosa esce come geometria, questa cosa serve a sapere se cio che
    /// esce appartiene al CRS che il piano ha dichiarato. Un prodotto il cui
    /// catalogo l'SRID lo sa non la usa mai — non c'e niente da verificare —
    /// ma dichiararla nel profilo la tiene dove sta la decisione, invece che
    /// in un `if` sul nome del prodotto dentro il compilatore del piano.
    fn geometry_srid_projection(&self, quoted: &str) -> String;

    /// Se il WKB uscito da quella proiezione non e quello atteso.
    ///
    /// Sta accanto alla proiezione perche ne e la conseguenza: chi sceglie la
    /// funzione sceglie anche quale dialetto ne esce. Separarli lascerebbe un
    /// controllo che valida l'output di una funzione che non conosce.
    fn geometry_output_is_unexpected(&self, srid: Option<u32>, dimensions: &str) -> bool;

    /// Le capability e i limiti che il prodotto pubblica.
    ///
    /// E il contratto su cui il consumatore decide cosa puo chiedere. ADR 0010
    /// e 0014 richiedono evidenza distinta per prodotto prima di aprire una
    /// capability.
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
    /// I codici sono superficie di prodotto: ADR 0014 misura divergenze come
    /// 1193 e 1054 su `MariaDB`. Ereditare questa tabella
    /// significherebbe classificare come "colonna non valida" un errore che
    /// sull'altro prodotto vuol dire altro — e la classificazione decide se
    /// il chiamante puo ritentare.
    fn classify_server_code(&self, code: u16) -> ServerCodeVerdict;

    /// Le cause di rifiuto per riga che il prodotto riconosce.
    ///
    /// La classificazione legge **solo il codice**. Il testo del messaggio e
    /// vendor, localizzato e trasporta valori di riga: interpretarlo
    /// significherebbe pubblicare come certo cio che e una congettura, e il
    /// contratto `plenora-row-diagnostics-v1` lo vieta.
    fn row_rejection_cause(&self, code: u16) -> Option<&'static str>;
}

/// Il profilo del prodotto `MySQL`.
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
        // MariaDB resta fail-closed quando il server non corrisponde al
        // profilo. Le divergenze misurate stanno in `docs/mariadb/EVIDENCE.md`,
        // con il server e il digest su cui sono
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
        if !looks_like_mariadb(product_version, version_comment) {
            return None;
        }
        Some(DatabaseError::new(
            ErrorCategory::Unsupported,
            ErrorPhase::Probe,
            Some(self.kind()),
            format!(
                "MariaDB rilevato (product_version={product_version:?}, \
                 version_comment={version_comment:?}) — provider `{}` non \
                 qualificato per MariaDB. Usare il provider `mariadb`.",
                self.product().to_ascii_lowercase()
            ),
        ))
    }

    fn qualified_versions(&self) -> Option<&'static [(u32, u32)]> {
        // Nessun limite dichiarato: la compatibilità MySQL non è una allowlist.
        // La matrice qualifica 9.7, 8.4 e 8.0, ma il provider non ha mai
        // rifiutato una versione diversa: trasformare quella matrice in un
        // rifiuto cambierebbe il comportamento di un provider qualificato —
        // una versione già accettata smetterebbe di connettersi — e non e una
        // decisione che si prende di passaggio mentre si aggiunge un secondo
        // profilo. Resta aperta, e visibile perche dichiarata.
        None
    }

    fn session_isolation_variable(&self) -> &'static str {
        // `tx_isolation` e stata rimossa in `MySQL` 8.0: chiederla qui
        // romperebbe ogni server che questo provider serve da sempre.
        "@@transaction_isolation"
    }

    fn statement_timeout_statement(&self, timeout_ms: u64) -> String {
        // `MAX_EXECUTION_TIME` e session-scoped e si misura in millisecondi,
        // la stessa unita del contratto: qui la conversione e l'identita, e
        // dichiararlo serve a non farla sparire quando smettera di esserlo.
        format!("SET SESSION MAX_EXECUTION_TIME = {timeout_ms}")
    }

    fn schemas_query(&self) -> &'static str {
        SCHEMAS_QUERY
    }

    fn objects_query(&self) -> &'static str {
        OBJECTS_QUERY
    }

    fn object_query(&self) -> &'static str {
        OBJECT_QUERY
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

    fn wire_column_spec(&self, column: &Column) -> Result<MysqlColumnSpec> {
        wire_column_spec_for(self.product(), column)
    }

    fn capabilities(&self, provider_version: String) -> ProviderCapabilities {
        ProviderCapabilities {
            schema_version: 2,
            provider: self.kind(),
            provider_version,
            extension_versions: BTreeMap::new(),
            reads: ReadCapabilities {
                streaming: true,
                server_cursor: false,
                // La finestra è governata dalla capability:
                // l'engine rifiuta un `row_offset` a un provider che non la
                // pubblica. Il piano di lettura la compila come `LIMIT ...
                // OFFSET n`, con il tetto del tipo quando il chiamante non ne
                // ha chiesto uno — `OFFSET` da solo non e sintassi valida su
                // questi motori.
                pagination: true,
                projection: true,
                filter: true,
                ordering: true,
                // Qualificato dal gate MySQL live: il checkpoint keyset
                // persiste, riapre una nuova sessione e riprende senza
                // duplicati ne buchi.
                resumable: true,
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
                // `TruncateInsert` e chiusa, e ora puo dirlo. Su MySQL
                // `TRUNCATE TABLE` e DDL con commit implicito: le righe
                // sparirebbero prima dell'INSERT e nessun rollback le
                // riporterebbe indietro. Il provider la rifiuta in prepare da
                // sempre; fino alla separazione di questa bandiera il
                // contratto diceva il contrario, perche `append` valeva per
                // tutt'e due.
                truncate_insert: false,
                update: true,
                upsert: true,
                replace: true,
                delete_by_keys: true,
                bulk: true,
                array_binding: false,
                returning: false,
                rollback_on_failure: true,
            },
            transactions: TransactionCapabilities {
                single_transaction: true,
                savepoints: true,
                // Le due chiuse hanno **una** ragione sola, ed e del motore.
                //
                // Ogni DDL di questo prodotto porta un commit implicito. Un
                // `CREATE TABLE` dentro una transazione non aspetta il
                // `COMMIT`: chiude quella in corso e apre la successiva, e cio
                // che stava dentro resta scritto anche se poi qualcosa
                // fallisce. `transactional_ddl` promette il contrario.
                //
                // `staged_swap` ne discende. Lo scambio pubblica una tabella
                // costruita a parte, e vale se lo scambio e **atomico con il
                // carico**: qui non lo e, perche la DDL che costruisce la
                // tabella di staging ha gia committato prima. `RENAME TABLE`
                // e atomico da solo, e non basta — l'atomicita che serve e
                // quella dell'insieme.
                //
                // Nessuna delle due si aprira: non sono lacune di misura, sono
                // il motore. PostgreSQL e SQL Server le hanno perche la loro
                // DDL e transazionale.
                transactional_ddl: false,
                // Stessa ragione della riga sopra: senza DDL transazionale
                // l'atomicita dell'insieme non c'e.
                staged_swap: false,
                scope: TransactionScope::Transaction,
            },
            // Una sola origine: la capability pubblicata **e** la decisione che
            // il piano di scrittura consulta. Dichiararle separatamente
            // permetterebbe a un profilo di negare `write_wkb` e accettare
            // comunque la compilazione spatial.
            spatial: SpatialCapabilities {
                write_wkb: self.write_spatial_is_qualified(),
                ..mysql_spatial_capabilities()
            },
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
            },
        }
    }

    fn classify_server_code(&self, code: u16) -> ServerCodeVerdict {
        classify_shared_code(self.product(), code)
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

    fn metadata_keys(&self) -> MetadataKeys {
        MetadataKeys {
            native_type: protocol::MYSQL_NATIVE_TYPE,
            native_declaration: protocol::MYSQL_NATIVE_DECLARATION,
            collation: protocol::MYSQL_COLLATION,
        }
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

    fn verified_spatial_functions(
        &self,
    ) -> &'static [plenora_database_core::query::SpatialFunction] {
        crate::query::VERIFIED_SPATIAL_FUNCTIONS
    }

    fn geometry_column_ddl(&self, srid: u32) -> String {
        // Il vincolo di colonna: questo prodotto lo ammette, e il catalogo lo
        // ripubblica in `information_schema.columns.SRS_ID`.
        format!("GEOMETRY SRID {srid}")
    }

    fn geometry_target_srid_is_compatible(&self, catalog: Option<u32>, declared: u32) -> bool {
        // La colonna e vincolata: il catalogo porta l'SRID, e deve essere
        // quello dichiarato. Una colonna senza vincolo esiste anche qui — la
        // DDL non lo impone — e li il catalogo tace: scriverci dentro senza
        // che nessuno possa confermare il CRS e cio che questo confronto
        // rifiuta.
        catalog == Some(declared)
    }

    fn geometry_srid_projection(&self, quoted: &str) -> String {
        // Senza alias: la colonna non compare in nessuno schema e si legge per
        // posizione. Un alias le darebbe un nome che qualcuno potrebbe
        // scambiare per una colonna del risultato.
        format!("ST_SRID({quoted})")
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

/// Il profilo di `MariaDB`, costruito sulle sole divergenze misurate.
///
/// Non esiste ancora un `MariadbProvider`: questo profilo non e raggiungibile
/// da nessun percorso di produzione, e non lo sara finche le superfici che
/// restano `not_measured` in `docs/mariadb/EVIDENCE.md` non saranno misurate.
/// Esiste perche le divergenze provate abbiano un posto dove vivere, e perche
/// i test differenziali possano confrontarle con quelle di `MysqlProfile`.
///
/// Cio che diverge lo dice l'evidenza, non la simmetria: identita, unita e
/// nome del timeout, due colonne di catalogo che non esistono, e le
/// conseguenze fail-closed che ne discendono. Tutto il resto — bootstrap,
/// isolamento, `START TRANSACTION`, mapper wire, proiezione geometrica,
/// tabella dei codici — e stato misurato uguale sui riferimenti e
/// resta codice condiviso. Spostarlo qui per simmetria darebbe due copie che
/// nessuna prova tiene allineate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MariadbProfile;

/// L'unica istanza: come [`MYSQL_PROFILE`], il profilo e senza stato.
pub(crate) static MARIADB_PROFILE: MariadbProfile = MariadbProfile;

impl ProductProfile for MariadbProfile {
    fn product(&self) -> &'static str {
        "MariaDB"
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::Mariadb
    }

    fn foreign_product_rejection(
        &self,
        product_version: &str,
        version_comment: &str,
    ) -> Option<DatabaseError> {
        // Speculare a quello di `MysqlProfile`, sulla stessa lettura: quel
        // profilo rifiuta cio che dice "mariadb", questo rifiuta cio che non
        // lo dice. Le due decisioni partizionano i server osservati, e una
        // guardia lo verifica sulle stringhe che ADR 0014 ha misurato.
        //
        // Che il rifiuto esista anche qui non e una formalita: un profilo
        // senza riconoscimento accetterebbe MySQL ed emetterebbe
        // `max_statement_time`, che su MySQL e una variabile sconosciuta —
        // cioe fallirebbe alla prima transazione con timeout invece che alla
        // probe, dove l'errore dice ancora cosa e successo.
        if looks_like_mariadb(product_version, version_comment) {
            return None;
        }
        Some(DatabaseError::new(
            ErrorCategory::Unsupported,
            ErrorPhase::Probe,
            Some(self.kind()),
            format!(
                "server non MariaDB rilevato (product_version={product_version:?}, \
                 version_comment={version_comment:?}) — profilo `{}` qualificato \
                 soltanto su MariaDB.",
                self.product().to_ascii_lowercase()
            ),
        ))
    }

    fn session_isolation_variable(&self) -> &'static str {
        // `tx_isolation` risponde su tutte e tre le versioni misurate —
        // 10.11, 11.8, 12.3 — mentre `transaction_isolation` compare solo
        // dalla 11.1. Fra i due nomi non si sceglie il piu moderno: si
        // sceglie quello che copre l'intera matrice, perche un nome che
        // copre meta delle righe fa fallire l'altra meta prima ancora che
        // la probe possa spiegarsi.
        "@@tx_isolation"
    }

    fn qualified_versions(&self) -> Option<&'static [(u32, u32)]> {
        // Solo le versioni sostenute dall'evidenza di ADR 0014. Il rifiuto dice
        // "non misurata", non "incompatibile", e deve avvenire prima di
        // interrogare variabili di sessione che possono divergere per major.
        Some(&[(10, 11), (11, 8), (12, 3)])
    }

    fn statement_timeout_statement(&self, timeout_ms: u64) -> String {
        // `MAX_EXECUTION_TIME` non esiste su MariaDB: ADR 0014 l'ha vista
        // rifiutare con 1193 su entrambi i riferimenti. L'equivalente e
        // `max_statement_time`, che non e la stessa variabile con un altro
        // nome — prende **secondi** dove il contratto parla in millisecondi.
        //
        // La conversione e esatta, in aritmetica intera: `max_statement_time`
        // e numerico e accetta secondi frazionari, quindi non c'e nulla da
        // arrotondare. Arrotondare per eccesso eviterebbe lo zero ma
        // trasformerebbe 200 ms in un secondo, cioe allungherebbe da solo
        // proprio il limite che qualcuno aveva chiesto di stringere.
        format!(
            "SET SESSION max_statement_time = {}.{:03}",
            timeout_ms / 1_000,
            timeout_ms % 1_000
        )
    }

    fn schemas_query(&self) -> &'static str {
        SCHEMAS_QUERY
    }

    fn objects_query(&self) -> &'static str {
        OBJECTS_QUERY
    }

    fn object_query(&self) -> &'static str {
        OBJECT_QUERY
    }

    fn object_columns_query(&self) -> &'static str {
        // `SRS_ID` non esiste in `information_schema.columns` su MariaDB:
        // errore 1054, misurato su entrambi i riferimenti. La colonna si
        // dichiara **nulla**, non si omette: il contratto degli alias vuole
        // che il lettore trovi sempre il campo, e "assente" e "nessun SRID
        // dichiarato" sono le due cose che l'evidenza tiene separate ovunque.
        //
        // Il resto della `SELECT` e identico a quello di `MysqlProfile`, e una
        // guardia verifica che le due query differiscano **solo** in questo
        // frammento: e l'unico modo perche una modifica al catalogo non passi
        // da una parte sola.
        "SELECT COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position, \
        DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, \
        IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, \
        CHARACTER_SET_NAME AS character_set_name, COLLATION_NAME AS collation_name, \
        NUMERIC_PRECISION AS numeric_precision, NUMERIC_SCALE AS numeric_scale, \
        DATETIME_PRECISION AS datetime_precision, NULL AS srs_id, \
        EXTRA AS extra, COALESCE(GENERATION_EXPRESSION, '') AS generation_expression \
        FROM information_schema.columns \
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
    }

    fn object_indexes_query(&self) -> &'static str {
        // `EXPRESSION` non esiste in `information_schema.statistics` su
        // MariaDB: errore 1054, misurato su entrambi i riferimenti. Il resto
        // della forma coincide — `INDEX_NAME`, `NON_UNIQUE`, `COLUMN_NAME`,
        // `SEQ_IN_INDEX` rendono le stesse righe per la stessa tabella.
        "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, \
        SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, \
        NULL AS expression \
        FROM information_schema.statistics \
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
        ORDER BY INDEX_NAME, SEQ_IN_INDEX"
    }

    fn reports_functional_index_parts(&self) -> bool {
        // Non e una scelta di stile: la colonna da cui si riconoscerebbero non
        // esiste. Dire `true` qui significherebbe promettere al catalogo parti
        // funzionali che questa query non puo produrre.
        //
        // La conseguenza e voluta: una parte di indice senza colonna diventa
        // un rifiuto — "senza colonna ne espressione" — invece di un indice
        // dichiarato confrontabile per colonne che non lo e. Come MariaDB
        // pubblichi davvero un indice su espressione non e misurato, e finche
        // non lo e il preflight Upsert deve fermarsi, non indovinare.
        false
    }

    fn wire_column_spec(&self, column: &Column) -> Result<MysqlColumnSpec> {
        // Stesso mapper, e non per comodita: ADR 0014 ha confrontato lo schema
        // Arrow colonna per colonna e i valori decodificati per intero, e
        // coincidono. L'unico campo che diverge — `native_type` `json` contro
        // `text` per la stessa DDL `document JSON` — diverge **prima** di qui,
        // perche MariaDB manda `MYSQL_TYPE_BLOB` dove MySQL manda
        // `MYSQL_TYPE_JSON`.
        //
        // Normalizzarlo a `json` e cio che questo profilo **non** puo fare: su
        // MariaDB `JSON` e un alias di `LONGTEXT` e sul filo le due
        // dichiarazioni sono indistinguibili, quindi la normalizzazione
        // dovrebbe inventare la DDL da metadata che non la portano. Il
        // contratto pubblicato resta percio quello del filo, ed e cio che
        // `MARIADB_NATIVE_TYPE` dichiara — la chiave di questo prodotto, non
        // quella dell'altro.
        wire_column_spec_for(self.product(), column)
    }

    fn metadata_keys(&self) -> MetadataKeys {
        // Namespace proprio, deciso qui e non ereditato. E la domanda che un
        // `MariadbProvider` non potrebbe piu porsi dopo aver pubblicato il
        // primo batch: i metadata sono contratto, e un contratto si cambia
        // una volta sola senza rompere chi lo legge.
        MetadataKeys {
            native_type: protocol::MARIADB_NATIVE_TYPE,
            native_declaration: protocol::MARIADB_NATIVE_DECLARATION,
            collation: protocol::MARIADB_COLLATION,
        }
    }

    fn is_spatial_native_type(&self, native_type: &str) -> bool {
        // Gli stessi nomi: `raw.prepare_metadata_geometry` ha visto
        // `MYSQL_TYPE_GEOMETRY` sui tre riferimenti, e il catalogo li dichiara
        // con gli stessi `DATA_TYPE`. Riconoscerli e anche la strada che
        // produce il rifiuto migliore: una geometry non riconosciuta finirebbe
        // fra i tipi non qualificati, con un messaggio che parla d'altro.
        MYSQL_PROFILE.is_spatial_native_type(native_type)
    }

    fn spatial_requires_declared_srid(&self) -> bool {
        // Su MariaDB `SRS_ID` non esiste, quindi `srs_id` e sempre nullo e
        // **ogni** colonna geometrica viene rifiutata alla descrizione. E la
        // risposta esatta alla domanda che il metodo pone: non "questa colonna
        // non ha un CRS", ma "non c'e modo di sapere se ne abbia uno". Il
        // contratto GeoArrow pubblicato dichiara un CRS; dichiararlo senza
        // saperlo sarebbe l'unico esito peggiore del rifiuto.
        true
    }

    fn geometry_projection(&self, quoted: &str) -> String {
        // `raw.spatial_functions` ha misurato lo stesso esito sui tre
        // riferimenti: `POINT srid=4326`, WKB di 21 byte. Stessa funzione,
        // stesso dialetto in uscita.
        MYSQL_PROFILE.geometry_projection(quoted)
    }

    fn verified_spatial_functions(
        &self,
    ) -> &'static [plenora_database_core::query::SpatialFunction] {
        crate::query::MARIADB_VERIFIED_SPATIAL_FUNCTIONS
    }

    fn geometry_column_ddl(&self, _srid: u32) -> String {
        // Senza vincolo, e non per prudenza: `raw.spatial_write_forms` ha
        // misurato 1064 su entrambe le major per `GEOMETRY SRID 4326`, e la
        // stessa prova rifiuta anche `REF_SYSTEM_ID`. Non esiste una DDL che vincoli una colonna
        // geometrica a un sistema di riferimento su questo prodotto.
        //
        // L'SRID dichiarato non viene perso: lo porta ogni valore, perche
        // `ST_GeomFromWKB(?, <n>)` e accettato e cio che resta memorizzato
        // conserva l'SRID — misurato 4326 su entrambe le major.
        "GEOMETRY".to_owned()
    }

    fn geometry_target_srid_is_compatible(&self, catalog: Option<u32>, _declared: u32) -> bool {
        // Il catalogo tace per costruzione, e quel silenzio e la risposta
        // giusta: `GEOMETRY_COLUMNS.SRID` vale sempre zero perche nessuna DDL
        // puo farlo diventare altro. Con niente da confrontare, il confronto
        // non e «fallito»: non si pone.
        //
        // Un SRID che comparisse sarebbe invece una sorpresa — vorrebbe dire
        // che questo prodotto ha cominciato a vincolare le colonne, e la
        // decisione qui sopra andrebbe rimisurata invece di essere aggirata.
        catalog.is_none()
    }

    fn geometry_srid_projection(&self, quoted: &str) -> String {
        // `ST_SRID` e nella lista che `raw.spatial_functions` ha attraversato
        // su tutti e tre i riferimenti. E' l'unico prodotto dei due che questa
        // proiezione la usa davvero: qui il CRS lo dichiara il chiamante, e
        // una dichiarazione non verificata non e una misura.
        MYSQL_PROFILE.geometry_srid_projection(quoted)
    }

    fn geometry_output_is_unexpected(&self, srid: Option<u32>, dimensions: &str) -> bool {
        // Conseguenza della proiezione condivisa: 21 byte sono esattamente il
        // WKB XY senza SRID incapsulato che il contratto dichiara.
        MYSQL_PROFILE.geometry_output_is_unexpected(srid, dimensions)
    }

    fn capabilities(&self, provider_version: String) -> ProviderCapabilities {
        // Ogni bandiera deriva dalle sonde del profilo MariaDB e non viene
        // ereditata dal profilo MySQL. Le superfici non misurate restano false.
        ProviderCapabilities {
            schema_version: 2,
            provider: self.kind(),
            provider_version,
            extension_versions: BTreeMap::new(),
            reads: ReadCapabilities {
                // `streaming` significa che le righe arrivano a blocchi, non
                // che esista un cursore: `query_stream` fa scorrere il result
                // set sul filo, ed e per questo che la bandiera accanto dice
                // `false`.
                streaming: true,
                server_cursor: false,
                // La finestra è governata dalla capability:
                // l'engine rifiuta un `row_offset` a un provider che non la
                // pubblica. Il piano di lettura la compila come `LIMIT ...
                // OFFSET n`, con il tetto del tipo quando il chiamante non ne
                // ha chiesto uno — `OFFSET` da solo non e sintassi valida su
                // questi motori.
                pagination: true,
                projection: true,
                filter: true,
                ordering: true,
                // Qualificato dal gate MariaDB dedicato su 10.11, 11.8 e
                // 12.3: il token JSON viene riaperto e la seconda pagina e
                // contigua alla prima.
                resumable: true,
            },
            // Le prove live verificano righe, rollback e cancellazione da una
            // seconda sessione. Su questi due motori
            // `CREATE TABLE` fa commit implicito, quindi la tabella non
            // appartiene alla transazione che segue — un batch rifiutato
            // annulla le righe e lascia lo schema, e cio che il chiamante
            // riceve non e `RolledBack` ma `Partial` con recupero richiesto.
            // Misurato uguale sui tre riferimenti, righe rilette da un'altra
            // sessione e catalogo interrogato per la forma.
            writes: WriteCapabilities {
                create: true,
                append: true,
                // Chiusa per la stessa ragione di MySQL — `TRUNCATE` con
                // commit implicito — e non solo perche non misurata.
                truncate_insert: false,
                // Queste modalita sono governate dalle keys: una chiave che non trova
                // riscontro e saltata, non fallita — e cosa il rollback
                // rimette: `Update` i valori di prima, `Replace` le righe che
                // il proprio `DELETE` aveva tolto. Quest'ultima e la prova
                // che conta di piu: un `Replace` fallito che non tornasse
                // indietro lascerebbe il target vuoto.
                update: true,
                upsert: true,
                replace: true,
                delete_by_keys: true,
                // `bulk` dice che le righe raggiungono il server a blocchi,
                // e su questo profilo e la stessa cosa che dice `MySQL`:
                // l'implementazione e condivisa e viene attraversata con piu batch.
                bulk: true,
                array_binding: false,
                returning: false,
                rollback_on_failure: true,
            },
            // Le sonde verificano commit, rollback, isolamento e savepoint.
            // Il rollback parziale
            // lascia la sola riga scritta prima del savepoint, riletta da
            // un'altra connessione, e un nome mai creato viene rifiutato. La
            // seconda e cio che rende vera la prima: senza, un motore che
            // dicesse di si a qualunque `ROLLBACK TO` supererebbe comunque il
            // controllo sul conteggio.
            transactions: TransactionCapabilities {
                single_transaction: true,
                savepoints: true,
                // Le due chiuse hanno **una** ragione sola, ed e del motore.
                //
                // Ogni DDL di questo prodotto porta un commit implicito. Un
                // `CREATE TABLE` dentro una transazione non aspetta il
                // `COMMIT`: chiude quella in corso e apre la successiva, e cio
                // che stava dentro resta scritto anche se poi qualcosa
                // fallisce. `transactional_ddl` promette il contrario.
                //
                // `staged_swap` ne discende. Lo scambio pubblica una tabella
                // costruita a parte, e vale se lo scambio e **atomico con il
                // carico**: qui non lo e, perche la DDL che costruisce la
                // tabella di staging ha gia committato prima. `RENAME TABLE`
                // e atomico da solo, e non basta — l'atomicita che serve e
                // quella dell'insieme.
                //
                // Nessuna delle due si aprira: non sono lacune di misura, sono
                // il motore. PostgreSQL e SQL Server le hanno perche la loro
                // DDL e transazionale.
                transactional_ddl: false,
                // Stessa ragione della riga sopra: senza DDL transazionale
                // l'atomicita dell'insieme non c'e.
                staged_swap: false,
                scope: TransactionScope::Transaction,
            },
            // La lettura geometrica richiede un CRS dichiarato. Le sonde
            // girano su una colonna `GEOMETRY`
            // che nessuna DDL vincola — l'unica forma che MariaDB ammette — e
            // misurano tre esiti diversi: senza dichiarazione la colonna resta
            // rifiutata, con la dichiarazione giusta le righe arrivano, con
            // una dichiarazione che i valori smentiscono la lettura fallisce
            // alla riga che la smentisce. La terza e quella che rende vera la
            // seconda: senza, `geometry: true` significherebbe che il provider
            // ripete cio che il chiamante gli ha detto.
            spatial: SpatialCapabilities {
                // Una semantica sola, quindi la voce e l'intersezione: su questo
                // prodotto `geography` non esiste, e non e una lacuna di misura.
                // La mappa non aggiunge niente qui, e dirlo esplicitamente e
                // meglio del silenzio — un consumatore che legge per semantica
                // trova la sua risposta senza dover sapere che il prodotto ne
                // ha una sola.
                functions_by_semantics: std::collections::BTreeMap::from([(
                    plenora_database_core::geometry::SpatialSemantics::Geometry,
                    self.verified_spatial_functions().to_vec(),
                )]),
                read_wkb: true,
                write_wkb: self.write_spatial_is_qualified(),
                geometry: true,
                // `geography` non esiste su questo prodotto, e non e una
                // lacuna di misura.
                geography: false,
                // MariaDB ammette lo SPATIAL INDEX sulla colonna non vincolata
                // prodotta da questo profilo.
                spatial_index: true,
                // Le sonde scrivono un punto e un poligono nella stessa colonna
                // e li rileggono verificandone il tipo.
                mixed_geometry_types: true,
                // Solo XY, e non perche le altre non siano state provate:
                // `raw.geometry_dimensions` ha chiesto al parser `POINT Z` nelle
                // due sintassi WKT e ha avuto `NULL` qui e 3037 su `MySQL`, e
                // `ST_Z` e `ST_M` sono assenti da entrambi. Non c'e una terza
                // dimensione da dichiarare.
                dimensions: vec![plenora_database_core::geometry::Dimensions::Xy],
                // Il catalogo **di questo prodotto**, misurato da
                // `provider.profile_spatial_functions` attraversando il
                // percorso di query su ogni riferimento della matrice. Non
                // coincide con quello di `MySQL`: la sonda deve attraversare
                // questa lista, che è la fonte, senza duplicarne il conteggio.
                //
                // `IsValid` resta fuori, e quello e un fatto e non un numero:
                // la 12.3 ce l'ha, la 11.8 LTS risponde 1305, e una capability
                // e una promessa a chi non sa su quale minor atterrera.
                functions: self.verified_spatial_functions().to_vec(),
                requires_declared_crs: true,
            },
            // I limiti non sono capability: dicono quanto il crate manda, non
            // cosa il prodotto sa fare. Sono i suoi stessi valori su un
            // protocollo condiviso, e dichiararli `None` — cioe "nessun limite
            // dichiarato" — sarebbe la sola lettura pericolosa fra le due.
            limits: ProviderLimits {
                max_identifier_bytes: None,
                max_bind_parameters: Some(crate::MAX_BIND_PARAMETERS as u64),
                max_statement_bytes: None,
                max_batch_rows: Some(crate::MAX_BATCH_ROWS as u64),
                max_payload_bytes: None,
            },
        }
    }

    fn write_spatial_is_qualified(&self) -> bool {
        // La stessa bandiera governa capability pubblicata e ammissione del
        // piano. `raw.spatial_write_forms` ha
        // misurato i tre fatti del server: la DDL vincolata e rifiutata con
        // 1064, `ST_GeomFromWKB(?, <n>)` e accettato, e l'SRID **resta
        // memorizzato** — 4326 su entrambe le major. Quest'ultimo e cio che
        // rende praticabile tutto il resto: dove la colonna non porta il CRS,
        // lo porta il valore.
        //
        // `provider.profile_write_spatial_create` e `..._append` misurano il
        // percorso: la `Create` emette la DDL che il profilo decide, la
        // `Append` scrive nella tabella che la prima ha lasciato, e la
        // rilettura da un'altra connessione verifica l'SRID di **ogni** riga.
        // E' la meta che chiude il cerchio con la lettura: se la scrittura
        // perdesse il CRS, la lettura di questo stesso crate rifiuterebbe le
        // righe che ha appena scritto.
        true
    }

    fn writable_geometry_type(&self, name: &str) -> bool {
        // L'insieme di MySQL, e non per eredita: i tipi geometrici sono nomi
        // dello standard OGC, il piano li usa per la sola dichiarazione
        // `exact`, e le sonde di scrittura girano su `mixed` — dove il tipo
        // non compare affatto.
        //
        // Resta pero una cosa non misurata, e va detta qui invece che dedotta
        // dal `true` di sopra: nessuna sonda ha scritto una colonna dichiarata
        // `exact` su questo prodotto. Il giorno in cui una lo facesse, questo
        // metodo e cio che deciderebbe, e allora il rinvio a `MySQL` andrebbe
        // sostenuto invece che assunto.
        crate::write::geometry_type_is_writable(name)
    }

    fn classify_server_code(&self, code: u16) -> ServerCodeVerdict {
        let product = self.product();
        // `ER_STATEMENT_TIMEOUT`, che su MySQL non esiste: e il codice con cui
        // arriva il timeout che **questo** profilo applica, misurato su
        // entrambi i riferimenti dopo `SET SESSION max_statement_time`.
        //
        // La classificazione deve concordare con lo statement di timeout del
        // profilo, cosi il chiamante distingue un limite atteso da un guasto.
        if code == MARIADB_STATEMENT_TIMEOUT {
            return ServerCodeVerdict {
                category: ErrorCategory::Timeout,
                retry: RetryDisposition::Never,
                message: format!("timeout {product} (codice {MARIADB_STATEMENT_TIMEOUT})"),
                remote_effect: None,
            };
        }
        // Gli altri codici passano dalla tabella condivisa **solo** se sono
        // stati osservati su questo prodotto. Non e pignoleria: 1213 dichiara
        // `retry: Safe` e `remote_effect: RolledBack`, cioe autorizza il
        // chiamante a rifare l'operazione e gli dice che non ha nulla da
        // ripulire. Ereditata senza misura, quella riga sarebbe una promessa
        // fatta a nome di un motore che nessuno aveva interrogato.
        if MEASURED_SERVER_CODES.contains(&code) {
            return classify_shared_code(product, code);
        }
        generic_server_code(product, code)
    }

    fn row_rejection_cause(&self, code: u16) -> Option<&'static str> {
        match code {
            // Quattro codici, e ciascuno misurato su entrambi i riferimenti.
            // Il CHECK e l'unico che diverge davvero: su MySQL arriva 3819, su
            // MariaDB 4025, dallo stesso `INSERT` che viola lo stesso vincolo.
            // Il codice di MySQL non compare qui, e non per simmetria: su
            // MariaDB non e mai arrivato, e attribuire una causa per analogia
            // e l'unico dei due errori che non si corregge leggendo l'errore
            // del server.
            1_048 | 1_062 | 1_452 | 4_025 => {
                Some(plenora_database_core::row_diagnostics::CAUSE_CONSTRAINT_VIOLATION)
            }
            _ => None,
        }
    }
}

/// Le tre interrogazioni di catalogo che i due prodotti condividono.
///
/// ADR 0014 ha misurato che `information_schema.schemata` e
/// `information_schema.tables` rispondono le stesse righe, con le stesse
/// colonne, sui tre riferimenti: non c'e divergenza da esprimere, e due copie
/// della stessa `SELECT` divergerebbero alla prima modifica fatta da una parte
/// sola. Le due query che **non** sono qui — colonne e indici — stanno nei
/// profili proprio perche li la divergenza e stata misurata.
const SCHEMAS_QUERY: &str = "SELECT SCHEMA_NAME AS schema_name \
    FROM information_schema.schemata \
    WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
    ORDER BY SCHEMA_NAME";

/// Vedi [`SCHEMAS_QUERY`].
const OBJECTS_QUERY: &str = "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, \
    TABLE_TYPE AS table_type, ENGINE AS engine \
    FROM information_schema.tables WHERE TABLE_SCHEMA = ? \
    ORDER BY TABLE_NAME";

/// Vedi [`SCHEMAS_QUERY`].
const OBJECT_QUERY: &str = "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, \
    TABLE_TYPE AS table_type, ENGINE AS engine \
    FROM information_schema.tables \
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

/// Il rifiuto di una versione che il profilo non dichiara qualificata.
///
/// Vive accanto al riconoscimento perche e la seconda meta della stessa
/// domanda, e perche la lettura della stringa deve essere una sola: `VERSION()`
/// porta `11.8.8-MariaDB-ubu2404` o `9.7.2`, e cio che conta sono le prime due
/// componenti.
///
/// Una versione **illeggibile** viene rifiutata come una non qualificata, per
/// il profilo che dichiara un elenco: non sapere quale versione risponde e
/// esattamente la condizione da cui l'elenco protegge.
pub(crate) fn unqualified_version_rejection(
    profile: &dyn ProductProfile,
    product_version: &str,
) -> Option<DatabaseError> {
    let qualified = profile.qualified_versions()?;
    let mut parts = product_version.split('.');
    let read = |part: Option<&str>| -> Option<u32> {
        let digits: String = part?.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    };
    let observed = read(parts.next()).zip(read(parts.next()));
    if observed.is_some_and(|version| qualified.contains(&version)) {
        return None;
    }
    let product = profile.product();
    let declared = qualified
        .iter()
        .map(|(major, minor)| format!("{major}.{minor}"))
        .collect::<Vec<_>>()
        .join(", ");
    Some(DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Probe,
        Some(profile.kind()),
        format!(
            "versione {product} {product_version:?} non misurata: il profilo e \
             qualificato su {declared}. Non e un difetto del server — e \
             una versione su cui nessuna prova e stata fatta."
        ),
    ))
}

/// Se le stringhe che il server espone dicono `MariaDB`.
///
/// Una sola lettura per due decisioni opposte: `MysqlProfile` rifiuta quando e
/// vera, `MariadbProfile` quando e falsa. Con due implementazioni un server
/// potrebbe finire rifiutato da entrambi — o accettato da entrambi, che e
/// peggio — e la partizione si romperebbe senza che nessuno la stia guardando.
///
/// Il riconoscimento e per stringa perche e cio che il server espone:
/// `VERSION()` e `@@version_comment`. ADR 0014 ha misurato che su tutti e tre
/// i riferimenti `MariaDB` entrambe portano "mariadb", e che su `MySQL` 9.7.2
/// nessuna delle due lo porta.
fn looks_like_mariadb(product_version: &str, version_comment: &str) -> bool {
    product_version.to_ascii_lowercase().contains("mariadb")
        || version_comment.to_ascii_lowercase().contains("mariadb")
}

/// La tabella dei codici che i due prodotti condividono, attribuita a chi
/// li ha emessi.
///
/// I numeri vengono dal protocollo `MySQL`, che `MariaDB` parla, e le sonde
/// comparative li osservano uno per uno: 1045, 1048, 1054, 1062,
/// 1146, 1205, 1213 e 1452 sono arrivati **dagli stessi tentativi** su tutti
/// e tre i riferimenti. Duplicare la tabella per quelli avrebbe dato due copie
/// da tenere allineate senza una divergenza che le giustifichi.
///
/// Non tutti pero sono stati misurati ovunque, ed e la ragione per cui questa
/// funzione non e il punto d'ingresso di nessun profilo: 1044 e 1049 non sono
/// mai arrivati da `MariaDB`, 3024 e il timeout di `MySQL` e 1969 quello di
/// `MariaDB`. Chi chiama sceglie **quali** codici passare di qui; questa
/// tabella dice solo cosa significano quelli che ci passano.
///
/// Cio che **non** e condiviso e il nome: un messaggio che dice `MySQL`
/// mentre a rifiutare e stato `MariaDB` manda chi legge a cercare sul server
/// sbagliato. Il prodotto arriva percio dal profilo chiamante, e una guardia
/// verifica che ogni messaggio lo nomini.
fn classify_shared_code(product: &str, code: u16) -> ServerCodeVerdict {
    match code {
        1_045 => ServerCodeVerdict {
            category: ErrorCategory::Authentication,
            retry: RetryDisposition::Never,
            message: format!("autenticazione {product} rifiutata (codice 1045)"),
            remote_effect: None,
        },
        // 1044 e il permesso negato su un database, 1142 su un comando o una
        // tabella. Entrambi sono rifiuti di autorizzazione, non guasti di
        // esecuzione.
        //
        // Le sonde lo ricevono identico dai riferimenti qualificati: una
        // classificazione si estende soltanto con una misura riproducibile.
        1_044 | 1_142 => ServerCodeVerdict {
            category: ErrorCategory::Authorization,
            retry: RetryDisposition::Never,
            message: format!("autorizzazione {product} negata (codice {code})"),
            remote_effect: None,
        },
        1_049 | 1_146 => ServerCodeVerdict {
            category: ErrorCategory::NotFound,
            retry: RetryDisposition::Never,
            message: format!("database o oggetto {product} non trovato (codice {code})"),
            remote_effect: None,
        },
        1_054 => ServerCodeVerdict {
            category: ErrorCategory::Schema,
            retry: RetryDisposition::Never,
            message: format!("colonna {product} non valida (codice 1054)"),
            remote_effect: None,
        },
        1_062 => ServerCodeVerdict {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            message: format!("vincolo univoco {product} violato (codice 1062)"),
            remote_effect: None,
        },
        // I codici che indicano un dato di riga non valido.
        //
        // La diagnostica per riga si attiva **solo** per `Append`: li 1048 e
        // 1406 diventano un rifiuto di riga con la sua causa. Per ogni altra
        // mode la scrittura e un bulk, il codice passa da qui, e da qui
        // usciva `Execution`/`Never` — «l'operazione e fallita sul server e
        // ritentarla non ha ragione di riuscire». Vero, e inutile: un dato
        // troppo lungo lo corregge chi chiama, un guasto no, e sono due
        // rimedi diversi; analogamente 1142 identifica un permesso mancante.
        //
        // Le campagne comparative verificano questi codici su tutti i
        // riferimenti supportati e nei relativi percorsi di rollback.
        1_048 => ServerCodeVerdict {
            category: ErrorCategory::DataMapping,
            retry: RetryDisposition::Never,
            message: format!("colonna {product} non nullable senza valore (codice 1048)"),
            remote_effect: None,
        },
        1_406 => ServerCodeVerdict {
            category: ErrorCategory::DataMapping,
            retry: RetryDisposition::Never,
            message: format!("valore oltre la larghezza della colonna {product} (codice 1406)"),
            remote_effect: None,
        },
        // I due lati dello stesso vincolo, e la categoria e la stessa di 1062
        // per la stessa ragione: non e la riga a essere malformata, e lo stato
        // del database a non ammetterla — una figlia che trattiene la madre,
        // o una madre che non c'e.
        1_451 | 1_452 => ServerCodeVerdict {
            category: ErrorCategory::Conflict,
            retry: RetryDisposition::Never,
            message: format!("integrita referenziale {product} violata (codice {code})"),
            remote_effect: None,
        },
        // L'unico codice che dichiara da se cosa e successo sul
        // server: la transazione vittima e gia annullata, e il
        // chiamante non ha nulla da ripulire.
        1_213 => ServerCodeVerdict {
            category: ErrorCategory::Transient,
            retry: RetryDisposition::Safe,
            message: format!("deadlock {product}; transazione vittima annullata"),
            remote_effect: Some(RemoteEffect::RolledBack),
        },
        1_205 | 3_024 => ServerCodeVerdict {
            category: ErrorCategory::Timeout,
            retry: RetryDisposition::Never,
            message: format!("timeout {product} (codice {code})"),
            remote_effect: None,
        },
        native => generic_server_code(product, native),
    }
}

/// Il verdetto di un codice che nessuna misura ha qualificato.
///
/// `Execution` e `Never` non sono un "non so" travestito: dicono che
/// l'operazione e fallita sul server e che ritentarla non ha ragione di
/// riuscire. E la risposta giusta per un codice che non si conosce, ed e per
/// questo che esiste una funzione sola — un profilo che se la riscrivesse
/// potrebbe farla diventare, senza volerlo, un permesso di ritentare.
fn generic_server_code(product: &str, code: u16) -> ServerCodeVerdict {
    ServerCodeVerdict {
        category: ErrorCategory::Execution,
        retry: RetryDisposition::Never,
        message: format!("errore server {product} redatto (codice {code})"),
        remote_effect: None,
    }
}

/// Il codice con cui `MariaDB` interrompe uno statement oltre
/// `max_statement_time` (`ER_STATEMENT_TIMEOUT`).
///
/// Su `MySQL` lo stesso evento arriva come 3024, e i due numeri non si
/// incrociano: e la seconda meta della divergenza sul timeout, quella che
/// l'istruzione corretta da sola non chiude.
pub(crate) const MARIADB_STATEMENT_TIMEOUT: u16 = 1_969;

/// I codici osservati su **entrambi** i riferimenti `MariaDB`, con lo stesso
/// significato che hanno su `MySQL`.
///
/// L'elenco e corto apposta. Ogni voce viene da un tentativo registrato in
/// `docs/mariadb/EVIDENCE.md`, e cio che non c'e non e negato: e non misurato,
/// e finisce nel verdetto generico.
///
/// Codici coperti direttamente dalla campagna live degli errori.
///
/// La lista limita quali classificazioni `MySQL` possano essere ereditate dal
/// profilo `MariaDB`: una voce non misurata resta nel ramo generico.
pub(crate) const MEASURED_SERVER_CODES: &[u16] = &[
    1_045, 1_048, 1_054, 1_062, 1_142, 1_146, 1_205, 1_213, 1_406, 1_451, 1_452,
];

/// Il mapper dei metadata wire, condiviso fra i prodotti che parlano il
/// protocollo `MySQL`.
///
/// ADR 0014 lo ha misurato riga per riga sui tre riferimenti: dagli stessi
/// metadata di `COM_STMT_PREPARE` escono lo stesso `kind` e lo stesso
/// `native_type`, e i valori decodificati coincidono per intero. Cio che
/// diverge non e questa funzione, e il suo **ingresso** — la stessa DDL
/// `document JSON` arriva come `MYSQL_TYPE_JSON` da `MySQL` e come
/// `MYSQL_TYPE_BLOB` da `MariaDB`, dove `JSON` e un alias di `LONGTEXT`.
///
/// Percio resta una sola: due copie divergerebbero senza che nessuna
/// evidenza lo chieda. L'unica cosa che il prodotto porta con se e
/// l'attribuzione dei rifiuti, che deve nominare chi ha rifiutato.
#[allow(clippy::too_many_lines)]
fn wire_column_spec_for(product: &str, column: &Column) -> Result<MysqlColumnSpec> {
    let name = column.name_str().into_owned();
    if name.is_empty() || name.contains('\0') {
        return Err(prepare_error(
            ErrorCategory::Schema,
            format!("colonna di output {product} senza nome utilizzabile"),
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
            (decimal_kind(product, column, unsigned)?, "decimal")
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
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB => text_kind(binary, flags, "mediumtext", "mediumblob"),
        ColumnType::MYSQL_TYPE_LONG_BLOB => text_kind(binary, flags, "longtext", "longblob"),
        ColumnType::MYSQL_TYPE_BLOB => text_kind(binary, flags, "text", "blob"),
        // Qui passa una geometria **non incapsulata**, cioe una
        // colonna proiettata tale e quale nel path query. Non ha involucro, e
        // il suo valore arriva nel formato interno del prodotto invece che in
        // WKB; e nessuna posizione nel piano la descrive, perche non e il
        // risultato di una funzione. Il contratto `GeoArrow` pubblica un CRS, e
        // qui non c'e niente da cui dedurlo. Le geometrie calcolate seguono
        // invece `SpatialFunction::crs_rule` e arrivano incapsulate come BLOB.
        ColumnType::MYSQL_TYPE_GEOMETRY => {
            return Err(unsupported(format!(
                "colonna geometrica {product} non incapsulata nel path query: nessun CRS \
                 dimostrabile"
            )));
        }
        other => {
            return Err(unsupported(format!(
                "tipo wire {product} non qualificato nel result set: {other:?}"
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
        // Il path query non riceve un piano di lettura, quindi non ha dove
        // ospitare una dichiarazione; e comunque non ci arriverebbe, perche
        // qui sopra `MYSQL_TYPE_GEOMETRY` e rifiutato prima.
        spatial_srid_declared: false,
    })
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
fn decimal_kind(product: &str, column: &Column, unsigned: bool) -> Result<MysqlColumnKind> {
    let scale = i8::try_from(column.decimals()).map_err(|_| {
        prepare_error(
            ErrorCategory::Unsupported,
            format!("scala decimal {product} non rappresentabile"),
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
                format!("precisione decimal {product} non ricostruibile dai metadati"),
            )
        })?;
    if precision == 0 || precision > 38 || scale < 0 || scale > precision.cast_signed() {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            format!("decimal {product} oltre Decimal128 Arrow"),
        ));
    }
    Ok(MysqlColumnKind::Decimal { precision, scale })
}

fn mysql_spatial_capabilities() -> SpatialCapabilities {
    SpatialCapabilities {
        // Stessa forma del profilo: una semantica, una voce.
        functions_by_semantics: std::collections::BTreeMap::from([(
            plenora_database_core::geometry::SpatialSemantics::Geometry,
            crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec(),
        )]),
        read_wkb: true,
        write_wkb: true,
        geometry: true,
        // Il tipo non esiste su questo prodotto. `GEOMETRY` con un SRID
        // geografico non e la stessa cosa: le funzioni restano cartesiane, e
        // una distanza su 4326 verrebbe resa in gradi invece che in metri.
        // Pubblicare `true` perche l'SRID e geografico sarebbe promettere una
        // semantica che il motore non applica.
        //
        // Non e una lacuna di misura: e la ragione per cui PostgreSQL e SQL
        // Server hanno due tipi e questo ne ha uno.
        geography: false,
        // Aperta insieme a quella di MariaDB, e per la stessa misura: la
        // clausola entra nella `CREATE TABLE` della mode `Create`, e la sonda
        // la attraversa su entrambi i prodotti. Restava chiusa qui per la
        // stessa ragione — il piano la rifiutava in prepare — e non per una
        // differenza fra i due.
        spatial_index: true,
        mixed_geometry_types: true,
        dimensions: vec![plenora_database_core::geometry::Dimensions::Xy],
        // Le funzioni spatial pubblicate sono quelle di
        // `crate::query::VERIFIED_SPATIAL_FUNCTIONS`, e cio che le qualifica
        // non e il dialect condiviso ma la sonda live che le attraversa una
        // per una. Il numero non e duplicato qui: la costante e l'unica fonte
        // e cambia soltanto con una misura live.
        functions: crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec(),
        // `true`, e non era la risposta attesa.
        //
        // `information_schema.columns.SRS_ID` esiste su questo prodotto e la
        // DDL `SRID 4326` e accettata, quindi sembrava che qui il catalogo il
        // CRS lo sapesse sempre e che chiederlo al chiamante fosse chiederlo
        // due volte. Le sonde del CRS dichiarato hanno misurato altro: su una
        // colonna `GEOMETRY` che la DDL **non** vincola, `SRS_ID` e nullo
        // anche su MySQL, e la colonna e rifiutata esattamente come su
        // MariaDB. Con la dichiarazione si legge, e con una dichiarazione
        // smentita dai valori fallisce alla riga che la smentisce: i tre esiti
        // coincidono sui tre riferimenti, senza divergenze.
        //
        // La bandiera dice «leggere una geometria **puo** richiedere un CRS
        // dichiarato», non «lo richiede sempre»: su MySQL vale per le colonne
        // non vincolate, su MariaDB per tutte, perche li vincolarle non si
        // puo. Pubblicare `false` qui avrebbe tenuto chiusa una lettura che
        // funziona, e per una ragione che nessuna misura sosteneva.
        requires_declared_crs: true,
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
///
/// Resta necessario anche ora che `MARIADB_PROFILE` esiste, e per una ragione
/// che i due profili reali non possono coprire: dove `MySQL` e `MariaDB`
/// **coincidono** — la categoria di un codice di errore, per dirne una — un
/// confronto fra loro non distingue una decisione presa dal profilo da una
/// ereditata per caso. Questo profilo diverge li apposta, su una divergenza
/// che nessun prodotto reale gli impone, ed e cio che rende visibile il
/// dispatch. Un test differenziale fra i due profili veri prova che le
/// divergenze misurate ci sono; questo prova che passano dal profilo.
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

    fn qualified_versions(&self) -> Option<&'static [(u32, u32)]> {
        MYSQL_PROFILE.qualified_versions()
    }

    fn session_isolation_variable(&self) -> &'static str {
        MYSQL_PROFILE.session_isolation_variable()
    }

    fn statement_timeout_statement(&self, timeout_ms: u64) -> String {
        // Diverge per nome **e** per unita, che e la forma della divergenza
        // che ADR 0014 ha misurato su MariaDB. Se la conversione tornasse a
        // vivere fuori dal profilo, questo profilo emetterebbe secondi con
        // un valore in millisecondi e nessuno se ne accorgerebbe.
        // Conversione **esatta**, in aritmetica intera. Arrotondare per
        // eccesso evitava lo zero ma allentava il contratto: 200 ms
        // diventavano un secondo, e un timeout che si allunga da solo e un
        // timeout che non protegge piu da cio per cui era stato chiesto.
        // `max_statement_time` e numerico e accetta secondi frazionari,
        // quindi la conversione giusta non perde nulla — e non serve un
        // float per farla.
        format!(
            "SET SESSION max_statement_time = {}.{:03}",
            timeout_ms / 1_000,
            timeout_ms % 1_000
        )
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

    fn metadata_keys(&self) -> MetadataKeys {
        MYSQL_PROFILE.metadata_keys()
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

    fn verified_spatial_functions(
        &self,
    ) -> &'static [plenora_database_core::query::SpatialFunction] {
        MYSQL_PROFILE.verified_spatial_functions()
    }

    fn geometry_column_ddl(&self, srid: u32) -> String {
        MYSQL_PROFILE.geometry_column_ddl(srid)
    }

    fn geometry_target_srid_is_compatible(&self, catalog: Option<u32>, declared: u32) -> bool {
        MYSQL_PROFILE.geometry_target_srid_is_compatible(catalog, declared)
    }

    fn geometry_srid_projection(&self, quoted: &str) -> String {
        MYSQL_PROFILE.geometry_srid_projection(quoted)
    }

    fn geometry_output_is_unexpected(&self, srid: Option<u32>, dimensions: &str) -> bool {
        MYSQL_PROFILE.geometry_output_is_unexpected(srid, dimensions)
    }

    fn capabilities(&self, provider_version: String) -> ProviderCapabilities {
        // Delega, ma non sulle proprie decisioni: la guardia di coerenza ha
        // trovato proprio questo — un profilo che nega la scrittura spatial e
        // pubblica la capability ereditata dice due cose diverse, e il
        // consumatore crede alla seconda.
        let mut published = MYSQL_PROFILE.capabilities(provider_version);
        published.provider = self.kind();
        published.spatial.write_wkb = self.write_spatial_is_qualified();
        published
    }

    fn write_spatial_is_qualified(&self) -> bool {
        // Nessuna evidenza di scrittura spatial per questo prodotto: e il
        // fail-closed che un secondo profilo reale erediterebbe finche la
        // prova non esiste.
        false
    }

    fn writable_geometry_type(&self, name: &str) -> bool {
        MYSQL_PROFILE.writable_geometry_type(name)
    }

    fn classify_server_code(&self, code: u16) -> ServerCodeVerdict {
        // Un codice che su questo prodotto significa altro. Serve a provare
        // che la classificazione viene davvero dal profilo: con una tabella
        // sola, categoria e retry sarebbero indistinguibili dall'ereditarla.
        if code == 1_054 {
            return ServerCodeVerdict {
                category: ErrorCategory::Unsupported,
                retry: RetryDisposition::Never,
                message: format!("codice 1054 non qualificato su {}", self.product()),
                remote_effect: None,
            };
        }
        MYSQL_PROFILE.classify_server_code(code)
    }

    fn row_rejection_cause(&self, code: u16) -> Option<&'static str> {
        MYSQL_PROFILE.row_rejection_cause(code)
    }
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
