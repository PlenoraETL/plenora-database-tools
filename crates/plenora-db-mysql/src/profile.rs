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
/// transazioni deve poterlo chiedere — prima tirava a indovinare provando a
/// interpretare i byte come UTF-8.
pub(crate) const BINARY_CHARACTER_SET: u16 = 63;

/// Cosa un codice di errore del server significa, per intero.
///
/// L'effetto remoto sta qui e non nel chiamante perche e parte del
/// significato: "deadlock, transazione vittima annullata" e un'affermazione
/// sullo stato del server, e chi conosce i codici e il profilo. Lasciarla
/// fuori voleva dire che un profilo poteva ridefinire la categoria di un
/// codice ma non se quel codice avesse gia rollbackato — e la seconda decide
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

/// se il chiamante debba ripulire.
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
    /// Sono due domande diverse, e tenerle in due metodi e la ragione per cui
    /// esiste questo. `foreign_product_rejection` chiede **quale prodotto** sta
    /// rispondendo; questo chiede **se quella versione** e stata misurata. Un
    /// riconoscimento che rispondesse a entrambe accetterebbe qualunque server che
    /// si chiama `MariaDB`, comprese major che nessuno ha mai acceso.
    ///
    /// `None` significa "nessun limite dichiarato", e non "tutte qualificate": e
    /// lo stato in cui il profilo `MySQL` si trova oggi, e cambiarlo
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
    /// Il metodo esiste perche il difetto che lo ha prodotto era invisibile:
    /// la probe chiedeva `@@transaction_isolation` insieme a `VERSION()`, e su
    /// 10.11 moriva con 1193 prima di poter dire che quella versione non era
    /// qualificata. Il nome della variabile appartiene al prodotto esattamente
    /// come vi appartiene quello del timeout, ed e la stessa ragione: una
    /// costante condivisa e vera per uno solo dei due.
    fn session_isolation_variable(&self) -> &'static str;

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
    /// Non e una comodita per la tabella delle capability: e il cancello che il
    /// renderer consulta. Le due cose erano separate — una lista sola per
    /// entrambi i prodotti, e una capability che ne pubblicava un'altra — e la
    /// conseguenza era che un piano poteva superare il cancello e morire sul
    /// server, con la capability che diceva giustamente di no.
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
    /// Sono due domande diverse a seconda del prodotto, e tenerle in una sola
    /// riga di confronto le confondeva. Dove la colonna e vincolata — `MySQL` —
    /// il catalogo porta l'SRID e deve essere **quello**: scrivere geometrie
    /// 3003 in una colonna dichiarata 4326 e un errore che il server
    /// rifiuterebbe comunque, ed e meglio dirlo in preflight.
    ///
    /// Dove la colonna non puo essere vincolata — `MariaDB` — il catalogo tace
    /// per costruzione, e non c'e niente con cui confrontare. Il confronto
    /// secco `catalogo == dichiarato` falliva **sempre**, quindi la scrittura
    /// spatial era chiusa da una riga di codice prima ancora che dalla
    /// bandiera: `None != Some(4326)`.
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
    fn classify_server_code(&self, code: u16) -> ServerCodeVerdict;

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
        if !looks_like_mariadb(product_version, version_comment) {
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

    fn qualified_versions(&self) -> Option<&'static [(u32, u32)]> {
        // Nessun limite, ed e lo stato di oggi scritto invece che sottinteso.
        // La matrice qualifica 9.7, 8.4 e 8.0, ma il provider non ha mai
        // rifiutato una versione diversa: trasformare quella matrice in un
        // rifiuto cambierebbe il comportamento di un provider qualificato —
        // un 9.8 che oggi funziona smetterebbe di connettersi — e non e una
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
                // La finestra si rende, e da oggi la bandiera la governa:
                // l'engine rifiuta un `row_offset` a un provider che non la
                // pubblica. Il piano di lettura la compila come `LIMIT ...
                // OFFSET n`, con il tetto del tipo quando il chiamante non ne
                // ha chiesto uno — `OFFSET` da solo non e sintassi valida su
                // questi motori.
                pagination: true,
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
                transactional_ddl: false,
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
/// tabella dei codici — e stato misurato **uguale** sui tre riferimenti e
/// resta codice condiviso. Spostarlo qui per simmetria darebbe due copie che
/// nessuna prova tiene allineate.
// Non raggiungibile dal codice di produzione: nessun provider lo seleziona,
// ed e la fase in cui il profilo doveva restare. L'`allow` e percio il segno
// di uno stato dichiarato, non di una svista — e sparisce insieme a esso, nel
// momento in cui `MariadbProvider` lo costruira. I test differenziali lo
// esercitano gia oggi.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MariadbProfile;

/// L'unica istanza: come [`MYSQL_PROFILE`], il profilo e senza stato.
#[allow(dead_code)]
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
        Some(DatabaseError {
            category: ErrorCategory::Unsupported,
            phase: ErrorPhase::Probe,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(self.kind()),
            execution_id: None,
            message: format!(
                "server non MariaDB rilevato (product_version={product_version:?}, \
                 version_comment={version_comment:?}) — profilo `{}` qualificato \
                 soltanto su MariaDB.",
                self.product().to_ascii_lowercase()
            ),
            diagnostics: None,
        })
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
        // Le tre che ADR 0014 ha misurato, e nessun'altra. Non e prudenza
        // eccessiva: cio che il profilo afferma — quali colonne di catalogo
        // esistono, quale variabile regge il timeout, con quale codice arriva
        // — e stato osservato su queste tre, e una major che non c'era ancora
        // quando la misura e stata fatta non e coperta da quella misura.
        //
        // 10.11 e entrata per ultima, ed e la piu vecchia: due cicli di
        // sviluppo indietro rispetto alla riga di evidenza, e la LTS che piu
        // gente ha davvero in produzione. Le cento sonde della campagna danno
        // su di lei lo stesso esito che danno sulle altre due — l'unica cosa
        // che la fermava era una variabile di sessione che il profilo ora
        // possiede.
        //
        // La riga che rende il rifiuto onesto e il messaggio: dice che la
        // versione non e stata misurata, non che non funzioni. Perche quel
        // messaggio arrivi davvero, la qualifica va decisa **prima** di
        // interrogare qualunque variabile di sessione: era dentro la stessa
        // query, e su 10.11 moriva prima di poter parlare.
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
        // prima tranche aveva gia misurato lo stesso rifiuto per
        // `REF_SYSTEM_ID`. Non esiste una DDL che vincoli una colonna
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
        // Le bandiere si aprono una alla volta, ciascuna con la sua misura.
        // Ereditare la tabella di MySQL era il difetto originale che ADR 0010
        // e 0014 hanno nominato: dichiarava qualificate sei write mode e
        // l'intera lista spatial di `MySQL` su un prodotto su cui nessuno le
        // aveva provate — e quella lista, misurata, era sbagliata pure su
        // `MySQL`.
        //
        // La lettura e la prima ad aprirsi, ed e la quinta tranche a
        // sostenerla, con sonde che verificano un contratto invece di
        // limitarsi a non fallire: `provider.profile_read_values` ha
        // decodificato le quattordici famiglie di tipo con lo stesso digest
        // sui tre riferimenti, `provider.profile_read_streaming` ha consegnato
        // due batch su 8193 righe — il taglio del lettore, non un caso — e
        // `provider.profile_read_filter_forms` ha verificato **tutte e
        // tredici** le forme che il renderer qualifica, ciascuna con il
        // proprio conteggio e la propria prima riga.
        //
        // `filter` significa quelle tredici, non "qualunque filtro": le due
        // che il renderer rifiuta — `LIKE` case-insensitive e il filtro
        // spatial — hanno una sonda che verifica che restino rifiutate.
        //
        // Le scritture sono arrivate dopo, una tranche per volta, e ciascuna
        // ha la propria nota accanto alla propria bandiera. Questa frase
        // diceva che erano chiuse per intero, ed e rimasta a dirlo mentre sei
        // mode si aprivano una sotto l'altra: un cappello che riassume un
        // elenco invecchia sempre prima dell'elenco.
        ProviderCapabilities {
            schema_version: 2,
            provider: self.kind(),
            provider_version,
            extension_versions: BTreeMap::new(),
            reads: ReadCapabilities {
                // `server_cursor` e `resumable` restano false perche il crate
                // non li offre a nessuno dei due prodotti: sono chiusi anche
                // per MySQL, e qui non c'e niente da qualificare. Fra i due
                // c'era anche `pagination`, che nel frattempo si e aperta tre
                // righe piu sotto — con la sua misura — senza che questo
                // elenco se ne accorgesse.
                //
                // `streaming` significa che le righe arrivano a blocchi, non
                // che esista un cursore: `query_stream` fa scorrere il result
                // set sul filo, ed e per questo che la bandiera accanto dice
                // `false`.
                streaming: true,
                server_cursor: false,
                // La finestra si rende, e da oggi la bandiera la governa:
                // l'engine rifiuta un `row_offset` a un provider che non la
                // pubblica. Il piano di lettura la compila come `LIMIT ...
                // OFFSET n`, con il tetto del tipo quando il chiamante non ne
                // ha chiesto uno — `OFFSET` da solo non e sintassi valida su
                // questi motori.
                pagination: true,
                projection: true,
                filter: true,
                ordering: true,
                resumable: false,
            },
            // `append` e la prima write mode aperta, e ora la bandiera e sua
            // soltanto: `truncate_insert` ha la propria, e resta chiusa. Le
            // tre sonde della settima tranche la sostengono, verdi su
            // entrambi i riferimenti e bloccanti nella campagna — le righe
            // arrivano e si rileggono da un'altra sessione, un secondo batch
            // rifiutato dal server annulla anche il primo, una cancellazione
            // a meta scrittura non lascia righe e il provider resta usabile.
            //
            // `rollback_on_failure` e aperta, e l'argomento con cui era
            // rimasta chiusa non reggeva. Il flag parla delle righe di
            // qualunque scrittura *che questo profilo ammette*, e ne ammette
            // una: `Append`. Le tre sonde della settima tranche girano con
            // `allow_partial: false` — proprio il piano che la bandiera
            // governa — e misurano l'esito che promette: un secondo batch
            // rifiutato annulla anche il primo, `RemoteEffect::RolledBack`
            // dichiarato e la rilettura da un'altra sessione a confermarlo.
            //
            // L'obiezione della cancellazione era fuori bersaglio: `Unknown`
            // e l'effetto di una **cancellazione**, non di un fallimento, e su
            // quel percorso nessun provider promette nulla — PostgreSQL
            // pubblica `true` e ha lo stesso esito ignoto a commit interrotto.
            // Tenendola chiusa, MariaDB rifiutava in `prepare` esattamente il
            // piano su cui la campagna aveva raccolto le prove.
            // `create` si apre con l'ottava tranche, e le tre sonde che la
            // sostengono verificano una cosa che quelle dell'Append non
            // potevano: cosa resta sul server. Su questi due motori
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
                // Le ultime quattro, aperte dalla nona tranche con tre sonde
                // ciascuna. Cio che distingue queste mode dalle due
                // precedenti sono le keys — una chiave che non trova
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
                // l'implementazione e condivisa, e le dodici sonde della nona
                // tranche l'hanno attraversata con due batch ciascuna.
                //
                // La prima stesura la lasciava chiusa con l'argomento che
                // nessun codice la consulta. L'argomento e vero — e il campo e
                // ora dichiarato descrittivo, con la sua guardia — ma non
                // riguarda questo profilo: da «nessuno la fa rispettare» non
                // segue «questo prodotto fa una cosa diversa dal gemello con
                // cui condivide il codice». Sarebbe stata una divergenza
                // inventata.
                bulk: true,
                array_binding: false,
                returning: false,
                rollback_on_failure: true,
            },
            // La terza tranche ha misurato commit, rollback e isolamento:
            // tredici sonde di sessione su tredici, stesso esito sui tre
            // riferimenti.
            //
            // `savepoints` si apre con la quattordicesima, e restava chiusa per
            // una ragione che non era «il prodotto non li ha»: nessuna sonda li
            // aveva toccati, e un savepoint dichiarato e non provato e proprio
            // il genere di promessa che si scopre rotta durante un rollback
            // parziale. Ora due sonde lo attraversano — il rollback parziale
            // lascia la sola riga scritta prima del savepoint, riletta da
            // un'altra connessione, e un nome mai creato viene rifiutato. La
            // seconda e cio che rende vera la prima: senza, un motore che
            // dicesse di si a qualunque `ROLLBACK TO` supererebbe comunque il
            // controllo sul conteggio.
            transactions: TransactionCapabilities {
                single_transaction: true,
                savepoints: true,
                transactional_ddl: false,
                staged_swap: false,
                scope: TransactionScope::Transaction,
            },
            // La lettura geometrica si apre con la dodicesima tranche, e non
            // da sola: si apre **insieme** alla condizione che la rende vera.
            //
            // Le tre sonde del CRS dichiarato girano su una colonna `GEOMETRY`
            // che nessuna DDL vincola — l'unica forma che MariaDB ammette — e
            // misurano tre esiti diversi: senza dichiarazione la colonna resta
            // rifiutata, con la dichiarazione giusta le righe arrivano, con
            // una dichiarazione che i valori smentiscono la lettura fallisce
            // alla riga che la smentisce. La terza e quella che rende vera la
            // seconda: senza, `geometry: true` significherebbe che il provider
            // ripete cio che il chiamante gli ha detto.
            spatial: SpatialCapabilities {
                read_wkb: true,
                write_wkb: self.write_spatial_is_qualified(),
                geometry: true,
                // `geography` non esiste su questo prodotto, e non e una
                // lacuna di misura.
                geography: false,
                // Aperta dalla diciottesima tranche. Il fatto del server lo
                // aveva misurato la diciassettesima — `SPATIAL INDEX` su una
                // colonna non vincolata, l'unica forma che questo prodotto
                // ammette — e mancava il percorso: il piano rifiutava
                // `create_spatial_index` in prepare, e una capability descrive
                // cio che il provider sa fare.
                spatial_index: true,
                // Aperta dalla diciassettesima tranche: un punto e un poligono
                // scritti nella stessa colonna e riletti per tipo, identici sui
                // tre riferimenti. Le sonde di scrittura precedenti portavano
                // soltanto punti, quindi `mixed` era una dichiarazione che
                // nessuna misura attraversava — la colonna avrebbe retto anche
                // se il prodotto avesse ammesso un tipo solo.
                mixed_geometry_types: true,
                // Solo XY, e non perche le altre non siano state provate:
                // `raw.geometry_dimensions` ha chiesto al parser `POINT Z` nelle
                // due sintassi WKT e ha avuto `NULL` qui e 3037 su `MySQL`, e
                // `ST_Z` e `ST_M` sono assenti da entrambi. Non c'e una terza
                // dimensione da dichiarare.
                dimensions: vec![plenora_database_core::geometry::Dimensions::Xy],
                // Quattordici, misurate da `provider.profile_spatial_functions`
                // attraversando il percorso di query su entrambe le major. La
                // quindicesima di MySQL — `IsValid` — resta fuori: la 12.3 ce
                // l'ha, la 11.8 LTS risponde 1305, e una capability e una
                // promessa a chi non sa su quale minor atterrera.
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
        // Aperta dalla quindicesima tranche, e questa bandiera e sia la
        // capability pubblicata sia il cancello che il piano consulta — una
        // sola origine, per scelta dichiarata. Le due cose insieme hanno un
        // effetto che va detto: finche era `false`, nessuna sonda poteva
        // attraversare il percorso di scrittura, perche `compile_write_column`
        // si fermava prima. Le prove e l'apertura sono quindi arrivate nello
        // stesso commit, e la campagna e stata cio che le ha rese vere.
        //
        // Cosa sostiene l'apertura, in ordine. `raw.spatial_write_forms` ha
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
        // Senza questa riga il profilo emetteva l'istruzione giusta e poi
        // classificava il suo esito nel ramo generico: il chiamante leggeva
        // "errore server MariaDB redatto (codice 1969)" invece di un timeout,
        // cioe non poteva distinguere un limite che ha fatto il suo lavoro da
        // un guasto. Era il difetto che l'istruzione corretta nascondeva.
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
    Some(DatabaseError {
        category: ErrorCategory::Unsupported,
        phase: ErrorPhase::Probe,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(profile.kind()),
        execution_id: None,
        message: format!(
            "versione {product} {product_version:?} non misurata: il profilo e \
             qualificato su {declared}. Non e un difetto del server — e \
             una versione su cui nessuna prova e stata fatta."
        ),
        diagnostics: None,
    })
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
/// I numeri vengono dal protocollo `MySQL`, che `MariaDB` parla, e la quarta
/// tranche di ADR 0014 li ha osservati uno per uno: 1045, 1048, 1054, 1062,
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
        // tabella. Sono la stessa risposta a due domande vicine, e finivano in
        // due posti diversi: 1142 non era in tabella, quindi un errore di
        // privilegio si classificava come esecuzione generica — cioe come un
        // guasto, invece che come "questo utente non puo".
        //
        // La quarta tranche l'ha visto arrivare identico dai tre riferimenti,
        // ed e per questo che la riga si allarga adesso: la classificazione di
        // un provider qualificato si cambia con una misura sotto, non con una
        // lettura della documentazione.
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
        // I tre codici che dicono «questa riga non va bene», e che fino alla
        // nona tranche arrivavano come guasto generico fuori dall'`Append`.
        //
        // La diagnostica per riga si attiva **solo** per `Append`: li 1048 e
        // 1406 diventano un rifiuto di riga con la sua causa. Per ogni altra
        // mode la scrittura e un bulk, il codice passa da qui, e da qui
        // usciva `Execution`/`Never` — «l'operazione e fallita sul server e
        // ritentarla non ha ragione di riuscire». Vero, e inutile: un dato
        // troppo lungo lo corregge chi chiama, un guasto no, e sono due
        // rimedi diversi. E' la stessa lacuna che la quarta tranche ha chiuso
        // su 1142, dove un permesso mancante si presentava come guasto.
        //
        // Tutti e tre arrivano identici dai tre riferimenti: 1048 e 1452 dalla
        // quarta tranche, 1406 dalle sonde di rollback di Upsert e Replace,
        // 1451 da quella di DeleteByKeys.
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
/// Dalla **quarta** tranche: una password sbagliata, una colonna che non
/// esiste, una chiave duplicata, una tabella assente, un'attesa di lock
/// scaduta, un deadlock con la vittima annullata, un permesso mancante, una
/// colonna non nullable senza valore, un vincolo referenziale violato in
/// inserimento.
///
/// Dalla **nona**: un valore piu lungo della colonna, e una cancellazione
/// trattenuta da una figlia.
///
/// Quattro di questi — 1048, 1406, 1451, 1452 — sono entrati qui insieme alla
/// classificazione condivisa che li riguarda, e la campagna ha mostrato perche
/// serve entrambe le cose: aggiungerli **solo** alla tabella li lasciava
/// generici su `MariaDB`, dove passano da questo filtro. Due dei quattro erano
/// misurati da mesi e non erano in elenco, perche fino ad allora non c'era
/// niente da ereditare.
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
        // Una geometria in uscita da una query resta chiusa, e la ragione non
        // e piu «manca un preflight»: e stata misurata, e non e la stessa che
        // il percorso di lettura ha risolto.
        //
        // Li il CRS di una **colonna** puo essere dichiarato dal chiamante e
        // verificato valore per valore, perche i valori lo portano. Qui la
        // geometria e **calcolata**, e `raw.geometry_result_forms` dice cosa
        // ne resta:
        //
        // * su `MySQL`, in un sistema di riferimento geografico — 4326, cioe
        //   il caso comune — `ST_Envelope`, `ST_Centroid` e `ST_Buffer`
        //   rispondono 3618: non sono implementate. Non c'e CRS da verificare
        //   perche non c'e risultato;
        // * su `MariaDB` funzionano, ma il CRS sopravvive **a seconda della
        //   funzione**: `ST_Envelope` e `ST_Centroid` conservano 4326,
        //   `ST_Buffer` rende 0;
        // * in cartesiano entrambi rendono 0 ovunque, che e l'indefinito OGC:
        //   pubblicarlo come CRS direbbe una cosa che nessuno ha dichiarato.
        //
        // Aprire questa superficie richiederebbe percio una regola di CRS per
        // **funzione e per tipo di sistema di riferimento**, misurata una
        // funzione alla volta — non un preflight. Finche quella regola non
        // esiste, il contratto `GeoArrow` non ha un CRS da pubblicare, e
        // pubblicarne uno inventato e l'unico esito peggiore del rifiuto.
        ColumnType::MYSQL_TYPE_GEOMETRY => {
            return Err(unsupported(format!(
                "geometria calcolata {product} senza un CRS dimostrabile nel path query"
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
        read_wkb: true,
        write_wkb: true,
        geometry: true,
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
        // per una. Qui non c'e il numero: e cambiato due volte — venti,
        // ventisei, quindici — e ogni volta questo commento era l'ultimo a
        // saperlo. Il numero sta nella costante, che e anche l'unico posto
        // dove qualcuno lo puo cambiare con una misura in mano.
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
mod tests {
    use super::{
        ProductProfile, COLUMN_ALIASES, INDEX_PART_ALIASES, MARIADB_PROFILE,
        MARIADB_STATEMENT_TIMEOUT, MEASURED_SERVER_CODES, MYSQL_PROFILE, OBJECT_ALIASES,
        SCHEMA_ALIASES, SECOND_PRODUCT_PROFILE,
    };
    use crate::types::MysqlColumnKind;
    use crate::MysqlConfig;
    use mysql_async::consts::ColumnType;
    use mysql_async::Column;
    use plenora_database_core::arrow::schema::{DataType, Field, Schema, SchemaRef};
    use plenora_database_core::loss::MappingPolicy;
    use plenora_database_core::plan::{
        ComparisonOperator, ObjectRef, TransactionProfile, WriteMode, WriteOperation,
    };
    use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource,
    };
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use plenora_database_core::CancellationToken;
    use plenora_database_core::{
        plan::ProviderKind, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
    };

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
        // produzioni del `native_type` e un secondo profilo ne cambierebbe
        // una sola.
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
        let mut presidiati = 0;
        for (module, source) in GUARDED_MODULES {
            let mut inspected = 0;
            // Ogni `impl` dei due trait, non solo quello di oggi: quando
            // nascera un secondo provider dovra essere presidiato senza che
            // nessuno si ricordi di aggiungerlo qui.
            // Gli intestatori si compongono a runtime: scritti per intero
            // comparirebbero in questo file, e la guardia ispezionerebbe se
            // stessa trovando zero metodi.
            let trait_headers = [
                format!("impl {} for ", "Provider"),
                format!("impl {} for ", "TransactionScope"),
            ];
            let headers: Vec<&str> = trait_headers
                .iter()
                .flat_map(|header| source.match_indices(header.as_str()).map(|(at, _)| at))
                .collect::<Vec<_>>()
                .iter()
                .map(|at| &source[*at..])
                .collect();
            if headers.is_empty() {
                continue;
            }
            presidiati += 1;
            for tail in headers {
                let end = tail
                    .find(format!("{}}}", '\n').as_str())
                    .map_or(tail.len(), |at| at);
                let block = &tail[..end];
                let mut methods = 0;
                for method in block.split(format!("{}    fn ", '\n').as_str()).skip(1) {
                    let name = method.split(['(', '<']).next().unwrap_or("?");
                    // Una delega pura non ristampa, e non deve: a ristampare e
                    // il provider interno, costruito con il profilo del
                    // prodotto. `MariadbProvider` e fatto cosi — un newtype
                    // che inoltra tutto — e pretendere il timbro qui vorrebbe
                    // dire timbrare due volte lo stesso bordo.
                    //
                    // L'eccezione e stretta apposta: vale solo se il corpo
                    // inoltra **lo stesso metodo** al campo interno. Una
                    // delega a un'operazione diversa, o a un altro oggetto,
                    // non la soddisfa — e sarebbe proprio il caso in cui
                    // l'attribuzione puo divergere senza che si veda.
                    if method.contains(&format!("self.0{}{name}(", '.')) {
                        methods += 1;
                        continue;
                    }
                    if !method.contains(boxed.as_str()) {
                        continue;
                    }
                    methods += 1;
                    assert!(
                        method.contains(stamped.as_str()),
                        "{module}::{name} restituisce un futuro senza ristampare l'attribuzione"
                    );
                }
                assert!(
                    methods >= 1,
                    "{module}: nessun metodo ispezionato in un impl presidiato"
                );
                inspected += methods;
            }
            assert!(
                inspected >= 8,
                "{module}: solo {inspected} metodi ispezionati in totale"
            );
        }
        assert_eq!(
            presidiati, 2,
            "i due trait devono vivere in due moduli: trovati {presidiati}"
        );
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
            },
            alias: None,
        });

        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &ParameterBag::default(),
                &ResourceBudget::new(ResourceLimits::default()).expect("budget"),
                &CancellationToken::new(),
            )
            .await;
        let Err(error) = outcome else {
            panic!("identificatore oltre il limite: doveva fallire");
        };
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
        // E cio che il profilo pubblica: sei mode su sette, `TruncateInsert`
        // fail-closed, e lo spatial completo — l'indice compreso, da quando la
        // clausola entra nella `CREATE TABLE` della mode `Create`.
        let published = MYSQL_PROFILE.capabilities("9.7.2".to_owned());
        assert_eq!(published.provider_version, "9.7.2");
        assert_eq!(published.provider, MYSQL_PROFILE.kind());
        assert!(published.spatial.read_wkb && published.spatial.write_wkb);
        assert!(published.spatial.spatial_index);
        assert_eq!(
            published.limits.max_bind_parameters,
            Some(crate::MAX_BIND_PARAMETERS as u64)
        );
    }

    #[test]
    fn every_catalog_query_exposes_the_aliases_its_reader_requires() {
        // Per **ogni** profilo, non solo per quello che oggi ha un provider:
        // il contratto degli alias esiste proprio perche un prodotto a cui
        // manca una colonna la dichiari nulla invece di ometterla, e con un
        // profilo solo nell'elenco quella regola non sarebbe mai verificata
        // dove serve.
        for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
            for (label, sql, aliases) in [
                ("schemi", profile.schemas_query(), SCHEMA_ALIASES),
                ("oggetti", profile.objects_query(), OBJECT_ALIASES),
                ("oggetto", profile.object_query(), OBJECT_ALIASES),
                ("colonne", profile.object_columns_query(), COLUMN_ALIASES),
                ("indici", profile.object_indexes_query(), INDEX_PART_ALIASES),
            ] {
                for alias in aliases {
                    assert!(
                        sql.contains(format!("AS {alias}").as_str()),
                        "{}: la query {label} non espone {alias}",
                        profile.product()
                    );
                }
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
        // senza esiste solo per chi il profilo non ce l'ha. Chiamandola da
        // dentro il crate si perde silenziosamente il prodotto, ed e successo
        // due volte — con il pool e con la compilazione della scrittura.
        //
        // La verifica copre tutti i moduli di produzione, non il solo
        // provider: un secondo provider vivrebbe in un file nuovo, e una
        // guardia che nomina i file da ispezionare invecchia esattamente
        // quando serve.
        let entries = [
            format!("Mysql{}::new", "Pool"),
            format!("probe{}server", "_"),
            format!("list{}schemas", "_"),
            format!("list{}objects", "_"),
            format!("describe{}object", "_"),
            format!("read{}operation", "_"),
            format!("query{}operation", "_"),
            format!("MysqlReadPlan::{}", "compile"),
            format!("from{}catalog", "_"),
            format!("query{}result{}columns", "_", "_"),
        ];
        for (module, source) in GUARDED_MODULES {
            let production = source
                .split_once(format!("{}mod tests {{", '\n').as_str())
                .map_or(*source, |(head, _)| head);
            for entry in &entries {
                let needle = format!("{entry}(");
                for at in production.match_indices(needle.as_str()).map(|(at, _)| at) {
                    // Confine di parola: `validate_query_operation` finisce
                    // con l'ago senza esserlo, e senza questo controllo la
                    // guardia grida su una funzione che non c'entra.
                    let head = &production[..at];
                    if head
                        .chars()
                        .next_back()
                        .is_some_and(|c| c.is_alphanumeric() || c == '_')
                    {
                        continue;
                    }
                    // La definizione non e una chiamata: e proprio li che la
                    // forma senza profilo deve continuare a esistere.
                    assert!(
                        head.trim_end().ends_with("fn"),
                        "{module} chiama {entry} senza profilo"
                    );
                }
            }
        }
    }

    /// Il messaggio non deve limitarsi a non contraddire l'attribuzione:
    /// deve nominare il prodotto servito. Asserire il solo `provider` e cio
    /// che ha lasciato passare il residuo del pool per una review intera.
    fn assert_names_the_second_product(error: &plenora_database_core::DatabaseError, what: &str) {
        assert_eq!(error.provider, Some(ProviderKind::Mariadb), "{what}");
        assert!(
            error.message.contains("SecondProduct"),
            "{what}: il messaggio non nomina il prodotto — {}",
            error.message
        );
        assert!(
            !error.message.contains("MySQL"),
            "{what}: il messaggio nomina ancora MySQL — {}",
            error.message
        );
    }

    /// Un errore che nasce dove il profilo non arriva non deve nominare
    /// nessun prodotto: il bordo ne corregge l'attribuzione, non il testo.
    fn assert_names_no_product(error: &plenora_database_core::DatabaseError, what: &str) {
        assert_eq!(error.provider, Some(ProviderKind::Mariadb), "{what}");
        assert!(
            !error.message.contains("MySQL"),
            "{what}: il messaggio nomina MySQL — {}",
            error.message
        );
        assert!(
            !error.message.contains("SecondProduct"),
            "{what}: il messaggio non puo nominare un prodotto che non conosce — {}",
            error.message
        );
        // E deve restare una frase. Togliere il nome del prodotto da un
        // messaggio ne ha lasciati alcuni senza soggetto o con la
        // punteggiatura sospesa: un errore pubblico degradato non e un
        // dettaglio di forma.
        assert!(
            !error.message.contains(" :")
                && !error.message.contains("  ")
                && !error.message.contains(" ,"),
            "{what}: punteggiatura sospesa — {}",
            error.message
        );
    }

    fn append_to_warehouse() -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
            },
            mode: WriteMode::Append,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    /// Una query che dichiara un parametro: senza fornirlo, il binding
    /// fallisce prima di qualunque connessione.
    fn parameterized_query() -> QueryOperation {
        let mut query = oversized_identifier_query();
        query.source = Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
            },
            alias: None,
        });
        query.filter = Some(QueryExpression::Compare {
            left: Box::new(QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            }),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "wanted".to_owned(),
            }),
        });
        query
    }

    fn schema_with(fields: Vec<Field>) -> SchemaRef {
        std::sync::Arc::new(Schema::new(fields))
    }

    fn second_product_config() -> MysqlConfig {
        MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        )
    }

    #[test]
    fn the_constructor_attributes_its_own_failures_to_the_profile() {
        // I due errori che un consumatore puo vedere senza aver mai toccato
        // il server. Uscivano entrambi con il segnaposto, e il test
        // precedente non li vedeva perche usava solo configurazioni valide.
        let invalid = MysqlConfig::new("", "warehouse", "loader", SecretString::new("s"));
        let error = crate::MysqlProvider::with_profile(invalid, 2, &SECOND_PRODUCT_PROFILE)
            .expect_err("configurazione invalida");
        assert_names_the_second_product(&error, "configurazione invalida");

        let error =
            crate::MysqlProvider::with_profile(second_product_config(), 0, &SECOND_PRODUCT_PROFILE)
                .expect_err("pool a capacita zero");
        assert_names_the_second_product(&error, "pool a capacita zero");
    }

    #[test]
    fn a_diverging_profile_changes_timeout_classification_and_spatial() {
        // Se il secondo profilo divergesse solo sull'identita, proverebbe il
        // transito dell'attribuzione e nient'altro: le altre decisioni
        // resterebbero indistinguibili da una tabella ereditata.

        // Timeout: nome e unita insieme, che e la forma della divergenza.
        let mysql = MYSQL_PROFILE.statement_timeout_statement(5_000);
        let second = SECOND_PRODUCT_PROFILE.statement_timeout_statement(5_000);
        assert_ne!(mysql, second);
        assert!(mysql.contains("5000"), "{mysql}");
        assert!(second.ends_with(" 5.000"), "{second}");
        // I due casi che una conversione approssimata sbaglierebbe in
        // direzioni opposte: la divisione intera porterebbe 200 ms a zero,
        // cioe a "nessun limite"; l'arrotondamento per eccesso li porterebbe
        // a un secondo, cioe a un timeout cinque volte piu lasco di quello
        // chiesto. La conversione esatta non perde nulla.
        assert!(
            SECOND_PRODUCT_PROFILE
                .statement_timeout_statement(200)
                .ends_with(" 0.200"),
            "sotto il secondo la conversione deve restare esatta"
        );
        assert!(
            SECOND_PRODUCT_PROFILE
                .statement_timeout_statement(1)
                .ends_with(" 0.001"),
            "un millisecondo non puo sparire"
        );

        // Classificazione: lo stesso codice, due significati.
        assert_eq!(
            MYSQL_PROFILE.classify_server_code(1_054).category,
            ErrorCategory::Schema
        );
        assert_eq!(
            SECOND_PRODUCT_PROFILE.classify_server_code(1_054).category,
            ErrorCategory::Unsupported
        );
        // E l'effetto remoto, che prima il chiamante decideva da solo.
        assert_eq!(
            MYSQL_PROFILE.classify_server_code(1_213).remote_effect,
            Some(RemoteEffect::RolledBack)
        );
        assert_eq!(
            MYSQL_PROFILE.classify_server_code(1_062).remote_effect,
            None
        );

        // Spatial: una sola origine, e il profilo che non ha la prova la nega
        // in entrambi i posti.
        assert!(MYSQL_PROFILE.write_spatial_is_qualified());
        assert!(!SECOND_PRODUCT_PROFILE.write_spatial_is_qualified());
        for profile in [
            &MYSQL_PROFILE as &dyn ProductProfile,
            &SECOND_PRODUCT_PROFILE as &dyn ProductProfile,
        ] {
            assert_eq!(
                profile.capabilities("9.7.2".to_owned()).spatial.write_wkb,
                profile.write_spatial_is_qualified(),
                "capability e decisione devono avere una sola origine"
            );
        }
    }

    /// Ogni modulo di produzione del crate, con il proprio sorgente.
    ///
    /// Le guardie strutturali non nominano piu i moduli che ispezionano: un
    /// `mariadb_provider.rs` nato domani sarebbe rimasto fuori da una lista
    /// scritta a mano, e le guardie avrebbero continuato a passare dicendo
    /// una cosa che non era piu vera. La lista qui e presidiata a sua volta
    /// contro le dichiarazioni `mod` di `lib.rs`.
    const GUARDED_MODULES: &[(&str, &str)] = &[
        ("arrow.rs", include_str!("arrow.rs")),
        ("catalog.rs", include_str!("catalog.rs")),
        ("config.rs", include_str!("config.rs")),
        ("error.rs", include_str!("error.rs")),
        ("parameter.rs", include_str!("parameter.rs")),
        ("pool.rs", include_str!("pool.rs")),
        ("profile.rs", include_str!("profile.rs")),
        ("provider.rs", include_str!("provider.rs")),
        ("query.rs", include_str!("query.rs")),
        ("read.rs", include_str!("read.rs")),
        ("row_diagnostics.rs", include_str!("row_diagnostics.rs")),
        ("session.rs", include_str!("session.rs")),
        ("transaction.rs", include_str!("transaction.rs")),
        ("types.rs", include_str!("types.rs")),
        ("write.rs", include_str!("write.rs")),
    ];

    #[test]
    fn no_literal_carries_a_collapsed_continuation() {
        // Una riga spezzata in un literal Rust si scrive con `\` a fine riga:
        // il compilatore toglie l'a capo **e** l'indentazione, e il messaggio
        // torna una frase sola. Se quel `\` si perde — succede scrivendo il
        // codice con uno strumento che lo mangia — la stringa resta valida e
        // compila, ma porta dentro l'indentazione: quello che il chiamante
        // legge diventa "la transazione                 non e cominciata".
        //
        // Non e un difetto di stile. Un messaggio d'errore e cio che qualcuno
        // legge alle tre di notte, e una riga sfondata da venti spazi lo
        // rende illeggibile proprio dove serve. Il compilatore non se ne
        // accorge, i test sul contenuto nemmeno — cercano sottostringhe corte
        // — e la review l'ha trovato a occhio: e il genere di cosa che va
        // presa da una guardia.
        //
        // Un `\n` esplicito seguito da indentazione e un'altra cosa: e SQL
        // scritto su piu righe, dove l'a capo appartiene alla stringa. Quello
        // resta legittimo, ed e l'unica eccezione.
        let sources: [(&str, &str); 9] = [
            ("arrow.rs", include_str!("arrow.rs")),
            ("catalog.rs", include_str!("catalog.rs")),
            ("evidence.rs", include_str!("evidence.rs")),
            ("live_tests.rs", include_str!("live_tests.rs")),
            ("mariadb_evidence.rs", include_str!("mariadb_evidence.rs")),
            ("profile.rs", include_str!("profile.rs")),
            ("session_evidence.rs", include_str!("session_evidence.rs")),
            ("transaction.rs", include_str!("transaction.rs")),
            ("types.rs", include_str!("types.rs")),
        ];
        let run = " ".repeat(4);
        for (module, source) in sources {
            for (at, line) in source.lines().enumerate() {
                let trimmed = line.trim_start();
                // Le righe di commento sono prosa: l'allineamento di una
                // tabella in un commento non finisce in nessun messaggio.
                if trimmed.starts_with("//") {
                    continue;
                }
                let Some(quoted) = trimmed.split_once('"').map(|(_, rest)| rest) else {
                    continue;
                };
                let Some(offset) = quoted.find(run.as_str()) else {
                    continue;
                };
                if quoted[..offset].ends_with(r"\n") {
                    continue;
                }
                assert!(
                    !quoted[..offset]
                        .chars()
                        .next_back()
                        .is_some_and(
                            |character| character.is_alphanumeric() || ".,:;)'".contains(character)
                        ),
                    "{module}:{}: literal con una continuazione persa — {trimmed}",
                    at + 1
                );
            }
        }
    }

    #[test]
    fn only_the_mariadb_provider_selects_the_mariadb_profile() {
        // Questa guardia diceva un'altra cosa, ed e stata riscritta il giorno
        // in cui ha smesso di essere vera — che era il suo scopo. Diceva:
        // «il profilo esiste, nessun provider lo sceglie», perche allora
        // `MariadbProfile` dichiarava chiuse quasi tutte le capability e un
        // percorso che ci fosse arrivato non avrebbe aperto MariaDB, l'avrebbe
        // fatta fallire in posti scelti a caso.
        //
        // Ora il provider c'e e le capability sono misurate, quindi la
        // proprieta da presidiare cambia ma non sparisce: la selezione deve
        // restare **una sola**, e dichiarata. Un secondo punto che scegliesse
        // quel profilo sarebbe una selezione che nessuno ha deciso, ed e
        // esattamente cio che ADR 0014 esclude quando dice «nessuna selezione
        // automatica».
        let marker = format!("{}mod tests {{", '\n');
        // L'intestazione, non il blocco: il corpo comincia con un fine riga,
        // che nel sorgente incluso puo essere `\n` o `\r\n` a seconda di come
        // il checkout ha normalizzato il file. Una guardia che dipende da quel
        // dettaglio fallisce su meta delle macchine per una ragione che non
        // riguarda cio che sorveglia.
        let declaration = format!("impl PublishedProfile for {}Provider", "Mariadb");
        for (module, source) in GUARDED_MODULES {
            let production = source
                .split_once(marker.as_str())
                .map_or(*source, |(head, _)| head);
            match *module {
                // Dove il profilo e definito: cercarlo qui vorrebbe dire
                // vietarne l'esistenza.
                "profile.rs" => {}
                // Dove e dichiarato, una volta sola e dentro l'`impl` che lo
                // pubblica. Il conteggio e la meta che conta: senza, una
                // seconda occorrenza altrove nel file passerebbe.
                "provider.rs" => {
                    assert_eq!(
                        production.matches("MARIADB_PROFILE").count(),
                        1,
                        "provider.rs nomina il profilo MariaDB piu di una volta: \
                         la selezione non e piu una sola"
                    );
                    let header = production
                        .find(declaration.as_str())
                        .expect("provider.rs non dichiara il profilo di MariadbProvider");
                    let selection = production
                        .find("MARIADB_PROFILE")
                        .expect("occorrenza gia contata");
                    // Dentro la dichiarazione, non da qualche parte dopo: fra
                    // l'intestazione e la costante ci sono poche decine di
                    // caratteri, e una selezione piu in la nel file sarebbe
                    // un secondo punto travestito da primo.
                    assert!(
                        selection > header && selection - header < 200,
                        "provider.rs nomina il profilo MariaDB fuori dalla \
                         dichiarazione che lo pubblica"
                    );
                }
                _ => {
                    for needle in ["MARIADB_PROFILE", "MariadbProfile"] {
                        assert!(
                            !production.contains(needle),
                            "{module} seleziona il profilo MariaDB, che solo \
                             MariadbProvider deve dichiarare"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn the_guarded_module_list_covers_every_production_module() {
        // I moduli di solo test non contano: non esistono nel binario che il
        // consumatore riceve, ed e quello che le guardie presidiano.
        //
        // Il riconoscimento accetta qualunque visibilita — `mod x;`,
        // `pub(crate) mod x;`, `pub mod x;` — perche la prima stesura
        // riconosceva solo la forma nuda, e un modulo dichiarato altrimenti
        // sarebbe rimasto fuori senza che nulla fallisse.
        let source = include_str!("lib.rs");
        let lines: Vec<&str> = source.lines().collect();
        let mut declared = Vec::new();
        for (at, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            let Some(rest) = trimmed
                .strip_prefix("pub(crate) mod ")
                .or_else(|| trimmed.strip_prefix("pub mod "))
                .or_else(|| trimmed.strip_prefix("mod "))
            else {
                continue;
            };
            // Un modulo inline (`mod x {`) non ha un file da ispezionare, e
            // uno annidato sfuggirebbe comunque a questa lettura: entrambi
            // vanno vietati, non ignorati.
            assert!(
                rest.ends_with(';'),
                "lib.rs dichiara un modulo inline: le guardie leggono file, non blocchi — {trimmed}"
            );
            let name = rest.trim_end_matches(';');
            assert!(
                !name.contains("::"),
                "lib.rs dichiara un modulo annidato: {name}"
            );
            if at > 0 && lines[at - 1].trim().starts_with("#[cfg(test)]") {
                continue;
            }
            declared.push(format!("{name}.rs"));
        }
        assert!(declared.len() >= 15, "moduli dichiarati: {declared:?}");
        for module in &declared {
            assert!(
                GUARDED_MODULES.iter().any(|(name, _)| name == module),
                "{module} e dichiarato in lib.rs ma nessuna guardia lo ispeziona"
            );
        }
        // E nessun modulo dichiarato altrove: i sorgenti del crate sono i
        // file di `src`, e un file non dichiarato non compila comunque, ma
        // uno dichiarato in un sottomodulo si.
        for (module, source) in GUARDED_MODULES {
            // Solo la produzione: il modulo di test e annidato per
            // definizione, e non e cio che le guardie devono ispezionare.
            let production = source
                .split_once(format!("{}mod tests {{", '\n').as_str())
                .map_or(*source, |(head, _)| head);
            // Qualunque dichiarazione `mod`, non solo la forma nuda seguita
            // da graffa: la prima stesura cercava un a capo seguito da
            // `mod ` e rifiutava solo le righe che finivano con `{`, quindi
            // `mod mariadb;` passava e `pub mod mariadb` non veniva
            // nemmeno vista. Le guardie leggono file: un modulo che non ha
            // un file proprio, o che non e dichiarato in `lib.rs`, resta
            // fuori da ogni ispezione senza che nulla lo segnali.
            for (at, line) in production.lines().enumerate() {
                let trimmed = line.trim();
                // Il riconoscimento non elenca piu le visibilita: `pub(self)`
                // e Rust valido e mancava, e un elenco di prefissi si elude
                // con uno spazio in piu fra i token. Si guarda la prima
                // parola dopo l'eventuale visibilita, qualunque essa sia.
                let rest = trimmed.strip_prefix("pub").map_or(trimmed, |after| {
                    let after = after.trim_start();
                    after.strip_prefix('(').map_or(after, |scoped| {
                        scoped
                            .find(')')
                            .map_or(after, |at| scoped[at + 1..].trim_start())
                    })
                });
                let is_module_declaration = rest
                    .strip_prefix("mod")
                    .is_some_and(|after| after.starts_with(char::is_whitespace));
                assert!(
                    !is_module_declaration,
                    "{module}:{} dichiara un modulo fuori da lib.rs: {trimmed}",
                    at + 1
                );
            }
        }
    }

    #[test]
    fn the_pool_keeps_naming_the_product_when_it_rebuilds_the_options() {
        // Il provider valida in costruzione, il pool ricostruisce le opzioni
        // al primo checkout: fra i due momenti la configurazione puo essere
        // diventata invalida, e l'errore che ne esce e il primo che il
        // consumatore vede. Passava per MySQL perche nessun test guardava il
        // testo, e il percorso non e raggiungibile dal costruttore.
        let error = crate::MysqlPool::new_with_profile(
            &second_product_config(),
            0,
            &SECOND_PRODUCT_PROFILE,
        )
        .expect_err("pool a capacita zero");
        assert_names_the_second_product(&error, "pool a capacita zero");

        // Una CA che non esiste: la validazione che il pool rifa al primo uso.
        // CA in memoria vuota invece di un percorso che si presume assente:
        // il test non deve dipendere da cosa esiste sul filesystem di chi lo
        // esegue, ne dai permessi con cui gira.
        let unreadable = second_product_config().with_private_ca_certificate_pem(Vec::new());
        let error = crate::MysqlPool::new_with_profile(&unreadable, 2, &SECOND_PRODUCT_PROFILE)
            .expect_err("CA vuota");
        assert_names_the_second_product(&error, "CA in memoria vuota");
    }

    #[test]
    fn the_shared_paths_name_the_product_in_their_messages() {
        // I tre testi che la review precedente ha lasciato non verificati.
        // Sono costruibili perche i loro costruttori sono stati estratti: un
        // messaggio che non si puo costruire in un test e un messaggio che
        // nessuno verifica, ed e cosi che il timeout della query e rimasto
        // cablato su MySQL mentre il ramo accanto era gia parametrizzato.
        assert_names_the_second_product(
            &crate::transaction::query_timeout_error(&SECOND_PRODUCT_PROFILE),
            "timeout della query",
        );
        assert_names_the_second_product(
            &crate::transaction::conditional_update_mismatch(&SECOND_PRODUCT_PROFILE, 1, 0),
            "update condizionale",
        );
        assert_names_the_second_product(
            &crate::session::state_error(ErrorPhase::Write, &SECOND_PRODUCT_PROFILE),
            "sessione non riusabile",
        );

        // E gli stessi tre su MySQL restano quelli di prima.
        for (what, error) in [
            (
                "timeout",
                crate::transaction::query_timeout_error(&MYSQL_PROFILE),
            ),
            (
                "update condizionale",
                crate::transaction::conditional_update_mismatch(&MYSQL_PROFILE, 1, 0),
            ),
            (
                "sessione",
                crate::session::state_error(ErrorPhase::Write, &MYSQL_PROFILE),
            ),
        ] {
            assert!(error.message.contains("MySQL"), "{what}: {}", error.message);
            assert_eq!(error.provider, Some(ProviderKind::Mysql), "{what}");
        }
    }

    #[tokio::test]
    async fn the_pure_paths_no_longer_contradict_the_attribution() {
        // I tre percorsi che la review indica come il confine fra un refactor
        // MySQL corretto e una base riusabile: il binding dei parametri,
        // l'AST non qualificato e il piano di scrittura invalido. Nessuno dei
        // tre ha un profilo in portata, e il bordo puo ristampare
        // l'attribuzione ma non riscrivere una frase — quindi la frase non
        // deve piu nominare un prodotto.
        let provider =
            crate::MysqlProvider::with_profile(second_product_config(), 2, &SECOND_PRODUCT_PROFILE)
                .expect("provider sul secondo profilo");
        let secret = SecretString::new("unique-secret");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();

        // 1. Binding: un parametro dichiarato e mai fornito.
        let outcome = provider
            .query(
                &secret,
                &parameterized_query(),
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("parametro mancante: doveva fallire");
        };
        assert_names_no_product(&error, "binding invalido");

        // 2. AST: una forma che il dialetto non qualifica.
        let mut unsupported = oversized_identifier_query();
        unsupported.common_table_expressions =
            vec![plenora_database_core::query::CommonTableExpression {
                name: "recenti".to_owned(),
                recursive: false,
                query: Box::new(oversized_identifier_query()),
            }];
        let outcome = provider
            .query(
                &secret,
                &unsupported,
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("CTE non qualificata: doveva fallire");
        };
        assert_names_no_product(&error, "AST non supportato");

        // 3. Piano di scrittura. Lo schema vuoto non basta: quell'errore
        //    nasce nel ramo che il profilo ce l'ha, e non attraversa le
        //    validazioni neutralizzate — e infatti non avrebbe intercettato
        //    le frasi che lo sweep ha rotto. Servono errori che nascono
        //    **dentro** quelle validazioni.
        let error = crate::write::MysqlWritePlan::compile_with_profile(
            &std::sync::Arc::new(Schema::empty()),
            &append_to_warehouse(),
            "warehouse",
            &SECOND_PRODUCT_PROFILE,
        )
        .expect_err("schema Arrow vuoto");
        // Questo ramo il profilo ce l'ha, quindi appartiene alla prima
        // categoria: nomina il prodotto invece di tacerlo.
        assert_names_the_second_product(&error, "piano write, ramo product-aware");

        // 3a. `TruncateInsert`: modalita non qualificata dal dialetto.
        let mut truncate = append_to_warehouse();
        truncate.mode = WriteMode::TruncateInsert;
        let error = crate::write::MysqlWritePlan::compile_with_profile(
            &schema_with(vec![Field::new("id", DataType::Int64, false)]),
            &truncate,
            "warehouse",
            &SECOND_PRODUCT_PROFILE,
        )
        .expect_err("TruncateInsert non qualificata");
        assert_names_no_product(&error, "write TruncateInsert");

        // 3b. Tipo Arrow che il mapping non qualifica.
        let error = crate::write::MysqlWritePlan::compile_with_profile(
            &schema_with(vec![Field::new(
                "durata",
                DataType::Duration(plenora_database_core::arrow::schema::TimeUnit::Second),
                false,
            )]),
            &append_to_warehouse(),
            "warehouse",
            &SECOND_PRODUCT_PROFILE,
        )
        .expect_err("tipo Arrow non qualificato");
        assert_names_no_product(&error, "write tipo non qualificato");

        // 3c. Chiave primaria su un tipo che il motore rifiuta in chiave.
        let mut create = append_to_warehouse();
        create.mode = WriteMode::Create;
        create.keys = vec!["etichetta".to_owned()];
        let error = crate::write::MysqlWritePlan::compile_with_profile(
            &schema_with(vec![Field::new("etichetta", DataType::Utf8, false)]),
            &create,
            "warehouse",
            &SECOND_PRODUCT_PROFILE,
        )
        .expect_err("chiave primaria su Utf8");
        assert_names_no_product(&error, "write chiave primaria");
        // La punteggiatura sospesa si vede meccanicamente, il soggetto
        // perduto no: togliendo il nome del prodotto questa causa era
        // diventata "diventa TEXT e rifiuta TEXT", senza piu dire chi
        // rifiuta. Chi rifiuta va nominato.
        assert!(
            error.message.contains("il motore"),
            "il rifiuto deve dire chi rifiuta — {}",
            error.message
        );
    }

    // I riferimenti su cui ADR 0014 ha misurato, con le stringhe che i
    // server hanno davvero esposto. Non sono esempi: sono le righe `probe.
    // version` e `probe.version_comment` di `docs/mariadb/EVIDENCE.md`, ed e
    // su queste che il riconoscimento deve partizionare.
    const MEASURED_SERVERS: &[(&str, &str, bool)] = &[
        ("9.7.2", "MySQL Community Server - GPL", false),
        (
            "12.3.2-MariaDB-ubu2404",
            "mariadb.org binary distribution",
            true,
        ),
        (
            "11.8.8-MariaDB-ubu2404",
            "mariadb.org binary distribution",
            true,
        ),
    ];

    #[test]
    fn the_two_profiles_partition_the_servers_that_were_measured() {
        // Il riconoscimento e una partizione, non due filtri indipendenti:
        // ogni server misurato e accettato da uno solo dei due profili. Con
        // due letture separate delle stesse stringhe si potrebbe arrivare a
        // un server rifiutato da entrambi — nessun provider lo servirebbe — o
        // accettato da entrambi, che e il caso peggiore perche la scelta
        // diventerebbe l'ordine in cui qualcuno li prova.
        for (version, comment, is_mariadb) in MEASURED_SERVERS {
            let by_mysql = MYSQL_PROFILE.foreign_product_rejection(version, comment);
            let by_mariadb = MARIADB_PROFILE.foreign_product_rejection(version, comment);
            assert_ne!(
                by_mysql.is_some(),
                by_mariadb.is_some(),
                "{version} / {comment}: i due profili devono dare esiti opposti"
            );
            let (rejection, expected_kind) = if *is_mariadb {
                (
                    by_mysql.expect("MySQL rifiuta MariaDB"),
                    ProviderKind::Mysql,
                )
            } else {
                (
                    by_mariadb.expect("MariaDB rifiuta cio che non lo e"),
                    ProviderKind::Mariadb,
                )
            };
            // Un rifiuto vale quanto la sua attribuzione: senza, chi lo legge
            // non sa quale dei due profili ha deciso, e i due messaggi
            // parlano di prodotti diversi.
            assert_eq!(rejection.category, ErrorCategory::Unsupported);
            assert_eq!(rejection.phase, ErrorPhase::Probe);
            assert_eq!(rejection.remote_effect, RemoteEffect::None);
            assert_eq!(rejection.provider, Some(expected_kind));
            assert!(
                rejection.message.contains(version) && rejection.message.contains(comment),
                "il rifiuto non riporta cio che ha letto: {}",
                rejection.message
            );
        }
    }

    #[test]
    fn the_version_is_a_second_question_after_the_product() {
        // Riconoscere il prodotto e qualificarne la versione sono due
        // domande, e la prima non risponde alla seconda: `contains("mariadb")`
        // e vero anche per una major che nessuno ha mai acceso, e sulla quale
        // tutto cio che il profilo afferma — quali colonne di catalogo
        // esistono, con quale codice arriva il timeout — non e stato misurato.
        //
        // La qualifica e per serie minor, che e la granularita con cui il
        // repository dichiara i riferimenti: `11.8` LTS e `12.3`, fissate per
        // digest e aggiornate di patch in patch.
        for qualified in [
            "11.8.8-MariaDB-ubu2404",
            "12.3.2-MariaDB-ubu2404",
            "11.8.0",
            "12.3.19-MariaDB",
        ] {
            assert!(
                super::unqualified_version_rejection(&MARIADB_PROFILE, qualified).is_none(),
                "{qualified} e una versione misurata"
            );
        }
        for unqualified in [
            "10.11.5-MariaDB",
            "13.0.0-MariaDB",
            "12.4.0-MariaDB",
            "",
            "MariaDB",
        ] {
            let rejection = super::unqualified_version_rejection(&MARIADB_PROFILE, unqualified)
                .unwrap_or_else(|| panic!("{unqualified} non e fra le versioni misurate"));
            assert_eq!(rejection.category, ErrorCategory::Unsupported);
            assert_eq!(rejection.phase, ErrorPhase::Probe);
            assert_eq!(rejection.remote_effect, RemoteEffect::None);
            assert_eq!(rejection.provider, Some(ProviderKind::Mariadb));
            // Il messaggio deve dire cosa e successo davvero: non "questo
            // server non va", ma "su questo server non e stata fatta nessuna
            // prova". Sono due affermazioni diverse, e solo la seconda e vera.
            assert!(
                rejection.message.contains("non misurata")
                    && rejection.message.contains("11.8")
                    && rejection.message.contains("12.3"),
                "il rifiuto non dice cosa manca: {}",
                rejection.message
            );
        }

        // MySQL non dichiara un elenco, e qui non rifiuta nulla: la matrice
        // qualifica 9.7, 8.4 e 8.0, ma il provider non ha mai rifiutato le
        // altre, e trasformare quella matrice in un rifiuto e una modifica al
        // comportamento di un provider qualificato — non un effetto collaterale
        // dell'aggiunta di un secondo profilo.
        assert!(MYSQL_PROFILE.qualified_versions().is_none());
        for version in ["9.7.2", "8.4.11", "8.0.46", "9.8.0", "sconosciuta"] {
            assert!(
                super::unqualified_version_rejection(&MYSQL_PROFILE, version).is_none(),
                "{version}: il profilo MySQL non dichiara un limite di versione"
            );
        }

        // E le due domande restano separate: una MariaDB 10.11 e riconosciuta
        // come MariaDB da entrambi i profili — il primo la rifiuta perche non
        // e sua, il secondo la accetta come prodotto — e viene fermata dalla
        // qualifica, non dal riconoscimento.
        assert!(MYSQL_PROFILE
            .foreign_product_rejection("10.11.5-MariaDB", "mariadb.org binary distribution")
            .is_some());
        assert!(MARIADB_PROFILE
            .foreign_product_rejection("10.11.5-MariaDB", "mariadb.org binary distribution")
            .is_none());
    }

    #[test]
    fn the_probe_asks_the_version_question_too() {
        // Il gate vive nel profilo, ma serve a qualcosa solo se il percorso
        // che apre una connessione lo attraversa. Senza questa riga il
        // profilo dichiarerebbe un elenco di versioni che nessuno consulta,
        // ed e la forma di fail-closed che non chiude niente.
        let production = include_str!("catalog.rs")
            .split_once(format!("{}mod tests {{", '\n').as_str())
            .map_or(include_str!("catalog.rs"), |(head, _)| head);
        assert!(
            production.contains("unqualified_version_rejection"),
            "la probe non verifica la qualifica della versione"
        );

        // E la verifica sta **fuori** dal bypass di test. Dentro, l'unico
        // percorso che accende il bypass — la misura di evidenza — sarebbe
        // anche l'unico a non attraversarla mai: il gate esisterebbe, e la
        // corsa che deve dimostrarlo lo salterebbe.
        let opening = "if !mariadb_rejection_bypassed() {";
        let at = production
            .find(opening)
            .expect("il bypass vive nella probe");
        let mut depth = 0_i32;
        let mut end = at;
        for (offset, character) in production[at..].char_indices() {
            match character {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = at + offset;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(depth == 0 && end > at, "blocco del bypass non delimitato");
        assert!(
            !production[at..=end].contains("unqualified_version_rejection"),
            "la qualifica della versione e dentro il bypass: la misura non la attraversa"
        );
    }

    #[test]
    fn the_mariadb_timeout_diverges_in_name_and_in_unit() {
        // Le due meta della divergenza misurata: `MAX_EXECUTION_TIME` non
        // esiste su MariaDB (1193), e cio che la sostituisce non prende la
        // stessa unita. Un profilo che copiasse solo il nome emetterebbe
        // millisecondi come se fossero secondi, cioe un timeout mille volte
        // piu largo di quello chiesto.
        let variable = format!("MAX_EXECUTION{}TIME", "_");
        for milliseconds in [1_u64, 200, 999, 1_000, 1_500, 5_000, 60_000] {
            let mysql = MYSQL_PROFILE.statement_timeout_statement(milliseconds);
            let mariadb = MARIADB_PROFILE.statement_timeout_statement(milliseconds);
            assert_ne!(mysql, mariadb, "{milliseconds} ms");
            assert!(mysql.contains(variable.as_str()));
            assert!(!mariadb.contains(variable.as_str()));
            assert!(mariadb.contains("max_statement_time"));

            // La conversione si verifica rileggendola: secondi e millesimi
            // devono ricomporre esattamente il valore chiesto. Un
            // arrotondamento — in qualunque verso — rompe questa uguaglianza,
            // ed e cio che distingue una conversione da un'approssimazione.
            let value = mariadb
                .rsplit_once(" = ")
                .expect("lo statement porta un valore")
                .1;
            let (seconds, thousandths) = value.split_once('.').expect("secondi frazionari");
            assert_eq!(thousandths.len(), 3, "i millesimi restano tre cifre");
            let recomposed = seconds.parse::<u64>().expect("secondi") * 1_000
                + thousandths.parse::<u64>().expect("millesimi");
            assert_eq!(recomposed, milliseconds, "{mariadb}");
        }
        // Il caso che rende visibile l'arrotondamento: 200 ms non diventano
        // un secondo. Se lo diventassero, il timeout si allungherebbe da solo
        // proprio dove qualcuno lo stava stringendo.
        assert_eq!(
            MARIADB_PROFILE.statement_timeout_statement(200),
            "SET SESSION max_statement_time = 0.200"
        );
    }

    /// Le differenze **ammesse** fra le due query del catalogo, e la misura da
    /// cui ciascuna discende.
    ///
    /// La guardia sotto sostituisce questi frammenti nella query di `MySQL` e
    /// pretende che ne esca, carattere per carattere, quella di `MariaDB`. Ogni
    /// riga qui e percio una divergenza dichiarata: chi ne aggiunge una senza
    /// passare da questa tabella fa fallire il confronto, ed e l'unico modo
    /// perche il catalogo non si sdoppi in silenzio.
    const DECLARED_CATALOG_DIVERGENCES: &[(&str, &str)] = &[
        // `SRS_ID` non esiste su MariaDB: 1054, misurato su entrambi i
        // riferimenti. La colonna si dichiara nulla, non si omette.
        ("SRS_ID AS srs_id", "NULL AS srs_id"),
        // `GENERATION_EXPRESSION` esiste, ma su MariaDB e **NULL** per le
        // colonne non generate, dove MySQL manda la stringa vuota. Il lettore
        // pretende una stringa, e "nessuna espressione" e la stringa vuota su
        // entrambi: la differenza e nella rappresentazione, non nel fatto.
        //
        // Non e una deduzione: la prima corsa di `provider.profile_describe_object`
        // sui due riferimenti falliva qui, con "campo catalogo
        // generation_expression non convertibile". Nessun test offline poteva
        // vederlo, perche la query compilava.
        (
            "GENERATION_EXPRESSION AS generation_expression",
            "COALESCE(GENERATION_EXPRESSION, '') AS generation_expression",
        ),
        // `EXPRESSION` non esiste su MariaDB: 1054, misurato su entrambi.
        ("EXPRESSION AS expression", "NULL AS expression"),
    ];

    #[test]
    fn the_mariadb_catalog_differs_only_where_the_measure_says_so() {
        // Le due query divergono per le divergenze dichiarate, e per nient'altro.
        // Scritta cosi, la guardia regge anche le modifiche future: chi aggiunge
        // un filtro o una colonna a una sola delle due la fa fallire.
        for (mysql, mariadb) in [
            (
                MYSQL_PROFILE.object_columns_query(),
                MARIADB_PROFILE.object_columns_query(),
            ),
            (
                MYSQL_PROFILE.object_indexes_query(),
                MARIADB_PROFILE.object_indexes_query(),
            ),
        ] {
            let mut translated = mysql.to_owned();
            for (from, to) in DECLARED_CATALOG_DIVERGENCES {
                translated = translated.replace(from, to);
            }
            assert_eq!(translated, mariadb);
            // E la divergenza c'e davvero: senza questa riga le due
            // asserzioni sopra passerebbero anche con due query identiche.
            assert_ne!(mysql, mariadb);
        }
        // Le colonne che non esistono non compaiono da nessuna parte nelle
        // query di MariaDB, nemmeno in un filtro o in un ORDER BY.
        assert!(!MARIADB_PROFILE.object_columns_query().contains("SRS_ID AS"));
        assert!(!MARIADB_PROFILE
            .object_indexes_query()
            .contains("EXPRESSION AS"));
    }

    #[test]
    fn the_catalog_queries_that_coincide_are_written_once() {
        // Dove l'evidenza non ha visto divergenze, il codice non ne inventa:
        // le tre query restano una costante sola.
        //
        // La guardia guarda la **sorgente**, non i valori, e non per gusto:
        // due `&'static str` con lo stesso contenuto sono spesso lo stesso
        // puntatore, perche il compilatore unifica i literal uguali. Un
        // confronto fra i valori — o fra gli indirizzi — passerebbe quindi
        // anche su due copie, cioe proprio nel caso da cui la costante
        // difende: la modifica fatta da una parte sola.
        let production = include_str!("profile.rs")
            .split_once("mod tests {")
            .expect("il modulo di test chiude la parte di produzione")
            .0;
        for (label, fragment, expected) in [
            ("schemi", "SELECT SCHEMA_NAME AS schema_name", 1),
            // Due volte: la lista di uno schema e il singolo oggetto sono due
            // domande diverse con lo stesso `SELECT`, e ciascuna e scritta una
            // volta sola.
            ("oggetti", "SELECT TABLE_SCHEMA AS table_schema", 2),
        ] {
            assert_eq!(
                production.matches(fragment).count(),
                expected,
                "la query {label} non e piu scritta una volta per profilo condiviso"
            );
        }
        // E cio che i due profili restituiscono e davvero la stessa cosa.
        assert_eq!(
            MYSQL_PROFILE.schemas_query(),
            MARIADB_PROFILE.schemas_query()
        );
        assert_eq!(
            MYSQL_PROFILE.objects_query(),
            MARIADB_PROFILE.objects_query()
        );
        assert_eq!(MYSQL_PROFILE.object_query(), MARIADB_PROFILE.object_query());
    }

    #[test]
    fn the_two_profiles_publish_distinct_metadata_namespaces() {
        // I metadata sono contratto pubblico: dicono al consumer cosa fosse la
        // colonna sul server. Con un namespace solo, un batch letto da MariaDB
        // arriverebbe annotato `plenora.mysql.*`, e chi lo legge dovrebbe
        // dedurre da un metadato che non lo dice quale tabella di tipi
        // applicare — mentre le due divergono davvero, `json` contro `text`
        // dalla stessa DDL.
        let mysql = MYSQL_PROFILE.metadata_keys();
        let mariadb = MARIADB_PROFILE.metadata_keys();
        assert_ne!(mysql.native_type, mariadb.native_type);
        assert_ne!(mysql.native_declaration, mariadb.native_declaration);
        assert_ne!(mysql.collation, mariadb.collation);
        for (profile, prefix) in [
            (&MYSQL_PROFILE as &dyn ProductProfile, "plenora.mysql."),
            (&MARIADB_PROFILE, "plenora.mariadb."),
        ] {
            let keys = profile.metadata_keys();
            for key in [keys.native_type, keys.native_declaration, keys.collation] {
                assert!(
                    key.starts_with(prefix),
                    "{}: la chiave {key} non e nel namespace del prodotto",
                    profile.product()
                );
            }
        }

        // E la scelta arriva fino allo schema: dalla stessa colonna escono due
        // annotazioni con lo stesso valore e chiavi diverse.
        let spec = crate::MysqlColumnSpec {
            name: "document".to_owned(),
            native_type: "text".to_owned(),
            native_declaration: String::new(),
            nullable: true,
            collation: None,
            kind: MysqlColumnKind::Utf8,
            spatial_srid: None,
            spatial_srid_declared: false,
        };
        let by_mysql = spec.arrow_field_with_profile(&MYSQL_PROFILE);
        let by_mariadb = spec.arrow_field_with_profile(&MARIADB_PROFILE);
        assert_eq!(by_mysql.data_type(), by_mariadb.data_type());
        assert_eq!(
            by_mysql.metadata().get(mysql.native_type),
            by_mariadb.metadata().get(mariadb.native_type)
        );
        assert!(by_mariadb.metadata().get(mysql.native_type).is_none());
        assert!(by_mysql.metadata().get(mariadb.native_type).is_none());
        // L'API pubblica resta quella di MySQL, che e il prodotto che il crate
        // serve: cambiarla sarebbe una rottura per chi la legge oggi.
        assert_eq!(
            spec.arrow_field().metadata().get(mysql.native_type),
            by_mysql.metadata().get(mysql.native_type)
        );
    }

    #[test]
    fn no_production_module_writes_the_metadata_namespace_itself() {
        // Il namespace si sceglie in un posto solo. Un modulo che scrivesse
        // direttamente `protocol::MYSQL_NATIVE_TYPE` annoterebbe con MySQL
        // anche cio che ha letto da un altro prodotto, e lo farebbe in un
        // punto dove nessuno pensa di guardare: lo schema esce corretto nei
        // tipi e sbagliato nell'origine.
        let marker = format!("{}mod tests {{", '\n');
        for (module, source) in GUARDED_MODULES {
            if *module == "profile.rs" {
                continue;
            }
            let production = source
                .split_once(marker.as_str())
                .map_or(*source, |(head, _)| head);
            for needle in [
                "MYSQL_NATIVE_TYPE",
                "MYSQL_NATIVE_DECLARATION",
                "MYSQL_COLLATION",
                "MARIADB_NATIVE_TYPE",
            ] {
                assert!(
                    !production.contains(needle),
                    "{module} sceglie il namespace dei metadata invece di chiederlo al profilo"
                );
            }
        }
    }

    #[test]
    fn the_wire_mapper_does_not_diverge_between_the_profiles() {
        // ADR 0014 ha misurato che dai metadata di `COM_STMT_PREPARE`
        // escono lo stesso `kind` e lo stesso `native_type` sui tre
        // riferimenti: a divergere e l'ingresso, non il mapper. La stessa
        // DDL `document JSON` arriva come `MYSQL_TYPE_JSON` da MySQL e come
        // `MYSQL_TYPE_BLOB` da MariaDB, dove `JSON` e un alias di `LONGTEXT`.
        for wire in [
            ColumnType::MYSQL_TYPE_JSON,
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnType::MYSQL_TYPE_LONG,
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
            ColumnType::MYSQL_TYPE_TIMESTAMP,
            ColumnType::MYSQL_TYPE_VAR_STRING,
        ] {
            let column = Column::new(wire)
                .with_name(b"document")
                .with_character_set(255);
            let by_mysql = MYSQL_PROFILE.wire_column_spec(&column);
            let by_mariadb = MARIADB_PROFILE.wire_column_spec(&column);
            match (by_mysql, by_mariadb) {
                (Ok(mysql), Ok(mariadb)) => {
                    assert_eq!(mysql.native_type, mariadb.native_type, "{wire:?}");
                    assert_eq!(mysql.kind, mariadb.kind, "{wire:?}");
                    assert_eq!(mysql.nullable, mariadb.nullable, "{wire:?}");
                }
                // Anche i rifiuti coincidono: stesso verdetto, e ciascuno
                // nomina il profilo che lo ha prodotto. Un ramo che
                // pretendesse solo `Ok` lascerebbe fuori proprio i tipi che
                // il mapper non qualifica, che sono quelli su cui due copie
                // divergerebbero per prime.
                (Err(mysql), Err(mariadb)) => {
                    assert_eq!(mysql.category, mariadb.category, "{wire:?}");
                    assert_eq!(mysql.phase, mariadb.phase, "{wire:?}");
                    assert_eq!(mysql.retry, mariadb.retry, "{wire:?}");
                    assert!(
                        mysql.message.contains("MySQL") && mariadb.message.contains("MariaDB"),
                        "{wire:?}: i rifiuti non nominano chi ha rifiutato"
                    );
                }
                (mysql, mariadb) => {
                    panic!("{wire:?}: esiti diversi — {mysql:?} contro {mariadb:?}")
                }
            }
        }
        // La divergenza pubblicata resta quella dell'ingresso, e questo e il
        // suo test: due tipi wire diversi, due `native_type` diversi, dallo
        // stesso mapper.
        let json = Column::new(ColumnType::MYSQL_TYPE_JSON)
            .with_name(b"document")
            .with_character_set(255);
        let blob = Column::new(ColumnType::MYSQL_TYPE_BLOB)
            .with_name(b"document")
            .with_character_set(255);
        assert_eq!(
            MARIADB_PROFILE
                .wire_column_spec(&json)
                .expect("json")
                .native_type,
            "json"
        );
        assert_eq!(
            MARIADB_PROFILE
                .wire_column_spec(&blob)
                .expect("blob")
                .native_type,
            "text"
        );
    }

    #[test]
    fn the_mapper_rejections_name_the_profile_that_refused() {
        // Il mapper e condiviso, l'attribuzione no: un rifiuto che dicesse
        // "MySQL" mentre a rifiutare e stato MariaDB manderebbe chi legge a
        // cercare sul server sbagliato.
        let unnamed = Column::new(ColumnType::MYSQL_TYPE_LONG).with_character_set(255);
        for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
            let error = profile
                .wire_column_spec(&unnamed)
                .expect_err("una colonna senza nome si rifiuta");
            assert!(
                error.message.contains(profile.product()),
                "{}: il rifiuto non nomina chi ha rifiutato — {}",
                profile.product(),
                error.message
            );
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn the_mariadb_capabilities_open_only_where_a_probe_supports_them() {
        let published = MARIADB_PROFILE.capabilities("11.8.8-MariaDB".to_owned());
        assert_eq!(published.provider, ProviderKind::Mariadb);
        assert_eq!(published.provider_version, "11.8.8-MariaDB");

        // La lettura e aperta, e ognuna delle quattro ha una sonda che la
        // sostiene: valori, streaming, proiezione, filtro, ordinamento. Le
        // altre quattro restano chiuse perche il crate non le offre a
        // nessuno dei due prodotti — non perche siano state provate e
        // fallite.
        let reads = &published.reads;
        assert!(reads.streaming && reads.projection && reads.filter && reads.ordering);
        // `pagination` si e aperta insieme al campo che la rende riscuotibile:
        // `ReadOperation` ha ora `row_offset`, il piano di lettura lo compila
        // e l'engine lega la bandiera al campo. Prima era `false` su questo
        // profilo e `true` su PostgreSQL, con lo stesso nulla sotto.
        assert!(reads.pagination);
        assert!(!reads.server_cursor);
        assert!(!reads.resumable);
        // E dove il crate non offre niente, i due prodotti dicono la stessa
        // cosa: una bandiera chiusa qui non e una divergenza di prodotto.
        let mysql_reads = &MYSQL_PROFILE.capabilities("9.7.2".to_owned()).reads;
        assert_eq!(reads.server_cursor, mysql_reads.server_cursor);
        assert_eq!(reads.pagination, mysql_reads.pagination);
        assert_eq!(reads.resumable, mysql_reads.resumable);

        // La scrittura procede una mode alla volta, ed e la differenza che
        // rende leggibile la tabella: non "tutto chiuso", ma "chiuso cio che
        // non e stato attraversato". `append` dalla settima tranche, `create`
        // dall'ottava, ciascuna con le proprie tre sonde. Le altre restano
        // chiuse: nessun piano le ha eseguite.
        let writes = &published.writes;
        assert!(writes.append && writes.create);
        assert!(writes.update && writes.upsert);
        assert!(writes.replace && writes.delete_by_keys);
        // Sei mode su sette, come `MySQL`: la settima e `TruncateInsert`, e
        // resta chiusa su entrambi i profili per la stessa ragione
        // permanente. Le due righe qui sotto lo verificano insieme, perche e
        // proprio la coincidenza a dire che non e una lacuna di `MariaDB`.
        assert!(!writes.truncate_insert);
        assert!(
            !MYSQL_PROFILE
                .capabilities("9.7.2".to_owned())
                .writes
                .truncate_insert
        );
        // `bulk` coincide con quella di `MySQL` perche l'implementazione e la
        // stessa: dichiararla diversa sarebbe una divergenza inventata.
        // `array_binding` e `returning` restano chiuse su entrambi, e la
        // seconda per una ragione che sta a monte dei provider — `WriteOutcome`
        // conta righe e non le trasporta.
        let mysql_writes = &MYSQL_PROFILE.capabilities("9.7.2".to_owned()).writes;
        assert_eq!(writes.bulk, mysql_writes.bulk);
        assert!(writes.bulk);
        assert!(!writes.array_binding && !writes.returning);
        // `rollback_on_failure` e aperta: parla delle **righe** di ogni
        // scrittura che il profilo ammette, e le righe tornano indietro in
        // entrambe le mode aperte — le sonde girano con `allow_partial:
        // false` e lo misurano rileggendo da un'altra sessione.
        assert!(writes.rollback_on_failure);
        // Che il rollback non riporti indietro anche lo **schema** non lo
        // dice quel flag: lo dice `transactional_ddl`, chiuso, e l'ottava
        // tranche e la misura che lo sostiene — la tabella creata da `Create`
        // sopravvive al rollback su tutti e tre i riferimenti. Le due
        // bandiere parlano di due cose, e questa riga esiste perche restino
        // distinte.
        assert!(!published.transactions.transactional_ddl);
        // `truncate_insert` e chiusa su **entrambi** i profili, e per una
        // ragione che non e "non misurata": su questi due motori `TRUNCATE` e
        // DDL con commit implicito, quindi le righe sparirebbero prima
        // dell'INSERT e nessun rollback le riporterebbe indietro. E una
        // chiusura permanente finche quello resta vero, e va detta accanto
        // alla bandiera — non da qualche parte nel file, dove `TRUNCATE`
        // compare anche altrove.
        assert!(!published.writes.truncate_insert);
        assert!(
            !MYSQL_PROFILE
                .capabilities("9.7.2".to_owned())
                .writes
                .truncate_insert
        );
        // Solo la produzione: questo stesso test nomina la bandiera chiusa
        // per cercarla, e contando tutto il file conterebbe anche se stesso.
        let source = include_str!("profile.rs")
            .split_once(format!("{}mod tests {{", '\n').as_str())
            .map_or_else(|| include_str!("profile.rs"), |(head, _)| head);
        let closed = "truncate_insert: false,";
        for (at, _) in source.match_indices(closed) {
            let start = source[..at].rfind("writes: WriteCapabilities").unwrap_or(0);
            assert!(
                source[start..at].contains("commit implicito"),
                "accanto alla bandiera chiusa non c'e scritto perche lo resta"
            );
        }
        assert_eq!(
            source.matches(closed).count(),
            2,
            "i due profili devono dichiararla entrambi, e chiusa"
        );
        assert!(writes.upsert && writes.replace && writes.delete_by_keys);

        // Spatial: la lettura si e aperta con la dodicesima tranche, e con la
        // condizione accanto. Le due bandiere vanno lette insieme —
        // `geometry: true, requires_declared_crs: true` — perche la prima da
        // sola prometterebbe che una lettura semplice basti.
        let spatial = &published.spatial;
        assert!(spatial.read_wkb && spatial.geometry);
        assert!(spatial.requires_declared_crs);
        assert_eq!(
            spatial.dimensions,
            vec![plenora_database_core::geometry::Dimensions::Xy]
        );
        // La condizione non e una divergenza di prodotto: le stesse tre sonde
        // danno lo stesso esito su MySQL, dove una colonna `GEOMETRY` non
        // vincolata dalla DDL ha `SRS_ID` nullo esattamente come qui. E' la
        // riga che impedisce di leggere questa apertura come «MariaDB ha un
        // problema che MySQL non ha».
        assert_eq!(
            spatial.requires_declared_crs,
            MYSQL_PROFILE
                .capabilities("9.7.2".to_owned())
                .spatial
                .requires_declared_crs
        );
        // Cio che resta chiuso, resta chiuso: `geography` non esiste su questo
        // prodotto, e i tipi misti non sono mai stati letti.
        assert!(!spatial.geography);
        // L'indice si apre con la diciottesima, su entrambi: il fatto del
        // server era misurato dalla diciassettesima, e mancava il percorso.
        assert!(spatial.spatial_index);
        assert_eq!(
            spatial.spatial_index,
            MYSQL_PROFILE
                .capabilities("9.7.2".to_owned())
                .spatial
                .spatial_index
        );
        // I tipi misti si aprono con la diciassettesima tranche, e coincidono
        // con MySQL: e la stessa colonna `GEOMETRY` che regge tipi diversi, e
        // le sonde lo misurano con lo stesso punto e lo stesso poligono.
        assert!(spatial.mixed_geometry_types);
        assert_eq!(
            spatial.mixed_geometry_types,
            MYSQL_PROFILE
                .capabilities("9.7.2".to_owned())
                .spatial
                .mixed_geometry_types
        );

        // Le funzioni si aprono con la sedicesima tranche, e **non** sono
        // quelle di MySQL: quattordici invece di quindici. La differenza e
        // `IsValid`, che la 12.3 esegue e la 11.8 LTS no — la prima divergenza
        // misurata fra le due major di questo prodotto.
        //
        // La lista pubblicata e la stessa che il renderer consulta: le due
        // erano separate, e un piano poteva superare il cancello di MySQL e
        // morire sul server mentre la capability diceva giustamente di no.
        assert_eq!(
            spatial.functions,
            MARIADB_PROFILE.verified_spatial_functions().to_vec()
        );

        // Le due liste non sono piu una il sottoinsieme dell'altra, e la
        // guardia lo verifica in **entrambe** le direzioni: cercare solo cio
        // che manca a MariaDB lascerebbe passare in silenzio il giorno in cui
        // MySQL perdesse qualcosa che qui c'e.
        let mysql_functions = MYSQL_PROFILE.verified_spatial_functions();
        let only_mysql: Vec<_> = mysql_functions
            .iter()
            .filter(|function| !spatial.functions.contains(function))
            .copied()
            .collect();
        let only_mariadb = spatial
            .functions
            .iter()
            .find(|function| !mysql_functions.contains(function));
        assert_eq!(
            only_mysql,
            vec![
                plenora_database_core::query::SpatialFunction::IsValid,
                plenora_database_core::query::SpatialFunction::HausdorffDistance,
                plenora_database_core::query::SpatialFunction::FrechetDistance
            ],
            "cio che MySQL ha e MariaDB no"
        );
        // Vuoto, e non per costruzione: `Relate` c'e stato per una campagna.
        // Il server ce l'ha — la sonda delle candidate lo aveva trovato — ma il
        // gate lo ha bocciato con 1582, perche `MariaDB` lo vuole a tre
        // argomenti e il contratto ne ammette anche due. Esiste e non e
        // utilizzabile nella forma che il piano permette, che sono due cose
        // diverse.
        assert!(only_mariadb.is_none(), "cio che MariaDB ha e MySQL no");
        assert_eq!(
            spatial.write_wkb,
            MARIADB_PROFILE.write_spatial_is_qualified(),
            "la capability spatial e la decisione del piano devono avere una sola sorgente"
        );

        // L'unica famiglia con dei `true`, e sono quelli che la terza tranche
        // ha misurato: commit, rollback e isolamento coincidono sui tre
        // riferimenti. I savepoint no, e restano chiusi.
        let transactions = &published.transactions;
        assert!(transactions.single_transaction);
        // Aperta dalla quattordicesima tranche, e insieme a MySQL: il crate li
        // implementa una volta sola per i due prodotti, e le due sonde danno lo
        // stesso esito sui tre riferimenti. Il confronto sta qui perche una
        // divergenza inventata su una superficie condivisa e il difetto che
        // ADR 0010 ha nominato.
        assert!(transactions.savepoints);
        assert_eq!(
            transactions.savepoints,
            MYSQL_PROFILE
                .capabilities("9.7.2".to_owned())
                .transactions
                .savepoints
        );
        assert!(!transactions.transactional_ddl && !transactions.staged_swap);

        // I limiti non sono capability: dicono quanto il crate manda. `None`
        // si leggerebbe come "nessun limite dichiarato", che e la sola delle
        // due letture che puo far male.
        assert_eq!(
            published.limits.max_bind_parameters,
            Some(crate::MAX_BIND_PARAMETERS as u64)
        );
        assert_eq!(
            published.limits.max_batch_rows,
            Some(crate::MAX_BATCH_ROWS as u64)
        );

        // E il confronto che rende la chiusura osservabile: dove MySQL
        // dichiara qualificata la scrittura e lo spatial, MariaDB non lo fa.
        // La lettura invece ora coincide, ed e il primo punto in cui i due
        // prodotti dichiarano la stessa cosa perche entrambi l'hanno provata.
        let mysql = MYSQL_PROFILE.capabilities("9.7.2".to_owned());
        assert!(mysql.writes.append && mysql.spatial.read_wkb);
        assert!(!mysql.spatial.functions.is_empty());
        assert_eq!(mysql.reads, published.reads);
    }

    #[test]
    fn the_shared_verdicts_are_shared_only_where_they_were_measured() {
        // Dove il codice e stato osservato su entrambi i prodotti, il verdetto
        // e lo stesso e cambia solo il nome di chi ha risposto.
        for code in MEASURED_SERVER_CODES {
            let mysql = MYSQL_PROFILE.classify_server_code(*code);
            let mariadb = MARIADB_PROFILE.classify_server_code(*code);
            assert_eq!(mysql.category, mariadb.category, "codice {code}");
            assert_eq!(mysql.retry, mariadb.retry, "codice {code}");
            assert_eq!(mysql.remote_effect, mariadb.remote_effect, "codice {code}");
            assert!(
                mysql.message.contains("MySQL") && mariadb.message.contains("MariaDB"),
                "codice {code}: i messaggi non nominano il prodotto che ha risposto"
            );
            assert_ne!(mysql.message, mariadb.message, "codice {code}");
        }

        // Dove non lo e, MariaDB non eredita. 1044 e 1049 non sono mai
        // arrivati dai due riferimenti — la quarta tranche ci ha provato, e ha
        // ricevuto 1142 al loro posto — e 3024 e il timeout dell'altro motore.
        //
        // La differenza non e cosmetica: su 1213 la tabella condivisa dichiara
        // `retry: Safe` e `remote_effect: RolledBack`. Un codice ereditato con
        // quelle due promesse direbbe al chiamante di rifare l'operazione, e
        // che non c'e niente da ripulire, su un motore che nessuno aveva
        // interrogato.
        for unmeasured in [1_044_u16, 1_049, 3_024] {
            let mysql = MYSQL_PROFILE.classify_server_code(unmeasured);
            let mariadb = MARIADB_PROFILE.classify_server_code(unmeasured);
            assert_ne!(
                mysql.category, mariadb.category,
                "codice {unmeasured}: MariaDB eredita una categoria che non ha misurato"
            );
            assert_eq!(mariadb.category, ErrorCategory::Execution);
            assert_eq!(mariadb.retry, RetryDisposition::Never);
            assert_eq!(mariadb.remote_effect, None);
            assert!(mariadb.message.contains("redatto"));
        }
        assert!(!MEASURED_SERVER_CODES.contains(&1_044));
        assert!(!MEASURED_SERVER_CODES.contains(&1_049));
        assert!(!MEASURED_SERVER_CODES.contains(&3_024));
    }

    #[test]
    fn a_privilege_error_is_authorization_on_both_products() {
        // 1142 arriva ogni volta che il permesso manca su un comando o una
        // tabella, ed e il codice che la quarta tranche ha ricevuto **al
        // posto** di 1044 e 1049: e la risposta piu comune del motore a una
        // richiesta che l'utente non puo fare.
        //
        // Restava fuori dalla tabella, quindi si classificava come esecuzione
        // generica: il chiamante leggeva un guasto dove c'era un permesso
        // mancante, e le due cose si risolvono in modi diversi — una si
        // ritenta, l'altra si concede. Il cambio tocca anche il provider
        // MySQL qualificato, ed e giusto che lo tocchi: la misura vale per
        // tutti e tre i riferimenti.
        for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
            let verdict = profile.classify_server_code(1_142);
            assert_eq!(
                verdict.category,
                ErrorCategory::Authorization,
                "{}",
                profile.product()
            );
            assert_eq!(verdict.retry, RetryDisposition::Never);
            assert_eq!(verdict.remote_effect, None);
            assert!(
                verdict.message.contains("autorizzazione") && verdict.message.contains("1142"),
                "{}: {}",
                profile.product(),
                verdict.message
            );
        }
        // E vale per entrambi perche e stato misurato su entrambi: sta
        // nell'elenco dei codici osservati, non in un ramo scritto a mano nel
        // profilo che lo ha visto per primo.
        assert!(MEASURED_SERVER_CODES.contains(&1_142));
        // 1044 no: la sonda che lo cercava ha ricevuto 1142, quindi su
        // MariaDB quel codice resta non misurato e prende il verdetto
        // generico. Due codici della stessa famiglia, due stati di prova
        // diversi.
        assert!(!MEASURED_SERVER_CODES.contains(&1_044));
    }

    #[test]
    fn the_statement_timeout_is_classified_as_a_timeout_on_both_products() {
        // Le due meta della stessa divergenza. L'istruzione giusta senza la
        // classificazione giusta era il difetto peggiore dei due, perche
        // invisibile: il limite scattava davvero, e il chiamante leggeva
        // "errore server redatto" invece di "timeout" — cioe non poteva
        // distinguere un limite che aveva fatto il suo lavoro da un guasto.
        let mysql = MYSQL_PROFILE.classify_server_code(3_024);
        let mariadb = MARIADB_PROFILE.classify_server_code(MARIADB_STATEMENT_TIMEOUT);
        for (product, verdict) in [("MySQL", &mysql), ("MariaDB", &mariadb)] {
            assert_eq!(verdict.category, ErrorCategory::Timeout, "{product}");
            assert_eq!(verdict.retry, RetryDisposition::Never, "{product}");
            assert!(verdict.message.contains("timeout"), "{product}");
            assert!(verdict.message.contains(product), "{product}");
        }
        // E i due numeri non si incrociano: il codice di un motore non
        // significa niente sull'altro, ed e la ragione per cui la riga vive
        // nel profilo e non nella tabella condivisa.
        assert_eq!(
            MYSQL_PROFILE
                .classify_server_code(MARIADB_STATEMENT_TIMEOUT)
                .category,
            ErrorCategory::Execution
        );
        assert_eq!(
            MARIADB_PROFILE.classify_server_code(3_024).category,
            ErrorCategory::Execution
        );
        // La conversione e il codice sono la stessa decisione vista due volte:
        // se l'istruzione tornasse a essere quella di MySQL, il codice 1969
        // non arriverebbe mai e questa riga sarebbe morta.
        assert!(MARIADB_PROFILE
            .statement_timeout_statement(200)
            .contains("max_statement_time"));
    }

    #[test]
    fn mariadb_attributes_only_the_row_causes_it_did_not_infer() {
        // I tre codici che i due prodotti mandano dallo stesso tentativo.
        for shared in [1_048_u16, 1_062, 1_452] {
            assert_eq!(
                MYSQL_PROFILE.row_rejection_cause(shared),
                MARIADB_PROFILE.row_rejection_cause(shared),
                "codice {shared}"
            );
            assert!(MARIADB_PROFILE.row_rejection_cause(shared).is_some());
        }
        // Il CHECK diverge, e la quarta tranche l'ha visto: lo stesso INSERT
        // che viola lo stesso vincolo arriva come 3819 da MySQL e come 4025 da
        // MariaDB. Ciascun profilo attribuisce il codice che ha ricevuto.
        assert!(MARIADB_PROFILE.row_rejection_cause(4_025).is_some());
        assert!(MYSQL_PROFILE.row_rejection_cause(3_819).is_some());
        assert!(
            MARIADB_PROFILE.row_rejection_cause(3_819).is_none(),
            "codice 3819: attribuito senza essere mai arrivato da MariaDB"
        );
    }

    #[test]
    fn the_spatial_decisions_diverge_only_where_the_catalog_cannot_answer() {
        // Lettura: stessa funzione, stesso WKB atteso. `raw.spatial_functions`
        // ha misurato `POINT srid=4326` e 21 byte sui tre riferimenti.
        assert_eq!(
            MYSQL_PROFILE.geometry_projection("`geom`"),
            MARIADB_PROFILE.geometry_projection("`geom`")
        );
        for (srid, dimensions) in [(None, "xy"), (Some(4_326), "xy"), (None, "xyz")] {
            assert_eq!(
                MYSQL_PROFILE.geometry_output_is_unexpected(srid, dimensions),
                MARIADB_PROFILE.geometry_output_is_unexpected(srid, dimensions)
            );
        }
        for native in ["geometry", "point", "geomcollection", "blob", "json", ""] {
            assert_eq!(
                MYSQL_PROFILE.is_spatial_native_type(native),
                MARIADB_PROFILE.is_spatial_native_type(native),
                "{native}"
            );
        }

        // La divergenza sta dove il catalogo non risponde: su MariaDB
        // `srs_id` e sempre nullo, quindi la regola dell'SRID dichiarato
        // rifiuta ogni colonna geometrica. Non e "questa colonna non ha un
        // CRS": e "non c'e modo di saperlo".
        assert!(MARIADB_PROFILE.spatial_requires_declared_srid());
        assert!(MARIADB_PROFILE
            .object_columns_query()
            .contains("NULL AS srs_id"));

        // Scrittura: aperta su entrambi dalla quindicesima tranche, e i tipi
        // scrivibili coincidono — sono nomi OGC, non una tabella di prodotto.
        assert!(MYSQL_PROFILE.write_spatial_is_qualified());
        assert!(MARIADB_PROFILE.write_spatial_is_qualified());
        for geometry in ["point", "linestring", "polygon", "multipoint"] {
            assert_eq!(
                MYSQL_PROFILE.writable_geometry_type(geometry),
                MARIADB_PROFILE.writable_geometry_type(geometry),
                "{geometry}"
            );
        }

        // E qui la divergenza vera della scrittura, che non e nella bandiera
        // ma nella **forma della colonna**. `MySQL` la vincola all'SRID;
        // `MariaDB` non puo — `raw.spatial_write_forms` ha misurato 1064 su
        // entrambe le major — e il CRS si sposta dentro i valori.
        assert_eq!(
            MYSQL_PROFILE.geometry_column_ddl(4_326),
            "GEOMETRY SRID 4326"
        );
        assert_eq!(MARIADB_PROFILE.geometry_column_ddl(4_326), "GEOMETRY");

        // Conseguenza diretta, e la riga che teneva chiusa la scrittura prima
        // ancora della bandiera: dove la colonna e vincolata il catalogo porta
        // l'SRID e deve essere quello, dove non puo esserlo il catalogo tace e
        // non c'e niente da confrontare. Il confronto secco falliva sempre sul
        // secondo, perche `None` non e mai uguale a `Some(4326)`.
        assert!(MYSQL_PROFILE.geometry_target_srid_is_compatible(Some(4_326), 4_326));
        assert!(!MYSQL_PROFILE.geometry_target_srid_is_compatible(None, 4_326));
        assert!(MARIADB_PROFILE.geometry_target_srid_is_compatible(None, 4_326));
        assert!(!MARIADB_PROFILE.geometry_target_srid_is_compatible(Some(4_326), 4_326));
    }

    #[test]
    fn mariadb_does_not_promise_index_parts_its_query_cannot_produce() {
        // Le due affermazioni devono restare vere insieme, per ogni profilo:
        // chi dichiara di pubblicare le parti funzionali deve selezionare la
        // colonna da cui si riconoscono, e chi non la seleziona non deve
        // dichiararlo. Su MariaDB quella colonna non esiste.
        for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
            assert_eq!(
                profile.reports_functional_index_parts(),
                profile.object_indexes_query().contains("EXPRESSION AS"),
                "{}: la bandiera non corrisponde alla query che la sostiene",
                profile.product()
            );
        }
        assert!(!MARIADB_PROFILE.reports_functional_index_parts());
    }

    #[test]
    fn the_profile_names_the_product_it_serves() {
        assert_eq!(MYSQL_PROFILE.product(), "MySQL");
        assert_eq!(MYSQL_PROFILE.kind(), ProviderKind::Mysql);
    }
}
