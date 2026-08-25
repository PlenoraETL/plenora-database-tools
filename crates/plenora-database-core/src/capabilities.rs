#![allow(clippy::struct_excessive_bools)]
// Il wire contract usa flag indipendenti: un bitset o enum renderebbe il JSON
// meno stabile e non rappresenterebbe capability combinabili liberamente.
//
// # Perche i campi opzionali hanno `#[serde(default)]`
//
// `contracts/v2/capabilities.schema.json` dichiara obbligatorio molto meno di
// quanto questi tipi pretendessero: `server_cursor`, `pagination`,
// `resumable`, mezze `writes`, l'intera sezione `transactions`, quattro flag
// spatial ed `extension_versions` erano facoltativi nello schema e
// obbligatori qui. Un documento valido secondo il contratto falliva la
// deserializzazione: il "contratto unico" era unico solo finche nessuno
// scriveva il documento minimo.
//
// L'allineamento va nella direzione che non rompe nulla — Rust diventa
// tollerante quanto lo schema, invece che lo schema severo quanto Rust — cosi
// non serve una major v3: un documento che era valido prima lo resta.
//
// I default non sono neutri, sono **fail-closed**, ed e la regola del
// progetto: una capability resta `false` finche non esiste una prova
// riproducibile che la sostiene, e `not_measured` non e un `no` ma non apre
// niente lo stesso. Un campo assente e una capability non dichiarata, quindi
// `false`; uno `scope` assente e [`TransactionScope::None`].
//
// La serializzazione non cambia: questi tipi continuano a emettere tutti i
// campi, quindi l'uscita resta valida per lo schema. Le due direzioni sono
// verificate su un unico documento, `contracts/v2/examples/
// capabilities-minimal.json`: `scripts/phase0_validate.py` lo valida contro lo
// schema, il test in fondo a questo file lo deserializza.

use crate::geometry::Dimensions;
use crate::plan::ProviderKind;
use crate::query::SpatialFunction;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadCapabilities {
    /// La lettura consegna batch invece di materializzare il result set.
    ///
    /// La consulta l'engine prima di accettare una `database.read`.
    pub streaming: bool,
    /// Esiste un **cursore server nominato**, che il consumatore puo
    /// indirizzare e riaprire.
    ///
    /// Non e lo streaming: `streaming = true` dice che i batch arrivano a
    /// pezzi, questo direbbe che quei pezzi hanno un nome sul server e che
    /// una seconda sessione puo riprenderli. Nessun provider lo offre — il
    /// data path `PostgreSQL` usa `RowStream` con backpressure, che e la
    /// prima cosa e non la seconda — e nessun piano lo chiede, quindi
    /// l'engine non lo consulta.
    ///
    /// **Descrittivo.** Sta nel documento capability perche un consumatore
    /// che pianifica una lettura lunga vuole sapere se puo riprenderla, e la
    /// risposta e no. Il giorno che un provider lo offrisse, servirebbe prima
    /// un'operazione nel contratto che lo chieda: aprirlo senza sarebbe una
    /// promessa senza superficie.
    #[serde(default)]
    pub server_cursor: bool,
    /// La lettura sa saltare le prime `row_offset` righe.
    ///
    /// E' cio che il renderer emette come `OFFSET`, e non ha niente a che
    /// vedere con `row_limit`: quello e un tetto, questo e una finestra, e
    /// una finestra senza `order_by` non e riproducibile — il contratto
    /// infatti rifiuta `row_offset` senza ordinamento.
    ///
    /// # Come e stata chiusa
    ///
    /// Il campo prometteva una superficie che la sua operazione non esponeva:
    /// `row_offset` viveva su [`crate::query::QueryOperation`] e
    /// [`crate::plan::ReadOperation`] aveva il solo `row_limit`, quindi **un
    /// piano di lettura non poteva chiedere una finestra**. Una promessa che
    /// nessun piano puo riscuotere non e ne mantenuta ne smentita.
    ///
    /// La sua assenza di definizione aveva gia prodotto due letture diverse
    /// dello stesso campo: `PostgreSQL` e SQL Server lo pubblicavano `true`,
    /// `MySQL` e `MariaDB` `false`, e nessuno dei quattro rendeva un offset
    /// su quel percorso — perche il percorso non ce l'aveva.
    ///
    /// Delle due uscite possibili — spostare il campo, o chiuderlo ovunque —
    /// e stata presa la terza: dare all'operazione cio che il campo
    /// descriveva. `ReadOperation` ha ora `row_offset`, i quattro provider lo
    /// rendono nella forma del proprio dialetto, e l'engine lega la bandiera
    /// al campo — un offset chiesto a un provider che non la pubblica viene
    /// rifiutato in `prepare`. Da descrittiva e diventata governata, ed e
    /// l'unica delle sei ad averlo fatto.
    #[serde(default)]
    pub pagination: bool,
    /// La lettura sa restituire un sottoinsieme delle colonne.
    pub projection: bool,
    /// La lettura sa filtrare, nelle forme che il renderer qualifica.
    ///
    /// `true` non significa «qualunque filtro»: significa le forme che il
    /// provider rende, e quelle che non rende restano rifiutate. Per `MySQL`
    /// e `MariaDB` sono tredici, e ciascuna ha la propria sonda.
    pub filter: bool,
    /// La lettura sa ordinare.
    pub ordering: bool,
    /// Una lettura interrotta puo riprendere da dove era arrivata.
    ///
    /// Richiede un punto di ripresa che sopravviva alla sessione — un
    /// cursore nominato, o una chiave di continuazione nel contratto — e non
    /// esiste ne l'uno ne l'altra. Falso ovunque.
    ///
    /// **Descrittivo**, come [`Self::server_cursor`], e per la stessa
    /// ragione: chi pianifica una lettura lunga deve saperlo prima di
    /// cominciarla.
    #[serde(default)]
    pub resumable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteCapabilities {
    pub create: bool,
    pub append: bool,
    /// Se il provider qualifica `TruncateInsert`, che **non** e un `Append`.
    ///
    /// Le due mode hanno condiviso una bandiera fino al contratto v1, e per
    /// `MySQL` quella bandiera diceva gia il falso: pubblica `append = true` e
    /// rifiuta `TruncateInsert` in prepare, perche li `TRUNCATE TABLE` e DDL
    /// con commit implicito — le righe sparirebbero prima dell'INSERT e nessun
    /// rollback le riporterebbe indietro. Il contratto prometteva percio cio
    /// che il provider negava, e il consumatore lo scopriva a piano gia
    /// compilato.
    ///
    /// L'alias e emerso qualificando `MariaDB`, che un provider pubblico non
    /// ce l'ha ancora: e stato il tentativo di aprire il suo `append` a far
    /// guardare **chi legge** la bandiera. La sovradichiarazione era di
    /// `MySQL`, e c'era da prima.
    ///
    /// Separarle non e una formalita: sono due promesse diverse su cosa
    /// succede alle righe che c'erano prima. `Append` le lascia, questa le
    /// toglie — e il modo in cui le toglie decide se un fallimento e
    /// recuperabile.
    pub truncate_insert: bool,
    pub update: bool,
    pub upsert: bool,
    pub replace: bool,
    #[serde(default)]
    pub delete_by_keys: bool,
    /// Le righe raggiungono il server **a blocchi**, non una per volta.
    ///
    /// E' cio che decide l'ordine di grandezza di una scrittura: un `INSERT`
    /// per riga e un `INSERT` per batch differiscono di un round-trip per
    /// riga. Tutti i provider di questo repository scrivono a blocchi, e
    /// nessuno espone la variante per riga.
    ///
    /// **Descrittivo.** L'engine non lo consulta perche non c'e un piano che
    /// chieda l'una o l'altra forma: la scelta e del provider, non del
    /// chiamante. Sta nel documento perche chi dimensiona un carico ha
    /// bisogno di saperlo prima di misurarlo.
    #[serde(default)]
    pub bulk: bool,
    /// Un parametro puo trasportare un **array**, legato una volta sola.
    ///
    /// Non e il batching di [`Self::bulk`]: quello manda piu righe in uno
    /// statement, questo manderebbe piu valori in un parametro — `WHERE id =
    /// ANY($1)` invece di `n` segnaposto. Cambia il numero di parametri
    /// legati, non il numero di round-trip, e conta dove il tetto e sui
    /// parametri: SQL Server ne ammette 2100.
    ///
    /// Nessun provider lo offre, e il contratto non ha una forma di piano che
    /// lo chieda. **Descrittivo**, e falso ovunque.
    #[serde(default)]
    pub array_binding: bool,
    /// L'esito di una scrittura trasporta le **righe restituite** dal server.
    ///
    /// `RETURNING` su `PostgreSQL`, `OUTPUT` su SQL Server: la scrittura
    /// rende cio che ha scritto — chiavi generate, valori di default,
    /// timestamp calcolati — senza una seconda interrogazione.
    ///
    /// Falso ovunque, e per una ragione che sta a monte dei provider:
    /// [`crate::outcome::WriteOutcome`] conta righe e non le trasporta. Il
    /// giorno che le trasportasse sarebbe una major del contratto, non
    /// l'apertura di una bandiera.
    ///
    /// **Descrittivo** finche quel giorno non arriva. Da non confondere con
    /// il `returning` degli statement portable, che e un'altra superficie e
    /// vive nel piano.
    #[serde(default)]
    pub returning: bool,
    /// Un fallimento prima del commit annulla **le righe** scritte
    /// dall'operazione.
    ///
    /// Il flag riguarda i dati, non lo schema. Una mode che prepara il target
    /// con del DDL — `Create`, e su alcuni provider `Replace` — puo lasciarlo
    /// dietro di se anche quando le righe tornano indietro, se il motore
    /// esegue il DDL fuori dalla transazione. Quel comportamento e descritto
    /// da [`TransactionCapabilities::transactional_ddl`]: `false` significa
    /// che il DDL di preparazione sopravvive al rollback.
    ///
    /// Letti insieme, i due flag dicono cosa aspettarsi:
    ///
    /// | `rollback_on_failure` | `transactional_ddl` | esito di un `Create` fallito |
    /// | --- | --- | --- |
    /// | `true` | `true` | niente resta: righe e tabella annullate |
    /// | `true` | `false` | la tabella resta vuota, le righe no |
    /// | `false` | — | possono restare righe parziali |
    ///
    /// Il caso `true`/`false` non e ambiguo per il chiamante: l'esito
    /// dell'operazione lo dichiara riga per riga con
    /// [`crate::error::RemoteEffect::Partial`] e
    /// [`crate::error::RetryDisposition::RequiresRecovery`], perche un retry
    /// cieco troverebbe il target gia esistente.
    #[serde(default)]
    pub rollback_on_failure: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionScope {
    /// Default deliberato: uno scope non dichiarato non e uno scope
    /// transazionale.
    #[default]
    None,
    Statement,
    Transaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionCapabilities {
    /// L'operazione intera sta in una transazione sola.
    ///
    /// La consulta l'engine quando il piano dichiara il profilo omonimo.
    #[serde(default)]
    pub single_transaction: bool,
    /// Il provider espone `SAVEPOINT` e `ROLLBACK TO` al chiamante.
    ///
    /// Non «il motore li supporta» — li supportano tutti — ma «il provider li
    /// mette a disposizione». `TransactionScope` non ha default per i tre
    /// metodi, quindi chi implementa quel contratto li implementa: `MySQL` e
    /// `PostgreSQL` li offrono entrambi. SQL Server no, e per una ragione a
    /// monte — non espone affatto uno scope transazionale, quindi non c'e
    /// niente su cui chiamarli.
    ///
    /// **Descrittivo.** L'engine non lo consulta perche un savepoint non si
    /// chiede in un piano: lo usa chi tiene lo scope in mano, e lo scopre dal
    /// tipo. Sta nel documento perche e una differenza reale fra due provider
    /// entrambi qualificati.
    #[serde(default)]
    pub savepoints: bool,
    /// Il DDL di preparazione appartiene alla transazione.
    ///
    /// `false` significa che il motore lo esegue con commit implicito, quindi
    /// una tabella creata da `Create` sopravvive al rollback delle righe. La
    /// tabella in [`WriteCapabilities::rollback_on_failure`] mostra le quattro
    /// combinazioni; qui basta dire che i due flag parlano di due oggetti
    /// diversi — le righe e lo schema.
    ///
    /// **Descrittivo**, ma con una conseguenza che il chiamante riceve
    /// comunque: sui provider dove e `false`, un `Create` fallito dichiara
    /// `Partial` e `RequiresRecovery` invece di `RolledBack`.
    #[serde(default)]
    pub transactional_ddl: bool,
    /// La sostituzione passa da un oggetto di staging scambiato alla fine.
    #[serde(default)]
    pub staged_swap: bool,
    /// L'ampiezza della garanzia transazionale.
    #[serde(default)]
    pub scope: TransactionScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialCapabilities {
    pub read_wkb: bool,
    pub write_wkb: bool,
    #[serde(default)]
    pub geometry: bool,
    #[serde(default)]
    pub geography: bool,
    #[serde(default)]
    pub spatial_index: bool,
    #[serde(default)]
    pub mixed_geometry_types: bool,
    pub dimensions: Vec<Dimensions>,
    /// Sottoinsieme garantito per ogni semantica spatial pubblicizzata.
    ///
    /// Un provider con capability native asimmetriche deve sotto-dichiarare
    /// l'intersezione; il contratto non consente di attribuire una funzione
    /// soltanto a `geometry` o soltanto a `geography`.
    #[serde(default)]
    pub functions: Vec<SpatialFunction>,
    /// Le colonne geometriche si leggono solo con un CRS dichiarato dal piano.
    ///
    /// Serve perche `geometry` da sola non sa dire la verita su un prodotto
    /// come `MariaDB`. Li il registro OGC esiste e porta un `SRID`, ma vale
    /// sempre zero: nessuna DDL puo vincolare una geometry a un sistema di
    /// riferimento. La lettura funziona — e misurata — **a condizione** che il
    /// chiamante dichiari il CRS in
    /// [`crate::plan::ReadOperation::declared_crs`], e che il provider lo
    /// verifichi valore per valore.
    ///
    /// Con la sola `geometry` quella condizione non e esprimibile: `false`
    /// negherebbe una lettura che funziona, `true` prometterebbe che una
    /// lettura semplice basti. Le due bandiere insieme dicono la cosa giusta —
    /// `geometry: true, requires_declared_crs: true` — e un chiamante che
    /// ignora la seconda riceve un rifiuto in prepare, non un CRS inventato.
    ///
    /// **Falsa** dove il catalogo il CRS lo sa: `PostgreSQL` lo legge da
    /// `geometry_columns`, `MySQL` da `information_schema.columns.SRS_ID`, e
    /// li una dichiarazione del chiamante sarebbe una seconda fonte per lo
    /// stesso fatto.
    #[serde(default)]
    pub requires_declared_crs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLimits {
    pub max_identifier_bytes: Option<u64>,
    pub max_bind_parameters: Option<u64>,
    pub max_statement_bytes: Option<u64>,
    pub max_batch_rows: Option<u64>,
    pub max_payload_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapabilities {
    pub schema_version: u32,
    pub provider: ProviderKind,
    pub provider_version: String,
    #[serde(default)]
    pub extension_versions: BTreeMap<String, String>,
    pub reads: ReadCapabilities,
    pub writes: WriteCapabilities,
    pub transactions: TransactionCapabilities,
    pub spatial: SpatialCapabilities,
    pub limits: ProviderLimits,
}

/// Lunghezze massime dichiarate da `contracts/v2/capabilities.schema.json`.
///
/// In **caratteri**: `maxLength` di JSON Schema conta caratteri Unicode.
const MAX_PROVIDER_VERSION_CHARS: usize = 512;
const MAX_EXTENSION_NAME_CHARS: usize = 128;
const MAX_EXTENSION_VERSION_CHARS: usize = 256;

impl ProviderCapabilities {
    /// Verifica che il documento sia un capability v2 coerente.
    ///
    /// Il controllo e provider-independent: riguarda la forma e le
    /// contraddizioni interne, non cosa un singolo motore sappia fare.
    ///
    /// Viveva soltanto in `plenora-database-testkit`, cioe era raggiungibile
    /// solo da chi scriveva test di conformita. Chi *consuma* un documento —
    /// `plenora_database_engine::prepare`, e da li la CLI — non lo attraversava
    /// e poteva quindi dichiarare "prepared" un piano confrontato con un
    /// documento che il contratto rifiuta. Sta qui perche ci sia una fonte
    /// sola: il testkit ora delega.
    ///
    /// Verifica **soltanto** cio che il contratto dichiara: major, campi non
    /// vuoti, lunghezze massime, duplicati, limiti diversi da zero. Le
    /// relazioni fra capability che lo schema non esprime stanno in
    /// [`Self::validate_coherence`], perche rifiutarle qui — sul percorso di
    /// consumo — significherebbe restringere la major v2.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per major non supportata, campi vuoti o oltre le
    /// lunghezze del contratto, duplicati e limiti a zero.
    pub fn validate(&self) -> crate::Result<()> {
        if self.schema_version != 2 {
            return Err(invalid(
                "documento capability con schema_version non supportata",
            ));
        }

        // `minLength: 1` conta i code point, non i caratteri non-spazio: una
        // versione fatta di soli spazi e dentro la major v2, per quanto sia
        // inutile. Qui si applica la lunghezza dichiarata e nient'altro; il
        // giudizio sul contenuto sta in `validate_coherence`.
        if self.provider_version.is_empty() {
            return Err(invalid("documento capability senza versione provider"));
        }
        if self.provider_version.chars().count() > MAX_PROVIDER_VERSION_CHARS {
            return Err(invalid("provider_version oltre la lunghezza del contratto"));
        }
        for (name, version) in &self.extension_versions {
            // Lo schema non pone alcun minimo su nome e versione di
            // un'estensione: solo `propertyNames.maxLength` e `maxLength`.
            if name.chars().count() > MAX_EXTENSION_NAME_CHARS
                || version.chars().count() > MAX_EXTENSION_VERSION_CHARS
            {
                return Err(invalid(
                    "nome o versione di estensione oltre la lunghezza del contratto",
                ));
            }
        }

        if has_duplicates(&self.spatial.dimensions) {
            return Err(invalid(
                "documento capability con dimensioni spatial duplicate",
            ));
        }
        if has_duplicates(&self.spatial.functions) {
            return Err(invalid(
                "documento capability con funzioni spatial duplicate",
            ));
        }

        let limits = &self.limits;
        let bounded = [
            limits.max_identifier_bytes,
            limits.max_bind_parameters,
            limits.max_statement_bytes,
            limits.max_batch_rows,
            limits.max_payload_bytes,
        ];
        if bounded.into_iter().flatten().any(|limit| limit == 0) {
            return Err(invalid(
                "documento capability con limite esplicito pari a zero",
            ));
        }
        Ok(())
    }

    /// Il documento appena costruito, sul punto di uscire dal provider.
    ///
    /// Qui il repository **pubblica**, e qui valgono entrambi i giudizi:
    /// quello del contratto e quello di coerenza. La distinzione fra i due
    /// serve dalla parte del consumo — dove rifiutare piu di quanto il
    /// contratto dica significherebbe restringere la major — e non da questa:
    /// chi produce non ha alcun motivo di emettere un documento che sa
    /// contraddittorio.
    ///
    /// Nessuno dei tre provider chiamava nulla prima di restituire il proprio
    /// documento. La versione del motore veniva da una probe: bastava un
    /// server che rispondesse una stringa vuota perche uscisse un documento
    /// fuori contratto, e il primo a saperlo sarebbe stato il consumatore.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` se il documento viola il contratto o e incoerente.
    pub fn published(self) -> crate::Result<Self> {
        self.validate()?;
        self.validate_coherence()?;
        Ok(self)
    }
}

impl ProviderCapabilities {
    /// Le relazioni fra capability che **il contratto non esprime**.
    ///
    /// Sono coerenze vere — `server_cursor` senza `streaming` non ha senso, e
    /// `staged_swap` senza DDL transazionale non e realizzabile — ma
    /// `contracts/v2/capabilities.schema.json` non le dichiara: un documento
    /// che le viola e **valido secondo il contratto pubblico**.
    ///
    /// Per questo non stanno in [`Self::validate`], che sta sul percorso di
    /// consumo di `prepare`. Averle messe li ha fatto rifiutare al prodotto
    /// documenti che la major v2 ammette, e restringere cio che una major
    /// accetta e proprio quello che la regola 2 di AGENTS.md vieta senza una
    /// major nuova.
    ///
    /// Restano pero verificate dove il repository **pubblica**: la conformita
    /// dei provider le chiama, quindi nessun provider di questo workspace puo
    /// emettere un documento incoerente. Se un giorno diventassero normative,
    /// il posto e `contracts/v3/`, con l'equivalenza schema-Serde-validatore
    /// provata da test incrociati.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per una combinazione di capability contraddittoria.
    pub fn validate_coherence(&self) -> crate::Result<()> {
        // Una versione provider di soli spazi, o un'estensione senza nome,
        // superano lo schema e non identificano nulla. Chi pubblica non deve
        // emetterle; chi consuma non puo rifiutarle senza cambiare major.
        if self.provider_version.trim().is_empty() {
            return Err(invalid(
                "versione provider di soli spazi nel documento capability",
            ));
        }
        for (name, version) in &self.extension_versions {
            if name.trim().is_empty() || version.trim().is_empty() {
                return Err(invalid(
                    "documento capability con estensione o versione vuota",
                ));
            }
        }

        let spatial = &self.spatial;
        let has_spatial_semantics = spatial.geometry || spatial.geography;
        let claims_spatial_behavior = spatial.read_wkb
            || spatial.write_wkb
            || spatial.spatial_index
            || spatial.mixed_geometry_types
            || !spatial.dimensions.is_empty()
            || !spatial.functions.is_empty();
        if claims_spatial_behavior && !has_spatial_semantics {
            return Err(invalid(
                "documento capability spatial senza geometry o geography",
            ));
        }
        if has_spatial_semantics && spatial.dimensions.is_empty() {
            return Err(invalid("documento capability spatial senza dimensionalita"));
        }

        if self.reads.server_cursor && !self.reads.streaming {
            return Err(invalid(
                "server_cursor richiede streaming nel documento capability",
            ));
        }

        let transactions = &self.transactions;
        if transactions.savepoints && !transactions.single_transaction {
            return Err(invalid("savepoints richiede single_transaction"));
        }
        if transactions.staged_swap
            && (!transactions.single_transaction || !transactions.transactional_ddl)
        {
            return Err(invalid(
                "staged_swap richiede transazione singola e DDL transazionale",
            ));
        }
        if transactions.single_transaction && transactions.scope == TransactionScope::None {
            return Err(invalid("single_transaction non puo avere scope none"));
        }
        if !transactions.single_transaction && transactions.scope == TransactionScope::Transaction {
            return Err(invalid("scope transaction richiede single_transaction"));
        }
        Ok(())
    }
}

fn invalid(message: &'static str) -> crate::DatabaseError {
    crate::DatabaseError::invalid_plan(message)
}

fn has_duplicates<T: PartialEq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[index + 1..].contains(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Il documento minimo ammesso dallo schema deve deserializzare.
    ///
    /// E' lo stesso file che `scripts/phase0_validate.py` valida contro
    /// `capabilities.schema.json`: una sola fonte, verificata da entrambi i
    /// lati. Se qualcuno rende obbligatorio qui un campo che lo schema lascia
    /// facoltativo, questo test lo dice subito invece di lasciarlo scoprire a
    /// un consumatore.
    /// Il confine fra cio che il contratto dichiara e cio che il prodotto
    /// pretende, fissato dove sta.
    ///
    /// `capabilities.schema.json` **non** esprime le relazioni fra capability:
    /// un documento con `server_cursor` senza `streaming` e valido secondo la
    /// major v2. Averlo fatto rifiutare da `validate()` — che sta sul percorso
    /// di consumo di `prepare` — restringeva cio che la v2 accetta, e
    /// restringere una major senza cambiarla e proprio quello che la regola 2
    /// di AGENTS.md vieta.
    ///
    /// Quindi: `validate()` lo accetta, `validate_coherence()` lo rifiuta, e la
    /// conformita dei provider chiama la seconda. Se qualcuno riporta la
    /// relazione in `validate()`, questo test lo dice.
    #[test]
    fn a_relation_the_contract_does_not_state_is_not_rejected_on_the_consumption_path() {
        let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
        let mut capabilities: ProviderCapabilities =
            serde_json::from_slice(bytes).expect("documento minimo");
        capabilities.reads.server_cursor = true;
        capabilities.reads.streaming = false;

        capabilities
            .validate()
            .expect("il contratto v2 non vieta questa combinazione");
        capabilities
            .validate_coherence()
            .expect_err("ma resta incoerente, e chi pubblica non deve emetterla");
    }

    /// Un limite che eccede `u64` e conforme allo schema v2 — che dice
    /// `"type": "integer"` senza massimo — e resta illeggibile da questa
    /// implementazione, che lo tiene in `u64`.
    ///
    /// Il documento non viene rifiutato: non arriva neppure a esistere. Il
    /// confine e la deserializzazione, e il messaggio di errore che ne esce
    /// deve dire *quello*, non far credere a un contratto piu stretto di
    /// quello pubblicato.
    #[test]
    fn a_limit_beyond_u64_is_within_the_contract_and_outside_this_reader() {
        let bytes = include_bytes!(
            "../../../contracts/v2/examples/unconsumable-capabilities-limit-over-u64.json"
        );
        serde_json::from_slice::<ProviderCapabilities>(bytes)
            .expect_err("un limite oltre u64 non e rappresentabile qui");
    }

    /// Cio che il contratto **dichiara** resta rifiutato dal percorso di
    /// consumo: il confine si sposta in un verso solo.
    #[test]
    fn what_the_contract_states_is_still_rejected_on_the_consumption_path() {
        let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
        let mut capabilities: ProviderCapabilities =
            serde_json::from_slice(bytes).expect("documento minimo");

        // Lo schema ha `minimum: 1` su questo limite.
        capabilities.limits.max_batch_rows = Some(0);
        capabilities.validate().expect_err("limite a zero");

        // E `minLength: 1` sulla versione del provider.
        capabilities = serde_json::from_slice(bytes).expect("documento minimo");
        capabilities.provider_version = String::new();
        capabilities.validate().expect_err("versione vuota");
    }

    #[test]
    fn the_minimal_contract_document_deserialises() {
        let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
        let capabilities: ProviderCapabilities =
            serde_json::from_slice(bytes).expect("il documento minimo del contratto deve caricare");

        // I default non sono neutri: sono la risposta conservativa.
        assert!(!capabilities.reads.server_cursor);
        assert!(!capabilities.reads.pagination);
        assert!(!capabilities.reads.resumable);
        assert!(!capabilities.writes.delete_by_keys);
        assert!(!capabilities.writes.bulk);
        assert!(!capabilities.writes.array_binding);
        assert!(!capabilities.writes.returning);
        assert!(!capabilities.writes.rollback_on_failure);
        assert!(!capabilities.transactions.single_transaction);
        assert!(!capabilities.transactions.savepoints);
        assert!(!capabilities.transactions.transactional_ddl);
        assert!(!capabilities.transactions.staged_swap);
        assert_eq!(capabilities.transactions.scope, TransactionScope::None);
        assert!(!capabilities.spatial.geometry);
        assert!(!capabilities.spatial.geography);
        assert!(!capabilities.spatial.spatial_index);
        assert!(!capabilities.spatial.mixed_geometry_types);
        assert!(capabilities.spatial.functions.is_empty());
        assert!(capabilities.extension_versions.is_empty());
        assert_eq!(capabilities.limits.max_identifier_bytes, None);
    }

    /// L'altra direzione: cio che questi tipi emettono contiene ogni campo
    /// che lo schema richiede. I default rendono tollerante la lettura, non
    /// reticente la scrittura.
    #[test]
    fn serialising_emits_every_field_the_schema_requires() {
        let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
        let capabilities: ProviderCapabilities =
            serde_json::from_slice(bytes).expect("documento minimo");
        let emitted = serde_json::to_value(&capabilities).expect("serializzabile");

        for field in [
            "schema_version",
            "provider",
            "provider_version",
            "reads",
            "writes",
            "transactions",
            "spatial",
            "limits",
        ] {
            assert!(emitted.get(field).is_some(), "manca `{field}`");
        }
        for field in ["streaming", "projection", "filter", "ordering"] {
            assert!(emitted["reads"].get(field).is_some(), "reads.{field}");
        }
        for field in [
            "create",
            "append",
            "truncate_insert",
            "update",
            "upsert",
            "replace",
        ] {
            assert!(emitted["writes"].get(field).is_some(), "writes.{field}");
        }
        for field in ["read_wkb", "write_wkb", "dimensions"] {
            assert!(emitted["spatial"].get(field).is_some(), "spatial.{field}");
        }
    }
}
