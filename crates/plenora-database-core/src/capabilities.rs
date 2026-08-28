#![allow(clippy::struct_excessive_bools)]
//! Capability dichiarate dai provider sul bordo pubblico.
//!
//! Il wire contract usa flag indipendenti: un bitset o un enum non
//! rappresenterebbe capability combinabili liberamente e renderebbe meno
//! stabile il JSON.
//!
//! I campi opzionali deserializzano con default fail-closed: una capability
//! assente resta `false` e uno scope assente resta [`TransactionScope::None`].
//! La serializzazione continua invece a emettere l'envelope completo. Il
//! documento minimo condiviso fra schema e decoder è
//! `contracts/v2/examples/capabilities-minimal.json`.

use crate::geometry::Dimensions;
use crate::geometry::SpatialSemantics;
use crate::plan::ProviderKind;
use crate::relational::SpatialFunction;
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
    /// **Descrittivo.** Un valore `true` richiederebbe anche un'operazione del
    /// contratto capace di indirizzare e riaprire il cursore.
    #[serde(default)]
    pub server_cursor: bool,
    /// La lettura sa saltare le prime `row_offset` righe.
    ///
    /// E' cio che il renderer emette come `OFFSET`, e non ha niente a che
    /// vedere con `row_limit`: quello e un tetto, questo e una finestra, e
    /// una finestra senza `order_by` non e riproducibile — il contratto
    /// infatti rifiuta `row_offset` senza ordinamento.
    ///
    /// E governata da [`crate::plan::ReadOperation::row_offset`]: i provider
    /// la rendono nel proprio dialetto e `prepare` rifiuta un offset quando la
    /// capability non e pubblicata.
    #[serde(default)]
    pub pagination: bool,
    /// La lettura sa restituire un sottoinsieme delle colonne.
    pub projection: bool,
    /// La lettura sa filtrare, nelle forme che il renderer qualifica.
    ///
    /// `true` non significa «qualunque filtro»: significa le forme che il
    /// provider rende, e quelle che non rende restano rifiutate e misurate
    /// dalle rispettive sonde.
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
    /// `Append` e `TruncateInsert` sono promesse diverse su cosa
    /// succede alle righe che c'erano prima. `Append` le lascia, questa le
    /// toglie. Su `MySQL` `TRUNCATE TABLE` implica commit, quindi la seconda non
    /// e recuperabile come un normale append e resta qualificata separatamente.
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
    /// Falso ovunque per la **forma** del percorso:
    /// [`crate::provider::Provider::write`] riceve uno stream di batch e rende
    /// un riassunto: e un pozzo che consuma e conta. Trasportare ogni riga
    /// restituita dentro quel riassunto vuol dire trattenere in memoria una
    /// quantita proporzionale a uno stream **illimitato per costruzione**, e
    /// il contratto non ha un posto dove far scorrere le righe che tornano.
    /// Cambiarlo non e aggiungere un campo: e cambiare la direzione di
    /// quell'API.
    ///
    /// **Descrittivo**: questa superficie non restituisce righe.
    ///
    /// # Superficie disponibile
    ///
    /// Il `returning` degli statement portable e un'altra superficie, vive nel
    /// piano, ed e limitata da cio che il chiamante scrive in quello statement.
    /// Rende cio che il server ha generato — chiavi di sequenza, default
    /// calcolati — e funziona: `live_portable_returning_carries_what_the_server_generated`
    /// lo attraversa nelle quattro forme su `PostgreSQL`.
    ///
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
    /// metodi, quindi chi implementa quel contratto li implementa.
    ///
    /// Cio che resta diverso e il **rilascio**: `PostgreSQL` e `MySQL` hanno
    /// `RELEASE SAVEPOINT`, T-SQL no — i suoi savepoint si liberano al commit.
    /// Questa bandiera non lo promette, e nomina soltanto le due istruzioni
    /// che tutti e quattro offrono.
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

/// L'intersezione delle funzioni offerte su ogni semantica.
///
/// Esiste perche `SpatialCapabilities::functions` non venga scritto a mano
/// accanto a `functions_by_semantics`: sarebbero due fonti per lo stesso fatto,
/// e la prima smetterebbe di seguire la seconda il giorno in cui qualcuno apre
/// una funzione su una semantica sola.
///
/// Una mappa vuota rende una lista vuota, che e la risposta giusta: un prodotto
/// senza semantiche spatial dichiarate non garantisce nessuna funzione.
#[must_use]
pub fn intersect_spatial_functions(
    by_semantics: &BTreeMap<SpatialSemantics, Vec<SpatialFunction>>,
) -> Vec<SpatialFunction> {
    let mut entries = by_semantics.values();
    let Some(first) = entries.next() else {
        return Vec::new();
    };
    let rest: Vec<_> = entries.collect();
    first
        .iter()
        .filter(|function| rest.iter().all(|other| other.contains(function)))
        .copied()
        .collect()
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
    /// Sottoinsieme garantito per **ogni** semantica spatial pubblicizzata.
    ///
    /// Un provider con capability native asimmetriche dichiara qui
    /// l'intersezione. Cio che compare vale su qualunque colonna spatial del
    /// prodotto; l'offerta completa sta in `functions_by_semantics`.
    #[serde(default)]
    pub functions: Vec<SpatialFunction>,
    /// Le funzioni offerte su ciascuna semantica, per intero.
    ///
    /// L'intersezione da sola nasconde le funzioni disponibili su una sola
    /// semantica; l'unione prometterebbe invece funzioni non valide per tutte.
    ///
    /// # La forma
    ///
    /// Ogni voce e **completa** per la sua semantica, non un elenco di aggiunte
    /// da unire all'intersezione. Un consumatore legge la voce del tipo della
    /// propria colonna e ha finito; non deve fare aritmetica su due liste per
    /// sapere cosa puo chiamare.
    ///
    /// Le chiavi sono esattamente le semantiche dichiarate: una voce per una
    /// semantica non pubblicizzata sarebbe una promessa su un tipo che il
    /// prodotto dice di non avere.
    ///
    /// # L'invariante
    ///
    /// `functions` e l'intersezione di queste voci, e non e una convenzione
    /// che ogni provider ricalcola a mano: la calcola
    /// [`intersect_spatial_functions`], e `ProviderCapabilities::validate` la
    /// pretende. Due fonti per lo stesso fatto sarebbero una fonte di troppo, e
    /// qui la seconda sarebbe quella che invecchia in silenzio.
    ///
    /// # Additivo
    ///
    /// Il default vuoto non aggiunge promesse; il lettore conserva
    /// l'intersezione in `functions`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub functions_by_semantics: BTreeMap<SpatialSemantics, Vec<SpatialFunction>>,
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
    /// Vive nel core perche provider, engine e testkit condividano lo stesso
    /// validatore del contratto.
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
    /// La validazione avviene sul bordo del provider, prima che un documento
    /// incoerente raggiunga il consumatore.
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
    /// Per questo non stanno in [`Self::validate`], sul percorso di consumo di
    /// `prepare`: rifiutare documenti ammessi dallo schema restringerebbe la
    /// major v2 senza una nuova major.
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
#[path = "capabilities_tests.rs"]
mod tests;
