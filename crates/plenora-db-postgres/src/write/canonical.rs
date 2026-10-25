//! Le regole V2 e P su PostgreSQL: cio che un valore puo diventare nel tipo
//! della sua colonna.
//!
//! - **V2** (valori indipendenti dalla sessione): un valore che la scrittura
//!   manda come testo, e che il server legge con la funzione d'ingresso del
//!   tipo, arriva soltanto a un **elenco chiuso** di tipi la cui lettura non
//!   dipende dalla sessione ([`TEXT_INPUTS`]), o ai tipi temporali nella forma
//!   canonica che non ne dipende ([`canonical`]). Ogni altro tipo, anche di
//!   un'estensione, si rifiuta (mondo chiuso).
//! - **P** (nessuna perdita silenziosa con `Strict`): un valore che il tipo
//!   del target arrotonderebbe o troncherebbe si rifiuta prima di ogni
//!   effetto, in ogni profilo e nel dry run.
//!
//! Misurato il 25/10 su PostgreSQL 16 (letture indipendenti):
//!
//! - `daterange` con il limite `'01/02/2026'`: 2 gennaio con `DateStyle =
//!   'ISO, MDY'`, 1 febbraio con `'ISO, DMY'`, entrambi `committed`;
//! - `tstzrange` con il limite `'2026-01-02 10:00:00'`, senza offset: l'istante
//!   cambiava con il `TimeZone` del ruolo;
//! - un `interval` scritto da `CopyText` cambiava segno con `IntervalStyle =
//!   sql_standard` (corretto in `binary_codec::interval_text`);
//! - F3, terza lettura: `'now'` verso una colonna `time` (oid 1083, fuori
//!   dall'elenco dei tipi della sessione di allora) con `Lossy` e `CopyText`
//!   si scriveva come l'ora del `TimeZone` della sessione;
//! - F5: `timestamp(0)` e `time(0)` arrotondavano 10:00:00.999999 a 10:00:01
//!   in ogni profilo, con `Strict`, dove la scala di un `numeric` si rifiuta.
//!
//! # La regola
//!
//! La transazione della scrittura (e il dry run con la DSN) legge dal
//! catalogo, con il percorso chiuso della regola Q2, le **foglie** del tipo di
//! ogni colonna dell'ingresso: domini, array, range, multirange e tipi
//! composti si attraversano fino ai tipi base ed enum, con la funzione
//! d'ingresso (il simbolo interno, `pg_proc.prosrc` con `prolang = internal`)
//! e il typmod di ciascuna ([`rules`]). La classe di una foglia viene dal
//! simbolo, non dal nome del tipo: `citext` legge con `textin` ed e testo.
//!
//! Poi ogni valore non nullo passa da [`check`] prima della codifica:
//!
//! - una foglia `money` (il separatore decimale dipende da `lc_monetary`) o
//!   fuori dall'elenco chiuso: rifiutata, qualunque sia il valore;
//! - una foglia di testo ([`TEXT_INPUTS`]): ogni valore; con `Strict`, un
//!   testo piu lungo di `varchar(n)`/`char(n)` si rifiuta (il server tronca
//!   in silenzio gli spazi in eccesso: `'abc   '` diventava `'abc'`);
//! - una foglia `jsonb` (regola J, quarta rilettura del 25/10): con `Strict`,
//!   un testo con un oggetto che ripete una chiave, a qualunque profondita e
//!   dopo gli escape, si rifiuta (`jsonb` tiene l'ultimo valore:
//!   `{"b":1,"a":2,"a":3}` diventava `{"a": 3, "b": 1}`); `json` conserva
//!   il testo e non si verifica. Un array si verifica elemento per elemento,
//!   come lista o come letterale nella forma ristretta di [`array_elements`]
//!   (quinta rilettura: il letterale intero si leggeva come JSON); un
//!   letterale fuori da quella forma, o un range scritto come testo, si
//!   rifiuta;
//! - una foglia numerica che arrotonda il testo (`numeric(p,s)`, `real`,
//!   `double precision`, anche dentro un dominio, un array, un range o un
//!   composto): con `Strict` un valore che arriva come testo si rifiuta;
//! - una foglia temporale (`date`, `time`, `timetz`, `timestamp`,
//!   `timestamptz`, `interval`): testo soltanto nella forma canonica — date
//!   ISO `AAAA-MM-GG`, ore `hh:mm[:ss[.f]]`, timestamp ISO, la zona esplicita
//!   per `timestamptz` e `timetz`, intervalli ISO 8601 o con il segno su ogni
//!   campo —, mai i valori speciali (`now`, `today`, `epoch`, `allballs`,
//!   `infinity`); con `Strict`, non piu cifre frazionarie di quante la
//!   colonna ne conserva. I valori Arrow tipizzati, che la scrittura formatta
//!   da se, si ammettono tranne le conversioni che passano dal `TimeZone`
//!   (una data, un timestamp senza zona o un `time` verso `timestamptz` o
//!   `timetz`);
//! - un composto, un multirange o un array di range: soltanto se ogni foglia
//!   e testo (e, con `Strict`, senza lunghezze massime, che qui non si
//!   verificano per campo).
//!
//! Con `Strict` una colonna temporale con precisione ridotta (`timestamp(p)`,
//! `time(p)`, `timetz(p)`, `timestamptz(p)` con `p < 6`, un `interval` con
//! campi o precisione) si rifiuta prima del primo valore ([`refuse_lossy`]),
//! come gia fa il preflight (`LossCategory::Precision`): e lo stesso
//! criterio di `MySQL`, che rifiuta `DATETIME(0)`. Con una politica non
//! stretta la perdita e nel `LossReport` del preflight e il valore passa.
//!
//! Con le politiche non strette ogni foglia che `Strict` rifiuterebbe ha la
//! sua perdita nel `LossReport` del preflight ([`leaf_losses`], lo stesso
//! cammino del catalogo): fino al 25/10 (quinta rilettura) il preflight
//! guardava soltanto la dichiarazione della colonna, e un dominio su
//! `numeric(5,2)`, un `numeric(5,2)[]`, un dominio su `real`, un composto e
//! gli array di domini su `varchar(n)` e `jsonb` arrotondavano o troncavano
//! con il `LossReport` vuoto.
//!
//! La sessione **non** si fissa: `TimeZone` e `DateStyle` decidono anche i
//! default e i trigger del target, e fissarli cambierebbe in silenzio cio che
//! il target scrive. La forma canonica non dipende dalla sessione.
//!
//! # Limite dichiarato
//!
//! - **Regola**: un valore testuale arriva soltanto a un tipo dell'elenco
//!   chiuso, o in forma canonica a un tipo temporale; con `Strict` nessun
//!   valore si arrotonda o si tronca.
//! - **Ambito**: scrittura e dry run con la DSN, per le colonne del target
//!   lette dal catalogo; il dry run senza DSN, e quello di Create, conoscono
//!   soltanto i tipi di `pg_catalog` che il piano dichiara ([`declared`]).
//!   Le colonne spatial hanno la verifica propria
//!   (`plenora_database_core::geometry_value`).
//! - **Hazard**: si rifiutano anche forme che non dipendono dalla sessione
//!   ma non sono nell'elenco (una zona per nome, `Europe/Rome`; una data con
//!   il nome del mese; `infinity`, che un valore Arrow non sa portare: oggi
//!   non si scrive). Un tipo di un'estensione con una funzione d'ingresso in
//!   C (`hstore`, `ltree`) non riceve valori. `char(n)` completa con spazi i
//!   valori piu corti: e la semantica del tipo, che li confronta senza gli
//!   spazi finali, e non si rifiuta. Allo stesso modo `jsonb` riordina le
//!   chiavi, toglie gli spazi e scrive i numeri nella forma di `numeric`
//!   (`1.50` resta `1.50`, `1e2` diventa `100`, `-0` diventa `0`): semantica
//!   del tipo, ammessa con `Strict` e dichiarata con le altre politiche nel
//!   `LossReport` (`native_type`, `warning`, con la chiave ripetuta). Con
//!   `Strict` un testo non arriva a un numero che lo arrotonda, e si rifiuta
//!   anche quando il valore non perderebbe niente (`'1.5'` verso
//!   `numeric(5,2)`: la prova per valore non c'e); con le altre politiche la
//!   perdita si dichiara per classe di foglia, non per valore. Un array di
//!   `jsonb` scritto come letterale fuori dalla forma ristretta (le dimensioni
//!   dichiarate, un elemento con spazi o escape senza virgolette) si rifiuta
//!   con `Strict` anche quando il server lo leggerebbe senza perdite. Le
//!   regole si leggono una volta, prima del primo valore e senza rileggerle
//!   dopo il lock; il preflight le legge su un'altra sessione, prima della
//!   transazione.
//! - **Rientro**: la verifica per campo dei composti; i domini e i composti
//!   dichiarati risolti dal catalogo anche senza DSN; le regole rilette con
//!   la regola R dopo il lock.

use super::mapping_error;
use super::plan::{ColumnSemantics, WriteColumnPlan};
use super::value_codec::range_value;
use crate::error::{classify_error, public_error};
use arrow_array::{Array, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::WriteOperation;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;
use tokio_postgres::GenericClient;

/// Il tipo temporale (o `money`) di una foglia.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionLeaf {
    Date,
    Time,
    Timestamp,
    TimestampTz,
    TimeTz,
    Interval,
    Money,
}

/// Dove sta la foglia nel tipo della colonna.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionShape {
    Scalar,
    Array,
    Range,
    /// Dentro un tipo composto, un multirange o un array di range: il testo
    /// non si verifica per posizione.
    Nested,
}

/// Che cosa la funzione d'ingresso di una foglia fa del testo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LeafClass {
    /// Una lettura che non dipende dalla sessione ([`TEXT_INPUTS`]), con la
    /// lunghezza massima in caratteri di `varchar(n)` e `char(n)`.
    Text { max_chars: Option<usize> },
    /// Un tipo temporale o `money`, con le cifre frazionarie dei secondi che
    /// conserva e se la precisione e ridotta (typmod).
    Session {
        leaf: SessionLeaf,
        fraction: u32,
        reduced: bool,
    },
    /// `jsonb` (regola J): una lettura che non dipende dalla sessione, che
    /// pero tiene soltanto l'ultimo valore di una chiave ripetuta.
    Jsonb,
    /// Un numero che arrotonda il testo: `numeric(p,s)` (typmod), `real`,
    /// `double precision`. Con `Strict` un testo non vi arriva (`'1.005'`
    /// diventava `1.01`); con le altre politiche il preflight dichiara la
    /// perdita ([`LeafLoss::Rounding`]): verso un dominio, un array o un
    /// composto il tipo Arrow e quello della colonna, e la differenza di tipo
    /// non la dichiara.
    Rounding,
    /// Fuori dall'elenco chiuso.
    Unknown,
}

/// Una foglia del tipo della colonna.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Leaf {
    pub(super) class: LeafClass,
    pub(super) shape: SessionShape,
}

/// Le regole V2 e P di una colonna.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionRule {
    pub(super) leaves: Vec<Leaf>,
    /// La politica di mapping e `Strict` (regola P).
    pub(super) strict: bool,
}

impl SessionRule {
    /// La foglia temporale della colonna, se ne ha una sola fuori da un
    /// composto.
    fn single(&self) -> Option<Leaf> {
        match self.leaves.as_slice() {
            [leaf] if leaf.shape != SessionShape::Nested => Some(*leaf),
            _ => None,
        }
    }
}

/// Il motivo pubblico del rifiuto della regola V2.
pub(super) const NOT_CANONICAL: &str = "valore non canonico, o verso un tipo fuori dall'elenco \
     chiuso, che il server interpreterebbe secondo la sessione (DateStyle, IntervalStyle, \
     TimeZone, lc_monetary)";

/// Il motivo pubblico del rifiuto della regola P.
pub(super) const LOSSY_VALUE: &str = "con MappingPolicy::Strict il tipo della colonna \
     arrotonderebbe o troncherebbe il valore (precisione frazionaria, lunghezza massima)";

/// Il motivo pubblico del rifiuto della regola J: un testo JSON con chiavi
/// ripetute verso `jsonb`, che tiene soltanto l'ultima.
pub(super) const DUPLICATE_KEYS: &str = "con MappingPolicy::Strict un oggetto JSON con chiavi \
     ripetute verso jsonb perderebbe i valori tranne l'ultimo";

fn refused() -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        false,
        &format!("scrittura rifiutata: {NOT_CANONICAL}"),
    )
}

fn lossy() -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        false,
        &format!("scrittura rifiutata: {LOSSY_VALUE}"),
    )
}

/// L'elenco chiuso dei simboli interni delle funzioni d'ingresso la cui
/// lettura del testo non dipende dalla sessione: testo, booleani, numeri
/// (`numeric_in` non usa `lc_numeric`), `uuid`, `json`/`jsonb`, indirizzi,
/// stringhe di bit, `bytea` (esadecimale o escape), enum. Fuori: `charin`
/// e `namein` (troncano in silenzio a 1 e 63 byte), `xml_in` (dipende da
/// `xmloption`), `regclassin` e gli altri `reg*` (risolvono nomi con il
/// `search_path`), e ogni funzione in C di un'estensione.
pub(super) const TEXT_INPUTS: &[&str] = &[
    "textin",
    "varcharin",
    "bpcharin",
    "boolin",
    "int2in",
    "int4in",
    "int8in",
    "float4in",
    "float8in",
    "numeric_in",
    "uuid_in",
    "json_in",
    "jsonb_in",
    "inet_in",
    "cidr_in",
    "macaddr_in",
    "macaddr8_in",
    "bit_in",
    "varbit_in",
    "byteain",
    "enum_in",
];

/// La classe di una foglia dal simbolo interno della sua funzione d'ingresso
/// (`None` per una funzione che non e `internal`) e dal suo typmod.
#[must_use]
pub(super) fn classify(symbol: Option<&str>, typmod: i32) -> LeafClass {
    let Some(symbol) = symbol else {
        return LeafClass::Unknown;
    };
    let session = |leaf| {
        // `time(p)`, `timetz(p)`, `timestamp(p)`, `timestamptz(p)`: il typmod
        // e la precisione; senza, sei cifre.
        let (fraction, reduced) = match u32::try_from(typmod) {
            Ok(precision) if precision < 6 => (precision, true),
            _ => (6, false),
        };
        LeafClass::Session {
            leaf,
            fraction,
            reduced,
        }
    };
    match symbol {
        "varcharin" | "bpcharin" => LeafClass::Text {
            // `VARHDRSZ` (4) piu la lunghezza in caratteri.
            max_chars: usize::try_from(typmod)
                .ok()
                .and_then(|typmod| typmod.checked_sub(4)),
        },
        "jsonb_in" => LeafClass::Jsonb,
        "float4in" | "float8in" => LeafClass::Rounding,
        "numeric_in" if typmod >= 0 => LeafClass::Rounding,
        _ if TEXT_INPUTS.contains(&symbol) => LeafClass::Text { max_chars: None },
        "date_in" => LeafClass::Session {
            leaf: SessionLeaf::Date,
            fraction: 0,
            reduced: false,
        },
        "time_in" => session(SessionLeaf::Time),
        "timetz_in" => session(SessionLeaf::TimeTz),
        "timestamp_in" => session(SessionLeaf::Timestamp),
        "timestamptz_in" => session(SessionLeaf::TimestampTz),
        // Il typmod di `interval` porta i campi (16 bit alti, `0x7FFF` tutti)
        // e la precisione (16 bit bassi, `0xFFFF` non dichiarata): `interval
        // day` tronca in silenzio ore e minuti, `interval(0)` arrotonda.
        "interval_in" => {
            let fields = (typmod >> 16) & 0x7FFF;
            let precision = typmod & 0xFFFF;
            LeafClass::Session {
                leaf: SessionLeaf::Interval,
                fraction: u32::try_from(precision)
                    .ok()
                    .filter(|p| *p < 6)
                    .unwrap_or(6),
                reduced: typmod >= 0
                    && (fields != 0x7FFF || (precision != 0xFFFF && precision < 6)),
            }
        }
        "cash_in" => LeafClass::Session {
            leaf: SessionLeaf::Money,
            fraction: 0,
            reduced: false,
        },
        _ => LeafClass::Unknown,
    }
}

/// Il cammino ricorsivo di [`RULES_SQL`] e [`TYPE_RULES_SQL`] dopo il seme:
/// domini (con il loro typmod), array (con quello della colonna), range,
/// multirange e composti, fino ai tipi base ed enum; a 32 livelli il nodo
/// raggiunto conta come foglia (e la sua funzione d'ingresso non e
/// nell'elenco).
macro_rules! walk_sql {
    () => {
        "
    SELECT w.name, x.next, x.typmod, x.shape, w.depth + 1 FROM walk w CROSS JOIN LATERAL (
        SELECT y.typbasetype AS next, y.typtypmod AS typmod, w.shape AS shape
          FROM pg_catalog.pg_type y WHERE y.oid = w.oid AND y.typtype = 'd'
        UNION ALL
        SELECT y.typelem, w.typmod, CASE WHEN w.shape = 'scalar' THEN 'array' ELSE 'nested' END
          FROM pg_catalog.pg_type y
         WHERE y.oid = w.oid AND y.typtype = 'b' AND y.typcategory = 'A'
           AND y.typelem <> 0::pg_catalog.oid
        UNION ALL
        SELECT r.rngsubtype, -1, CASE WHEN w.shape = 'scalar' THEN 'range' ELSE 'nested' END
          FROM pg_catalog.pg_range r WHERE r.rngtypid = w.oid
        UNION ALL
        SELECT r.rngsubtype, -1, 'nested' FROM pg_catalog.pg_range r WHERE r.rngmultitypid = w.oid
        UNION ALL
        SELECT a.atttypid, a.atttypmod, 'nested'
          FROM pg_catalog.pg_type y JOIN pg_catalog.pg_attribute a ON a.attrelid = y.typrelid
         WHERE y.oid = w.oid AND y.typtype = 'c' AND a.attnum > 0 AND NOT a.attisdropped
    ) x
    WHERE w.depth < 32
)
SELECT w.name, w.shape,
       CASE WHEN p.prolang = 12::pg_catalog.oid THEN p.prosrc::pg_catalog.text END,
       w.typmod
  FROM walk w
  JOIN pg_catalog.pg_type y ON y.oid = w.oid
  LEFT JOIN pg_catalog.pg_proc p ON p.oid = y.typinput
 WHERE w.depth = 32
    OR (y.typtype IN ('b', 'e', 'p')
        AND NOT (y.typtype = 'b' AND y.typcategory = 'A' AND y.typelem <> 0::pg_catalog.oid))
"
    };
}

/// Le foglie del tipo di ogni colonna `$3` del target (`walk_sql!`). Gira
/// con il percorso chiuso (regola Q2).
const RULES_SQL: &str = concat!(
    "\
WITH RECURSIVE target AS (
    SELECT c.oid FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text
), walk(name, oid, typmod, shape, depth) AS (
    SELECT a.attname::pg_catalog.text, a.atttypid, a.atttypmod, 'scalar'::pg_catalog.text, 0
      FROM target t JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
     WHERE a.attnum > 0 AND NOT a.attisdropped
       AND a.attname::pg_catalog.text = ANY ($3::pg_catalog.text[])
    UNION ALL",
    walk_sql!()
);

/// Le foglie dei tipi `$2` (oid), per i nomi `$1`: il cammino di
/// `walk_sql!` da un tipo invece che da una colonna, per Create, la cui
/// colonna non esiste ancora. Senza il typmod della colonna: i tipi che lo
/// portano nella dichiarazione (`numeric(p,s)`) li conosce [`declared`]. Gira
/// con il percorso chiuso (regola Q2).
const TYPE_RULES_SQL: &str = concat!(
    "\
WITH RECURSIVE walk(name, oid, typmod, shape, depth) AS (
    SELECT x.name, x.oid, -1, 'scalar'::pg_catalog.text, 0
      FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.text[]),
                      pg_catalog.unnest($2::pg_catalog.oid[])) AS x(name, oid)
    UNION ALL",
    walk_sql!()
);

/// Il tipo di una dichiarazione di Create, risolto con il percorso della
/// sessione, come lo risolve `CREATE TABLE`.
const TYPE_OID_SQL: &str = "SELECT pg_catalog.to_regtype($1::pg_catalog.text)::pg_catalog.oid";

/// Le regole delle colonne dell'ingresso sul target, per nome. Una colonna
/// che il target non ha non ne ha: lo statement la rifiutera da se.
///
/// # Errors
///
/// L'errore classificato della lettura, in `phase`.
pub(super) async fn rules<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    schema: &SchemaRef,
    phase: ErrorPhase,
) -> Result<BTreeMap<String, SessionRule>> {
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let target_schema = crate::target_schema::schema_of(&operation.target)?;
    let rows = client
        .query(
            RULES_SQL,
            &[&target_schema, &operation.target.object, &names],
        )
        .await
        .map_err(|error| classify_error(phase, &error))?;
    Ok(collect_rules(
        &rows,
        operation.mapping_policy == MappingPolicy::Strict,
    ))
}

/// Le regole delle colonne di Create che dichiarano un tipo dell'utente
/// (enum, dominio, composto: `plenora.type_kind`), per nome, dal catalogo:
/// il tipo si risolve con il percorso della sessione, come lo risolverebbe
/// `CREATE TABLE`, poi le foglie si leggono con il percorso chiuso (regola
/// Q2), in una transazione che si annulla. I tipi di `pg_catalog` li
/// conosce [`declared`]; un tipo che non si risolve non ha regole (lo
/// rifiutera `CREATE TABLE`).
///
/// # Errors
///
/// L'errore classificato della lettura, in `phase`.
pub(super) async fn type_rules(
    client: &tokio_postgres::Client,
    declarations: &[(String, String)],
    strict: bool,
    phase: ErrorPhase,
) -> Result<BTreeMap<String, SessionRule>> {
    let read = |error: tokio_postgres::Error| classify_error(phase, &error);
    let mut names = Vec::new();
    let mut oids: Vec<u32> = Vec::new();
    for (name, declaration) in declarations {
        let oid: Option<u32> = client
            .query_one(TYPE_OID_SQL, &[declaration])
            .await
            .map_err(read)?
            .get(0);
        if let Some(oid) = oid {
            names.push(name.clone());
            oids.push(oid);
        }
    }
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    client.batch_execute("BEGIN").await.map_err(read)?;
    let walked = async {
        crate::search_path::SessionPath::close(client, phase).await?;
        client
            .query(TYPE_RULES_SQL, &[&names, &oids])
            .await
            .map_err(read)
    }
    .await;
    client.batch_execute("ROLLBACK").await.map_err(read)?;
    Ok(collect_rules(&walked?, strict))
}

fn collect_rules(rows: &[tokio_postgres::Row], strict: bool) -> BTreeMap<String, SessionRule> {
    let mut rules: BTreeMap<String, SessionRule> = BTreeMap::new();
    for row in rows {
        let name: String = row.get(0);
        let shape = match row.get::<_, String>(1).as_str() {
            "scalar" => SessionShape::Scalar,
            "array" => SessionShape::Array,
            "range" => SessionShape::Range,
            _ => SessionShape::Nested,
        };
        let symbol: Option<String> = row.get(2);
        let class = classify(symbol.as_deref(), row.get(3));
        rules
            .entry(name)
            .or_insert_with(|| SessionRule {
                leaves: Vec::new(),
                strict,
            })
            .leaves
            .push(Leaf { class, shape });
    }
    rules
}

/// Assegna a ogni piano la regola della sua colonna; le colonne spatial
/// hanno la verifica propria.
pub(super) fn assign(
    plans: &mut [WriteColumnPlan],
    schema: &SchemaRef,
    rules: &BTreeMap<String, SessionRule>,
) {
    for (plan, field) in plans.iter_mut().zip(schema.fields()) {
        plan.session_rule = if plan.is_spatial() {
            None
        } else {
            rules.get(field.name()).cloned()
        };
    }
}

/// Assegna a ogni piano la regola che dichiara ([`declared`]).
pub(super) fn assign_declared(plans: &mut [WriteColumnPlan], operation: &WriteOperation) {
    let strict = operation.mapping_policy == MappingPolicy::Strict;
    for plan in plans {
        plan.session_rule = declared(plan, strict);
    }
}

/// La regola P prima del primo valore: con `Strict`, una colonna con una
/// foglia temporale a precisione ridotta si rifiuta, qualunque valore
/// arrivi (lo stesso criterio del preflight).
///
/// # Errors
///
/// `data_mapping` con [`LOSSY_VALUE`].
pub(super) fn refuse_lossy(plans: &[WriteColumnPlan]) -> Result<()> {
    let reduced = plans
        .iter()
        .filter_map(|plan| plan.session_rule.as_ref())
        .any(|rule| {
            rule.strict
                && rule
                    .leaves
                    .iter()
                    .any(|leaf| matches!(leaf.class, LeafClass::Session { reduced: true, .. }))
        });
    if reduced {
        Err(lossy())
    } else {
        Ok(())
    }
}

/// Una perdita che una foglia del tipo di una colonna da a un valore che,
/// con `Strict`, [`check`] o [`refuse_lossy`] rifiuterebbero: con le altre
/// politiche il valore passa, e la perdita si dichiara nel `LossReport` del
/// preflight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LeafLoss {
    /// Un numero che arrotonda il testo: `numeric(p,s)`, `real`, `double
    /// precision` (`'1.005'` verso `numeric(5,2)` diventa `1.01`).
    Rounding,
    /// Una lunghezza massima (`varchar(n)`, `char(n)`): gli spazi finali
    /// oltre la lunghezza si troncano.
    Bounded,
    /// `jsonb` tiene l'ultimo valore di una chiave ripetuta.
    Jsonb,
    /// Una precisione temporale sotto il microsecondo, o un `interval` con
    /// campi.
    ReducedTemporal,
}

/// Le perdite delle foglie di `rule` per un valore Arrow di tipo
/// `data_type`: lo stesso cammino (domini, array, range, multirange,
/// composti) e la stessa classe di foglia con cui `Strict` rifiuta.
///
/// Un numero arrotonda soltanto un valore che arriva come testo (stringa,
/// letterale di array, limiti di range, campi di un composto): un valore
/// tipizzato si converte esatto, o il preflight dichiara il tipo Arrow non
/// equivalente.
pub(super) fn leaf_losses(
    rule: &SessionRule,
    data_type: &DataType,
) -> std::collections::BTreeSet<LeafLoss> {
    rule.leaves
        .iter()
        .filter_map(|leaf| match leaf.class {
            LeafClass::Rounding if text_bearing(data_type) => Some(LeafLoss::Rounding),
            LeafClass::Text { max_chars: Some(_) } => Some(LeafLoss::Bounded),
            LeafClass::Jsonb => Some(LeafLoss::Jsonb),
            LeafClass::Session { reduced: true, .. } => Some(LeafLoss::ReducedTemporal),
            _ => None,
        })
        .collect()
}

/// La regola P sul tipo dichiarato di una colonna, come lo scrive
/// `format_type` (`timestamp(0) without time zone`, `interval day`,
/// `time(3) with time zone[]`): la colonna arrotonda o tronca un valore Arrow
/// al microsecondo. Per il preflight, che la pubblica nel `LossReport`.
#[must_use]
pub fn reduced_temporal(declaration: &str) -> bool {
    let base = declaration.trim_end_matches("[]");
    let precision = |rest: &str| {
        rest.strip_prefix('(')
            .and_then(|tail| tail.split_once(')'))
            .and_then(|(digits, _)| digits.parse::<u32>().ok())
    };
    if let Some(rest) = base
        .strip_prefix("timestamp")
        .or_else(|| base.strip_prefix("time"))
    {
        return precision(rest).is_some_and(|digits| digits < 6);
    }
    if let Some(rest) = base.strip_prefix("interval") {
        // `interval(p)` arrotonda sotto le sei cifre; `interval day`,
        // `interval year to month` troncano i campi.
        return precision(rest).map_or(!rest.is_empty(), |digits| digits < 6);
    }
    false
}

/// La regola che il piano dichiara da se, per i tipi nativi di `pg_catalog`
/// che porta nei metadati (`plenora.native_type`): quella che la colonna avra
/// dopo `CREATE TABLE`, e quella che il dry run senza DSN conosce. I domini e
/// i tipi composti dichiarati non si risolvono senza il catalogo.
pub(super) fn declared(plan: &WriteColumnPlan, strict: bool) -> Option<SessionRule> {
    let declaration = plan.native_declaration_sql.as_deref()?;
    let (base, array) = declaration
        .strip_suffix("[]")
        .map_or((declaration, false), |base| (base, true));
    let (symbol, range) = match base {
        "date" => ("date_in", false),
        "time without time zone" => ("time_in", false),
        "timestamp without time zone" => ("timestamp_in", false),
        "timestamp with time zone" => ("timestamptz_in", false),
        "time with time zone" => ("timetz_in", false),
        "interval" => ("interval_in", false),
        "daterange" => ("date_in", true),
        "tsrange" => ("timestamp_in", true),
        "tstzrange" => ("timestamptz_in", true),
        "jsonb" => ("jsonb_in", false),
        "real" => ("float4in", false),
        "double precision" => ("float8in", false),
        _ if base.starts_with("numeric(") => {
            return Some(SessionRule {
                leaves: vec![Leaf {
                    class: LeafClass::Rounding,
                    shape: if array {
                        SessionShape::Array
                    } else {
                        SessionShape::Scalar
                    },
                }],
                strict,
            });
        }
        _ => return None,
    };
    let shape = match (array, range) {
        (false, false) => SessionShape::Scalar,
        (true, false) => SessionShape::Array,
        (false, true) => SessionShape::Range,
        (true, true) => SessionShape::Nested,
    };
    Some(SessionRule {
        leaves: vec![Leaf {
            class: classify(Some(symbol), -1),
            shape,
        }],
        strict,
    })
}

/// Il valore non nullo di `array` alla riga `row` contro la regola del piano.
///
/// # Errors
///
/// `data_mapping` con [`NOT_CANONICAL`] (regola V2) o con [`LOSSY_VALUE`]
/// (regola P).
pub(super) fn check(plan: &WriteColumnPlan, array: &dyn Array, row: usize) -> Result<()> {
    let Some(rule) = plan.session_rule.as_ref() else {
        return Ok(());
    };
    // Mondo chiuso: `money` e i tipi fuori elenco non ricevono niente.
    if rule.leaves.iter().any(|leaf| {
        matches!(
            leaf.class,
            LeafClass::Unknown
                | LeafClass::Session {
                    leaf: SessionLeaf::Money,
                    ..
                }
        )
    }) {
        return Err(refused());
    }
    let Some(leaf) = rule.single() else {
        // Un composto, un multirange o un array di range: soltanto testo,
        // e con `Strict` senza lunghezze massime da verificare per campo.
        let text_only = rule.leaves.iter().all(|leaf| match leaf.class {
            LeafClass::Text { max_chars } => !(rule.strict && max_chars.is_some()),
            LeafClass::Jsonb | LeafClass::Rounding => !rule.strict,
            _ => false,
        });
        return if text_only {
            Ok(())
        } else if rule.leaves.iter().all(|leaf| {
            matches!(
                leaf.class,
                LeafClass::Text { .. } | LeafClass::Jsonb | LeafClass::Rounding
            )
        }) {
            Err(lossy())
        } else {
            Err(refused())
        };
    };
    match leaf.class {
        LeafClass::Text { max_chars } => check_text(rule.strict, max_chars, array, row),
        LeafClass::Jsonb => check_jsonb(plan, rule.strict, leaf.shape, array, row),
        // Un valore Arrow tipizzato si converte da se, esatto o rifiutato
        // dal preflight; un testo con `Strict` no, nemmeno nei limiti di un
        // range (una struttura di testi).
        LeafClass::Rounding => {
            if rule.strict && text_bearing(array.data_type()) {
                Err(lossy())
            } else {
                Ok(())
            }
        }
        LeafClass::Session {
            leaf: session,
            fraction,
            ..
        } => check_session(plan, rule.strict, leaf.shape, session, fraction, array, row),
        LeafClass::Unknown => Err(refused()),
    }
}

/// Una foglia di testo: con `Strict`, la lunghezza massima.
fn check_text(strict: bool, max_chars: Option<usize>, array: &dyn Array, row: usize) -> Result<()> {
    let Some(limit) = max_chars.filter(|_| strict) else {
        return Ok(());
    };
    let fits = |text: &str| text.chars().count() <= limit;
    let accepted = match array.data_type() {
        DataType::Utf8 => fits(string(array, row)?),
        DataType::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(mapping_error)?;
            let values = list.value(row);
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .is_none_or(|items| {
                    (0..items.len()).all(|index| items.is_null(index) || fits(items.value(index)))
                })
        }
        _ => true,
    };
    if accepted {
        Ok(())
    } else {
        Err(lossy())
    }
}

/// Un valore che arriva al server come testo: una stringa, un array (che la
/// scrittura compone come letterale) o una struttura (i limiti di un range,
/// i campi di un composto).
const fn text_bearing(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::List(_) | DataType::Struct(_)
    )
}

/// Il motivo pubblico del rifiuto di un letterale che non si verifica per
/// elemento.
pub(super) const UNCHECKED_LITERAL: &str = "con MappingPolicy::Strict un letterale di array \
     fuori dalla forma che si verifica elemento per elemento, o un range scritto come testo, non \
     si verifica: va scritto come lista, come struttura o nella forma ristretta";

fn unchecked_literal() -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        false,
        &format!("scrittura rifiutata: {UNCHECKED_LITERAL}"),
    )
}

/// Una foglia `jsonb` (regola J): con `Strict`, un oggetto con una chiave
/// ripetuta, a qualunque profondita, si rifiuta.
///
/// Gli elementi si verificano uno per uno: un array come lista di testi o
/// come letterale di testo nella forma ristretta di [`array_elements`], un
/// range come struttura con i due limiti. Fino al 25/10 (quinta rilettura)
/// la verifica leggeva come JSON l'intero letterale
/// `{"{\"b\":1,\"a\":2,\"a\":3}"}` verso `jsonb[]`, in cui l'oggetto e una
/// stringa fra virgolette, e `Prepared` e `CopyText` committavano
/// `{"{\"a\": 3, \"b\": 1}"}`. Un letterale fuori da quella forma, o un
/// range scritto come testo, si rifiuta (mondo chiuso, [`UNCHECKED_LITERAL`]):
/// la verifica non indovina come il server lo leggerebbe.
///
/// `jsonb` tiene l'ultimo valore di una chiave ripetuta: fino al 25/10
/// (quarta rilettura, S4) `{"b":1,"a":2,"a":3}` si committava come
/// `{"a": 3, "b": 1}` in ogni profilo, con `Strict` e il `LossReport` vuoto.
/// Le chiavi si confrontano dopo gli escape, come le confronta il server
/// (`{"a\u0062": 1, "ab": 2}` ha la chiave `ab` due volte). L'ordine delle
/// chiavi e gli spazi sono invece la semantica del tipo (come gli spazi di
/// `char(n)`): non si rifiutano, e con le altre politiche il preflight li
/// dichiara con la chiave ripetuta (`LossSeverity::Warning`). Un testo che la
/// verifica non sa leggere (un escape non valido, che il server rifiuterebbe
/// comunque) si rifiuta con `Strict`: la prova che nessun valore si perde non
/// c'e.
fn check_jsonb(
    plan: &WriteColumnPlan,
    strict: bool,
    shape: SessionShape,
    array: &dyn Array,
    row: usize,
) -> Result<()> {
    if !strict {
        return Ok(());
    }
    let verify = |text: &str| -> Result<()> {
        if json_duplicate_keys(text) == Some(false) {
            Ok(())
        } else {
            Err(duplicate_keys())
        }
    };
    match (shape, array.data_type()) {
        (SessionShape::Scalar, DataType::Utf8) => verify(string(array, row)?),
        (SessionShape::Range, DataType::Struct(_)) if plan.semantics == ColumnSemantics::Range => {
            let range = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = range_value(range, row)?;
            for bound in [value.lower.as_deref(), value.upper.as_deref()]
                .into_iter()
                .flatten()
            {
                verify(bound)?;
            }
            Ok(())
        }
        (SessionShape::Array, DataType::List(_)) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(mapping_error)?;
            let values = list.value(row);
            if let Some(items) = values.as_any().downcast_ref::<StringArray>() {
                for index in 0..items.len() {
                    if !items.is_null(index) {
                        verify(items.value(index))?;
                    }
                }
            }
            Ok(())
        }
        // Un letterale di array: gli elementi uno per uno, se il letterale
        // sta nella forma che `array_elements` legge come il server.
        (SessionShape::Array, DataType::Utf8) => {
            let elements = array_elements(string(array, row)?).ok_or_else(unchecked_literal)?;
            for element in elements.iter().flatten() {
                verify(element)?;
            }
            Ok(())
        }
        // Un testo che non e il valore di una foglia (un letterale di range,
        // una lista o una struttura che non corrisponde alla forma della
        // colonna) non si verifica per elemento.
        (_, data_type) if text_bearing(data_type) => Err(unchecked_literal()),
        // Un valore tipizzato (numero, booleano) non ha chiavi.
        _ => Ok(()),
    }
}

/// Gli elementi di un letterale di array PostgreSQL (`NULL` come `None`), a
/// qualunque profondita, in una forma ristretta che si legge in un solo modo:
///
/// - parentesi graffe bilanciate, elementi separati da `,` (il delimitatore
///   di ogni tipo tranne `box`), spazi ammessi intorno agli elementi e alle
///   parentesi;
/// - un elemento fra virgolette, con `\` che rende letterale il carattere
///   dopo; oppure senza virgolette, senza spazi, virgolette, parentesi,
///   virgole o `\` al suo interno (`NULL`, in qualunque combinazione di
///   maiuscole, e il valore nullo).
///
/// Ogni altra forma — le dimensioni dichiarate (`[1:2]={...}`), le virgolette
/// a meta elemento, gli escape fuori dalle virgolette, un elemento vuoto, un
/// testo dopo la parentesi di chiusura — rende `None`: il chiamante rifiuta
/// invece di indovinare come la leggerebbe il server.
#[must_use]
pub(super) fn array_elements(text: &str) -> Option<Vec<Option<String>>> {
    let space = |byte: u8| matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C);
    let bytes = text.as_bytes();
    let skip = |index: &mut usize| {
        while bytes.get(*index).copied().is_some_and(space) {
            *index += 1;
        }
    };
    let mut index = 0;
    skip(&mut index);
    if bytes.get(index) != Some(&b'{') {
        return None;
    }
    let mut elements = Vec::new();
    // La profondita delle parentesi aperte, e se dopo l'ultimo `{` o `,` si
    // aspetta un elemento.
    let mut depth = 0_usize;
    let mut expect = true;
    loop {
        skip(&mut index);
        let byte = *bytes.get(index)?;
        match byte {
            b'{' if expect => {
                depth += 1;
                index += 1;
                // `{}` e un array vuoto, anche annidato.
                skip(&mut index);
                if bytes.get(index) == Some(&b'}') {
                    depth -= 1;
                    index += 1;
                    expect = false;
                    if depth == 0 {
                        break;
                    }
                }
            }
            b'}' if !expect => {
                depth = depth.checked_sub(1)?;
                index += 1;
                if depth == 0 {
                    break;
                }
            }
            b',' if !expect && depth > 0 => {
                index += 1;
                expect = true;
            }
            b'"' if expect => {
                let mut element = Vec::new();
                index += 1;
                loop {
                    match *bytes.get(index)? {
                        b'"' => break,
                        b'\\' => {
                            element.push(*bytes.get(index + 1)?);
                            index += 2;
                        }
                        other => {
                            element.push(other);
                            index += 1;
                        }
                    }
                }
                index += 1;
                elements.push(Some(String::from_utf8(element).ok()?));
                expect = false;
            }
            _ if expect => {
                let start = index;
                while bytes.get(index).is_some_and(|byte| {
                    !space(*byte) && !matches!(byte, b'{' | b'}' | b'"' | b',' | b'\\')
                }) {
                    index += 1;
                }
                if index == start {
                    return None;
                }
                let element = &text[start..index];
                elements.push((!element.eq_ignore_ascii_case("null")).then(|| element.to_owned()));
                expect = false;
            }
            _ => return None,
        }
    }
    skip(&mut index);
    (index == bytes.len()).then_some(elements)
}

fn duplicate_keys() -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        false,
        &format!("scrittura rifiutata: {DUPLICATE_KEYS}"),
    )
}

/// Se un testo JSON ha un oggetto che ripete una chiave, a qualunque
/// profondita: `Some(true)` o `Some(false)`, `None` se la lettura non riesce
/// (un escape non valido). La validita del resto la decide il server; i
/// numeri non si leggono (`jsonb` li tiene come `numeric`, qualunque
/// grandezza). Iterativo: nessun limite di profondita oltre la memoria del
/// testo.
pub(super) fn json_duplicate_keys(text: &str) -> Option<bool> {
    enum Frame {
        Object {
            keys: std::collections::HashSet<String>,
            expect_key: bool,
        },
        Array,
    }
    let bytes = text.as_bytes();
    let mut stack: Vec<Frame> = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'{' => {
                stack.push(Frame::Object {
                    keys: std::collections::HashSet::new(),
                    expect_key: true,
                });
                index += 1;
            }
            b'[' => {
                stack.push(Frame::Array);
                index += 1;
            }
            b'}' | b']' => {
                stack.pop();
                index += 1;
            }
            b',' => {
                if let Some(Frame::Object { expect_key, .. }) = stack.last_mut() {
                    *expect_key = true;
                }
                index += 1;
            }
            b'"' => {
                let (value, end) = json_string(bytes, index + 1)?;
                index = end;
                if let Some(Frame::Object { keys, expect_key }) = stack.last_mut() {
                    if *expect_key {
                        if !keys.insert(value) {
                            return Some(true);
                        }
                        *expect_key = false;
                    }
                }
            }
            _ => index += 1,
        }
    }
    Some(false)
}

/// La stringa JSON che comincia a `from` (dopo le virgolette), con gli
/// escape risolti, e la posizione dopo le virgolette di chiusura.
fn json_string(bytes: &[u8], from: usize) -> Option<(String, usize)> {
    let mut out: Vec<u8> = Vec::new();
    let mut index = from;
    loop {
        match *bytes.get(index)? {
            b'"' => return Some((String::from_utf8(out).ok()?, index + 1)),
            b'\\' => {
                let escape = *bytes.get(index + 1)?;
                index += 2;
                let decoded = match escape {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'/' => '/',
                    b'b' => '\u{8}',
                    b'f' => '\u{c}',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'u' => {
                        let high = json_hex4(bytes, index)?;
                        index += 4;
                        if (0xD800..0xDC00).contains(&high) {
                            if bytes.get(index) != Some(&b'\\')
                                || bytes.get(index + 1) != Some(&b'u')
                            {
                                return None;
                            }
                            let low = json_hex4(bytes, index + 2)?;
                            if !(0xDC00..0xE000).contains(&low) {
                                return None;
                            }
                            index += 6;
                            char::from_u32(0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00))?
                        } else {
                            char::from_u32(high)?
                        }
                    }
                    _ => return None,
                };
                let mut buffer = [0_u8; 4];
                out.extend_from_slice(decoded.encode_utf8(&mut buffer).as_bytes());
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
}

fn json_hex4(bytes: &[u8], from: usize) -> Option<u32> {
    let digits = std::str::from_utf8(bytes.get(from..from.checked_add(4)?)?).ok()?;
    u32::from_str_radix(digits, 16).ok()
}

/// Una foglia temporale: testo canonico, valori tipizzati che non passano
/// dal `TimeZone`; con `Strict`, non piu cifre frazionarie di `fraction`.
fn check_session(
    plan: &WriteColumnPlan,
    strict: bool,
    shape: SessionShape,
    leaf: SessionLeaf,
    fraction: u32,
    array: &dyn Array,
    row: usize,
) -> Result<()> {
    let text = |value: &str| -> Result<()> {
        if !canonical(leaf, value) {
            return Err(refused());
        }
        if strict && fraction_digits(value) > fraction {
            return Err(lossy());
        }
        Ok(())
    };
    let zoned = matches!(leaf, SessionLeaf::TimestampTz | SessionLeaf::TimeTz);
    match (shape, array.data_type()) {
        (SessionShape::Scalar, DataType::Utf8) => text(string(array, row)?),
        (SessionShape::Scalar, DataType::Date32) => {
            if leaf == SessionLeaf::TimestampTz {
                Err(refused())
            } else {
                Ok(())
            }
        }
        (SessionShape::Scalar, DataType::Timestamp(_, None) | DataType::Time64(_)) => {
            if zoned {
                Err(refused())
            } else {
                Ok(())
            }
        }
        // Un array o un range scritto come un unico letterale di testo cade
        // nell'ultimo ramo: rifiutato.
        (SessionShape::Array, DataType::List(_)) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(mapping_error)?;
            let values = list.value(row);
            if let Some(items) = values.as_any().downcast_ref::<StringArray>() {
                for index in 0..items.len() {
                    if !items.is_null(index) {
                        text(items.value(index))?;
                    }
                }
            }
            Ok(())
        }
        (SessionShape::Range, DataType::Struct(_)) if plan.semantics == ColumnSemantics::Range => {
            let range = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = range_value(range, row)?;
            for bound in [value.lower.as_deref(), value.upper.as_deref()]
                .into_iter()
                .flatten()
            {
                text(bound)?;
            }
            Ok(())
        }
        // I valori Arrow tipizzati che la scrittura formatta da se.
        (
            _,
            DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Binary
            | DataType::Interval(_)
            | DataType::Timestamp(_, Some(_)),
        ) => Ok(()),
        // Una lista o una struttura che non corrisponde alla forma della
        // colonna (un composto verso una `date`, una lista verso un range):
        // il suo testo arriverebbe al server senza verifica.
        _ => Err(refused()),
    }
}

fn string(array: &dyn Array, row: usize) -> Result<&str> {
    Ok(array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(mapping_error)?
        .value(row))
}

/// Le cifre frazionarie piu lunghe di `text`: dopo ogni `.` seguito da
/// cifre (i secondi di un'ora o di un intervallo, la frazione di un campo).
#[must_use]
pub(super) fn fraction_digits(text: &str) -> u32 {
    let bytes = text.as_bytes();
    let mut longest = 0_u32;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'.' {
            let digits = bytes[index + 1..]
                .iter()
                .take_while(|byte| byte.is_ascii_digit())
                .count();
            longest = longest.max(u32::try_from(digits).unwrap_or(u32::MAX));
        }
    }
    longest
}

/// La forma canonica di `text` per la foglia: mai un valore speciale
/// (`now`, `today`, `epoch`, `allballs`, `infinity`), che la grammatica qui
/// sotto non ammette.
pub(super) fn canonical(leaf: SessionLeaf, text: &str) -> bool {
    match leaf {
        SessionLeaf::Date => date_end(text.as_bytes()).is_some_and(|end| bc_end(text, end)),
        SessionLeaf::Time => time_end(text.as_bytes(), 0).is_some_and(|end| end == text.len()),
        SessionLeaf::Timestamp => timestamp(text, false),
        SessionLeaf::TimestampTz => timestamp(text, true),
        SessionLeaf::TimeTz => time_end(text.as_bytes(), 0)
            .and_then(|end| zone_end(text.as_bytes(), end))
            .is_some_and(|end| end == text.len()),
        SessionLeaf::Interval => interval(text),
        SessionLeaf::Money => false,
    }
}

fn digits(bytes: &[u8], from: usize, count: usize) -> Option<usize> {
    let end = from.checked_add(count)?;
    bytes
        .get(from..end)?
        .iter()
        .all(u8::is_ascii_digit)
        .then_some(end)
}

/// `AAAA-MM-GG` (anni di quattro o piu cifre), con la fine.
fn date_end(bytes: &[u8]) -> Option<usize> {
    let year = bytes
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if year < 4 || bytes.get(year) != Some(&b'-') {
        return None;
    }
    let month = digits(bytes, year + 1, 2)?;
    if bytes.get(month) != Some(&b'-') {
        return None;
    }
    digits(bytes, month + 1, 2)
}

/// `hh:mm[:ss[.f...]]` da `from`, con la fine.
fn time_end(bytes: &[u8], from: usize) -> Option<usize> {
    let hours = digits(bytes, from, 2)?;
    if bytes.get(hours) != Some(&b':') {
        return None;
    }
    let minutes = digits(bytes, hours + 1, 2)?;
    if bytes.get(minutes) != Some(&b':') {
        return Some(minutes);
    }
    let seconds = digits(bytes, minutes + 1, 2)?;
    if bytes.get(seconds) != Some(&b'.') {
        return Some(seconds);
    }
    let fraction = bytes[seconds + 1..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (fraction > 0).then_some(seconds + 1 + fraction)
}

/// `[ ]Z` o `[ ]+hh[[:]mm[[:]ss]]` da `from`, con la fine.
fn zone_end(bytes: &[u8], from: usize) -> Option<usize> {
    let from = if bytes.get(from) == Some(&b' ') {
        from + 1
    } else {
        from
    };
    match bytes.get(from) {
        Some(b'Z') => Some(from + 1),
        Some(b'+' | b'-') => {
            let mut end = digits(bytes, from + 1, 2)?;
            for _ in 0..2 {
                let next = if bytes.get(end) == Some(&b':') {
                    end + 1
                } else {
                    end
                };
                match digits(bytes, next, 2) {
                    Some(after) => end = after,
                    None if next == end => break,
                    None => return None,
                }
            }
            Some(end)
        }
        _ => None,
    }
}

/// La fine del testo, o ` BC` e poi la fine.
fn bc_end(text: &str, end: usize) -> bool {
    end == text.len() || &text[end..] == " BC"
}

fn timestamp(text: &str, zoned: bool) -> bool {
    let bytes = text.as_bytes();
    let Some(date) = date_end(bytes) else {
        return false;
    };
    if !matches!(bytes.get(date), Some(b'T' | b' ')) {
        return false;
    }
    let Some(time) = time_end(bytes, date + 1) else {
        return false;
    };
    if zoned {
        zone_end(bytes, time).is_some_and(|end| bc_end(text, end))
    } else {
        bc_end(text, time)
    }
}

/// Un intervallo la cui lettura non dipende da `IntervalStyle`: la forma ISO
/// 8601 con le designazioni (`P1Y2M3DT4H5M6S`, segni ammessi sui numeri),
/// oppure una forma in cui il primo campo numerico non e negativo o ogni
/// campo numerico dopo il primo porta il proprio segno (`sql_standard`
/// applica il segno del primo campo ai campi che non ne hanno uno).
fn interval(text: &str) -> bool {
    if let Some(rest) = text.strip_prefix('P') {
        return iso_interval(rest);
    }
    // Il server divide i campi anche sulla punteggiatura e su ogni spazio, e
    // attacca un segno isolato alla parola dopo (`- 1 day`): un testo che
    // divide diversamente da qui si rifiuta invece di indovinarlo.
    let bytes = text.as_bytes();
    let separated = bytes.iter().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'.' | b':' | b'@')
            || *byte == b' '
            || (matches!(byte, b'+' | b'-')
                && bytes
                    .get(index + 1)
                    .is_some_and(|next| next.is_ascii_digit() || *next == b'.'))
    });
    if !separated {
        return false;
    }
    let mut numeric = text.split_ascii_whitespace().filter(|word| {
        word.trim_start_matches(['+', '-'])
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_digit() || *byte == b'.')
    });
    // Senza un campo numerico non e un intervallo canonico: `infinity`
    // (PostgreSQL 17) e i valori speciali si rifiutano.
    let Some(first) = numeric.next() else {
        return false;
    };
    !first.starts_with('-') || numeric.all(|word| word.starts_with(['+', '-']))
}

/// I campi ISO 8601 di `part` nell'ordine di `designators`, ciascuno con un
/// numero (segno ammesso) e la sua designazione.
fn iso_fields(part: &str, designators: &[u8]) -> Option<usize> {
    let bytes = part.as_bytes();
    let mut index = 0;
    let mut allowed = designators;
    let mut count = 0;
    while index < bytes.len() {
        if matches!(bytes[index], b'+' | b'-') {
            index += 1;
        }
        let number = bytes[index..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit() || **byte == b'.')
            .count();
        if number == 0 {
            return None;
        }
        index += number;
        let designator = *bytes.get(index)?;
        let position = allowed.iter().position(|item| *item == designator)?;
        allowed = &allowed[position + 1..];
        index += 1;
        count += 1;
    }
    Some(count)
}

fn iso_interval(rest: &str) -> bool {
    match rest.split_once('T') {
        None => iso_fields(rest, b"YMWD").is_some_and(|count| count > 0),
        Some((date, time)) => {
            iso_fields(date, b"YMWD").is_some()
                && iso_fields(time, b"HMS").is_some_and(|count| count > 0)
        }
    }
}

#[cfg(test)]
#[path = "canonical_tests.rs"]
mod tests;
