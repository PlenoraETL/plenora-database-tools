//! Le famiglie di tipo degli operandi e la regola senza conversioni
//! implicite (regola F), con la regola B per colonna.
//!
//! # Regola B: le colonne binarie calcolate dal server
//!
//! Una colonna e **derivata** ([`ColumnOrigin::Derived`]) se la sua relazione
//! e derivata (vista, anche tipizzata o di oggetti; vista materializzata;
//! MQT) **oppure** se il valore lo calcola il server: colonna generata,
//! calcolata o virtuale, con qualunque memorizzazione (`attgenerated` di
//! `PostgreSQL`; `GENERATED` in `Extra` di `MySQL` e `MariaDB`;
//! `is_computed` di SQL Server; `GENERATED` di `SYSCAT.COLUMNS` di Db2;
//! `VIRTUAL_COLUMN` di `ALL_TAB_COLS` di Oracle). Una colonna derivata della
//! famiglia binaria si giudica geometrica ([`catalog_column_is_geometric`]):
//! `w bytea GENERATED ALWAYS AS (ST_AsBinary(g)) STORED` porta il WKB della
//! geometria, e due poligoni uguali con i vertici in ordine diverso hanno WKB
//! diversi (misurato il 25/10 su ogni provider: `w = :wkb` rendeva una riga
//! su due). Fino al 25/10 l'origine era della relazione, e la colonna
//! generata di una tabella restava scalare.
//!
//! # Regola F: nessuna conversione implicita fra famiglie
//!
//! In un confronto (`=`, `<>`, `<`, `<=`, `>`, `>=`), in `IN` (lista e
//! subquery), `BETWEEN`, `LIKE`, fra i rami di `CASE`, gli argomenti di
//! `COALESCE`, le posizioni dei rami di una set operation che confronta
//! righe, gli argomenti di un'aritmetica (soltanto numerica), di `SUM`,
//! `AVG` (numerica), `LOWER`, `UPPER` (testo), e
//! in ogni operando di `ORDER BY`, `GROUP BY`, `DISTINCT`, `PARTITION BY`, le
//! famiglie ([`OperandFamily`]) dei valori devono coincidere. La famiglia di
//! una colonna e quella del catalogo, di un parametro quella del suo valore
//! ([`parameter_family`]) o del suo tipo dichiarato, di un `CAST` quella del
//! tipo di destinazione. Due famiglie diverse si rifiutano prima
//! dell'esecuzione, `invalid_plan` in fase `prepare`
//! ([`OPERAND_FAMILY_MISMATCH`]): il server non converte mai da se.
//!
//! La tabella delle famiglie, uguale per ogni provider:
//!
//! | famiglia | tipi |
//! | --- | --- |
//! | geometrica | i tipi spatial del provider |
//! | binaria | `bytea`; `BINARY`, `VARBINARY`, `*BLOB`; `binary`, `varbinary`, `image`, `timestamp`/`rowversion`, `sql_variant`; `BLOB`, `RAW`, `LONG RAW`, `BFILE`; `BLOB`, `BINARY`, `VARBINARY`, `FOR BIT DATA`; un bind `Bytes` |
//! | testo | i tipi carattere e i loro LOB; `ENUM` e `SET` di `MySQL`; un bind `String` o `Enum` |
//! | numerica | interi, decimali, virgola mobile, `money`, `bit`, `boolean` (un booleano e un intero su `MySQL`, SQL Server, Oracle prima della 23); un bind `Bool`, `I32`, `I64`, `F64`, `Decimal` |
//! | temporale | data, ora, timestamp con e senza fuso; un bind `Date`, `Timestamp`, `TimestampTz` |
//! | altra | tutto il resto (uuid, json, xml, intervalli, enumerazioni e tipi composti di `PostgreSQL`, tipi dell'utente): non si giudica |
//!
//! Dentro una famiglia il server converte senza perdere il confronto (un
//! intero con un decimale, una data con un timestamp): resta ammesso.
//!
//! Un `CAST` esplicito alla famiglia binaria di un valore testo, di un valore
//! derivato o di un valore di cui il giudizio non sa la famiglia rende un
//! **binario derivato**: sotto un operatore non spatial si giudica geometrico
//! (regola B), `unsupported` con
//! [`GEOMETRY_SCALAR_OPERAND`](crate::spatial_probe::GEOMETRY_SCALAR_OPERAND).
//! Il `CAST` binario di un parametro resta ammesso: i byte li sceglie il
//! chiamante.
//!
//! # Il confine del mondo chiuso
//!
//! - **Geometria, giudicata.** I tipi spatial del catalogo; le colonne
//!   binarie derivate (regola B); i binari derivati da un `CAST` del piano; le
//!   funzioni spatial che rendono una geometria o una sua codifica binaria
//!   (`SpatialOutput`, `AsMvt`, `AsGeobuf`); un `SpatialValue`; un bind `Wkb`
//!   (famiglia geometrica, quindi diverso da ogni altra).
//! - **Byte, confrontati come byte per scelta.** Le colonne binarie
//!   memorizzate di una tabella, che riempie il chiamante: la libreria non
//!   scrive geometrie in colonne binarie, e i byte confrontati sono quelli che
//!   il chiamante ha scelto.
//! - **Testo, confrontato come testo.** Le forme testuali di una geometria
//!   (WKT, `GeoJSON`, l'esadecimale di un WKB) in una colonna testo, anche di
//!   una vista o generata, confrontate con un parametro testo: il catalogo
//!   non distingue un WKT da un altro testo, e un testo confrontato con un
//!   testo non e una conversione.
//!
//! # Limiti dichiarati
//!
//! - **Regola.** Il confine del mondo chiuso: le colonne binarie memorizzate
//!   di una tabella e le forme testuali di una geometria si confrontano per
//!   byte e per testo.
//! - **Ambito.** Ogni provider, ogni percorso che giudica sul catalogo.
//! - **Hazard.** Una tabella riempita dal chiamante con il WKB di una
//!   geometria (`INSERT ... SELECT ST_AsBinary(g)`); un WKT confrontato con un
//!   WKT: due geometrie uguali con rappresentazioni diverse non sono uguali,
//!   senza errore.
//! - **Rientro.** Nessuno: i byte e il testo li sceglie il chiamante.
//!
//! - **Regola.** La regola F giudica soltanto le famiglie che conosce.
//! - **Ambito.** Un operando della famiglia «altra»; un parametro senza
//!   valore noto (la compilazione senza esecuzione, un `Null`); un
//!   riferimento che non si risolve in un piano senza geometrie (una
//!   relazione di un altro database, un sinonimo verso un database link, un
//!   `*` non espanso), che il mondo chiuso lascia al server.
//! - **Hazard.** Un uuid confrontato con un testo, un json con un testo, un
//!   intervallo con un numero: la conversione la decide il server.
//! - **Rientro.** Quando la tabella delle famiglie distingue quei tipi su
//!   ogni provider.
//!
//! - **Regola.** `sql_variant` di SQL Server e della famiglia binaria.
//! - **Ambito.** SQL Server.
//! - **Hazard.** Nessun passaggio silenzioso: un `sql_variant` puo portare un
//!   `varbinary` (e una geometria resa binaria), e si giudica come un binario.
//!   Il costo e il rifiuto di un `sql_variant` confrontato con un numero o un
//!   testo.
//! - **Rientro.** Quando il giudizio legge il tipo base di ogni valore.
//!
//! - **Regola.** L'aritmetica e soltanto numerica.
//! - **Ambito.** Ogni provider: `data + numero`, `data - data` si rifiutano.
//! - **Hazard.** Nessun passaggio silenzioso: il risultato dipende dal
//!   dialetto (un numero su Oracle, un intervallo su `PostgreSQL`, la data
//!   resa numero su `MySQL`). Il costo e il rifiuto dell'aritmetica delle
//!   date.
//! - **Rientro.** Quando la tabella dice, per dialetto e operatore, la
//!   famiglia del risultato.
//!
//! - **Regola.** Le conversioni dentro una famiglia restano ammesse.
//! - **Ambito.** Ogni provider: un intero con un decimale o un `float`, una
//!   data con un timestamp, un timestamp con fuso con uno senza, `CHAR` con
//!   `VARCHAR`, un booleano con un intero.
//! - **Hazard.** Un `float` confrontato con un decimale esatto arrotonda; un
//!   timestamp senza fuso confrontato con uno con fuso dipende dal fuso della
//!   sessione; `CHAR` completa con spazi.
//! - **Rientro.** Quando la tabella separa le sottofamiglie che il contratto
//!   distingue.
//!
//! - **Regola.** La regola F giudica i confronti, gli ordinamenti, i
//!   raggruppamenti e i calcoli, non le scritture.
//! - **Ambito.** I valori di `INSERT`, le assegnazioni di `UPDATE ... SET`,
//!   le righe di un upsert; i valori della projection che non si confrontano.
//! - **Hazard.** Un testo scritto in una colonna binaria (o il contrario) si
//!   converte sul server: il valore scritto e quello che il server ricava,
//!   non quello legato. Nessun confronto del piano che scrive ne dipende.
//! - **Rientro.** Quando la scrittura giudica la famiglia di ogni valore
//!   contro la colonna di destinazione.
//!
//! - **Regola.** Una colonna che il server calcola senza che il catalogo
//!   della colonna lo dica non e derivata.
//! - **Ambito.** I trigger che scrivono una colonna; un default di
//!   espressione che legge altre colonne dove il catalogo non lo distingue da
//!   un letterale.
//! - **Hazard.** Una colonna binaria scritta da un trigger con il WKB di una
//!   geometria si confronta per byte.
//! - **Rientro.** Quando il giudizio legge le dipendenze dei trigger e dei
//!   default sulla colonna.
//!
//! - **Regola.** Un nome che il giudizio non sa legare in modo univoco porta
//!   tutte le famiglie dei suoi candidati (la colonna e il nome di uscita
//!   omonimo, piu colonne omonime): due famiglie fra i candidati si
//!   rifiutano.
//! - **Ambito.** Ogni provider.
//! - **Hazard.** Nessun passaggio silenzioso; il costo e il rifiuto di un
//!   piano che ombreggia una colonna con un alias di un'altra famiglia.
//! - **Rientro.** Quando il giudizio emula il legame dei nomi di uscita per
//!   clausola.

use super::{
    address, Candidate, CommonTableExpression, EntrySource, Item, QueryExpression, QueryOperation,
    ScalarFunction, Walk,
};
use crate::provider::{ParameterBag, ParameterValue};
use crate::relational::{ColumnRef, QueryParameterType, SpatialFunction};
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;

/// Motivo stabile del rifiuto di una conversione implicita fra famiglie di
/// tipo (regola F).
pub const OPERAND_FAMILY_MISMATCH: &str =
    "conversione implicita fra famiglie di tipo: i lati di un confronto, di IN, BETWEEN e LIKE (soltanto testo), i rami di CASE, COALESCE e di una set operation, gli argomenti di un'aritmetica, di SUM e AVG (numerica), di LOWER e UPPER (testo), e ogni operando di ORDER BY, GROUP BY, DISTINCT, PARTITION BY devono avere una sola famiglia (geometrica, binaria, testo, numerica, temporale) secondo il catalogo e il tipo dei parametri; si scriva un CAST esplicito";

/// Motivo stabile del rifiuto di un'espressione troppo annidata per il
/// giudizio delle famiglie.
pub const OPERAND_TYPING_TOO_DEEP: &str =
    "espressione troppo annidata per il giudizio delle famiglie di tipo";

/// Oltre questa profondita un'espressione non si giudica, e si rifiuta.
const MAX_TYPING_DEPTH: usize = 256;

/// La famiglia di tipo di un operando (la tabella del modulo).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OperandFamily {
    /// Un tipo spatial del provider, un bind `Wkb`.
    Geometry,
    /// Un tipo binario o LOB binario, un bind `Bytes`.
    Binary,
    /// Un tipo carattere, un bind `String` o `Enum`.
    Text,
    /// Un tipo numerico o booleano.
    Numeric,
    /// Data, ora, timestamp.
    Temporal,
    /// Ogni altro tipo: non si giudica.
    Other,
}

impl OperandFamily {
    const fn bit(self) -> u8 {
        match self {
            Self::Geometry => 1,
            Self::Binary => 2,
            Self::Text => 4,
            Self::Numeric => 8,
            Self::Temporal => 16,
            Self::Other => 32,
        }
    }
}

/// I bit delle famiglie che la regola F giudica: tutte tranne «altra».
const JUDGED: u8 = 31;

/// Da dove viene il valore di una colonna del catalogo (regola B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnOrigin {
    /// Il valore lo scrive chi riempie la tabella.
    Stored,
    /// Il valore lo calcola il server: una colonna di una relazione derivata,
    /// o una colonna generata, calcolata o virtuale.
    Derived,
}

impl ColumnOrigin {
    /// Derivata se la relazione e derivata o se il server calcola la
    /// colonna.
    #[must_use]
    pub const fn of(derived_relation: bool, computed_column: bool) -> Self {
        if derived_relation || computed_column {
            Self::Derived
        } else {
            Self::Stored
        }
    }
}

/// La regola delle colonne derivate binarie, la stessa per ogni provider.
///
/// Una colonna e geometrica, sotto un operatore non spatial, se il suo tipo
/// e geometrico, o se e binaria e derivata ([`ColumnOrigin::Derived`]).
///
/// Una vista o una colonna generata porta una geometria sotto un tipo
/// binario quando e resa da `ST_AsBinary(g)`, `g::bytea`, `ST_AsWKB(g)`,
/// `CAST(g AS BINARY)`, `g.STAsBinary()`, `CAST(g AS varbinary(max))`,
/// `SDO_UTIL.TO_WKBGEOMETRY(g)`, `g.GET_WKB()`, `ST_ASBINARY(G)`,
/// `CAST(G AS VARBINARY)`, o da una funzione spatial che il catalogo descrive
/// con il tipo binario di cui la geometria e un tipo distinto (Db2,
/// `ST_CENTROID(G)` e `BLOB`). Il catalogo non dice la funzione che rende la
/// colonna: a mondo chiuso, la colonna binaria derivata non si prova non
/// geometrica. Il testo non sta in questa regola: vedi il confine del mondo
/// chiuso nel modulo.
#[must_use]
pub const fn catalog_column_is_geometric(origin: ColumnOrigin, family: OperandFamily) -> bool {
    match family {
        OperandFamily::Geometry => true,
        OperandFamily::Binary => matches!(origin, ColumnOrigin::Derived),
        _ => false,
    }
}

/// Il tipo di una colonna del catalogo: origine e famiglia.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnType {
    pub origin: ColumnOrigin,
    pub family: OperandFamily,
}

impl ColumnType {
    #[must_use]
    pub const fn new(origin: ColumnOrigin, family: OperandFamily) -> Self {
        Self { origin, family }
    }

    /// La regola B sulla colonna.
    #[must_use]
    pub const fn is_geometric(self) -> bool {
        catalog_column_is_geometric(self.origin, self.family)
    }
}

/// Una colonna del catalogo: il nome e il tipo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogColumn {
    pub name: String,
    pub column_type: ColumnType,
}

impl CatalogColumn {
    #[must_use]
    pub fn new(name: impl Into<String>, origin: ColumnOrigin, family: OperandFamily) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::new(origin, family),
        }
    }

    /// Una colonna di cui si sa soltanto se e geometrica (un mapping, un
    /// catalogo di prova): memorizzata, geometrica o della famiglia «altra»,
    /// che la regola F non giudica.
    #[must_use]
    pub fn from_flag(name: impl Into<String>, geometric: bool) -> Self {
        let family = if geometric {
            OperandFamily::Geometry
        } else {
            OperandFamily::Other
        };
        Self::new(name, ColumnOrigin::Stored, family)
    }

    /// La regola B sulla colonna.
    #[must_use]
    pub const fn is_geometric(&self) -> bool {
        self.column_type.is_geometric()
    }
}

/// La famiglia del valore di un parametro; `None` per un `Null`, che non ne
/// ha.
#[must_use]
pub const fn parameter_family(value: &ParameterValue) -> Option<OperandFamily> {
    Some(match value {
        ParameterValue::Bool(_)
        | ParameterValue::I32(_)
        | ParameterValue::I64(_)
        | ParameterValue::F64(_)
        | ParameterValue::Decimal(_) => OperandFamily::Numeric,
        ParameterValue::String(_) | ParameterValue::Enum { .. } => OperandFamily::Text,
        ParameterValue::Bytes(_) => OperandFamily::Binary,
        ParameterValue::Date(_) | ParameterValue::Timestamp(_) | ParameterValue::TimestampTz(_) => {
            OperandFamily::Temporal
        }
        ParameterValue::Uuid(_) | ParameterValue::Json(_) => OperandFamily::Other,
        ParameterValue::Wkb { .. } => OperandFamily::Geometry,
        ParameterValue::Null { .. } => return None,
    })
}

/// La famiglia di un tipo dichiarato dal piano (`TypedParameter`, `CAST`).
#[must_use]
pub const fn query_parameter_family(parameter_type: QueryParameterType) -> OperandFamily {
    match parameter_type {
        QueryParameterType::Boolean
        | QueryParameterType::Integer
        | QueryParameterType::BigInteger
        | QueryParameterType::Float
        | QueryParameterType::Decimal => OperandFamily::Numeric,
        QueryParameterType::String => OperandFamily::Text,
        QueryParameterType::Binary => OperandFamily::Binary,
        QueryParameterType::Date
        | QueryParameterType::Timestamp
        | QueryParameterType::TimestampTz => OperandFamily::Temporal,
        QueryParameterType::Uuid | QueryParameterType::Json => OperandFamily::Other,
    }
}

/// La famiglia del risultato di una funzione spatial che non rende una
/// geometria; `None` per quelle che la rendono.
const fn spatial_result(
    function: SpatialFunction,
    arguments: usize,
) -> Option<(OperandFamily, bool)> {
    use SpatialFunction as F;
    if function.returns_geometry() {
        return None;
    }
    if function.returns_boolean(arguments) {
        return Some((OperandFamily::Numeric, false));
    }
    Some(match function {
        // `ST_Relate(a, b)` rende la matrice DE-9IM: un testo derivato dalle
        // geometrie (seconda lettura del 25/10: era numerica).
        F::Relate => (OperandFamily::Text, true),
        F::GeometryType | F::AsGeoJson => (OperandFamily::Text, false),
        // La codifica binaria di una geometria: un binario derivato.
        F::AsMvt | F::AsGeobuf => (OperandFamily::Binary, true),
        // Il riquadro di `Extent` ha un tipo del provider.
        F::Extent => (OperandFamily::Other, false),
        _ => (OperandFamily::Numeric, false),
    })
}

/// Le famiglie dei parametri per nome, come le dice il valore legato.
///
/// I bit di ogni famiglia vista per quel nome (piu d'una se lo stesso nome e legato
/// a valori di famiglie diverse, e allora ogni suo uso si rifiuta).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParameterFamilies(BTreeMap<String, u8>);

impl ParameterFamilies {
    /// Da un `ParameterBag` (i path query e lettura dei provider).
    #[must_use]
    pub fn from_bag(bag: &ParameterBag) -> Self {
        let mut families = Self::default();
        for (name, value) in bag.iter() {
            if let Some(family) = parameter_family(value) {
                families.insert(name.clone(), family);
            }
        }
        families
    }

    /// Dai nomi dei bind nell'ordine dello SQL e dai valori posizionali (lo
    /// SQL compilato dallo SDK). Un numero diverso di nomi e di valori non si
    /// lega: nessuna famiglia nota, e l'esecuzione lo rifiutera. Un nome
    /// legato piu volte con famiglie diverse le porta tutte: ogni suo uso e
    /// una conversione implicita, e si rifiuta (fino alla seconda lettura del
    /// 25/10 il nome perdeva la famiglia, e passava).
    #[must_use]
    pub fn from_positional(names: &[String], values: &[ParameterValue]) -> Self {
        let mut families = Self::default();
        if names.len() != values.len() {
            return families;
        }
        for (name, value) in names.iter().zip(values) {
            if let Some(family) = parameter_family(value) {
                families.insert(name.clone(), family);
            }
        }
        families
    }

    /// Una famiglia vista per un parametro con nome, in piu di quelle gia
    /// viste.
    pub fn insert(&mut self, name: impl Into<String>, family: OperandFamily) {
        *self.0.entry(name.into()).or_insert(0) |= family.bit();
    }

    /// La famiglia del parametro, se e una sola.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<OperandFamily> {
        let bits = self.bits(name);
        [
            OperandFamily::Geometry,
            OperandFamily::Binary,
            OperandFamily::Text,
            OperandFamily::Numeric,
            OperandFamily::Temporal,
            OperandFamily::Other,
        ]
        .into_iter()
        .find(|family| family.bit() == bits)
    }

    fn bits(&self, name: &str) -> u8 {
        self.0.get(name).copied().unwrap_or(0)
    }

    pub(super) fn merged(&self, other: &Self) -> Self {
        let mut merged = self.clone();
        for (name, bits) in &other.0 {
            *merged.0.entry(name.clone()).or_insert(0) |= bits;
        }
        merged
    }
}

/// Il tipo di un operando come lo vede il giudizio: le famiglie possibili
/// (piu d'una se il nome ha piu candidati), se il valore e derivato, se e un
/// parametro del chiamante.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Typed {
    families: u8,
    derived: bool,
    parameter: bool,
}

impl Typed {
    const UNKNOWN: Self = Self {
        families: 0,
        derived: false,
        parameter: false,
    };

    const fn of(family: OperandFamily, derived: bool) -> Self {
        Self {
            families: family.bit(),
            derived,
            parameter: false,
        }
    }

    const fn parameter(families: u8) -> Self {
        Self {
            families,
            derived: false,
            parameter: true,
        }
    }

    const fn judged(self) -> u8 {
        self.families & JUDGED
    }

    const fn has(self, family: OperandFamily) -> bool {
        self.families & family.bit() != 0
    }

    /// Geometrico sotto un operatore: geometria, o binario derivato.
    pub(super) const fn is_geometric(self) -> bool {
        self.has(OperandFamily::Geometry) || (self.has(OperandFamily::Binary) && self.derived)
    }

    /// Piu d'una famiglia giudicata.
    const fn mixed(self) -> bool {
        self.judged().count_ones() > 1
    }

    /// L'unione: ogni famiglia di entrambi.
    const fn union(self, other: Self) -> Self {
        Self {
            families: self.families | other.families,
            derived: self.derived || other.derived,
            parameter: self.parameter && other.parameter,
        }
    }

    /// Le famiglie giudicate stanno tutte in `allowed`.
    const fn within(self, allowed: u8) -> bool {
        self.judged() & !allowed == 0
    }
}

pub(super) fn mismatch(provider: crate::plan::ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        ErrorPhase::Prepare,
        Some(provider),
        OPERAND_FAMILY_MISMATCH,
    )
}

fn too_deep(provider: crate::plan::ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        ErrorPhase::Prepare,
        Some(provider),
        OPERAND_TYPING_TOO_DEEP,
    )
}

/// Lo stato della regola F in una visita.
#[derive(Default)]
pub(super) struct Typing {
    expressions: BTreeMap<(usize, usize), Typed>,
    positions: BTreeMap<(usize, Option<usize>, usize), Typed>,
    in_progress: std::collections::BTreeSet<(usize, Option<usize>, usize)>,
    depth: usize,
    /// La prima conversione implicita trovata: si rende alla fine della
    /// visita, dopo i rifiuti della regola degli operandi geometrici, che
    /// dicono di piu.
    pub(super) mismatch: Option<DatabaseError>,
}

/// Una conversione implicita si annota e la visita continua; ogni altro
/// errore si rende subito.
fn deferred<T>(typing: &mut Typing, outcome: Result<T>) -> Result<Option<T>> {
    match outcome {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.message == OPERAND_FAMILY_MISMATCH => {
            typing.mismatch.get_or_insert(error);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

impl<'a> Walk<'a, '_> {
    /// La regola F e il binario derivato su un'espressione visitata, con il
    /// catalogo: le famiglie dentro l'espressione, e, se e un operando, una
    /// sola famiglia e nessuna geometria.
    pub(super) fn judge_types(
        &mut self,
        expression: &'a QueryExpression,
        operand: bool,
        index: usize,
    ) -> Result<()> {
        let outcome = self.type_of(expression, index);
        let Some(typed) = deferred(&mut self.typing, outcome)? else {
            return Ok(());
        };
        if operand {
            if typed.is_geometric() {
                return Err(super::refusal(self.provider));
            }
            if typed.mixed() {
                self.typing
                    .mismatch
                    .get_or_insert_with(|| mismatch(self.provider));
            }
        }
        Ok(())
    }

    /// Le posizioni di uscita dei rami di una set operation che confronta
    /// righe hanno una sola famiglia.
    pub(super) fn judge_branches(
        &mut self,
        operation: &'a QueryOperation,
        parent: Option<usize>,
        index: usize,
    ) -> Result<()> {
        let (entries, _) = self.entries_of(index, 0);
        // Il blocco e gia aperto (`index`): le CTE visibili sono le sue.
        let visible = self.blocks[index].visible.clone();
        for position in 0..entries.len() {
            let outcome = self.position_type(operation, visible.clone(), parent, position);
            let Some(typed) = deferred(&mut self.typing, outcome)? else {
                continue;
            };
            if typed.mixed() {
                self.typing
                    .mismatch
                    .get_or_insert_with(|| mismatch(self.provider));
            }
        }
        Ok(())
    }

    fn type_of(&mut self, expression: &'a QueryExpression, index: usize) -> Result<Typed> {
        let key = (std::ptr::from_ref(expression) as usize, index);
        if let Some(typed) = self.typing.expressions.get(&key) {
            return Ok(*typed);
        }
        if self.typing.depth >= MAX_TYPING_DEPTH {
            return Err(too_deep(self.provider));
        }
        self.typing.depth += 1;
        let typed = self.type_uncached(expression, index);
        self.typing.depth -= 1;
        let typed = typed?;
        self.typing.expressions.insert(key, typed);
        Ok(typed)
    }

    /// Il tipo di un operando: una sola famiglia. Anche un'espressione
    /// geometrica: la conversione si annota (`deferred`) e si rende dopo i
    /// rifiuti della regola degli operandi geometrici, che restano i primi.
    fn operand(&mut self, expression: &'a QueryExpression, index: usize) -> Result<Typed> {
        let typed = self.type_of(expression, index)?;
        if typed.mixed() {
            return Err(mismatch(self.provider));
        }
        Ok(typed)
    }

    /// Due operandi della stessa famiglia: l'unione.
    fn same(&self, left: Typed, right: Typed) -> Result<Typed> {
        let both = left.union(right);
        if left.judged() != 0 && right.judged() != 0 && both.mixed() {
            return Err(mismatch(self.provider));
        }
        Ok(both)
    }

    /// Operandi che devono stare in `allowed`.
    fn within(&self, typed: Typed, allowed: u8) -> Result<Typed> {
        if typed.within(allowed) {
            Ok(typed)
        } else {
            Err(mismatch(self.provider))
        }
    }

    #[allow(clippy::too_many_lines)]
    fn type_uncached(&mut self, expression: &'a QueryExpression, index: usize) -> Result<Typed> {
        use OperandFamily as F;
        let boolean = Typed::of(F::Numeric, false);
        Ok(match expression {
            QueryExpression::Wildcard { .. } => Typed::UNKNOWN,
            QueryExpression::Column { column } => self.reference_type(column, index)?,
            QueryExpression::Parameter { name } => Typed::parameter(self.parameters.bits(name)),
            QueryExpression::TypedParameter { parameter_type, .. } => {
                Typed::parameter(query_parameter_family(*parameter_type).bit())
            }
            QueryExpression::Cast {
                expression,
                target_type,
            } => {
                let inner = self.type_of(expression, index)?;
                let family = query_parameter_family(*target_type);
                let derived = if family == F::Binary {
                    // Il binario di un testo, di un valore derivato o di un
                    // valore che non si conosce: un binario derivato. Il
                    // binario di un parametro, i byte del chiamante.
                    !inner.parameter && (inner.has(F::Text) || inner.derived || inner.families == 0)
                } else {
                    inner.derived
                };
                Typed::of(family, derived)
            }
            QueryExpression::NullsOrder { expression, .. } => self.type_of(expression, index)?,
            QueryExpression::Case {
                branches,
                else_expression,
            } => {
                let mut result: Option<Typed> = None;
                for value in branches
                    .iter()
                    .map(|branch| &branch.then)
                    .chain(else_expression.as_deref())
                {
                    let typed = self.operand(value, index)?;
                    result = Some(match result {
                        Some(result) => self.same(result, typed)?,
                        None => typed,
                    });
                }
                result.unwrap_or(Typed::UNKNOWN)
            }
            QueryExpression::Scalar {
                function,
                arguments,
            }
            | QueryExpression::Window {
                function,
                arguments,
                ..
            } => self.function_type(*function, arguments, index)?,
            QueryExpression::Spatial {
                function,
                arguments,
            }
            | QueryExpression::SpatialWindow {
                function,
                arguments,
                ..
            } => match spatial_result(*function, arguments.len()) {
                None => Typed::of(F::Geometry, true),
                Some((family, derived)) => Typed::of(family, derived),
            },
            QueryExpression::SpatialValue { .. } => Typed::of(F::Geometry, false),
            QueryExpression::SpatialOutput { .. } => Typed::of(F::Binary, true),
            QueryExpression::Arithmetic { left, right, .. } => {
                // Soltanto numeri: l'aritmetica delle date ha un risultato
                // che dipende dal dialetto (un numero su Oracle, un
                // intervallo su PostgreSQL, la data resa numero su `MySQL`),
                // e una data con un numero e una conversione su `MySQL`.
                let numeric = F::Numeric.bit();
                let left = self.operand(left, index)?;
                let right = self.operand(right, index)?;
                let left = self.within(left, numeric)?;
                let right = self.within(right, numeric)?;
                Typed::of(F::Numeric, left.derived || right.derived)
            }
            QueryExpression::Compare { left, right, .. } => {
                let left = self.operand(left, index)?;
                let right = self.operand(right, index)?;
                self.same(left, right)?;
                boolean
            }
            QueryExpression::InList {
                expression, values, ..
            } => {
                let mut result = self.operand(expression, index)?;
                for value in values {
                    let typed = self.operand(value, index)?;
                    result = self.same(result, typed)?;
                }
                boolean
            }
            QueryExpression::Between {
                expression,
                lower,
                upper,
                ..
            } => {
                let mut result = self.operand(expression, index)?;
                for bound in [lower, upper] {
                    let typed = self.operand(bound, index)?;
                    result = self.same(result, typed)?;
                }
                boolean
            }
            QueryExpression::Like {
                expression,
                pattern,
                ..
            } => {
                let text = F::Text.bit();
                let value = self.operand(expression, index)?;
                let pattern = self.operand(pattern, index)?;
                self.within(value, text)?;
                self.within(pattern, text)?;
                boolean
            }
            QueryExpression::ScalarSubquery { query } => {
                let visible = self.blocks[index].visible.clone();
                self.position_type(query, visible, Some(index), 0)?
            }
            QueryExpression::InSubquery {
                expression, query, ..
            } => {
                let value = self.operand(expression, index)?;
                let visible = self.blocks[index].visible.clone();
                let projected = self.position_type(query, visible, Some(index), 0)?;
                if projected.mixed() {
                    return Err(mismatch(self.provider));
                }
                self.same(value, projected)?;
                boolean
            }
            QueryExpression::SpatialOperator { .. }
            | QueryExpression::Exists { .. }
            | QueryExpression::Not { .. }
            | QueryExpression::And { .. }
            | QueryExpression::Or { .. }
            | QueryExpression::IsNull { .. } => boolean,
        })
    }

    fn function_type(
        &mut self,
        function: ScalarFunction,
        arguments: &'a [QueryExpression],
        index: usize,
    ) -> Result<Typed> {
        use OperandFamily as F;
        let first = match arguments.first() {
            Some(first) => Some(self.operand(first, index)?),
            None => None,
        };
        let first = first.unwrap_or(Typed::UNKNOWN);
        Ok(match function {
            ScalarFunction::Lower | ScalarFunction::Upper => {
                let argument = self.within(first, F::Text.bit())?;
                Typed::of(F::Text, argument.derived)
            }
            ScalarFunction::Sum | ScalarFunction::Average => {
                let argument = self.within(first, F::Numeric.bit())?;
                Typed::of(F::Numeric, argument.derived)
            }
            ScalarFunction::Minimum | ScalarFunction::Maximum => first,
            ScalarFunction::Coalesce => {
                let mut result = first;
                for argument in arguments.iter().skip(1) {
                    let typed = self.operand(argument, index)?;
                    result = self.same(result, typed)?;
                }
                result
            }
            // Il valore di default di `LAG`/`LEAD` (terzo argomento) e della
            // famiglia del primo.
            ScalarFunction::Lag | ScalarFunction::Lead => {
                if let Some(default) = arguments.get(2) {
                    let typed = self.operand(default, index)?;
                    self.same(first, typed)?;
                }
                first
            }
            ScalarFunction::Count
            | ScalarFunction::RowNumber
            | ScalarFunction::Rank
            | ScalarFunction::DenseRank => Typed::of(F::Numeric, false),
        })
    }

    /// Il tipo di un riferimento di colonna: l'unione dei suoi candidati,
    /// come li raccoglie il giudizio geometrico (`reference`).
    fn reference_type(&mut self, column: &ColumnRef, index: usize) -> Result<Typed> {
        let mut candidates = match &column.relation {
            Some(relation) => self.qualified(relation, &column.field, index),
            None => self.unqualified(&column.field, index),
        };
        if let Ok(own) = self.output_positions(index, column) {
            let block = &self.blocks[index];
            let (operation, visible, parent) =
                (block.operation, block.visible.clone(), block.parent);
            for position in own {
                candidates.push(Candidate::Follow(Item::Position(
                    operation,
                    visible.clone(),
                    parent,
                    position,
                )));
            }
        }
        self.candidates_type(candidates)
    }

    fn candidates_type(&mut self, candidates: Vec<Candidate<'a>>) -> Result<Typed> {
        let mut result: Option<Typed> = None;
        for candidate in candidates {
            let typed = match candidate {
                Candidate::Geometry => Typed::of(OperandFamily::Geometry, false),
                Candidate::Typed(column) => Typed::of(
                    column.family,
                    matches!(column.origin, ColumnOrigin::Derived),
                ),
                Candidate::Scalar | Candidate::Unresolved | Candidate::Ambiguous => continue,
                Candidate::Follow(item) => match item {
                    Item::Position(query, visible, parent, position) => {
                        self.position_type(query, visible, parent, position)?
                    }
                    Item::Reference(column, index) => self.reference_type(&column, index)?,
                    Item::Expression(expression, _, index) => self.type_of(expression, index)?,
                    Item::Operation(..) => continue,
                },
            };
            result = Some(result.map_or(typed, |result| result.union(typed)));
        }
        Ok(result.unwrap_or(Typed::UNKNOWN))
    }

    /// Il tipo della colonna di uscita in posizione `position` di `query`:
    /// l'unione dei rami di set operation.
    fn position_type(
        &mut self,
        query: &'a QueryOperation,
        visible: Vec<&'a CommonTableExpression>,
        parent: Option<usize>,
        position: usize,
    ) -> Result<Typed> {
        let key = (address(query), parent, position);
        if let Some(typed) = self.typing.positions.get(&key) {
            return Ok(*typed);
        }
        // Una CTE ricorsiva che si nomina: il ramo ricorsivo non aggiunge
        // famiglie a quelle dell'ancora.
        if !self.typing.in_progress.insert(key) {
            return Ok(Typed::UNKNOWN);
        }
        let first = self.open_block(query, visible, parent);
        let mut branches = vec![first];
        let mut cursor = 0;
        while let Some(branch) = branches.get(cursor).copied() {
            let block = &self.blocks[branch];
            let (operation, inner) = (block.operation, block.visible.clone());
            for set in &operation.set_operations {
                branches.push(self.open_block(&set.query, inner.clone(), parent));
            }
            cursor += 1;
        }
        let mut result: Option<Typed> = None;
        for branch in branches {
            let (entries, _) = self.entries_of(branch, 0);
            let Some(entry) = entries.get(position) else {
                continue;
            };
            let operation = self.blocks[branch].operation;
            let typed = match &entry.source {
                EntrySource::Projection(item) => {
                    self.type_of(&operation.projection[*item].expression, branch)
                }
                EntrySource::Column(column) => self.reference_type(column, branch),
                EntrySource::Position(query, visible, parent, position) => {
                    self.position_type(query, visible.clone(), *parent, *position)
                }
            };
            let typed = match typed {
                Ok(typed) => typed,
                Err(error) => {
                    self.typing.in_progress.remove(&key);
                    return Err(error);
                }
            };
            result = Some(result.map_or(typed, |result| result.union(typed)));
        }
        self.typing.in_progress.remove(&key);
        let typed = result.unwrap_or(Typed::UNKNOWN);
        self.typing.positions.insert(key, typed);
        Ok(typed)
    }
}

#[cfg(test)]
#[path = "spatial_operand_types_tests.rs"]
mod tests;
