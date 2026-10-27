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
//! tipo di destinazione. Il tipo dichiarato vale perche il valore legato e del
//! suo tipo: la regola T ([`crate::typed_parameter`]) rifiuta prima un valore
//! di un altro tipo (fino alla quattordicesima lettura del 25/10 la famiglia
//! del tipo dichiarato copriva un valore di un'altra famiglia). Due famiglie diverse si rifiutano prima
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
//! | altra | tutto il resto (uuid, json, xml, intervalli, enumerazioni e tipi composti di `PostgreSQL`, tipi dell'utente), con l'identita del tipo; un bind `Uuid` o `Json` dove il provider lo lega nel suo tipo (`uuid` e `jsonb` su `PostgreSQL`, `uniqueidentifier` su SQL Server, `JSON` su Oracle; un testo altrove) |
//!
//! # Regola B del mondo chiuso: «altra»
//!
//! La famiglia «altra» si giudica come le altre: con un'altra famiglia si
//! rifiuta; con «altra» soltanto se l'identita del tipo e la stessa (il nome
//! completo del catalogo, `uuid`, `jsonb`, un dominio o un tipo dell'utente
//! con il proprio nome), e un'identita che non si conosce non e uguale a
//! nessuna. Fino alla quindicesima lettura del 25/10 «altra» non si
//! giudicava: `n INT = typed_parameter(uuid)` rendeva `n = 1` su `MySQL` e
//! `MariaDB`. La famiglia di un parametro tipizzato e di un `CAST` e quella
//! del tipo che il renderer scrive (`rendered_family`): un `uuid` reso
//! `CHAR(36)` o senza `CAST` e un testo.
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
//! - **Regola.** La regola F non giudica un parametro senza valore noto.
//! - **Ambito.** La compilazione senza esecuzione, un `Null`, uno statement
//!   portable eseguito senza valori legati.
//! - **Hazard.** Nessuno nell'esecuzione: un `Null` non confronta niente, e
//!   ogni percorso che esegue un piano lega i valori.
//! - **Rientro.** Non serve.
//!
//! - **Regola.** Un uuid legato come testo (`MySQL`, `MariaDB`, Oracle, Db2)
//!   e un testo: con una colonna `UUID` di `MariaDB` si rifiuta.
//! - **Ambito.** `MariaDB` (il tipo `UUID`).
//! - **Hazard.** Nessun passaggio silenzioso: il costo e il rifiuto di un
//!   confronto che il server converte esatto.
//! - **Rientro.** Quando il binding lega un uuid nel tipo `UUID` di
//!   `MariaDB`.
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
//! - **Regola.** L'origine di una colonna guarda ogni relazione di cui il
//!   riferimento rende le righe: i discendenti di `PostgreSQL` (tabelle
//!   figlie e partizioni, a ogni livello) e le tabelle esterne; su `MySQL` e
//!   `MariaDB` le tabelle `MERGE`, `FEDERATED`, `CONNECT`, `SPIDER`. Le
//!   partizioni di Oracle, Db2, `MySQL` e SQL Server condividono la
//!   definizione della tabella (colonne virtuali, generate, calcolate
//!   comprese); le viste partizionate di SQL Server e le gerarchie di tabelle
//!   tipizzate di Db2 sono viste o relazioni gia lette.
//! - **Ambito.** Ogni provider (quattordicesima lettura del 25/10: su
//!   `PostgreSQL` 14 e 15 una figlia generata sotto un padre memorizzato
//!   rendeva una riga su due).
//! - **Hazard.** Il costo: una colonna binaria di una tabella esterna o di una
//!   `MERGE` si giudica geometrica sotto un operatore non spatial, anche se le
//!   sue righe portano byte del chiamante.
//! - **Rientro.** Quando il giudizio legge la definizione remota.
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

/// Motivo stabile del rifiuto di valori posizionali che non sono tanti quanti
/// i parametri del piano.
pub const BIND_LAYOUT_MISMATCH: &str =
    "i valori legati non sono tanti quanti i parametri del piano: ogni valore si lega a un solo segnaposto";

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

/// I bit delle famiglie che la regola F giudica: tutte, «altra» compresa
/// (regola B del mondo chiuso: fino alla quindicesima lettura del 25/10
/// «altra» non si giudicava, e un uuid confrontato con un intero passava).
const JUDGED: u8 = 63;

/// L'identita di un tipo «altra» che non si conosce: non e uguale a nessuna.
const UNKNOWN_IDENTITY: u16 = u16::MAX;

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

/// Il tipo di una colonna del catalogo.
///
/// Origine, famiglia e, dove il
/// catalogo lo dice, il tipo con precisione, scala e lunghezza (regola C,
/// [`crate::cast_table`]) e l'identita del tipo (il nome completo che il
/// catalogo da: per la famiglia «altra» due tipi si confrontano soltanto se
/// l'identita e la stessa, regola B).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    pub origin: ColumnOrigin,
    pub family: OperandFamily,
    pub catalog: Option<crate::cast_table::CatalogType>,
    pub identity: Option<String>,
}

impl ColumnType {
    #[must_use]
    pub const fn new(origin: ColumnOrigin, family: OperandFamily) -> Self {
        Self {
            origin,
            family,
            catalog: None,
            identity: None,
        }
    }

    /// La regola B sulla colonna.
    #[must_use]
    pub const fn is_geometric(&self) -> bool {
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
    /// catalogo di prova): memorizzata, geometrica o della famiglia «altra»
    /// con un'identita sola, `mapping`.
    #[must_use]
    pub fn from_flag(name: impl Into<String>, geometric: bool) -> Self {
        let family = if geometric {
            OperandFamily::Geometry
        } else {
            OperandFamily::Other
        };
        let mut column = Self::new(name, ColumnOrigin::Stored, family);
        // Un'identita sola per le colonne non geometriche di un mapping:
        // si confrontano fra loro, non con valori di un'altra famiglia.
        column.column_type.identity = (!geometric).then(|| "mapping".to_owned());
        column
    }

    /// La regola B sulla colonna.
    #[must_use]
    pub const fn is_geometric(&self) -> bool {
        self.column_type.is_geometric()
    }

    /// La famiglia e l'identita della colonna, per la regola F di una lettura
    /// (`spatial_operand::ensure_scalar_filter_fields`).
    #[must_use]
    pub fn field_family(&self) -> super::FieldFamily {
        (self.column_type.family, self.column_type.identity.clone())
    }

    /// La colonna con il tipo del catalogo ([`crate::cast_table::CatalogType`])
    /// e la sua identita (il nome completo del tipo, senza maiuscole).
    #[must_use]
    pub fn with_type(mut self, catalog: crate::cast_table::CatalogType, identity: &str) -> Self {
        self.column_type.catalog = Some(catalog);
        self.column_type.identity = Some(identity.to_lowercase());
        self
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

/// La famiglia e l'identita di un tipo reso dalla libreria.
///
/// Un parametro tipizzato o un `CAST` hanno quelle del tipo che il renderer scrive
/// ([`crate::cast_table::rendered_target`]), non del tipo dichiarato. Un
/// `uuid` reso `CHAR(36)` e un testo, un `json` reso `NVARCHAR(MAX)` o
/// `CLOB` anche; `UUID` e `JSONB` di `PostgreSQL`, `UNIQUEIDENTIFIER` di SQL
/// Server e `JSON` di `MySQL` sono della famiglia «altra», con la loro
/// identita.
#[must_use]
pub fn rendered_family(
    provider: crate::plan::ProviderKind,
    parameter_type: QueryParameterType,
) -> (OperandFamily, Option<&'static str>) {
    use crate::cast_table::OperandType as O;
    use crate::plan::ProviderKind as P;
    match crate::cast_table::rendered_target(provider, parameter_type) {
        O::Text { .. } | O::Enumeration => (OperandFamily::Text, None),
        O::Binary { .. } => (OperandFamily::Binary, None),
        O::Uuid => (
            OperandFamily::Other,
            Some(if provider == P::Sqlserver {
                "uniqueidentifier"
            } else {
                "uuid"
            }),
        ),
        O::Json => (
            OperandFamily::Other,
            Some(if provider == P::Postgres {
                "jsonb"
            } else {
                "json"
            }),
        ),
        _ => (query_parameter_family(parameter_type), None),
    }
}

/// La famiglia e l'identita di un valore legato `Uuid` o `Json`, come il
/// provider lo lega: un uuid e un `uuid` su `PostgreSQL`, un testo che il
/// server converte esatto in `uniqueidentifier` su SQL Server, un testo
/// altrove; un json e un `jsonb` su `PostgreSQL`, un `JSON` su Oracle, un
/// testo su SQL Server e Db2, e su `MySQL` e `MariaDB` un testo che il server
/// confronta con un `JSON` come stringa JSON: «altra» senza identita, che non
/// si confronta con niente.
const fn bound_other(
    provider: crate::plan::ProviderKind,
    json: bool,
) -> (OperandFamily, Option<&'static str>, bool) {
    use crate::plan::ProviderKind as P;
    match (json, provider) {
        (false, P::Postgres) => (OperandFamily::Other, Some("uuid"), false),
        (false, P::Sqlserver) => (OperandFamily::Other, Some("uniqueidentifier"), false),
        (true, P::Postgres) => (OperandFamily::Other, Some("jsonb"), false),
        (true, P::Oracle) => (OperandFamily::Other, Some("json"), false),
        (true, P::Mysql | P::Mariadb) => (OperandFamily::Other, None, true),
        _ => (OperandFamily::Text, None, false),
    }
}

/// Le famiglie dei parametri per nome, come le dice il valore legato sul
/// provider, e i valori stessi per le regole T e C
/// ([`crate::typed_parameter`], [`crate::cast_table`]).
///
/// I bit di ogni famiglia vista per quel nome (piu d'una se lo stesso nome e legato
/// a valori di famiglie diverse, e allora ogni suo uso si rifiuta).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParameterFamilies {
    families: BTreeMap<String, u8>,
    bound: crate::typed_parameter::BoundValues,
    /// I valori sono quelli che l'esecuzione lega: la regola C li giudica.
    values_known: bool,
}

impl ParameterFamilies {
    /// Da un `ParameterBag` (i path query e lettura dei provider).
    #[must_use]
    pub fn from_bag(bag: &ParameterBag) -> Self {
        let mut families = Self {
            values_known: true,
            ..Self::default()
        };
        for (name, value) in bag.iter() {
            families.bind(name, value);
        }
        families
    }

    /// Dai nomi dei bind nell'ordine dello SQL e dai valori posizionali (lo
    /// SQL compilato dallo SDK). Un nome legato piu volte con famiglie
    /// diverse le porta tutte: ogni suo uso e una conversione implicita, e si
    /// rifiuta (fino alla seconda lettura del 25/10 il nome perdeva la
    /// famiglia, e passava).
    ///
    /// # Errors
    ///
    /// `invalid_plan` in fase `prepare` se i valori non sono tanti quanti i
    /// nomi: un valore in piu non si lega a nessun parametro del piano, e
    /// non tutti i driver lo rifiutano (`sp_executesql` di SQL Server
    /// accetta parametri che lo SQL non usa). Fino alla quattordicesima
    /// lettura del 25/10 il giudizio proseguiva senza famiglie.
    pub fn from_positional(
        provider: crate::plan::ProviderKind,
        names: &[String],
        values: &[ParameterValue],
    ) -> Result<Self> {
        if names.len() != values.len() {
            return Err(DatabaseError::new(
                ErrorCategory::InvalidPlan,
                ErrorPhase::Prepare,
                Some(provider),
                BIND_LAYOUT_MISMATCH,
            ));
        }
        let mut families = Self {
            values_known: true,
            ..Self::default()
        };
        for (name, value) in names.iter().zip(values) {
            families.bind(name, value);
        }
        Ok(families)
    }

    /// Un valore legato al nome: la sua famiglia la decide il provider
    /// ([`Self::get`]).
    pub(super) fn bind(&mut self, name: &str, value: &ParameterValue) {
        self.bound.add(name, value);
    }

    /// Una famiglia vista per un parametro con nome, in piu di quelle gia
    /// viste.
    pub fn insert(&mut self, name: impl Into<String>, family: OperandFamily) {
        *self.families.entry(name.into()).or_insert(0) |= family.bit();
    }

    /// La famiglia del parametro sul provider, se e una sola, con l'identita
    /// di un tipo «altra» (`None` se non si conosce).
    #[must_use]
    pub fn get(
        &self,
        provider: crate::plan::ProviderKind,
        name: &str,
    ) -> Option<(OperandFamily, Option<&'static str>)> {
        let (bits, identity, unknown) = self.resolved(provider, name);
        let identity = if unknown { None } else { identity };
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
        .map(|family| (family, identity))
    }

    /// I bit delle famiglie del nome, l'identita «altra» e se un'identita
    /// «altra» non si conosce o ce n'e piu d'una.
    pub(super) fn resolved(
        &self,
        provider: crate::plan::ProviderKind,
        name: &str,
    ) -> (u8, Option<&'static str>, bool) {
        let mut bits = self.families.get(name).copied().unwrap_or(0);
        // Una famiglia «altra» inserita senza valore non ha identita.
        let mut unknown = bits & OperandFamily::Other.bit() != 0;
        let mut identity: Option<&'static str> = None;
        if let Some(bound) = self.bound.get(name) {
            let kinds = bound.kinds();
            for (kind, family) in crate::typed_parameter::KIND_FAMILIES {
                if kinds & kind != 0 {
                    bits |= family.bit();
                }
            }
            for (kind, json) in [
                (crate::typed_parameter::UUID_KIND, false),
                (crate::typed_parameter::JSON_KIND, true),
            ] {
                if kinds & kind == 0 {
                    continue;
                }
                let (family, other, nameless) = bound_other(provider, json);
                bits |= family.bit();
                unknown |= nameless;
                if let Some(other) = other {
                    if identity.is_some_and(|seen| seen != other) {
                        unknown = true;
                    }
                    identity = Some(other);
                }
            }
        }
        (bits, identity, unknown)
    }

    /// La regola T ([`crate::typed_parameter`]) sui parametri tipizzati del
    /// piano, con i valori legati: prima del giudizio sul catalogo, e anche
    /// quando il piano non ha niente da leggere.
    ///
    /// # Errors
    ///
    /// `invalid_plan` ([`crate::typed_parameter::TYPED_PARAMETER_MISMATCH`]).
    pub fn judge_typed(
        &self,
        provider: crate::plan::ProviderKind,
        plan: &super::OperandPlan<'_>,
    ) -> Result<()> {
        match *plan {
            super::OperandPlan::Query(query) => self.bound.judge_query(provider, query),
            super::OperandPlan::Mutation(mutation) => self.bound.judge_mutation(provider, mutation),
            // Lo statement portable porta valori, non parametri tipizzati.
            super::OperandPlan::Portable(_) => Ok(()),
        }
    }

    /// I valori legati al nome (regola C); vale soltanto se l'esecuzione li
    /// conosce ([`Self::values_known`]).
    pub(super) fn values(&self, name: &str) -> Option<&crate::typed_parameter::BoundValue> {
        self.bound.get(name)
    }

    pub(super) const fn values_known(&self) -> bool {
        self.values_known
    }

    pub(super) fn merged(&self, other: &Self) -> Self {
        let mut merged = self.clone();
        for (name, bits) in &other.families {
            *merged.families.entry(name.clone()).or_insert(0) |= bits;
        }
        merged.bound.merge(&other.bound);
        merged.values_known |= other.values_known;
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
    /// L'identita del tipo «altra» (regola B): `0` senza «altra»,
    /// [`UNKNOWN_IDENTITY`] se non si conosce, altrimenti l'indice piu uno
    /// nel registro della visita.
    other: u16,
    /// Due identita «altra» diverse (o una non nota) si sono incontrate.
    conflict: bool,
}

impl Typed {
    const UNKNOWN: Self = Self {
        families: 0,
        derived: false,
        parameter: false,
        other: 0,
        conflict: false,
    };

    const fn of(family: OperandFamily, derived: bool) -> Self {
        Self {
            families: family.bit(),
            derived,
            parameter: false,
            other: if matches!(family, OperandFamily::Other) {
                UNKNOWN_IDENTITY
            } else {
                0
            },
            conflict: false,
        }
    }

    /// Con l'identita «altra» `other` (se la famiglia e «altra»).
    const fn with_other(mut self, other: u16) -> Self {
        if self.families & OperandFamily::Other.bit() != 0 {
            self.other = other;
        }
        self
    }

    const fn parameter(families: u8) -> Self {
        Self {
            families,
            derived: false,
            parameter: true,
            other: if families & OperandFamily::Other.bit() != 0 {
                UNKNOWN_IDENTITY
            } else {
                0
            },
            conflict: false,
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

    /// Piu d'una famiglia giudicata, o due tipi «altra» che non si provano
    /// uguali.
    const fn mixed(self) -> bool {
        self.judged().count_ones() > 1 || self.conflict
    }

    /// L'unione: ogni famiglia di entrambi; due identita «altra» si
    /// uniscono soltanto se sono la stessa e nota.
    const fn union(self, other: Self) -> Self {
        let both_other = self.other != 0 && other.other != 0;
        let conflict = self.conflict
            || other.conflict
            || (both_other && (self.other != other.other || self.other == UNKNOWN_IDENTITY));
        Self {
            families: self.families | other.families,
            derived: self.derived || other.derived,
            parameter: self.parameter && other.parameter,
            other: if self.other != 0 {
                self.other
            } else {
                other.other
            },
            conflict,
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
    /// Il registro delle identita «altra» della visita (regola B).
    identities: Vec<String>,
    expressions: BTreeMap<(usize, usize), Typed>,
    positions: BTreeMap<(usize, Option<usize>, usize), Typed>,
    in_progress: std::collections::BTreeSet<(usize, Option<usize>, usize)>,
    depth: usize,
    /// La prima conversione implicita trovata: si rende alla fine della
    /// visita, dopo i rifiuti della regola degli operandi geometrici, che
    /// dicono di piu.
    pub(super) mismatch: Option<DatabaseError>,
    /// Il primo `CAST` che non si prova esatto (regola C): si rende dopo le
    /// conversioni implicite.
    pub(super) cast: Option<DatabaseError>,
}

impl Typing {
    /// L'indice (piu uno) dell'identita nel registro.
    fn identity(&mut self, identity: Option<&str>) -> u16 {
        let Some(identity) = identity else {
            return UNKNOWN_IDENTITY;
        };
        let identity = identity.to_lowercase();
        let position = self
            .identities
            .iter()
            .position(|seen| *seen == identity)
            .unwrap_or_else(|| {
                self.identities.push(identity);
                self.identities.len() - 1
            });
        u16::try_from(position + 1)
            .ok()
            .filter(|id| *id != UNKNOWN_IDENTITY)
            .unwrap_or(UNKNOWN_IDENTITY)
    }
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
        // Regola C: un `CAST` che la libreria rende si ammette soltanto se e
        // esatto, in ogni posizione, quando l'esecuzione conosce i valori.
        if let QueryExpression::Cast {
            expression: inner,
            target_type,
        } = expression
        {
            // Un `CAST` a testo su `MySQL` e `MariaDB` cambia la collation del
            // confronto: si rifiuta senza guardare l'operando, anche senza
            // valori (sedicesima lettura del 25/10).
            if crate::cast_table::cast_changes_collation(self.provider, *target_type) {
                let provider = self.provider;
                self.typing
                    .cast
                    .get_or_insert_with(|| crate::cast_table::text_cast_refusal(provider));
            } else if self.parameters.values_known() {
                let target = crate::cast_table::rendered_target(self.provider, *target_type);
                self.cast_progress.clear();
                if !self.cast_exact(inner, &target, index, 0)? {
                    let provider = self.provider;
                    self.typing
                        .cast
                        .get_or_insert_with(|| crate::cast_table::cast_refusal(provider));
                }
            }
        }
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

    /// Il valore di `inner` sta esatto in `target` (regola C): un parametro
    /// con i suoi valori legati, una colonna con il tipo del catalogo (anche
    /// attraverso una sorgente derivata, in ogni ramo), un altro `CAST` con il
    /// tipo che rende. Ogni altra espressione non si prova.
    fn cast_exact(
        &mut self,
        inner: &'a QueryExpression,
        target: &crate::cast_table::OperandType,
        index: usize,
        depth: usize,
    ) -> Result<bool> {
        if depth >= MAX_TYPING_DEPTH {
            return Err(too_deep(self.provider));
        }
        let provider = self.provider;
        Ok(match inner {
            QueryExpression::Parameter { name } | QueryExpression::TypedParameter { name, .. } => {
                self.parameters.values_known()
                    && self
                        .parameters
                        .values(name)
                        .is_some_and(|bound| bound.fits(provider, target))
            }
            QueryExpression::Cast { target_type, .. } => crate::cast_table::widens(
                provider,
                &crate::cast_table::rendered_target(provider, *target_type),
                "",
                target,
            ),
            QueryExpression::NullsOrder { expression, .. } => {
                self.cast_exact(expression, target, index, depth + 1)?
            }
            QueryExpression::Column { column } => {
                self.column_exact(column, target, index, depth)?
            }
            _ => false,
        })
    }

    /// Ogni colonna che il riferimento puo nominare sta esatta in `target`.
    fn column_exact(
        &mut self,
        column: &ColumnRef,
        target: &crate::cast_table::OperandType,
        index: usize,
        depth: usize,
    ) -> Result<bool> {
        let provider = self.provider;
        let mut candidates = match &column.relation {
            Some(relation) => self.qualified(relation, &column.field, index),
            None => self.unqualified(&column.field, index),
        };
        let Ok(own) = self.output_positions(index, column) else {
            return Ok(false);
        };
        let block = &self.blocks[index];
        let (operation, visible, parent) = (block.operation, block.visible.clone(), block.parent);
        for position in own {
            candidates.push(Candidate::Follow(Item::Position(
                operation,
                visible.clone(),
                parent,
                position,
            )));
        }
        if candidates.is_empty() {
            return Ok(false);
        }
        for candidate in candidates {
            let exact = match candidate {
                Candidate::Typed(column) => column.catalog.as_ref().is_some_and(|catalog| {
                    crate::cast_table::widens(
                        provider,
                        &crate::cast_table::operand_type(provider, catalog),
                        &catalog.name,
                        target,
                    )
                }),
                Candidate::Follow(Item::Position(query, visible, parent, position)) => {
                    self.position_exact(query, visible, parent, position, target, depth + 1)?
                }
                Candidate::Follow(Item::Reference(column, index)) => {
                    self.column_exact(&column, target, index, depth + 1)?
                }
                Candidate::Follow(Item::Expression(expression, _, index)) => {
                    self.cast_exact(expression, target, index, depth + 1)?
                }
                _ => false,
            };
            if !exact {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// La colonna di uscita in posizione `position` di `query`, in ogni ramo
    /// di set operation, sta esatta in `target`.
    fn position_exact(
        &mut self,
        query: &'a QueryOperation,
        visible: Vec<&'a CommonTableExpression>,
        parent: Option<usize>,
        position: usize,
        target: &crate::cast_table::OperandType,
        depth: usize,
    ) -> Result<bool> {
        // Una CTE ricorsiva che si nomina non si prova.
        if !self
            .cast_progress
            .insert((address(query), parent, position))
        {
            return Ok(false);
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
        for branch in branches {
            let (entries, _) = self.entries_of(branch, 0);
            let Some(entry) = entries.get(position) else {
                return Ok(false);
            };
            let operation = self.blocks[branch].operation;
            let exact = match &entry.source {
                EntrySource::Projection(item) => self.cast_exact(
                    &operation.projection[*item].expression,
                    target,
                    branch,
                    depth + 1,
                )?,
                EntrySource::Column(column) => {
                    self.column_exact(column, target, branch, depth + 1)?
                }
                EntrySource::Position(query, visible, parent, position) => self.position_exact(
                    query,
                    visible.clone(),
                    *parent,
                    *position,
                    target,
                    depth + 1,
                )?,
            };
            if !exact {
                return Ok(false);
            }
        }
        Ok(true)
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
            QueryExpression::Parameter { name } => {
                let (bits, identity, unknown) = self.parameters.resolved(self.provider, name);
                let identity = if unknown { None } else { identity };
                let other = self.typing.identity(identity);
                Typed::parameter(bits).with_other(other)
            }
            QueryExpression::TypedParameter { parameter_type, .. } => {
                let (family, identity) = rendered_family(self.provider, *parameter_type);
                let other = self.typing.identity(identity);
                Typed::parameter(family.bit()).with_other(other)
            }
            QueryExpression::Cast {
                expression,
                target_type,
            } => {
                let inner = self.type_of(expression, index)?;
                let (family, identity) = rendered_family(self.provider, *target_type);
                let other = self.typing.identity(identity);
                let derived = if family == F::Binary {
                    // Il binario di un testo, di un valore derivato o di un
                    // valore che non si conosce: un binario derivato. Il
                    // binario di un parametro, i byte del chiamante.
                    !inner.parameter && (inner.has(F::Text) || inner.derived || inner.families == 0)
                } else {
                    inner.derived
                };
                Typed::of(family, derived).with_other(other)
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
                Candidate::Typed(column) => {
                    let other = self.typing.identity(column.identity.as_deref());
                    Typed::of(
                        column.family,
                        matches!(column.origin, ColumnOrigin::Derived),
                    )
                    .with_other(other)
                }
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
