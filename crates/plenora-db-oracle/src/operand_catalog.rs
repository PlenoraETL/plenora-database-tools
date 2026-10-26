//! Il catalogo degli operandi geometrici di Oracle
//! (`plenora_database_core::spatial_operand`), letto nella transazione e
//! nella lettura del provider.
//!
//! La guardia del server (`spatial_operand::operand_server_guard`) rifiuta
//! ogni operatore scalare su `MDSYS.SDO_GEOMETRY`, ma non su una geometria
//! che una vista rende binaria: `SDO_UTIL.TO_WKBGEOMETRY(G)` e
//! `G.GET_WKB()` sono `BLOB`, `DBMS_LOB.SUBSTR(SDO_UTIL.TO_WKBGEOMETRY(G))` e
//! `RAW`, e `CAST(V.W AS RAW(2000)) = ?`, `ORDER BY CAST(V.W AS RAW(2000))`,
//! `V.R = ?` ne confrontano i byte senza errore (misurato su Oracle AI
//! Database 26ai Free 23.26.2 il 25/10: due poligoni uguali per
//! `SDO_GEOM.RELATE` con i vertici in ordine diverso non sono uguali). Il
//! giudizio quindi legge le colonne di ogni relazione nominata dal piano:
//!
//! - **il nome.** Il renderer quota ogni identificatore: il nome del piano e
//!   quello del catalogo, per byte. Una relazione senza schema si risolve come
//!   la risolve il server: lo schema corrente della sessione
//!   (`CURRENT_SCHEMA`), poi un sinonimo pubblico;
//! - **la relazione.** Una tabella (anche di oggetti, esterna), una vista,
//!   una vista materializzata (`ALL_OBJECTS`). Un sinonimo si segue fino al
//!   suo oggetto (`ALL_SYNONYMS`), anche attraverso altri sinonimi; un
//!   sinonimo verso un database link, che non arriva a una relazione o oltre
//!   [`MAX_SYNONYM_DEPTH`] non si conosce: nessun riferimento si risolve;
//! - **il tipo.** Una colonna e geometrica se il suo tipo e di `MDSYS`
//!   (`SDO_GEOMETRY` e gli altri tipi spatial). Il proprietario si legge
//!   risolto: una colonna dichiarata `SDO_GEOMETRY` senza schema ha
//!   `DATA_TYPE_OWNER` `PUBLIC` (il sinonimo pubblico, misurato il 25/10), e
//!   vale `MDSYS` soltanto se `ALL_SYNONYMS` dice che il sinonimo omonimo
//!   arriva a `MDSYS.SDO_GEOMETRY`. Un tipo omonimo di un altro schema non e
//!   una geometria;
//! - **la colonna calcolata.** In una vista o in una vista materializzata, e
//!   per una colonna virtuale di una tabella (`VIRTUAL_COLUMN` di
//!   `ALL_TAB_COLS`, `G_WKB AS (SDO_UTIL.TO_WKBGEOMETRY(G))`), una colonna
//!   `BLOB`, `RAW`, `LONG RAW` o `BFILE` si giudica geometrica (regola B per
//!   colonna, `spatial_operand::catalog_column_is_geometric`). Fino al 25/10
//!   l'origine era della relazione, e la colonna virtuale binaria di una
//!   tabella restava scalare (misurato: `W = :1` con il WKB di un poligono
//!   uguale con i vertici in un altro ordine rendeva una riga su due);
//! - **la famiglia.** La regola F (`spatial_operand::OperandFamily`): `NUMBER`,
//!   `FLOAT`, `BINARY_FLOAT`, `BINARY_DOUBLE`, `BOOLEAN` numerica; `CHAR`,
//!   `VARCHAR2`, `NCHAR`, `NVARCHAR2`, `CLOB`, `NCLOB`, `LONG` testo; `DATE` e
//!   `TIMESTAMP` (con o senza fuso) temporale; `BLOB`, `RAW`, `LONG RAW`,
//!   `BFILE` binaria; un tipo di `MDSYS` geometrico; il resto (`INTERVAL`,
//!   `ROWID`, `JSON`, `XMLTYPE`, i tipi dell'utente) «altra». Una colonna testo
//!   di una vista resa da `RAWTOHEX(...)` confrontata con
//!   `CAST(:1 AS RAW(2000))` arrivava al server, che converte da se
//!   (misurato il 25/10: una riga su due); ora si rifiuta prima.
//!
//! # Limite dichiarato
//!
//! - **Regola.** Il giudizio legge il catalogo con istruzioni sulla sessione
//!   che esegue la query: una per relazione nominata, piu una per sinonimo
//!   seguito e una per il sinonimo pubblico di un nome senza schema.
//! - **Ambito.** Oracle, l'esecuzione giudicata (SDK, ORM, facade, CLI
//!   portable) e la lettura del provider con filtro o ordinamento.
//! - **Hazard.** Il driver (`vendor/oracle-rs`, `statement_cache.rs`) non
//!   manda la chiusura dei cursori: ogni istruzione completata lascia un
//!   cursore aperto sul server, e una sessione del pool arriva a ORA-01000
//!   (misurato il 25/10: la trecentesima `SELECT 1 FROM DUAL` sulla stessa
//!   sessione, con `open_cursors` 300 dell'immagine). Il giudizio anticipa
//!   l'errore, che resta esplicito: nessun risultato sbagliato.
//! - **Rientro.** Quando il driver chiude i cursori (la chiusura a cavallo
//!   del messaggio successivo, come `python-oracledb`).

use plenora_database_core::plan::ProviderKind;
use plenora_database_core::spatial_operand::{
    CatalogColumn, ColumnOrigin, OperandCatalog, OperandFamily, ParameterFamilies, RelationColumns,
};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

/// Oltre questa catena di sinonimi la relazione non si conosce.
pub const MAX_SYNONYM_DEPTH: usize = 32;

/// Il proprietario dei sinonimi pubblici, dove il server cerca un nome senza
/// schema che lo schema corrente non ha.
const PUBLIC_OWNER: &str = "PUBLIC";

/// Il proprietario dei tipi spatial del prodotto.
const SPATIAL_OWNER: &str = "MDSYS";

/// I tipi binari predefiniti (senza proprietario).
const BINARY_TYPES: [&str; 4] = ["BLOB", "RAW", "LONG RAW", "BFILE"];

/// I tipi carattere predefiniti.
const TEXT_TYPES: [&str; 7] = [
    "CHAR",
    "VARCHAR2",
    "NCHAR",
    "NVARCHAR2",
    "CLOB",
    "NCLOB",
    "LONG",
];

/// I tipi numerici predefiniti (`BOOLEAN` e un intero nella regola F).
const NUMERIC_TYPES: [&str; 6] = [
    "NUMBER",
    "FLOAT",
    "BINARY_FLOAT",
    "BINARY_DOUBLE",
    "INTEGER",
    "BOOLEAN",
];

/// Il proprietario del tipo risolto: se `DATA_TYPE_OWNER` ha un sinonimo
/// omonimo del tipo, senza database link, che arriva a un oggetto omonimo, il
/// proprietario di quell'oggetto (due sinonimi sono un errore della subquery
/// scalare, non una scelta). Lo stesso proprietario della descrizione
/// (`catalog::DESCRIBE_COLUMNS_SQL`).
macro_rules! relation_sql {
    ($owner:literal, $name:literal) => {
        concat!(
            "SELECT O.OWNER, O.OBJECT_TYPE, C.COLUMN_NAME, C.DATA_TYPE, ",
            "COALESCE((SELECT S.TABLE_OWNER FROM ALL_SYNONYMS S WHERE S.OWNER = C.DATA_TYPE_OWNER ",
            "AND S.SYNONYM_NAME = C.DATA_TYPE AND S.TABLE_NAME = C.DATA_TYPE AND S.DB_LINK IS NULL), ",
            "C.DATA_TYPE_OWNER), C.VIRTUAL_COLUMN, ",
            "TO_CHAR(CASE WHEN C.DATA_TYPE = 'RAW' THEN C.DATA_LENGTH ",
            "WHEN C.CHAR_LENGTH > 0 THEN C.CHAR_LENGTH END), ",
            "TO_CHAR(C.DATA_PRECISION), TO_CHAR(C.DATA_SCALE) ",
            "FROM ALL_OBJECTS O LEFT JOIN ALL_TAB_COLS C ON O.OBJECT_TYPE IN ('TABLE', 'VIEW') ",
            "AND C.OWNER = O.OWNER AND C.TABLE_NAME = O.OBJECT_NAME AND C.USER_GENERATED = 'YES' ",
            "WHERE O.OWNER = ",
            $owner,
            " AND O.OBJECT_NAME = ",
            $name,
            " AND O.OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SYNONYM') ",
            "ORDER BY O.OBJECT_TYPE, C.COLUMN_ID NULLS LAST, C.COLUMN_NAME"
        )
    };
}

/// L'oggetto con quel proprietario e quel nome, con le sue colonne: una
/// lettura sola per relazione (il driver non chiude i cursori, vedi il limite
/// del modulo). `ALL_TAB_COLS` con `USER_GENERATED` e l'insieme di righe di
/// `ALL_TAB_COLUMNS`, con `VIRTUAL_COLUMN` che quella non ha.
pub const RELATION_SQL: &str = relation_sql!(":1", ":2");

/// Lo stesso, nello schema corrente della sessione.
pub const RELATION_CURRENT_SQL: &str =
    relation_sql!("SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')", ":1");

pub const SYNONYM_SQL: &str = "SELECT TABLE_OWNER, TABLE_NAME, DB_LINK FROM ALL_SYNONYMS \
    WHERE OWNER = :1 AND SYNONYM_NAME = :2";

/// Un nome qualificato del catalogo: proprietario, nome.
pub type QualifiedName = (String, String);

/// Un sinonimo di `ALL_SYNONYMS`: l'oggetto a cui arriva, `None` se e di un
/// database link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynonymRow {
    pub target: Option<QualifiedName>,
}

/// Una colonna di `ALL_TAB_COLS`: nome, tipo, proprietario risolto del
/// tipo, colonna virtuale; la lunghezza (in caratteri per un testo, in byte
/// per un `RAW`), la precisione e la scala per la regola C
/// (`plenora_database_core::cast_table`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnRow {
    pub name: String,
    pub data_type: String,
    pub data_type_owner: Option<String>,
    pub virtual_column: bool,
    pub length: Option<u64>,
    pub precision: Option<u64>,
    pub scale: Option<i64>,
}

/// Una riga di [`RELATION_SQL`]: il proprietario, il tipo di oggetto
/// (tabella, vista, vista materializzata, sinonimo) e, per una tabella o una
/// vista, una colonna.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRow {
    pub owner: String,
    pub kind: String,
    pub column: Option<ColumnRow>,
}

/// Le letture del catalogo su cui si regge il giudizio. Ogni nome si
/// confronta per byte con quello del catalogo.
#[allow(async_fn_in_trait)]
pub trait OracleCatalog {
    /// L'oggetto con quel proprietario (lo schema corrente se `None`) e quel
    /// nome esatti, con le sue colonne nell'ordine del catalogo.
    ///
    /// # Errors
    ///
    /// Gli errori del driver.
    async fn relation(&mut self, owner: Option<&str>, name: &str) -> Result<Vec<ObjectRow>>;

    /// Il sinonimo con quel proprietario e quel nome, se c'e.
    ///
    /// # Errors
    ///
    /// Gli errori del driver.
    async fn synonym(&mut self, owner: &str, name: &str) -> Result<Option<SynonymRow>>;
}

/// Il catalogo delle relazioni (schema, oggetto) come il piano le nomina.
///
/// # Errors
///
/// Gli errori del catalogo.
pub async fn operand_catalog(
    catalog: &mut impl OracleCatalog,
    relations: &[(Option<String>, String)],
) -> Result<OperandCatalog> {
    let mut result = OperandCatalog::new(ProviderKind::Oracle);
    for (schema, object) in relations {
        let columns = relation_columns(catalog, schema.as_deref(), object).await?;
        result.insert(schema.clone(), object.clone(), columns);
    }
    Ok(result)
}

/// Il giudizio dei campi di una lettura del provider (`ReadOperation`) sulla
/// relazione con schema esplicito: un campo geometrico, o un campo di una
/// relazione che il catalogo non descrive, sotto un filtro scalare o in
/// `ORDER BY` si rifiuta.
///
/// # Errors
///
/// `unsupported` ([`GEOMETRY_SCALAR_OPERAND`](plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND));
/// gli errori del catalogo.
pub async fn ensure_scalar_read_fields(
    catalog: &mut impl OracleCatalog,
    schema: &str,
    object: &str,
    operation: &plenora_database_core::plan::ReadOperation,
    parameters: &plenora_database_core::provider::ParameterBag,
) -> Result<()> {
    if operation.filter.is_none() && operation.order_by.is_empty() {
        return Ok(());
    }
    let columns = relation_columns(catalog, Some(schema), object).await?;
    let column = |field: &str| match &columns {
        RelationColumns::Known(columns) => columns.iter().find(|column| column.name == field),
        _ => None,
    };
    plenora_database_core::spatial_operand::ensure_scalar_filter_fields(
        ProviderKind::Oracle,
        operation.filter.as_ref(),
        &operation.order_by,
        |field| match &columns {
            RelationColumns::Known(_) => column(field).is_some_and(CatalogColumn::is_geometric),
            _ => true,
        },
        |field| Ok(column(field).map(CatalogColumn::field_family)),
        &ParameterFamilies::from_bag(parameters),
    )
}

/// La famiglia di un tipo di `ALL_TAB_COLS` (la tabella del modulo): un tipo
/// di `MDSYS` geometrico, un tipo predefinito (senza proprietario) dalla sua
/// famiglia, ogni altro «altra».
#[must_use]
pub fn column_family(data_type: &str, data_type_owner: Option<&str>) -> OperandFamily {
    match data_type_owner {
        Some(SPATIAL_OWNER) => OperandFamily::Geometry,
        None if BINARY_TYPES.contains(&data_type) => OperandFamily::Binary,
        None if TEXT_TYPES.contains(&data_type) => OperandFamily::Text,
        None if NUMERIC_TYPES.contains(&data_type) => OperandFamily::Numeric,
        None if data_type == "DATE" || data_type.starts_with("TIMESTAMP") => {
            OperandFamily::Temporal
        }
        // Un tipo di un altro proprietario, o un predefinito di un'altra
        // famiglia.
        _ => OperandFamily::Other,
    }
}

/// La colonna e geometrica con la regola dei cataloghi: un tipo di `MDSYS`,
/// o un tipo binario derivato (una vista, una vista materializzata, una
/// colonna virtuale).
#[must_use]
pub fn column_is_geometric(
    origin: ColumnOrigin,
    data_type: &str,
    data_type_owner: Option<&str>,
) -> bool {
    plenora_database_core::spatial_operand::catalog_column_is_geometric(
        origin,
        column_family(data_type, data_type_owner),
    )
}

async fn relation_columns(
    catalog: &mut impl OracleCatalog,
    schema: Option<&str>,
    object: &str,
) -> Result<RelationColumns> {
    let mut rows = catalog.relation(schema, object).await?;
    if rows.is_empty() && schema.is_none() {
        // Senza schema il server cerca poi un sinonimo pubblico.
        rows = catalog.relation(Some(PUBLIC_OWNER), object).await?;
    }
    let mut name = object.to_owned();
    for depth in 0..=MAX_SYNONYM_DEPTH {
        let Some(owner) = rows.first().map(|row| row.owner.clone()) else {
            // Il nome del piano non c'e nel catalogo: i riferimenti non si
            // legano (una tabella temporanea privata, `ORA$PTT_*`, non e in
            // `ALL_OBJECTS` ma il server la legge). Il bersaglio di un
            // sinonimo che non c'e non si conosce.
            return Ok(if depth == 0 {
                RelationColumns::Absent
            } else {
                RelationColumns::Unknown
            });
        };
        if !rows.iter().any(|row| row.kind == "SYNONYM") {
            return Ok(known_columns(&rows));
        }
        let target = catalog
            .synonym(&owner, &name)
            .await?
            .and_then(|synonym| synonym.target);
        let Some((target_owner, target_name)) = target else {
            // Un database link: un altro catalogo.
            return Ok(RelationColumns::Unknown);
        };
        rows = catalog.relation(Some(&target_owner), &target_name).await?;
        name = target_name;
    }
    // Una catena di sinonimi oltre il limite, o un ciclo.
    Ok(RelationColumns::Unknown)
}

/// Le colonne di una tabella, una vista o una vista materializzata (le
/// colonne della sua tabella), con l'origine per colonna: derivata in una
/// vista o in una vista materializzata, e per una colonna virtuale.
fn known_columns(rows: &[ObjectRow]) -> RelationColumns {
    let derived = rows
        .iter()
        .any(|row| row.kind == "VIEW" || row.kind == "MATERIALIZED VIEW");
    let columns: Vec<CatalogColumn> = rows
        .iter()
        .filter_map(|row| row.column.as_ref())
        .map(|column| {
            // L'identita: il tipo con il suo proprietario, per un tipo
            // dell'utente; il nome predefinito altrimenti (regola B).
            let identity = column.data_type_owner.as_deref().map_or_else(
                || column.data_type.clone(),
                |owner| format!("{owner}.{}", column.data_type),
            );
            let catalog = plenora_database_core::cast_table::CatalogType {
                name: column.data_type.clone(),
                length: column.length,
                precision: column.precision,
                scale: column.scale,
                ..plenora_database_core::cast_table::CatalogType::default()
            };
            CatalogColumn::new(
                column.name.clone(),
                ColumnOrigin::of(derived, column.virtual_column),
                column_family(&column.data_type, column.data_type_owner.as_deref()),
            )
            .with_type(catalog, &identity)
        })
        .collect();
    if columns.is_empty() {
        RelationColumns::Unknown
    } else {
        RelationColumns::Known(columns)
    }
}

/// Il catalogo letto su una connessione del driver.
pub struct ConnectionCatalog<'c> {
    connection: &'c oracle_rs::Connection,
    timeout: std::time::Duration,
    cancellation: &'c plenora_database_core::CancellationToken,
}

impl<'c> ConnectionCatalog<'c> {
    pub const fn new(
        connection: &'c oracle_rs::Connection,
        timeout: std::time::Duration,
        cancellation: &'c plenora_database_core::CancellationToken,
    ) -> Self {
        Self {
            connection,
            timeout,
            cancellation,
        }
    }

    async fn rows(
        &self,
        sql: &str,
        parameters: &[&str],
    ) -> Result<Vec<plenora_database_core::row::Row>> {
        use plenora_database_core::provider::ParameterValue;
        let parameters = crate::parameter::bind_parameters(
            &parameters
                .iter()
                .map(|value| ParameterValue::String((*value).to_owned()))
                .collect::<Vec<_>>(),
        )?;
        let mut result = crate::connection::with_timeout_duration(
            self.timeout,
            ErrorPhase::Prepare,
            self.cancellation,
            self.connection.query(sql, &parameters),
        )
        .await?;
        while result.has_more_rows {
            let page = crate::connection::with_timeout_duration(
                self.timeout,
                ErrorPhase::Prepare,
                self.cancellation,
                self.connection
                    .fetch_more(result.cursor_id, &result.columns, 256),
            )
            .await?;
            result.rows.extend(page.rows);
            result.cursor_id = page.cursor_id;
            result.has_more_rows = page.has_more_rows;
        }
        crate::decode::rows_from_result(
            self.connection,
            self.timeout,
            ErrorPhase::Prepare,
            self.cancellation,
            result,
        )
        .await
    }
}

const COLUMN_TYPES_SQL: &str = "SELECT COLUMN_NAME, DATA_TYPE, TO_CHAR(CHAR_LENGTH), \
    TO_CHAR(DATA_LENGTH), TO_CHAR(DATA_PRECISION), TO_CHAR(DATA_SCALE), CHAR_USED, \
    (SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET') \
    FROM ALL_TAB_COLUMNS \
    WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID";
const COLUMN_TYPES_CURRENT_SQL: &str = "SELECT COLUMN_NAME, DATA_TYPE, TO_CHAR(CHAR_LENGTH), \
    TO_CHAR(DATA_LENGTH), TO_CHAR(DATA_PRECISION), TO_CHAR(DATA_SCALE), CHAR_USED, \
    (SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET') \
    FROM ALL_TAB_COLUMNS \
    WHERE OWNER = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND TABLE_NAME = :1 \
    ORDER BY COLUMN_ID";

impl ConnectionCatalog<'_> {
    /// I tipi delle colonne di una relazione (`ALL_TAB_COLUMNS`), con
    /// lunghezza, precisione e scala, per l'ammissione dei letterali e dei
    /// filtri (`plenora_database_core::operand_admission`). `None` per una
    /// relazione che il catalogo non conosce.
    ///
    /// # Errors
    ///
    /// Gli errori del catalogo.
    pub async fn column_types(
        &self,
        owner: Option<&str>,
        name: &str,
    ) -> Result<Option<plenora_database_core::operand_admission::RelationTypes>> {
        use plenora_database_core::operand_admission::{CatalogType, Charset, LengthUnit};
        let rows = match owner {
            Some(owner) => self.rows(COLUMN_TYPES_SQL, &[owner, name]).await?,
            None => self.rows(COLUMN_TYPES_CURRENT_SQL, &[name]).await?,
        };
        if rows.is_empty() {
            return Ok(None);
        }
        let number = |row, index| -> Result<Option<u64>> {
            text(row, index)?
                .map(|value| {
                    value
                        .trim()
                        .parse::<u64>()
                        .map_err(|_| malformed("lunghezza del catalogo Oracle non numerica"))
                })
                .transpose()
        };
        rows.iter()
            .map(|row| {
                let data_type = required(row, 1, "colonna Oracle senza tipo")?;
                let upper = data_type.to_ascii_uppercase();
                let national = upper.starts_with('N');
                // `VARCHAR2(n BYTE)` conta i byte (`DATA_LENGTH`), `n CHAR`
                // i caratteri (`CHAR_LENGTH`); le forme nazionali in
                // AL16UTF16 contano le unita UTF-16, `RAW` i byte.
                let (length, length_unit) = match (upper.as_str(), text(row, 6)?.as_deref()) {
                    ("RAW", _) => (number(row, 3)?, LengthUnit::Bytes),
                    _ if national => (number(row, 2)?, LengthUnit::Utf16),
                    (_, Some("B")) => (number(row, 3)?, LengthUnit::Bytes),
                    _ => (number(row, 2)?, LengthUnit::Characters),
                };
                // Il charset del database per le forme del database; le
                // forme nazionali sono AL16UTF16 (il fork rifiuta le altre).
                let charset = match text(row, 7)?.as_deref() {
                    _ if national => Charset::Unicode,
                    Some("AL32UTF8" | "UTF8") => Charset::Unicode,
                    Some("WE8MSWIN1252") => Charset::Windows1252,
                    _ => Charset::Ascii,
                };
                let scale = text(row, 5)?
                    .map(|value| {
                        value
                            .trim()
                            .parse::<i64>()
                            .map_err(|_| malformed("scala del catalogo Oracle non numerica"))
                    })
                    .transpose()?;
                Ok((
                    required(row, 0, "colonna Oracle senza nome")?,
                    CatalogType {
                        name: data_type,
                        declaration: None,
                        length,
                        precision: number(row, 4)?,
                        scale,
                        datetime_precision: None,
                        length_unit,
                        charset,
                    },
                ))
            })
            .collect::<Result<_>>()
            .map(Some)
    }
}

fn text(row: &plenora_database_core::row::Row, index: usize) -> Result<Option<String>> {
    use plenora_database_core::provider::ParameterValue;
    match row.get_index(index) {
        Some(ParameterValue::String(value)) => Ok(Some(value.clone())),
        Some(ParameterValue::Null { .. }) => Ok(None),
        _ => Err(malformed("campo del catalogo Oracle non testuale")),
    }
}

fn required(
    row: &plenora_database_core::row::Row,
    index: usize,
    message: &'static str,
) -> Result<String> {
    text(row, index)?.ok_or_else(|| malformed(message))
}

/// Un numero del catalogo reso con `TO_CHAR`; `None` per un `NULL`.
fn number<T: std::str::FromStr>(
    row: &plenora_database_core::row::Row,
    index: usize,
) -> Result<Option<T>> {
    text(row, index)?
        .map(|value| {
            value
                .trim()
                .parse()
                .map_err(|_| malformed("numero del catalogo Oracle non valido"))
        })
        .transpose()
}

fn malformed(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Protocol,
        ErrorPhase::Prepare,
        Some(ProviderKind::Oracle),
        message,
    )
}

impl OracleCatalog for ConnectionCatalog<'_> {
    async fn relation(&mut self, owner: Option<&str>, name: &str) -> Result<Vec<ObjectRow>> {
        let rows = match owner {
            Some(owner) => self.rows(RELATION_SQL, &[owner, name]).await?,
            None => self.rows(RELATION_CURRENT_SQL, &[name]).await?,
        };
        rows.iter()
            .map(|row| {
                let column = match text(row, 2)? {
                    Some(column) => Some(ColumnRow {
                        name: column,
                        data_type: required(row, 3, "colonna Oracle senza tipo")?,
                        data_type_owner: text(row, 4)?,
                        virtual_column: match required(
                            row,
                            5,
                            "colonna Oracle senza VIRTUAL_COLUMN",
                        )?
                        .as_str()
                        {
                            "YES" => true,
                            "NO" => false,
                            _ => return Err(malformed("VIRTUAL_COLUMN Oracle non riconosciuta")),
                        },
                        length: number(row, 6)?,
                        precision: number(row, 7)?,
                        scale: number(row, 8)?,
                    }),
                    None => None,
                };
                Ok(ObjectRow {
                    owner: required(row, 0, "oggetto Oracle senza proprietario")?,
                    kind: required(row, 1, "oggetto Oracle senza tipo")?,
                    column,
                })
            })
            .collect()
    }

    async fn synonym(&mut self, owner: &str, name: &str) -> Result<Option<SynonymRow>> {
        let rows = self.rows(SYNONYM_SQL, &[owner, name]).await?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => {
                let target = if text(row, 2)?.is_some() {
                    None
                } else {
                    Some((
                        required(row, 0, "sinonimo Oracle senza proprietario")?,
                        required(row, 1, "sinonimo Oracle senza oggetto")?,
                    ))
                };
                Ok(Some(SynonymRow { target }))
            }
            _ => Err(malformed("sinonimo Oracle non univoco nel catalogo")),
        }
    }
}
