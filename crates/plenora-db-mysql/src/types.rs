use crate::{MysqlColumn, MysqlObjectDescription};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::plan::{FilterExpression, ReadOperation, SortDirection};
use plenora_database_core::protocol::{self, contract_schema};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    lower_filter, select_columns_by_name, Dialect, DialectCapabilities, Expression, FilterLowering,
    Identifier, ObjectName, Renderer,
};
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MysqlColumnKind {
    Bool,
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Utf8,
    Binary,
    Date,
    Time,
    Timestamp,
    Decimal { precision: u8, scale: i8 },
    Geometry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlColumnSpec {
    pub name: String,
    pub native_type: String,
    pub native_declaration: String,
    pub nullable: bool,
    pub collation: Option<String>,
    pub kind: MysqlColumnKind,
    pub spatial_srid: Option<u32>,
    /// L'SRID viene dal **piano**, non dal catalogo.
    ///
    /// Cambia cosa il provider deve fare, non cosa pubblica: un SRID di
    /// catalogo e vincolato dalla DDL e vale per costruzione, uno dichiarato
    /// dal chiamante e un'ipotesi finche qualcuno non la verifica. Le due
    /// cose finiscono nello stesso campo — il contratto `GeoArrow` pubblica un
    /// CRS e basta — ma la seconda obbliga la lettura a controllare ogni
    /// valore, e questa bandiera e cio che glielo ricorda.
    pub spatial_srid_declared: bool,
}

/// Un controllo di CRS da eseguire riga per riga.
///
/// Il valore arriva in una colonna che il chiamante non ha chiesto e che non
/// comparira nello schema Arrow: e `ST_SRID` della geometria, proiettata in
/// coda alle colonne visibili. Portarla in coda e cio che rende il controllo
/// possibile senza cambiare gli indici di tutto il resto — il decoder itera
/// sulle colonne del piano, quindi cio che sta oltre non lo tocca.
#[derive(Debug, Clone)]
pub struct MysqlCrsCheck {
    /// La posizione della colonna `ST_SRID` nel result set.
    pub result_index: usize,
    /// La colonna geometrica di cui e il CRS.
    pub column: String,
    /// Cio che il piano ha dichiarato, e che ogni valore deve confermare.
    pub expected: u32,
}

#[derive(Debug, Clone)]
pub struct MysqlReadPlan {
    pub columns: Vec<MysqlColumnSpec>,
    /// I CRS dichiarati dal piano, da verificare su ogni riga.
    ///
    /// Vuoto quando nessuna colonna ne ha bisogno, che e il caso di ogni
    /// prodotto il cui catalogo l'SRID lo sa.
    pub crs_checks: Vec<MysqlCrsCheck>,
    pub schema: SchemaRef,
    pub sql: String,
    pub bind_names: Vec<String>,
    pub schema_token: String,
}

impl MysqlReadPlan {
    /// Compila un piano di lettura senza interpolare valori nel testo SQL.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per tipi, colonne, identificatori o filtri non
    /// rappresentabili. Un limite senza ordinamento esplicito e rifiutato.
    pub fn compile(
        description: &MysqlObjectDescription,
        operation: &ReadOperation,
    ) -> Result<Self> {
        Self::compile_with_profile(description, operation, &crate::profile::MYSQL_PROFILE)
    }

    /// Il piano di lettura, con il profilo che decide la parte spatial.
    ///
    /// # Errors
    ///
    /// Come `compile`.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn compile_with_profile(
        description: &MysqlObjectDescription,
        operation: &ReadOperation,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<Self> {
        // Vedi `MysqlWritePlan::compile_with_profile`: chi ha il profilo
        // attribuisce da se.
        crate::profile::attributed(
            profile,
            Self::compile_unattributed(description, operation, profile),
        )
    }

    /// Lunga di una riga oltre la soglia da quando rende anche la finestra, e
    /// resta intera: e la compilazione di un piano di lettura, dove le
    /// clausole si scrivono nell'ordine in cui il dialetto le vuole. Spezzarla
    /// per il conteggio separerebbe l'ordine dal posto in cui si legge.
    #[allow(clippy::too_many_lines)]
    fn compile_unattributed(
        description: &MysqlObjectDescription,
        operation: &ReadOperation,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<Self> {
        let product = profile.product();
        if description.columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                format!("oggetto {product} privo di colonne leggibili"),
            ));
        }
        // La finestra vale quanto il tetto: senza un ordinamento esplicito due
        // letture consecutive possono rendere righe diverse, e un offset su un
        // risultato non ordinato non e riproducibile nemmeno in linea di
        // principio.
        if (operation.row_limit.is_some() || operation.row_offset.is_some())
            && operation.order_by.is_empty()
        {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                format!(
                    "LIMIT e OFFSET {product} richiedono ORDER BY esplicito \
                     per un risultato deterministico"
                ),
            ));
        }
        let renderer = mysql_renderer();
        let declared = resolve_declared_crs(description, operation, profile)?;
        let available = description
            .columns
            .iter()
            .map(|column| {
                MysqlColumnSpec::from_catalog_declaring(
                    column,
                    profile,
                    declared.get(column.name.as_str()).copied(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let columns = select_columns(&available, &operation.projection)?;
        let mut projections = columns
            .iter()
            .map(|column| column.projection(&renderer, profile))
            .collect::<Result<Vec<_>>>()?;
        // Le colonne del controllo vanno **in coda**, dopo tutte le visibili.
        // E' cio che rende il controllo invisibile al resto: il decoder itera
        // sulle colonne del piano, e cio che sta oltre non lo tocca — nessun
        // indice cambia, nessun campo Arrow compare, e uno schema pubblicato
        // resta identico a quello di una lettura senza dichiarazioni.
        let mut crs_checks = Vec::new();
        for column in &columns {
            if !column.spatial_srid_declared {
                continue;
            }
            let expected = column.spatial_srid.ok_or_else(|| {
                prepare_error(
                    ErrorCategory::Crs,
                    format!("colonna spatial {product} dichiarata senza SRID"),
                )
            })?;
            let quoted = renderer.quote_identifier(&mysql_identifier(&column.name)?)?;
            crs_checks.push(MysqlCrsCheck {
                result_index: projections.len(),
                column: column.name.clone(),
                expected,
            });
            projections.push(profile.geometry_srid_projection(&quoted));
        }
        let projection = projections.join(", ");
        let object = ObjectName {
            catalog: None,
            schema: Some(mysql_identifier(&description.schema)?),
            object: mysql_identifier(&description.name)?,
        };
        let mut sql = format!(
            "SELECT {projection} FROM {}",
            renderer.quote_object(&object)?
        );
        let mut bind_names = Vec::new();
        if let Some(filter) = &operation.filter {
            ensure_filter_columns(filter, &available)?;
            let rendered_filter = renderer.render_filter(&convert_filter(filter)?)?;
            sql.push_str(" WHERE ");
            sql.push_str(&rendered_filter.sql);
            bind_names.extend(rendered_filter.binds.into_iter().map(|bind| bind.name));
        }
        if !operation.order_by.is_empty() {
            let available_names = available
                .iter()
                .map(|column| column.name.as_str())
                .collect::<BTreeSet<_>>();
            let ordering = operation
                .order_by
                .iter()
                .map(|order| {
                    if !available_names.contains(order.field.as_str()) {
                        return Err(prepare_error(
                            ErrorCategory::NotFound,
                            format!("colonna ORDER BY {product} non trovata"),
                        ));
                    }
                    let field = renderer.quote_identifier(&mysql_identifier(&order.field)?)?;
                    let direction = match order.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    };
                    Ok(format!("{field} {direction}"))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(" ORDER BY ");
            sql.push_str(&ordering.join(", "));
        }
        // `OFFSET` senza `LIMIT` non e sintassi valida su questi motori, e il
        // tetto massimo e la forma che il dialetto accetta per dire «da qui in
        // poi, tutto». Il valore non arriva dal piano: e il limite del tipo, e
        // metterlo qui e diverso dal dichiarare un limite che il chiamante non
        // ha chiesto — chi legge il SQL vede che la finestra e aperta.
        match (operation.row_limit, operation.row_offset) {
            (Some(limit), _) => {
                sql.push_str(" LIMIT ");
                sql.push_str(&limit.to_string());
            }
            (None, Some(_)) => {
                sql.push_str(" LIMIT 18446744073709551615");
            }
            (None, None) => {}
        }
        if let Some(offset) = operation.row_offset {
            sql.push_str(" OFFSET ");
            sql.push_str(&offset.to_string());
        }
        sql.push(';');
        let schema = contract_schema(
            columns
                .iter()
                .map(|column| column.arrow_field_with_profile(profile))
                .collect(),
        );
        Ok(Self {
            columns,
            crs_checks,
            schema,
            sql,
            bind_names,
            schema_token: description.token.0.clone(),
        })
    }

    /// Costruisce il piano di una `QueryOperation` dai metadati del prepared
    /// statement, che sono l'unica descrizione autoritativa dell'output.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso quando il result set non espone colonne.
    /// Il piano di una query, con le geometrie calcolate che il piano descrive.
    ///
    /// Il tipo wire di una geometria calcolata e BLOB, perche il renderer la
    /// incapsula nella stessa forma che
    /// [`crate::profile::ProductProfile::geometry_projection`] scrive per una
    /// colonna, e nessuna ispezione dei metadati la distingue da un blob
    /// qualunque. Cio che la distingue e la posizione nella projection, ed e la
    /// ragione per cui questa funzione riceve il piano e non solo le colonne.
    ///
    /// # Errors
    ///
    /// `Schema` per un result set vuoto o piu corto di cio che il piano
    /// descrive, e `Crs` quando la colonna in quella posizione non ha la forma
    /// che l'involucro avrebbe prodotto — che significherebbe che il renderer e
    /// il provider non stanno parlando della stessa query.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn from_query_columns_with_geometry(
        sql: String,
        bind_names: Vec<String>,
        mut columns: Vec<MysqlColumnSpec>,
        plan: &crate::query::MysqlRenderedQuery,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<Self> {
        if columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "QueryOperation priva di colonne risultanti",
            ));
        }
        let product = profile.product();
        for geometry in &plan.geometries {
            let column = columns.get_mut(geometry.result_index).ok_or_else(|| {
                prepare_error(
                    ErrorCategory::Schema,
                    format!("result set {product} piu corto della projection del piano"),
                )
            })?;
            if column.kind != MysqlColumnKind::Binary {
                return Err(prepare_error(
                    ErrorCategory::Crs,
                    format!(
                        "geometria calcolata {product} non incapsulata: il piano e il renderer \
                         descrivono result set diversi"
                    ),
                ));
            }
            column.kind = MysqlColumnKind::Geometry;
            "geometry".clone_into(&mut column.native_type);
            column.spatial_srid = Some(geometry.srid);
            // Dichiarato, non letto da un catalogo: e cio che il piano ha detto
            // e che ogni riga deve confermare.
            column.spatial_srid_declared = true;
        }
        // Le colonne di controllo stanno in coda e non appartengono al
        // chiamante: lo schema pubblicato si ferma dove finisce la sua
        // projection.
        if plan.visible_columns < columns.len() {
            columns.truncate(plan.visible_columns);
        }
        let schema = contract_schema(
            columns
                .iter()
                .map(|column| column.arrow_field_with_profile(profile))
                .collect(),
        );
        Ok(Self {
            columns,
            crs_checks: plan.crs_checks.clone(),
            schema,
            sql,
            bind_names,
            schema_token: QUERY_RESULT_SCHEMA_TOKEN.to_owned(),
        })
    }
}

/// Le dichiarazioni di CRS del piano, verificate contro il catalogo.
///
/// Verificate **prima** che diventino un SRID, perche una dichiarazione
/// sbagliata e piu pericolosa di una assente: l'assenza fa fallire la lettura,
/// l'errore la fa riuscire pubblicando un CRS che nessuno ha controllato.
///
/// Tre rifiuti, e ciascuno nomina una cosa diversa:
///
/// * la colonna non esiste — un nome sbagliato non deve passare per una
///   dichiarazione che non serviva;
/// * la colonna non e geometrica — un CRS su un `BIGINT` non e un rinforzo, e
///   un malinteso su cosa contiene quella tabella;
/// * il catalogo la descrive gia — due fonti per lo stesso fatto sono una
///   fonte di troppo, e quando divergono nessuna delle due e piu quella
///   giusta.
fn resolve_declared_crs<'a>(
    description: &MysqlObjectDescription,
    operation: &'a ReadOperation,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<std::collections::BTreeMap<&'a str, u32>> {
    let product = profile.product();
    let mut declared = std::collections::BTreeMap::new();
    for declaration in &operation.declared_crs {
        let Some(column) = description
            .columns
            .iter()
            .find(|candidate| candidate.name == declaration.column)
        else {
            return Err(prepare_error(
                ErrorCategory::NotFound,
                format!("declared_crs nomina una colonna che l'oggetto {product} non ha"),
            ));
        };
        if !profile.is_spatial_native_type(&column.data_type.to_ascii_lowercase()) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                format!("declared_crs su una colonna {product} non geometrica"),
            ));
        }
        if column.spatial_srid.is_some() {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                format!(
                    "declared_crs su una colonna {product} il cui SRID il catalogo \
                     gia descrive"
                ),
            ));
        }
        if declared
            .insert(declaration.column.as_str(), declaration.srid)
            .is_some()
        {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "declared_crs dichiara due volte la stessa colonna",
            ));
        }
    }
    Ok(declared)
}

/// Il token strutturale di una query non e un token di catalogo: l'unica
/// verifica successiva possibile e il ricontrollo dei metadati di riga.
const QUERY_RESULT_SCHEMA_TOKEN: &str = "mysql-query-result-metadata-v1";

impl MysqlColumnSpec {
    /// Traduce una colonna del catalogo nel sottoinsieme Arrow supportato.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per tipi sconosciuti, decimal oltre 128 bit e profili
    /// spatial senza SRID dichiarato.
    pub fn from_catalog(column: &MysqlColumn) -> Result<Self> {
        Self::from_catalog_with_profile(column, &crate::profile::MYSQL_PROFILE)
    }

    /// La colonna del catalogo, senza dichiarazioni dal piano.
    ///
    /// # Errors
    ///
    /// Come `from_catalog`.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn from_catalog_with_profile(
        column: &MysqlColumn,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<Self> {
        Self::from_catalog_declaring(column, profile, None)
    }

    /// La colonna del catalogo, con il profilo e l'eventuale CRS dichiarato.
    ///
    /// `declared` vale soltanto dove il catalogo l'SRID non ce l'ha, e la
    /// verifica che sia vero spetta a chi compila il piano: qui il campo
    /// arriva gia risolto, perche una colonna non sa da sola se il chiamante
    /// aveva il diritto di parlarne.
    ///
    /// # Errors
    ///
    /// Come `from_catalog`.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn from_catalog_declaring(
        column: &MysqlColumn,
        profile: &dyn crate::profile::ProductProfile,
        declared: Option<u32>,
    ) -> Result<Self> {
        let product = profile.product();
        let native_type = column.data_type.to_ascii_lowercase();
        let native_declaration = column.native_declaration.to_ascii_lowercase();
        let unsigned = native_declaration
            .split_ascii_whitespace()
            .any(|part| part == "unsigned");
        let kind = match native_type.as_str() {
            "tinyint" if native_declaration.starts_with("tinyint(1)") && !unsigned => {
                MysqlColumnKind::Bool
            }
            "tinyint" if unsigned => MysqlColumnKind::U8,
            "tinyint" => MysqlColumnKind::I8,
            "smallint" if unsigned => MysqlColumnKind::U16,
            "smallint" | "year" => MysqlColumnKind::I16,
            "mediumint" | "int" | "integer" if unsigned => MysqlColumnKind::U32,
            "mediumint" | "int" | "integer" => MysqlColumnKind::I32,
            "bigint" if unsigned => MysqlColumnKind::U64,
            "bigint" => MysqlColumnKind::I64,
            "float" => MysqlColumnKind::F32,
            "double" | "real" => MysqlColumnKind::F64,
            "decimal" | "numeric" => decimal_kind(column)?,
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum"
            | "set" | "json" => MysqlColumnKind::Utf8,
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
                MysqlColumnKind::Binary
            }
            "date" => MysqlColumnKind::Date,
            "time" => MysqlColumnKind::Time,
            "datetime" | "timestamp" => MysqlColumnKind::Timestamp,
            spatial if profile.is_spatial_native_type(spatial) => {
                // Tre casi, non due. Il catalogo lo sa: si usa quello. Il
                // catalogo non lo sa e il piano lo dichiara: si usa il piano,
                // e la lettura dovra verificarlo. Nessuno dei due: rifiuto,
                // che resta l'unica risposta onesta — il contratto GeoArrow
                // pubblica un CRS, e pubblicarlo senza saperlo e peggio del
                // rifiuto.
                if profile.spatial_requires_declared_srid()
                    && column.spatial_srid.is_none()
                    && declared.is_none()
                {
                    return Err(prepare_error(
                        ErrorCategory::Crs,
                        format!(
                            "colonna spatial {product} senza SRID: il catalogo tace e il piano non lo dichiara"
                        ),
                    ));
                }
                MysqlColumnKind::Geometry
            }
            _ => {
                return Err(prepare_error(
                    ErrorCategory::Unsupported,
                    format!("tipo {product} non supportato: {native_type}"),
                ));
            }
        };
        // Il catalogo ha la precedenza dove parla: una dichiarazione su una
        // colonna che il catalogo sa gia descrivere e rifiutata da chi compila
        // il piano, quindi qui `declared` e presente solo dove l'altro tace.
        let (spatial_srid, spatial_srid_declared) = match (column.spatial_srid, declared) {
            (Some(catalog), _) => (Some(catalog), false),
            (None, Some(plan)) => (Some(plan), true),
            (None, None) => (None, false),
        };
        Ok(Self {
            name: column.name.clone(),
            native_type,
            native_declaration,
            nullable: column.nullable,
            collation: column.collation.clone(),
            kind,
            spatial_srid,
            spatial_srid_declared,
        })
    }

    fn projection(
        &self,
        renderer: &Renderer,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<String> {
        let identifier = mysql_identifier(&self.name)?;
        let quoted = renderer.quote_identifier(&identifier)?;
        if self.kind == MysqlColumnKind::Geometry {
            Ok(profile.geometry_projection(&quoted))
        } else {
            Ok(quoted)
        }
    }

    /// Il campo Arrow di questa colonna, con i metadata di `MySQL`.
    ///
    /// Resta l'API pubblica del crate, che serve `MySQL`: il namespace dei
    /// metadata e il suo. La variante con il profilo e cio che un secondo
    /// prodotto usa, ed e da li che passano tutti i percorsi interni.
    #[must_use]
    pub fn arrow_field(&self) -> Field {
        self.arrow_field_with_profile(&crate::profile::MYSQL_PROFILE)
    }

    /// Come [`Self::arrow_field`], con il namespace del prodotto che ha
    /// risposto.
    #[must_use]
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn arrow_field_with_profile(
        &self,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Field {
        let keys = profile.metadata_keys();
        let data_type = match self.kind {
            MysqlColumnKind::Bool => DataType::Boolean,
            MysqlColumnKind::I8 => DataType::Int8,
            MysqlColumnKind::U8 => DataType::UInt8,
            MysqlColumnKind::I16 => DataType::Int16,
            MysqlColumnKind::U16 => DataType::UInt16,
            MysqlColumnKind::I32 => DataType::Int32,
            MysqlColumnKind::U32 => DataType::UInt32,
            MysqlColumnKind::I64 => DataType::Int64,
            MysqlColumnKind::U64 => DataType::UInt64,
            MysqlColumnKind::F32 => DataType::Float32,
            MysqlColumnKind::F64 => DataType::Float64,
            // MySQL TIME rappresenta anche durate negative fino a 838 ore e
            // non e semanticamente equivalente ad Arrow Time64.
            MysqlColumnKind::Utf8 | MysqlColumnKind::Time => DataType::Utf8,
            MysqlColumnKind::Binary | MysqlColumnKind::Geometry => DataType::Binary,
            MysqlColumnKind::Date => DataType::Date32,
            MysqlColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            MysqlColumnKind::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
        };
        let mut metadata = HashMap::from([(keys.native_type.to_owned(), self.native_type.clone())]);
        if !self.native_declaration.is_empty() {
            metadata.insert(
                keys.native_declaration.to_owned(),
                self.native_declaration.clone(),
            );
        }
        if let Some(collation) = &self.collation {
            metadata.insert(keys.collation.to_owned(), collation.clone());
        }
        if self.kind == MysqlColumnKind::Geometry {
            metadata.extend([
                (
                    protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                    "geoarrow.wkb".to_owned(),
                ),
                (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
                (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
                (
                    protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                    if self.native_type == "geometry" {
                        "mixed".to_owned()
                    } else {
                        "exact".to_owned()
                    },
                ),
                (
                    protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                    "geometry".to_owned(),
                ),
                (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
                (
                    protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                    "declared_unresolved".to_owned(),
                ),
            ]);
            if let Some(srid) = self.spatial_srid {
                metadata.insert(protocol::GEOMETRY_SRID.to_owned(), srid.to_string());
            }
            if self.native_type != "geometry" {
                metadata.insert(
                    protocol::GEOMETRY_TYPES.to_owned(),
                    canonical_geometry_type(&self.native_type).to_owned(),
                );
            }
        }
        Field::new(&self.name, data_type, self.nullable).with_metadata(metadata)
    }
}

fn canonical_geometry_type(native_type: &str) -> &str {
    if native_type == "geomcollection" {
        "geometrycollection"
    } else {
        native_type
    }
}

fn decimal_kind(column: &MysqlColumn) -> Result<MysqlColumnKind> {
    let precision = column
        .numeric_precision
        .ok_or_else(|| prepare_error(ErrorCategory::DataMapping, "decimal senza precisione"))?;
    let scale = column
        .numeric_scale
        .ok_or_else(|| prepare_error(ErrorCategory::DataMapping, "decimal senza scala"))?;
    let precision = u8::try_from(precision).map_err(|_| {
        prepare_error(
            ErrorCategory::Unsupported,
            "precisione decimal non rappresentabile",
        )
    })?;
    let scale = i8::try_from(scale).map_err(|_| {
        prepare_error(
            ErrorCategory::Unsupported,
            "scala decimal non rappresentabile",
        )
    })?;
    if precision == 0 || precision > 38 || scale < 0 || scale > precision.cast_signed() {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "decimal oltre Decimal128 Arrow",
        ));
    }
    Ok(MysqlColumnKind::Decimal { precision, scale })
}

fn select_columns(
    available: &[MysqlColumnSpec],
    projection: &[String],
) -> Result<Vec<MysqlColumnSpec>> {
    select_columns_by_name(
        available,
        projection,
        |column| column.name.as_str(),
        || prepare_error(ErrorCategory::NotFound, "colonna projection non trovata"),
    )
}

fn convert_filter(expression: &FilterExpression) -> Result<Expression> {
    lower_filter(
        expression,
        FilterLowering {
            provider: crate::profile::PROVISIONAL_KIND,
            case_insensitive_like: false,
            spatial: false,
        },
        mysql_identifier,
    )
}

fn ensure_filter_columns(expression: &FilterExpression, columns: &[MysqlColumnSpec]) -> Result<()> {
    let available = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if expression.all_fields(&|field| available.contains(field)) {
        Ok(())
    } else {
        Err(prepare_error(
            ErrorCategory::NotFound,
            "colonna filtro non trovata",
        ))
    }
}

pub fn mysql_identifier(value: &str) -> Result<Identifier> {
    if value.chars().count() > crate::MAX_IDENTIFIER_CHARACTERS {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "identificatore oltre 64 caratteri",
        ));
    }
    Identifier::new(value.to_owned())
}

pub const fn mysql_renderer() -> Renderer {
    Renderer::new(
        Dialect::Mysql,
        DialectCapabilities {
            // Abilita predicati e funzioni scalari dell'AST spatial.
            // Il rendering usa i nomi `ST_*` e
            // `ST_GeomFromWKB` per i parametri geometry.
            spatial_intersects: true,
        },
    )
}

fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
