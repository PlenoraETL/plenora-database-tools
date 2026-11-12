//! Registro unico delle regole di rischio. Non abilita capability o semantiche provider.
use crate::loss::{LossCategory, LossSeverity, MappingLoss};
use crate::plan::ProviderKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applicability {
    Postgres,
    Sqlserver,
    Shared,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassificationMode {
    Exact,
    Conservative,
    Mixed,
    HardReject,
    NoChange,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PredicateId {
    BinaryLimit,
    Conservative,
    EmptyToNull,
    FractionalSeconds,
    JsonNumbers,
    NegativeZero,
    NoChange,
    NonAscii,
    PadBinary,
    PadText,
    PgJson,
    PgNumeric,
    PgTemporal,
    PgText,
    SsEncoding,
    SsOffset,
    SsTemporal,
    SsText,
    StripTrailing,
    Underflow,
}
impl PredicateId {
    pub const ALL: &'static [Self] = &[
        Self::BinaryLimit,
        Self::Conservative,
        Self::EmptyToNull,
        Self::FractionalSeconds,
        Self::JsonNumbers,
        Self::NegativeZero,
        Self::NoChange,
        Self::NonAscii,
        Self::PadBinary,
        Self::PadText,
        Self::PgJson,
        Self::PgNumeric,
        Self::PgTemporal,
        Self::PgText,
        Self::SsEncoding,
        Self::SsOffset,
        Self::SsTemporal,
        Self::SsText,
        Self::StripTrailing,
        Self::Underflow,
    ];
}
#[derive(Debug, Clone, Copy)]
pub struct RuleDefinition {
    pub id: RuleId,
    pub applicability: Applicability,
    pub category: LossCategory,
    pub severity: LossSeverity,
    pub reason: &'static str,
    pub selector: RuleId,
    pub predicate: PredicateId,
    pub mode: ClassificationMode,
    pub postgres_alias: Option<RuleId>,
}
macro_rules! rules {
    ($(($variant:ident, $wire:literal, $provider:ident, $category:ident, $severity:ident, $predicate:ident, $mode:ident, $alias:expr, $reason:literal)),* $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        pub enum RuleId { $(#[serde(rename = $wire)] $variant),* }
        impl RuleId {
            pub const ALL: &'static [Self] = &[$(Self::$variant),*];
            #[must_use]
            pub const fn as_str(self) -> &'static str { match self { $(Self::$variant => $wire),* } }
            #[must_use]
            pub const fn definition(self) -> RuleDefinition {
                match self { $(Self::$variant => RuleDefinition {
                    id: self, applicability: Applicability::$provider, category: LossCategory::$category,
                    severity: LossSeverity::$severity, reason: $reason, selector: self,
                    predicate: PredicateId::$predicate, mode: ClassificationMode::$mode, postgres_alias: $alias,
                }),* }
            }
        }
    };
}
rules! {
    (PgAddNullable, "pg.add_nullable", Postgres, NativeType, Information, NoChange, NoChange, None, "colonna aggiunta al target come nullable"),
    (PgArrowMismatch, "pg.arrow_mismatch", Postgres, NativeType, DataLoss, Conservative, Conservative, None, "tipo Arrow non equivalente al target PostgreSQL"),
    (PgCrs, "pg.crs", Postgres, Crs, DataLoss, Conservative, Conservative, None, "identificatore CRS sorgente e target differente"),
    (PgJsonb, "pg.jsonb", Postgres, NativeType, Warning, PgJson, Exact, None, "jsonb tiene l'ultimo valore di una chiave ripetuta e normalizza ordine delle chiavi e spazi"),
    (PgMissingColumn, "pg.missing_column", Postgres, NativeType, DataLoss, Conservative, Conservative, None, "colonna sorgente assente nel target"),
    (PgNullability, "pg.nullability", Postgres, Nullability, DataLoss, NoChange, NoChange, None, "sorgente nullable verso colonna NOT NULL"),
    (PgNumeric, "pg.numeric", Postgres, Precision, DataLoss, PgNumeric, Mixed, None, "un numero del tipo della colonna (numeric con scala, real, double precision) arrotonderebbe il valore testuale"),
    (PgPadText, "pg.pad_text", Postgres, NativeType, DataLoss, PadText, Exact, None, "regola S: il tipo a lunghezza fissa completa con spazi un testo piu corto"),
    (PgSrid, "pg.srid", Postgres, Srid, DataLoss, Conservative, Conservative, None, "SRID sorgente e target differenti"),
    (PgTemporal, "pg.temporal", Postgres, Precision, DataLoss, PgTemporal, Mixed, None, "precisione temporale della colonna sotto il microsecondo: il valore si arrotonderebbe o troncherebbe"),
    (PgTextLimit, "pg.text_limit", Postgres, Precision, DataLoss, PgText, HardReject, None, "il contenuto oltre la lunghezza della colonna si troncherebbe, inclusi caratteri non spazio"),
    (SsAddNullable, "ss.add_nullable", Sqlserver, NativeType, Information, NoChange, NoChange, None, "colonna aggiunta al target SQL Server come nullable"),
    (SsBinaryLimit, "ss.binary_limit", Sqlserver, Precision, DataLoss, BinaryLimit, Exact, None, "il contenuto oltre la lunghezza della colonna si troncherebbe, inclusi byte non nulli"),
    (SsEncoding, "ss.encoding", Sqlserver, Encoding, Warning, SsEncoding, Mixed, None, "i caratteri fuori dalla code page della collation diventerebbero '?'"),
    (SsOffset, "ss.offset", Sqlserver, Precision, Warning, SsOffset, Exact, None, "le cifre frazionarie oltre la scala della colonna si arrotonderebbero"),
    (SsTemporal, "ss.temporal", Sqlserver, Precision, DataLoss, SsTemporal, Exact, None, "precisione temporale della colonna sotto il microsecondo"),
    (SsTextLimit, "ss.text_limit", Sqlserver, Precision, DataLoss, SsText, Exact, None, "il contenuto oltre la lunghezza della colonna si troncherebbe, inclusi caratteri non spazio"),
    (SsXml, "ss.xml", Sqlserver, NativeType, Warning, Conservative, Conservative, None, "xml conserva il documento, non il testo: dichiarazione, apici, forma degli elementi vuoti, sezioni CDATA e riferimenti ai caratteri si normalizzano"),
    (StoreEmptyToNull, "store.empty_to_null", Shared, Nullability, DataLoss, EmptyToNull, Exact, None, "regola S: il server salva come NULL il testo vuoto e il binario vuoto"),
    (StoreEvery, "store.every", Shared, NativeType, DataLoss, Conservative, Conservative, None, "regola S: la misura della coppia di tipi (regola A) ha trovato valori riscritti al salvataggio"),
    (StoreExcessSpaces, "store.excess_spaces", Shared, NativeType, DataLoss, Conservative, Conservative, None, "regola S: la colonna tronca gli spazi finali oltre la propria lunghezza"),
    (StoreFractionalSeconds, "store.fractional_seconds", Shared, Precision, DataLoss, FractionalSeconds, Exact, None, "regola S: il tipo temporale arrotonda o tronca le cifre frazionarie oltre la propria precisione"),
    (StoreJsonNumbers, "store.json_numbers", Shared, Precision, DataLoss, JsonNumbers, Conservative, None, "regola S: il tipo JSON del target riscrive la forma dei numeri fuori dagli interi brevi"),
    (StoreNegativeZero, "store.negative_zero", Shared, Precision, DataLoss, NegativeZero, Exact, None, "regola S: il tipo numerico salva -0.0 come 0"),
    (StoreNonAscii, "store.non_ascii", Shared, Encoding, DataLoss, NonAscii, Conservative, None, "regola S: il charset della colonna non conserva ogni carattere fuori da ASCII"),
    (StorePadBinary, "store.pad_binary", Shared, NativeType, DataLoss, PadBinary, Exact, None, "regola S: il tipo a lunghezza fissa completa con 0x00 un binario piu corto"),
    (StorePadText, "store.pad_text", Shared, NativeType, DataLoss, PadText, Mixed, Some(RuleId::PgPadText), "regola S: il tipo a lunghezza fissa completa con spazi un testo piu corto"),
    (StoreStripTrailing, "store.strip_trailing", Shared, NativeType, DataLoss, StripTrailing, Exact, None, "regola S: la colonna toglie gli spazi finali del testo e gli 0x00 finali del binario"),
    (StoreUnderflow, "store.underflow", Shared, Precision, DataLoss, Underflow, Exact, None, "regola S: il tipo numerico salva come 0 un float sotto il proprio minimo"),
}
impl RuleId {
    #[must_use]
    pub const fn canonical(self, provider: ProviderKind) -> Self {
        if matches!(provider, ProviderKind::Postgres) {
            if let Some(alias) = self.definition().postgres_alias {
                return alias;
            }
        }
        self
    }

    /// Vista v2: non aggiunge campi al contratto storico chiuso.
    #[must_use]
    pub fn legacy(
        self,
        field_id: u32,
        source_type: Option<String>,
        target_type: Option<String>,
    ) -> MappingLoss {
        let rule = self.definition();
        MappingLoss {
            field_id,
            category: rule.category,
            severity: rule.severity,
            reason: rule.reason.to_owned(),
            source_type,
            target_type,
        }
    }
}

/// Parametri del target: non sono nuove regole e non si ricavano da reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuleParameters {
    pub length: Option<u64>,
    pub precision: Option<u8>,
    pub scale: Option<i32>,
    pub text_unit: Option<crate::store_rewrite::TextLengthUnit>,
    pub shape: ValueShape,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ValueShape {
    #[default]
    Scalar,
    Nested,
    Opaque,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleInstance {
    pub field_id: u32,
    pub rule_id: RuleId,
    pub parameters: RuleParameters,
    pub source_type: Option<String>,
    pub target_type: Option<String>,
}
impl RuleInstance {
    #[must_use]
    pub fn risk(&self, policy: crate::loss::MappingPolicy) -> crate::execution_loss::RiskEntry {
        crate::execution_loss::RiskEntry::new(
            self.field_id,
            self.rule_id,
            policy,
            self.source_type.clone(),
            self.target_type.clone(),
        )
    }
}
/// Il piano canonico alimenta sia i controlli sia il rapporto, con gli stessi ID.
/// # Errors
/// Rifiuta alias incompatibili e riserva tutti i contatori prima dell'ingresso.
pub fn compile_instances(
    provider: ProviderKind,
    policy: crate::loss::MappingPolicy,
    instances: impl IntoIterator<Item = RuleInstance>,
) -> crate::Result<(
    Vec<RuleInstance>,
    crate::execution_loss::ExecutionLossReport,
)> {
    let mut controls: std::collections::BTreeMap<_, RuleInstance> =
        std::collections::BTreeMap::new();
    for mut instance in instances {
        let applicable = match instance.rule_id.definition().applicability {
            Applicability::Shared => true,
            Applicability::Postgres => provider == ProviderKind::Postgres,
            Applicability::Sqlserver => provider == ProviderKind::Sqlserver,
        };
        if !applicable {
            return Err(crate::execution_loss::invalid());
        }
        instance.rule_id = instance.rule_id.canonical(provider);
        let key = (instance.field_id, instance.rule_id);
        if let Some(previous) = controls.get_mut(&key) {
            if previous.parameters != instance.parameters
                || [&previous.source_type, &previous.target_type]
                    .into_iter()
                    .zip([&instance.source_type, &instance.target_type])
                    .any(|(left, right)| matches!((left, right), (Some(a), Some(b)) if a != b))
            {
                return Err(crate::execution_loss::invalid());
            }
            if previous.source_type.is_none() {
                previous.source_type = instance.source_type;
            }
            if previous.target_type.is_none() {
                previous.target_type = instance.target_type;
            }
        } else {
            controls.insert(key, instance);
        }
    }
    let controls: Vec<RuleInstance> = controls.into_values().collect();
    let report = crate::execution_loss::ExecutionLossReport::planned(
        policy,
        controls.iter().map(|i| i.risk(policy)).collect(),
    )?;
    Ok((controls, report))
}
