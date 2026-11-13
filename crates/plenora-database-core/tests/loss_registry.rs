//! Inventario indipendente: AST produttivo, fixture reali e registro in entrambe le direzioni.
use plenora_database_core::loss_registry::{PredicateId, RuleId};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::store_rewrite::StoreRewrite;
use plenora_database_core::write_admission::{
    instantiate, AdmissionTable, MeasuredRewrite, TargetDimensions,
};
use std::collections::BTreeSet;
use std::path::Path;
use syn::visit::{self, Visit};

fn no_orphans(registered: &[PredicateId], emitted: impl Iterator<Item = PredicateId>) -> bool {
    registered.iter().copied().collect::<BTreeSet<_>>() == emitted.collect()
}
#[test]
fn vocabulary_and_predicates_are_bidirectional() {
    let syntax = syn::parse_file(include_str!("../src/loss_registry.rs")).unwrap();
    let declared = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(item) if item.ident == "PredicateId" => Some(item.variants.len()),
            _ => None,
        })
        .unwrap();
    assert_eq!(declared, PredicateId::ALL.len());
    let ids = RuleId::ALL.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), RuleId::ALL.len());
    let mut wire: Vec<_> = ids.iter().map(|id| id.as_str()).collect();
    let original = wire.clone();
    wire.sort_unstable();
    assert_eq!(wire, original);
    for id in ids {
        let definition = id.definition();
        assert_eq!(definition.id, id);
        assert_eq!(definition.selector, id);
        assert!(!definition.reason.is_empty());
        assert_eq!(
            serde_json::from_value::<RuleId>(serde_json::json!(id.as_str())).unwrap(),
            id
        );
    }
    assert!(no_orphans(
        PredicateId::ALL,
        RuleId::ALL.iter().map(|id| id.definition().predicate)
    ));
    assert!(!no_orphans(
        PredicateId::ALL,
        RuleId::ALL
            .iter()
            .map(|id| id.definition().predicate)
            .filter(|p| *p != PredicateId::JsonNumbers)
    ));
}
fn variants() -> Vec<StoreRewrite> {
    vec![
        StoreRewrite::EmptyToNull,
        StoreRewrite::PadText { chars: 4 },
        StoreRewrite::PadBinary { bytes: 4 },
        StoreRewrite::StripTrailing,
        StoreRewrite::ExcessSpaces { bytes: 4 },
        StoreRewrite::Underflow { min_abs: 1e-130 },
        StoreRewrite::NegativeZero,
        StoreRewrite::FractionalSeconds { digits: 3 },
        StoreRewrite::NonAscii,
        StoreRewrite::JsonNumbers,
        StoreRewrite::Every,
    ]
}
#[test]
fn every_store_variant_has_one_registered_rule_and_alias_is_explicit() {
    let syntax = syn::parse_file(include_str!("../src/store_rewrite.rs")).unwrap();
    let count = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(e) if e.ident == "StoreRewrite" => Some(e.variants.len()),
            _ => None,
        })
        .unwrap();
    let variants = variants();
    assert_eq!(variants.len(), count);
    for (variant, expected) in variants.iter().zip([
        "store.empty_to_null",
        "store.pad_text",
        "store.pad_binary",
        "store.strip_trailing",
        "store.excess_spaces",
        "store.underflow",
        "store.negative_zero",
        "store.fractional_seconds",
        "store.non_ascii",
        "store.json_numbers",
        "store.every",
    ]) {
        assert_eq!(variant.rule_id().as_str(), expected);
    }
    let ids: BTreeSet<_> = variants.iter().map(|r| r.rule_id()).collect();
    assert_eq!(ids.len(), count);
    assert_eq!(
        ids,
        RuleId::ALL
            .iter()
            .copied()
            .filter(|id| id.as_str().starts_with("store."))
            .collect()
    );
    assert_eq!(
        RuleId::StorePadText.canonical(ProviderKind::Postgres),
        RuleId::PgPadText
    );
    assert_ne!(
        RuleId::StoreJsonNumbers.canonical(ProviderKind::Postgres),
        RuleId::PgJsonb
    );
    assert_ne!(
        RuleId::StoreEvery.canonical(ProviderKind::Sqlserver),
        RuleId::SsXml
    );
}
#[test]
fn measured_fixture_rewrites_resolve_without_promoting_unmeasured_pairs() {
    for (provider, bytes) in [
        (
            ProviderKind::Mysql,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/mysql.json").as_slice(),
        ),
        (
            ProviderKind::Postgres,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/postgres.json").as_slice(),
        ),
        (
            ProviderKind::Sqlserver,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/sqlserver.json")
                .as_slice(),
        ),
    ] {
        let table = AdmissionTable::parse(provider, bytes).unwrap();
        let mut found = 0;
        for measurement in &table.fixture().misure {
            for key in &measurement.chiavi {
                for &rewrite in &key.riscritture {
                    let rule = instantiate(
                        provider,
                        rewrite,
                        TargetDimensions {
                            chars: Some(4),
                            bytes: Some(4),
                        },
                    )
                    .unwrap()
                    .rule_id();
                    assert!(RuleId::ALL.contains(&rule));
                    found += 1;
                }
            }
        }
        assert!(found > 0);
    }
    assert!(serde_json::from_str::<MeasuredRewrite>(r#"{"classe":"unknown"}"#).is_err());
}
#[derive(Default)]
struct Constructors {
    count: usize,
    hidden: usize,
}
impl<'ast> Visit<'ast> for Constructors {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !node.attrs.iter().any(|a| {
            a.path().is_ident("cfg")
                && a.meta
                    .require_list()
                    .is_ok_and(|l| l.tokens.to_string() == "test")
        }) {
            visit::visit_item_mod(self, node);
        }
    }
    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if node
            .path
            .segments
            .last()
            .is_some_and(|p| p.ident == "MappingLoss")
        {
            self.count += 1;
        }
        visit::visit_expr_struct(self, node);
    }
    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if node
            .tokens
            .to_string()
            .split_ascii_whitespace()
            .any(|token| token == "MappingLoss")
        {
            self.hidden += 1;
        }
        visit::visit_macro(self, node);
    }
}
fn scan(path: &Path) -> usize {
    let mut total = 0;
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if path.file_name().unwrap() != "tests" {
                total += scan(&path);
            }
            continue;
        }
        if path.extension().is_none_or(|ext| ext != "rs")
            || path.file_name().unwrap().to_string_lossy().contains("test")
        {
            continue;
        }
        let mut visitor = Constructors::default();
        visitor.visit_file(&syn::parse_file(&std::fs::read_to_string(&path).unwrap()).unwrap());
        assert_eq!(
            visitor.hidden,
            0,
            "costruttore nascosto: {}",
            path.display()
        );
        assert_eq!(
            visitor.count,
            usize::from(path.ends_with("loss_registry.rs")),
            "costruttore fuori registro: {}",
            path.display()
        );
        total += visitor.count;
    }
    total
}
#[test]
fn only_registry_constructs_production_mapping_losses() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut count = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let src = entry.unwrap().path().join("src");
        if src.is_dir() {
            count += scan(&src);
        }
    }
    assert_eq!(count, 1);
}
#[test]
fn new_emission_and_macro_cannot_escape_inventory() {
    let mut visitor = Constructors::default();
    visitor.visit_file(&syn::parse_file("fn emit() { let x = other::MappingLoss { field_id: 0 }; } macro_rules! bypass { () => { MappingLoss { field_id: 0 } }; }").unwrap());
    assert_eq!(visitor.count, 1);
    assert_eq!(visitor.hidden, 1);
}
