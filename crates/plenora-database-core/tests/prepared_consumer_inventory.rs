//! Inventario sintattico dei consumer, eseguito dal cargo test del workspace.
use std::collections::BTreeMap;
use std::path::Path;
use syn::visit::{self, Visit};

#[derive(Default, Debug, serde::Serialize, PartialEq, Eq)]
struct Sites {
    prepared: usize,
    prepare: usize,
    write: usize,
    consent: usize,
    overrides: usize,
    direct: usize,
}

impl Sites {
    fn call(&mut self, name: &syn::Ident) {
        match name.to_string().as_str() {
            "prepare_write" => self.prepare += 1,
            "write" => self.write += 1,
            "write_with_consent" => self.consent += 1,
            "write_prepared" | "write_prepared_with_fault" | "write_prepared_inner" => {
                self.direct += 1;
            }
            _ => {}
        }
    }
}

impl<'ast> Visit<'ast> for Sites {
    fn visit_path_segment(&mut self, node: &'ast syn::PathSegment) {
        if node.ident == "PreparedWrite" {
            self.prepared += 1;
        }
        visit::visit_path_segment(self, node);
    }
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        self.call(&node.method);
        visit::visit_expr_method_call(self, node);
    }
    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = node.func.as_ref() {
            if let Some(segment) = path.path.segments.last() {
                self.call(&segment.ident);
            }
        }
        visit::visit_expr_call(self, node);
    }
    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if node.sig.ident == "write" {
            struct NoManufacturedConsent;
            impl<'ast> Visit<'ast> for NoManufacturedConsent {
                fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
                    let segments: Vec<_> = node
                        .path
                        .segments
                        .iter()
                        .map(|part| part.ident.to_string())
                        .collect();
                    assert!(
                        !segments.windows(2).any(|pair| pair[0] == "LossConsent"
                            && ["Accept", "from_accept_loss", "default"]
                                .contains(&pair[1].as_str())),
                        "adapter ricostruisce il consenso"
                    );
                    visit::visit_expr_path(self, node);
                }
            }
            NoManufacturedConsent.visit_block(&node.block);
        }
        visit::visit_impl_item_fn(self, node);
    }
    fn visit_signature(&mut self, node: &'ast syn::Signature) {
        if node.ident == "write_with_consent" {
            self.overrides += 1;
        }
        let prepared = node.inputs.iter().any(|input| matches!(input, syn::FnArg::Typed(arg) if matches!(arg.ty.as_ref(), syn::Type::Path(ty) if ty.path.segments.last().is_some_and(|part| part.ident == "PreparedWrite" || part.ident == "PreparedSqlServerWrite"))));
        if prepared
            && [
                "write",
                "write_prepared",
                "write_prepared_with_fault",
                "write_prepared_inner",
            ]
            .iter()
            .any(|name| node.ident == name)
        {
            assert!(node.inputs.iter().any(|input| matches!(input, syn::FnArg::Typed(arg) if matches!(arg.ty.as_ref(), syn::Type::Path(ty) if ty.path.segments.last().is_some_and(|part| part.ident == "LossConsent")))), "consenso obbligatorio assente: {}", node.ident);
        }
        visit::visit_signature(self, node);
    }
}

struct PublicBoundary;

impl PublicBoundary {
    fn check(signature: &syn::Signature, block: &syn::Block) {
        let mut sites = Sites::default();
        sites.visit_signature(signature);
        sites.visit_block(block);
        if sites.prepare != 0 || sites.prepared != 0 || sites.consent != 0 {
            assert_eq!(
                sites.write, 0,
                "consumer pubblico fuori confine: {}",
                signature.ident
            );
            assert!(
                sites.consent >= sites.prepare,
                "preparazione senza consenso: {}",
                signature.ident
            );
        }
    }
}

impl<'ast> Visit<'ast> for PublicBoundary {
    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        Self::check(&node.sig, &node.block);
        visit::visit_item_fn(self, node);
    }
    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        Self::check(&node.sig, &node.block);
        visit::visit_impl_item_fn(self, node);
    }
}

fn scan(root: &Path, directory: &Path, found: &mut BTreeMap<String, Sites>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            scan(root, &path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && !path.ends_with("prepared_consumer_inventory.rs")
        {
            let source = std::fs::read_to_string(&path).unwrap();
            let syntax = syn::parse_file(&source)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            let mut sites = Sites::default();
            sites.visit_file(&syntax);
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if [
                "plenora-database-cli/src/main.rs",
                "plenora-database-cli/src/write_cmd.rs",
                "plenora-database-engine/src/runtime_binding.rs",
                "plenora-database-py/src/write.rs",
                "plenora-database-py/src/family_write.rs",
            ]
            .contains(&relative.as_str())
            {
                PublicBoundary.visit_file(&syntax);
            }
            if !path.ends_with("plenora-database-core/src/provider.rs") {
                assert_eq!(
                    sites.overrides,
                    0,
                    "override del consenso: {}",
                    path.display()
                );
            }
            // Anche i file che chiamano solo write sono censiti: il tipo del
            // risultato di una preparazione puo essere inferito o passato.
            if sites != Sites::default() {
                found.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/"),
                    sites,
                );
            }
        }
    }
}

#[test]
fn every_preparation_consumer_is_registered_and_public_consumers_use_consent() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut found = BTreeMap::new();
    scan(root, root, &mut found);
    let actual = serde_json::to_value(&found).unwrap();
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("prepared_consumers.json")).unwrap();
    assert!(
        actual == expected,
        "inventario da riesaminare: {}",
        serde_json::to_string_pretty(&actual).unwrap()
    );
}

#[test]
fn syntax_covers_ufcs_spacing_and_every_call_and_override() {
    let syntax = syn::parse_file(
        r"
        fn consumer(p: &impl Provider) {
            let a = Provider::prepare_write(p);
            p . prepare_write ();
            p.write_with_consent(a);
            p.write(a);
            Provider::write(p, a);
        }
        impl Provider for Adapter {
            fn write_with_consent <'another> (&self) {}
        }
    ",
    )
    .unwrap();
    let mut sites = Sites::default();
    sites.visit_file(&syntax);
    assert_eq!(sites.prepare, 2);
    assert_eq!(sites.write, 2);
    assert_eq!(sites.consent, 1);
    assert_eq!(sites.overrides, 1);
}

#[test]
#[should_panic(expected = "consumer pubblico fuori confine")]
fn a_write_after_consent_is_still_rejected() {
    let syntax = syn::parse_file("fn consumer(p: &impl Provider) { let a = Provider::prepare_write(p); p.write_with_consent(a); p.write(a); }").unwrap();
    PublicBoundary.visit_file(&syntax);
}

#[test]
#[should_panic(expected = "consenso obbligatorio assente")]
fn direct_writer_cannot_omit_consent() {
    let mut sites = Sites::default();
    sites.visit_file(
        &syn::parse_file("fn write_prepared(prepared: PreparedSqlServerWrite) {}").unwrap(),
    );
}

#[test]
#[should_panic(expected = "adapter ricostruisce il consenso")]
fn adapter_cannot_replace_deny_with_accept() {
    let mut sites = Sites::default();
    sites.visit_file(&syn::parse_file("impl Provider for Adapter { fn write(prepared: PreparedWrite, consent: LossConsent) { provider.write(prepared, LossConsent::Accept); } }").unwrap());
}
