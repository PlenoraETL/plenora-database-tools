#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect};
use plenora_database_engine::{parse_and_validate, validate};

fuzz_target!(|input: &[u8]| {
    match parse_and_validate(input) {
        Ok(validated) => {
            // Il fingerprint è un digest SHA-256 reso in esadecimale minuscolo.
            let fingerprint = validated.fingerprint().to_owned();
            assert_eq!(fingerprint.len(), 64);
            assert!(fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));

            // Un piano accettato deve sopravvivere al proprio round-trip
            // canonico senza cambiare né struttura né fingerprint.
            let canonical = serde_json::to_vec(validated.plan()).expect("piano serializzabile");
            let reparsed = parse_and_validate(&canonical).expect("round-trip del piano accettato");
            assert_eq!(reparsed.plan(), validated.plan());
            assert_eq!(reparsed.fingerprint(), fingerprint);

            // La validazione diretta non può divergere dal percorso di parsing.
            let revalidated = validate(validated.plan().clone()).expect("piano già validato");
            assert_eq!(revalidated.fingerprint(), fingerprint);

            // Il limite hard di parsing resta fail-closed anche su un piano
            // altrimenti accettabile.
            assert!(canonical.len() <= 4 * 1024 * 1024);
        }
        Err(error) => {
            // La fase offline classifica ogni rifiuto come piano non valido e
            // non può dichiarare effetti remoti: nessuna connessione è stata
            // aperta.
            assert_eq!(error.category, ErrorCategory::InvalidPlan);
            assert_eq!(error.phase, ErrorPhase::Validate);
            assert_eq!(error.remote_effect, RemoteEffect::None);
            assert!(!error.message.is_empty());
            assert!(error.execution_id.is_none());
        }
    }
});
