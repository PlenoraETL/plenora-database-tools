#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::{ErrorCategory, ErrorPhase};
use plenora_db_postgres::PostgresTlsConfig;

/// Divide l'input in tre sezioni PEM con un separatore improbabile in un PEM
/// valido, così il fuzzer può far evolvere le tre parti separatamente.
const SEPARATOR: &[u8] = b"\x00PEM\x00";

fn split_sections(input: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let mut sections = [&input[0..0]; 3];
    let mut rest = input;
    let mut index = 0;
    while index < 2 {
        let Some(position) = rest
            .windows(SEPARATOR.len())
            .position(|window| window == SEPARATOR)
        else {
            break;
        };
        sections[index] = &rest[..position];
        rest = &rest[position + SEPARATOR.len()..];
        index += 1;
    }
    sections[index] = rest;
    (sections[0], sections[1], sections[2])
}

fn check_rejection(error: &plenora_database_core::DatabaseError) {
    // La configurazione TLS è offline: ogni rifiuto resta classificato come
    // configurazione non valida e non può portare un identificativo di
    // esecuzione remota.
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
    assert_eq!(error.phase, ErrorPhase::Connect);
    assert!(!error.message.is_empty());
    assert!(error.execution_id.is_none());
}

fuzz_target!(|input: &[u8]| {
    let (ca_pem, client_chain_pem, client_key_pem) = split_sections(input);

    // Solo CA private: il trust store pubblico resta escluso, quindi un PEM
    // non valido o vuoto deve fallire chiuso.
    match PostgresTlsConfig::private_ca_pem(ca_pem) {
        Ok(_) => assert!(!ca_pem.is_empty()),
        Err(error) => check_rejection(&error),
    }

    // mTLS: catena client e chiave privata sono accettate solo insieme e solo
    // se combaciano.
    if let Err(error) = PostgresTlsConfig::private_ca_with_client_identity_pem(
        ca_pem,
        client_chain_pem,
        client_key_pem,
    ) {
        check_rejection(&error);
    }

    // Costruttore completo senza radici pubbliche: caricare `webpki-roots` a
    // ogni iterazione renderebbe la campagna dominata dal trust store.
    if let Err(error) = PostgresTlsConfig::from_pem(false, ca_pem, Some(client_chain_pem), None) {
        check_rejection(&error);
    }
});
