use super::*;

fn loss(field_id: usize) -> MappingLoss {
    let field_id = u32::try_from(field_id).expect("indice entro u32");
    MappingLoss {
        field_id,
        category: LossCategory::NativeType,
        severity: LossSeverity::Information,
        reason: "tipo non equivalente".to_owned(),
        source_type: None,
        target_type: None,
    }
}

fn report(losses: Vec<MappingLoss>) -> LossReport {
    LossReport {
        schema_version: 2,
        policy: MappingPolicy::Compatible,
        losses,
    }
}

/// Il limite e un confine, e un confine si prova da entrambi i lati.
#[test]
fn the_loss_ceiling_is_inclusive() {
    let at_limit = report((0..MAX_LOSSES).map(loss).collect());
    assert!(at_limit.validate().is_ok(), "{MAX_LOSSES} deve passare");

    let over_limit = report((0..=MAX_LOSSES).map(loss).collect());
    let error = over_limit.validate().expect_err("4097 deve fallire");
    assert_eq!(error.category, crate::ErrorCategory::ResourceLimit);
}

#[test]
fn an_oversized_reason_is_rejected() {
    let mut oversized = loss(0);
    oversized.reason = "x".repeat(MAX_REASON_CHARS + 1);
    assert_eq!(
        report(vec![oversized])
            .validate()
            .expect_err("motivo troppo lungo")
            .category,
        crate::ErrorCategory::ResourceLimit
    );
}

/// Il limite del contratto e in **caratteri**, non in byte.
///
/// Un `reason` di 1024 caratteri accentati pesa 2048 byte: misurandolo
/// con `String::len()` veniva rifiutato pur essendo schema-valido. E' il
/// caso normale, non un estremo: questi messaggi sono in italiano.
#[test]
fn the_limit_counts_characters_not_bytes() {
    let mut multibyte = loss(0);
    multibyte.reason = "e\u{300}".repeat(MAX_REASON_CHARS / 2);
    assert_eq!(multibyte.reason.chars().count(), MAX_REASON_CHARS);
    assert!(
        multibyte.reason.len() > MAX_REASON_CHARS,
        "deve essere multibyte"
    );
    assert!(
        report(vec![multibyte]).validate().is_ok(),
        "{MAX_REASON_CHARS} caratteri devono passare, comunque siano codificati"
    );
}

/// Cio che il tipo serializza deve stare nel proprio contratto.
///
/// `source_type` e `target_type` sono `{"type": "string"}` e non
/// obbligatori: ammessi come stringa o assenti, mai `null`. Senza
/// `skip_serializing_if` uscivano come `null` e il documento non era piu
/// valido per lo schema che lo descrive.
#[test]
fn absent_type_names_are_omitted_not_null() {
    let document = serde_json::to_value(report(vec![loss(0)])).expect("serializzabile");
    let entry = &document["losses"][0];
    assert!(entry.get("source_type").is_none(), "{entry}");
    assert!(entry.get("target_type").is_none(), "{entry}");
    // E il giro inverso continua a funzionare: i campi restano opzionali.
    let parsed: LossReport = serde_json::from_value(document).expect("deserializzabile");
    assert_eq!(parsed.losses[0].source_type, None);
}

#[test]
fn an_oversized_type_name_is_rejected() {
    let mut oversized = loss(0);
    oversized.source_type = Some("x".repeat(MAX_TYPE_CHARS + 1));
    assert_eq!(
        report(vec![oversized])
            .validate()
            .expect_err("tipo troppo lungo")
            .category,
        crate::ErrorCategory::ResourceLimit
    );
}

/// Il messaggio riporta la misura, non il contenuto misurato.
#[test]
fn the_error_message_does_not_carry_the_offending_text() {
    let mut oversized = loss(0);
    oversized.reason = "SEGRETO".repeat(MAX_REASON_CHARS);
    let error = report(vec![oversized])
        .validate()
        .expect_err("troppo lungo");
    assert!(!error.message.contains("SEGRETO"), "{}", error.message);
}
