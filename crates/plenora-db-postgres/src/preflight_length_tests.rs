use super::*;

/// Un cast limitato puo perdere contenuto: non e soltanto un avviso sugli spazi.
#[test]
fn bounded_report_declares_content_loss() {
    let field = arrow_schema::Field::new("v", arrow_schema::DataType::Utf8, true);
    for native in ["character varying(10)", "character(10)"] {
        let loss = bounded_loss(0, &field, Some(native.to_owned())).unwrap();
        assert_eq!(loss.severity, LossSeverity::DataLoss);
        assert!(loss.reason.contains("contenuto oltre"));
        assert!(loss.reason.contains("non spazio"));
    }
}
