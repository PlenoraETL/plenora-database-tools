use super::bind_error_context;

#[test]
fn bind_context_extracts_only_structural_metadata() {
    assert_eq!(
        bind_error_context(
            "bind PostgreSQL incompatibile al parametro 2: tipo portabile i64, target uuid"
        ),
        Some((2, "i64".to_owned(), "uuid".to_owned()))
    );
}

#[test]
fn unrelated_public_messages_do_not_invent_bind_metadata() {
    assert_eq!(bind_error_context("operazione PostgreSQL fallita"), None);
}
