//! Rifiuti di Parse riproducibili senza server e senza rete.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

async fn frame(io: &mut DuplexStream, tag: u8, body: &[u8]) {
    io.write_u8(tag).await.unwrap();
    io.write_u32(u32::try_from(body.len()).unwrap() + 4)
        .await
        .unwrap();
    io.write_all(body).await.unwrap();
}

async fn message(io: &mut DuplexStream) -> (u8, Vec<u8>) {
    let tag = io.read_u8().await.unwrap();
    let len = io.read_u32().await.unwrap();
    let mut body = vec![0; usize::try_from(len - 4).unwrap()];
    io.read_exact(&mut body).await.unwrap();
    (tag, body)
}

async fn rejection(code: &'static str, vendor_message: &'static str) {
    let (client_io, mut server_io) = tokio::io::duplex(16_384);
    let server = tokio::spawn(async move {
        let len = server_io.read_u32().await.unwrap();
        let mut startup = vec![0; usize::try_from(len - 4).unwrap()];
        server_io.read_exact(&mut startup).await.unwrap();
        frame(&mut server_io, b'R', &0_u32.to_be_bytes()).await;
        frame(&mut server_io, b'Z', b"I").await;
        let (tag, body) = message(&mut server_io).await;
        assert_eq!(tag, b'Q');
        assert_eq!(body, b"START TRANSACTION\0");
        frame(&mut server_io, b'C', b"BEGIN\0").await;
        frame(&mut server_io, b'Z', b"T").await;
        let (tag, body) = message(&mut server_io).await;
        assert_eq!(tag, b'P');
        assert_ne!(body[0], 0, "la prova richiede un Parse nominato");
        let response = format!("SERROR\0C{code}\0M{vendor_message}\0\0");
        frame(&mut server_io, b'E', response.as_bytes()).await;
        while message(&mut server_io).await.0 != b'S' {}
        frame(&mut server_io, b'Z', b"E").await;
        // Il controllo fallito non deve aver avviato COPY: il prossimo
        // messaggio e il rollback esplicito del chiamante, non CopyFail.
        let (tag, body) = message(&mut server_io).await;
        assert_eq!(tag, b'Q');
        assert_eq!(body, b"ROLLBACK\0");
        frame(&mut server_io, b'C', b"ROLLBACK\0").await;
        frame(&mut server_io, b'Z', b"I").await;
    });
    let mut config = tokio_postgres::Config::new();
    config
        .user("protocol_test")
        .ssl_mode(tokio_postgres::config::SslMode::Disable);
    let (mut client, connection) = config
        .connect_raw(client_io, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let transaction = client.transaction().await.unwrap();
    let raw = transaction
        .prepare("COPY protocol_test FROM STDIN BINARY")
        .await
        .unwrap_err();
    let error = classify_error(ErrorPhase::Prepare, &raw);
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
    assert_eq!(error.provider, Some(ProviderKind::Postgres));
    assert_eq!(error.retry, RetryDisposition::Never);
    assert!(error
        .message
        .starts_with("PG_PREPARED_STATEMENTS_REQUIRED:"));
    assert!(!format!("{error:?}").contains("secret_payload"));
    let error = classify_error(ErrorPhase::Commit, &raw);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    transaction.rollback().await.unwrap();
    server.await.unwrap();
    connection.abort();
}

#[tokio::test]
async fn prepared_rejections_are_typed_redacted_and_do_not_start_copy() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for (code, message) in [
            ("26000", "prepared statement secret_payload does not exist"),
            ("42P05", "prepared statement secret_payload already exists"),
            ("0A000", "prepared statements not supported: secret_payload"),
            ("08P01", "prepared statements disabled: secret_payload"),
        ] {
            rejection(code, message).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn unrelated_errors_are_not_pooler_configuration_errors() {
    for (code, message) in [
        (Some("0A000"), Some("COPY not supported")),
        (Some("08P01"), Some("invalid message format")),
        (Some("42501"), Some("prepared statements not supported")),
        (None, Some("prepared statements disabled")),
        (Some("0A000"), None),
    ] {
        assert!(!prepared_statement_rejected(code, message));
    }
}
