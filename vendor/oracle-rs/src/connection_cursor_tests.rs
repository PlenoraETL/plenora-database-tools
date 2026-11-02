// Chiusura dei cursori server: codifica del piggyback CLOSE_CURSORS e
// percorsi che lo accodano. Peer sintetico, nessun server Oracle.

#[test]
fn close_cursors_piggyback_matches_python_oracledb_encoding() {
    // Senza token (TTC 16) e con il token number del 23ai (TTC 18).
    for (version, token) in [(16_u8, &[][..]), (18, &[0][..])] {
        let mut buf = WriteBuffer::new();
        write_close_cursors_piggyback(&mut buf, 7, version, &[42, 300, 256]).unwrap();
        let mut expected = vec![17, 105, 7];
        expected.extend_from_slice(token);
        // puntatore, numero UB4, poi ogni id UB4
        expected.extend_from_slice(&[1, 1, 3, 1, 42, 2, 1, 44, 2, 1, 0]);
        assert_eq!(buf.as_slice(), expected.as_slice(), "TTC {version}");
    }
}

#[test]
fn queued_cursors_ride_the_next_call_once_with_shifted_sequence() {
    for large in [false, true] {
        let mut inner = ConnectionInner::new();
        inner.large_sdu = large;
        inner.sdu_size = 8192;
        inner.capabilities.ttc_field_version = 16;
        // La chiamata ha gia preso la sequenza 9.
        inner.sequence_number = 9;
        let call = ConnectionInner::build_data_chunk(&[3, 94, 9, 0xAA, 0xBB], 0, large).unwrap();
        assert!(inner.attach_close_cursors(&call).unwrap().is_none());
        for cursor_id in [42, 42, 0, 7] {
            inner.release_cursor(cursor_id);
        }
        assert_eq!(
            inner.cursors_to_close,
            vec![42, 7],
            "uno per id, mai lo zero"
        );
        // MARKER e DATA che non sono chiamate non lo portano.
        let marker = inner.build_marker(1).unwrap();
        assert!(inner.attach_close_cursors(&marker).unwrap().is_none());
        let not_call = ConnectionInner::build_data_chunk(&[29], 0, large).unwrap();
        assert!(inner.attach_close_cursors(&not_call).unwrap().is_none());
        assert_eq!(inner.cursors_to_close, vec![42, 7]);

        let packet = inner.attach_close_cursors(&call).unwrap().unwrap();
        let expected = [
            0, 0, // flag DATA
            17, 105, 9, 1, 1, 2, 1, 42, 1, 7, // piggyback con la sequenza della chiamata
            3, 94, 10, 0xAA, 0xBB, // la chiamata con la sequenza successiva
        ];
        assert_eq!(
            packet,
            encode_packet(PacketType::Data, &expected, large).unwrap()
        );
        assert!(inner.cursors_to_close.is_empty());
        assert_eq!(inner.sequence_number, 10);
    }
}

#[test]
fn multi_packet_message_carries_the_piggyback_in_front() {
    // Il messaggio diviso su piu pacchetti (LOB grandi) lo porta in testa.
    let mut inner = ConnectionInner::new();
    inner.capabilities.ttc_field_version = 16;
    inner.release_cursor(5);
    inner.sequence_number = 4;
    let (message, count) = inner.close_cursors_prefix(&[3, 96, 4, 1]).unwrap().unwrap();
    assert_eq!(count, 1);
    assert_eq!(message, vec![17, 105, 4, 1, 1, 1, 1, 5, 3, 96, 5, 1]);
    // Un messaggio che comincia gia col piggyback non ne riceve un secondo.
    assert!(inner.close_cursors_prefix(&message).unwrap().is_none());
}

// Difetto del 25/10: una richiesta oltre l'SDU partiva in un pacchetto solo e
// il server chiudeva la connessione (PL/SQL con un bind di 32767 byte).
#[tokio::test]
async fn requests_larger_than_the_sdu_are_split_into_sdu_frames() {
    let (conn, mut peer) = synthetic_connection(false).await;
    let mut inner = conn.inner.lock().await;
    inner.capabilities.ttc_field_version = 16;
    inner.sdu_size = 64;
    // La chiamata ha gia preso la sequenza 1.
    inner.sequence_number = 1;
    inner.release_cursor(42);
    let mut message = vec![3, 94, 1];
    message.extend(std::iter::repeat_n(0x55, 297));
    let call = ConnectionInner::build_data_chunk(&message, 0, false).unwrap();
    inner.send(&call).await.unwrap();
    let mut expected = vec![17, 105, 1, 1, 1, 1, 1, 42, 3, 94, 2];
    expected.extend_from_slice(&message[3..]);
    let mut received = Vec::new();
    while received.len() < expected.len() {
        let frame = next_request(&mut peer).await;
        assert!(frame.len() <= 64, "pacchetto di {} byte", frame.len());
        assert_eq!(frame[4], PacketType::Data as u8);
        received.extend_from_slice(&frame[PACKET_HEADER_SIZE + 2..]);
    }
    assert_eq!(received, expected);
    assert!(inner.cursors_to_close.is_empty());
    drop(inner);
    conn.closed.store(true, Ordering::Release);
}

// Difetto del 25/10 (sonda VARCHAR2): un bind testo o binario oltre 252 byte
// partiva senza i pezzi UB4 e senza il terminatore, e il server aspettava
// altri dati fino al timeout. Lo stesso per OSON e VECTOR.
#[test]
fn long_binds_use_chunked_length_with_terminator() {
    let mut caps = Capabilities::new();
    caps.ttc_field_version = 16;
    for length in [252_usize, 253, 300, 4000, 32_767, 32_768, 70_000] {
        for binary in [false, true] {
            let value = if binary {
                Value::Bytes(vec![b'x'; length])
            } else {
                Value::String("x".repeat(length))
            };
            let statement = Statement::new("INSERT INTO t (v) VALUES (:1)");
            let mut message = ExecuteMessage::new(&statement, ExecuteOptions::for_dml(false));
            message.set_bind_values(vec![value]);
            let request = message.build_request_with_sdu(&caps, true).unwrap();
            let mut expected = WriteBuffer::new();
            expected
                .write_bytes_with_length(Some(&vec![b'x'; length]))
                .unwrap();
            let expected = expected.as_slice();
            if length > 252 {
                assert_eq!(expected[0], 254);
                assert_eq!(expected[expected.len() - 1], 0, "terminatore");
            }
            assert!(
                request.ends_with(expected),
                "{length} byte, binario {binary}: valore non codificato a pezzi"
            );
        }
    }
}

#[test]
fn at_most_one_piggyback_batch_per_call() {
    let mut inner = ConnectionInner::new();
    inner.capabilities.ttc_field_version = 16;
    inner.sdu_size = u32::from(u16::MAX);
    for cursor_id in 1..=u16::try_from(MAX_CURSORS_PER_PIGGYBACK + 3).unwrap() {
        inner.release_cursor(cursor_id);
    }
    let call = ConnectionInner::build_data_chunk(&[3, 94, 1], 0, false).unwrap();
    assert!(inner.attach_close_cursors(&call).unwrap().is_some());
    // Il resto parte con la chiamata successiva, nell'ordine di accodamento.
    assert_eq!(inner.cursors_to_close, vec![501, 502, 503]);
}

async fn next_request(peer: &mut TcpStream) -> Vec<u8> {
    let mut header = [0_u8; PACKET_HEADER_SIZE];
    peer.read_exact(&mut header).await.unwrap();
    let length = usize::from(u16::from_be_bytes([header[0], header[1]]));
    let mut packet = header.to_vec();
    packet.resize(length, 0);
    peer.read_exact(&mut packet[PACKET_HEADER_SIZE..])
        .await
        .unwrap();
    packet
}

fn row_page(code: u32) -> Vec<u8> {
    let mut data = describe_number();
    data.extend_from_slice(&io_vector_frame(&[32], 1));
    data.extend_from_slice(&[7, 2, 193, 2]);
    data.extend_from_slice(&completion_packet(code, 1)[2..]);
    data
}

// Il cursore 42 (riportato da `completion_packet`) si chiude nella chiamata
// che segue, o resta aperto se il risultato ha altre righe.
#[tokio::test]
async fn every_finished_statement_closes_its_cursor_with_the_next_call() {
    for case in 0..7 {
        let (conn, mut peer) = synthetic_connection(false).await;
        {
            let mut inner = conn.inner.lock().await;
            inner.capabilities.ttc_field_version = 16;
            inner.capabilities.supports_end_of_response = false;
        }
        let (response, ok, closes) = match case {
            0 => (row_page(1403), true, true), // query esaurita alla prima pagina
            1 => (row_page(0), true, false),   // altre righe: il cursore resta al chiamante
            2 => (completion_packet(942, 0)[2..].to_vec(), false, true), // query fallita
            3 => (completion_packet(0, 1)[2..].to_vec(), true, true), // DML
            4 => (completion_packet(1, 0)[2..].to_vec(), false, true), // DML fallito
            5 => (completion_packet(0, 0)[2..].to_vec(), true, true), // PL/SQL
            _ => (completion_packet(0, 1)[2..].to_vec(), true, true), // batch
        };
        peer.write_all(&ConnectionInner::build_data_chunk(&response, 0, false).unwrap())
            .await
            .unwrap();
        let result = match case {
            0..=2 => conn.query("SELECT n FROM t", &[]).await.map(|_| ()),
            3 | 4 => conn.execute("UPDATE t SET n = 1", &[]).await.map(|_| ()),
            5 => conn
                .execute_plsql("BEGIN NULL; END;", &[])
                .await
                .map(|_| ()),
            _ => {
                let batch = crate::batch::BatchBuilder::new("UPDATE t SET n = :1")
                    .add_row(vec![Value::Integer(1)])
                    .build();
                conn.execute_batch(&batch).await.map(|_| ())
            }
        };
        assert_eq!(result.is_ok(), ok, "caso {case}: {result:?}");
        let first = next_request(&mut peer).await;
        assert_eq!(first[10], MessageType::Function as u8, "caso {case}");

        peer.write_all(
            &ConnectionInner::build_data_chunk(&completion_packet(0, 1)[2..], 0, false).unwrap(),
        )
        .await
        .unwrap();
        conn.execute("UPDATE t SET n = 2", &[]).await.unwrap();
        let second = next_request(&mut peer).await;
        let sequence = first[12].wrapping_add(1);
        if closes {
            assert_eq!(
                &second[10..18],
                &[17, 105, sequence, 1, 1, 1, 1, 42],
                "caso {case}"
            );
            assert_eq!(&second[18..21], &[3, 94, sequence.wrapping_add(1)]);
        } else {
            assert_eq!(&second[10..13], &[3, 94, sequence], "caso {case}");
        }
        conn.closed.store(true, Ordering::Release);
    }
}

#[tokio::test]
async fn exhausting_fetch_closes_and_reexecution_parses_a_new_cursor() {
    let (conn, mut peer) = synthetic_connection(false).await;
    {
        let mut inner = conn.inner.lock().await;
        inner.capabilities.ttc_field_version = 16;
        inner.capabilities.supports_end_of_response = false;
    }
    let mut responses = row_page(0);
    let mut chunks = vec![ConnectionInner::build_data_chunk(&responses, 0, false).unwrap()];
    // Lo stesso testo mentre il primo risultato e aperto: un parse nuovo, non
    // la riesecuzione del cursore in lettura.
    chunks.push(ConnectionInner::build_data_chunk(&responses, 0, false).unwrap());
    responses = vec![7, 2, 193, 3];
    responses.extend_from_slice(&completion_packet(1403, 2)[2..]);
    chunks.push(ConnectionInner::build_data_chunk(&responses, 0, false).unwrap());
    chunks
        .push(ConnectionInner::build_data_chunk(&completion_packet(0, 1)[2..], 0, false).unwrap());
    for chunk in chunks {
        peer.write_all(&chunk).await.unwrap();
    }
    let page = conn.query("SELECT n FROM t", &[]).await.unwrap();
    assert!(page.has_more_rows);
    let again = conn.query("SELECT n FROM t", &[]).await.unwrap();
    assert!(again.has_more_rows);
    assert_eq!(again.columns[0].name, "N");
    let last = conn.fetch_more(42, &page.columns, 10).await.unwrap();
    assert!(!last.has_more_rows);
    assert_eq!(last.rows[0].get(0).and_then(Value::as_str), Some("2"));
    conn.execute("UPDATE t SET n = 2", &[]).await.unwrap();

    let first = next_request(&mut peer).await;
    let second = next_request(&mut peer).await;
    let fetch = next_request(&mut peer).await;
    let after = next_request(&mut peer).await;
    for request in [&first, &second] {
        // Execute (94), mai Reexecute: niente cursori riusati fra esecuzioni.
        assert_eq!(&request[10..12], &[3, 94]);
    }
    assert_eq!(
        fetch[10],
        MessageType::Function as u8,
        "fetch senza piggyback"
    );
    assert_eq!(&after[10..18], &[17, 105, fetch[12] + 1, 1, 1, 1, 1, 42]);
    conn.closed.store(true, Ordering::Release);
}

#[tokio::test]
async fn closed_scrollable_cursor_is_closed_on_the_server() {
    let (conn, mut peer) = synthetic_connection(false).await;
    {
        let mut inner = conn.inner.lock().await;
        inner.capabilities.ttc_field_version = 16;
        inner.capabilities.supports_end_of_response = false;
    }
    let mut cursor = ScrollableCursor::new(77, Vec::new());
    conn.close_cursor(&mut cursor).await.unwrap();
    // Una seconda chiusura non lo riaccoda.
    conn.close_cursor(&mut cursor).await.unwrap();
    assert_eq!(conn.inner.lock().await.cursors_to_close, vec![77]);
    peer.write_all(
        &ConnectionInner::build_data_chunk(&completion_packet(0, 1)[2..], 0, false).unwrap(),
    )
    .await
    .unwrap();
    conn.execute("UPDATE t SET n = 2", &[]).await.unwrap();
    let request = next_request(&mut peer).await;
    assert_eq!(&request[10..12], &[17, 105]);
    assert_eq!(&request[13..18], &[1, 1, 1, 1, 77]);
    conn.closed.store(true, Ordering::Release);
}
