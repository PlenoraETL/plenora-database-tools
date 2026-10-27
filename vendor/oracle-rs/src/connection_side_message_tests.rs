// Synthetic TTC fields reproduce the message ordering and direction byte seen live.
fn io_vector_frame(directions: &[u8], iteration: u32) -> Vec<u8> {
    let mut buf = WriteBuffer::new();
    buf.write_u8(11).unwrap();
    buf.write_u8(0).unwrap();
    buf.write_ub2(directions.len() as u16).unwrap();
    buf.write_ub4(0).unwrap();
    buf.write_ub4(iteration).unwrap();
    buf.write_ub2(0).unwrap();
    buf.write_ub2(0).unwrap();
    buf.write_ub4(0).unwrap();
    buf.write_bytes(directions).unwrap();
    buf.as_slice().to_vec()
}

fn assert_fragmented_frame(payload: &[u8], lob: bool) {
    let mut caps = Capabilities::new();
    caps.ttc_field_version = 16;
    caps.supports_end_of_response = false;
    let mut framing = PageFraming::new(&[]);
    if lob {
        framing.lob = Some((40, true));
        framing.lob_tail = 2;
    }
    for end in 0..payload.len() {
        assert!(!framing
            .advance(&bytes::Bytes::copy_from_slice(&payload[..end]), &caps)
            .unwrap());
    }
    assert!(framing
        .advance(&bytes::Bytes::copy_from_slice(payload), &caps)
        .unwrap());
    assert_eq!(framing.offset, payload.len());
}

#[test]
fn input_io_vector_without_metadata_consumes_all_directions() {
    let conn = parser_connection();
    let mut caps = Capabilities::new();
    caps.ttc_field_version = 16;
    for iteration in [1, 256] {
        for count in [1, 2, 3] {
            let mut payload = io_vector_frame(&vec![32; count], iteration);
            // First unconsumed direction was reported at offset 10 or 11 live.
            assert_eq!(payload.len() - count, if iteration == 1 { 10 } else { 11 });
            let mut buf = ReadBuffer::from_slice(&payload[1..]);
            let params = [BindParam::input(Value::Integer(1))];
            assert!(Connection::parse_io_vector(&mut buf, &params)
                .unwrap()
                .0
                .is_empty());
            assert_eq!(buf.remaining(), 0);
            payload.extend_from_slice(&completion_packet(0, 3)[2..]);
            assert_fragmented_frame(&payload, false);
            let mut packet = vec![0, 0];
            packet.extend_from_slice(&payload);
            assert_eq!(
                conn.parse_dml_response(&packet, &caps)
                    .unwrap()
                    .rows_affected,
                3
            );
            assert!(conn.parse_batch_response(&packet, 3, false, &caps).is_ok());
        }
    }
}

#[test]
fn io_vector_requires_output_metadata_and_valid_directions() {
    for direction in [16, 48, 255] {
        let payload = io_vector_frame(&[direction], 1);
        assert!(matches!(
            Connection::parse_io_vector(&mut ReadBuffer::from_slice(&payload[1..]), &[]),
            Err(Error::Protocol(_))
        ));
    }
    let payload = io_vector_frame(&[16, 32, 48], 1);
    let params = [
        BindParam::output(OracleType::Number, 22),
        BindParam::input(Value::Integer(1)),
        BindParam::input_output(Value::Integer(2), 22),
    ];
    let (indices, columns) =
        Connection::parse_io_vector(&mut ReadBuffer::from_slice(&payload[1..]), &params).unwrap();
    assert_eq!(indices, [0, 2]);
    assert_eq!(columns.len(), 2);
}

#[test]
fn create_temp_consumes_charset_flags_then_amount_before_completion() {
    let conn = parser_connection();
    let mut caps = Capabilities::new();
    caps.ttc_field_version = 16;
    let mut buf = WriteBuffer::new();
    buf.write_u8(8).unwrap();
    buf.write_bytes(&[29; 40]).unwrap();
    buf.write_ub2(873).unwrap();
    buf.write_u8(0).unwrap();
    buf.write_ub8(1).unwrap();
    assert_eq!(buf.as_slice().len(), 47); // completion offset observed live
    let mut payload = buf.as_slice().to_vec();
    payload.extend_from_slice(&completion_packet(0, 0)[2..]);
    assert_fragmented_frame(&payload, true);
    let mut packet = vec![0, 0];
    packet.extend_from_slice(&payload);
    assert!(conn
        .parse_temp_lob_response(&packet, &caps, OracleType::Blob)
        .is_ok());
}

#[test]
fn sync_pairs_before_completion_are_consumed_by_framing_and_parsers() {
    let conn = parser_connection();
    let mut caps = Capabilities::new();
    caps.ttc_field_version = 16;
    for length in [11, 18] {
        let mut buf = WriteBuffer::new();
        buf.write_bytes(&[23, 5]).unwrap();
        buf.write_ub2(1).unwrap();
        buf.write_u8(0).unwrap();
        buf.write_ub2(1).unwrap();
        buf.write_u8(0).unwrap();
        buf.write_ub2(length).unwrap();
        buf.write_bytes_with_length(Some(&vec![b'x'; length as usize]))
            .unwrap();
        buf.write_ub2(0).unwrap();
        buf.write_ub2(0).unwrap();
        buf.write_ub4(1).unwrap();
        let mut payload = buf.as_slice().to_vec();
        assert_eq!(payload.len(), length as usize + 15);
        payload.extend_from_slice(&completion_packet(0, 0)[2..]);
        assert_fragmented_frame(&payload, false);
        let mut packet = vec![0, 0];
        packet.extend_from_slice(&payload);
        assert!(conn.parse_dml_response(&packet, &caps).is_ok());
        assert!(conn.parse_batch_response(&packet, 1, false, &caps).is_ok());
        assert!(conn.parse_fetch_response(&packet, &[], &caps, None).is_ok());
        assert!(conn.parse_query_response(&packet, &caps).is_ok());
        assert!(conn.parse_plsql_response(&packet, &caps, &[]).is_ok());
        for kind in [OracleType::Blob, OracleType::Clob] {
            let locator = LobLocator::new(bytes::Bytes::from_static(&[1, 2, 3]), 0, 0, kind, 0);
            let mut lob_packet =
                packet[..packet.len() - completion_packet(0, 0).len() + 2].to_vec();
            lob_packet.extend_from_slice(&[14, 3, b'a', b'b', b'c']);
            lob_packet.extend_from_slice(&completion_packet(0, 0)[2..]);
            let result = conn
                .parse_lob_read_response(&lob_packet, &locator, &caps)
                .unwrap();
            match result {
                LobData::Bytes(data) => assert_eq!(&data[..], b"abc"),
                LobData::String(data) => assert_eq!(data, "abc"),
            }
            assert!(conn
                .parse_lob_bool_response(&packet, &locator, &caps)
                .is_ok());
            assert!(conn
                .parse_lob_simple_response(&packet, &locator, false, &caps)
                .is_ok());
            assert!(conn
                .parse_lob_amount_response(&packet, &locator, &caps)
                .is_ok());
        }
    }
    assert!(matches!(
        Connection::parse_server_piggyback(&mut ReadBuffer::from_slice(&[255])),
        Err(Error::Protocol(_))
    ));
}
