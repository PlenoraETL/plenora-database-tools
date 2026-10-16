// Offline fault injection covers await points TCP flush cannot reliably block on.
pub(super) struct WriteFault {
    at_flush: bool,
    cancel: bool,
    writes_left: usize,
}

impl WriteFault {
    pub(super) async fn run(&mut self, flushing: bool) -> std::io::Result<()> {
        if flushing == self.at_flush && self.writes_left == 0 {
            if self.cancel {
                std::future::pending::<()>().await;
            }
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.writes_left = self.writes_left.saturating_sub(1);
        Ok(())
    }
}

#[tokio::test]
async fn send_errors_and_cancellation_discard_socket_at_every_await() {
    for multi in [false, true] {
        for at_flush in [false, true] {
            for cancel in [false, true] {
                let mut inner = ConnectionInner::new_with_cache(0);
                inner.sdu_size = 12;
                inner.stream = Some(OracleStream::WriteFault(WriteFault {
                    at_flush,
                    cancel,
                    writes_left: usize::from(multi && !at_flush),
                }));
                let result = tokio::time::timeout(std::time::Duration::from_millis(10), async {
                    if multi {
                        inner.send_multi_packet(&[1; 10], 0).await
                    } else {
                        inner.send(&[1; 10]).await
                    }
                })
                .await;
                if cancel {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().is_err());
                }
                assert!(inner.stream.is_none());
                assert!(inner.send(&[1]).await.is_err());
                assert!(inner.receive_page().await.is_err());
            }
        }
    }
}

#[tokio::test]
async fn markers_and_reset_share_wire_budget_including_headers() {
    for large in [false, true] {
        for reset_seen in [false, true] {
            let (conn, mut peer) = synthetic_connection(large).await;
            let mut inner = conn.inner.lock().await;
            // DATA, BREAK, sent RESET and repeated MARKER headers share one budget.
            inner.response_limit = 200;
            peer.write_all(&ConnectionInner::build_data_chunk(&[], 0, large).unwrap())
                .await
                .unwrap();
            for kind in [1, if reset_seen { 2 } else { 1 }]
                .into_iter()
                .chain([1; 20])
            {
                peer.write_all(&inner.build_marker(kind).unwrap())
                    .await
                    .unwrap();
            }
            assert_eq!(
                inner.receive_page().await.unwrap()[4],
                PacketType::Marker as u8
            );
            assert_eq!(inner.response_bytes, 21);
            assert!(inner.handle_marker_reset().await.is_err());
            assert!(inner.stream.is_none());
            conn.closed.store(true, Ordering::Release);
        }
    }
}

#[tokio::test]
async fn framing_workspace_counts_against_memory_budget() {
    let (conn, mut peer) = synthetic_connection(false).await;
    let mut inner = conn.inner.lock().await;
    inner.response_limit = 100;
    peer.write_all(&ConnectionInner::build_data_chunk(&[9, 0, 0], 0, false).unwrap())
        .await
        .unwrap();
    // Wire fits; buffer plus framing workspace does not.
    assert!(inner.receive_page().await.is_err());
    assert!(inner.stream.is_none());
    conn.closed.store(true, Ordering::Release);
}

#[test]
fn framing_reuses_backing_allocation_for_incomplete_suffix() {
    let mut data = bytes::BytesMut::with_capacity(4096);
    data.extend_from_slice(&[4]);
    let pointer = data.as_ptr();
    let mut framing = PageFraming::new(&[]);
    let mut caps = Capabilities::new();
    caps.ttc_field_version = 16;
    let terminal = completion_packet(0, 1);
    for byte in &terminal[3..] {
        let frozen = data.freeze();
        assert!(!framing.advance(&frozen, &caps).unwrap());
        data = frozen.try_into_mut().unwrap();
        assert_eq!(pointer, data.as_ptr());
        data.extend_from_slice(&[*byte]);
    }
}

async fn call_lob_route(conn: &Connection, route: usize, locator: &LobLocator) -> Result<()> {
    match route {
        0 => conn.create_temp_lob(OracleType::Blob).await.map(|_| ()),
        1 => conn.lob_chunk_size(locator).await.map(|_| ()),
        2 => conn.lob_length(locator).await.map(|_| ()),
        3 => conn.lob_trim(locator, 0).await,
        4 => conn.bfile_exists(locator).await.map(|_| ()),
        5 => conn.bfile_open(locator).await,
        6 => conn.bfile_close(locator).await,
        7 => conn.bfile_is_open(locator).await.map(|_| ()),
        8 => conn.read_lob(locator).await.map(|_| ()),
        _ => conn.write_lob(locator, 1, &[1]).await,
    }
}

#[tokio::test]
async fn every_lob_route_collects_fragments_and_rejects_bad_completion() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        for route in 0..10 {
            for bad in [false, true] {
                for large in [false, true] {
                    let (conn, mut peer) = synthetic_connection(large).await;
                    {
                        let mut inner = conn.inner.lock().await;
                        inner.capabilities.ttc_field_version = 16;
                        inner.capabilities.supports_end_of_response = false;
                    }
                    let locator = LobLocator::new(
                        bytes::Bytes::from_static(&[29, 4, 9]),
                        0,
                        0,
                        if (4..=7).contains(&route) {
                            OracleType::Bfile
                        } else {
                            OracleType::Blob
                        },
                        0,
                    );
                    let mut payload = vec![8];
                    if route == 0 {
                        payload.extend_from_slice(&[29; 40]);
                        payload.extend_from_slice(&[0, 0]);
                    } else {
                        payload.extend_from_slice(locator.locator_bytes());
                        if matches!(route, 1..=5 | 7 | 8) {
                            payload.push(0);
                        }
                    }
                    let completion = completion_packet(0, 0);
                    payload.extend_from_slice(if bad {
                        &completion[2..6]
                    } else {
                        &completion[2..]
                    });
                    for (index, byte) in payload.iter().enumerate() {
                        let flags = if index + 1 == payload.len() {
                            crate::constants::data_flags::END_OF_RESPONSE
                        } else {
                            0
                        };
                        peer.write_all(
                            &ConnectionInner::build_data_chunk(&[*byte], flags, large).unwrap(),
                        )
                        .await
                        .unwrap();
                    }
                    let result = call_lob_route(&conn, route, &locator).await;
                    if bad {
                        assert!(result.is_err(), "route {route}");
                        assert!(conn.inner.lock().await.stream.is_none());
                    } else {
                        assert!(result.is_ok(), "route {route}: {result:?}");
                        peer.write_all(
                            &ConnectionInner::build_data_chunk(&[9, 0, 0], 0, large).unwrap(),
                        )
                        .await
                        .unwrap();
                        assert_eq!(
                            &conn.inner.lock().await.receive_page().await.unwrap()[10..],
                            &[9, 0, 0]
                        );
                    }
                    conn.closed.store(true, Ordering::Release);
                }
            }
        }
    })
    .await
    .expect("LOB collector must not wait for peer EOF");
}

#[tokio::test]
async fn final_data_after_reset_cannot_restart_wire_budget() {
    let (conn, mut peer) = synthetic_connection(false).await;
    let mut inner = conn.inner.lock().await;
    inner.response_limit = 250;
    peer.write_all(&ConnectionInner::build_data_chunk(&[], 0, false).unwrap())
        .await
        .unwrap();
    for kind in [1, 2].into_iter().chain([1; 18]) {
        peer.write_all(&inner.build_marker(kind).unwrap())
            .await
            .unwrap();
    }
    // 10 DATA + 20*11 incoming MARKER + 11 outgoing RESET = 241.
    let final_frame = ConnectionInner::build_data_chunk(&[9, 0, 0], 0, false).unwrap();
    peer.write_all(&final_frame[..8]).await.unwrap();
    inner.receive_page().await.unwrap();
    assert!(tokio::time::timeout(
        std::time::Duration::from_secs(1),
        inner.handle_marker_reset()
    )
    .await
    .unwrap()
    .is_err());
    assert!(inner.stream.is_none());
    conn.closed.store(true, Ordering::Release);
}

#[tokio::test]
async fn missing_temp_locator_and_server_lob_errors_invalidate_channel() {
    for code in [0, 1476] {
        let (conn, mut peer) = synthetic_connection(false).await;
        {
            let mut inner = conn.inner.lock().await;
            inner.capabilities.ttc_field_version = 16;
            inner.capabilities.supports_end_of_response = false;
        }
        let payload = completion_packet(code, 0);
        peer.write_all(&ConnectionInner::build_data_chunk(&payload[2..], 0, false).unwrap())
            .await
            .unwrap();
        let result = conn.create_temp_lob(OracleType::Blob).await;
        if code == 0 {
            assert!(matches!(result, Err(Error::Protocol(_))));
        } else {
            assert!(matches!(result, Err(Error::OracleError { code: 1476, .. })));
        }
        assert!(conn.inner.lock().await.stream.is_none());
        conn.closed.store(true, Ordering::Release);
    }
}
