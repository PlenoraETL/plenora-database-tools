//! Fetch message for retrieving rows from a cursor
//!
//! This module implements the fetch message used to retrieve additional
//! rows from an already-executed query cursor.

use crate::packet::encode_packet;
use bytes::Bytes;

use crate::buffer::WriteBuffer;
use crate::capabilities::Capabilities;
use crate::constants::{FetchOrientation, FunctionCode, MessageType, PacketType};
use crate::error::Result;

/// Fetch message to retrieve rows from a cursor
#[derive(Debug)]
pub struct FetchMessage {
    /// Cursor ID to fetch from
    cursor_id: u16,
    /// Number of rows to fetch
    num_rows: u32,
    /// Fetch orientation for scrollable cursors
    orientation: Option<FetchOrientation>,
    /// Fetch offset/position for scrollable cursors
    offset: i64,
}

impl FetchMessage {
    /// Create a new fetch message
    pub fn new(cursor_id: u16, num_rows: u32) -> Self {
        Self {
            cursor_id,
            num_rows,
            orientation: None,
            offset: 0,
        }
    }

    /// Create a new scrollable fetch message
    pub fn new_scrollable(
        cursor_id: u16,
        num_rows: u32,
        orientation: FetchOrientation,
        offset: i64,
    ) -> Self {
        Self {
            cursor_id,
            num_rows,
            orientation: Some(orientation),
            offset,
        }
    }

    /// Build the fetch request packet
    pub fn build_request(&self, caps: &Capabilities) -> Result<Bytes> {
        self.build_request_with_sdu(caps, false)
    }

    /// Build the fetch request using the negotiated length format.
    pub fn build_request_with_sdu(&self, _caps: &Capabilities, large_sdu: bool) -> Result<Bytes> {
        let mut buf = WriteBuffer::new();
        buf.write_u16_be(0)?;

        // Write message header
        buf.write_u8(MessageType::Function as u8)?;
        buf.write_u8(FunctionCode::Fetch as u8)?;
        buf.write_u8(0)?; // Sequence number

        // Write fetch body
        buf.write_ub4(self.cursor_id as u32)?;
        buf.write_ub4(self.num_rows)?;

        // Write scrollable cursor fields if present
        if let Some(orientation) = self.orientation {
            buf.write_ub4(orientation as u32)?; // Fetch orientation
            buf.write_ub4(self.offset as u32)?; // Fetch position (for absolute/relative)
        }

        encode_packet(PacketType::Data, buf.as_slice(), large_sdu)
    }

    /// Get the cursor ID
    pub fn cursor_id(&self) -> u16 {
        self.cursor_id
    }

    /// Get the number of rows to fetch
    pub fn num_rows(&self) -> u32 {
        self.num_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::PACKET_HEADER_SIZE;

    #[test]
    fn scrollable_fetch_packet_bytes() {
        let caps = Capabilities::new();
        let msg = FetchMessage::new_scrollable(1, 1, FetchOrientation::Absolute, 2);
        for large_sdu in [false, true] {
            let packet = msg.build_request_with_sdu(&caps, large_sdu).unwrap();
            crate::packet::assert_packet_bytes(&packet, PacketType::Data, large_sdu);
            assert_eq!(&packet[8..], &[0, 0, 3, 5, 0, 1, 1, 1, 1, 1, 32, 1, 2]);
        }
        assert_eq!(
            FetchMessage::new(1, 1).build_request(&caps).unwrap(),
            FetchMessage::new(1, 1)
                .build_request_with_sdu(&caps, false)
                .unwrap()
        );
    }

    #[test]
    fn packet_bytes_both_sdu_formats() {
        let caps = Capabilities::new();
        let msg = FetchMessage::new(1, 1);
        for large_sdu in [false, true] {
            let packet = msg.build_request_with_sdu(&caps, large_sdu).unwrap();
            crate::packet::assert_packet_bytes(&packet, PacketType::Data, large_sdu);
            assert_eq!(&packet[8..10], &[0, 0]);
            assert_eq!(&packet[8..], &[0, 0, 3, 5, 0, 1, 1, 1, 1]);
            assert_eq!(packet.len(), 17);
        }
    }

    #[test]
    fn test_fetch_message_creation() {
        let msg = FetchMessage::new(1, 100);
        assert_eq!(msg.cursor_id(), 1);
        assert_eq!(msg.num_rows(), 100);
    }

    #[test]
    fn test_fetch_message_builds_packet() {
        let msg = FetchMessage::new(1, 100);
        let caps = Capabilities::new();

        let packet = msg.build_request(&caps).unwrap();

        // Check packet header
        assert!(packet.len() > PACKET_HEADER_SIZE);
        assert_eq!(packet[4], PacketType::Data as u8);

        // Check data flags are present
        assert_eq!(packet[8], 0);
        assert_eq!(packet[9], 0);

        // Check function type (byte 10) is Function (3)
        assert_eq!(packet[10], MessageType::Function as u8);

        // Check function code (byte 11) is Fetch (5)
        assert_eq!(packet[11], FunctionCode::Fetch as u8);
    }
}
