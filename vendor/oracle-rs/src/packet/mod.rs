//! TNS packet encoding/decoding
//!
//! This module handles the TNS packet layer, including the 8-byte packet header
//! and packet type-specific parsing.

mod header;

pub use header::PacketHeader;

use bytes::Bytes;

use crate::constants::PacketType;
use crate::error::Result;

/// Encode a packet from its complete payload, including DATA flags when present.
pub(crate) fn encode_packet(
    packet_type: PacketType,
    payload: &[u8],
    large_sdu: bool,
) -> Result<Bytes> {
    let length = payload
        .len()
        .checked_add(crate::constants::PACKET_HEADER_SIZE)
        .and_then(|n| u32::try_from(n).ok())
        .filter(|&n| large_sdu || n <= u16::MAX as u32)
        .ok_or_else(|| {
            crate::error::Error::Protocol("Packet exceeds negotiated length format".into())
        })?;
    let mut buf = crate::buffer::WriteBuffer::with_capacity(length as usize);
    PacketHeader::new(packet_type, length).write(&mut buf, large_sdu)?;
    buf.write_bytes(payload)?;
    Ok(buf.freeze())
}

/// A complete TNS packet with header and payload
#[derive(Debug, Clone)]
pub struct Packet {
    /// The packet header
    pub header: PacketHeader,
    /// The packet payload (everything after the 8-byte header)
    pub payload: Bytes,
}

impl Packet {
    /// Create a new packet with the given header and payload
    pub fn new(header: PacketHeader, payload: Bytes) -> Self {
        Self { header, payload }
    }

    /// Create a packet from raw bytes
    pub fn from_bytes(data: Bytes) -> Result<Self> {
        let header = PacketHeader::parse(&data)?;
        let payload = data.slice(crate::constants::PACKET_HEADER_SIZE..);
        Ok(Self { header, payload })
    }

    /// Get the packet type
    pub fn packet_type(&self) -> PacketType {
        self.header.packet_type
    }

    /// Get the total packet size
    pub fn total_size(&self) -> usize {
        self.header.length as usize
    }

    /// Get the payload size
    pub fn payload_size(&self) -> usize {
        self.payload.len()
    }

    /// Check if this is a DATA packet
    pub fn is_data(&self) -> bool {
        self.header.packet_type == PacketType::Data
    }

    /// Check if this is an ACCEPT packet
    pub fn is_accept(&self) -> bool {
        self.header.packet_type == PacketType::Accept
    }

    /// Check if this is a REFUSE packet
    pub fn is_refuse(&self) -> bool {
        self.header.packet_type == PacketType::Refuse
    }

    /// Check if this is a REDIRECT packet
    pub fn is_redirect(&self) -> bool {
        self.header.packet_type == PacketType::Redirect
    }

    /// Check if this is a MARKER packet
    pub fn is_marker(&self) -> bool {
        self.header.packet_type == PacketType::Marker
    }

    /// Check if this is a CONTROL packet
    pub fn is_control(&self) -> bool {
        self.header.packet_type == PacketType::Control
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_length_boundaries() {
        for large_sdu in [false, true] {
            let payload = vec![0x5a; u16::MAX as usize - 8];
            let packet = encode_packet(PacketType::Data, &payload, large_sdu).unwrap();
            assert_packet_bytes(&packet, PacketType::Data, large_sdu);
            assert_eq!(&packet[8..], &payload);
            let payload = vec![0x5a; u16::MAX as usize - 7];
            let result = encode_packet(PacketType::Data, &payload, large_sdu);
            if large_sdu {
                let packet = result.unwrap();
                assert_eq!(&packet[..8], &[0, 1, 0, 0, 6, 0, 0, 0]);
                assert_eq!(&packet[8..], &payload);
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn test_packet_from_bytes() {
        // Minimal CONNECT packet
        let data = Bytes::from_static(&[
            0x00, 0x0a, // Length: 10
            0x00, 0x00, // Packet checksum
            0x01, // Type: CONNECT
            0x00, // Flags
            0x00, 0x00, // Header checksum
            0x41, 0x42, // Payload: "AB"
        ]);

        let packet = Packet::from_bytes(data).unwrap();
        assert_eq!(packet.packet_type(), PacketType::Connect);
        assert_eq!(packet.total_size(), 10);
        assert_eq!(packet.payload_size(), 2);
        assert_eq!(&packet.payload[..], &[0x41, 0x42]);
    }
}

#[cfg(test)]
pub(crate) fn assert_packet_bytes(packet: &[u8], kind: PacketType, large_sdu: bool) {
    let n = packet.len();
    let length = if large_sdu {
        (n as u32).to_be_bytes()
    } else {
        [((n >> 8) & 255) as u8, (n & 255) as u8, 0, 0]
    };
    assert_eq!(&packet[..4], &length);
    assert_eq!(&packet[4..8], &[kind as u8, 0, 0, 0]);
}
