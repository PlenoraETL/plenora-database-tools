//! OSON (Oracle's Binary JSON Format) encoding and decoding
//!
//! OSON is Oracle's binary representation of JSON data, optimized for storage
//! and wire transfer. This module provides encoding and decoding functions
//! between Rust's serde_json::Value and Oracle's OSON wire format.

use super::binary::{decode_binary_double, decode_binary_float};
use crate::{Error, Result};
use bytes::Bytes;
use std::collections::HashMap;

// OSON Magic bytes
const MAGIC_BYTE_1: u8 = 0xFF;
const MAGIC_BYTE_2: u8 = 0x4A; // 'J'
const MAGIC_BYTE_3: u8 = 0x5A; // 'Z'

// OSON Versions
const VERSION_MAX_FNAME_255: u8 = 1;
const VERSION_MAX_FNAME_65535: u8 = 3;

// Primary flags
const FLAG_REL_OFFSET_MODE: u16 = 0x0001;
const FLAG_INLINE_LEAF: u16 = 0x0002;
const FLAG_NUM_FNAMES_UINT32: u16 = 0x0008;
const FLAG_IS_SCALAR: u16 = 0x0010;
const FLAG_HASH_ID_UINT8: u16 = 0x0100;
const FLAG_NUM_FNAMES_UINT16: u16 = 0x0400;
const FLAG_FNAMES_SEG_UINT32: u16 = 0x0800;
const FLAG_TREE_SEG_UINT32: u16 = 0x1000;
const FLAG_TINY_NODES_STAT: u16 = 0x2000;

// Secondary flags (for long field names)
const FLAG_SEC_FNAMES_SEG_UINT16: u16 = 0x0100;

// Node types
const TYPE_NULL: u8 = 0x30;
const TYPE_TRUE: u8 = 0x31;
const TYPE_FALSE: u8 = 0x32;
const TYPE_STRING_LEN_U8: u8 = 0x33;
const TYPE_NUMBER_LEN_U8: u8 = 0x34;
const TYPE_BINARY_DOUBLE: u8 = 0x36;
const TYPE_STRING_LEN_U16: u8 = 0x37;
const TYPE_STRING_LEN_U32: u8 = 0x38;
const TYPE_TIMESTAMP: u8 = 0x39;
const TYPE_BINARY_LEN_U16: u8 = 0x3a;
const TYPE_BINARY_LEN_U32: u8 = 0x3b;
const TYPE_DATE: u8 = 0x3c;
const TYPE_INTERVAL_YM: u8 = 0x3d;
const TYPE_INTERVAL_DS: u8 = 0x3e;
const TYPE_TIMESTAMP_TZ: u8 = 0x7c;
const TYPE_TIMESTAMP7: u8 = 0x7d;
const TYPE_ID: u8 = 0x7e;
const TYPE_BINARY_FLOAT: u8 = 0x7f;
const TYPE_OBJECT: u8 = 0x84;
const TYPE_ARRAY: u8 = 0xc0;
const TYPE_EXTENDED: u8 = 0x7b;

// Plenora fork, declared limits of the OSON codec (rule: a JSON value
// either round-trips exactly or is refused with an error):
// - encoder: field names up to 255 bytes (OSON version 1) and up to 65535
//   distinct names; nesting up to `OSON_MAX_DEPTH`. Scope: JSON binds.
//   Hazard: the upstream encoder truncated names, counts and ids with `as`.
//   Re-entry: the version 3 long-name segment, proved against the server.
// - decoder: scalars render as `JSON_SERIALIZE` writes them (dates and
//   timestamps as text, RAW as upper-case hex, ids as lower-case hex), so a
//   binary scalar reads as the same text a JSON client sees; INTERVAL,
//   VECTOR and a TIMESTAMP WITH TIME ZONE before 1582-10-16 are refused, as
//   is a NUMBER no i64, u64 or f64 represents exactly. Scope: JSON reads.
//   Re-entry: typed JSON values in the provider (serde_json without
//   arbitrary precision cannot carry them).
// - decoder, BINARY_FLOAT: deviation from the `JSON_SERIALIZE` rendering.
//   Rule: the library returns the exact stored f32 widened to f64 (lossless).
//   Scope: BINARY_FLOAT scalars inside JSON reads. Hazard: the text differs
//   from `JSON_SERIALIZE`, whose float rendering is neither the shortest nor
//   the exact decimal (stored 123456792 is written 123456792, stored
//   1.401298464324817e-45 is written 1.4E-45, stored 0.1f32 is written 0.1):
//   no rule reproduces it, and the shortest f32 decimal (round 2) changed
//   the value (123456790, 1e-45). A client comparing text with the server's
//   serialization sees a different string for the same value. Re-entry: a
//   documented Oracle rendering algorithm, proved on every f32.

/// Plenora fork: deepest nesting of objects and arrays the codec accepts in
/// either direction. Both directions recurse; past this depth a document is
/// refused instead of overflowing the stack. 127 is the deepest nesting
/// serde_json parses from text (its recursion limit of 128 refuses 128
/// nested containers, measured), so what this codec accepts is exactly what
/// a JSON text can carry through Plenora, and it keeps a debug build inside
/// a 2 MiB thread stack (measured: the decoder overflowed it between 200 and
/// 400 levels).
pub const OSON_MAX_DEPTH: usize = 127;

/// Plenora fork: longest field name the encoder writes (OSON version 1, one
/// length byte). Longer names need the version 3 long-name segment, which the
/// encoder does not write: they are refused.
pub const OSON_MAX_FIELD_NAME_BYTES: usize = 255;

/// Plenora fork: most distinct field names the encoder writes (two-byte
/// field ids). More are refused.
pub const OSON_MAX_FIELD_NAMES: usize = 65535;

fn oson_error(message: &str) -> Error {
    Error::DataConversionError(message.to_string())
}

/// Plenora fork: render an Oracle DATE or TIMESTAMP found inside OSON with
/// every digit it carries (the upstream decoder truncated nanoseconds to
/// microseconds and underflowed on a zero time byte).
fn render_oson_datetime(bytes: &[u8]) -> Result<String> {
    let date = super::date::decode_oracle_date(
        bytes
            .get(..7)
            .ok_or_else(|| oson_error("invalid OSON date bytes"))?,
    )?;
    let base = render_fields(
        date.year,
        date.month,
        date.day,
        date.hour,
        date.minute,
        date.second,
    );
    match bytes.len() {
        7 => Ok(base),
        11 => Ok(format!("{}{}", base, render_fraction(&bytes[7..11])?)),
        _ => Err(oson_error("invalid OSON timestamp bytes")),
    }
}

/// `YYYY-MM-DDTHH:MI:SS` as `JSON_SERIALIZE` writes it, with a signed
/// four-digit year before Christ (`-0044`, where `{:04}` gives `-044`).
fn render_fields(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> String {
    let year = if year < 0 {
        format!("-{:04}", year.unsigned_abs())
    } else {
        format!("{:04}", year)
    };
    format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    )
}

/// The fraction of a TIMESTAMP as `JSON_SERIALIZE` writes it: six digits,
/// nine when the value carries nanoseconds.
fn render_fraction(bytes: &[u8]) -> Result<String> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| oson_error("invalid OSON timestamp fraction"))?;
    let nanos = u32::from_be_bytes(bytes);
    if nanos >= 1_000_000_000 {
        return Err(oson_error("invalid OSON timestamp fraction"));
    }
    if nanos % 1000 == 0 {
        Ok(format!(".{:06}", nanos / 1000))
    } else {
        Ok(format!(".{:09}", nanos))
    }
}

/// Plenora fork: a TIMESTAMP WITH TIME ZONE inside OSON, rendered as
/// `JSON_SERIALIZE` does: the local time and the offset. The bytes carry the
/// UTC instant; the local fields are computed with chrono, whose proleptic
/// Gregorian calendar is Oracle's only from 1582-10-15, so an earlier instant
/// (with a day of margin for the offset) is refused, as is a region id. The
/// upstream decoder dropped the offset and printed the UTC fields as a local
/// time.
fn render_oson_timestamp_tz(bytes: &[u8]) -> Result<String> {
    use chrono::{Datelike, Timelike};
    let invalid = || oson_error("invalid OSON timestamp bytes");
    if bytes.len() != 13 {
        return Err(invalid());
    }
    let utc = super::date::decode_oracle_date(&bytes[..7])?;
    let fraction = render_fraction(&bytes[7..11])?;
    let (hour, minute) = super::date::decode_tz_offset(bytes[11], bytes[12])?;
    if (utc.year, utc.month, utc.day) < (1582, 10, 16) {
        return Err(oson_error(
            "OSON TIMESTAMP WITH TIME ZONE before the Gregorian calendar",
        ));
    }
    let offset_seconds = i32::from(hour) * 3600 + i32::from(minute) * 60;
    let local = chrono::NaiveDate::from_ymd_opt(utc.year, u32::from(utc.month), u32::from(utc.day))
        .and_then(|date| {
            date.and_hms_opt(
                u32::from(utc.hour),
                u32::from(utc.minute),
                u32::from(utc.second),
            )
        })
        .and_then(|instant| {
            instant.checked_add_signed(chrono::TimeDelta::seconds(i64::from(offset_seconds)))
        })
        .ok_or_else(invalid)?;
    let byte = |value: u32| u8::try_from(value).map_err(|_| invalid());
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let magnitude = offset_seconds.unsigned_abs();
    // `JSON_SERIALIZE` writes a zero offset as `Z`.
    let offset = if magnitude == 0 {
        "Z".to_owned()
    } else {
        format!(
            "{sign}{:02}:{:02}",
            magnitude / 3600,
            (magnitude % 3600) / 60
        )
    };
    Ok(format!(
        "{}{}{}",
        render_fields(
            local.year(),
            byte(local.month())?,
            byte(local.day())?,
            byte(local.hour())?,
            byte(local.minute())?,
            byte(local.second())?,
        ),
        fraction,
        offset
    ))
}

/// Hexadecimal text of an OSON binary scalar, as `JSON_SERIALIZE` writes it:
/// upper case for RAW values, lower case for ids.
fn render_hex(bytes: &[u8], upper: bool) -> String {
    bytes
        .iter()
        .map(|byte| {
            if upper {
                format!("{byte:02X}")
            } else {
                format!("{byte:02x}")
            }
        })
        .collect()
}

/// OSON Decoder - parses OSON binary format to serde_json::Value
pub struct OsonDecoder {
    data: Bytes,
    pos: usize,
    version: u8,
    primary_flags: u16,
    secondary_flags: u16,
    field_names: Vec<String>,
    field_id_length: usize,
    tree_seg_pos: usize,
    relative_offsets: bool,
    depth: usize,
}

impl OsonDecoder {
    /// Decode OSON bytes to a JSON value
    pub fn decode(data: Bytes) -> Result<serde_json::Value> {
        let mut decoder = OsonDecoder {
            data,
            pos: 0,
            version: 0,
            primary_flags: 0,
            secondary_flags: 0,
            field_names: Vec::new(),
            field_id_length: 1,
            tree_seg_pos: 0,
            relative_offsets: false,
            depth: 0,
        };
        decoder.parse()
    }

    fn parse(&mut self) -> Result<serde_json::Value> {
        // Verify magic bytes
        if self.data.len() < 4 {
            return Err(Error::ProtocolError("OSON data too short".to_string()));
        }

        if self.data[0] != MAGIC_BYTE_1
            || self.data[1] != MAGIC_BYTE_2
            || self.data[2] != MAGIC_BYTE_3
        {
            return Err(Error::ProtocolError("Invalid OSON magic bytes".to_string()));
        }
        self.pos = 3;

        // Read version
        self.version = self.read_u8()?;
        if self.version != VERSION_MAX_FNAME_255 && self.version != VERSION_MAX_FNAME_65535 {
            return Err(Error::ProtocolError(format!(
                "Unsupported OSON version: {}",
                self.version
            )));
        }

        // Read primary flags
        self.primary_flags = self.read_u16_be()?;
        self.relative_offsets = (self.primary_flags & FLAG_REL_OFFSET_MODE) != 0;

        // Handle scalar values (simple case)
        if (self.primary_flags & FLAG_IS_SCALAR) != 0 {
            // Skip tree segment size
            if (self.primary_flags & FLAG_TREE_SEG_UINT32) != 0 {
                self.pos += 4;
            } else {
                self.pos += 2;
            }
            return self.decode_node();
        }

        // Read number of field names
        let num_short_field_names: u32;
        if (self.primary_flags & FLAG_NUM_FNAMES_UINT32) != 0 {
            num_short_field_names = self.read_u32_be()?;
            self.field_id_length = 4;
        } else if (self.primary_flags & FLAG_NUM_FNAMES_UINT16) != 0 {
            num_short_field_names = self.read_u16_be()? as u32;
            self.field_id_length = 2;
        } else {
            num_short_field_names = self.read_u8()? as u32;
            self.field_id_length = 1;
        }

        // Read field names segment size
        let short_field_names_seg_size: u32;
        let short_field_name_offsets_size: usize;
        if (self.primary_flags & FLAG_FNAMES_SEG_UINT32) != 0 {
            short_field_name_offsets_size = 4;
            short_field_names_seg_size = self.read_u32_be()?;
        } else {
            short_field_name_offsets_size = 2;
            short_field_names_seg_size = self.read_u16_be()? as u32;
        }

        // Handle long field names if version supports it
        let mut num_long_field_names: u32 = 0;
        let mut long_field_names_seg_size: u32 = 0;
        let mut long_field_name_offsets_size: usize = 0;

        if self.version == VERSION_MAX_FNAME_65535 {
            self.secondary_flags = self.read_u16_be()?;
            long_field_name_offsets_size =
                if (self.secondary_flags & FLAG_SEC_FNAMES_SEG_UINT16) != 0 {
                    2
                } else {
                    4
                };
            num_long_field_names = self.read_u32_be()?;
            long_field_names_seg_size = self.read_u32_be()?;
        }

        // Read tree segment size
        let _tree_seg_size: u32 = if (self.primary_flags & FLAG_TREE_SEG_UINT32) != 0 {
            self.read_u32_be()?
        } else {
            self.read_u16_be()? as u32
        };

        // Read number of tiny nodes (skip)
        let _num_tiny_nodes = self.read_u16_be()?;

        // Read short field names
        if num_short_field_names > 0 {
            let names = self.read_short_field_names(
                num_short_field_names,
                short_field_name_offsets_size,
                short_field_names_seg_size,
            )?;
            self.field_names.extend(names);
        }

        // Read long field names
        if num_long_field_names > 0 {
            let names = self.read_long_field_names(
                num_long_field_names,
                long_field_name_offsets_size,
                long_field_names_seg_size,
            )?;
            self.field_names.extend(names);
        }

        // Save tree segment position
        self.tree_seg_pos = self.pos;

        // Decode root node
        self.decode_node()
    }

    fn read_short_field_names(
        &mut self,
        num_fields: u32,
        offsets_size: usize,
        seg_size: u32,
    ) -> Result<Vec<String>> {
        // Skip hash ID array (1 byte per field)
        self.pos += num_fields as usize;

        // Read offsets
        let _offsets_start = self.pos;
        let mut offsets = Vec::with_capacity(self.bounded_capacity(num_fields));
        for _ in 0..num_fields {
            let offset = if offsets_size == 2 {
                self.read_u16_be()? as u32
            } else {
                self.read_u32_be()?
            };
            offsets.push(offset);
        }

        // Read field names segment
        let field_names_start = self.pos;
        let field_names_data = self.checked_segment(field_names_start, seg_size)?;
        self.pos += seg_size as usize;
        // Plenora fork: every name inside the segment, checked before the
        // indexing below (it used to panic on a malformed segment).
        for offset in &offsets {
            let start = *offset as usize;
            let len = usize::from(
                *field_names_data
                    .get(start)
                    .ok_or_else(|| oson_error("OSON field name outside its segment"))?,
            );
            if start + 1 + len > field_names_data.len() {
                return Err(oson_error("OSON field name outside its segment"));
            }
        }

        // Parse field names
        let mut names = Vec::with_capacity(num_fields as usize);
        for offset in offsets {
            let name_start = offset as usize;
            let name_len = field_names_data[name_start] as usize;
            let name_bytes = &field_names_data[name_start + 1..name_start + 1 + name_len];
            let name = String::from_utf8(name_bytes.to_vec()).map_err(|_| {
                Error::DataConversionError("nome di campo OSON non UTF-8".to_string())
            })?;
            names.push(name);
        }

        Ok(names)
    }

    fn read_long_field_names(
        &mut self,
        num_fields: u32,
        offsets_size: usize,
        seg_size: u32,
    ) -> Result<Vec<String>> {
        // Skip hash ID array (2 bytes per field)
        self.pos += num_fields as usize * 2;

        // Read offsets
        let mut offsets = Vec::with_capacity(self.bounded_capacity(num_fields));
        for _ in 0..num_fields {
            let offset = if offsets_size == 2 {
                self.read_u16_be()? as u32
            } else {
                self.read_u32_be()?
            };
            offsets.push(offset);
        }

        // Read field names segment
        let field_names_start = self.pos;
        let field_names_data = self.checked_segment(field_names_start, seg_size)?;
        self.pos += seg_size as usize;
        for offset in &offsets {
            let start = *offset as usize;
            let header = field_names_data
                .get(start..start + 2)
                .ok_or_else(|| oson_error("OSON field name outside its segment"))?;
            let len = usize::from(u16::from_be_bytes([header[0], header[1]]));
            if start + 2 + len > field_names_data.len() {
                return Err(oson_error("OSON field name outside its segment"));
            }
        }

        // Parse field names (long names use 2-byte length prefix)
        let mut names = Vec::with_capacity(num_fields as usize);
        for offset in offsets {
            let name_start = offset as usize;
            let name_len = u16::from_be_bytes([
                field_names_data[name_start],
                field_names_data[name_start + 1],
            ]) as usize;
            let name_bytes = &field_names_data[name_start + 2..name_start + 2 + name_len];
            let name = String::from_utf8(name_bytes.to_vec()).map_err(|_| {
                Error::DataConversionError("nome di campo OSON non UTF-8".to_string())
            })?;
            names.push(name);
        }

        Ok(names)
    }

    fn decode_node(&mut self) -> Result<serde_json::Value> {
        let node_type = self.read_u8()?;

        // Container node (object or array)
        if (node_type & 0x80) != 0 {
            return self.decode_container(node_type);
        }
        self.decode_scalar(node_type)
    }

    /// Plenora fork: scalars decode outside the recursive path, so a level
    /// of nesting costs only the small frames of `decode_node` and
    /// `decode_container_body` (with every scalar arm inlined, a debug build
    /// overflowed a 2 MiB stack below 150 levels).
    #[inline(never)]
    fn decode_scalar(&mut self, node_type: u8) -> Result<serde_json::Value> {
        // Simple scalar types
        match node_type {
            TYPE_NULL => Ok(serde_json::Value::Null),
            TYPE_TRUE => Ok(serde_json::Value::Bool(true)),
            TYPE_FALSE => Ok(serde_json::Value::Bool(false)),

            // String types
            TYPE_STRING_LEN_U8 => {
                let len = self.read_u8()? as usize;
                let s = self.read_string(len)?;
                Ok(serde_json::Value::String(s))
            }
            TYPE_STRING_LEN_U16 => {
                let len = self.read_u16_be()? as usize;
                let s = self.read_string(len)?;
                Ok(serde_json::Value::String(s))
            }
            TYPE_STRING_LEN_U32 => {
                let len = self.read_u32_be()? as usize;
                let s = self.read_string(len)?;
                Ok(serde_json::Value::String(s))
            }

            // Number type with length prefix
            TYPE_NUMBER_LEN_U8 => {
                let len = self.read_u8()? as usize;
                let num = self.decode_oracle_number(len)?;
                Ok(num)
            }

            // Binary double. Plenora fork: a non-finite value is refused,
            // `json!(NaN)` used to give `null`.
            TYPE_BINARY_DOUBLE => {
                let bytes: [u8; 8] = self
                    .read_bytes(8)?
                    .try_into()
                    .map_err(|_| oson_error("invalid OSON BINARY_DOUBLE"))?;
                serde_json::Number::from_f64(decode_binary_double(&bytes))
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| oson_error("non-finite OSON BINARY_DOUBLE"))
            }

            // Binary float (widened exactly to f64)
            TYPE_BINARY_FLOAT => {
                let bytes: [u8; 4] = self
                    .read_bytes(4)?
                    .try_into()
                    .map_err(|_| oson_error("invalid OSON BINARY_FLOAT"))?;
                // Plenora fork: the exact stored value, widened to f64
                // (declared deviation from `JSON_SERIALIZE`, see the header).
                serde_json::Number::from_f64(f64::from(decode_binary_float(&bytes)))
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| oson_error("non-finite OSON BINARY_FLOAT"))
            }

            // Date types (convert to string for JSON)
            TYPE_DATE | TYPE_TIMESTAMP7 => {
                let bytes = self.read_bytes(7)?;
                Ok(serde_json::Value::String(render_oson_datetime(&bytes)?))
            }

            TYPE_TIMESTAMP => {
                let bytes = self.read_bytes(11)?;
                Ok(serde_json::Value::String(render_oson_datetime(&bytes)?))
            }

            TYPE_TIMESTAMP_TZ => {
                let bytes = self.read_bytes(13)?;
                Ok(serde_json::Value::String(render_oson_timestamp_tz(&bytes)?))
            }

            // Plenora fork: binary scalars and ids become the hexadecimal
            // text `JSON_SERIALIZE` writes (the declared rendering of this
            // decoder); the upstream decoder wrote base64, a text Oracle
            // never produces for them.
            TYPE_BINARY_LEN_U16 => {
                let len = self.read_u16_be()? as usize;
                let bytes = self.read_bytes(len)?;
                Ok(serde_json::Value::String(render_hex(&bytes, true)))
            }
            TYPE_BINARY_LEN_U32 => {
                let len = self.read_u32_be()? as usize;
                let bytes = self.read_bytes(len)?;
                Ok(serde_json::Value::String(render_hex(&bytes, true)))
            }
            TYPE_ID => {
                let len = self.read_u8()? as usize;
                let bytes = self.read_bytes(len)?;
                Ok(serde_json::Value::String(render_hex(&bytes, false)))
            }

            // Extended types. Plenora fork: a VECTOR is refused, it used to
            // become an empty array.
            TYPE_EXTENDED => Err(oson_error("OSON extended type not supported")),

            // Intervals (not commonly used in JSON)
            TYPE_INTERVAL_YM | TYPE_INTERVAL_DS => Err(Error::ProtocolError(
                "Interval types not supported in JSON".to_string(),
            )),

            _ => {
                // Check for inline number/decimal (node_type & 0xf0 == 0x20 or 0x60)
                if (node_type & 0xf0) == 0x20 || (node_type & 0xf0) == 0x60 {
                    let len = (node_type & 0x0f) as usize + 1;
                    let num = self.decode_oracle_number(len)?;
                    return Ok(num);
                }

                // Check for inline integer (node_type & 0xf0 == 0x40 or 0x50)
                if (node_type & 0xf0) == 0x40 || (node_type & 0xf0) == 0x50 {
                    let len = (node_type & 0x0f) as usize;
                    let num = self.decode_oracle_number(len)?;
                    return Ok(num);
                }

                // Check for inline string (node_type & 0xe0 == 0)
                if (node_type & 0xe0) == 0 {
                    if node_type == 0 {
                        return Ok(serde_json::Value::String(String::new()));
                    }
                    let s = self.read_string(node_type as usize)?;
                    return Ok(serde_json::Value::String(s));
                }

                Err(Error::ProtocolError(format!(
                    "Unsupported OSON node type: 0x{:02x}",
                    node_type
                )))
            }
        }
    }

    fn decode_container(&mut self, node_type: u8) -> Result<serde_json::Value> {
        if self.depth >= OSON_MAX_DEPTH {
            return Err(oson_error("OSON nesting deeper than the supported limit"));
        }
        self.depth += 1;
        let value = self.decode_container_body(node_type);
        self.depth -= 1;
        value
    }

    fn decode_container_body(&mut self, node_type: u8) -> Result<serde_json::Value> {
        let is_object = (node_type & 0x40) == 0;
        let container_offset = u32::try_from(self.pos - self.tree_seg_pos - 1)
            .map_err(|_| oson_error("OSON offset overflow"))?;

        // Determine number of children
        let children_bits = node_type & 0x18;
        let is_shared = children_bits == 0x18;
        let num_children: u32 = match children_bits {
            0x00 => self.read_u8()? as u32,
            0x08 => self.read_u16_be()? as u32,
            0x10 => self.read_u32_be()?,
            0x18 => 0, // Will be read after offset
            _ => unreachable!(),
        };

        // Determine field_ids_pos and offsets_pos based on whether field IDs
        // are shared with another node or local.
        let (mut field_ids_pos, mut offsets_pos, num_children) = if is_shared {
            // Shared: read offset to the shared node, save current pos as offsets_pos,
            // then jump to shared node to get num_children and field_ids_pos
            let offset = self.get_offset(node_type)?;
            let local_offsets_pos = self.pos;
            self.pos = self.tree_seg_pos + offset as usize;
            let shared_node_type = self.read_u8()?;
            let shared_children_bits = shared_node_type & 0x18;
            let nc = match shared_children_bits {
                0x00 => self.read_u8()? as u32,
                0x08 => self.read_u16_be()? as u32,
                0x10 => self.read_u32_be()?,
                // Plenora fork: a shared node pointing at another shared
                // node is malformed; it used to read as zero children.
                _ => return Err(oson_error("OSON shared node without its own children")),
            };
            let shared_field_ids_pos = self.pos;
            (shared_field_ids_pos, local_offsets_pos, nc)
        } else if is_object {
            let fids = self.pos;
            let offs = fids + self.field_id_length * num_children as usize;
            (fids, offs, num_children)
        } else {
            (0, self.pos, num_children)
        };

        if is_object {
            let mut map = serde_json::Map::new();

            for _i in 0..num_children {
                // Read field ID
                self.pos = field_ids_pos;
                let field_id = match self.field_id_length {
                    1 => self.read_u8()? as usize,
                    2 => self.read_u16_be()? as usize,
                    4 => self.read_u32_be()? as usize,
                    _ => unreachable!(),
                };
                field_ids_pos = self.pos;

                // Get field name. Plenora fork: an unknown id is an error, it
                // used to invent the name "field_<id>".
                let field_name = field_id
                    .checked_sub(1)
                    .and_then(|index| self.field_names.get(index))
                    .cloned()
                    .ok_or_else(|| oson_error("OSON field id without a field name"))?;

                // Read value offset
                self.pos = offsets_pos;
                let value_offset = self.get_offset(node_type)?;
                offsets_pos = self.pos;

                // Calculate actual offset
                let actual_offset = if self.relative_offsets {
                    container_offset
                        .checked_add(value_offset)
                        .ok_or_else(|| oson_error("OSON offset overflow"))?
                } else {
                    value_offset
                };

                // Read value at offset
                self.pos = self.tree_seg_pos + actual_offset as usize;
                let value = self.decode_node()?;
                if map.insert(field_name, value).is_some() {
                    return Err(oson_error("OSON object with a repeated field"));
                }
            }

            Ok(serde_json::Value::Object(map))
        } else {
            // Array
            let mut arr = Vec::with_capacity(self.bounded_capacity(num_children));
            let mut offsets_pos = self.pos;

            for _i in 0..num_children {
                self.pos = offsets_pos;
                let value_offset = self.get_offset(node_type)?;
                offsets_pos = self.pos;

                let actual_offset = if self.relative_offsets {
                    container_offset
                        .checked_add(value_offset)
                        .ok_or_else(|| oson_error("OSON offset overflow"))?
                } else {
                    value_offset
                };

                self.pos = self.tree_seg_pos + actual_offset as usize;
                let value = self.decode_node()?;
                arr.push(value);
            }

            Ok(serde_json::Value::Array(arr))
        }
    }

    fn get_offset(&mut self, node_type: u8) -> Result<u32> {
        if (node_type & 0x20) != 0 {
            self.read_u32_be()
        } else {
            Ok(self.read_u16_be()? as u32)
        }
    }

    fn decode_oracle_number(&mut self, len: usize) -> Result<serde_json::Value> {
        let bytes = self.read_bytes(len)?;
        // Plenora fork: upstream turned an undecodable NUMBER into JSON null,
        // a non-number into a JSON string, and any non-integer into the
        // nearest f64 even when that changed the value
        // (123456789012345678901234567890 read back as 1.2345678901234568e29).
        // Without serde_json's arbitrary precision the exact value is kept
        // only when an integer or an f64 represents it; otherwise the read
        // fails instead of returning a different number.
        let num = crate::types::decode_oracle_number(&bytes)?;
        if let Ok(integer) = num.value.parse::<i64>() {
            return Ok(serde_json::Value::from(integer));
        }
        if let Ok(integer) = num.value.parse::<u64>() {
            return Ok(serde_json::Value::from(integer));
        }
        let exact = num
            .value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .filter(|number| super::number::same_decimal_value(&number.to_string(), &num.value));
        exact.map(serde_json::Value::Number).ok_or_else(|| {
            Error::DataConversionError(
                "Oracle NUMBER in JSON not representable exactly".to_string(),
            )
        })
    }

    /// Capacity for a vector of `count` items read from this document: never
    /// more than the bytes left, so a forged count cannot exhaust memory
    /// before the reads fail.
    fn bounded_capacity(&self, count: u32) -> usize {
        (count as usize).min(self.data.len().saturating_sub(self.pos))
    }

    /// The `size` bytes at `start`, or an error when they are not all there.
    fn checked_segment(&self, start: usize, size: u32) -> Result<Bytes> {
        let end = start
            .checked_add(size as usize)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| oson_error("OSON segment outside the document"))?;
        Ok(self.data.slice(start..end))
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.data.len() {
            return Err(Error::ProtocolError(
                "Unexpected end of OSON data".to_string(),
            ));
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u16_be(&mut self) -> Result<u16> {
        if self.pos + 2 > self.data.len() {
            return Err(Error::ProtocolError(
                "Unexpected end of OSON data".to_string(),
            ));
        }
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_u32_be(&mut self) -> Result<u32> {
        if self.pos + 4 > self.data.len() {
            return Err(Error::ProtocolError(
                "Unexpected end of OSON data".to_string(),
            ));
        }
        let v = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        if self.pos + len > self.data.len() {
            return Err(Error::ProtocolError(
                "Unexpected end of OSON data".to_string(),
            ));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    fn read_string(&mut self, len: usize) -> Result<String> {
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes)
            .map_err(|_| Error::DataConversionError("stringa OSON non UTF-8".to_string()))
    }
}

/// OSON Encoder - encodes serde_json::Value to OSON binary format
pub struct OsonEncoder {
    buffer: Vec<u8>,
    field_names: Vec<String>,
    field_name_to_id: HashMap<String, u32>,
}

impl OsonEncoder {
    /// Encode a JSON value to OSON bytes
    pub fn encode(value: &serde_json::Value) -> Result<Bytes> {
        let mut encoder = OsonEncoder {
            buffer: Vec::new(),
            field_names: Vec::new(),
            field_name_to_id: HashMap::new(),
        };
        encoder.encode_value(value)
    }

    fn encode_value(&mut self, value: &serde_json::Value) -> Result<Bytes> {
        // Collect field names if this is an object/array
        self.collect_field_names(value, 0)?;
        if self.field_names.len() > OSON_MAX_FIELD_NAMES {
            return Err(oson_error(
                "JSON value with more field names than OSON encodes",
            ));
        }

        // Sort field names and update ID mapping BEFORE encoding tree segment
        // This ensures field IDs in the tree match the sorted order in the field names segment
        self.sort_and_update_field_ids();

        // Encode tree segment first (to know its size)
        let mut tree_seg = Vec::new();
        self.encode_node(value, &mut tree_seg)?;

        // Write header
        self.buffer.push(MAGIC_BYTE_1);
        self.buffer.push(MAGIC_BYTE_2);
        self.buffer.push(MAGIC_BYTE_3);
        self.buffer.push(VERSION_MAX_FNAME_255);

        // Calculate flags
        let mut flags: u16 = FLAG_INLINE_LEAF;
        let is_scalar = !matches!(
            value,
            serde_json::Value::Object(_) | serde_json::Value::Array(_)
        );

        // Prepare field names components first if not scalar (to know segment size for flags)
        let field_names_components = if !is_scalar {
            Some(self.build_field_names_components()?)
        } else {
            None
        };

        if is_scalar {
            flags |= FLAG_IS_SCALAR;
        } else {
            flags |= FLAG_HASH_ID_UINT8 | FLAG_TINY_NODES_STAT;
            // Check if field names data needs 32-bit size
            if let Some((_, _, ref names_data)) = field_names_components {
                if names_data.len() > 65535 {
                    flags |= FLAG_FNAMES_SEG_UINT32;
                }
            }
            // Plenora fork: more than 255 names need two-byte field ids;
            // the upstream encoder wrote the count and every id `as u8`.
            if self.field_names.len() > 255 {
                flags |= FLAG_NUM_FNAMES_UINT16;
            }
        }

        let tree_seg_len = u32::try_from(tree_seg.len())
            .map_err(|_| oson_error("JSON value too large for OSON"))?;
        if tree_seg_len > 65535 {
            flags |= FLAG_TREE_SEG_UINT32;
        }

        // Write flags
        self.buffer.extend_from_slice(&flags.to_be_bytes());

        // Every narrowing below is guarded by the flag chosen above for the
        // same size, so the conversions are exact.
        let tree_seg_size = |buffer: &mut Vec<u8>| {
            if (flags & FLAG_TREE_SEG_UINT32) != 0 {
                buffer.extend_from_slice(&tree_seg_len.to_be_bytes());
            } else {
                buffer.extend_from_slice(&(tree_seg_len as u16).to_be_bytes());
            }
        };
        if is_scalar {
            // Write tree segment size
            tree_seg_size(&mut self.buffer);
        } else if let Some((hash_ids, offsets, names_data)) = field_names_components {
            // Write number of field names
            let count = self.field_names.len();
            if (flags & FLAG_NUM_FNAMES_UINT16) != 0 {
                self.buffer.extend_from_slice(&(count as u16).to_be_bytes());
            } else {
                self.buffer.push(count as u8);
            }

            // Write field names segment size (ONLY the names data, per Python)
            let names_len = u32::try_from(names_data.len())
                .map_err(|_| oson_error("JSON field names too large for OSON"))?;
            if (flags & FLAG_FNAMES_SEG_UINT32) != 0 {
                self.buffer.extend_from_slice(&names_len.to_be_bytes());
            } else {
                self.buffer
                    .extend_from_slice(&(names_len as u16).to_be_bytes());
            }

            // Write tree segment size
            tree_seg_size(&mut self.buffer);

            // Write number of tiny nodes (0)
            self.buffer.extend_from_slice(&0u16.to_be_bytes());

            // Write hash IDs, offsets, and field names data separately
            self.buffer.extend_from_slice(&hash_ids);
            self.buffer.extend_from_slice(&offsets);
            self.buffer.extend_from_slice(&names_data);
        }

        // Write tree segment
        self.buffer.extend_from_slice(&tree_seg);

        Ok(Bytes::from(std::mem::take(&mut self.buffer)))
    }

    /// Plenora fork: a name longer than 255 bytes and nesting deeper than
    /// [`OSON_MAX_DEPTH`] are refused here, before anything is written. The
    /// upstream encoder wrote the name length `as u8` (a 300-byte key became
    /// 44 bytes long) and recursed without bound.
    fn collect_field_names(&mut self, value: &serde_json::Value, depth: usize) -> Result<()> {
        match value {
            serde_json::Value::Object(_) | serde_json::Value::Array(_)
                if depth >= OSON_MAX_DEPTH =>
            {
                return Err(oson_error("JSON nesting deeper than the supported limit"));
            }
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    if key.len() > OSON_MAX_FIELD_NAME_BYTES {
                        return Err(oson_error("JSON field name longer than OSON encodes"));
                    }
                    if !self.field_name_to_id.contains_key(key) {
                        // Temporarily use 0 as placeholder - will be updated by sort_and_update_field_ids
                        self.field_names.push(key.clone());
                        self.field_name_to_id.insert(key.clone(), 0);
                    }
                    self.collect_field_names(child, depth + 1)?;
                }
            }
            serde_json::Value::Array(arr) => {
                for item in arr {
                    self.collect_field_names(item, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Sort field names by hash and update the ID mapping to match sorted order
    fn sort_and_update_field_ids(&mut self) {
        if self.field_names.is_empty() {
            return;
        }

        // Sort field names by hash (same logic as build_field_names_components)
        let mut sorted_names: Vec<_> = self.field_names.iter().cloned().enumerate().collect();
        // Plenora fork: ties are broken by the name bytes as well, so the
        // order (and the field ids) do not depend on insertion order.
        sorted_names.sort_by(|a, b| {
            let hash_a = bernstein_hash(a.1.as_bytes()) & 0xff;
            let hash_b = bernstein_hash(b.1.as_bytes()) & 0xff;
            hash_a
                .cmp(&hash_b)
                .then_with(|| a.1.len().cmp(&b.1.len()))
                .then_with(|| a.1.as_bytes().cmp(b.1.as_bytes()))
        });

        // Update field_name_to_id with sorted indices (1-based)
        for (sorted_index, (_, name)) in sorted_names.iter().enumerate() {
            self.field_name_to_id
                .insert(name.clone(), (sorted_index + 1) as u32);
        }

        // Also update field_names to be in sorted order for build_field_names_components
        self.field_names = sorted_names.into_iter().map(|(_, name)| name).collect();
    }

    /// Build field names components: (hash_ids, offsets, names_data)
    /// Returns three separate vectors per Python's OSON format:
    /// - hash_ids: 1 byte hash per field name
    /// - offsets: 2 byte BE offset per field name (into names_data)
    /// - names_data: length-prefixed field name strings
    /// Note: field_names must already be sorted by sort_and_update_field_ids
    ///
    /// Plenora fork: offsets are four bytes when the names segment exceeds
    /// 65535 bytes (the flag the decoder reads, `FLAG_FNAMES_SEG_UINT32`);
    /// the upstream encoder always wrote them `as u16`. Name lengths fit one
    /// byte because `collect_field_names` refused longer names.
    fn build_field_names_components(&self) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let mut hash_ids = Vec::new();
        let mut offsets_bytes = Vec::new();
        let mut names_data = Vec::new();

        // Build hash IDs (1 byte each) - field_names already sorted
        for name in &self.field_names {
            hash_ids.push(bernstein_hash(name.as_bytes()).to_le_bytes()[0]);
        }

        // Build names data and collect offsets
        let mut offsets = Vec::new();
        for name in &self.field_names {
            offsets.push(names_data.len());
            names_data.push(
                u8::try_from(name.len())
                    .map_err(|_| oson_error("JSON field name longer than OSON encodes"))?,
            );
            names_data.extend_from_slice(name.as_bytes());
        }

        // Build offsets bytes (2 or 4 bytes each)
        let wide = names_data.len() > 65535;
        for offset in &offsets {
            if wide {
                offsets_bytes.extend_from_slice(&(*offset as u32).to_be_bytes());
            } else {
                offsets_bytes.extend_from_slice(&(*offset as u16).to_be_bytes());
            }
        }

        Ok((hash_ids, offsets_bytes, names_data))
    }

    fn encode_node(&self, value: &serde_json::Value, buf: &mut Vec<u8>) -> Result<()> {
        match value {
            serde_json::Value::Array(arr) => self.encode_array(arr, buf),
            serde_json::Value::Object(map) => self.encode_object(map, buf),
            scalar => Self::encode_scalar(scalar, buf),
        }
    }

    /// Plenora fork: scalars encode outside the recursive path (see
    /// `OsonDecoder::decode_scalar`).
    #[inline(never)]
    fn encode_scalar(value: &serde_json::Value, buf: &mut Vec<u8>) -> Result<()> {
        match value {
            serde_json::Value::Null => {
                buf.push(TYPE_NULL);
            }
            serde_json::Value::Bool(true) => {
                buf.push(TYPE_TRUE);
            }
            serde_json::Value::Bool(false) => {
                buf.push(TYPE_FALSE);
            }
            serde_json::Value::Number(n) => {
                // Encode as Oracle NUMBER format
                let s = n.to_string();
                let num_bytes = crate::types::encode_oracle_number(&s)?;
                // Plenora fork: -0.0 would be stored as NUMBER 0, sign lost
                // (the same rule as a bound f64 -0.0, which is refused).
                if num_bytes == [128] && s.starts_with('-') {
                    return Err(Error::DataConversionError(
                        "Negative zero not representable in Oracle JSON".to_string(),
                    ));
                }
                buf.push(TYPE_NUMBER_LEN_U8);
                buf.push(num_bytes.len() as u8);
                buf.extend_from_slice(&num_bytes);
            }
            serde_json::Value::String(s) => {
                let bytes = s.as_bytes();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| oson_error("JSON string too long for OSON"))?;
                // The branches bound `len`, so each narrowing is exact.
                if len < 256 {
                    buf.push(TYPE_STRING_LEN_U8);
                    buf.push(len as u8);
                } else if len < 65536 {
                    buf.push(TYPE_STRING_LEN_U16);
                    buf.extend_from_slice(&(len as u16).to_be_bytes());
                } else {
                    buf.push(TYPE_STRING_LEN_U32);
                    buf.extend_from_slice(&len.to_be_bytes());
                }
                buf.extend_from_slice(bytes);
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                return Err(oson_error("container passed as an OSON scalar"));
            }
        }
        Ok(())
    }

    fn encode_array(&self, arr: &[serde_json::Value], buf: &mut Vec<u8>) -> Result<()> {
        let num_children = arr.len();

        // Node type for array with uint32 offsets
        let mut node_type: u8 = TYPE_ARRAY | 0x20;
        if num_children > 65535 {
            node_type |= 0x10;
        } else if num_children > 255 {
            node_type |= 0x08;
        }
        buf.push(node_type);

        write_child_count(buf, num_children)?;

        // Reserve space for offsets and remember where they start
        let offsets_start = buf.len();
        buf.resize(buf.len() + num_children * 4, 0);

        // Encode each child directly to buffer, recording its position
        for i in 0..num_children {
            let child_pos = buf.len();
            // Write offset at the reserved position
            let offset_bytes = tree_offset(child_pos)?.to_be_bytes();
            buf[offsets_start + i * 4..offsets_start + i * 4 + 4].copy_from_slice(&offset_bytes);
            // Encode the child
            self.encode_node(&arr[i], buf)?;
        }

        Ok(())
    }

    fn encode_object(
        &self,
        map: &serde_json::Map<String, serde_json::Value>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let num_children = map.len();

        // Node type for object with uint32 offsets
        let mut node_type: u8 = TYPE_OBJECT | 0x20;
        if num_children > 65535 {
            node_type |= 0x10;
        } else if num_children > 255 {
            node_type |= 0x08;
        }
        buf.push(node_type);

        write_child_count(buf, num_children)?;

        // Write field IDs. Plenora fork: two bytes each past 255 names (the
        // decoder reads `field_id_length` from FLAG_NUM_FNAMES_UINT16); the
        // upstream encoder wrote every id `as u8` and defaulted a missing
        // one to 0.
        let wide_ids = self.field_names.len() > 255;
        for key in map.keys() {
            let id = *self
                .field_name_to_id
                .get(key)
                .ok_or_else(|| oson_error("JSON field name without an OSON id"))?;
            if wide_ids {
                let id = u16::try_from(id).map_err(|_| oson_error("OSON field id overflow"))?;
                buf.extend_from_slice(&id.to_be_bytes());
            } else {
                buf.push(u8::try_from(id).map_err(|_| oson_error("OSON field id overflow"))?);
            }
        }

        // Reserve space for offsets and remember where they start
        let offsets_start = buf.len();
        buf.resize(buf.len() + num_children * 4, 0);

        // Encode each child directly to buffer, recording its position
        for (i, (_, value)) in map.iter().enumerate() {
            let child_pos = buf.len();
            // Write offset at the reserved position
            let offset_bytes = tree_offset(child_pos)?.to_be_bytes();
            buf[offsets_start + i * 4..offsets_start + i * 4 + 4].copy_from_slice(&offset_bytes);
            // Encode the child
            self.encode_node(value, buf)?;
        }

        Ok(())
    }
}

/// Plenora fork: the child count of a container, in the width its node type
/// announces (the type bits use the same thresholds).
fn write_child_count(buf: &mut Vec<u8>, num_children: usize) -> Result<()> {
    let count =
        u32::try_from(num_children).map_err(|_| oson_error("JSON container too large for OSON"))?;
    if count < 256 {
        buf.push(count as u8);
    } else if count < 65536 {
        buf.extend_from_slice(&(count as u16).to_be_bytes());
    } else {
        buf.extend_from_slice(&count.to_be_bytes());
    }
    Ok(())
}

/// Plenora fork: a position in the tree segment as a four-byte offset.
fn tree_offset(position: usize) -> Result<u32> {
    u32::try_from(position).map_err(|_| oson_error("JSON value too large for OSON"))
}

/// Bernstein hash function (FNV-1a variant used by Oracle)
fn bernstein_hash(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C9DC5;
    for &b in data {
        hash = (hash ^ b as u32).wrapping_mul(16777619);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oson_decode_null() {
        // Simple scalar null: magic + version + flags + tree_size + TYPE_NULL
        let data = vec![0xFF, 0x4A, 0x5A, 0x01, 0x00, 0x12, 0x00, 0x01, 0x30];
        let result = OsonDecoder::decode(Bytes::from(data));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::Value::Null);
    }

    #[test]
    fn test_oson_decode_true() {
        let data = vec![0xFF, 0x4A, 0x5A, 0x01, 0x00, 0x12, 0x00, 0x01, 0x31];
        let result = OsonDecoder::decode(Bytes::from(data));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::Value::Bool(true));
    }

    #[test]
    fn test_oson_decode_false() {
        let data = vec![0xFF, 0x4A, 0x5A, 0x01, 0x00, 0x12, 0x00, 0x01, 0x32];
        let result = OsonDecoder::decode(Bytes::from(data));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::Value::Bool(false));
    }

    #[test]
    fn test_oson_decode_empty_string() {
        // Inline empty string: node_type = 0x00
        let data = vec![0xFF, 0x4A, 0x5A, 0x01, 0x00, 0x12, 0x00, 0x01, 0x00];
        let result = OsonDecoder::decode(Bytes::from(data));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::Value::String(String::new()));
    }
}
