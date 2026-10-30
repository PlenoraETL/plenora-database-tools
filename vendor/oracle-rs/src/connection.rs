//! Oracle database connection
//!
//! This module provides the main `Connection` type for interacting with Oracle databases.
//!
//! # Example
//!
//! ```rust,ignore
//! use oracle_rs::{Connection, Config};
//!
//! #[tokio::main]
//! async fn main() -> oracle_rs::Result<()> {
//!     // Create a connection
//!     let conn = Connection::connect("localhost:1521/ORCLPDB1", "user", "password").await?;
//!
//!     // Execute a query
//!     let rows = conn.query("SELECT * FROM employees WHERE department_id = :1", &[&10]).await?;
//!
//!     for row in rows {
//!         println!("{:?}", row);
//!     }
//!
//!     // Commit and close
//!     conn.commit().await?;
//!     conn.close().await?;
//!     Ok(())
//! }
//! ```

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::batch::{BatchBinds, BatchResult};
use crate::buffer::{ReadBuffer, WriteBuffer};
use crate::capabilities::Capabilities;
use crate::config::{Config, ServiceMethod};
use crate::constants::{
    BindDirection, FetchOrientation, FunctionCode, MessageType, OracleType, PacketType,
    PACKET_HEADER_SIZE,
};
use crate::cursor::{ScrollResult, ScrollableCursor};
use crate::error::{Error, Result};
use crate::implicit::{ImplicitResult, ImplicitResults};
use crate::messages::{
    AcceptMessage, AuthMessage, AuthPhase, ConnectMessage, ExecuteMessage, ExecuteOptions,
    FetchMessage, LobOpMessage,
};
use crate::packet::{encode_packet, Packet};
use crate::row::{Row, Value};
use crate::statement::{BindParam, ColumnInfo, Statement, StatementType};
use crate::transport::{connect_tls, TlsConfig, TlsOracleStream};
use crate::types::{LobData, LobLocator, LobValue};

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// TCP connection established
    Connected,
    /// Protocol negotiation complete
    ProtocolNegotiated,
    /// Data types negotiated
    DataTypesNegotiated,
    /// Fully authenticated and ready
    Ready,
    /// Connection is closed
    Closed,
}

/// Options for query execution
#[derive(Debug, Clone)]
pub struct QueryOptions {
    /// Number of rows to prefetch
    pub prefetch_rows: u32,
    /// Array size for batch operations
    pub array_size: u32,
    /// Whether to auto-commit after DML
    pub auto_commit: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            prefetch_rows: 100,
            array_size: 100,
            auto_commit: false,
        }
    }
}

/// Result set from a query
#[derive(Debug)]
pub struct QueryResult {
    /// Column information
    pub columns: Vec<ColumnInfo>,
    /// Rows returned
    pub rows: Vec<Row>,
    /// Number of rows affected (for DML)
    pub rows_affected: u64,
    /// Whether there are more rows to fetch
    pub has_more_rows: bool,
    /// Cursor ID for subsequent fetches (needed for fetch_more)
    pub cursor_id: u16,
}

impl QueryResult {
    /// Create an empty query result
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            has_more_rows: false,
            cursor_id: 0,
        }
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get a column by name
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Get column index by name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Iterate over rows
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    /// Get a single row (first row)
    pub fn first(&self) -> Option<&Row> {
        self.rows.first()
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// Result from executing a PL/SQL block with OUT parameters
#[derive(Debug)]
pub struct PlsqlResult {
    /// OUT parameter values indexed by position (0-based)
    pub out_values: Vec<Value>,
    /// Number of rows affected (if applicable)
    pub rows_affected: u64,
    /// Cursor ID (if the result contains a REF CURSOR)
    pub cursor_id: Option<u16>,
    /// Implicit result sets returned via DBMS_SQL.RETURN_RESULT
    pub implicit_results: ImplicitResults,
}

impl PlsqlResult {
    /// Create an empty PL/SQL result
    pub fn empty() -> Self {
        Self {
            out_values: Vec::new(),
            rows_affected: 0,
            cursor_id: None,
            implicit_results: ImplicitResults::new(),
        }
    }

    /// Get an OUT value by position (0-based)
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.out_values.get(index)
    }

    /// Get a string OUT value by position
    pub fn get_string(&self, index: usize) -> Option<&str> {
        self.out_values.get(index).and_then(|v| v.as_str())
    }

    /// Get an integer OUT value by position
    pub fn get_integer(&self, index: usize) -> Option<i64> {
        self.out_values.get(index).and_then(|v| v.as_i64())
    }

    /// Get a float OUT value by position
    pub fn get_float(&self, index: usize) -> Option<f64> {
        self.out_values.get(index).and_then(|v| v.as_f64())
    }

    /// Get a cursor ID from OUT value by position (for REF CURSOR)
    pub fn get_cursor_id(&self, index: usize) -> Option<u16> {
        self.out_values.get(index).and_then(|v| v.as_cursor_id())
    }
}

/// Server information obtained during connection
#[derive(Debug, Clone, Default)]
pub struct ServerInfo {
    /// Oracle version string
    pub version: String,
    /// Server banner
    pub banner: String,
    /// Session ID (SID)
    pub session_id: u32,
    /// Serial number
    pub serial_number: u32,
    /// Instance name
    pub instance_name: Option<String>,
    /// Service name
    pub service_name: Option<String>,
    /// Database name
    pub database_name: Option<String>,
    /// Negotiated protocol version
    pub protocol_version: u16,
    /// Whether server supports OOB (out of band) data
    pub supports_oob: bool,
}

/// Stream type that can be either plain TCP or TLS-encrypted
enum OracleStream {
    /// Plain TCP connection
    Plain(TcpStream),
    /// TLS-encrypted connection
    Tls(TlsOracleStream),
    #[cfg(test)]
    WriteFault(tests::WriteFault),
}

impl OracleStream {
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            #[cfg(test)]
            OracleStream::WriteFault(_) => Err(std::io::ErrorKind::BrokenPipe.into()),
            OracleStream::Plain(stream) => {
                AsyncReadExt::read_exact(stream, buf).await?;
                Ok(())
            }
            OracleStream::Tls(stream) => {
                AsyncReadExt::read_exact(stream, buf).await?;
                Ok(())
            }
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            OracleStream::Plain(stream) => stream.write_all(buf).await,
            OracleStream::Tls(stream) => stream.write_all(buf).await,
            #[cfg(test)]
            OracleStream::WriteFault(fault) => fault.run(false).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            OracleStream::Plain(stream) => stream.flush().await,
            OracleStream::Tls(stream) => stream.flush().await,
            #[cfg(test)]
            OracleStream::WriteFault(fault) => fault.run(true).await,
        }
    }
}

/// Internal connection state shared across async operations
struct ConnectionInner {
    stream: Option<OracleStream>,
    capabilities: Capabilities,
    state: ConnectionState,
    server_info: ServerInfo,
    sdu_size: u32,
    large_sdu: bool,
    response_limit: usize,
    response_in_progress: bool,
    response_bytes: usize,
    /// Sequence number for TTC messages (increments per message)
    sequence_number: u8,
    /// Cursori server che il client non referenzia piu: partono col piggyback
    /// `CLOSE_CURSORS` davanti alla prossima chiamata TTC, come in
    /// python-oracledb thin. Ogni id compare una volta sola.
    cursors_to_close: Vec<u16>,
    /// Ultima riga letta di ogni risultato ancora aperto: la prima riga della
    /// pagina successiva puo ripeterne le colonne (bit vector), e senza di
    /// essa quelle colonne diventavano NULL.
    last_rows: std::collections::HashMap<u16, Vec<Value>>,
}

/// Cursori chiusi da un solo piggyback: il resto aspetta la chiamata dopo.
const MAX_CURSORS_PER_PIGGYBACK: usize = 500;

/// Versione TTC dal cui valore ogni messaggio porta il token number (23ai).
const TTC_FIELD_VERSION_TOKEN: u8 = 18;

/// Codifica il piggyback `CLOSE_CURSORS` (python-oracledb thin,
/// `_write_close_cursors_piggyback`): codice, sequenza, token dal 23ai,
/// puntatore, numero di cursori e poi ogni id, tutti UB4.
fn write_close_cursors_piggyback(
    buf: &mut WriteBuffer,
    sequence_number: u8,
    ttc_field_version: u8,
    cursor_ids: &[u16],
) -> Result<()> {
    let count = u32::try_from(cursor_ids.len())
        .map_err(|_| Error::Protocol("Too many cursors to close".into()))?;
    buf.write_u8(MessageType::Piggyback as u8)?;
    buf.write_u8(FunctionCode::CloseCursors as u8)?;
    buf.write_u8(sequence_number)?;
    if ttc_field_version >= TTC_FIELD_VERSION_TOKEN {
        buf.write_ub8(0)?; // token number
    }
    buf.write_u8(1)?; // pointer
    buf.write_ub4(count)?;
    for cursor_id in cursor_ids {
        buf.write_ub4(u32::from(*cursor_id))?;
    }
    Ok(())
}

// The checkpoint advances only after a complete TTC message. An underflow
// retries just that message after the next frame, never searches cell bytes.
struct PageFraming<'a> {
    params: &'a [BindParam],
    row_counts: bool,
    lob: Option<(usize, bool)>,
    lob_tail: usize,
    offset: usize,
    columns: std::borrow::Cow<'a, [ColumnInfo]>,
    bit_vector: Option<Vec<u8>>,
}

impl<'a> PageFraming<'a> {
    fn new(columns: &'a [ColumnInfo]) -> Self {
        Self {
            params: &[],
            row_counts: false,
            lob: None,
            lob_tail: 0,
            offset: 0,
            columns: std::borrow::Cow::Borrowed(columns),
            bit_vector: None,
        }
    }

    fn advance(&mut self, data: &bytes::Bytes, caps: &Capabilities) -> Result<bool> {
        let start = self.offset;
        let mut buf = ReadBuffer::new(data.slice(start..));
        while buf.remaining() > 0 {
            let mut columns = None;
            let mut bit_vector = None;
            let mut clear_bits = false;
            let parsed = (|| -> Result<bool> {
                let kind = buf.read_u8()?;
                match kind {
                    x if x == MessageType::Error as u8 => {
                        Connection::parse_error_info_with_rowcount(&mut buf, caps)?;
                        return Ok(!caps.supports_end_of_response);
                    }
                    x if x == MessageType::Status as u8 => {
                        buf.read_ub4()?;
                        buf.read_ub2()?;
                        return Ok(!caps.supports_end_of_response);
                    }
                    x if x == MessageType::EndOfResponse as u8 => return Ok(true),
                    x if x == MessageType::DescribeInfo as u8 => {
                        buf.skip_raw_bytes_chunked()?;
                        columns = Some(Connection::parse_describe_info(
                            &mut buf,
                            caps.ttc_field_version,
                        )?);
                    }
                    x if x == MessageType::RowHeader as u8 => {
                        bit_vector = Connection::parse_row_header(&mut buf)?;
                    }
                    x if x == MessageType::RowData as u8 => {
                        if self.columns.is_empty() {
                            return Err(Error::Protocol("Oracle row without column layout".into()));
                        }
                        // Framing only advances over the row: a duplicate
                        // column carries no bytes and its value is discarded.
                        let discarded = vec![Value::Null; self.columns.len()];
                        Connection::parse_row_data_with_bitvector(
                            &mut buf,
                            &self.columns,
                            caps,
                            self.bit_vector.as_deref(),
                            Some(&discarded),
                        )?;
                        clear_bits = true;
                    }
                    x if x == MessageType::BitVector as u8 => {
                        buf.read_ub2()?;
                        bit_vector = Some(buf.read_bytes_vec((self.columns.len() + 7) / 8)?);
                    }
                    x if x == MessageType::Parameter as u8 => {
                        if let Some((locator_len, amount)) = self.lob {
                            buf.skip(locator_len)?;
                            if self.lob_tail == 2 {
                                buf.skip_ub2()?;
                            }
                            if self.lob_tail != 0 {
                                buf.skip(1)?;
                            }
                            if amount {
                                buf.read_ub8()?;
                            }
                        } else {
                            Connection::parse_return_parameters_internal(
                                &mut buf,
                                self.row_counts,
                            )?;
                        }
                    }
                    x if x == MessageType::LobData as u8 => buf.skip_raw_bytes_chunked()?,
                    x if x == MessageType::IoVector as u8 => {
                        let output_columns = Connection::parse_io_vector(&mut buf, self.params)?.1;
                        if !output_columns.is_empty() {
                            columns = Some(output_columns);
                        }
                    }
                    x if x == MessageType::FlushOutBinds as u8 => {}
                    x if x == MessageType::ImplicitResultset as u8 => {
                        Connection::parse_implicit_results(&mut buf, caps)?;
                    }
                    x if x == MessageType::ServerSidePiggyback as u8 => {
                        Connection::parse_server_piggyback(&mut buf)?;
                    }
                    _ => {
                        return Err(Error::Protocol(
                            "Unsupported Oracle response framing message".into(),
                        ))
                    }
                }
                Ok(false)
            })();
            match parsed {
                Ok(terminal) => {
                    self.offset = start + buf.position();
                    if let Some(columns) = columns {
                        self.columns = std::borrow::Cow::Owned(columns);
                    }
                    if clear_bits {
                        self.bit_vector = None;
                    }
                    if let Some(bits) = bit_vector {
                        self.bit_vector = Some(bits);
                    }
                    if terminal {
                        return Ok(true);
                    }
                }
                Err(Error::BufferUnderflow { .. }) => return Ok(false),
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }
}

impl ConnectionInner {
    fn new() -> Self {
        Self {
            stream: None,
            capabilities: Capabilities::default(),
            state: ConnectionState::Disconnected,
            server_info: ServerInfo::default(),
            sdu_size: 8192,
            large_sdu: false,
            response_limit: crate::config::DEFAULT_RESPONSE_LIMIT,
            response_in_progress: false,
            response_bytes: 0,
            sequence_number: 0,
            cursors_to_close: Vec::new(),
            last_rows: std::collections::HashMap::new(),
        }
    }

    /// Il client non referenzia piu `cursor_id`: il server lo chiudera
    /// davanti alla prossima chiamata. Un id gia in coda non si ripete,
    /// perche dopo la chiusura il server puo riassegnarlo.
    fn release_cursor(&mut self, cursor_id: u16) {
        self.last_rows.remove(&cursor_id);
        if cursor_id != 0 && !self.cursors_to_close.contains(&cursor_id) {
            self.cursors_to_close.push(cursor_id);
        }
    }

    /// Un fetch che esaurisce il risultato, o che fallisce, chiude il cursore:
    /// il chiamante non lo legge piu.
    fn release_finished_fetch(&mut self, cursor_id: u16, result: &Result<QueryResult>) {
        match result {
            Ok(page) if page.has_more_rows => self.remember_last_row(cursor_id, page),
            _ => self.release_cursor(cursor_id),
        }
    }

    /// Conserva l'ultima riga di una pagina con altre righe dopo.
    fn remember_last_row(&mut self, cursor_id: u16, page: &QueryResult) {
        if let Some(row) = page.rows.last() {
            self.last_rows.insert(cursor_id, row.values().to_vec());
        }
    }

    /// Antepone a una chiamata TTC (`message` comincia dal tipo messaggio)
    /// il piggyback dei cursori in coda. Il piggyback prende la sequenza
    /// della chiamata e la chiamata la successiva, nell'ordine di scrittura di
    /// python-oracledb. `None` se non c'e niente da chiudere o se il messaggio
    /// non e una chiamata. Con il messaggio torna quanti cursori porta: li
    /// toglie dalla coda [`Self::closed_cursors_sent`] quando la richiesta e
    /// pronta da inviare, cosi nessuno si perde.
    fn close_cursors_prefix(&mut self, message: &[u8]) -> Result<Option<(Vec<u8>, usize)>> {
        if self.cursors_to_close.is_empty()
            || message.len() < 3
            || message[0] != MessageType::Function as u8
        {
            return Ok(None);
        }
        let count = self.cursors_to_close.len().min(MAX_CURSORS_PER_PIGGYBACK);
        let mut buf = WriteBuffer::new();
        write_close_cursors_piggyback(
            &mut buf,
            message[2],
            self.capabilities.ttc_field_version,
            &self.cursors_to_close[..count],
        )?;
        let mut prefixed = Vec::with_capacity(buf.len() + message.len());
        prefixed.extend_from_slice(buf.as_slice());
        prefixed.extend_from_slice(&message[..2]);
        prefixed.push(self.next_sequence_number());
        prefixed.extend_from_slice(&message[3..]);
        Ok(Some((prefixed, count)))
    }

    fn closed_cursors_sent(&mut self, count: usize) {
        self.cursors_to_close.drain(..count);
    }

    /// Lo stesso piggyback dentro un pacchetto DATA gia costruito; se supera
    /// l'SDU, `send` lo divide.
    fn attach_close_cursors(&mut self, packet: &[u8]) -> Result<Option<bytes::Bytes>> {
        let data_start = PACKET_HEADER_SIZE + 2;
        if self.cursors_to_close.is_empty()
            || packet.len() <= data_start
            || packet[4] != PacketType::Data as u8
        {
            return Ok(None);
        }
        let Some((message, count)) = self.close_cursors_prefix(&packet[data_start..])? else {
            return Ok(None);
        };
        let mut payload = Vec::with_capacity(2 + message.len());
        payload.extend_from_slice(&packet[PACKET_HEADER_SIZE..data_start]);
        payload.extend_from_slice(&message);
        let packet = encode_packet(PacketType::Data, &payload, self.large_sdu)?;
        self.closed_cursors_sent(count);
        Ok(Some(packet))
    }

    /// Get the next sequence number (auto-increments, wraps at 255 to 1)
    fn next_sequence_number(&mut self) -> u8 {
        self.sequence_number = self.sequence_number.wrapping_add(1);
        if self.sequence_number == 0 {
            self.sequence_number = 1;
        }
        self.sequence_number
    }

    async fn send(&mut self, data: &[u8]) -> Result<()> {
        if self.response_in_progress {
            return Err(Error::ConnectionClosed);
        }
        let attached = self.attach_close_cursors(data)?;
        let data = attached.as_deref().unwrap_or(data);
        // Plenora fork: a DATA packet longer than the negotiated SDU (an
        // execute with binds above about 8 KB) was written as one frame and
        // the server dropped the connection. It is sent as SDU-sized frames,
        // the data flags on the first, as python-oracledb does.
        if data.len() > self.sdu_size as usize
            && data.get(4) == Some(&(PacketType::Data as u8))
            && data.len() >= PACKET_HEADER_SIZE + 2
        {
            let flags =
                u16::from_be_bytes([data[PACKET_HEADER_SIZE], data[PACKET_HEADER_SIZE + 1]]);
            // Boxed: inlined, this rare path would enlarge every future that
            // sends a packet.
            return Box::pin(self.send_multi_packet(&data[PACKET_HEADER_SIZE + 2..], flags)).await;
        }
        // Moving the socket into the future makes both errors and cancellation fatal.
        let mut stream = self.stream.take().ok_or(Error::ConnectionClosed)?;
        if data.get(4) == Some(&(PacketType::Marker as u8)) {
            self.response_bytes = self.response_bytes.saturating_add(data.len());
            if self.response_bytes > self.response_limit {
                return Err(Error::Protocol(
                    "Oracle response byte limit exceeded".into(),
                ));
            }
        } else {
            self.response_bytes = 0;
        }
        stream.write_all(data).await?;
        stream.flush().await?;
        self.stream = Some(stream);
        Ok(())
    }

    async fn renegotiate_tls(&mut self, server_name: &str, config: &TlsConfig) -> Result<()> {
        let stream = self.stream.take().ok_or(Error::ConnectionClosed)?;
        let tls_stream = match stream {
            #[cfg(test)]
            OracleStream::WriteFault(_) => return Err(Error::ConnectionClosed),
            OracleStream::Tls(stream) => {
                stream.renegotiate(server_name, config).await.map_err(|_| {
                    Error::ProtocolError(
                        "Oracle TCPS second handshake was not completed".to_string(),
                    )
                })?
            }
            OracleStream::Plain(stream) => {
                self.stream = Some(OracleStream::Plain(stream));
                return Err(Error::ProtocolError(
                    "TLS renegotiation requested on a plaintext connection".to_string(),
                ));
            }
        };
        self.stream = Some(OracleStream::Tls(tls_stream));
        Ok(())
    }

    fn build_data_chunk(payload: &[u8], flags: u16, large_sdu: bool) -> Result<bytes::Bytes> {
        let mut buf = WriteBuffer::new();
        buf.write_u16_be(flags)?;
        buf.write_bytes(payload)?;
        encode_packet(PacketType::Data, buf.as_slice(), large_sdu)
    }

    /// Send a payload that may need to be split across multiple packets.
    ///
    /// This is used for large LOB writes and other operations where the payload
    /// exceeds the SDU size. The payload is split into multiple DATA packets,
    /// each with proper headers.
    ///
    /// # Arguments
    /// * `payload` - The raw message payload (without packet header or data flags)
    /// * `data_flags` - The data flags for the first packet (typically 0)
    async fn send_multi_packet(&mut self, payload: &[u8], data_flags: u16) -> Result<()> {
        if self.response_in_progress {
            return Err(Error::ConnectionClosed);
        }
        let attached = match self.close_cursors_prefix(payload)? {
            Some((message, count)) => {
                self.closed_cursors_sent(count);
                Some(message)
            }
            None => None,
        };
        let payload = attached.as_deref().unwrap_or(payload);
        let mut stream = self.stream.take().ok_or(Error::ConnectionClosed)?;
        self.response_bytes = 0;

        // Calculate max payload per packet: SDU - header (8) - data flags (2)
        let max_payload_per_packet = self.sdu_size as usize - PACKET_HEADER_SIZE - 2;

        let mut offset = 0;
        let mut is_first = true;

        while offset < payload.len() {
            let remaining = payload.len() - offset;
            let chunk_size = std::cmp::min(remaining, max_payload_per_packet);
            let is_last = offset + chunk_size >= payload.len();

            let flags = if is_first { data_flags } else { 0 };
            is_first = false;
            let packet = Self::build_data_chunk(
                &payload[offset..offset + chunk_size],
                flags,
                self.large_sdu,
            )?;
            stream.write_all(&packet).await?;

            offset += chunk_size;

            // Don't flush until the last packet to improve performance
            if is_last {
                stream.flush().await?;
            }
        }

        self.stream = Some(stream);
        Ok(())
    }

    async fn receive(&mut self) -> Result<bytes::Bytes> {
        self.receive_limited(self.response_limit.saturating_sub(self.response_bytes))
            .await
    }

    // Take ownership while reading: an error or cancelled read drops the channel.
    async fn receive_limited(&mut self, remaining: usize) -> Result<bytes::Bytes> {
        let mut stream = self.stream.take().ok_or(Error::ConnectionClosed)?;
        let mut header = [0u8; PACKET_HEADER_SIZE];
        stream.read_exact(&mut header).await?;
        let packet_len = if self.large_sdu {
            u32::from_be_bytes(header[..4].try_into().unwrap()) as usize
        } else {
            u16::from_be_bytes(header[..2].try_into().unwrap()) as usize
        };
        // The negotiated SDU bounds each frame; the cumulative wire budget also
        // counts headers/flags, so empty frames cannot evade the response limit.
        if packet_len < PACKET_HEADER_SIZE
            || packet_len > self.sdu_size as usize
            || packet_len > remaining
        {
            self.state = ConnectionState::Disconnected;
            return Err(Error::Protocol(
                "Oracle response byte limit or frame length violated".into(),
            ));
        }
        let mut packet = vec![0; packet_len];
        packet[..PACKET_HEADER_SIZE].copy_from_slice(&header);
        stream.read_exact(&mut packet[PACKET_HEADER_SIZE..]).await?;
        self.response_bytes += packet_len;
        self.stream = Some(stream);
        Ok(bytes::Bytes::from(packet))
    }

    async fn receive_page(&mut self) -> Result<bytes::Bytes> {
        self.receive_page_with_columns(&[]).await
    }

    async fn receive_page_with_columns(&mut self, columns: &[ColumnInfo]) -> Result<bytes::Bytes> {
        self.receive_response_checked(PageFraming::new(columns))
            .await
    }

    async fn receive_response_checked(&mut self, framing: PageFraming<'_>) -> Result<bytes::Bytes> {
        if self.response_in_progress {
            return Err(Error::ConnectionClosed);
        }
        self.response_in_progress = true;
        let result = self.collect_response(framing).await;
        if let Ok(packet) = &result {
            self.response_in_progress = packet[4] == PacketType::Marker as u8;
        } else {
            self.stream = None;
            self.state = ConnectionState::Disconnected;
        }
        result
    }

    async fn receive_lob_response<T>(
        &mut self,
        locator_len: usize,
        amount: bool,
        tail: usize,
        parse: impl FnOnce(&[u8], &Capabilities) -> Result<T>,
    ) -> Result<T> {
        let mut framing = PageFraming::new(&[]);
        framing.lob = Some((locator_len, amount));
        framing.lob_tail = tail;
        let result = async {
            let mut response = self.receive_response_checked(framing).await?;
            if response[4] == PacketType::Marker as u8 {
                response = self.handle_marker_reset().await?;
            }
            if response.len() < PACKET_HEADER_SIZE + 2 || response[4] != PacketType::Data as u8 {
                return Err(Error::Protocol("Invalid LOB response frame".into()));
            }
            parse(&response[PACKET_HEADER_SIZE..], &self.capabilities)
        }
        .await;
        self.checked_result(result)
    }

    // A fully collected Oracle error leaves no unread exchange. Decode failures do.
    fn checked_parse_result<T>(&mut self, result: Result<T>) -> Result<T> {
        if matches!(&result, Err(Error::OracleError { .. })) {
            result
        } else {
            self.checked_result(result)
        }
    }

    fn checked_result<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.stream = None;
            self.state = ConnectionState::Disconnected;
        }
        result
    }

    async fn collect_response(&mut self, mut framing: PageFraming<'_>) -> Result<bytes::Bytes> {
        use crate::constants::data_flags;
        let mut result = bytes::BytesMut::new();
        loop {
            // Bound the new frame before allocation while the accumulated page lives.
            let remaining = self.response_limit.saturating_sub(self.response_bytes);
            let frame_space = self.response_limit.saturating_sub(result.capacity());
            let packet = self.receive_limited(remaining.min(frame_space)).await?;
            if packet[4] != PacketType::Data as u8 {
                // BREAK aborts a partial result; the caller performs the
                // MARKER/reset handshake before reading the server error.
                if packet[4] != PacketType::Marker as u8 {
                    return Err(Error::Protocol(
                        "Non-DATA frame inside Oracle response".into(),
                    ));
                }
                return Ok(packet);
            }
            if packet.len() < PACKET_HEADER_SIZE + 2 {
                return Err(Error::Protocol("DATA packet payload too small".into()));
            }
            let flags = u16::from_be_bytes([packet[8], packet[9]]);
            let data = if result.is_empty() {
                &packet[..]
            } else {
                &packet[10..]
            };
            let needed = result.len().saturating_add(data.len());
            // Framing retains metadata and decodes at most one message at a time.
            let scratch = needed
                .saturating_mul(16)
                .saturating_add(framing.columns.len().saturating_mul(1024));
            let available = self
                .response_limit
                .saturating_sub(packet.len().saturating_add(scratch));
            if needed > available || result.capacity() > available {
                return Err(Error::Protocol(
                    "Oracle response memory limit exceeded".into(),
                ));
            }
            if needed > result.capacity() {
                // The old and new allocations coexist while growing.
                let available = available.saturating_sub(result.capacity());
                if needed > available {
                    return Err(Error::Protocol(
                        "Oracle response memory limit exceeded".into(),
                    ));
                }
                let capacity = needed
                    .max(result.capacity().saturating_mul(2))
                    .min(available);
                let mut grown = bytes::BytesMut::with_capacity(capacity);
                grown.extend_from_slice(&result);
                result = grown;
            }
            result.extend_from_slice(data);
            let flagged = flags & (data_flags::END_OF_RESPONSE | data_flags::EOF) != 0;
            let frozen = result.freeze();
            let terminal = framing.advance(&frozen.slice(10..), &self.capabilities)?;
            result = frozen
                .try_into_mut()
                .map_err(|_| Error::Protocol("Oracle response buffer still shared".into()))?;
            if flagged {
                if !terminal || framing.offset != result.len() - 10 {
                    return Err(Error::Protocol("Incomplete Oracle response".into()));
                }
                break;
            }
            if terminal {
                if framing.offset != result.len() - 10 {
                    return Err(Error::Protocol("Trailing Oracle response data".into()));
                }
                break;
            }
        }
        // Consumers use the payload, not the synthetic length. Encode the
        // length only when representable (a legacy response may exceed u16).
        let len = result.len();
        if self.large_sdu {
            result[..4].copy_from_slice(&(len as u32).to_be_bytes());
        } else {
            result[..2].copy_from_slice(&u16::try_from(len).unwrap_or(0).to_be_bytes());
        }
        self.response_bytes = 0;
        Ok(result.freeze())
    }

    /// Send a marker packet with the specified marker type
    async fn send_marker(&mut self, marker_type: u8) -> Result<()> {
        let packet = self.build_marker(marker_type)?;
        // RESET belongs to the pending exchange; keep its guard across both awaits.
        let mut stream = self.stream.take().ok_or(Error::ConnectionClosed)?;
        self.response_bytes = self.response_bytes.saturating_add(packet.len());
        if self.response_bytes > self.response_limit {
            self.state = ConnectionState::Disconnected;
            return Err(Error::Protocol(
                "Oracle response byte limit exceeded".into(),
            ));
        }
        stream.write_all(&packet).await?;
        stream.flush().await?;
        self.stream = Some(stream);
        Ok(())
    }

    fn build_simple_function(&mut self, function_code: FunctionCode) -> Result<bytes::Bytes> {
        // Build function message
        let mut buf = WriteBuffer::new();

        // Get next sequence number (must be done before sending)
        let seq_num = self.next_sequence_number();

        // Data flags
        buf.write_u16_be(0)?;
        // Message type: Function
        buf.write_u8(MessageType::Function as u8)?;
        // Function code
        buf.write_u8(function_code as u8)?;
        // Sequence number (tracked per connection)
        buf.write_u8(seq_num)?;

        // Token number (required for TTC field version >= 18, i.e. Oracle 23ai)
        if self.capabilities.ttc_field_version >= 18 {
            buf.write_ub8(0)?;
        }

        encode_packet(PacketType::Data, buf.as_slice(), self.large_sdu)
    }

    fn build_marker(&self, marker_type: u8) -> Result<bytes::Bytes> {
        encode_packet(PacketType::Marker, &[1, 0, marker_type], self.large_sdu)
    }

    /// Handle the reset protocol after receiving a MARKER packet
    /// This sends a reset marker, waits for the reset response, then returns the error packet
    /// Un errore di trasporto durante il reset resta un errore, senza attribuirlo al server.
    async fn handle_marker_reset(&mut self) -> Result<bytes::Bytes> {
        self.response_in_progress = true;
        let result = async {
            self.send_marker(2).await?;
            loop {
                let packet = self.collect_response(PageFraming::new(&[])).await?;
                if packet[4] == PacketType::Marker as u8 {
                    if packet.len() != PACKET_HEADER_SIZE + 3
                        || !matches!(packet[PACKET_HEADER_SIZE + 2], 1 | 2)
                    {
                        return Err(Error::Protocol("Invalid Oracle reset marker".into()));
                    }
                    continue;
                }
                if packet[4] != PacketType::Data as u8 {
                    return Err(Error::Protocol("Invalid Oracle reset response".into()));
                }
                return Ok(packet);
            }
        }
        .await;
        if result.is_ok() {
            self.response_in_progress = false;
        }
        self.checked_result(result)
    }
}

/// An Oracle database connection.
///
/// This is the main type for interacting with Oracle databases. It provides
/// methods for executing queries, DML statements, PL/SQL blocks, and managing
/// transactions.
///
/// Connections are created using [`Connection::connect`] or
/// [`Connection::connect_with_config`]. For connection pooling, use the
/// `deadpool-oracle` crate.
///
/// # Example
///
/// ```rust,no_run
/// use oracle_rs::{Config, Connection, Value};
///
/// # async fn example() -> oracle_rs::Result<()> {
/// // Create a connection
/// let config = Config::new("localhost", 1521, "FREEPDB1", "user", "password");
/// let conn = Connection::connect_with_config(config).await?;
///
/// // Execute a query
/// let result = conn.query("SELECT * FROM employees WHERE dept_id = :1", &[10.into()]).await?;
/// for row in &result.rows {
///     let name = row.get_by_name("name").and_then(|v| v.as_str()).unwrap_or("");
///     println!("Employee: {}", name);
/// }
///
/// // Execute DML with transaction
/// conn.execute("INSERT INTO logs (msg) VALUES (:1)", &["Hello".into()]).await?;
/// conn.commit().await?;
///
/// // Close the connection
/// conn.close().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Thread Safety
///
/// `Connection` is `Send` and `Sync`, but operations are serialized internally
/// via a mutex. For parallel query execution, use multiple connections (e.g.,
/// via a connection pool).
pub struct Connection {
    inner: Arc<Mutex<ConnectionInner>>,
    config: Config,
    closed: AtomicBool,
    id: u32,
    /// Cursore dello statement riportato dall'ultimo completamento TTC
    /// (anche di errore): i parser lo scrivono e le chiamate lo leggono
    /// dentro la stessa sezione sotto il lock di `inner`.
    reported_cursor: AtomicU16,
}

// Connection ID counter
static CONNECTION_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

impl Connection {
    /// Create a new connection to an Oracle database
    ///
    /// # Arguments
    ///
    /// * `connect_string` - Connection string in EZConnect format (e.g., "host:port/service")
    /// * `username` - Database username
    /// * `password` - Database password
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let conn = Connection::connect("localhost:1521/ORCLPDB1", "scott", "tiger").await?;
    /// ```
    pub async fn connect(connect_string: &str, username: &str, password: &str) -> Result<Self> {
        let mut config: Config = connect_string.parse()?;
        config.username = username.to_string();
        config.set_password(password);
        Self::connect_with_config(config).await
    }

    /// Create a new connection using a [`Config`].
    ///
    /// This is the preferred way to create connections as it gives full control
    /// over connection parameters including TLS, timeouts, and statement caching.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use oracle_rs::{Config, Connection};
    ///
    /// # async fn example() -> oracle_rs::Result<()> {
    /// let config = Config::new("localhost", 1521, "FREEPDB1", "user", "password")
    ///     .with_statement_cache_size(50);
    ///
    /// let conn = Connection::connect_with_config(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_config(config: Config) -> Result<Self> {
        let id = CONNECTION_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Create TCP connection
        let addr = config.socket_addr();
        let tcp_stream = TcpStream::connect(&addr).await?;

        // Set TCP options
        tcp_stream.set_nodelay(true)?;

        // Wrap with TLS if configured
        let stream = if config.is_tls_enabled() {
            let tls_config = config
                .tls_config
                .as_ref()
                .cloned()
                .unwrap_or_else(TlsConfig::new);

            let tls_stream = connect_tls(tcp_stream, &config.host, &tls_config).await?;
            OracleStream::Tls(tls_stream)
        } else {
            OracleStream::Plain(tcp_stream)
        };

        let mut inner = ConnectionInner::new();
        inner.response_limit = config.response_limit;
        inner.stream = Some(stream);
        inner.state = ConnectionState::Connected;

        let conn = Connection {
            inner: Arc::new(Mutex::new(inner)),
            config,
            closed: AtomicBool::new(false),
            id,
            reported_cursor: AtomicU16::new(0),
        };

        // Perform connection handshake
        conn.perform_handshake().await?;

        Ok(conn)
    }

    /// Get the connection ID
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Check if the connection is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Mark the connection as closed
    ///
    /// This should be called when the underlying connection is detected as broken.
    /// Once marked closed, `is_closed()` returns true and operations will fail fast.
    pub fn mark_closed(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }

    /// Helper to mark connection as closed if the result is a connection error
    fn handle_result<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(ref e) = result {
            if e.is_connection_error() {
                self.mark_closed();
            }
        }
        result
    }

    fn report_cursor(&self, cursor_id: u16) {
        if cursor_id != 0 {
            self.reported_cursor.store(cursor_id, Ordering::Relaxed);
        }
    }

    fn take_reported_cursor(&self) -> u16 {
        self.reported_cursor.swap(0, Ordering::Relaxed)
    }

    /// Get server information
    pub async fn server_info(&self) -> ServerInfo {
        let inner = self.inner.lock().await;
        inner.server_info.clone()
    }

    /// Get the current connection state
    pub async fn state(&self) -> ConnectionState {
        let inner = self.inner.lock().await;
        inner.state
    }

    /// Perform the connection handshake
    async fn perform_handshake(&self) -> Result<()> {
        // Step 1: Send CONNECT packet and parse ACCEPT response
        self.send_connect_packet().await?;

        // Step 2: OOB check (required for protocol version >= 318 AND server supports OOB)
        // Both conditions must be met - server must have indicated OOB support in ACCEPT
        let needs_oob_check = {
            let inner = self.inner.lock().await;
            inner.server_info.protocol_version >= crate::constants::version::MIN_OOB_CHECK
                && inner.server_info.supports_oob
        };
        if needs_oob_check {
            self.send_oob_check().await?;
        }

        // Step 3: Protocol negotiation
        self.negotiate_protocol().await?;

        // Step 4: Data types negotiation
        self.negotiate_data_types().await?;

        // Step 5: Authentication
        self.authenticate().await?;

        Ok(())
    }

    /// Send OOB (Out of Band) check
    /// Required for protocol version >= 318
    async fn send_oob_check(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Step 1: Send raw byte "!" (0x21) for OOB check
        inner.send(&[0x21]).await?;

        // Step 2: Send MARKER packet with Reset
        let marker_payload = [1u8, 0u8, crate::constants::MarkerType::Reset as u8];
        let packet = encode_packet(PacketType::Marker, &marker_payload, large_sdu)?;
        inner.send(&packet).await?;

        // Step 3: Wait for OOB reset response
        // The server sends back a MARKER packet or reset acknowledgment
        let response = inner.receive().await?;

        // Validate response - should be a MARKER packet type (12)
        if response.len() > 4 && response[4] == PacketType::Marker as u8 {
            Ok(())
        } else {
            // Server might just acknowledge without a specific packet
            // This is acceptable in some Oracle versions
            Ok(())
        }
    }

    /// Send the initial CONNECT packet
    async fn send_connect_packet(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;

        // Build connect packet using ConnectMessage for proper packet format
        let connect_msg = ConnectMessage::from_config(&self.config);
        let (connect_packet, continuation) = connect_msg.build_with_continuation()?;

        // Send the CONNECT packet
        inner.send(&connect_packet).await?;

        // If we have a continuation DATA packet (for large connect strings), send it
        if let Some(ref data_packet) = continuation {
            inner.send(data_packet).await?;
        }

        const MAX_RESENDS: u8 = 3;
        let mut resend_count: u8 = 0;

        loop {
            // Wait for response
            let response = inner.receive().await?;

            // Parse response packet type
            if response.len() < PACKET_HEADER_SIZE {
                return Err(Error::PacketTooShort {
                    expected: PACKET_HEADER_SIZE,
                    actual: response.len(),
                });
            }

            let packet_type = response[4];

            match packet_type {
                2 => {
                    // ACCEPT - parse the accept message to get protocol version and capabilities
                    let packet = Packet::from_bytes(response)?;
                    let accept = AcceptMessage::parse(&packet)?;

                    // Set large_sdu mode if protocol version >= 315
                    inner.large_sdu = accept.uses_large_sdu();

                    // Update server info
                    inner.server_info.protocol_version = accept.protocol_version;
                    inner.server_info.supports_oob = accept.supports_oob;
                    // Plenora fork: an SDU too small for a packet header and
                    // the data flags made `send_multi_packet` underflow.
                    if accept.sdu <= (PACKET_HEADER_SIZE + 2) as u32 {
                        return Err(Error::Protocol("server SDU too small".to_string()));
                    }
                    inner.sdu_size = accept.sdu;

                    inner.state = ConnectionState::Connected;
                    return Ok(());
                }
                4 => {
                    // REFUSE
                    let mut buf = ReadBuffer::new(response.slice(PACKET_HEADER_SIZE..));
                    let _reason = buf.read_u8()?;
                    let _user_reason = buf.read_u8()?;

                    return Err(Error::ConnectionRefused {
                        error_code: None,
                        message: Some("Connection refused by server".to_string()),
                    });
                }
                5 => {
                    // REDIRECT
                    return Err(Error::ConnectionRedirect(
                        "redirect not implemented".to_string(),
                    ));
                }
                11 => {
                    // In the traditional TCPS flow the listener answers with
                    // RESEND + TLS_RENEG, hands the socket to the database
                    // process and expects a fresh, independently verified TLS
                    // session before CONNECT is sent again.
                    resend_count += 1;
                    if resend_count > MAX_RESENDS {
                        return Err(Error::ProtocolError(
                            "Server requested too many resends during connect".to_string(),
                        ));
                    }
                    if response[5] & crate::constants::packet_flags::TLS_RENEG != 0 {
                        let tls_config = self.config.tls_config.as_ref().ok_or_else(|| {
                            Error::ProtocolError(
                                "TLS renegotiation requested without TLS configuration".to_string(),
                            )
                        })?;
                        inner.renegotiate_tls(&self.config.host, tls_config).await?;
                        inner.server_info.supports_oob = false;
                    }
                    inner.send(&connect_packet).await?;
                    if let Some(ref data_packet) = continuation {
                        inner.send(data_packet).await?;
                    }
                }
                _ => {
                    return Err(Error::ProtocolError(format!(
                        "Unexpected packet type during connect: {}",
                        packet_type,
                    )));
                }
            }
        }
    }

    /// Negotiate protocol version and capabilities
    async fn negotiate_protocol(&self) -> Result<()> {
        use crate::messages::ProtocolMessage;

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Build protocol request (includes header)
        let protocol_msg = ProtocolMessage::new();
        let packet = protocol_msg.build_request(large_sdu)?;
        inner.send(&packet).await?;

        // Receive response
        let response = inner.receive().await?;

        // Validate packet type (at offset 4 for both SDU modes)
        if response.len() <= 4 || response[4] != PacketType::Data as u8 {
            return Err(Error::ProtocolError(
                "Protocol negotiation failed".to_string(),
            ));
        }

        // Parse the Protocol response to extract server capabilities
        // The payload starts after the 8-byte header
        let payload = &response[PACKET_HEADER_SIZE..];
        let mut protocol_msg = ProtocolMessage::new();
        protocol_msg.parse_response(payload, &mut inner.capabilities)?;
        // Plenora fork: NCHAR, NVARCHAR2 and NCLOB are decoded as AL16UTF16
        // (`decode_column_text`, `parse_lob_read_response`); a database with
        // another national character set is refused at connect time, as
        // python-oracledb does (DPY-3012), instead of being decoded wrongly.
        if inner.capabilities.ncharset_id != crate::constants::charset::UTF16 {
            return Err(Error::FeatureNotSupported(
                "national character set other than AL16UTF16".to_string(),
            ));
        }

        // Update server info with banner
        if let Some(banner) = &protocol_msg.server_banner {
            inner.server_info.banner = banner.clone();
        }

        inner.state = ConnectionState::ProtocolNegotiated;
        Ok(())
    }

    /// Negotiate data types
    async fn negotiate_data_types(&self) -> Result<()> {
        use crate::messages::DataTypesMessage;

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Build data types request using DataTypesMessage (includes all ~320 data types)
        let data_types_msg = DataTypesMessage::new();
        let packet = data_types_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&packet).await?;

        // Receive response
        let response = inner.receive().await?;

        // Basic validation - packet type is at offset 4 regardless of large_sdu
        if response.len() > 4 && response[4] == PacketType::Data as u8 {
            inner.state = ConnectionState::DataTypesNegotiated;
            Ok(())
        } else {
            Err(Error::ProtocolError(
                "Data types negotiation failed".to_string(),
            ))
        }
    }

    /// Perform authentication
    async fn authenticate(&self) -> Result<()> {
        let service_name = match &self.config.service {
            ServiceMethod::ServiceName(name) => name.clone(),
            ServiceMethod::Sid(sid) => sid.clone(),
        };

        let mut auth = AuthMessage::new(
            &self.config.username,
            self.config.password().as_bytes(),
            &service_name,
        );

        // Phase one: send username and session info
        {
            let mut inner = self.inner.lock().await;
            let large_sdu = inner.large_sdu;
            let request = auth.build_request(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;

            let response = inner.receive().await?;
            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol("Empty auth response".to_string()));
            }

            // Check for error message type
            if response.len() > PACKET_HEADER_SIZE + 2 {
                let msg_type = response[PACKET_HEADER_SIZE + 2];
                if msg_type == MessageType::Error as u8 {
                    return Err(Error::AuthenticationFailed(
                        "Server rejected authentication phase one".to_string(),
                    ));
                }
            }

            auth.parse_response(&response[PACKET_HEADER_SIZE..])?;
        }

        // Phase two: send encrypted password
        if auth.phase() == AuthPhase::Two {
            let mut inner = self.inner.lock().await;
            let large_sdu = inner.large_sdu;
            let request = auth.build_request(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;

            let response = inner.receive().await?;
            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol("Empty auth phase two response".to_string()));
            }

            // Check for error message type or marker
            let packet_type = response[4];
            if packet_type == 12 {
                // Marker - authentication failed
                return Err(Error::AuthenticationFailed(
                    "Server sent MARKER - authentication rejected".to_string(),
                ));
            }

            if response.len() > PACKET_HEADER_SIZE + 2 {
                let msg_type = response[PACKET_HEADER_SIZE + 2];
                if msg_type == MessageType::Error as u8 {
                    return Err(Error::InvalidCredentials);
                }
            }

            auth.parse_response(&response[PACKET_HEADER_SIZE..])?;
        }

        // Verify authentication completed
        if !auth.is_complete() {
            return Err(Error::AuthenticationFailed(
                "Authentication did not complete".to_string(),
            ));
        }

        // Store combo key for later use (encrypted data)
        let mut inner = self.inner.lock().await;
        if let Some(combo_key) = auth.combo_key() {
            inner.capabilities.combo_key = Some(combo_key.to_vec());
        }
        // Auth used sequence numbers 1 and 2, set to 2 so next is 3
        inner.sequence_number = 2;
        inner.state = ConnectionState::Ready;

        Ok(())
    }

    /// Execute a SQL statement and return the result
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL statement to execute
    /// * `params` - Bind parameters (use `Value::Integer`, `Value::String`, etc.)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use oracle_rs::Value;
    ///
    /// // Query with bind parameters
    /// let result = conn.execute(
    ///     "SELECT * FROM employees WHERE department_id = :1",
    ///     &[Value::Integer(10)]
    /// ).await?;
    ///
    /// // DML with bind parameters
    /// let result = conn.execute(
    ///     "UPDATE employees SET salary = :1 WHERE employee_id = :2",
    ///     &[Value::Integer(50000), Value::Integer(100)]
    /// ).await?;
    /// println!("Rows affected: {}", result.rows_affected);
    /// ```
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.ensure_ready().await?;
        // Ogni esecuzione analizza il testo su un cursore nuovo: vedi
        // `execute_query_with_params` per la statement cache.
        let statement = Statement::new(sql);
        match statement.statement_type() {
            StatementType::Query => self.execute_query_with_params(&statement, params).await,
            _ => self.execute_dml_with_params(&statement, params).await,
        }
    }

    /// Execute a query and return rows
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use oracle_rs::Value;
    ///
    /// let result = conn.query(
    ///     "SELECT * FROM employees WHERE salary > :1",
    ///     &[Value::Integer(50000)]
    /// ).await?;
    /// ```
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.ensure_ready().await?;
        let result = self
            .execute_query_with_params(&Statement::new(sql), params)
            .await;
        self.handle_result(result)
    }

    /// Execute DML (INSERT, UPDATE, DELETE) and return rows affected
    pub async fn execute_dml_sql(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.ensure_ready().await?;
        let result = self
            .execute_dml_with_params(&Statement::new(sql), params)
            .await;
        self.handle_result(result).map(|r| r.rows_affected)
    }

    /// Execute a PL/SQL block with IN/OUT/INOUT parameters
    ///
    /// This method allows execution of PL/SQL anonymous blocks or procedure calls
    /// that have OUT or IN OUT parameters. The `params` slice specifies the direction
    /// and type of each bind parameter.
    ///
    /// # Arguments
    ///
    /// * `sql` - The PL/SQL block or procedure call
    /// * `params` - The bind parameters with direction information
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use oracle_rs::{Connection, BindParam, OracleType, Value};
    ///
    /// // Call a procedure with IN and OUT parameters
    /// let result = conn.execute_plsql(
    ///     "BEGIN get_employee_name(:1, :2); END;",
    ///     &[
    ///         BindParam::input(Value::Integer(100)),           // IN: employee_id
    ///         BindParam::output(OracleType::Varchar, 100),     // OUT: employee_name
    ///     ]
    /// ).await?;
    ///
    /// // Get the OUT parameter value
    /// let name = result.get_string(0).unwrap_or("Unknown");
    /// println!("Employee name: {}", name);
    /// ```
    ///
    /// # REF CURSOR Example
    ///
    /// ```rust,ignore
    /// use oracle_rs::{Connection, BindParam, Value};
    ///
    /// // Call a procedure that returns a REF CURSOR
    /// let result = conn.execute_plsql(
    ///     "BEGIN OPEN :1 FOR SELECT * FROM employees; END;",
    ///     &[BindParam::output_cursor()]
    /// ).await?;
    ///
    /// // Get the cursor ID and fetch rows
    /// if let Some(cursor_id) = result.get_cursor_id(0) {
    ///     let rows = conn.fetch_cursor(cursor_id, 100).await?;
    ///     for row in rows {
    ///         println!("{:?}", row);
    ///     }
    /// }
    /// ```
    pub async fn execute_plsql(&self, sql: &str, params: &[BindParam]) -> Result<PlsqlResult> {
        self.ensure_ready().await?;

        let statement = Statement::new(sql);

        // Build values for bind parameters
        // IN params: use provided value or Null
        // OUT params: use placeholder value (required for metadata, server ignores actual value)
        // INOUT params: use provided value or Null
        let bind_values: Vec<Value> = params
            .iter()
            .map(|p| {
                if p.direction == BindDirection::Output {
                    // OUT params get a placeholder based on their type
                    // Oracle still needs a value sent in the request (even though it's ignored)
                    p.placeholder_value()
                } else {
                    // IN and INOUT params use the provided value or Null
                    p.value.clone().unwrap_or(Value::Null)
                }
            })
            .collect();

        // Build bind metadata for proper buffer sizes
        // For OUTPUT params, use the user-specified buffer_size
        // For INPUT params, derive buffer_size from the actual value
        let bind_metadata: Vec<crate::messages::BindMetadata> = params
            .iter()
            .zip(bind_values.iter())
            .map(|(p, v)| {
                let buffer_size = if p.buffer_size > 0 {
                    p.buffer_size
                } else {
                    // Derive from value
                    match v {
                        Value::String(s) => std::cmp::max(s.len() as u32, 1),
                        Value::Bytes(b) => std::cmp::max(b.len() as u32, 1),
                        Value::Integer(_) | Value::Number(_) => 22, // Oracle NUMBER max size
                        Value::Float(_) => 8,                       // BINARY_DOUBLE
                        Value::Boolean(_) => 1,
                        Value::Timestamp(_) => 13,
                        Value::Date(_) => 7,
                        Value::RowId(_) => 18,
                        _ => 100, // Default fallback
                    }
                };
                crate::messages::BindMetadata {
                    oracle_type: p.oracle_type,
                    buffer_size,
                }
            })
            .collect();

        // Create execute message with PL/SQL options
        let options = ExecuteOptions::for_plsql();
        let mut execute_msg = ExecuteMessage::new(&statement, options);
        execute_msg.set_bind_values(bind_values);
        execute_msg.set_bind_metadata(bind_metadata);

        let mut inner = self.inner.lock().await;
        self.take_reported_cursor();
        let result = Box::pin(async {
            let large_sdu = inner.large_sdu;
            let seq_num = inner.next_sequence_number();
            execute_msg.set_sequence_number(seq_num);
            let request = execute_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;

            // Receive response
            let mut framing = PageFraming::new(&[]);
            framing.params = params;
            let response = inner.receive_response_checked(framing).await?;
            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol("Empty PL/SQL response".to_string()));
            }

            // Check for MARKER packet (indicates error - requires reset protocol)
            let packet_type = response[4];
            if packet_type == PacketType::Marker as u8 {
                // Handle marker reset protocol and get the error packet
                let error_response = inner.handle_marker_reset().await?;
                let payload = &error_response[PACKET_HEADER_SIZE..];
                // Parse error response to extract the actual Oracle error
                let parsed = self.parse_error_response(payload, &inner.capabilities);
                let _: QueryResult = inner.checked_parse_result(parsed)?;
                return Err(Error::Protocol("PL/SQL execution failed".to_string()));
            }

            // Parse the PL/SQL response
            let payload = &response[PACKET_HEADER_SIZE..];
            let caps = inner.capabilities.clone();

            let result = self.parse_plsql_response(payload, &caps, params);
            inner.checked_parse_result(result)
        })
        .await;
        // Il cursore del blocco si chiude sempre; i REF CURSOR e i risultati
        // impliciti restano al chiamante, che li legge fino in fondo.
        let reported = self.take_reported_cursor();
        inner.release_cursor(reported);
        result
    }

    /// Execute a batch of DML statements with multiple rows of bind values
    ///
    /// This method efficiently executes the same SQL statement multiple times
    /// with different bind values (executemany pattern).
    ///
    /// # Arguments
    ///
    /// * `batch` - The batch containing SQL and rows of bind values
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use oracle_rs::{Connection, BatchBuilder, Value};
    ///
    /// let batch = BatchBuilder::new("INSERT INTO users (id, name) VALUES (:1, :2)")
    ///     .add_row(vec![Value::Integer(1), Value::String("Alice".to_string())])
    ///     .add_row(vec![Value::Integer(2), Value::String("Bob".to_string())])
    ///     .with_row_counts()
    ///     .build();
    ///
    /// let result = conn.execute_batch(&batch).await?;
    /// println!("Total rows affected: {}", result.total_rows_affected);
    /// ```
    pub async fn execute_batch(&self, batch: &BatchBinds) -> Result<BatchResult> {
        self.ensure_ready().await?;

        // Validate the batch
        batch.validate()?;

        if batch.rows.is_empty() {
            return Ok(BatchResult::new());
        }

        // Build execute options for batch DML
        let mut options = ExecuteOptions::for_dml(batch.options.auto_commit);
        options.num_execs = batch.rows.len() as u32;
        options.batch_errors = batch.options.batch_errors;
        options.dml_row_counts = batch.options.array_dml_row_counts;

        // Create execute message with batch bind values
        let mut execute_msg = ExecuteMessage::new(&batch.statement, options);
        execute_msg.set_batch_bind_values(batch.rows.clone());

        let mut inner = self.inner.lock().await;
        self.take_reported_cursor();
        let result = Box::pin(async {
            let large_sdu = inner.large_sdu;
            let seq_num = inner.next_sequence_number();
            execute_msg.set_sequence_number(seq_num);
            let request = execute_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;

            // Receive response
            let mut framing = PageFraming::new(&[]);
            framing.row_counts = batch.options.array_dml_row_counts;
            let mut response = inner.receive_response_checked(framing).await?;
            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol("Empty batch response".to_string()));
            }

            // Check packet type - handle MARKER packets
            let packet_type = response[4];
            if packet_type == PacketType::Marker as u8 {
                // Handle BREAK/RESET protocol same as regular DML
                response = inner.handle_marker_reset().await?;
            }

            // Parse the batch response
            let payload = &response[PACKET_HEADER_SIZE..];
            let caps = inner.capabilities.clone();

            let result = self.parse_batch_response(
                payload,
                batch.rows.len(),
                batch.options.array_dml_row_counts,
                &caps,
            );
            inner.checked_parse_result(result)
        })
        .await;
        let reported = self.take_reported_cursor();
        inner.release_cursor(reported);
        result
    }

    /// Parse batch execution response
    fn parse_batch_response(
        &self,
        payload: &[u8],
        batch_size: usize,
        want_row_counts: bool,
        caps: &Capabilities,
    ) -> Result<BatchResult> {
        if payload.len() < 3 {
            return Err(Error::Protocol("Batch response too short".to_string()));
        }

        let mut buf = ReadBuffer::from_slice(payload);

        // Skip data flags
        buf.skip(2)?;

        let mut rows_affected: u64 = 0;
        let mut row_counts: Option<Vec<u64>> = None;
        let mut end_of_response = false;

        // Process messages until end_of_response or out of data
        while !end_of_response && buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                x if x == MessageType::IoVector as u8 => {
                    Self::parse_io_vector(&mut buf, &[])?;
                }

                // Error (4) - may contain error or success info
                x if x == MessageType::Error as u8 => {
                    let (error_code, error_msg, cid, row_count) =
                        Self::parse_error_info_with_rowcount(&mut buf, caps)?;
                    self.report_cursor(cid);
                    rows_affected = row_count;
                    if error_code != 0 && error_code != 1403 {
                        return Err(Error::OracleError {
                            code: error_code,
                            message: error_msg.unwrap_or_default(),
                        });
                    }
                }

                // Parameter (8) - return parameters (may contain row counts)
                x if x == MessageType::Parameter as u8 => {
                    if let Some(counts) =
                        Self::parse_return_parameters_internal(&mut buf, want_row_counts)?
                    {
                        row_counts = Some(counts);
                    }
                }

                // Server side piggyback (23)
                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                // Status (9) - call status
                x if x == MessageType::Status as u8 => {
                    let _call_status = buf.read_ub4()?;
                    let _end_to_end_seq = buf.read_ub2()?;
                }

                // BitVector (21)
                21 => {
                    let _num_columns_sent = buf.read_ub2()?;
                    if buf.remaining() > 0 {
                        let _byte = buf.read_u8()?;
                    }
                }

                // End of Response (29) - explicit end marker
                29 => {
                    end_of_response = true;
                }

                _ => {
                    // Unknown message type - continue processing
                }
            }
        }

        let mut result = BatchResult::new();
        result.total_rows_affected = rows_affected;
        result.success_count = batch_size;
        result.row_counts = row_counts;

        Ok(result)
    }

    /// Set the cumulative wire-byte ceiling for each response, including frames.
    /// Call before sending an operation; a rejected response closes the channel.
    pub async fn set_response_byte_limit(&self, limit: usize) {
        self.inner.lock().await.response_limit = limit;
    }

    /// Fetch more rows from an open cursor
    ///
    /// This method is used when a query result has `has_more_rows == true`
    /// to retrieve additional rows from the server.
    ///
    /// # Arguments
    ///
    /// * `cursor_id` - The cursor ID from a previous query result
    /// * `columns` - Column information from the original query
    /// * `fetch_size` - Number of rows to fetch
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut result = conn.query("SELECT * FROM large_table", &[]).await?;
    /// let mut all_rows = result.rows.clone();
    ///
    /// while result.has_more_rows {
    ///     result = conn.fetch_more(result.cursor_id, &result.columns, 100).await?;
    ///     all_rows.extend(result.rows);
    /// }
    /// ```
    pub async fn fetch_more(
        &self,
        cursor_id: u16,
        columns: &[ColumnInfo],
        fetch_size: u32,
    ) -> Result<QueryResult> {
        self.ensure_ready().await?;

        // Build fetch message
        let mut fetch_msg = FetchMessage::new(cursor_id, fetch_size);

        let mut inner = self.inner.lock().await;
        let result = Box::pin(async {
            fetch_msg.set_sequence_number(inner.next_sequence_number());
            let request = fetch_msg.build_request_with_sdu(&inner.capabilities, inner.large_sdu)?;
            inner.send(&request).await?;

            // Receive and parse response
            let response = inner.receive_page_with_columns(columns).await?;
            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol("Empty fetch response".to_string()));
            }

            // Fetch errors use the same MARKER/reset exchange as query execution.
            // Parsing a MARKER as row data would silently end the stream.
            if response[4] == PacketType::Marker as u8 {
                let error_response = inner.handle_marker_reset().await?;
                let payload = &error_response[PACKET_HEADER_SIZE..];
                let parsed = self.parse_error_response(payload, &inner.capabilities);
                return inner.checked_parse_result(parsed);
            }

            // Parse row data from response
            let payload = &response[PACKET_HEADER_SIZE..];
            let caps = inner.capabilities.clone();
            let previous = inner.last_rows.get(&cursor_id).cloned();
            let parsed = self.parse_fetch_response_after(payload, columns, &caps, previous);
            let mut result = inner.checked_parse_result(parsed)?;
            result.cursor_id = cursor_id;
            Ok(result)
        })
        .await;
        inner.release_finished_fetch(cursor_id, &result);
        result
    }

    /// Fetch rows from a REF CURSOR
    ///
    /// This method fetches rows from a REF CURSOR that was returned from a
    /// PL/SQL procedure or function. The cursor contains the column metadata
    /// and cursor ID needed to fetch the rows.
    ///
    /// # Arguments
    ///
    /// * `cursor` - The REF CURSOR returned from PL/SQL
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use oracle_rs::{Connection, BindParam, Value};
    ///
    /// // Call a procedure that returns a REF CURSOR
    /// let result = conn.execute_plsql(
    ///     "BEGIN OPEN :1 FOR SELECT id, name FROM employees; END;",
    ///     &[BindParam::output_cursor()]
    /// ).await?;
    ///
    /// // Get the cursor and fetch rows
    /// if let Value::Cursor(cursor) = &result.out_values[0] {
    ///     let rows = conn.fetch_cursor(cursor).await?;
    ///     println!("Fetched {} rows", rows.row_count());
    ///     for row in &rows.rows {
    ///         println!("{:?}", row);
    ///     }
    /// }
    /// ```
    pub async fn fetch_cursor(&self, cursor: &crate::types::RefCursor) -> Result<QueryResult> {
        self.fetch_cursor_with_size(cursor, 100).await
    }

    /// Fetch rows from a REF CURSOR with a specified fetch size
    ///
    /// This is the same as `fetch_cursor` but allows specifying how many
    /// rows to fetch at once.
    ///
    /// REF CURSORs use an ExecuteMessage with only the FETCH option because
    /// the cursor is already open from the PL/SQL execution. The cursor_id
    /// and column metadata were obtained when the REF CURSOR was returned.
    ///
    /// # Arguments
    ///
    /// * `cursor` - The REF CURSOR returned from PL/SQL
    /// * `fetch_size` - Number of rows to fetch (default is 100)
    pub async fn fetch_cursor_with_size(
        &self,
        cursor: &crate::types::RefCursor,
        fetch_size: u32,
    ) -> Result<QueryResult> {
        use crate::messages::ExecuteMessage;

        if cursor.cursor_id() == 0 {
            return Err(Error::InvalidCursor(
                "Cursor ID is 0 (not initialized)".to_string(),
            ));
        }

        self.ensure_ready().await?;

        // REF CURSOR uses ExecuteMessage with FETCH only (no SQL, no EXECUTE)
        // Create a statement with the cursor's metadata
        let mut stmt = Statement::new(""); // No SQL for REF CURSOR
        stmt.set_cursor_id(cursor.cursor_id());
        stmt.set_columns(cursor.columns().to_vec());
        stmt.set_executed(true); // Already executed by Oracle
        stmt.set_statement_type(crate::statement::StatementType::Query); // This is a query cursor

        // Build execute message with only FETCH option
        let options = crate::messages::ExecuteOptions::for_ref_cursor(fetch_size);
        let mut execute_msg = ExecuteMessage::new(&stmt, options);

        let mut inner = self.inner.lock().await;
        let result = Box::pin(async {
            let large_sdu = inner.large_sdu;
            let seq_num = inner.next_sequence_number();
            execute_msg.set_sequence_number(seq_num);

            let request = execute_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;

            // Receive and parse response
            let response = inner.receive_page_with_columns(cursor.columns()).await?;
            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol("Empty cursor response".to_string()));
            }

            // Check for MARKER packet (indicates error)
            let packet_type = response[4];
            if packet_type == PacketType::Marker as u8 {
                let error_response = inner.handle_marker_reset().await?;
                let payload = &error_response[PACKET_HEADER_SIZE..];
                let parsed = self.parse_error_response(payload, &inner.capabilities);
                return inner.checked_parse_result(parsed);
            }

            // Parse query response - use cursor's columns since they're already defined
            let payload = &response[PACKET_HEADER_SIZE..];
            let caps = inner.capabilities.clone();
            let previous = inner.last_rows.get(&cursor.cursor_id()).cloned();
            let parsed =
                self.parse_fetch_response_after(payload, cursor.columns(), &caps, previous);
            let mut result = inner.checked_parse_result(parsed)?;
            result.cursor_id = cursor.cursor_id();
            Ok(result)
        })
        .await;
        inner.release_finished_fetch(cursor.cursor_id(), &result);
        result
    }

    /// Fetch rows from an implicit result set
    ///
    /// Implicit results are returned via `DBMS_SQL.RETURN_RESULT` from PL/SQL.
    /// They contain cursor metadata but no rows until fetched.
    ///
    /// # Arguments
    ///
    /// * `result` - The implicit result from PL/SQL execution
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let plsql_result = conn.execute_plsql(r#"
    ///     declare
    ///         c sys_refcursor;
    ///     begin
    ///         open c for select * from employees;
    ///         dbms_sql.return_result(c);
    ///     end;
    /// "#, &[]).await?;
    ///
    /// for implicit in plsql_result.implicit_results.iter() {
    ///     let rows = conn.fetch_implicit_result(implicit).await?;
    ///     println!("Fetched {} rows", rows.row_count());
    /// }
    /// ```
    pub async fn fetch_implicit_result(&self, result: &ImplicitResult) -> Result<QueryResult> {
        self.fetch_implicit_result_with_size(result, 100).await
    }

    /// Fetch rows from an implicit result set with a specified fetch size
    pub async fn fetch_implicit_result_with_size(
        &self,
        result: &ImplicitResult,
        fetch_size: u32,
    ) -> Result<QueryResult> {
        // Convert implicit result to RefCursor and use fetch_cursor mechanism
        let cursor = crate::types::RefCursor::new(result.cursor_id, result.columns.clone());
        self.fetch_cursor_with_size(&cursor, fetch_size).await
    }

    /// Parse fetch response to extract additional rows
    ///
    /// REF CURSOR fetch responses contain a series of messages:
    /// - RowHeader (6): Contains metadata about the following row data
    /// - RowData (7): Contains the actual row values
    /// - Error (4): Contains error info with cursor_id and row counts
    #[cfg(test)]
    fn parse_fetch_response(
        &self,
        payload: &[u8],
        columns: &[ColumnInfo],
        caps: &Capabilities,
        previous: Option<Vec<Value>>,
    ) -> Result<QueryResult> {
        self.parse_fetch_response_after(payload, columns, caps, previous)
    }

    /// Risposta FETCH, con l'ultima riga della pagina
    /// precedente per le colonne ripetute della prima riga.
    fn parse_fetch_response_after(
        &self,
        payload: &[u8],
        columns: &[ColumnInfo],
        caps: &Capabilities,
        previous: Option<Vec<Value>>,
    ) -> Result<QueryResult> {
        if payload.len() < 3 {
            return Err(Error::Protocol("Fetch response too short".to_string()));
        }

        let mut buf = ReadBuffer::from_slice(payload);
        let mut rows = Vec::new();
        let mut has_more_rows = None;

        // Bit vector for duplicate column optimization
        let mut bit_vector: Option<Vec<u8>> = None;
        let mut previous_row_values: Option<Vec<Value>> = previous;

        // Skip data flags
        buf.skip(2)?;

        // Process multiple messages in the response
        while buf.remaining() >= 1 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                x if x == MessageType::IoVector as u8 => {
                    Self::parse_io_vector(&mut buf, &[])?;
                }
                x if x == MessageType::RowHeader as u8 => {
                    bit_vector = Self::parse_row_header(&mut buf)?;
                }
                x if x == MessageType::RowData as u8 => {
                    // Parse actual row data with bit vector support
                    let row = Self::parse_row_data_with_bitvector(
                        &mut buf,
                        columns,
                        caps,
                        bit_vector.as_deref(),
                        previous_row_values.as_ref(),
                    )?;
                    previous_row_values = Some(row.values().to_vec());
                    bit_vector = None;
                    rows.push(row);
                }
                x if x == MessageType::BitVector as u8 => {
                    // BitVector indicates which columns have actual data vs duplicates
                    let _num_columns_sent = buf.read_ub2()?;
                    let num_bytes = (columns.len() + 7) / 8; // Round up
                    if num_bytes > 0 {
                        let bv = buf.read_bytes_vec(num_bytes)?;
                        bit_vector = Some(bv);
                    }
                    // Continue processing - RowData follows
                }
                x if x == MessageType::Error as u8 => {
                    // Error message contains row count and cursor info
                    let (error_code, error_msg, _, _) =
                        Self::parse_error_info_with_rowcount(&mut buf, caps)?;
                    has_more_rows = Some(error_code != 1403);
                    if error_code != 0 && error_code != 1403 {
                        // 1403 = no data found
                        return Err(Error::OracleError {
                            code: error_code,
                            message: error_msg.unwrap_or_default(),
                        });
                    }
                    break; // Error message marks end of response
                }
                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                x if x == MessageType::Status as u8 => {
                    buf.skip_ub4()?;
                    buf.skip_ub2()?;
                }
                x if x == MessageType::EndOfResponse as u8 => {
                    break;
                }
                _ => {
                    return Err(Error::Protocol("Messaggio fetch non riconosciuto".into()));
                }
            }
        }

        Ok(QueryResult {
            columns: columns.to_vec(),
            rows,
            rows_affected: 0,
            has_more_rows: has_more_rows
                .ok_or_else(|| Error::Protocol("Fetch senza completamento del cursore".into()))?,
            cursor_id: 0,
        })
    }

    /// Open a scrollable cursor for bidirectional navigation
    ///
    /// Scrollable cursors allow moving forward and backward through result sets,
    /// jumping to specific positions, and fetching from various locations.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL query to execute
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut cursor = conn.open_scrollable_cursor("SELECT * FROM employees").await?;
    ///
    /// // Move to different positions
    /// let first = conn.scroll(&mut cursor, FetchOrientation::First, 0).await?;
    /// let last = conn.scroll(&mut cursor, FetchOrientation::Last, 0).await?;
    /// let row5 = conn.scroll(&mut cursor, FetchOrientation::Absolute, 5).await?;
    ///
    /// conn.close_cursor(&mut cursor).await?;
    /// ```
    pub async fn open_scrollable_cursor(&self, sql: &str) -> Result<ScrollableCursor> {
        self.ensure_ready().await?;

        let statement = Statement::new(sql);

        // For scrollable cursors, execute with scrollable flag and prefetch 1 row
        // to get column metadata. The scroll() method will fetch actual rows at
        // specific positions.
        let mut options = ExecuteOptions::for_query(1); // prefetch 1 row for column info

        options.scrollable = true;

        let mut execute_msg = ExecuteMessage::new(&statement, options);

        let mut inner = self.inner.lock().await;
        self.take_reported_cursor();
        let result = Box::pin(async {
            let large_sdu = inner.large_sdu;
            let seq_num = inner.next_sequence_number();
            execute_msg.set_sequence_number(seq_num);
            let request = execute_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;

            // Receive and parse response
            let response = inner.receive_page().await?;

            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol(
                    "Empty scrollable cursor response".to_string(),
                ));
            }

            // Check for MARKER packet (indicates error - requires reset protocol)
            let packet_type = response[4];
            if packet_type == PacketType::Marker as u8 {
                // Handle marker reset protocol and get the error packet
                let error_response = inner.handle_marker_reset().await?;
                let payload = &error_response[PACKET_HEADER_SIZE..];
                // Parse error response to extract the actual Oracle error
                let parsed = self.parse_error_response(payload, &inner.capabilities);
                let _: QueryResult = inner.checked_parse_result(parsed)?;
                // If we get here without error, something unexpected happened
                return Err(Error::Protocol(
                    "Unexpected successful response after MARKER".to_string(),
                ));
            }

            // Parse describe info to get columns
            let payload = &response[PACKET_HEADER_SIZE..];
            let parsed = self.parse_query_response(payload, &inner.capabilities);
            let result = inner.checked_parse_result(parsed)?;

            Ok(ScrollableCursor::new(result.cursor_id, result.columns))
        })
        .await;
        // Aperto, il cursore e del chiamante fino a `close_cursor`.
        let reported = self.take_reported_cursor();
        if result.is_err() {
            inner.release_cursor(reported);
        }
        result
    }

    /// Scroll to a position in a scrollable cursor and fetch rows
    ///
    /// # Arguments
    ///
    /// * `cursor` - The scrollable cursor to scroll
    /// * `orientation` - The direction/mode of scrolling
    /// * `offset` - Position offset (used for Absolute and Relative modes)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Go to first row
    /// let first = conn.scroll(&mut cursor, FetchOrientation::First, 0).await?;
    ///
    /// // Go to absolute position 10
    /// let row10 = conn.scroll(&mut cursor, FetchOrientation::Absolute, 10).await?;
    ///
    /// // Move 5 rows forward from current position
    /// let plus5 = conn.scroll(&mut cursor, FetchOrientation::Relative, 5).await?;
    ///
    /// // Move 3 rows backward
    /// let minus3 = conn.scroll(&mut cursor, FetchOrientation::Relative, -3).await?;
    /// ```
    pub async fn scroll(
        &self,
        cursor: &mut ScrollableCursor,
        orientation: FetchOrientation,
        offset: i64,
    ) -> Result<ScrollResult> {
        self.ensure_ready().await?;

        if !cursor.is_open() {
            return Err(Error::CursorClosed);
        }

        // Create a statement for the scroll operation (uses the existing cursor)
        let mut stmt = Statement::new("");
        stmt.set_cursor_id(cursor.cursor_id);
        stmt.set_columns(cursor.columns.clone());
        stmt.set_executed(true);
        stmt.set_statement_type(crate::statement::StatementType::Query);

        // Build execute message with scroll_operation=true
        let mut options = ExecuteOptions::for_query(1);
        options.scrollable = true;
        options.scroll_operation = true;
        options.fetch_orientation = orientation as u32;
        // Calculate fetch_pos based on orientation
        options.fetch_pos = match orientation {
            FetchOrientation::First => 1,
            FetchOrientation::Last => 0, // Server calculates
            FetchOrientation::Absolute => offset.max(0) as u32,
            FetchOrientation::Relative => (cursor.position + offset).max(0) as u32,
            FetchOrientation::Next => (cursor.position + 1).max(0) as u32,
            FetchOrientation::Prior => (cursor.position - 1).max(0) as u32,
            FetchOrientation::Current => cursor.position.max(0) as u32,
        };

        let mut execute_msg = ExecuteMessage::new(&stmt, options);

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;
        let seq_num = inner.next_sequence_number();
        execute_msg.set_sequence_number(seq_num);
        let request = execute_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        // Receive and parse response
        let response = inner.receive_page_with_columns(&cursor.columns).await?;
        if response.len() <= PACKET_HEADER_SIZE {
            return Err(Error::Protocol("Empty scroll response".to_string()));
        }

        // Check for MARKER packet
        let packet_type = response[4];
        if packet_type == PacketType::Marker as u8 {
            let error_response = inner.handle_marker_reset().await?;
            let payload = &error_response[PACKET_HEADER_SIZE..];
            let parsed = self.parse_error_response(payload, &inner.capabilities);
            let _: QueryResult = inner.checked_parse_result(parsed)?;
            return Err(Error::Protocol("Scroll operation failed".to_string()));
        }

        let payload = &response[PACKET_HEADER_SIZE..];
        // Use cursor's columns since Oracle doesn't re-send column metadata for scroll operations
        let parsed =
            self.parse_query_response_with_columns(payload, &inner.capabilities, &cursor.columns);
        let query_result = inner.checked_parse_result(parsed)?;

        // Use position from Oracle's response (rows_affected contains the row position)
        // For scrollable cursors, Oracle returns the row number in error_info.rowcount
        let new_position = if !query_result.rows.is_empty() {
            // Position is the actual row number from Oracle
            query_result.rows_affected as i64
        } else {
            // No rows returned - calculate position based on orientation
            match orientation {
                FetchOrientation::First => 0, // Before first
                FetchOrientation::Last => cursor.row_count.unwrap_or(0) as i64 + 1, // After last
                FetchOrientation::Next => cursor.position + 1,
                FetchOrientation::Prior => cursor.position - 1,
                FetchOrientation::Absolute => offset,
                FetchOrientation::Relative => cursor.position + offset,
                FetchOrientation::Current => cursor.position,
            }
        };

        cursor.update_position(new_position);

        let mut result = ScrollResult::new(query_result.rows, new_position);
        result.at_end = !query_result.has_more_rows;
        result.at_beginning = new_position <= 1;

        Ok(result)
    }

    /// Close a scrollable cursor
    ///
    /// # Arguments
    ///
    /// * `cursor` - The scrollable cursor to close
    pub async fn close_cursor(&self, cursor: &mut ScrollableCursor) -> Result<()> {
        if !cursor.is_open() {
            return Ok(());
        }

        // Il server lo chiude col piggyback della prossima chiamata.
        self.inner.lock().await.release_cursor(cursor.cursor_id);
        cursor.mark_closed();
        Ok(())
    }

    /// Get type information for a database object or collection type
    ///
    /// This method queries Oracle's data dictionary to retrieve type metadata
    /// for collections (VARRAY, Nested Table) and user-defined object types.
    ///
    /// # Arguments
    ///
    /// * `type_name` - Fully qualified type name (e.g., "SCHEMA.TYPE_NAME" or just "TYPE_NAME")
    ///
    /// # Returns
    ///
    /// A `DbObjectType` containing the type metadata, including:
    /// - Schema and type name
    /// - Whether it's a collection
    /// - Collection type (VARRAY, Nested Table, etc.)
    /// - Element type for collections
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let number_array = conn.get_type("MY_NUMBER_ARRAY").await?;
    /// assert!(number_array.is_collection);
    /// ```
    pub async fn get_type(&self, type_name: &str) -> Result<crate::dbobject::DbObjectType> {
        use crate::dbobject::{CollectionType, DbObjectType};

        self.ensure_ready().await?;

        // Parse type name into schema and name
        let (schema, name) = parse_type_name(type_name, &self.config.username);

        // First, query ALL_TYPES to get basic type info
        let type_info = self
            .query(
                "SELECT typecode, type_oid FROM all_types WHERE owner = :1 AND type_name = :2",
                &[Value::String(schema.clone()), Value::String(name.clone())],
            )
            .await?;

        if type_info.rows.is_empty() {
            return Err(Error::OracleError {
                code: 4043, // ORA-04043: object does not exist
                message: format!("Type {}.{} not found", schema, name),
            });
        }

        let row = &type_info.rows[0];
        let typecode = row.get(0).and_then(|v| v.as_str()).unwrap_or("");
        let type_oid = row.get(1).and_then(|v| v.as_bytes()).map(|b| b.to_vec());

        // Check if it's a collection
        if typecode == "COLLECTION" {
            // Query ALL_COLL_TYPES for collection details
            let coll_info = self.query(
                "SELECT coll_type, elem_type_name, elem_type_owner, upper_bound FROM all_coll_types WHERE owner = :1 AND type_name = :2",
                &[Value::String(schema.clone()), Value::String(name.clone())],
            ).await?;

            if coll_info.rows.is_empty() {
                return Err(Error::OracleError {
                    code: 4043,
                    message: format!("Collection type {}.{} metadata not found", schema, name),
                });
            }

            let coll_row = &coll_info.rows[0];
            let coll_type_str = coll_row.get(0).and_then(|v| v.as_str()).unwrap_or("");
            let elem_type_name = coll_row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("VARCHAR2");
            let _elem_type_owner = coll_row.get(2).and_then(|v| v.as_str());

            let collection_type = match coll_type_str {
                "VARYING ARRAY" => CollectionType::Varray,
                "TABLE" => CollectionType::NestedTable,
                _ => CollectionType::Varray, // Default
            };

            let element_type = oracle_type_from_name(elem_type_name);

            let mut obj_type =
                DbObjectType::collection(schema, name, collection_type, element_type);
            obj_type.oid = type_oid;
            Ok(obj_type)
        } else {
            // Regular object type (not yet fully implemented)
            let mut obj_type = DbObjectType::new(schema, name);
            obj_type.oid = type_oid;
            Ok(obj_type)
        }
    }

    /// Internal: Execute a query statement with optional bind parameters
    ///
    /// Il cursore della query resta aperto finche il risultato ha altre
    /// righe (`has_more_rows`): il chiamante lo legge con [`Self::fetch_more`]
    /// fino in fondo, e allora si chiude. Esaurito alla prima pagina, o
    /// fallito, si chiude subito.
    ///
    /// Deviazione dal driver ufficiale (python-oracledb tiene il cursore nella
    /// statement cache e lo riesegue): regola, riuso dei cursori server fra
    /// esecuzioni; ambito, ogni esecuzione di query, DML, PL/SQL e batch di
    /// questa connessione analizza il testo su un cursore nuovo e la
    /// [`crate::StatementCache`] non e piu consultata; hazard, solo un parse
    /// leggero in piu per esecuzione, mentre la riesecuzione di un cursore in
    /// cache non verificava i metadati dei bind (`binds_changed` non era mai
    /// impostato) e poteva rieseguire un cursore ancora in lettura; rientro,
    /// il riuso torna con il confronto dei metadati dei bind, l'esclusione dei
    /// cursori con un risultato aperto e un oracolo live contro questo
    /// percorso.
    async fn execute_query_with_params(
        &self,
        statement: &Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let prefetch_rows = 100; // Default prefetch

        // For first execution, check if we might have LOBs (no prefetch for safety)
        // This can be optimized later with describe-only first
        let options = ExecuteOptions::for_query(prefetch_rows);
        let mut execute_msg = ExecuteMessage::new(statement, options);

        // Set bind values if provided
        if !params.is_empty() {
            execute_msg.set_bind_values(params.to_vec());
        }

        let mut inner = self.inner.lock().await;
        self.take_reported_cursor();
        let result = Box::pin(async {
            let large_sdu = inner.large_sdu;
            let seq_num = inner.next_sequence_number();
            execute_msg.set_sequence_number(seq_num);
            let request = execute_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;

            // Receive and parse response
            let response = inner.receive_page_with_columns(statement.columns()).await?;
            if response.len() <= PACKET_HEADER_SIZE {
                return Err(Error::Protocol("Empty query response".to_string()));
            }

            // Check for MARKER packet (indicates error - requires reset protocol)
            let packet_type = response[4];
            if packet_type == PacketType::Marker as u8 {
                // Handle marker reset protocol and get the error packet
                let error_response = inner.handle_marker_reset().await?;
                let payload = &error_response[PACKET_HEADER_SIZE..];
                let parsed = self.parse_error_response(payload, &inner.capabilities);
                return inner.checked_parse_result(parsed);
            }

            // Parse the response to extract columns and rows
            let payload = &response[PACKET_HEADER_SIZE..];
            let parsed = self.parse_query_response_with_columns(
                payload,
                &inner.capabilities,
                statement.columns(),
            );
            let mut result = inner.checked_parse_result(parsed)?;

            // Check if any columns are LOB types that require defines
            let has_lob_columns = result.columns.iter().any(|col| col.is_lob());

            if has_lob_columns && !statement.requires_define() {
                // Il cursore e gia aperto: se la riesecuzione fallisce si chiude.
                self.report_cursor(result.cursor_id);
                // We need to re-execute with column defines
                // Create a modified statement with the define flag set
                let mut stmt_with_define = statement.clone();
                stmt_with_define.set_columns(result.columns.clone());
                stmt_with_define.set_cursor_id(result.cursor_id);
                stmt_with_define.set_requires_define(true);
                stmt_with_define.set_no_prefetch(true);
                stmt_with_define.set_executed(true);

                // Re-execute with defines
                let define_options = ExecuteOptions::for_query(prefetch_rows);
                let mut define_msg = ExecuteMessage::new(&stmt_with_define, define_options);
                let seq_num = inner.next_sequence_number();
                define_msg.set_sequence_number(seq_num);

                let define_request =
                    define_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;
                inner.send(&define_request).await?;

                // Receive the re-execute response
                let define_response = inner
                    .receive_page_with_columns(stmt_with_define.columns())
                    .await?;
                if define_response.len() <= PACKET_HEADER_SIZE {
                    return Err(Error::Protocol("Empty define response".to_string()));
                }

                // Check for MARKER packet
                let packet_type = define_response[4];
                if packet_type == PacketType::Marker as u8 {
                    let error_response = inner.handle_marker_reset().await?;
                    let payload = &error_response[PACKET_HEADER_SIZE..];
                    let parsed = self.parse_error_response(payload, &inner.capabilities);
                    return inner.checked_parse_result(parsed);
                }

                // Parse the response with LOB data, using the columns we already know
                let payload = &define_response[PACKET_HEADER_SIZE..];
                let parsed = self.parse_query_response_with_columns(
                    payload,
                    &inner.capabilities,
                    stmt_with_define.columns(),
                );
                result = inner.checked_parse_result(parsed)?;
            }

            Ok(result)
        })
        .await;
        let reported = self.take_reported_cursor();
        match &result {
            Ok(query) if query.has_more_rows => inner.remember_last_row(query.cursor_id, query),
            Ok(query) => {
                inner.release_cursor(query.cursor_id);
                inner.release_cursor(reported);
            }
            Err(_) => inner.release_cursor(reported),
        }
        result
    }

    /// Internal: Execute a DML statement with optional bind parameters
    async fn execute_dml_with_params(
        &self,
        statement: &Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let options = ExecuteOptions::for_dml(false); // Don't auto-commit
        let mut execute_msg = ExecuteMessage::new(statement, options);

        // Set bind values if provided
        if !params.is_empty() {
            execute_msg.set_bind_values(params.to_vec());
        }

        let mut inner = self.inner.lock().await;
        self.take_reported_cursor();
        let result = Box::pin(async {
            let large_sdu = inner.large_sdu;
            let seq_num = inner.next_sequence_number();
            execute_msg.set_sequence_number(seq_num);
            let request = execute_msg.build_request_with_sdu(&inner.capabilities, large_sdu)?;

            inner.send(&request).await?;

            let mut response = inner.receive_page().await?;
            if response[4] == PacketType::Marker as u8 {
                response = inner.handle_marker_reset().await?;
            }
            let result =
                self.parse_dml_response(&response[PACKET_HEADER_SIZE..], &inner.capabilities);
            inner.checked_parse_result(result)
        })
        .await;
        // DML, DDL e PL/SQL non hanno una fase di fetch: il cursore si chiude.
        let reported = self.take_reported_cursor();
        if let Ok(dml) = &result {
            inner.release_cursor(dml.cursor_id);
        }
        inner.release_cursor(reported);
        result
    }

    /// Parse query response to extract columns and rows
    ///
    /// Oracle sends multiple messages in a single response:
    /// - DescribeInfo (16): column metadata
    /// - RowHeader (6): row header info
    /// - RowData (7): actual column values
    /// - Error (4): completion status (may contain error or success)
    fn parse_query_response(&self, payload: &[u8], caps: &Capabilities) -> Result<QueryResult> {
        self.parse_query_response_with_columns(payload, caps, &[])
    }

    /// Parse query response with pre-known columns (for re-execute after define)
    fn parse_query_response_with_columns(
        &self,
        payload: &[u8],
        caps: &Capabilities,
        known_columns: &[ColumnInfo],
    ) -> Result<QueryResult> {
        if payload.len() < 3 {
            return Err(Error::Protocol("Query response too short".to_string()));
        }

        let mut buf = ReadBuffer::from_slice(payload);

        // Skip data flags
        buf.skip(2)?;

        // Use known columns if provided, otherwise parse from describe info
        let mut columns: Vec<ColumnInfo> = known_columns.to_vec();
        let mut rows: Vec<Row> = Vec::new();
        let mut cursor_id: u16 = 0;
        let mut row_count: u64 = 0;
        let mut end_of_response = false;
        let mut has_more_rows = None;

        // Bit vector for duplicate column optimization
        // When Some, indicates which columns have actual data (bit=1) vs duplicates (bit=0)
        let mut bit_vector: Option<Vec<u8>> = None;
        // Previous row values for copying duplicates
        let mut previous_row_values: Option<Vec<Value>> = None;

        // Process messages until we hit end of response or run out of data
        while !end_of_response && buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                x if x == MessageType::IoVector as u8 => {
                    Self::parse_io_vector(&mut buf, &[])?;
                }
                // DescribeInfo (16) - column metadata
                x if x == MessageType::DescribeInfo as u8 => {
                    // Skip chunked bytes first
                    buf.skip_raw_bytes_chunked()?;
                    columns = Self::parse_describe_info(&mut buf, caps.ttc_field_version)?;
                }

                // RowHeader (6) - header info for rows
                x if x == MessageType::RowHeader as u8 => {
                    bit_vector = Self::parse_row_header(&mut buf)?;
                }

                // RowData (7) - actual row values
                x if x == MessageType::RowData as u8 => {
                    let row = Self::parse_row_data_with_bitvector(
                        &mut buf,
                        &columns,
                        caps,
                        bit_vector.as_deref(),
                        previous_row_values.as_ref(),
                    )?;
                    // Store this row's values for potential duplicate copying
                    previous_row_values = Some(row.values().to_vec());
                    // Clear bit vector after using it (it's per-row)
                    bit_vector = None;
                    rows.push(row);
                }

                // Error (4) - completion or error
                x if x == MessageType::Error as u8 => {
                    let (error_code, error_msg, cid, rc) =
                        Self::parse_error_info_with_rowcount(&mut buf, caps)?;
                    self.report_cursor(cid);
                    cursor_id = cid;
                    row_count = rc;
                    has_more_rows = Some(error_code != 1403);
                    if error_code != 0 && error_code != 1403 {
                        // 1403 is "no data found" which is not an error for queries
                        return Err(Error::OracleError {
                            code: error_code,
                            message: error_msg.unwrap_or_default(),
                        });
                    }
                    end_of_response = true;
                }

                // Parameter (8) - return parameters
                x if x == MessageType::Parameter as u8 => {
                    Self::parse_return_parameters(&mut buf)?;
                }

                // Server side piggyback (23)
                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                // Status (9) - call status
                x if x == MessageType::Status as u8 => {
                    // Read call status and end-to-end seq number
                    let _call_status = buf.read_ub4()?;
                    let _end_to_end_seq = buf.read_ub2()?;
                    // Note: end_of_response only if supports_end_of_response is false
                    // For now, we assume it's not the end
                }

                // BitVector (21) - column presence bitmap for sparse results
                // Bit=1 means actual data is sent, bit=0 means duplicate from previous row
                21 => {
                    // Read num columns sent
                    let _num_columns_sent = buf.read_ub2()?;
                    // Read bit vector (1 byte per 8 columns, rounded up)
                    let num_bytes = (columns.len() + 7) / 8;
                    if num_bytes > 0 {
                        let bv = buf.read_bytes_vec(num_bytes)?;
                        bit_vector = Some(bv);
                    }
                }

                _ => {
                    return Err(Error::Protocol("Messaggio query non riconosciuto".into()));
                }
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: row_count,
            has_more_rows: has_more_rows
                .ok_or_else(|| Error::Protocol("Query senza completamento del cursore".into()))?,
            cursor_id,
        })
    }

    /// Parse a PL/SQL response containing OUT parameter values
    ///
    /// PL/SQL responses may contain:
    /// - IoVector (11): bind directions for each parameter
    /// - RowData (7): OUT parameter values
    /// - FlushOutBinds (19): signals end of OUT bind data
    /// - Error (4): completion status
    fn parse_plsql_response(
        &self,
        payload: &[u8],
        caps: &Capabilities,
        params: &[BindParam],
    ) -> Result<PlsqlResult> {
        if payload.len() < 3 {
            return Err(Error::Protocol("PL/SQL response too short".to_string()));
        }

        let mut buf = ReadBuffer::from_slice(payload);

        // Skip data flags
        buf.skip(2)?;

        let mut out_values: Vec<Value> = Vec::new();
        let mut _out_indices: Vec<usize> = Vec::new();
        let mut row_count: u64 = 0;
        let mut cursor_id: Option<u16> = None;
        let mut end_of_response = false;
        let mut implicit_results = ImplicitResults::new();

        // Create column infos for OUT params based on their oracle types
        let mut out_columns: Vec<ColumnInfo> = Vec::new();

        while !end_of_response && buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                // IoVector (11) - bind directions from server
                x if x == MessageType::IoVector as u8 => {
                    let (indices, cols) = Self::parse_io_vector(&mut buf, params)?;
                    _out_indices = indices;
                    out_columns = cols;
                }

                // RowHeader (6)
                x if x == MessageType::RowHeader as u8 => {
                    Self::parse_row_header(&mut buf)?;
                }

                // RowData (7) - OUT parameter values
                x if x == MessageType::RowData as u8 => {
                    if !out_columns.is_empty() {
                        let row = self.parse_row_data_single(&mut buf, &out_columns, caps)?;
                        // Extract values from the row into out_values
                        for (idx, value) in row.into_values().into_iter().enumerate() {
                            // Check if this is a cursor
                            if let Value::Cursor(cursor) = &value {
                                if cursor_id.is_none() && cursor.cursor_id() != 0 {
                                    cursor_id = Some(cursor.cursor_id());
                                }
                            }
                            // Map back to original param position if we have indices
                            if idx < out_values.len() {
                                out_values[idx] = value;
                            } else {
                                out_values.push(value);
                            }
                        }
                    } else {
                        // Skip the row data if we don't have column info
                        // This shouldn't normally happen
                        break;
                    }
                }

                // DescribeInfo (16) - for REF CURSOR describe
                x if x == MessageType::DescribeInfo as u8 => {
                    buf.skip_raw_bytes_chunked()?;
                    let cursor_columns =
                        Self::parse_describe_info(&mut buf, caps.ttc_field_version)?;
                    // Store cursor columns if needed
                    let _ = cursor_columns; // For now, just skip
                }

                // FlushOutBinds (19) - signals end of OUT bind data
                x if x == MessageType::FlushOutBinds as u8 => {
                    // This indicates that OUT bind data is done
                    // Just continue to get the error/completion status
                }

                // Error (4) - completion or error
                x if x == MessageType::Error as u8 => {
                    let (error_code, error_msg, cid, rc) =
                        Self::parse_error_info_with_rowcount(&mut buf, caps)?;
                    self.report_cursor(cid);
                    row_count = rc;
                    if error_code != 0 {
                        return Err(Error::OracleError {
                            code: error_code,
                            message: error_msg.unwrap_or_default(),
                        });
                    }
                    end_of_response = true;
                }

                // Parameter (8) - return parameters
                x if x == MessageType::Parameter as u8 => {
                    Self::parse_return_parameters(&mut buf)?;
                }

                // Server side piggyback (23)
                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                // Status (9)
                x if x == MessageType::Status as u8 => {
                    let _call_status = buf.read_ub4()?;
                    let _end_to_end_seq = buf.read_ub2()?;
                }

                // ImplicitResultset (27) - result sets from DBMS_SQL.RETURN_RESULT
                x if x == MessageType::ImplicitResultset as u8 => {
                    let parsed_results = Self::parse_implicit_results(&mut buf, caps)?;
                    implicit_results = parsed_results;
                }

                _ => {
                    // Unknown message type - break to avoid parsing errors
                    break;
                }
            }
        }

        // If no IoVector was received, all params might be IN-only
        // In that case, out_values should be empty
        Ok(PlsqlResult {
            out_values,
            rows_affected: row_count,
            cursor_id,
            implicit_results,
        })
    }

    /// Parse implicit result sets from DBMS_SQL.RETURN_RESULT
    ///
    /// Format per Python base.pyx _process_implicit_result:
    /// - num_results: ub4 (number of implicit result sets)
    /// - For each result:
    ///   - num_bytes: ub1 + raw bytes (metadata to skip)
    ///   - describe_info: column metadata
    ///   - cursor_id: ub2
    fn parse_implicit_results(
        buf: &mut ReadBuffer,
        caps: &Capabilities,
    ) -> Result<ImplicitResults> {
        let num_results = buf.read_ub4()?;
        let mut results = ImplicitResults::new();

        for _ in 0..num_results {
            // Skip metadata bytes
            let num_bytes = buf.read_u8()?;
            if num_bytes > 0 {
                buf.skip(num_bytes as usize)?;
            }

            // Parse column metadata for this result set
            let columns = Self::parse_describe_info(buf, caps.ttc_field_version)?;

            // Read cursor ID
            let cursor_id = buf.read_ub2()?;

            // Create implicit result with metadata but no rows yet
            // Rows will be fetched separately using fetch_implicit_result
            let result = ImplicitResult::new(cursor_id, columns, Vec::new());
            results.add(result);
        }

        Ok(results)
    }

    /// Parse IO Vector message to get bind directions
    ///
    /// Returns a tuple of:
    /// - indices of OUT/INOUT parameters in the params list
    /// - column infos for parsing OUT values
    fn parse_io_vector(
        buf: &mut ReadBuffer,
        params: &[BindParam],
    ) -> Result<(Vec<usize>, Vec<ColumnInfo>)> {
        // I/O vector format (from Python reference):
        // - skip 1 byte (flag)
        // - read ub2 (num requests)
        // - read ub4 (num iters)
        // - num_binds = num_iters * 256 + num_requests
        // - skip ub4 (num iters this time)
        // - skip ub2 (uac buffer length)
        // - read ub2 (num_bytes for bit vector), skip if > 0
        // - read ub2 (num_bytes for rowid), skip if > 0
        // - for each bind: read ub1 (bind_dir)

        buf.skip(1)?; // flag
        let num_requests = buf.read_ub2()? as u32;
        let num_iters = buf.read_ub4()?;
        let num_binds = num_iters
            .checked_mul(256)
            .and_then(|count| count.checked_add(num_requests))
            .ok_or_else(|| Error::Protocol("Oracle bind count overflow".into()))?;
        let _ = buf.read_ub4()?; // num iters this time (discard)
        let _ = buf.read_ub2()?; // uac buffer length (discard)

        // Bit vector
        let num_bytes = buf.read_ub2()? as usize;
        if num_bytes > 0 {
            buf.skip(num_bytes)?;
        }

        // Rowid (raw bytes, not length-prefixed here)
        let num_bytes = buf.read_ub4()? as usize;
        if num_bytes > 0 {
            buf.skip_raw_bytes_chunked()?; // Skip rowid bytes using chunked read
        }

        // Read bind directions
        let mut out_indices = Vec::new();
        let mut out_columns = Vec::new();

        // Wire consumption must not depend on locally available bind metadata.
        for i in 0..num_binds as usize {
            let dir_byte = buf.read_u8()?;
            let dir = BindDirection::try_from(dir_byte)?;

            // If this is not an INPUT-only parameter, it has OUT data
            if dir != BindDirection::Input {
                out_indices.push(i);

                // Create a column info for parsing the OUT value
                let param = params.get(i).ok_or_else(|| {
                    Error::Protocol("Oracle output bind without parameter metadata".into())
                })?;
                let mut col = ColumnInfo::new(format!("OUT_{}", i), param.oracle_type);
                col.buffer_size = param.buffer_size;
                col.data_size = param.buffer_size;
                col.nullable = true;

                // For collection OUT params, extract element type from the placeholder
                if let Some(Value::Collection(ref placeholder)) = param.value {
                    if let Some(Value::Integer(elem_type_code)) = placeholder.get("_element_type") {
                        col.element_type =
                            crate::constants::OracleType::try_from(*elem_type_code as u8).ok();
                    }
                }

                out_columns.push(col);
            }
        }

        Ok((out_indices, out_columns))
    }

    /// Parse row header (TNS_MSG_TYPE_ROW_HEADER = 6)
    fn parse_row_header(buf: &mut ReadBuffer) -> Result<Option<Vec<u8>>> {
        buf.skip_ub1()?; // flags
        buf.skip_ub2()?; // num requests
        buf.skip_ub4()?; // iteration number
        buf.skip_ub4()?; // num iters
        buf.skip_ub2()?; // buffer length
        let num_bytes = buf.read_ub4()? as usize;
        let bits = if num_bytes > 0 {
            buf.skip_ub1()?; // skip repeated length
            Some(buf.read_bytes_vec(num_bytes)?)
        } else {
            None
        };
        let num_bytes = buf.read_ub4()? as usize;
        if num_bytes > 0 {
            buf.skip_raw_bytes_chunked()?; // rxhrid
        }
        Ok(bits)
    }

    /// Parse return parameters (TNS_MSG_TYPE_PARAMETER = 8)
    fn parse_return_parameters(buf: &mut ReadBuffer) -> Result<()> {
        Self::parse_return_parameters_internal(buf, false).map(|_| ())
    }

    // SYNC side messages precede completion on Oracle Free (opcode 5).
    // Consume fields by their lengths; unknown opcodes remain fail-closed.
    fn parse_server_piggyback(buf: &mut ReadBuffer) -> Result<()> {
        match buf.read_u8()? {
            1 | 3 => {}
            opcode @ (5 | 9) => {
                buf.skip_ub2()?;
                buf.skip_ub1()?;
                if opcode == 5 {
                    let pairs = buf.read_ub2()?;
                    buf.skip_ub1()?;
                    for _ in 0..pairs {
                        for _ in 0..2 {
                            let length = buf.read_ub2()?;
                            if length != 0 {
                                buf.skip_raw_bytes_chunked()?;
                            }
                        }
                        buf.skip_ub2()?;
                    }
                    buf.skip_ub4()?;
                }
            }
            _ => {
                return Err(Error::Protocol(
                    "Unsupported Oracle server piggyback".into(),
                ))
            }
        }
        Ok(())
    }

    /// Parse return parameters with optional row counts extraction
    /// When `want_row_counts` is true, attempts to read arraydmlrowcounts from the response.
    fn parse_return_parameters_internal(
        buf: &mut ReadBuffer,
        want_row_counts: bool,
    ) -> Result<Option<Vec<u64>>> {
        // Per Python's _process_return_parameters
        let num_params = buf.read_ub2()?; // al8o4l (ignored)
        for _ in 0..num_params {
            buf.skip_ub4()?;
        }

        let al8txl = buf.read_ub2()?; // al8txl (ignored)
        if al8txl > 0 {
            buf.skip(al8txl as usize)?;
        }

        // num key/value pairs - skip for now
        let num_pairs = buf.read_ub2()?;
        for _ in 0..num_pairs {
            buf.read_bytes_with_length()?; // text value
            buf.read_bytes_with_length()?; // binary value
            buf.skip_ub2()?; // keyword num
        }

        // registration
        let num_bytes = buf.read_ub2()?;
        if num_bytes > 0 {
            buf.skip(num_bytes as usize)?;
        }

        // If arraydmlrowcounts was requested, parse the row counts
        if want_row_counts {
            let num_rows = buf.read_ub4()? as usize;
            let mut row_counts = Vec::new();
            for _ in 0..num_rows {
                let count = buf.read_ub8()?;
                row_counts.push(count);
            }
            Ok(Some(row_counts))
        } else {
            Ok(None)
        }
    }

    /// Parse a single row of data
    fn parse_row_data_single(
        &self,
        buf: &mut ReadBuffer,
        columns: &[ColumnInfo],
        caps: &Capabilities,
    ) -> Result<Row> {
        let mut values = Vec::with_capacity(columns.len());

        for col in columns {
            let value = Self::parse_column_value(buf, col, caps)?;
            values.push(value);
        }

        Ok(Row::new(values))
    }

    /// Parse a single row of data with bit vector support for duplicate column optimization
    ///
    /// Oracle sends a BitVector message before RowData when some columns have the same
    /// value as the previous row. Bits that are SET (1) indicate data is sent in the buffer;
    /// bits that are CLEAR (0) indicate the value should be copied from the previous row.
    fn parse_row_data_with_bitvector(
        buf: &mut ReadBuffer,
        columns: &[ColumnInfo],
        caps: &Capabilities,
        bit_vector: Option<&[u8]>,
        previous_values: Option<&Vec<Value>>,
    ) -> Result<Row> {
        let mut values = Vec::with_capacity(columns.len());

        for (col_idx, col) in columns.iter().enumerate() {
            // Check if this column is a duplicate (bit=0 means duplicate)
            let is_duplicate = if let Some(bv) = bit_vector {
                let byte_num = col_idx / 8;
                let bit_num = col_idx % 8;
                if byte_num < bv.len() {
                    // If bit is 0, it's a duplicate
                    (bv[byte_num] & (1 << bit_num)) == 0
                } else {
                    false
                }
            } else {
                false
            };

            if is_duplicate {
                // Copy value from previous row. Plenora fork: a duplicate
                // without a previous value is a protocol error; it used to
                // become NULL in silence.
                let previous = previous_values
                    .and_then(|prev| prev.get(col_idx))
                    .ok_or_else(|| {
                        Error::Protocol("duplicate column without a previous row".to_string())
                    })?;
                values.push(previous.clone());
            } else {
                // Read actual value from buffer
                let value = Self::parse_column_value(buf, col, caps)?;
                values.push(value);
            }
        }

        Ok(Row::new(values))
    }

    /// Parse a single column value from the buffer
    fn parse_column_value(
        buf: &mut ReadBuffer,
        col: &ColumnInfo,
        caps: &Capabilities,
    ) -> Result<Value> {
        use crate::constants::OracleType;

        // Handle LOB columns specially - they have a different format
        if col.is_lob() {
            return Self::parse_lob_value(buf, col);
        }

        // Handle CURSOR type - REF CURSOR from PL/SQL
        if col.oracle_type == OracleType::Cursor {
            return Self::parse_cursor_value(buf, caps);
        }

        // Handle Object type - collections (VARRAY, Nested Table) and UDTs
        if col.oracle_type == OracleType::Object {
            return Self::parse_object_value(buf, col);
        }

        // Read the value based on the column type
        // First, check if it's NULL
        let data = buf.read_bytes_with_length()?;

        match data {
            None => Ok(Value::Null),
            Some(bytes) if bytes.is_empty() => Ok(Value::Null),
            Some(bytes) => {
                // Decode based on oracle type
                match col.oracle_type {
                    OracleType::Number => {
                        // Oracle NUMBER format - decode to string
                        let num = crate::types::decode_oracle_number(&bytes)?;
                        Ok(Value::String(num.value))
                    }
                    // Il testo si decodifica esatto o non si decodifica: una
                    // sostituzione con U+FFFD cambierebbe il valore in silenzio.
                    // La forma nazionale (`csfrm` 2) viaggia in UTF-16BE.
                    OracleType::Varchar | OracleType::Char | OracleType::Long => {
                        decode_column_text(&bytes, col.csfrm, col.charset_id).map(Value::String)
                    }
                    OracleType::BinaryDouble => {
                        let bytes: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
                            Error::DataConversionError("BINARY_DOUBLE senza 8 byte".to_string())
                        })?;
                        Ok(Value::Float(crate::types::decode_binary_double(&bytes)))
                    }
                    OracleType::BinaryFloat => {
                        let bytes: [u8; 4] = bytes.as_slice().try_into().map_err(|_| {
                            Error::DataConversionError("BINARY_FLOAT senza 4 byte".to_string())
                        })?;
                        Ok(Value::Float(f64::from(crate::types::decode_binary_float(
                            &bytes,
                        ))))
                    }
                    OracleType::Boolean => match bytes.last() {
                        Some(1) => Ok(Value::Boolean(true)),
                        Some(0) => Ok(Value::Boolean(false)),
                        _ => Err(Error::DataConversionError(
                            "BOOLEAN Oracle non decodificabile".to_string(),
                        )),
                    },
                    OracleType::Raw | OracleType::LongRaw => {
                        // RAW/LONG RAW types - return as bytes
                        Ok(Value::Bytes(bytes.to_vec()))
                    }
                    OracleType::Date => {
                        // Oracle DATE format - 7 bytes
                        let date = crate::types::decode_oracle_date(&bytes)?;
                        Ok(Value::Date(date))
                    }
                    OracleType::Timestamp | OracleType::TimestampLtz => {
                        // Oracle TIMESTAMP format - 11 bytes (date + fractional seconds)
                        let ts = crate::types::decode_oracle_timestamp(&bytes)?;
                        Ok(Value::Timestamp(ts))
                    }
                    OracleType::TimestampTz => {
                        // Oracle TIMESTAMP WITH TIME ZONE - 13 bytes
                        let ts = crate::types::decode_oracle_timestamp(&bytes)?;
                        Ok(Value::Timestamp(ts))
                    }
                    // Un tipo senza decoder non diventa testo per caso: i
                    // suoi byte letti come UTF-8 sarebbero un valore falso.
                    _ => Err(Error::DataConversionError(
                        "tipo di colonna Oracle senza decoder".to_string(),
                    )),
                }
            }
        }
    }

    /// Parse a REF CURSOR value from the buffer
    ///
    /// Per Python base.pyx lines 1038-1046:
    /// - Skip 1 byte (length indicator - fixed value)
    /// - Read describe info (column metadata for the cursor)
    /// - Read cursor_id (UB2)
    fn parse_cursor_value(buf: &mut ReadBuffer, caps: &Capabilities) -> Result<Value> {
        use crate::types::RefCursor;

        // Skip length indicator (fixed value for cursors)
        let _length = buf.read_u8()?;

        // Read column metadata for this cursor
        let cursor_columns = Self::parse_describe_info(buf, caps.ttc_field_version)?;

        // Read the cursor ID
        let cursor_id = buf.read_ub2()?;

        // Create RefCursor with the metadata
        let ref_cursor = RefCursor::new(cursor_id, cursor_columns);

        Ok(Value::Cursor(ref_cursor))
    }

    /// Parse an Object/Collection value from the buffer
    ///
    /// Object format from Oracle (per Python packet.pyx read_dbobject):
    /// - UB4: type OID length, then type OID bytes if > 0
    /// - UB4: OID length, then OID bytes if > 0
    /// - UB4: snapshot length, then snapshot bytes if > 0 (discarded)
    /// - UB2: version (skip)
    /// - UB4: packed data length
    /// - UB2: flags (skip)
    /// - Bytes: packed data (pickle format)
    fn parse_object_value(buf: &mut ReadBuffer, col: &ColumnInfo) -> Result<Value> {
        use crate::dbobject::{CollectionType, DbObject, DbObjectType};
        use crate::types::decode_collection;

        // Read type OID
        let toid_len = buf.read_ub4()?;
        let _toid = if toid_len > 0 {
            Some(buf.read_bytes_vec(toid_len as usize)?)
        } else {
            None
        };

        // Read OID
        let oid_len = buf.read_ub4()?;
        let _oid = if oid_len > 0 {
            Some(buf.read_bytes_vec(oid_len as usize)?)
        } else {
            None
        };

        // Read and discard snapshot
        let snapshot_len = buf.read_ub4()?;
        if snapshot_len > 0 {
            buf.skip_raw_bytes_chunked()?;
        }

        // Skip version (length-prefixed UB2)
        let _version = buf.read_ub2()?;

        // Read packed data length
        let data_len = buf.read_ub4()?;

        // Skip flags (length-prefixed UB2)
        let _flags = buf.read_ub2()?;

        if data_len == 0 {
            return Ok(Value::Null);
        }

        // Read packed data (chunked format like other byte sequences)
        let packed_data = buf.read_bytes_with_length()?;

        match packed_data {
            None => Ok(Value::Null),
            Some(data) if data.is_empty() => Ok(Value::Null),
            Some(data) => {
                // Create a placeholder type based on column info
                let type_name = col
                    .type_name
                    .clone()
                    .unwrap_or_else(|| "UNKNOWN".to_string());

                // Try to determine if this is a collection based on the pickle data
                // The first byte contains flags - check for IS_COLLECTION (0x08)
                let is_collection = !data.is_empty() && (data[0] & 0x08) != 0;

                if is_collection {
                    // Plenora fork: the element type must be known. The
                    // describe parser never sets it, and the VARCHAR default
                    // decoded every collection as one of strings.
                    let element_type = col.element_type.ok_or_else(|| {
                        Error::DataConversionError(
                            "collection column without its element type".to_string(),
                        )
                    })?;

                    // Determine collection type from pickle flags
                    // Collection flags are after header - but we'll default for now
                    let collection_type = CollectionType::Varray;

                    let obj_type = DbObjectType::collection(
                        &col.type_schema.clone().unwrap_or_default(),
                        &type_name,
                        collection_type,
                        element_type,
                    );

                    // Plenora fork: a collection that does not decode is an
                    // error. It used to come back as its raw pickle bytes,
                    // indistinguishable from a RAW value (and the warning
                    // logged data bytes, against the no-data-in-errors rule).
                    decode_collection(&obj_type, &data).map(Value::Collection)
                } else {
                    // Regular object type - not yet fully implemented
                    let mut obj = DbObject::new(&type_name);
                    // Store raw pickle data for later inspection
                    obj.set("_raw_data", Value::Bytes(data));
                    Ok(Value::Collection(obj))
                }
            }
        }
    }

    /// Parse a LOB column value from the buffer
    ///
    /// LOB format from Oracle (per Python's read_lob_with_length):
    /// - UB4: num_bytes (indicator that LOB data follows)
    /// - If num_bytes > 0:
    ///   - For non-BFILE: UB8 size, UB4 chunk_size
    ///   - Bytes: LOB locator (chunked format)
    ///
    /// The actual LOB content must be fetched separately using LOB operations.
    /// For JSON columns, the data is OSON-encoded and decoded directly.
    /// For VECTOR columns, the data is decoded from Oracle's vector binary format.
    fn parse_lob_value(buf: &mut ReadBuffer, col: &ColumnInfo) -> Result<Value> {
        use crate::constants::OracleType;
        use crate::types::{decode_vector, OsonDecoder};

        // Read length indicator
        let num_bytes = buf.read_ub4()?;

        if num_bytes == 0 {
            // Plenora fork: SQL NULL in a JSON column is `Value::Null`. It was
            // `Value::Json(Null)`, the JSON literal `null`, so a NULL row read
            // back as the text "null" and could not be told from a stored
            // JSON null.
            if col.oracle_type == OracleType::Json
                || col.is_json
                || col.oracle_type == OracleType::Vector
            {
                return Ok(Value::Null);
            }
            return Ok(Value::Lob(LobValue::Null));
        }

        // For BFILE, there's no size/chunk_size metadata
        let (size, chunk_size) = if col.oracle_type == OracleType::Bfile {
            (0u64, 0u32)
        } else {
            // Read LOB size and chunk size
            let size = buf.read_ub8()?;
            let chunk_size = buf.read_ub4()?;
            (size, chunk_size)
        };

        // Read LOB data (could be locator or inline data depending on size)
        let data_bytes = buf.read_bytes_with_length()?;

        // Handle JSON columns - decode OSON format
        // JSON is sent as a LOB with prefetched data + a LOB locator that must be consumed
        if col.oracle_type == OracleType::Json || col.is_json {
            // Read and discard the LOB locator
            let _locator = buf.read_bytes_with_length()?;

            if let Some(data) = data_bytes {
                if !data.is_empty() {
                    // Decode OSON to JSON.
                    //
                    // Plenora fork: upstream logged the failure and returned
                    // JSON null, so a document whose NUMBER cannot be read
                    // exactly (or any undecodable document) reached the caller
                    // as a valid null. The error now reaches the caller.
                    return OsonDecoder::decode(bytes::Bytes::from(data)).map(Value::Json);
                }
            }
            // Plenora fork: a JSON value without OSON bytes is not the JSON
            // literal `null` (SQL NULL is `num_bytes == 0`, above); it was
            // returned as `Value::Json(Null)`.
            return Err(Error::Protocol("JSON value without OSON data".to_string()));
        }

        // Handle VECTOR columns - decode vector binary format
        if col.oracle_type == OracleType::Vector {
            // Read and discard LOB locator (not needed for VECTOR)
            let _locator = buf.read_bytes_with_length()?;

            // Plenora fork: a VECTOR that does not decode is an error, it
            // used to become NULL.
            return match data_bytes {
                Some(data) if !data.is_empty() => decode_vector(&data).map(Value::Vector),
                _ => Err(Error::Protocol("VECTOR value without data".to_string())),
            };
        }

        // Create a LOB locator for fetching the data later
        if let Some(locator_data) = data_bytes {
            if !locator_data.is_empty() {
                let locator = LobLocator::new(
                    bytes::Bytes::from(locator_data),
                    size,
                    chunk_size,
                    col.oracle_type,
                    col.csfrm,
                )
                .with_charset(col.charset_id);
                return Ok(Value::Lob(LobValue::locator(locator)));
            }
        }

        // If we have size but no locator, it might be an empty LOB
        if size == 0 {
            return Ok(Value::Lob(LobValue::Empty));
        }

        // Plenora fork: a LOB with a size but no locator cannot be read; it
        // used to be reported as an empty LOB.
        Err(Error::Protocol("LOB value without a locator".to_string()))
    }

    /// Parse error info message and extract cursor_id
    /// Format per Python's _process_error_info in base.pyx
    fn parse_error_info(
        &self,
        buf: &mut ReadBuffer,
        caps: &Capabilities,
    ) -> Result<(u32, Option<String>, u16)> {
        let (code, message, cursor_id, _) = Self::parse_error_info_with_rowcount(buf, caps)?;
        Ok((code, message, cursor_id))
    }

    fn parse_error_response(&self, payload: &[u8], caps: &Capabilities) -> Result<QueryResult> {
        if payload.len() < 3 {
            return Err(Error::Protocol("Error response too short".to_string()));
        }

        let mut buf = ReadBuffer::from_slice(payload);

        // Skip data flags
        buf.skip(2)?;

        while buf.remaining() > 0 {
            match buf.read_u8()? {
                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                x if x == MessageType::Error as u8 => {
                    let (code, message, cid, _) =
                        Self::parse_error_info_with_rowcount(&mut buf, caps)?;
                    self.report_cursor(cid);
                    return Err(Error::OracleError {
                        code,
                        message: message.unwrap_or_else(|| format!("ORA-{:05}", code)),
                    });
                }
                _ => return Err(Error::Protocol("Expected Oracle error completion".into())),
            }
        }
        Err(Error::Protocol("Missing Oracle error completion".into()))
    }

    /// Parse DML response to extract rows affected
    fn parse_dml_response(&self, payload: &[u8], caps: &Capabilities) -> Result<QueryResult> {
        if payload.len() < 3 {
            return Err(Error::Protocol("DML response too short".to_string()));
        }

        let mut buf = ReadBuffer::from_slice(payload);

        // Skip data flags
        buf.skip(2)?;

        let mut rows_affected: u64 = 0;
        let mut cursor_id: u16 = 0;
        let mut end_of_response = false;

        // Process messages until end_of_response or out of data
        // Note: If supports_end_of_response is true, we must continue until msg type 29
        while !end_of_response && buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                x if x == MessageType::IoVector as u8 => {
                    Self::parse_io_vector(&mut buf, &[])?;
                }

                // Error (4) - may contain error or success info
                x if x == MessageType::Error as u8 => {
                    let (error_code, error_msg, cid, row_count) =
                        Self::parse_error_info_with_rowcount(&mut buf, caps)?;
                    self.report_cursor(cid);
                    cursor_id = cid;
                    rows_affected = row_count;
                    if error_code != 0 && error_code != 1403 {
                        return Err(Error::OracleError {
                            code: error_code,
                            message: error_msg.unwrap_or_default(),
                        });
                    }
                    // Only end if server doesn't support end_of_response
                    // Otherwise, continue until we get msg type 29
                }

                // Parameter (8) - return parameters
                x if x == MessageType::Parameter as u8 => {
                    Self::parse_return_parameters(&mut buf)?;
                }

                // Server side piggyback (23)
                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                // Status (9) - call status
                x if x == MessageType::Status as u8 => {
                    let _call_status = buf.read_ub4()?;
                    let _end_to_end_seq = buf.read_ub2()?;
                }

                // BitVector (21)
                21 => {
                    let _num_columns_sent = buf.read_ub2()?;
                    // No columns for DML, but read the byte if present
                    if buf.remaining() > 0 {
                        let _byte = buf.read_u8()?;
                    }
                }

                // End of Response (29) - explicit end marker
                29 => {
                    end_of_response = true;
                }

                _ => {
                    // Unknown message type - continue processing
                }
            }
        }

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected,
            has_more_rows: false,
            cursor_id,
        })
    }

    /// Parse error info and return (error_code, error_msg, cursor_id, row_count)
    fn parse_error_info_with_rowcount(
        buf: &mut ReadBuffer,
        caps: &Capabilities,
    ) -> Result<(u32, Option<String>, u16, u64)> {
        // End of call status
        let _call_status = buf.read_ub4()?;
        // End to end seq#
        buf.skip_ub2()?;
        // Current row number
        buf.skip_ub4()?;
        // Error number (short form)
        buf.skip_ub2()?;
        // Array elem error
        buf.skip_ub2()?;
        // Array elem error
        buf.skip_ub2()?;
        // Cursor ID
        let cursor_id = buf.read_ub2()?;
        // Error position
        let _error_pos = buf.read_sb2()?;
        // SQL type (19c and earlier)
        buf.skip_ub1()?;
        // Fatal?
        buf.skip_ub1()?;
        // Flags
        buf.skip_ub1()?;
        // User cursor options
        buf.skip_ub1()?;
        // UPI parameter
        buf.skip_ub1()?;
        // Flags (second)
        buf.skip_ub1()?;
        // Rowid (rba, partition_id, skip 1, block_num, slot_num)
        buf.skip_ub4()?; // rba
        buf.skip_ub2()?; // partition_id
        buf.skip_ub1()?; // skip
        buf.skip_ub4()?; // block_num
        buf.skip_ub2()?; // slot_num
                         // OS error
        buf.skip_ub4()?;
        // Statement number
        buf.skip_ub1()?;
        // Call number
        buf.skip_ub1()?;
        // Padding
        buf.skip_ub2()?;
        // Success iters
        buf.skip_ub4()?;
        // oerrdd (logical rowid)
        let oerrdd_len = buf.read_ub4()?;
        if oerrdd_len > 0 {
            buf.skip_raw_bytes_chunked()?;
        }

        // Batch error codes array
        let num_batch_errors = buf.read_ub2()?;
        if num_batch_errors > 0 {
            buf.skip_ub1()?; // first byte
            for _ in 0..num_batch_errors {
                buf.skip_ub2()?; // error code
            }
        }

        // Batch error row offset array
        let num_offsets = buf.read_ub4()?;
        if num_offsets > 0 {
            buf.skip_ub1()?; // first byte
            for _ in 0..num_offsets {
                buf.skip_ub4()?; // offset
            }
        }

        // Batch error messages array
        let num_batch_msgs = buf.read_ub2()?;
        if num_batch_msgs > 0 {
            buf.skip_ub1()?; // packed size
            for _ in 0..num_batch_msgs {
                buf.skip_ub2()?; // chunk length
                buf.read_string_with_length()?; // message
                buf.skip(2)?; // end marker
            }
        }

        // Extended error number (UB4)
        let error_code = buf.read_ub4()?;
        // Row count (UB8) - this is the rows affected!
        let row_count = buf.read_ub8()?;

        // Questi campi esistono solo dalla versione TTC 16 negoziata.
        if caps.ttc_field_version >= 16 {
            buf.skip_ub4()?; // sql_type
            buf.skip_ub4()?; // server_checksum
        }

        // Error message
        let error_msg = if error_code != 0 {
            buf.read_string_with_length()?.map(|s| s.trim().to_string())
        } else {
            None
        };

        Ok((error_code, error_msg, cursor_id, row_count))
    }

    /// Parse describe info from response to extract column metadata
    ///
    /// Per Python's _process_describe_info, the format is:
    /// - UB4: max row size (skip)
    /// - UB4: number of columns
    /// - If num_columns > 0: UB1 (skip one byte)
    /// - For each column: metadata fields
    /// - After columns: current date, dcb flags, etc.
    fn parse_describe_info(buf: &mut ReadBuffer, ttc_field_version: u8) -> Result<Vec<ColumnInfo>> {
        use crate::constants::ccap_value;

        // Skip max row size
        buf.skip_ub4()?;

        // Read number of columns
        let num_columns = buf.read_ub4()? as usize;
        if num_columns == 0 {
            return Ok(Vec::new());
        }

        // Skip one byte if we have columns
        buf.skip_ub1()?;

        let mut columns = Vec::new();

        for _col_idx in 0..num_columns {
            // Parse column metadata per Python's _process_metadata
            // Plenora fork: precision and scale (after the flags) are SB1.
            // Read as unsigned, a NUMBER without scale (-127) reported 129 and
            // NUMBER(p,-2) 254.
            let ora_type_num = buf.read_u8()?;
            buf.skip_ub1()?; // flags
            let precision = i16::from(i8::from_be_bytes([buf.read_u8()?])); // precision as SB1
            let scale = i16::from(i8::from_be_bytes([buf.read_u8()?])); // scale as SB1
            let buffer_size = buf.read_ub4()?;

            buf.skip_ub4()?; // max_num_array_elements
            buf.skip_ub8()?; // cont_flags
            let _oid = buf.read_bytes_with_length()?; // OID
            buf.skip_ub2()?; // version
                             // La forma e l'id del charset decidono la decodifica del testo
                             // (`decode_column_text`): scartarli faceva leggere un NVARCHAR2
                             // in AL16UTF16 come UTF-8, `'ab'` diventava "\0a\0b".
            let charset_id = buf.read_ub2()?; // charset_id
            let csfrm = buf.read_u8()?; // charset form
            let max_size = buf.read_ub4()?;

            // For TTC field version >= 12.2 (8), skip oaccolid
            if ttc_field_version >= ccap_value::FIELD_VERSION_12_2 {
                buf.skip_ub4()?; // oaccolid
            }

            // Plenora fork: a column without a name (below) is a protocol
            // error; it used to become "", colliding with any other unnamed
            // column.
            let _nulls_allowed = buf.read_u8()?;
            buf.skip_ub1()?; // v7 length of name
            let name = buf
                .read_string_with_ub4_length()?
                .ok_or_else(|| Error::Protocol("column without a name".to_string()))?;
            let _schema = buf.read_string_with_ub4_length()?; // schema
            let _type_name = buf.read_string_with_ub4_length()?; // type_name
            buf.skip_ub2()?; // column position
            buf.skip_ub4()?; // uds_flags

            // For TTC field version >= 23.1 (17), read domain fields
            if ttc_field_version >= ccap_value::FIELD_VERSION_23_1 {
                let _domain_schema = buf.read_string_with_ub4_length()?;
                let _domain_name = buf.read_string_with_ub4_length()?;
            }

            // For TTC field version >= 20 (23.1_EXT_3), read annotations
            if ttc_field_version >= 20 {
                let num_annotations = buf.read_ub4()?;
                if num_annotations > 0 {
                    buf.skip_ub1()?;
                    // Read the actual annotations count (yes, it's read twice in Python)
                    let actual_num = buf.read_ub4()?;
                    buf.skip_ub1()?;
                    for _ in 0..actual_num {
                        // Skip annotation key and value (both are string with UB4 length)
                        let _key = buf.read_string_with_ub4_length()?;
                        let _value = buf.read_string_with_ub4_length()?;
                        buf.skip_ub4()?; // flags per annotation
                    }
                    buf.skip_ub4()?; // final flags
                }
            }

            // For TTC field version >= 24 (23.4), read vector fields
            if ttc_field_version >= ccap_value::FIELD_VERSION_23_4 {
                buf.skip_ub4()?; // vector_dimensions
                buf.skip_ub1()?; // vector_format
                buf.skip_ub1()?; // vector_flags
            }

            // Convert data type to OracleType. Plenora fork: an unknown type
            // is refused; it used to be described as VARCHAR2 and its bytes
            // decoded as text.
            let oracle_type = crate::constants::OracleType::try_from(ora_type_num)?;

            let mut col = ColumnInfo::new(&name, oracle_type);
            // NCHAR, NVARCHAR2 e NCLOB arrivano nel charset nazionale
            // (AL16UTF16): senza il form il testo si decodificava come UTF-8.
            col.csfrm = csfrm;
            col.data_size = if max_size > 0 { max_size } else { buffer_size };
            // Identity casts: kept as upstream wrote them (the values are
            // already signed i16 above).
            col.precision = precision as i16;
            col.scale = scale as i16;
            col.csfrm = csfrm;
            col.charset_id = charset_id;
            columns.push(col);
        }

        // After columns: skip remaining describe info fields
        // Python's _process_describe_info uses:
        //   buf.read_ub4(&num_bytes)
        //   if num_bytes > 0:
        //       buf.skip_raw_bytes_chunked()    # current date
        //   buf.skip_ub4()                      # dcbflag
        //   buf.skip_ub4()                      # dcbmdbz
        //   buf.skip_ub4()                      # dcbmnpr
        //   buf.skip_ub4()                      # dcbmxpr
        //   buf.read_ub4(&num_bytes)
        //   if num_bytes > 0:
        //       buf.skip_raw_bytes_chunked()    # dcbqcky

        // current_date - read UB4 indicator first, then skip chunked bytes if > 0
        let current_date_indicator = buf.read_ub4()?;
        if current_date_indicator > 0 {
            buf.skip_raw_bytes_chunked()?;
        }

        // dcb* fields as UB4
        buf.skip_ub4()?; // dcbflag
        buf.skip_ub4()?; // dcbmdbz
        buf.skip_ub4()?; // dcbmnpr
        buf.skip_ub4()?; // dcbmxpr

        // dcbqcky - read UB4 indicator first, then skip chunked bytes if > 0
        let dcbqcky_indicator = buf.read_ub4()?;
        if dcbqcky_indicator > 0 {
            buf.skip_raw_bytes_chunked()?;
        }

        // After dcbqcky, the next message (RowHeader) follows directly
        // No additional fields to skip here

        Ok(columns)
    }

    /// Commit the current transaction.
    ///
    /// Makes all changes in the current transaction permanent. After commit,
    /// a new transaction begins automatically.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use oracle_rs::{Connection, Value};
    /// # async fn example(conn: Connection) -> oracle_rs::Result<()> {
    /// conn.execute("INSERT INTO users (name) VALUES (:1)", &["Alice".into()]).await?;
    /// conn.execute("INSERT INTO users (name) VALUES (:1)", &["Bob".into()]).await?;
    /// conn.commit().await?; // Both inserts are now permanent
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit(&self) -> Result<()> {
        self.ensure_ready().await?;
        // Use SQL COMMIT statement instead of simple function
        // The simple function protocol triggers BREAK marker + connection close on Oracle Free 23ai
        self.execute("COMMIT", &[]).await?;
        Ok(())
    }

    /// Rollback the current transaction.
    ///
    /// Discards all changes made in the current transaction. After rollback,
    /// a new transaction begins automatically.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use oracle_rs::{Connection, Value};
    /// # async fn example(conn: Connection) -> oracle_rs::Result<()> {
    /// conn.execute("DELETE FROM users WHERE id = :1", &[1.into()]).await?;
    /// // Oops, wrong user!
    /// conn.rollback().await?; // Delete is undone
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rollback(&self) -> Result<()> {
        self.ensure_ready().await?;
        // Use SQL ROLLBACK statement instead of simple function
        // The simple function protocol triggers BREAK marker + connection close on Oracle Free 23ai
        self.execute("ROLLBACK", &[]).await?;
        Ok(())
    }

    /// Create a savepoint within the current transaction
    ///
    /// Savepoints allow partial rollback of a transaction. You can create multiple
    /// savepoints and rollback to any of them without affecting work done before
    /// that savepoint.
    ///
    /// # Arguments
    /// * `name` - The savepoint name (must be a valid Oracle identifier)
    ///
    /// # Example
    /// ```rust,ignore
    /// conn.execute("INSERT INTO t VALUES (1)", &[]).await?;
    /// conn.savepoint("sp1").await?;
    /// conn.execute("INSERT INTO t VALUES (2)", &[]).await?;
    /// conn.rollback_to_savepoint("sp1").await?; // Undoes the second insert
    /// conn.commit().await?; // Commits only the first insert
    /// ```
    pub async fn savepoint(&self, name: &str) -> Result<()> {
        self.ensure_ready().await?;
        self.execute(&format!("SAVEPOINT {}", name), &[]).await?;
        Ok(())
    }

    /// Rollback to a previously created savepoint
    ///
    /// This undoes all changes made after the savepoint was created, but keeps
    /// the transaction active. Changes made before the savepoint are preserved.
    ///
    /// # Arguments
    /// * `name` - The savepoint name to rollback to
    pub async fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        self.ensure_ready().await?;
        self.execute(&format!("ROLLBACK TO SAVEPOINT {}", name), &[])
            .await?;
        Ok(())
    }

    /// Ping the server to check if the connection is still alive.
    ///
    /// This executes a lightweight query (`SELECT 1 FROM DUAL`) to verify
    /// the connection is responsive. Useful for connection health checks
    /// in pooling scenarios.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use oracle_rs::Connection;
    /// # async fn example(conn: Connection) -> oracle_rs::Result<()> {
    /// if conn.ping().await.is_ok() {
    ///     println!("Connection is alive");
    /// } else {
    ///     println!("Connection is dead");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ping(&self) -> Result<()> {
        self.ensure_ready().await?;
        // Use SELECT FROM DUAL instead of simple function
        // The simple function protocol triggers BREAK marker + connection close on Oracle Free 23ai
        self.query("SELECT 1 FROM DUAL", &[]).await?;
        Ok(())
    }

    /// Clear the statement cache
    ///
    /// Non fa niente: la connessione non tiene cursori in cache (vedi la
    /// deviazione su `execute_query_with_params`). Resta per l'API pubblica.
    pub async fn clear_statement_cache(&self) {}

    /// Read data from a LOB (CLOB or BLOB)
    ///
    /// # Arguments
    /// * `locator` - The LOB locator obtained from a query result
    /// * `offset` - Starting position (1-based, in characters for CLOB, bytes for BLOB)
    /// * `amount` - Amount to read (0 for entire LOB)
    ///
    /// # Returns
    /// For CLOB: returns the text content as a String
    /// For BLOB: returns the binary content as bytes
    pub async fn read_lob(&self, locator: &LobLocator) -> Result<LobData> {
        self.ensure_ready().await?;

        // Read the entire LOB starting at offset 1
        let offset = 1u64;
        let amount = locator.size();

        self.read_lob_internal(locator, offset, amount).await
    }

    /// Read a portion of a LOB
    pub async fn read_lob_range(
        &self,
        locator: &LobLocator,
        offset: u64,
        amount: u64,
    ) -> Result<LobData> {
        self.ensure_ready().await?;
        self.read_lob_internal(locator, offset, amount).await
    }

    /// Read a CLOB and return as String
    ///
    /// This is a convenience method for reading CLOB data directly as a String.
    /// Returns an error if the LOB is not a CLOB.
    pub async fn read_clob(&self, locator: &LobLocator) -> Result<String> {
        if locator.is_blob() || locator.is_bfile() {
            return Err(Error::Protocol(
                "Cannot read BLOB/BFILE as string, use read_blob instead".to_string(),
            ));
        }

        let data = self.read_lob(locator).await?;
        match data {
            LobData::String(s) => Ok(s),
            LobData::Bytes(_) => Err(Error::Protocol(
                "Unexpected bytes from CLOB read".to_string(),
            )),
        }
    }

    /// Read a BLOB and return as bytes
    ///
    /// This is a convenience method for reading BLOB data directly as bytes.
    /// Returns an error if the LOB is a CLOB (use read_clob instead).
    pub async fn read_blob(&self, locator: &LobLocator) -> Result<bytes::Bytes> {
        if locator.is_clob() {
            return Err(Error::Protocol(
                "Cannot read CLOB as bytes, use read_clob instead".to_string(),
            ));
        }

        let data = self.read_lob(locator).await?;
        match data {
            LobData::Bytes(b) => Ok(b),
            LobData::String(_) => Err(Error::Protocol(
                "Unexpected string from BLOB read".to_string(),
            )),
        }
    }

    /// Read a LOB in chunks, calling a callback for each chunk
    ///
    /// This is useful for processing large LOBs without loading the entire
    /// content into memory. The callback receives each chunk as it's read.
    ///
    /// # Arguments
    /// * `locator` - The LOB locator
    /// * `chunk_size` - Size of each chunk to read (0 uses the LOB's natural chunk size)
    /// * `callback` - Async function called for each chunk
    ///
    /// # Example
    /// ```ignore
    /// let mut total_size = 0;
    /// conn.read_lob_chunked(&locator, 8192, |chunk| async move {
    ///     match chunk {
    ///         LobData::Bytes(b) => total_size += b.len(),
    ///         LobData::String(s) => total_size += s.len(),
    ///     }
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn read_lob_chunked<F, Fut>(
        &self,
        locator: &LobLocator,
        chunk_size: u64,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(LobData) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.ensure_ready().await?;

        let total_size = locator.size();
        if total_size == 0 {
            return Ok(());
        }

        // Use LOB's natural chunk size if not specified
        let chunk_size = if chunk_size == 0 {
            self.lob_chunk_size(locator).await?.max(8192) as u64
        } else {
            chunk_size
        };

        let mut offset = 1u64;
        while offset <= total_size {
            let remaining = total_size - offset + 1;
            let amount = std::cmp::min(remaining, chunk_size);

            let chunk = self.read_lob_internal(locator, offset, amount).await?;
            callback(chunk).await?;

            offset += amount;
        }

        Ok(())
    }

    /// Get the optimal chunk size for a LOB
    ///
    /// This returns the chunk size that Oracle recommends for efficient
    /// reading and writing to this LOB.
    pub async fn lob_chunk_size(&self, locator: &LobLocator) -> Result<u32> {
        self.ensure_ready().await?;

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Create LOB operation message for get chunk size
        let mut lob_msg = LobOpMessage::new_get_chunk_size(locator);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        // Build and send the request
        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), true, 0, |payload, caps| {
                // Plenora fork: checked, the amount is a u64 on the wire.
                self.parse_lob_amount_response(payload, locator, caps)
                    .and_then(|v| {
                        u32::try_from(v)
                            .map_err(|_| Error::Protocol("LOB chunk size beyond u32".to_string()))
                    })
            })
            .await
    }

    /// Internal LOB read implementation
    async fn read_lob_internal(
        &self,
        locator: &LobLocator,
        offset: u64,
        amount: u64,
    ) -> Result<LobData> {
        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Create LOB operation message for read
        let mut lob_msg = LobOpMessage::new_read(locator, offset, amount);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        // Build and send the request
        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), true, 0, |payload, caps| {
                self.parse_lob_read_response(payload, locator, caps)
            })
            .await
    }

    /// Parse LOB read response
    fn parse_lob_read_response(
        &self,
        payload: &[u8],
        locator: &LobLocator,
        caps: &Capabilities,
    ) -> Result<LobData> {
        use crate::buffer::ReadBuffer;

        let mut buf = ReadBuffer::from_slice(payload);

        // Skip data flags
        buf.skip(2)?;

        let mut lob_data: Option<Vec<u8>> = None;

        // Process messages until end of response
        while buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                // LobData message (14)
                x if x == MessageType::LobData as u8 => {
                    // Read LOB data with length
                    let data = buf.read_raw_bytes_chunked()?;
                    lob_data = Some(data);
                }

                // Parameter return (8) - contains updated locator and amount
                x if x == MessageType::Parameter as u8 => {
                    // Skip the updated locator (same length as original)
                    let locator_len = locator.locator_bytes().len();
                    buf.skip(locator_len)?;

                    // Read back the amount (ub8)
                    let _returned_amount = buf.read_ub8()?;
                }

                // Error/Status message (4) - code 0 means success
                x if x == MessageType::Error as u8 => {
                    // Parse error info - code 0 means success
                    {
                        let (code, msg, _) = self.parse_error_info(&mut buf, caps)?;
                        if code != 0 {
                            let message = msg.unwrap_or_else(|| "LOB error".to_string());
                            return Err(Error::OracleError { code, message });
                        }
                        // code 0 = success, continue processing
                    }
                }

                // End of response (29)
                x if x == MessageType::EndOfResponse as u8 => {
                    break;
                }

                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }

                // Skip other message types
                _ => {
                    if msg_type == MessageType::Status as u8 {
                        buf.read_ub4()?;
                        buf.read_ub2()?;
                    } else {
                        return Err(Error::Protocol("Unexpected LOB response message".into()));
                    }
                }
            }
        }

        // Convert to appropriate type based on LOB type
        match lob_data {
            Some(data) => {
                if locator.is_blob() || locator.is_bfile() {
                    Ok(LobData::Bytes(bytes::Bytes::from(data)))
                } else {
                    // CLOB - decode based on encoding
                    // Un CLOB si decodifica esatto o non si decodifica: la
                    // sostituzione con U+FFFD e il byte dispari scartato
                    // cambiavano il valore in silenzio (`61FF62` -> "a\u{FFFD}b").
                    // Un NCLOB (forma nazionale) si decodifica sul charset
                    // nazionale che il server descrive per la colonna:
                    // AL16UTF16, l'unico misurato; un altro si rifiuta invece
                    // di indovinare (`decode_column_text`). Un CLOB del
                    // database a lunghezza variabile viaggia in UTF-16 (il
                    // flag del locator).
                    let text = if locator._csfrm == 2 {
                        decode_column_text(&data, 2, locator.charset_id)?
                    } else if locator.uses_var_length_charset() {
                        decode_column_text(&data, 2, 2000)?
                    } else {
                        String::from_utf8(data.to_vec()).map_err(|_| {
                            Error::DataConversionError("CLOB Oracle non UTF-8".to_string())
                        })?
                    };
                    Ok(LobData::String(text))
                }
            }
            None => {
                // Empty LOB
                if locator.is_blob() || locator.is_bfile() {
                    Ok(LobData::Bytes(bytes::Bytes::new()))
                } else {
                    Ok(LobData::String(String::new()))
                }
            }
        }
    }

    /// Write data to a LOB
    ///
    /// # Arguments
    /// * `locator` - The LOB locator obtained from a query result
    /// * `offset` - Starting position (1-based, in characters for CLOB, bytes for BLOB)
    /// * `data` - Data to write (bytes for BLOB, UTF-8 encoded bytes for CLOB)
    pub async fn write_lob(&self, locator: &LobLocator, offset: u64, data: &[u8]) -> Result<()> {
        self.ensure_ready().await?;

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;
        let sdu_size = inner.sdu_size as usize;

        // Encode data for CLOB if necessary
        let encoded_data: Vec<u8>;
        let write_data = if locator.is_clob()
            && (locator.is_nchar() || locator.uses_var_length_charset())
        {
            // Convert UTF-8 to UTF-16 BE for NCLOB or var length charset
            let text = std::str::from_utf8(data)
                .map_err(|_| Error::DataConversionError("CLOB data is not valid UTF-8".into()))?;
            encoded_data = text.encode_utf16().flat_map(|c| c.to_be_bytes()).collect();
            &encoded_data[..]
        } else {
            data
        };

        // Create LOB operation message for write
        let mut lob_msg = LobOpMessage::new_write(locator, offset, write_data);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        // Build message content (without packet header or data flags)
        let message = lob_msg.build_message_only(&inner.capabilities)?;

        // Calculate if this fits in a single packet
        // Single packet max payload = SDU - header (8) - data flags (2)
        let max_single_packet_payload = sdu_size.saturating_sub(PACKET_HEADER_SIZE + 2);

        let is_multi_packet = message.len() > max_single_packet_payload;

        if is_multi_packet {
            // Needs multiple packets - use multi-packet sender
            inner.send_multi_packet(&message, 0).await?;
        } else {
            // Fits in one packet - use standard send
            let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
            inner.send(&request).await?;
        }

        inner
            .receive_lob_response(locator.locator_bytes().len(), false, 0, |payload, caps| {
                self.parse_lob_simple_response(payload, locator, false, caps)
            })
            .await
    }

    /// Write string data to a CLOB
    pub async fn write_clob(&self, locator: &LobLocator, offset: u64, text: &str) -> Result<()> {
        if locator.is_blob() || locator.is_bfile() {
            return Err(Error::Protocol(
                "Cannot write string to BLOB/BFILE, use write_blob instead".to_string(),
            ));
        }
        self.write_lob(locator, offset, text.as_bytes()).await
    }

    /// Write binary data to a BLOB
    pub async fn write_blob(&self, locator: &LobLocator, offset: u64, data: &[u8]) -> Result<()> {
        if locator.is_clob() {
            return Err(Error::Protocol(
                "Cannot write bytes to CLOB, use write_clob instead".to_string(),
            ));
        }
        self.write_lob(locator, offset, data).await
    }

    /// Get the length of a LOB
    ///
    /// For CLOB: returns length in characters
    /// For BLOB: returns length in bytes
    pub async fn lob_length(&self, locator: &LobLocator) -> Result<u64> {
        self.ensure_ready().await?;

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Create LOB operation message for get length
        let mut lob_msg = LobOpMessage::new_get_length(locator);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        // Build and send the request
        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), true, 0, |payload, caps| {
                self.parse_lob_amount_response(payload, locator, caps)
            })
            .await
    }

    /// Trim a LOB to a specified length
    ///
    /// # Arguments
    /// * `locator` - The LOB locator
    /// * `new_size` - The new size (in characters for CLOB, bytes for BLOB)
    pub async fn lob_trim(&self, locator: &LobLocator, new_size: u64) -> Result<()> {
        self.ensure_ready().await?;

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Create LOB operation message for trim
        let mut lob_msg = LobOpMessage::new_trim(locator, new_size);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        // Build and send the request
        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), true, 0, |payload, caps| {
                self.parse_lob_simple_response(payload, locator, true, caps)
            })
            .await
    }

    /// Create a temporary LOB on the server
    ///
    /// Creates a temporary LOB of the specified type that lives until the connection
    /// is closed or the LOB is explicitly freed.
    ///
    /// # Arguments
    /// * `oracle_type` - The LOB type to create (Clob or Blob)
    ///
    /// # Returns
    /// A `LobLocator` for the newly created temporary LOB
    ///
    /// # Example
    /// ```ignore
    /// use oracle_rs::OracleType;
    ///
    /// let locator = conn.create_temp_lob(OracleType::Clob).await?;
    /// conn.write_clob(&locator, 1, "Hello, World!").await?;
    /// // Now bind the locator to insert into a CLOB column
    /// ```
    pub async fn create_temp_lob(&self, oracle_type: OracleType) -> Result<LobLocator> {
        // Validate oracle_type is a LOB type
        match oracle_type {
            OracleType::Clob | OracleType::Blob => {}
            _ => {
                return Err(Error::Protocol(format!(
                    "create_temp_lob: invalid type {:?}, must be Clob or Blob",
                    oracle_type
                )));
            }
        }

        self.ensure_ready().await?;

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        // Create the CREATE_TEMP message
        let mut lob_msg = LobOpMessage::new_create_temp(oracle_type);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        // Build and send the request
        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(40, true, 2, |payload, caps| {
                self.parse_temp_lob_response(payload, caps, oracle_type)
            })
            .await
    }

    fn parse_temp_lob_response(
        &self,
        payload: &[u8],
        caps: &Capabilities,
        oracle_type: OracleType,
    ) -> Result<LobLocator> {
        // Parse response to extract the locator
        let mut buf = ReadBuffer::from_slice(payload);
        buf.skip(2)?; // Skip data flags

        let mut locator_bytes: Option<Vec<u8>> = None;

        while buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                // Parameter return (8) - contains the populated locator
                x if x == MessageType::Parameter as u8 => {
                    // Read the 40-byte locator (matches the 40 bytes sent in request)
                    let loc_data = buf.read_bytes_vec(40)?;
                    locator_bytes = Some(loc_data);
                    // Charset, raw flags, then amount requested by CREATE_TEMP.
                    buf.skip_ub2()?;
                    buf.skip(1)?;
                    buf.read_ub8()?;
                }

                // Error/Status message (4) - code 0 means success
                x if x == MessageType::Error as u8 => {
                    let (code, msg, _) = self.parse_error_info(&mut buf, caps)?;
                    if code != 0 {
                        let message = msg.unwrap_or_else(|| "CREATE_TEMP LOB error".to_string());
                        return Err(Error::OracleError { code, message });
                    }
                }

                // End of response (29)
                x if x == MessageType::EndOfResponse as u8 => {
                    break;
                }

                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                x if x == MessageType::Status as u8 => {
                    buf.read_ub4()?;
                    buf.read_ub2()?;
                }
                _ => return Err(Error::Protocol("Unexpected LOB response message".into())),
            }
        }

        // Create the LobLocator from the returned bytes
        let loc_bytes = locator_bytes.ok_or_else(|| {
            Error::Protocol("CREATE_TEMP LOB response did not contain locator".to_string())
        })?;

        // Create LobLocator with size 0, chunk_size 0 (will be fetched if needed)
        let locator = LobLocator::new(
            bytes::Bytes::from(loc_bytes),
            0, // size - unknown for new temp LOB
            0, // chunk_size - unknown, can be fetched later
            oracle_type,
            1, // csfrm - 1 for CLOB, 0 for BLOB (but we store it on the locator type)
        );

        Ok(locator)
    }

    // ==================== BFILE Operations ====================

    /// Check if a BFILE exists on the server
    ///
    /// Returns true if the file referenced by the BFILE locator exists on the server.
    pub async fn bfile_exists(&self, locator: &LobLocator) -> Result<bool> {
        self.ensure_ready().await?;

        if !locator.is_bfile() {
            return Err(Error::Protocol(
                "bfile_exists called on non-BFILE locator".to_string(),
            ));
        }

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        let mut lob_msg = LobOpMessage::new_file_exists(locator);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), false, 1, |payload, caps| {
                self.parse_lob_bool_response(payload, locator, caps)
            })
            .await
    }

    /// Open a BFILE for reading
    ///
    /// The BFILE must be opened before reading. After reading, close it with bfile_close.
    pub async fn bfile_open(&self, locator: &LobLocator) -> Result<()> {
        self.ensure_ready().await?;

        if !locator.is_bfile() {
            return Err(Error::Protocol(
                "bfile_open called on non-BFILE locator".to_string(),
            ));
        }

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        let mut lob_msg = LobOpMessage::new_file_open(locator);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), true, 0, |payload, caps| {
                self.parse_lob_simple_response(payload, locator, true, caps)
            })
            .await
    }

    /// Close a BFILE after reading
    pub async fn bfile_close(&self, locator: &LobLocator) -> Result<()> {
        self.ensure_ready().await?;

        if !locator.is_bfile() {
            return Err(Error::Protocol(
                "bfile_close called on non-BFILE locator".to_string(),
            ));
        }

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        let mut lob_msg = LobOpMessage::new_file_close(locator);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), false, 0, |payload, caps| {
                self.parse_lob_simple_response(payload, locator, false, caps)
            })
            .await
    }

    /// Check if a BFILE is currently open
    pub async fn bfile_is_open(&self, locator: &LobLocator) -> Result<bool> {
        self.ensure_ready().await?;

        if !locator.is_bfile() {
            return Err(Error::Protocol(
                "bfile_is_open called on non-BFILE locator".to_string(),
            ));
        }

        let mut inner = self.inner.lock().await;
        let large_sdu = inner.large_sdu;

        let mut lob_msg = LobOpMessage::new_file_is_open(locator);
        let seq_num = inner.next_sequence_number();
        lob_msg.set_sequence_number(seq_num);

        let request = lob_msg.build_request(&inner.capabilities, large_sdu)?;
        inner.send(&request).await?;

        inner
            .receive_lob_response(locator.locator_bytes().len(), false, 1, |payload, caps| {
                self.parse_lob_bool_response(payload, locator, caps)
            })
            .await
    }

    /// Read BFILE data
    ///
    /// This is a convenience method that opens the BFILE if needed, reads all content,
    /// and returns it as bytes. For large BFILEs, consider using read_lob_chunked.
    pub async fn read_bfile(&self, locator: &LobLocator) -> Result<bytes::Bytes> {
        if !locator.is_bfile() {
            return Err(Error::Protocol(
                "read_bfile called on non-BFILE locator".to_string(),
            ));
        }

        // Check if file is open, open if needed
        let should_close = if !self.bfile_is_open(locator).await? {
            self.bfile_open(locator).await?;
            true
        } else {
            false
        };

        // Read all data
        let result = self.read_blob(locator).await;

        // Close if we opened it
        if should_close {
            let _ = self.bfile_close(locator).await;
        }

        result
    }

    /// Parse a LOB operation response that returns a boolean (file_exists, is_open)
    fn parse_lob_bool_response(
        &self,
        payload: &[u8],
        locator: &LobLocator,
        caps: &Capabilities,
    ) -> Result<bool> {
        use crate::buffer::ReadBuffer;

        let mut buf = ReadBuffer::from_slice(payload);
        buf.skip(2)?; // Skip data flags

        let mut bool_result: bool = false;

        while buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                // Parameter return (8) - contains updated locator and bool flag
                x if x == MessageType::Parameter as u8 => {
                    let locator_len = locator.locator_bytes().len();
                    buf.skip(locator_len)?;
                    // Boolean flag is a single byte, > 0 means true
                    let flag = buf.read_u8()?;
                    bool_result = flag > 0;
                }

                // Error/Status message (4) - code 0 means success
                x if x == MessageType::Error as u8 => {
                    let (code, msg, _) = self.parse_error_info(&mut buf, caps)?;
                    if code != 0 {
                        let message = msg.unwrap_or_else(|| "LOB error".to_string());
                        return Err(Error::OracleError { code, message });
                    }
                }

                // End of response (29)
                x if x == MessageType::EndOfResponse as u8 => {
                    break;
                }

                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                x if x == MessageType::Status as u8 => {
                    buf.read_ub4()?;
                    buf.read_ub2()?;
                }
                _ => return Err(Error::Protocol("Unexpected LOB response message".into())),
            }
        }

        Ok(bool_result)
    }

    /// Parse a simple LOB operation response (write, trim)
    fn parse_lob_simple_response(
        &self,
        payload: &[u8],
        locator: &LobLocator,
        amount: bool,
        caps: &Capabilities,
    ) -> Result<()> {
        use crate::buffer::ReadBuffer;

        let mut buf = ReadBuffer::from_slice(payload);
        buf.skip(2)?; // Skip data flags

        while buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                // Parameter return (8) - contains updated locator and possibly amount
                x if x == MessageType::Parameter as u8 => {
                    let locator_len = locator.locator_bytes().len();
                    buf.skip(locator_len)?;
                    if amount {
                        buf.read_ub8()?;
                    }
                }

                // Error/Status message (4) - code 0 means success
                x if x == MessageType::Error as u8 => {
                    let (code, msg, _) = self.parse_error_info(&mut buf, caps)?;
                    if code != 0 {
                        let message = msg.unwrap_or_else(|| "LOB error".to_string());
                        return Err(Error::OracleError { code, message });
                    }
                }

                // End of response (29)
                x if x == MessageType::EndOfResponse as u8 => {
                    break;
                }

                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                x if x == MessageType::Status as u8 => {
                    buf.read_ub4()?;
                    buf.read_ub2()?;
                }
                _ => return Err(Error::Protocol("Unexpected LOB response message".into())),
            }
        }

        Ok(())
    }

    /// Parse a LOB operation response that returns an amount (get_length, get_chunk_size)
    fn parse_lob_amount_response(
        &self,
        payload: &[u8],
        locator: &LobLocator,
        caps: &Capabilities,
    ) -> Result<u64> {
        use crate::buffer::ReadBuffer;

        let mut buf = ReadBuffer::from_slice(payload);
        buf.skip(2)?; // Skip data flags

        let mut returned_amount: u64 = 0;

        while buf.remaining() > 0 {
            let msg_type = buf.read_u8()?;

            match msg_type {
                // Parameter return (8) - contains updated locator and amount
                x if x == MessageType::Parameter as u8 => {
                    let locator_len = locator.locator_bytes().len();
                    buf.skip(locator_len)?;
                    returned_amount = buf.read_ub8()?;
                }

                // Error/Status message (4) - code 0 means success
                x if x == MessageType::Error as u8 => {
                    let (code, msg, _) = self.parse_error_info(&mut buf, caps)?;
                    if code != 0 {
                        let message = msg.unwrap_or_else(|| "LOB error".to_string());
                        return Err(Error::OracleError { code, message });
                    }
                }

                // End of response (29)
                x if x == MessageType::EndOfResponse as u8 => {
                    break;
                }

                x if x == MessageType::ServerSidePiggyback as u8 => {
                    Self::parse_server_piggyback(&mut buf)?;
                }
                x if x == MessageType::Status as u8 => {
                    buf.read_ub4()?;
                    buf.read_ub2()?;
                }
                _ => return Err(Error::Protocol("Unexpected LOB response message".into())),
            }
        }

        Ok(returned_amount)
    }

    /// Close the connection.
    ///
    /// Sends a logoff message to the server and closes the underlying TCP
    /// connection. After calling close, the connection cannot be reused.
    ///
    /// If the connection is already closed, this method returns `Ok(())`
    /// without doing anything.
    ///
    /// # Note
    ///
    /// Any uncommitted transaction is rolled back by the server when the
    /// connection is closed.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use oracle_rs::{Config, Connection};
    /// # async fn example() -> oracle_rs::Result<()> {
    /// let config = Config::new("localhost", 1521, "FREEPDB1", "user", "password");
    /// let conn = Connection::connect_with_config(config).await?;
    ///
    /// // Do work...
    /// conn.commit().await?;
    ///
    /// // Explicitly close when done
    /// conn.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::Relaxed) {
            // Already closed
            return Ok(());
        }

        let mut inner = self.inner.lock().await;

        if inner.state == ConnectionState::Ready {
            // Send logoff
            let _ = self
                .send_simple_function_inner(&mut inner, FunctionCode::Logoff)
                .await;
        }

        inner.state = ConnectionState::Closed;
        inner.last_rows.clear();

        // Close the TCP stream
        if let Some(stream) = inner.stream.take() {
            drop(stream);
        }

        Ok(())
    }

    async fn send_simple_function_inner(
        &self,
        inner: &mut ConnectionInner,
        function_code: FunctionCode,
    ) -> Result<()> {
        let packet = inner.build_simple_function(function_code)?;
        inner.send(&packet).await?;
        let mut response = inner.receive_page().await?;
        if response[4] == PacketType::Marker as u8 {
            response = inner.handle_marker_reset().await?;
        }
        let result = self.parse_dml_response(&response[PACKET_HEADER_SIZE..], &inner.capabilities);
        inner.checked_parse_result(result).map(|_| ())
    }

    /// Ensure the connection is ready for operations
    async fn ensure_ready(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::ConnectionClosed);
        }

        let inner = self.inner.lock().await;
        if inner.stream.is_none() || inner.response_in_progress {
            return Err(Error::ConnectionClosed);
        }
        if inner.state != ConnectionState::Ready {
            return Err(Error::ConnectionNotReady);
        }

        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Note: Can't do async cleanup in Drop
        // Users should call close() explicitly
        self.closed.store(true, Ordering::Relaxed);
    }
}

// =============================================================================
// Helper functions for get_type()
// =============================================================================

/// Parse a type name into (schema, name) components
///
/// Handles formats like:
/// - "SCHEMA.TYPE_NAME" -> ("SCHEMA", "TYPE_NAME")
/// - "TYPE_NAME" -> (default_schema, "TYPE_NAME")
fn parse_type_name(type_name: &str, default_schema: &str) -> (String, String) {
    let parts: Vec<&str> = type_name.split('.').collect();
    match parts.len() {
        1 => (default_schema.to_uppercase(), parts[0].to_uppercase()),
        2 => (parts[0].to_uppercase(), parts[1].to_uppercase()),
        _ => {
            // Multiple dots - take first as schema, rest as name
            (parts[0].to_uppercase(), parts[1..].join(".").to_uppercase())
        }
    }
}

/// Convert an Oracle type name from data dictionary to OracleType enum
fn oracle_type_from_name(type_name: &str) -> crate::constants::OracleType {
    use crate::constants::OracleType;

    match type_name.to_uppercase().as_str() {
        "NUMBER" => OracleType::Number,
        "INTEGER" | "INT" | "SMALLINT" => OracleType::Number,
        "FLOAT" | "REAL" | "DOUBLE PRECISION" => OracleType::BinaryDouble,
        "BINARY_FLOAT" => OracleType::BinaryFloat,
        "BINARY_DOUBLE" => OracleType::BinaryDouble,
        "VARCHAR2" | "VARCHAR" | "NVARCHAR2" => OracleType::Varchar,
        "CHAR" | "NCHAR" => OracleType::Char,
        "DATE" => OracleType::Date,
        "TIMESTAMP" => OracleType::Timestamp,
        "TIMESTAMP WITH TIME ZONE" => OracleType::TimestampTz,
        "TIMESTAMP WITH LOCAL TIME ZONE" => OracleType::TimestampLtz,
        "RAW" => OracleType::Raw,
        "BLOB" => OracleType::Blob,
        "CLOB" | "NCLOB" => OracleType::Clob,
        "BOOLEAN" | "PL/SQL BOOLEAN" => OracleType::Boolean,
        "ROWID" | "UROWID" => OracleType::Rowid,
        "XMLTYPE" => OracleType::Varchar, // Treat XMLType as string for now
        _ => OracleType::Varchar,         // Default to VARCHAR for unknown types
    }
}

#[cfg(test)]
#[path = "describe_fidelity_tests.rs"]
mod describe_fidelity_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;

    // Risposta TTC con ROWID compresso, conteggio cumulativo e campi 20c+.
    fn completion_packet(code: u32, count: u64) -> Vec<u8> {
        completion_packet_version(code, count, 16)
    }

    fn completion_packet_version(code: u32, count: u64, version: u8) -> Vec<u8> {
        let mut buf = WriteBuffer::new();
        buf.write_u16_be(0).unwrap();
        buf.write_u8(MessageType::Error as u8).unwrap();
        for _ in 0..6 {
            buf.write_u8(0).unwrap();
        }
        buf.write_ub2(42).unwrap();
        for _ in 0..21 {
            buf.write_u8(0).unwrap();
        }
        buf.write_ub4(code).unwrap();
        buf.write_ub8(count).unwrap();
        if version >= 16 {
            buf.write_ub4(0).unwrap();
            buf.write_ub4(0).unwrap();
        }
        if code != 0 {
            buf.write_string_with_length(Some("errore sintetico"))
                .unwrap();
        }
        buf.as_slice().to_vec()
    }

    include!("connection_exchange_tests.rs");
    include!("connection_side_message_tests.rs");
    include!("connection_cursor_tests.rs");
    include!("connection_read_tests.rs");

    fn parser_connection() -> Connection {
        Connection {
            inner: Arc::new(Mutex::new(ConnectionInner::new())),
            config: Config::default(),
            closed: AtomicBool::new(true),
            id: 0,
            reported_cursor: AtomicU16::new(0),
        }
    }

    #[test]
    fn completion_respects_negotiated_ttc_version() {
        let conn = parser_connection();
        for version in [15, 16, 19] {
            let mut caps = Capabilities::new();
            caps.ttc_field_version = version;
            for code in [0, 1403, 1476] {
                let packet = completion_packet_version(code, 102, version);
                let mut buf = ReadBuffer::from_slice(&packet[3..]);
                let (actual, message, cursor, count) =
                    Connection::parse_error_info_with_rowcount(&mut buf, &caps).unwrap();
                assert_eq!((actual, cursor, count), (code, 42, 102));
                assert_eq!(buf.remaining(), 0);
                assert_eq!(
                    message.as_deref(),
                    (code != 0).then_some("errore sintetico")
                );
                if code == 1476 {
                    assert!(matches!(
                        conn.parse_error_response(&packet, &caps),
                        Err(Error::OracleError { code: 1476, .. })
                    ));
                } else {
                    assert_eq!(
                        conn.parse_fetch_response(&packet, &[], &caps, None)
                            .unwrap()
                            .has_more_rows,
                        code == 0
                    );
                }
            }
        }
    }

    // Nessun server Oracle: il peer locale emette solo frame sintetici controllati.
    async fn synthetic_connection(large_sdu: bool) -> (Connection, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, server) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let mut inner = ConnectionInner::new();
        inner.stream = Some(OracleStream::Plain(client.unwrap()));
        inner.state = ConnectionState::Ready;
        inner.large_sdu = large_sdu;
        inner.sdu_size = u32::from(u16::MAX);
        (
            Connection {
                inner: Arc::new(Mutex::new(inner)),
                config: Config::default(),
                closed: AtomicBool::new(false),
                id: 0,
                reported_cursor: AtomicU16::new(0),
            },
            server.unwrap().0,
        )
    }

    async fn send_fragments(peer: &mut TcpStream, payload: &[u8], split: usize, large: bool) {
        for (data, flags) in [
            (&payload[2..split], 0),
            (
                &payload[split..],
                crate::constants::data_flags::END_OF_RESPONSE,
            ),
        ] {
            let packet = ConnectionInner::build_data_chunk(data, flags, large).unwrap();
            peer.write_all(&packet).await.unwrap();
        }
    }

    #[tokio::test]
    async fn legacy_terminals_without_flags_keep_peer_open_and_next_response_intact() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            for large in [false, true] {
                for version in [15, 16, 19] {
                    for code in [0, 1403, 1476] {
                        let payload = completion_packet_version(code, 3, version);
                        for split in 3..payload.len() {
                            let (conn, mut peer) = synthetic_connection(large).await;
                            let mut inner = conn.inner.lock().await;
                            inner.capabilities.ttc_field_version = version;
                            inner.capabilities.supports_end_of_response = false;
                            for data in [&payload[2..split], &payload[split..], &[9, 0, 0]] {
                                peer.write_all(
                                    &ConnectionInner::build_data_chunk(data, 0, large).unwrap(),
                                )
                                .await
                                .unwrap();
                            }
                            let response = inner.receive_page().await.unwrap();
                            assert_eq!(&response[8..], payload);
                            assert_eq!(&inner.receive_page().await.unwrap()[10..], &[9, 0, 0]);
                            // Peer remains open until both reads finish.
                            conn.closed.store(true, Ordering::Release);
                        }
                    }
                }
            }
        })
        .await
        .expect("legacy response must not wait for peer EOF");
    }

    #[tokio::test]
    async fn legacy_dml_and_marker_reset_complete_without_flags() {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            for reset in [false, true] {
                let (conn, mut peer) = synthetic_connection(false).await;
                {
                    let mut inner = conn.inner.lock().await;
                    inner.capabilities.supports_end_of_response = false;
                    inner.capabilities.ttc_field_version = 16;
                }
                if reset {
                    for kind in [1, 2] {
                        peer.write_all(
                            &encode_packet(PacketType::Marker, &[1, 0, kind], false).unwrap(),
                        )
                        .await
                        .unwrap();
                    }
                }
                let payload = completion_packet(if reset { 1476 } else { 0 }, 7);
                for byte in &payload[2..] {
                    peer.write_all(&ConnectionInner::build_data_chunk(&[*byte], 0, false).unwrap())
                        .await
                        .unwrap();
                }
                let result = conn.execute("UPDATE synthetic SET n = 1", &[]).await;
                if reset {
                    assert!(matches!(result, Err(Error::OracleError { code: 1476, .. })));
                } else {
                    assert_eq!(result.unwrap().rows_affected, 7);
                }
                conn.closed.store(true, Ordering::Release);
            }
        })
        .await
        .expect("legacy DML/reset must not wait for EOF");
    }

    #[test]
    fn framing_preserves_sparse_row_header_and_partial_terminal() {
        let mut caps = Capabilities::new();
        caps.ttc_field_version = 16;
        caps.supports_end_of_response = false;
        let columns = [ColumnInfo::new("RAW", crate::constants::OracleType::Raw)];
        let mut framing = PageFraming::new(&columns);
        // ROW_HEADER with one bitmap byte (all duplicates), then empty ROW_DATA.
        let mut data = vec![6, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 7];
        assert!(!framing
            .advance(&bytes::Bytes::copy_from_slice(&data), &caps)
            .unwrap());
        data.extend_from_slice(&completion_packet(1403, 1)[2..]);
        for end in 13..data.len() {
            assert!(!framing
                .advance(&bytes::Bytes::copy_from_slice(&data[..end]), &caps)
                .unwrap());
        }
        assert!(framing
            .advance(&bytes::Bytes::copy_from_slice(&data), &caps)
            .unwrap());
    }

    #[tokio::test]
    async fn negotiated_eor_requires_complete_terminal_and_explicit_end_message() {
        let (conn, mut peer) = synthetic_connection(true).await;
        let mut inner = conn.inner.lock().await;
        inner.capabilities.ttc_field_version = 16;
        inner.capabilities.supports_end_of_response = true;
        let payload = completion_packet(0, 1);
        // The final TTC byte is in its own frame, with no TNS flags.
        for data in [&payload[2..3], &payload[3..], &[29]] {
            peer.write_all(&ConnectionInner::build_data_chunk(data, 0, true).unwrap())
                .await
                .unwrap();
        }
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(2), inner.receive_page())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&response[10..response.len() - 1], &payload[2..]);
        assert_eq!(response.last(), Some(&29));
        conn.closed.store(true, Ordering::Release);
    }

    #[tokio::test]
    async fn legacy_rows_and_lob_locators_do_not_delimit_responses() {
        for lob in [false, true] {
            let (conn, mut peer) = synthetic_connection(false).await;
            let mut inner = conn.inner.lock().await;
            inner.capabilities.ttc_field_version = 16;
            inner.capabilities.supports_end_of_response = false;
            let mut payload = if lob {
                vec![8, 29, 4, 9]
            } else {
                vec![7, 3, 29, 4, 9]
            };
            payload.extend_from_slice(&completion_packet(1403, 1)[2..]);
            for byte in &payload {
                peer.write_all(&ConnectionInner::build_data_chunk(&[*byte], 0, false).unwrap())
                    .await
                    .unwrap();
            }
            let columns = [ColumnInfo::new("RAW", crate::constants::OracleType::Raw)];
            let mut framing = PageFraming::new(&columns);
            if lob {
                framing.lob = Some((3, false));
            }
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                inner.receive_response_checked(framing),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(&response[10..], payload);
            conn.closed.store(true, Ordering::Release);
        }
    }

    #[test]
    fn array_counts_are_fields_not_terminal_bytes() {
        let mut caps = Capabilities::new();
        caps.ttc_field_version = 16;
        caps.supports_end_of_response = false;
        let mut framing = PageFraming::new(&[]);
        framing.row_counts = true;
        let mut buf = WriteBuffer::new();
        buf.write_u8(MessageType::Parameter as u8).unwrap();
        for _ in 0..4 {
            buf.write_ub2(0).unwrap();
        }
        buf.write_ub4(3).unwrap();
        for count in [29, 4, 9] {
            buf.write_ub8(count).unwrap();
        }
        let mut data = buf.as_slice().to_vec();
        data.extend_from_slice(&completion_packet(0, 42)[2..]);
        for end in 1..data.len() {
            assert!(!framing
                .advance(&bytes::Bytes::copy_from_slice(&data[..end]), &caps)
                .unwrap());
        }
        assert!(framing
            .advance(&bytes::Bytes::copy_from_slice(&data), &caps)
            .unwrap());
    }

    #[tokio::test]
    async fn response_limits_reject_before_body_and_discard_channel() {
        for large in [false, true] {
            for cumulative in [false, true] {
                let (conn, mut peer) = synthetic_connection(large).await;
                conn.set_response_byte_limit(25).await;
                let mut inner = conn.inner.lock().await;
                inner.sdu_size = if cumulative { 25 } else { 12 };
                if cumulative {
                    // Two legal frames exhaust the budget without a terminal.
                    for _ in 0..2 {
                        peer.write_all(&ConnectionInner::build_data_chunk(&[], 0, large).unwrap())
                            .await
                            .unwrap();
                    }
                }
                // Header only: rejection must occur without waiting for a body.
                let packet = ConnectionInner::build_data_chunk(&[0; 3], 0, large).unwrap();
                peer.write_all(&packet[..8]).await.unwrap();
                assert!(tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    inner.receive_page()
                )
                .await
                .unwrap()
                .is_err());
                assert!(inner.stream.is_none());
                assert!(inner.send(&[0]).await.is_err());
                assert!(inner.receive_page().await.is_err());
                conn.closed.store(true, Ordering::Release);
            }
        }
    }

    #[tokio::test]
    async fn cancelled_partial_response_cannot_reuse_channel() {
        let (conn, mut peer) = synthetic_connection(false).await;
        peer.write_all(&ConnectionInner::build_data_chunk(&[4], 0, false).unwrap())
            .await
            .unwrap();
        let mut inner = conn.inner.lock().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), inner.receive_page())
                .await
                .is_err()
        );
        assert!(inner.stream.is_none());
        assert!(inner.send(&[0]).await.is_err());
        conn.closed.store(true, Ordering::Release);
    }

    #[tokio::test]
    async fn page_boundaries_never_depend_on_cell_bytes() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (conn, mut peer) = synthetic_connection(false).await;
            // 29 e 4 in pacchetti isolati non sono necessariamente messaggi TTC.
            for (data, flags) in [
                (&[7, 3][..], 0),
                (&[29][..], 0),
                (&[4][..], 0),
                (
                    &[9, 9, 0, 0][..],
                    crate::constants::data_flags::END_OF_RESPONSE,
                ),
            ] {
                peer.write_all(&ConnectionInner::build_data_chunk(data, flags, false).unwrap())
                    .await
                    .unwrap();
            }
            let packet = conn
                .inner
                .lock()
                .await
                .receive_page_with_columns(&[ColumnInfo::new(
                    "RAW",
                    crate::constants::OracleType::Raw,
                )])
                .await
                .unwrap();
            assert_eq!(&packet[8..], &[0, 0, 7, 3, 29, 4, 9, 9, 0, 0]);
            conn.closed.store(true, Ordering::Release);
        })
        .await
        .expect("risposta sintetica entro la scadenza");
    }

    #[tokio::test]
    async fn interrupted_partial_page_is_an_error_not_exhaustion() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (conn, mut peer) = synthetic_connection(false).await;
            peer.write_all(&ConnectionInner::build_data_chunk(&[7, 2, 193, 2], 0, false).unwrap())
                .await
                .unwrap();
            peer.shutdown().await.unwrap();
            assert!(conn
                .fetch_more(
                    42,
                    &[ColumnInfo::new("N", crate::constants::OracleType::Number)],
                    10
                )
                .await
                .is_err());
            conn.closed.store(true, Ordering::Release);
        })
        .await
        .expect("risposta sintetica entro la scadenza");
    }

    #[tokio::test]
    async fn pages_reassemble_at_every_byte_boundary() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for large in [false, true] {
                let (conn, mut peer) = synthetic_connection(large).await;
                let caps = Capabilities::new();
                let columns = [ColumnInfo::new("N", crate::constants::OracleType::Number)];
                let mut payload = vec![0, 0, 7, 2, 193, 2];
                payload.extend_from_slice(&completion_packet(1403, 1)[2..]);
                for split in 3..payload.len() {
                    send_fragments(&mut peer, &payload, split, large).await;
                    let response = conn
                        .inner
                        .lock()
                        .await
                        .receive_page_with_columns(&columns)
                        .await
                        .unwrap();
                    assert_eq!(&response[8..], payload);
                    for result in [
                        conn.parse_query_response_with_columns(&response[8..], &caps, &columns),
                        conn.parse_fetch_response(&response[8..], &columns, &caps, None),
                    ] {
                        let result = result.unwrap();
                        assert_eq!(result.rows.len(), 1);
                        assert!(!result.has_more_rows);
                    }
                }
                conn.closed.store(true, Ordering::Release);
            }
        })
        .await
        .expect("risposta sintetica entro la scadenza");
    }

    #[tokio::test]
    async fn public_fetch_paths_consume_all_packets_and_keep_next_response() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for large in [false, true] {
                for route in 0..4 {
                    let (conn, mut peer) = synthetic_connection(large).await;
                    let columns = vec![ColumnInfo::new("N", crate::constants::OracleType::Number)];
                    let mut payload = vec![0, 0];
                    // Una pagina supera l'SDU e contiene byte uguali ai tipi messaggio.
                    if route != 0 {
                        for _ in 0..2500 {
                            payload.extend_from_slice(&[7, 2, 193, 2]);
                        }
                    }
                    payload.extend_from_slice(&completion_packet(1403, 2500)[2..]);
                    send_fragments(&mut peer, &payload, payload.len() - 3, large).await;
                    let next = completion_packet(1403, 0);
                    send_fragments(&mut peer, &next, 4, large).await;
                    let result = match route {
                        0 => conn.query("SELECT 1 FROM DUAL", &[]).await,
                        1 => conn.fetch_more(42, &columns, 2500).await,
                        2 => {
                            conn.fetch_cursor_with_size(
                                &crate::types::RefCursor::new(42, columns.clone()),
                                2500,
                            )
                            .await
                        }
                        _ => {
                            conn.fetch_implicit_result_with_size(
                                &ImplicitResult::new(42, columns.clone(), vec![]),
                                2500,
                            )
                            .await
                        }
                    }
                    .unwrap();
                    assert_eq!(result.rows.len(), if route == 0 { 0 } else { 2500 });
                    assert!(!result.has_more_rows);
                    assert!(
                        !conn
                            .fetch_more(42, &columns, 1)
                            .await
                            .unwrap()
                            .has_more_rows
                    );
                    conn.closed.store(true, Ordering::Release);
                }
            }
        })
        .await
        .expect("risposta sintetica entro la scadenza");
    }

    #[tokio::test]
    async fn partial_rows_then_error_or_marker_leave_connection_aligned() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for version in [15, 16, 19] {
                for marker in [false, true] {
                    let (conn, mut peer) = synthetic_connection(false).await;
                    conn.inner.lock().await.capabilities.ttc_field_version = version;
                    let columns = [ColumnInfo::new("N", crate::constants::OracleType::Number)];
                    let partial =
                        ConnectionInner::build_data_chunk(&[7, 2, 193, 2], 0, false).unwrap();
                    peer.write_all(&partial).await.unwrap();
                    if marker {
                        for kind in [1, 2] {
                            peer.write_all(
                                &encode_packet(PacketType::Marker, &[1, 0, kind], false).unwrap(),
                            )
                            .await
                            .unwrap();
                        }
                    }
                    let error = completion_packet_version(1476, 1, version);
                    send_fragments(&mut peer, &error, 4, false).await;
                    let next = completion_packet_version(1403, 0, version);
                    send_fragments(&mut peer, &next, 4, false).await;
                    assert!(matches!(
                        conn.fetch_more(42, &columns, 10).await,
                        Err(Error::OracleError { code: 1476, .. })
                    ));
                    assert!(
                        !conn
                            .fetch_more(42, &columns, 10)
                            .await
                            .unwrap()
                            .has_more_rows
                    );
                    conn.closed.store(true, Ordering::Release);
                }
            }
        })
        .await
        .expect("risposta sintetica entro la scadenza");
    }

    #[test]
    fn query_and_fetch_require_explicit_exhaustion() {
        let conn = parser_connection();
        let caps = Capabilities::new();
        // Un conteggio positivo non significa altre righe; zero non significa EOF.
        for (code, count, more) in [(0, 100, true), (0, 0, true), (1403, 102, false)] {
            let packet = completion_packet(code, count);
            let query = conn.parse_query_response(&packet, &caps).unwrap();
            assert_eq!(query.has_more_rows, more);
            assert_eq!(query.cursor_id, 42);
            let fetch = conn
                .parse_fetch_response(&packet, &[], &caps, None)
                .unwrap();
            assert_eq!(fetch.has_more_rows, more);
        }
        let packet = completion_packet(1476, 102);
        for result in [
            conn.parse_query_response(&packet, &caps),
            conn.parse_fetch_response(&packet, &[], &caps, None),
            conn.parse_error_response(&packet, &caps),
        ] {
            assert!(matches!(result, Err(Error::OracleError { code: 1476, .. })));
        }
    }

    #[test]
    fn prefetch_and_two_fetches_deliver_102_rows_before_error() {
        let conn = parser_connection();
        let caps = Capabilities::new();
        let columns = [ColumnInfo::new("N", crate::constants::OracleType::Number)];
        let mut delivered = 0;
        for (size, cumulative) in [(100, 100), (1, 101), (1, 102)] {
            let mut packet = vec![0, 0];
            for _ in 0..size {
                // RowData, lunghezza NUMBER, valore Oracle NUMBER 1.
                packet.extend_from_slice(&[7, 2, 193, 2]);
            }
            packet.extend_from_slice(&completion_packet(0, cumulative)[2..]);
            let result = if delivered == 0 {
                conn.parse_query_response_with_columns(&packet, &caps, &columns)
            } else {
                conn.parse_fetch_response(&packet, &columns, &caps, None)
            }
            .unwrap();
            assert_eq!(result.rows.len(), size);
            assert!(result.has_more_rows);
            delivered += result.rows.len();
        }
        assert_eq!(delivered, 102);
        assert!(matches!(
            conn.parse_fetch_response(&completion_packet(1476, 102), &columns, &caps, None),
            Err(Error::OracleError { code: 1476, .. })
        ));
    }

    #[test]
    fn incomplete_or_unknown_responses_are_not_eof() {
        let conn = parser_connection();
        let caps = Capabilities::new();
        for packet in [
            vec![0, 0, 255],
            vec![0, 0, 29],
            vec![0, 0, 9, 0, 0],
            completion_packet(0, 100)[..15].to_vec(),
        ] {
            assert!(conn.parse_query_response(&packet, &caps).is_err());
            assert!(conn
                .parse_fetch_response(&packet, &[], &caps, None)
                .is_err());
        }
    }

    #[test]
    fn data_chunk_packet_bytes() {
        for large_sdu in [false, true] {
            for flags in [0, 0x1234] {
                for size in [0, 1, 8192 - 10] {
                    let payload = vec![0x5a; size];
                    let packet =
                        ConnectionInner::build_data_chunk(&payload, flags, large_sdu).unwrap();
                    crate::packet::assert_packet_bytes(&packet, PacketType::Data, large_sdu);
                    assert_eq!(&packet[8..10], &flags.to_be_bytes());
                    assert_eq!(&packet[10..], &payload);
                }
            }
        }
    }

    #[test]
    fn simple_function_packet_bytes() {
        for large_sdu in [false, true] {
            for version in [17, 18] {
                let mut inner = ConnectionInner::new();
                inner.large_sdu = large_sdu;
                inner.capabilities.ttc_field_version = version;
                for function in [
                    FunctionCode::Commit,
                    FunctionCode::Rollback,
                    FunctionCode::Logoff,
                ] {
                    let packet = inner.build_simple_function(function).unwrap();
                    crate::packet::assert_packet_bytes(&packet, PacketType::Data, large_sdu);
                    assert_eq!(&packet[8..12], &[0, 0, 3, function as u8]);
                    assert_eq!(packet.len(), if version >= 18 { 14 } else { 13 });
                }
            }
        }
    }

    #[test]
    fn test_marker_packet_length_encoding() {
        let mut inner = ConnectionInner::new();
        for (large_sdu, length) in [(false, [0, 11, 0, 0]), (true, [0, 0, 0, 11])] {
            inner.large_sdu = large_sdu;
            for marker_type in [1, 2, 3] {
                let packet = inner.build_marker(marker_type).unwrap();
                assert_eq!(&packet[..4], &length);
                assert_eq!(&packet[4..], &[12, 0, 0, 0, 1, 0, marker_type]);
            }
        }
    }

    #[test]
    fn test_query_options_default() {
        let opts = QueryOptions::default();
        assert_eq!(opts.prefetch_rows, 100);
        assert_eq!(opts.array_size, 100);
        assert!(!opts.auto_commit);
    }

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult::empty();
        assert!(result.is_empty());
        assert_eq!(result.column_count(), 0);
        assert_eq!(result.row_count(), 0);
        assert!(result.first().is_none());
    }

    #[test]
    fn test_query_result_with_rows() {
        let columns = vec![ColumnInfo::new("ID", crate::constants::OracleType::Number)];
        let rows = vec![Row::new(vec![Value::Integer(1)])];

        let result = QueryResult {
            columns,
            rows,
            rows_affected: 0,
            has_more_rows: false,
            cursor_id: 1,
        };

        assert!(!result.is_empty());
        assert_eq!(result.column_count(), 1);
        assert_eq!(result.row_count(), 1);
        assert!(result.first().is_some());
        assert!(result.column_by_name("ID").is_some());
        assert!(result.column_by_name("id").is_some()); // Case insensitive
        assert_eq!(result.column_index("ID"), Some(0));
    }

    #[test]
    fn test_server_info_default() {
        let info = ServerInfo::default();
        assert!(info.version.is_empty());
        assert_eq!(info.session_id, 0);
    }

    #[test]
    fn test_connection_state_transitions() {
        assert_eq!(ConnectionState::Disconnected, ConnectionState::Disconnected);
        assert_ne!(ConnectionState::Connected, ConnectionState::Ready);
    }

    #[test]
    fn test_query_result_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        let result = QueryResult {
            columns: vec![],
            rows,
            rows_affected: 0,
            has_more_rows: false,
            cursor_id: 0,
        };

        let collected: Vec<_> = result.iter().collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_query_result_into_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        let result = QueryResult {
            columns: vec![],
            rows,
            rows_affected: 0,
            has_more_rows: false,
            cursor_id: 0,
        };

        let collected: Vec<Row> = result.into_iter().collect();
        assert_eq!(collected.len(), 2);
    }
}

/// Il testo di una colonna `VARCHAR2`/`CHAR`/`LONG` e delle forme nazionali,
/// esatto: UTF-16BE per AL16UTF16 (`charset_id` 2000, il charset nazionale di
/// default), UTF-8 per la forma del database (il server la converte nel
/// charset del client, AL32UTF8). La forma nazionale con un charset diverso
/// da AL16UTF16 (UTF8, 871) si rifiuta: non e misurata, e decodificarla come
/// UTF-8 o come UTF-16 sarebbe indovinare (terzo giro della #135).
pub(crate) fn decode_column_text(bytes: &[u8], csfrm: u8, charset_id: u16) -> Result<String> {
    let utf16 = match (csfrm, charset_id) {
        (_, 2000) => true,
        (2, _) => {
            return Err(Error::DataConversionError(
                "charset nazionale Oracle diverso da AL16UTF16: non qualificato".to_string(),
            ))
        }
        _ => false,
    };
    if utf16 {
        let (pairs, remainder) = bytes.as_chunks::<2>();
        if !remainder.is_empty() {
            return Err(Error::DataConversionError(
                "testo Oracle nazionale con byte dispari".to_string(),
            ));
        }
        return char::decode_utf16(pairs.iter().map(|pair| u16::from_be_bytes(*pair)))
            .collect::<std::result::Result<String, _>>()
            .map_err(|_| Error::DataConversionError("testo Oracle UTF-16 non valido".to_string()));
    }
    String::from_utf8(bytes.to_vec())
        .map_err(|_| Error::DataConversionError("testo Oracle UTF-8 non valido".to_string()))
}

#[cfg(test)]
#[path = "text_decoding_tests.rs"]
mod text_decoding_tests;
