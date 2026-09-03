//! Fingerprint canonici condivisi fra engine e cataloghi provider.

use serde::Serialize;
use sha2::{Digest, Sha256};

/// Serializza un valore in JSON e restituisce il digest SHA-256 esadecimale.
///
/// La funzione non decide cosa entri nel documento canonico: quella scelta
/// resta al proprietario del contratto. Centralizza soltanto serializzazione,
/// algoritmo e codifica, che non devono divergere fra provider.
///
/// # Errors
///
/// Restituisce l'errore di serializzazione senza trasformarlo in un messaggio
/// pubblico; il chiamante deve mapparlo senza copiarne il testo.
pub fn canonical_json_sha256<T: Serialize + ?Sized>(
    value: &T,
) -> std::result::Result<String, serde_json::Error> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let canonical = serde_json::to_vec(value)?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}
