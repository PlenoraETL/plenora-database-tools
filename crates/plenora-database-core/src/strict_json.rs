//! Lettura JSON che rifiuta le chiavi ripetute.
//!
//! `serde_json` deserializza una chiave ripetuta in una mappa o in un `Value`
//! tenendo l'ultima occorrenza: il documento `{"a": 1, "a": 2}` diventa
//! `{"a": 2}` senza che nessuno lo sappia. Per una struct derivata l'errore
//! c'e, per una `BTreeMap` (i parametri) o un `Value` (il payload runtime) no.
//! Un documento ambiguo e un documento che due lettori possono leggere in due
//! modi, e un consumatore che tiene la prima occorrenza vedrebbe un'altra
//! richiesta. Qui si rifiuta prima di interpretarlo.

use serde::de::{self, DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::collections::BTreeSet;
use std::fmt;

/// Come `serde_json::from_slice`, ma una chiave ripetuta a qualunque
/// profondita e un errore con la sua posizione, non l'ultima occorrenza.
///
/// # Errors
///
/// L'errore di `serde_json`: sintassi, chiave ripetuta o forma diversa da `T`.
/// Il testo porta riga e colonna, mai la chiave o il valore.
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    reject_repeated_keys(bytes)?;
    serde_json::from_slice(bytes)
}

/// Verifica soltanto che nessun oggetto ripeta una chiave.
///
/// # Errors
///
/// Sintassi JSON non valida o una chiave ripetuta.
pub fn reject_repeated_keys(bytes: &[u8]) -> Result<(), serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    UniqueKeys.deserialize(&mut deserializer)?;
    deserializer.end()
}

struct UniqueKeys;

impl<'de> DeserializeSeed<'de> for UniqueKeys {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for UniqueKeys {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("un documento JSON")
    }

    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<(), A::Error> {
        while items.next_element_seed(Self)?.is_some() {}
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<(), A::Error> {
        let mut seen = BTreeSet::new();
        while let Some(key) = entries.next_key::<String>()? {
            if !seen.insert(key) {
                return Err(de::Error::custom("chiave JSON ripetuta"));
            }
            entries.next_value_seed(Self)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "strict_json_tests.rs"]
mod tests;
