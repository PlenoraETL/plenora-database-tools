//! La regola dei parametri bindati, una per ogni provider.
//!
//! **Regola.** Ogni variante di [`ParameterValue`](crate::provider::ParameterValue),
//! su ogni provider e su ogni percorso che la binda (statement del chiamante,
//! AST portable, filtro della lettura, parameter array), o ha un binding
//! esatto — il server riceve proprio quel valore, lo conserva e lo ritrova
//! con `col = ?` — provato da un test live di andata e ritorno, oppure e
//! rifiutata con un errore esplicito prima dell'invio. Nessuna conversione
//! implicita del server che cambi il valore: un testo che il server tronca
//! al tipo del segnaposto, un double scritto in cifre decimali, byte spediti
//! come la loro rappresentazione esadecimale.
//!
//! Le prove stanno in `plenora_database_testkit::parameter_fidelity` e nei
//! test `live_parameter_values_bind_exactly_or_are_refused` di ogni crate
//! provider; qui stanno le parti comuni della regola, cosi che i provider non
//! ne scrivano ciascuno una propria.
//!
//! # Limiti dichiarati
//!
//! - **Precisione temporale.** `Timestamp` e `TimestampTz` portano al piu il
//!   microsecondo, la precisione con cui ogni decoder del repository li
//!   rilegge. Ambito: tutti i provider, anche dove il server conserverebbe di
//!   piu (SQL Server `datetime2(7)`, Oracle `TIMESTAMP(9)`, Db2
//!   `TIMESTAMP(12)`). Hazard: una frazione oltre il microsecondo e rifiutata
//!   ([`reject_sub_microsecond`]) invece di essere arrotondata dal server.
//!   Rientro: un tipo temporale a nanosecondi nel contratto, con decoder che
//!   lo rileggono.
//! - **Assegnazione a una colonna piu stretta.** Il binding garantisce il
//!   valore che il server riceve, non quello che una colonna di tipo diverso
//!   conserva: un `F64` in un `REAL`, un decimale con piu cifre della scala
//!   di un `DECIMAL(p, s)`, un testo in un `CHAR(n)` che lo completa con
//!   spazi seguono le regole di assegnazione del server. Ambito: lo statement
//!   del chiamante sui provider che non descrivono il segnaposto (`MySQL`,
//!   `MariaDB`, SQL Server, Oracle). PostgreSQL e Db2 descrivono il tipo del
//!   segnaposto e rifiutano un valore che non vi entra esatto. Hazard: il
//!   valore conservato puo differire da quello bindato. Rientro: la
//!   descrizione dei segnaposto (`sp_describe_undeclared_parameters`, il
//!   describe di `MySQL` e Oracle) anche su quei provider.
//! - **Decimali con esponente.** `Decimal` si accetta soltanto in notazione
//!   posizionale (`[+-]cifre[.cifre]`, [`DecimalLiteral`]) dove il binding ne
//!   ricava precisione e scala; Oracle, che lo binda come `NUMBER` nativo,
//!   ammette anche l'esponente. Rientro: una normalizzazione dell'esponente
//!   provata contro ogni server.

use crate::plan::ProviderKind;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};

fn mapping(provider: ProviderKind, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        Some(provider),
        message,
    )
}

/// Vero se il testo temporale porta una frazione di secondo oltre il
/// microsecondo diversa da zero.
///
/// Il controllo e sul testo, non sul valore gia letto da un parser: un
/// parser a nanosecondi scarterebbe in silenzio la decima cifra, e il
/// rifiuto non la vedrebbe. La frazione e l'ultima sequenza di cifre dopo un
/// punto, cosi vale per ISO-8601, RFC 3339 e la forma Db2
/// `AAAA-MM-GG-hh.mm.ss.ffffff`.
#[must_use]
pub fn has_sub_microsecond(value: &str) -> bool {
    value.rfind('.').is_some_and(|dot| {
        value[dot + 1..]
            .bytes()
            .take_while(u8::is_ascii_digit)
            .skip(6)
            .any(|digit| digit != b'0')
    })
}

/// Rifiuta un testo temporale con una frazione di secondo oltre il
/// microsecondo diversa da zero ([`has_sub_microsecond`]).
///
/// # Errors
///
/// `DataMapping` in fase `Prepare`. Il messaggio non riporta il valore.
pub fn reject_sub_microsecond(provider: ProviderKind, value: &str) -> Result<()> {
    if has_sub_microsecond(value) {
        return Err(mapping(
            provider,
            "parametro temporale oltre il microsecondo: la precisione bindabile e il microsecondo",
        ));
    }
    Ok(())
}

/// Rifiuta un testo che contiene il carattere NUL.
///
/// # Errors
///
/// `DataMapping` in fase `Prepare`.
pub fn reject_nul(provider: ProviderKind, value: &str) -> Result<()> {
    if value.contains('\0') {
        return Err(mapping(provider, "parametro testuale con carattere NUL"));
    }
    Ok(())
}

/// Un UUID nella sola forma canonica `8-4-4-4-12` di cifre esadecimali.
///
/// # Errors
///
/// `DataMapping` in fase `Prepare` per ogni altra forma.
pub fn canonical_uuid(provider: ProviderKind, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let dashes = [8, 13, 18, 23];
    let canonical = bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| {
            if dashes.contains(&index) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    if canonical {
        Ok(())
    } else {
        Err(mapping(
            provider,
            "parametro UUID non in forma canonica 8-4-4-4-12",
        ))
    }
}

/// Il valore `f32` che rappresenta esattamente `value`, se esiste.
///
/// `NaN` e gli infiniti si conservano come tali; ogni altro valore deve
/// tornare identico dopo il passaggio a 32 bit.
#[must_use]
#[allow(clippy::cast_possible_truncation)] // la riduzione e verificata subito dopo
pub fn exact_f32(value: f64) -> Option<f32> {
    let narrow = value as f32;
    (value.is_nan() || f64::from(narrow).to_bits() == value.to_bits()).then_some(narrow)
}

/// Un decimale in notazione posizionale, scomposto per il binding.
///
/// La forma accettata e `[+-]cifre[.cifre]`, con almeno una cifra; niente
/// esponente, spazi o separatori di migliaia.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalLiteral {
    /// Il segno; `-0` resta negativo nel testo ma vale zero.
    pub negative: bool,
    /// Le cifre intere senza zeri iniziali (vuoto per zero).
    pub integer: String,
    /// Le cifre frazionarie come scritte, zeri finali compresi.
    pub fraction: String,
}

impl DecimalLiteral {
    /// # Errors
    ///
    /// `DataMapping` in fase `Prepare` per un testo fuori dalla forma.
    pub fn parse(provider: ProviderKind, value: &str) -> Result<Self> {
        let (negative, unsigned) = match value.as_bytes().first() {
            Some(b'-') => (true, &value[1..]),
            Some(b'+') => (false, &value[1..]),
            _ => (false, value),
        };
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
        if (integer.is_empty() && fraction.is_empty()) || !digits(integer) || !digits(fraction) {
            return Err(mapping(
                provider,
                "parametro decimal fuori dalla forma [+-]cifre[.cifre]",
            ));
        }
        Ok(Self {
            negative,
            integer: integer.trim_start_matches('0').to_owned(),
            fraction: fraction.to_owned(),
        })
    }

    /// Le cifre frazionarie significative, senza gli zeri finali.
    #[must_use]
    pub fn significant_fraction(&self) -> &str {
        self.fraction.trim_end_matches('0')
    }

    /// Vero se il valore entra esatto in `DECIMAL(precision, scale)`.
    #[must_use]
    pub fn fits(&self, precision: usize, scale: usize) -> bool {
        self.significant_fraction().len() <= scale
            && self.integer.len() <= precision.saturating_sub(scale)
    }

    /// Mantissa intera e scala, per i binding numerici nativi.
    ///
    /// # Errors
    ///
    /// `DataMapping` se le cifre non entrano in un `i128` o la scala in
    /// `u8`.
    pub fn mantissa_and_scale(&self, provider: ProviderKind) -> Result<(i128, u8)> {
        let fraction = self.significant_fraction();
        let scale = u8::try_from(fraction.len())
            .map_err(|_| mapping(provider, "parametro decimal con scala non rappresentabile"))?;
        let digits = format!("{}{fraction}", self.integer);
        let magnitude = if digits.is_empty() {
            0
        } else {
            digits
                .parse::<i128>()
                .map_err(|_| mapping(provider, "parametro decimal oltre 38 cifre"))?
        };
        Ok((if self.negative { -magnitude } else { magnitude }, scale))
    }
}

#[cfg(test)]
#[path = "parameter_fidelity_tests.rs"]
mod tests;
