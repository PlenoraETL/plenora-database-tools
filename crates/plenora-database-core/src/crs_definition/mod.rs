//! Gli identificatori di primo livello di una definizione CRS e le autorita
//! dei cataloghi locali (Arrow Geometry Semantics 1.0, GEO-005 e GEO-020).
//!
//! Il confine non risolve CRS: legge soltanto gli identificatori che la
//! definizione dichiara alla propria radice e ne verifica la buona forma nel
//! formato dichiarato. Un identificatore annidato (l'`AUTHORITY` del `GEOGCS`
//! dentro un `PROJCS`) non soddisfa e non contraddice mai l'identita del
//! campo: lo leggerebbe GEO-015, che vale per chi calcola con le coordinate, e
//! database-tools le scrive senza calcolarci.
//!
//! La lettura e la stessa dell'oracolo dei contratti (`tools/arrow_data.py`,
//! `parse_wkt`, `load_projjson`, `definition_contradicts`) e di IO-tools
//! (`plenora-io-model::crs_definizione`), da cui questo modulo e portato: due
//! componenti della stessa pipeline non devono dare due verdetti sulla stessa
//! definizione.

mod projjson;

/// Il formato dichiarato da `plenora.geometry.crs_definition_format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefinitionFormat {
    Wkt,
    Wkt2,
    Projjson,
}

impl DefinitionFormat {
    /// Il formato dal valore della chiave; `None` fuori dall'insieme chiuso.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "wkt" => Some(Self::Wkt),
            "wkt2" => Some(Self::Wkt2),
            "projjson" => Some(Self::Projjson),
            _ => None,
        }
    }
}

/// La profondita di annidamento oltre la quale una definizione WKT e
/// malformata, la stessa del contratto per la geometria.
const MAX_DEPTH: usize = 32;

/// Il tetto sui byte di una definizione che entra nel lettore.
///
/// Una definizione reale e di qualche KiB; il lettore WKT tiene un token per
/// elemento. Oltre il tetto il chiamante riporta `resource_limit` prima di
/// leggere ([`within_size_limit`]); il lettore, per difesa, la tratta come
/// malformata.
pub const MAX_DEFINITION_BYTES: usize = 1 << 20;

/// La definizione sta nel tetto [`MAX_DEFINITION_BYTES`].
#[must_use]
pub const fn within_size_limit(text: &str) -> bool {
    text.len() <= MAX_DEFINITION_BYTES
}

/// Una definizione che non e ben formata nel formato che dichiara.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Malformed;

/// Un'autorita o un codice letti da una definizione.
///
/// Un PROJJSON puo portare una stringa con un surrogato isolato (`"\ud800"`):
/// e JSON ben formato per l'oracolo, e nessuna stringa Rust lo rappresenta.
/// Un testo cosi non e uguale a nessun `crs_id`, che e sempre Unicode valido.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefinitionText {
    Unicode(String),
    LoneSurrogates,
}

impl DefinitionText {
    fn equals(&self, other: &str) -> bool {
        matches!(self, Self::Unicode(text) if text == other)
    }

    /// La stessa autorita, ASCII case-insensitive come vuole GEO-005: una
    /// lettera non ASCII resta se stessa (`EPſG` non e `EPSG`). L'oracolo al
    /// pin usa le maiuscole Unicode e qui diverge; la correzione dell'oracolo
    /// e plenora-contracts #53.
    fn same_authority(&self, other: &str) -> bool {
        matches!(self, Self::Unicode(text) if text.eq_ignore_ascii_case(other))
    }
}

/// Uno spazio per l'oracolo: `str.isspace()` di Python.
///
/// E anche il `\s` delle sue espressioni regolari su `str` (la grammatica di
/// `crs_id`, lo spoglio del WKT): il `White_Space` di Unicode piu i quattro
/// separatori U+001C..U+001F. Elenco chiuso, quello di Python 3.11
/// (Unicode 14).
#[must_use]
pub const fn python_space(c: char) -> bool {
    matches!(
        c,
        '\u{9}'..='\u{d}'
            | '\u{1c}'..='\u{20}'
            | '\u{85}'
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
    )
}

const WKT1_ROOTS: [&str; 7] = [
    "GEOGCS",
    "PROJCS",
    "GEOCCS",
    "VERT_CS",
    "COMPD_CS",
    "LOCAL_CS",
    "FITTED_CS",
];
const WKT2_ROOTS: [&str; 15] = [
    "GEOGCRS",
    "GEOGRAPHICCRS",
    "GEODCRS",
    "GEODETICCRS",
    "PROJCRS",
    "PROJECTEDCRS",
    "VERTCRS",
    "VERTICALCRS",
    "COMPOUNDCRS",
    "ENGCRS",
    "ENGINEERINGCRS",
    "BOUNDCRS",
    "DERIVEDPROJCRS",
    "TIMECRS",
    "PARAMETRICCRS",
];

/// Le autorita di un catalogo locale (GEO-020): i loro codici identificano un
/// CRS soltanto dentro l'installazione che li ha scritti. Lista chiusa: un
/// nome si aggiunge solo con una versione del contratto.
pub const LOCAL_AUTHORITIES: [&str; 7] = [
    "POSTGIS", "MYSQL", "MARIADB", "MSSQL", "ORACLE", "DB2", "SQLITE",
];

/// `crs_id` nomina l'autorita di un catalogo locale (ASCII case-insensitive).
#[must_use]
pub fn is_local_authority(crs_id: &str) -> bool {
    crs_id.split_once(':').is_some_and(|(authority, _)| {
        LOCAL_AUTHORITIES
            .iter()
            .any(|local| authority.eq_ignore_ascii_case(local))
    })
}

/// La grammatica di `plenora.geometry.crs_id` (GEO-016):
/// `[A-Za-z][A-Za-z0-9_.-]*:[^\s:]+`, sul valore intero.
#[must_use]
pub fn is_well_formed_crs_id(text: &str) -> bool {
    let Some((authority, code)) = text.split_once(':') else {
        return false;
    };
    let mut letters = authority.chars();
    letters.next().is_some_and(|c| c.is_ascii_alphabetic())
        && letters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        && !code.is_empty()
        && !code.chars().any(|c| python_space(c) || c == ':')
}

/// La definizione contraddice `crs_id`: porta almeno un identificatore di
/// primo livello dell'autorita di `crs_id` e nessuno uguale a `crs_id`.
///
/// # Errors
///
/// [`Malformed`] se la definizione non e ben formata nel formato dichiarato,
/// anche quando `crs_id` manca: la buona forma non dipende dall'identita.
pub fn contradicts(
    crs_id: Option<&str>,
    text: &str,
    format: DefinitionFormat,
) -> Result<bool, Malformed> {
    let pairs = identifiers(text, format)?;
    let Some((authority, code)) = crs_id.and_then(|id| id.split_once(':')) else {
        return Ok(false);
    };
    let same: Vec<_> = pairs
        .iter()
        .filter(|(a, _)| a.same_authority(authority))
        .collect();
    Ok(!same.is_empty() && !same.iter().any(|(_, c)| c.equals(code)))
}

/// Le coppie (autorita, codice) di primo livello della definizione.
///
/// # Errors
///
/// [`Malformed`] se la definizione non e ben formata nel formato dichiarato.
pub fn identifiers(
    text: &str,
    format: DefinitionFormat,
) -> Result<Vec<(DefinitionText, DefinitionText)>, Malformed> {
    let wkt = |pairs: Vec<(String, String)>| {
        pairs
            .into_iter()
            .map(|(a, c)| (DefinitionText::Unicode(a), DefinitionText::Unicode(c)))
            .collect()
    };
    match format {
        DefinitionFormat::Projjson => projjson::identifiers(text),
        DefinitionFormat::Wkt => Ok(wkt(wkt_identifiers(
            &wkt_root(text, &WKT1_ROOTS)?,
            "AUTHORITY",
        )?)),
        DefinitionFormat::Wkt2 => Ok(wkt(wkt_identifiers(&wkt_root(text, &WKT2_ROOTS)?, "ID")?)),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Token<'a> {
    Open(u8),
    Close(u8),
    Comma,
    Text(String),
    Number(&'a str),
    Word(&'a str),
}

fn wkt_tokens(text: &str) -> Result<Vec<Token<'_>>, Malformed> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = text[i..].chars().next().ok_or(Malformed)?;
        // Lo spazio dell'oracolo (`\s*`), non `White_Space`.
        if python_space(c) {
            i += c.len_utf8();
            continue;
        }
        match bytes[i] {
            b'[' | b'(' => {
                tokens.push(Token::Open(bytes[i]));
                i += 1;
            }
            b']' | b')' => {
                tokens.push(Token::Close(bytes[i]));
                i += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            b'"' => {
                let mut value = String::new();
                let mut j = i + 1;
                loop {
                    match bytes.get(j) {
                        None => return Err(Malformed),
                        Some(b'"') if bytes.get(j + 1) == Some(&b'"') => {
                            value.push('"');
                            j += 2;
                        }
                        Some(b'"') => break,
                        Some(_) => {
                            let character = text[j..].chars().next().ok_or(Malformed)?;
                            value.push(character);
                            j += character.len_utf8();
                        }
                    }
                }
                tokens.push(Token::Text(value));
                i = j + 1;
            }
            b'+' | b'-' | b'.' | b'0'..=b'9' => {
                let end = number_end(bytes, i).ok_or(Malformed)?;
                tokens.push(Token::Number(&text[i..end]));
                i = end;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                let mut j = i + 1;
                while bytes
                    .get(j)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                {
                    j += 1;
                }
                tokens.push(Token::Word(&text[i..j]));
                i = j;
            }
            _ => return Err(Malformed),
        }
    }
    Ok(tokens)
}

/// La fine di un numero `[+-]?(cifre[.cifre*]|.cifre)([eE][+-]?cifre)?` che
/// comincia in `start`, o `None` se li non comincia un numero. Solo cifre
/// ASCII (GEO-005).
fn number_end(bytes: &[u8], start: usize) -> Option<usize> {
    let digits = |from: usize| {
        bytes.get(from..).map_or(0, |rest| {
            rest.iter().take_while(|b| b.is_ascii_digit()).count()
        })
    };
    let mut i = start;
    if matches!(bytes.get(i), Some(b'+' | b'-')) {
        i += 1;
    }
    let integer = digits(i);
    i += integer;
    if integer > 0 {
        if bytes.get(i) == Some(&b'.') {
            i += 1;
            i += digits(i);
        }
    } else {
        if bytes.get(i) != Some(&b'.') {
            return None;
        }
        let decimals = digits(i + 1);
        if decimals == 0 {
            return None;
        }
        i += 1 + decimals;
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(bytes.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        let exponent = digits(j);
        if exponent > 0 {
            i = j + exponent;
        }
    }
    Some(i)
}

#[derive(Debug)]
struct Node {
    keyword: String,
    items: Vec<Item>,
}

#[derive(Debug)]
enum Item {
    Node(Node),
    Text(String),
    Number(String),
    Word,
}

fn wkt_root(text: &str, admitted: &[&str]) -> Result<Node, Malformed> {
    if !within_size_limit(text) {
        return Err(Malformed);
    }
    let tokens = wkt_tokens(text)?;
    let mut position = 0;
    let root = node(&tokens, &mut position, 0)?;
    if position != tokens.len()
        || !admitted
            .iter()
            .any(|r| root.keyword.eq_ignore_ascii_case(r))
    {
        return Err(Malformed);
    }
    Ok(root)
}

fn node(tokens: &[Token<'_>], position: &mut usize, depth: usize) -> Result<Node, Malformed> {
    if depth > MAX_DEPTH {
        return Err(Malformed);
    }
    let (Some(Token::Word(keyword)), Some(Token::Open(opening))) =
        (tokens.get(*position), tokens.get(*position + 1))
    else {
        return Err(Malformed);
    };
    // Una parentesi si chiude con il genere che l'ha aperta (GEO-005).
    let closing = if *opening == b'[' { b']' } else { b')' };
    *position += 2;
    let mut items = Vec::new();
    loop {
        match (tokens.get(*position), tokens.get(*position + 1)) {
            (Some(Token::Word(_)), Some(Token::Open(_))) => {
                items.push(Item::Node(node(tokens, position, depth + 1)?));
            }
            (Some(Token::Text(value)), _) => {
                items.push(Item::Text(value.clone()));
                *position += 1;
            }
            (Some(Token::Number(value)), _) => {
                items.push(Item::Number((*value).to_owned()));
                *position += 1;
            }
            (Some(Token::Word(_)), _) => {
                items.push(Item::Word);
                *position += 1;
            }
            _ => return Err(Malformed),
        }
        match tokens.get(*position) {
            Some(Token::Close(c)) if *c == closing => {
                *position += 1;
                return Ok(Node {
                    keyword: (*keyword).to_owned(),
                    items,
                });
            }
            Some(Token::Comma) => {
                *position += 1;
            }
            _ => return Err(Malformed),
        }
    }
}

fn wkt_identifiers(root: &Node, keyword: &str) -> Result<Vec<(String, String)>, Malformed> {
    let mut pairs = Vec::new();
    for item in &root.items {
        let Item::Node(child) = item else { continue };
        if !child.keyword.eq_ignore_ascii_case(keyword) {
            continue;
        }
        let Some(Item::Text(authority)) = child.items.first() else {
            return Err(Malformed);
        };
        let code = match child.items.get(1) {
            Some(Item::Text(code)) => code.clone(),
            Some(Item::Number(number)) => numeric_code(number)?,
            _ => return Err(Malformed),
        };
        pairs.push((authority.clone(), code));
    }
    Ok(pairs)
}

/// Un codice numerico scritto come intero decimale (`+0043` vale `43`).
fn numeric_code(number: &str) -> Result<String, Malformed> {
    let digits = number.strip_prefix('+').unwrap_or(number);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Malformed);
    }
    let trimmed = digits.trim_start_matches('0');
    Ok(if trimmed.is_empty() {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    })
}

#[cfg(test)]
mod tests;
