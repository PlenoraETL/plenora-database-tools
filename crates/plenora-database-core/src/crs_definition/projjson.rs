//! Gli identificatori di primo livello di un PROJJSON, letti come li legge
//! l'oracolo dei contratti (`arrow_data.py`, `load_projjson` e
//! `projjson_identifiers`).
//!
//! # Perche' non `serde_json`
//!
//! L'oracolo legge il documento con `json.loads` di Python, interi a precisione
//! arbitraria e decimali esatti (`parse_float=Decimal`). `serde_json` senza le
//! feature `arbitrary_precision` e `unbounded_depth` -- che il workspace non
//! abilita, e una dipendenza non cambia feature per questo -- rifiuta come
//! malformati documenti che per l'oracolo sono ben formati: un numero oltre
//! `f64` (`1e400`), una stringa con un surrogato isolato (`"\ud800"`), piu' di
//! 128 livelli di annidamento; e legge `-0` o un intero oltre `u64` come
//! decimali, che come codice di un identificatore sono malformati. Il bordo non
//! deve rifiutare cio' che il contratto accetta (GEO-005, REJ-001), quindi qui
//! c'e' un lettore piccolo e dedicato: verifica la sintassi del documento intero
//! e materializza soltanto `id` e `ids` della radice.
//!
//! # Che cosa replica
//!
//! La grammatica di `json.loads` con i ganci dell'oracolo: spazi solo
//! `[ \t\n\r]`; nessun carattere di controllo crudo nelle stringhe; gli escape
//! di JSON, con le coppie di surrogati unite e i surrogati isolati conservati;
//! numeri `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?` di qualunque
//! grandezza; `NaN` e `Infinity` rifiutati (`parse_constant`); una chiave
//! ripetuta in qualunque oggetto rifiutata (`object_pairs_hook`); la radice
//! deve essere un oggetto.
//!
//! # Dove l'oracolo non da' un verdetto
//!
//! L'oracolo, in `CPython`, si ferma con un'eccezione che non e' `Malformed`
//! -- cioe' senza un verdetto del contratto -- in tre casi, tutti limiti
//! dell'interprete e non della grammatica:
//!
//! - oltre circa mille livelli di annidamento (`RecursionError`);
//! - su un intero di piu' di 4300 cifre (`ValueError`);
//! - su un decimale con un esponente oltre i limiti di `decimal`, per esempio
//!   `9e9999999999999999999999999999` (`decimal.InvalidOperation`).
//!
//! Questo lettore e' iterativo e legge i numeri come testo, senza valutarli:
//! nei tre casi da' il verdetto della grammatica. Il documento e' ben formato;
//! un intero lungo e' un codice valido, un decimale (con qualunque esponente)
//! come codice e' malformato e altrove e' un valore qualunque.
//!
//! # Provenienza
//!
//! Portato da `plenora-io-model::crs_definizione::projjson` di IO-tools, con
//! le misure di memoria fatte li': i due componenti devono dare lo stesso
//! verdetto sulla stessa definizione, e un secondo lettore scritto da capo
//! sarebbe un secondo posto in cui divergere dall'oracolo.
//!
//! # Memoria
//!
//! Lineare nei byte, con un fattore piccolo e dichiarato:
//!
//! - un byte per ogni contenitore aperto (il suo genere);
//! - per ogni chiave di un oggetto aperto un'impronta e una posizione (16
//!   byte); le chiavi di un oggetto si liberano alla sua chiusura;
//! - della radice si materializzano soltanto `id` e `ids`, e di ogni
//!   identificatore soltanto `authority` e `code`, decodificati in UTF-8.
//!
//! Il doppione si cerca alla chiusura dell'oggetto, ordinando le impronte
//! (`SipHash` con chiavi casuali, quindi non costruibili da chi scrive il
//! documento) e confrontando per intero le sole chiavi con la stessa impronta:
//! il verdetto e' esatto, l'impronta decide solo quanto lavoro fare.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

use super::{DefinitionText, Malformed};

/// Le coppie (autorita', codice) di `id` o `ids` della radice.
pub(super) fn identifiers(testo: &str) -> Result<Vec<(DefinitionText, DefinitionText)>, Malformed> {
    if testo.len() > super::MAX_DEFINITION_BYTES {
        return Err(Malformed);
    }
    valida(testo.as_bytes())?;
    let mut cursore = Cursore {
        byte: testo.as_bytes(),
        pos: 0,
    };
    radice(&mut cursore)
}

struct Cursore<'a> {
    byte: &'a [u8],
    pos: usize,
}

impl Cursore<'_> {
    fn spazi(&mut self) {
        while matches!(self.byte.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn guarda(&self) -> Option<u8> {
        self.byte.get(self.pos).copied()
    }

    fn atteso(&mut self, byte: u8) -> Result<(), Malformed> {
        if self.guarda() == Some(byte) {
            self.pos += 1;
            Ok(())
        } else {
            Err(Malformed)
        }
    }

    /// Il prossimo punto di codice di una stringa JSON gia' aperta, o `None`
    /// dopo averne consumato le virgolette di chiusura.
    fn punto(&mut self) -> Result<Option<u32>, Malformed> {
        let byte = self.guarda().ok_or(Malformed)?;
        match byte {
            b'"' => {
                self.pos += 1;
                Ok(None)
            }
            b'\\' => {
                self.pos += 1;
                self.escape().map(Some)
            }
            // `strict=True`: nessun carattere di controllo crudo.
            0x00..=0x1f => Err(Malformed),
            _ => {
                // Il documento e' una `&str`: dal byte iniziale si sa quanti
                // byte ha il carattere, e si decodifica solo lui (decodificare
                // tutto il resto a ogni carattere sarebbe quadratico).
                let lunghezza = match byte {
                    0x00..=0x7f => 1,
                    0xc0..=0xdf => 2,
                    0xe0..=0xef => 3,
                    _ => 4,
                };
                let pezzo = self
                    .byte
                    .get(self.pos..self.pos + lunghezza)
                    .ok_or(Malformed)?;
                let carattere = std::str::from_utf8(pezzo)
                    .map_err(|_| Malformed)?
                    .chars()
                    .next()
                    .ok_or(Malformed)?;
                self.pos += carattere.len_utf8();
                Ok(Some(u32::from(carattere)))
            }
        }
    }

    /// Una stringa JSON, dalle virgolette d'apertura, decodificata: in UTF-8,
    /// o [`DefinitionText::LoneSurrogates`] se porta un surrogato isolato.
    fn testo(&mut self) -> Result<DefinitionText, Malformed> {
        self.atteso(b'"')?;
        let mut testo = String::new();
        let mut surrogati = false;
        while let Some(punto) = self.punto()? {
            match char::from_u32(punto) {
                Some(carattere) if !surrogati => testo.push(carattere),
                Some(_) => {}
                None => surrogati = true,
            }
        }
        Ok(if surrogati {
            DefinitionText::LoneSurrogates
        } else {
            DefinitionText::Unicode(testo)
        })
    }

    /// Consuma una stringa JSON, dalle virgolette d'apertura, senza tenerla.
    fn salta_stringa(&mut self) -> Result<(), Malformed> {
        self.atteso(b'"')?;
        while self.punto()?.is_some() {}
        Ok(())
    }

    /// L'impronta dei punti di codice di una stringa JSON, dalle virgolette
    /// d'apertura: due grafie dello stesso testo (`"a"` e `"a"`) hanno la
    /// stessa impronta.
    fn impronta(&mut self, stato: &RandomState) -> Result<u64, Malformed> {
        self.atteso(b'"')?;
        let mut hasher = stato.build_hasher();
        while let Some(punto) = self.punto()? {
            hasher.write_u32(punto);
        }
        Ok(hasher.finish())
    }

    /// Una stringa JSON, dalle virgolette d'apertura, confrontata con i nomi
    /// dati: l'indice di quello uguale, se c'e'. Della stringa si tengono al
    /// piu' i primi caratteri che servono al confronto.
    fn nome_fra(&mut self, nomi: &[&str]) -> Result<Option<usize>, Malformed> {
        const TENUTI: usize = 16;
        self.atteso(b'"')?;
        let mut inizio = String::new();
        let mut troppo = false;
        while let Some(punto) = self.punto()? {
            match char::from_u32(punto) {
                Some(carattere) if !troppo && inizio.len() < TENUTI => inizio.push(carattere),
                _ => troppo = true,
            }
        }
        Ok(if troppo {
            None
        } else {
            nomi.iter().position(|nome| *nome == inizio)
        })
    }

    fn escape(&mut self) -> Result<u32, Malformed> {
        let byte = self.guarda().ok_or(Malformed)?;
        self.pos += 1;
        let semplice = match byte {
            b'"' => 0x22,
            b'\\' => 0x5c,
            b'/' => 0x2f,
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => 0x0a,
            b'r' => 0x0d,
            b't' => 0x09,
            b'u' => {
                let alto = self.esadecimale()?;
                // Una coppia di surrogati si unisce; un alto non seguito da un
                // basso resta isolato, e cio' che segue si rilegge da capo.
                if (0xd800..=0xdbff).contains(&alto)
                    && self.byte.get(self.pos..self.pos + 2) == Some(b"\\u")
                {
                    let ritorno = self.pos;
                    self.pos += 2;
                    let basso = self.esadecimale()?;
                    if (0xdc00..=0xdfff).contains(&basso) {
                        return Ok(0x10000 + ((alto - 0xd800) << 10) + (basso - 0xdc00));
                    }
                    self.pos = ritorno;
                }
                return Ok(alto);
            }
            _ => return Err(Malformed),
        };
        Ok(semplice)
    }

    fn esadecimale(&mut self) -> Result<u32, Malformed> {
        let quattro = self.byte.get(self.pos..self.pos + 4).ok_or(Malformed)?;
        let mut valore = 0_u32;
        for byte in quattro {
            let cifra = char::from(*byte).to_digit(16).ok_or(Malformed)?;
            valore = valore * 16 + cifra;
        }
        self.pos += 4;
        Ok(valore)
    }

    /// Un numero JSON: `Some(inizio..fine)` della sua parte intera se e'
    /// intero, `None` se decimale. Come la regex di `json.scanner`, una
    /// frazione o un esponente incompleti non si consumano: il byte che resta
    /// lo rifiuta chi viene dopo. Il numero non si valuta: un esponente
    /// qualunque e' soltanto testo.
    fn numero(&mut self) -> Result<Option<std::ops::Range<usize>>, Malformed> {
        let inizio = self.pos;
        if self.guarda() == Some(b'-') {
            self.pos += 1;
        }
        match self.guarda() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => self.cifre(),
            _ => return Err(Malformed),
        }
        let fine_intero = self.pos;
        if self.guarda() == Some(b'.')
            && self.byte.get(self.pos + 1).is_some_and(u8::is_ascii_digit)
        {
            self.pos += 1;
            self.cifre();
        }
        if matches!(self.guarda(), Some(b'e' | b'E')) {
            let mut dopo = self.pos + 1;
            if matches!(self.byte.get(dopo), Some(b'+' | b'-')) {
                dopo += 1;
            }
            if self.byte.get(dopo).is_some_and(u8::is_ascii_digit) {
                self.pos = dopo;
                self.cifre();
            }
        }
        // Una frazione o un esponente consumati fanno un decimale.
        Ok((self.pos == fine_intero).then_some(inizio..fine_intero))
    }

    fn cifre(&mut self) {
        while self.guarda().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
    }

    fn letterale(&mut self, parola: &[u8]) -> Result<(), Malformed> {
        if self.byte.get(self.pos..self.pos + parola.len()) == Some(parola) {
            self.pos += parola.len();
            Ok(())
        } else {
            Err(Malformed)
        }
    }

    /// Consuma un valore scalare e restituisce `true`, oppure restituisce
    /// `false` davanti all'apertura di un contenitore (`{` o `[`, non
    /// consumata).
    fn salta_scalare(&mut self) -> Result<bool, Malformed> {
        match self.guarda().ok_or(Malformed)? {
            b'{' | b'[' => return Ok(false),
            b'"' => self.salta_stringa()?,
            b'-' | b'0'..=b'9' => {
                self.numero()?;
            }
            b't' => self.letterale(b"true")?,
            b'f' => self.letterale(b"false")?,
            b'n' => self.letterale(b"null")?,
            // `NaN`, `Infinity`, `-Infinity` (gia' rifiutato da `numero`) e
            // ogni altro byte.
            _ => return Err(Malformed),
        }
        Ok(true)
    }

    /// Consuma un valore qualunque, gia' validato, senza tenerne niente.
    fn salta_valore(&mut self) -> Result<(), Malformed> {
        if self.salta_scalare()? {
            Ok(())
        } else {
            salta_contenitore(self)
        }
    }
}

/// Una chiave di un oggetto aperto: l'impronta dei suoi punti di codice e la
/// posizione delle sue virgolette d'apertura, da cui rileggerla per intero.
///
/// Il bit alto della posizione segna la prima chiave di un oggetto: e' cio'
/// che dice dove cominciano le chiavi dell'oggetto che si chiude, senza una
/// seconda pila. Una posizione in una `&[u8]` non arriva mai a quel bit
/// (una fetta non supera `isize::MAX` byte).
#[derive(Clone, Copy)]
struct Chiave {
    impronta: u64,
    posizione: usize,
}

const PRIMA_DELL_OGGETTO: usize = 1 << (usize::BITS - 1);

impl Chiave {
    const fn inizio(self) -> usize {
        self.posizione & !PRIMA_DELL_OGGETTO
    }

    const fn prima(self) -> bool {
        self.posizione & PRIMA_DELL_OGGETTO != 0
    }
}

/// La sintassi del documento intero, senza ricorsione.
///
/// La pila dei contenitori aperti tiene un byte per livello; le chiavi degli
/// oggetti aperti stanno in un solo vettore, e la prima di ogni oggetto e'
/// segnata.
fn valida(byte: &[u8]) -> Result<(), Malformed> {
    let stato = RandomState::new();
    let mut cursore = Cursore { byte, pos: 0 };
    // `true` per un oggetto, `false` per una lista.
    let mut generi: Vec<bool> = Vec::new();
    let mut chiavi: Vec<Chiave> = Vec::new();
    'valore: loop {
        cursore.spazi();
        if !cursore.salta_scalare()? {
            let oggetto = cursore.guarda() == Some(b'{');
            cursore.pos += 1;
            cursore.spazi();
            if cursore.guarda() == Some(if oggetto { b'}' } else { b']' }) {
                cursore.pos += 1;
            } else {
                generi.push(oggetto);
                if oggetto {
                    chiave(&mut cursore, &stato, &mut chiavi, true)?;
                }
                continue 'valore;
            }
        }
        // Un valore e' finito: si chiudono i contenitori che finiscono con lui.
        loop {
            cursore.spazi();
            let Some(&oggetto) = generi.last() else {
                break 'valore;
            };
            match cursore.guarda() {
                Some(b',') => {
                    cursore.pos += 1;
                    if oggetto {
                        cursore.spazi();
                        chiave(&mut cursore, &stato, &mut chiavi, false)?;
                    }
                    continue 'valore;
                }
                Some(b'}') if oggetto => {
                    cursore.pos += 1;
                    generi.pop();
                    let inizio = chiavi
                        .iter()
                        .rposition(|chiave| chiave.prima())
                        .ok_or(Malformed)?;
                    let proprie = chiavi.get_mut(inizio..).ok_or(Malformed)?;
                    nessun_doppione(byte, proprie)?;
                    chiavi.truncate(inizio);
                }
                Some(b']') if !oggetto => {
                    cursore.pos += 1;
                    generi.pop();
                }
                _ => return Err(Malformed),
            }
        }
    }
    cursore.spazi();
    if cursore.pos == byte.len() {
        Ok(())
    } else {
        Err(Malformed)
    }
}

/// Una chiave e i due punti; la chiave si registra fra quelle dell'oggetto
/// aperto.
fn chiave(
    cursore: &mut Cursore<'_>,
    stato: &RandomState,
    chiavi: &mut Vec<Chiave>,
    prima: bool,
) -> Result<(), Malformed> {
    let inizio = cursore.pos;
    if inizio & PRIMA_DELL_OGGETTO != 0 {
        return Err(Malformed);
    }
    let impronta = cursore.impronta(stato)?;
    chiavi.push(Chiave {
        impronta,
        posizione: if prima {
            inizio | PRIMA_DELL_OGGETTO
        } else {
            inizio
        },
    });
    cursore.spazi();
    cursore.atteso(b':')
}

/// Le chiavi di un oggetto che si chiude sono tutte diverse.
///
/// Si ordinano per impronta, e soltanto le chiavi con la stessa impronta si
/// confrontano per intero: due chiavi uguali hanno per forza la stessa
/// impronta, quindi nessun doppione sfugge.
fn nessun_doppione(byte: &[u8], chiavi: &mut [Chiave]) -> Result<(), Malformed> {
    chiavi.sort_unstable_by_key(|chiave| chiave.impronta);
    let mut gruppo = 0;
    while gruppo < chiavi.len() {
        let impronta = chiavi[gruppo].impronta;
        let fine = chiavi[gruppo..]
            .iter()
            .position(|chiave| chiave.impronta != impronta)
            .map_or(chiavi.len(), |scarto| gruppo + scarto);
        for prima in gruppo..fine {
            for seconda in prima + 1..fine {
                if stesso_testo(byte, chiavi[prima].inizio(), chiavi[seconda].inizio())? {
                    return Err(Malformed);
                }
            }
        }
        gruppo = fine;
    }
    Ok(())
}

/// Le due stringhe JSON che cominciano in `prima` e `seconda` hanno gli stessi
/// punti di codice.
fn stesso_testo(byte: &[u8], prima: usize, seconda: usize) -> Result<bool, Malformed> {
    let mut a = Cursore { byte, pos: prima };
    let mut b = Cursore { byte, pos: seconda };
    a.atteso(b'"')?;
    b.atteso(b'"')?;
    loop {
        let (pa, pb) = (a.punto()?, b.punto()?);
        if pa != pb {
            return Ok(false);
        }
        if pa.is_none() {
            return Ok(true);
        }
    }
}

type Coppia = (DefinitionText, DefinitionText);

/// `id` e `ids` della radice, che deve essere un oggetto. Il documento e' gia'
/// stato validato: qui si percorre soltanto, e degli altri membri non si tiene
/// niente.
///
/// `id` e `ids` insieme, o `ids` che non e' una lista, sono malformati; ogni
/// voce deve essere un identificatore.
fn radice(cursore: &mut Cursore<'_>) -> Result<Vec<Coppia>, Malformed> {
    cursore.spazi();
    let mut id: Option<Coppia> = None;
    let mut ids: Option<Vec<Coppia>> = None;
    membri(
        cursore,
        |cursore, nome| {
            match nome {
                Some(0) => id = Some(identificatore(cursore)?),
                Some(_) => {
                    cursore.atteso(b'[')?;
                    let mut voci = Vec::new();
                    cursore.spazi();
                    if cursore.guarda() == Some(b']') {
                        cursore.pos += 1;
                    } else {
                        loop {
                            cursore.spazi();
                            voci.push(identificatore(cursore)?);
                            cursore.spazi();
                            match cursore.guarda() {
                                Some(b',') => cursore.pos += 1,
                                Some(b']') => {
                                    cursore.pos += 1;
                                    break;
                                }
                                _ => return Err(Malformed),
                            }
                        }
                    }
                    ids = Some(voci);
                }
                None => cursore.salta_valore()?,
            }
            Ok(())
        },
        &["id", "ids"],
    )?;
    match (id, ids) {
        (Some(id), None) => Ok(vec![id]),
        (None, Some(ids)) => Ok(ids),
        (None, None) => Ok(Vec::new()),
        (Some(_), Some(_)) => Err(Malformed),
    }
}

/// Un identificatore: un oggetto con `authority` stringa e `code` stringa o
/// intero. Gli altri membri non si tengono.
fn identificatore(cursore: &mut Cursore<'_>) -> Result<Coppia, Malformed> {
    let mut autorita = None;
    let mut codice = None;
    membri(
        cursore,
        |cursore, nome| {
            match nome {
                Some(0) => {
                    if cursore.guarda() != Some(b'"') {
                        return Err(Malformed);
                    }
                    autorita = Some(cursore.testo()?);
                }
                Some(_) => {
                    codice = Some(match cursore.guarda() {
                        Some(b'"') => cursore.testo()?,
                        Some(b'-' | b'0'..=b'9') => {
                            let intero = cursore.numero()?.ok_or(Malformed)?;
                            let testo =
                                std::str::from_utf8(cursore.byte.get(intero).ok_or(Malformed)?)
                                    .map_err(|_| Malformed)?;
                            // La forma di `str(int(...))` di Python: `-0` vale `0`.
                            DefinitionText::Unicode(if testo == "-0" {
                                "0".to_owned()
                            } else {
                                testo.to_owned()
                            })
                        }
                        _ => return Err(Malformed),
                    });
                }
                None => cursore.salta_valore()?,
            }
            Ok(())
        },
        &["authority", "code"],
    )?;
    match (autorita, codice) {
        (Some(autorita), Some(codice)) => Ok((autorita, codice)),
        _ => Err(Malformed),
    }
}

/// Percorre i membri dell'oggetto che comincia al cursore: per ciascuno,
/// `visita` riceve il cursore sul valore e l'indice in `nomi` della chiave, se
/// e' uno di quelli, e consuma il valore. Un valore che non e' un oggetto e'
/// malformato.
fn membri(
    cursore: &mut Cursore<'_>,
    mut visita: impl FnMut(&mut Cursore<'_>, Option<usize>) -> Result<(), Malformed>,
    nomi: &[&str],
) -> Result<(), Malformed> {
    cursore.atteso(b'{')?;
    cursore.spazi();
    if cursore.guarda() == Some(b'}') {
        cursore.pos += 1;
        return Ok(());
    }
    loop {
        cursore.spazi();
        let trovato = cursore.nome_fra(nomi)?;
        cursore.spazi();
        cursore.atteso(b':')?;
        cursore.spazi();
        visita(cursore, trovato)?;
        cursore.spazi();
        match cursore.guarda() {
            Some(b',') => cursore.pos += 1,
            Some(b'}') => {
                cursore.pos += 1;
                return Ok(());
            }
            _ => return Err(Malformed),
        }
    }
}

/// Salta un contenitore gia' validato contando le parentesi fuori dalle
/// stringhe, senza ricorsione e senza tenere niente.
fn salta_contenitore(cursore: &mut Cursore<'_>) -> Result<(), Malformed> {
    let mut aperti = 0_usize;
    loop {
        match cursore.guarda().ok_or(Malformed)? {
            b'"' => {
                cursore.salta_stringa()?;
                continue;
            }
            b'{' | b'[' => aperti += 1,
            b'}' | b']' => {
                aperti = aperti.checked_sub(1).ok_or(Malformed)?;
                if aperti == 0 {
                    cursore.pos += 1;
                    return Ok(());
                }
            }
            _ => {}
        }
        cursore.pos += 1;
    }
}
