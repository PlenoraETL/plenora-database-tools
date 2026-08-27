#!/usr/bin/env python3
"""Inventario dei test live, derivato dal codice e dalla suite compilata.

La logica è condivisa tra i provider: raccoglie i test dai sorgenti (una
funzione annotata come test, non un nome che comincia per `live_`), chiede a
`cargo test -- --list` cosa contiene la suite compilata e confronta le due con
l'esecuzione usando i nomi completi.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

# Una definizione di funzione con il blocco di attributi che la precede.
#
# L'espressione copre esplicitamente queste forme Rust valide:
#
# * il blocco puo essere **vuoto** e gli attributi possono stare sulla stessa
#   riga della firma (`#[test] fn x() {}`);
# * una riga puo portare **piu** attributi (`#[test] #[allow(dead_code)]`);
# * un attributo puo spezzarsi su piu righe, quindi dopo la parentesi chiusa si
#   ammettono i ritorni a capo — `flatten_attributes` li ha gia resi innocui,
#   ma la tolleranza resta;
# * le righe vuote non spezzano il blocco: sono cio in cui `strip_noncode`
#   trasforma i commenti;
# * il nome puo essere un identificatore raw (`fn r#live_x()`).
#
# La forma della riga e inoltre volutamente **non ambigua**: gli spazi prima
# dell'attributo e quelli dopo non possono contendersi lo stesso testo, perche
# fra i due c'e un `#[...]` obbligatorio. Una versione con due `[ \t]*`
# separati da un gruppo opzionale sembra equivalente e non lo e: su una lunga
# sequenza di righe bianche il motore le ripartisce in ogni modo possibile, e
# il match diventa esponenziale.
DEFINITION = re.compile(
    r"(?P<attributes>(?:[ \t]*(?:\#\[[^\]]*\][ \t\r\n]*)*\r?\n)*)"
    r"[ \t]*(?P<inline>(?:\#\[[^\]]*\][ \t]*)*)"
    r"(?:pub(?:\([^)]*\))?[ \t]+)?(?:async[ \t]+)?fn[ \t]+"
    r"(?:r\#)?(?P<name>[^\s(),;:]+)[ \t]*\(",
    re.MULTILINE,
)

# `#[test]`, `#[tokio::test]`, `#[tokio::test(flavor = "multi_thread")]`: il
# segmento `test` deve essere l'**ultimo** del path, cioe seguito da `]` o
# dagli argomenti. Un `\b` non basta: `#[foo::test::case]` lo soddisfaceva, e
# una funzione qualsiasi entrava nell'inventario come test.
TEST_ATTRIBUTE = re.compile(
    r"\#\[[ \t]*(?:[A-Za-z0-9_]+[ \t]*::[ \t]*)*test[ \t]*[\](]"
)

# Un singolo attributo emesso da `cfg_attr`, riconosciuto **dal suo inizio**:
# il path deve essere tutto l'attributo, non una parola dentro i suoi
# argomenti. `allow(dead_code, test, unused)` non è quindi scambiato per un
# attributo di test emesso.
EMITTED_TEST = re.compile(r"^(?:[A-Za-z0-9_]+\s*::\s*)*test\s*(?:\(|$)")


def top_level_parts(arguments: str) -> list[str]:
    """Gli argomenti separati dalle virgole di **primo** livello.

    `allow(dead_code, test)` e un argomento solo: le sue virgole stanno dentro
    le parentesi, e spezzarle avrebbe trasformato un lint in un attributo.
    """

    parts: list[str] = []
    depth = 0
    current = ""
    for char in arguments:
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
            continue
        current += char
    parts.append(current)
    return parts

# L'inizio di un `#[cfg_attr(...)]`, con lo spazio che Rust ammette prima
# della parentesi. Gli argomenti **non** si prendono con una regex: `.*` in
# modalita `DOTALL` e greedy, e su due `cfg_attr` consecutivi inglobava il
# secondo insieme al primo, perdendo entrambi. Si leggono invece con le
# parentesi bilanciate, che e l'unico modo di sapere dove finisce il primo.
CFG_ATTR_START = re.compile(r"\#\[[ \t]*cfg_attr[ \t]*\(")

def predicate_truth(predicate: str) -> bool | None:
    """Il valore di un predicato `cfg`, quando e costante.

    `all()` e vero per definizione e `any()` e falso, e da li `not`, `all` e
    `any` compongono. Tutto il resto dipende dalla configurazione e resta
    ignoto — `feature = "x"` puo essere vero o falso, e in dubbio il test si
    conta.

    Serve perche cio che un predicato costantemente falso emette non viene
    compilato: pretenderne l'esecuzione renderebbe il gate rosso per sempre.
    Riconoscere i soli `any()` e `false` letterali lasciava fuori forme valide
    e altrettanto costanti come `not(all())` e `all(any())`.
    """

    text = predicate.strip()
    if text in {"true", "false"}:
        return text == "true"
    for name in ("all", "any", "not"):
        if not text.startswith(name):
            continue
        rest = text[len(name) :].lstrip()
        if not rest.startswith("("):
            continue
        inner = rest[1:-1] if rest.endswith(")") else rest[1:]
        parts = [part for part in top_level_parts(inner) if part.strip()]
        values = [predicate_truth(part) for part in parts]
        if name == "not":
            return None if len(values) != 1 or values[0] is None else not values[0]
        if name == "all":
            if any(value is False for value in values):
                return False
            return True if all(value is True for value in values) else None
        if any(value is True for value in values):
            return True
        return False if all(value is False for value in values) else None
    return None


def cfg_attr_arguments(attributes: str) -> list[str]:
    """Gli argomenti di ogni `cfg_attr`, letti con parentesi bilanciate."""

    found: list[str] = []
    for match in CFG_ATTR_START.finditer(attributes):
        depth = 1
        index = match.end()
        while index < len(attributes) and depth:
            if attributes[index] == "(":
                depth += 1
            elif attributes[index] == ")":
                depth -= 1
                if depth == 0:
                    break
            index += 1
        found.append(attributes[match.end() : index])
    return found


# La scansione e pura e viene ripetuta sugli stessi file da piu chiamanti — i
# self-test la invocano decine di volte. Senza memoria il gate PostgreSQL
# passava da un secondo a settantacinque.
@lru_cache(maxsize=128)
def strip_noncode(source: str) -> str:
    """Il sorgente con commenti e contenuto delle stringhe ridotti a spazi.

    Le righe restano allineate — si sostituisce carattere per carattere, i
    ritorni a capo si conservano — cosi le espressioni che seguono lavorano
    sullo stesso testo senza doversi difendere da cio che testo non e.

    Serve a due difetti opposti e reali. Un test archiviato dentro `/* ... */`,
    o dentro un raw string literal usato come fixture SQL, conserva il proprio
    attributo: la scansione lo raccoglieva e il gate pretendeva l'esecuzione di
    codice che non viene nemmeno compilato. Un commento fra gli attributi e la
    firma, all'opposto, spezzava il blocco e toglieva dall'inventario un test
    vero.

    I lifetime non sono char literal: `&'input T` non apre nulla. Si consuma un
    `'` solo quando chiude entro un carattere o una escape.
    """

    out: list[str] = []
    index = 0
    length = len(source)

    def blank(segment: str) -> str:
        return "".join("\n" if char == "\n" else " " for char in segment)

    while index < length:
        char = source[index]
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = length if end < 0 else end
            out.append(blank(source[index:end]))
            index = end
        elif source.startswith("/*", index):
            depth = 1
            end = index + 2
            while end < length and depth:
                if source.startswith("/*", end):
                    depth += 1
                    end += 2
                elif source.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
            out.append(blank(source[index:end]))
            index = end
        elif char in "rb" and (raw := _raw_string(source, index)) is not None:
            end, hashes = raw
            out.append(blank(source[index:end]))
            index = end
        elif char == '"':
            end = index + 1
            while end < length:
                if source[end] == "\\":
                    end += 2
                    continue
                if source[end] == '"':
                    end += 1
                    break
                end += 1
            out.append(blank(source[index:end]))
            index = end
        elif char == "'":
            end = _char_literal(source, index)
            if end is None:
                out.append(char)
                index += 1
            else:
                out.append(blank(source[index:end]))
                index = end
        else:
            out.append(char)
            index += 1
    return "".join(out)


def flatten_attributes(code: str) -> str:
    """Gli attributi su una riga sola, senza cambiare le posizioni.

    `#[tokio ::\\n test]` e Rust valido, e la lettura per righe non lo vedeva:
    il ritorno a capo dentro l'attributo spezzava il blocco e il test spariva
    dall'inventario. Qui i soli ritorni a capo **interni a un attributo**
    diventano spazi — un carattere per un carattere, quindi gli offset restano
    quelli del sorgente e `ignore_reasons` puo continuare a rileggerlo.
    """

    out = list(code)
    index = 0
    length = len(code)
    while index < length:
        if code.startswith("#[", index):
            depth = 0
            cursor = index + 1
            while cursor < length:
                if code[cursor] == "[":
                    depth += 1
                elif code[cursor] == "]":
                    depth -= 1
                    if depth == 0:
                        break
                elif code[cursor] == "\n":
                    out[cursor] = " "
                cursor += 1
            index = cursor + 1
            continue
        index += 1
    return "".join(out)


def _raw_string(source: str, index: int) -> tuple[int, int] | None:
    """Fine e numero di cancelletti di un `r"..."` / `br#"..."#`, se c'e."""

    cursor = index
    if source[cursor] == "b":
        cursor += 1
    if cursor >= len(source) or source[cursor] != "r":
        return None
    cursor += 1
    hashes = 0
    while cursor < len(source) and source[cursor] == "#":
        hashes += 1
        cursor += 1
    if cursor >= len(source) or source[cursor] != '"':
        return None
    terminator = '"' + "#" * hashes
    end = source.find(terminator, cursor + 1)
    end = len(source) if end < 0 else end + len(terminator)
    return end, hashes


def _char_literal(source: str, index: int) -> int | None:
    """Fine di un char literal, oppure `None` se e un lifetime."""

    if source.startswith("'\\", index):
        end = source.find("'", index + 2)
        return None if end < 0 else end + 1
    if index + 2 < len(source) and source[index + 2] == "'":
        return index + 3
    return None


def declares_a_test(attributes: str) -> bool:
    """Il blocco di attributi dichiara la funzione come test."""

    if TEST_ATTRIBUTE.search(attributes):
        return True
    for arguments in cfg_attr_arguments(attributes):
        # Il primo argomento e il predicato, gli altri sono gli attributi
        # emessi: si guardano uno per uno, e ciascuno deve **essere** un path
        # che finisce per `test`, non contenerne uno fra i propri argomenti.
        parts = top_level_parts(arguments)
        if predicate_truth(parts[0]) is False:
            continue
        if any(EMITTED_TEST.match(part.strip()) for part in parts[1:]):
            return True
    return False

# Una riga di `cargo test -- --list`: `modulo::tests::nome: test`.
LISTED = re.compile(r"^(\S+): test$", re.MULTILINE)

# Una riga di esito di `cargo test`, con il nome completo del test.
EXECUTED = re.compile(r"^test (\S+) \.\.\. ok$", re.MULTILINE)

# Il motivo che un `#[ignore = "..."]` porta con se.
IGNORE_REASON = re.compile(r'\#\[ignore\s*=\s*"([^"]*)"\]')


def leaf(qualified: str) -> str:
    """L'ultimo segmento di un nome pienamente qualificato."""

    return qualified.rsplit("::", 1)[-1]


def annotated_tests(source: str) -> list[str]:
    """I nomi delle funzioni annotate come test in un sorgente Rust.

    Restituisce una lista, non un insieme: due definizioni con lo stesso nome
    sono un fatto che il chiamante deve poter vedere.
    """

    return list(_annotated_tests(source))


@lru_cache(maxsize=128)
def _annotated_tests(source: str) -> tuple[str, ...]:
    code = flatten_attributes(strip_noncode(source))
    return tuple(
        match.group("name")
        for match in DEFINITION.finditer(code)
        if declares_a_test(match.group("attributes") + match.group("inline"))
        and match.group("name").isidentifier()
    )


def source_inventory(paths: list[Path], keep=lambda name: True) -> set[str]:
    """I test annotati nei sorgenti indicati, senza omonimi.

    Due test con lo stesso nome foglia in moduli diversi sono un errore: il
    confronto con l'esecuzione avviene sui nomi completi, e un insieme di nomi
    foglia li farebbe collassare in uno, lasciando che l'esecuzione di uno solo
    soddisfi entrambi.
    """

    inventory: set[str] = set()
    duplicates: list[str] = []
    for path in sorted(paths):
        for name in annotated_tests(path.read_text(encoding="utf-8")):
            if not keep(name):
                continue
            if name in inventory:
                duplicates.append(name)
            inventory.add(name)
    if duplicates:
        raise RuntimeError(
            f"test con lo stesso nome in moduli diversi: {sorted(set(duplicates))}. "
            "Il confronto con l'esecuzione userebbe un nome per due test."
        )
    if not inventory:
        raise RuntimeError(
            "nessun test trovato nei sorgenti indicati: l'inventario non puo "
            "essere vuoto"
        )
    return inventory


def listed_tests(output: str, keep=lambda name: True) -> set[str]:
    """I test che la suite compilata contiene, con il nome completo."""

    listed = {name for name in LISTED.findall(output) if keep(name)}
    if not listed:
        raise RuntimeError("`cargo test -- --list` non ha elencato nessun test atteso")
    return listed


def executed_tests(output: str) -> set[str]:
    """I test che la corsa ha riportato `ok`, con il nome completo."""

    return set(EXECUTED.findall(output))


def ignore_reasons(source: str) -> dict[str, str]:
    """Per ogni test annotato, il motivo del suo `#[ignore = "..."]`."""

    reasons: dict[str, str] = {}
    for match in DEFINITION.finditer(flatten_attributes(strip_noncode(source))):
        if not declares_a_test(match.group("attributes") + match.group("inline")):
            continue
        # Il motivo vive **dentro** una stringa, che `strip_noncode` ha
        # svuotato: va riletto dal sorgente originale. Le posizioni coincidono
        # perche lo svuotamento sostituisce carattere per carattere.
        start = match.start("attributes")
        end = match.end("inline")
        reason = IGNORE_REASON.search(source[start:end])
        if reason:
            reasons[match.group("name")] = reason.group(1)
    return reasons
