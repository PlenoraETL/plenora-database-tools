#!/usr/bin/env python3
"""Rifiuta il testo UTF-8 codificato due volte nei sorgenti del repository.

Un file UTF-8 riletto come Latin-1 o Windows-1252 e salvato di nuovo in UTF-8
trasforma ogni lettera accentata e ogni segno tipografico in due o tre
caratteri: la `e` con l'accento grave diventa una `A` con la tilde seguita da
una diaeresi, il trattino lungo diventa una `a` con l'accento circonflesso, il
simbolo dell'euro e una virgoletta. Il compilatore non se ne accorge: il testo
resta UTF-8 valido. Il 25/10 (seconda rilettura)
`crates/plenora-db-mysql/src/write.rs` ne aveva 25 righe, tre in messaggi
d'errore pubblici.

Il controllo e oggettivo: cerca le sequenze che un carattere italiano o di
punteggiatura comune produce dopo la doppia codifica, che nessun testo
italiano o inglese corretto contiene. Le sequenze sono costruite qui dai loro
punti di codice, cosi che questo file non le contenga.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SUFFIXES = {
    ".md",
    ".py",
    ".pyi",
    ".rs",
    ".sh",
    ".sql",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}

#: Le parti del repository che non sono sorgenti nostri: codice di terzi
#: copiato com'e, e i contratti importati da un altro repository.
SKIP_PARTS = {"vendor", "contracts", "target", ".git"}

#: I caratteri che Windows-1252 mette fra 0x80 e 0x9F: il secondo byte di una
#: lettera accentata riletto con quella tabella.
_CP1252_EXTRA = (
    0x0152, 0x0153, 0x0160, 0x0161, 0x0178, 0x017D, 0x017E, 0x0192, 0x02C6,
    0x02DC, 0x2013, 0x2014, 0x2018, 0x2019, 0x201A, 0x201C, 0x201D, 0x201E,
    0x2020, 0x2021, 0x2022, 0x2026, 0x2030, 0x2039, 0x203A, 0x20AC, 0x2122,
)


#: Il secondo byte (0x80-0x8F) dei segni fra U+2000 e U+23FF riletto con
#: Windows-1252: il trattino e le virgolette (0x80), l'euro (0x82), il
#: marchio (0x84) e gli altri.
_CP1252_E2_SECOND = (
    0x20AC, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030,
    0x0160, 0x2039, 0x0152, 0x017D,
)


def _escaped(code: int) -> str:
    return re.escape(chr(code))


def _range(low: int, high: int) -> str:
    return f"{_escaped(low)}-{_escaped(high)}"


#: Tre famiglie:
#: - U+00C3 seguita da U+0080-U+00BF, da un segno di Windows-1252 o da uno
#:   spazio (la `a` accentata diventa U+00C3 piu uno spazio non separabile,
#:   che un editor riscrive come spazio): le lettere accentate latine;
#: - U+00E2 seguita da un secondo byte fra 0x80 e 0x8F riletto con
#:   Windows-1252 e da un carattere: i segni fra U+2000 e U+23FF (trattini,
#:   apostrofi e virgolette tipografiche, puntini, euro);
#: - U+00C2 seguita da U+00A0-U+00BF: virgolette basse, paragrafo, grado,
#:   spazio non separabile.
MOJIBAKE = re.compile(
    _escaped(0x00C3)
    + "["
    + _range(0x0080, 0x00BF)
    + "".join(_escaped(code) for code in _CP1252_EXTRA)
    + " ]"
    + "|"
    + _escaped(0x00E2)
    + "["
    + "".join(_escaped(code) for code in _CP1252_E2_SECOND)
    + "]."
    + "|"
    + _escaped(0x00C2)
    + "["
    + _range(0x00A0, 0x00BF)
    + "]"
)


def tracked_files(root: Path) -> list[Path]:
    """I file tracciati da git, o tutti i file sotto `root` fuori da git."""
    try:
        listed = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=root,
            check=True,
            capture_output=True,
        ).stdout.decode("utf-8")
        candidates = [root / name for name in listed.split("\0") if name]
    except (OSError, subprocess.CalledProcessError):
        candidates = [path for path in root.rglob("*") if path.is_file()]
    return sorted(
        path
        for path in candidates
        if path.suffix in SUFFIXES
        and not SKIP_PARTS.intersection(path.relative_to(root).parts)
        and path.is_file()
    )


def violations(path: Path) -> list[tuple[int, str]]:
    """Le righe di `path` con una sequenza della doppia codifica."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return [(0, "il file non e UTF-8 valido")]
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        match = MOJIBAKE.search(line)
        if match:
            found.append((number, match.group(0)))
    return found


def check_repository(root: Path = ROOT) -> tuple[int, list[str]]:
    files = tracked_files(root)
    problems = [
        f"{path.relative_to(root).as_posix()}:{number}: sequenza {ascii(sequence)}"
        for path in files
        for number, sequence in violations(path)
    ]
    return len(files), problems


def main() -> int:
    checked, problems = check_repository()
    for problem in problems:
        print(problem)
    if problems:
        print(f"testo codificato due volte: {len(problems)} righe in {checked} file controllati")
        return 1
    print(f"nessuna doppia codifica in {checked} file")
    return 0


if __name__ == "__main__":
    sys.exit(main())
