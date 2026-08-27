#!/usr/bin/env python3
"""Inventario dei test del provider MySQL letto dalla sorgente Rust.

Il gate fissa per nome ogni test che si aspetta di vedere passare. Un
inventario scritto a mano invecchia in silenzio: un test aggiunto e mai
eseguito, o rimosso e mai notato, non produce alcun segnale. Questo modulo
estrae l'inventario reale da `crates/plenora-db-mysql/src/**.rs` e permette
al gate di fallire chiuso appena la lista dichiarata e la sorgente divergono.

Le tre famiglie hanno runner diversi e vanno tenute distinte:

* `unit`  — test senza server, eseguiti con `--skip live_`;
* `live_default` — test live **non** `#[ignore]`, che il runner di default
  esegue e che quindi richiedono un server anche in una `cargo test` nuda;
* `live_reference` — test live `#[ignore]`, eseguiti solo con `--ignored`.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

# Import piatto, come `render_state` fa con questo modulo: `scripts` non e un
# pacchetto installato, e importarlo per nome funziona solo quando la radice e
# gia sul path — cioe non sempre.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_inventory  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "crates" / "plenora-db-mysql" / "src"
LIVE_MODULE = "live_tests"

ATTRIBUTE = re.compile(r"^\s*#\[")
# Un attributo completo a inizio riga, con cio che segue sulla stessa riga.
INLINE_ATTRIBUTE = re.compile(r"^\s*#\[[^\]]*\]\s*(?=\S)")
# Qualunque attributo il cui ultimo segmento sia `test`, non i soli
# `#[test]` e `#[tokio::test]`: la forma stretta non e una convenzione che
# questo modulo verifica, e un runner diverso avrebbe smesso di essere
# visto.
TEST_ATTRIBUTE = re.compile(
    r"^\s*#\[\s*(?:\w+\s*::\s*)*test\s*(?:\([^)]*\))?\]\s*$"
)
IGNORE_ATTRIBUTE = re.compile(r"^\s*#\[ignore\b")
# `pub(crate)` e `pub(super)` sono visibilità valide e vanno inventariate.
# `r#` e il prefisso degli identificatori raw e **non** fa parte del nome:
# `fn r#type()` si chiama `type`. Non accettarlo faceva sparire il test senza
# dire niente, ed e la stessa correzione gia fatta nell'inventario condiviso.
FUNCTION = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(?:r\#)?([^\s(),;:]+)\s*\("
)

# Un `mod` annidato dentro `mod tests`. Il percorso che questo modulo
# costruisce ha due soli segmenti — `file::tests::nome` — quindi un livello in
# piu non e rappresentabile: meglio fallire che inventare un nome che cargo
# non stampera mai.
TEST_MODULE = re.compile(r"^[ 	]*mod[ 	]+tests[ 	]*\{[ 	]*$")


def nested_module_name(line: str) -> str | None:
    """Il nome del `mod` dichiarato su questa riga, se ce n'e uno.

    Letto con operazioni su stringhe e non con una regex: la versione con i
    gruppi opzionali e le classi negate faceva esplodere il backtracking del
    motore fino a `internal error in regular expression engine`, e il
    fallimento compariva o no a seconda di quanto stack aveva gia consumato il
    chiamante. Un difetto che dipende dal chiamante non e un difetto che si
    corregge stringendo la regex.

    La visibilita fa parte della dichiarazione — `pub mod`, `pub(crate) mod` —
    e il prefisso `r#` non appartiene al nome.
    """

    head = line.strip()
    if not head.endswith("{"):
        return None
    head = head[:-1].strip()
    if head.startswith("pub"):
        rest = head[3:]
        if rest.startswith("("):
            if ")" not in rest:
                return None
            rest = rest.split(")", 1)[1]
        head = rest.strip()
    if not head.startswith("mod "):
        return None
    name = head[4:].strip().removeprefix("r#")
    return name if name.isidentifier() else None


@dataclass(frozen=True)
class MysqlTest:
    """Un test del provider, con il percorso che `cargo test` stampa."""

    path: str
    ignored: bool
    live: bool

    @property
    def family(self) -> str:
        if not self.live:
            return "unit"
        return "live_reference" if self.ignored else "live_default"


def _scan(source: Path) -> list[MysqlTest]:
    """Estrae i test di un singolo file sorgente.

    Il repository segue una convenzione stretta: i test live stanno al primo
    livello di `live_tests.rs`, tutti gli altri dentro `mod tests` in fondo al
    file del modulo. La convenzione viene verificata qui, non assunta: un test
    fuori posto e un errore, non un test perso.

    # Raises

    `RuntimeError` quando un test unitario compare prima di `mod tests` o
    quando un test live non rispetta il prefisso di nome del suo runner.
    """

    stem = source.stem
    # Commenti e stringhe vengono svuotati prima di guardare le righe: un test
    # dentro `/* ... */` o dentro una fixture testuale non e un test, e una
    # scansione per righe non ha modo di accorgersene da sola. Lo svuotamento
    # conserva le righe, quindi gli indici restano quelli del file.
    # `flatten_attributes` porta su una riga sola gli attributi spezzati su
    # piu righe: una scansione per righe non li vedrebbe, e il test sparirebbe
    # dall'inventario in silenzio.
    lines = live_inventory.flatten_attributes(
        live_inventory.strip_noncode(source.read_text(encoding="utf-8"))
    ).splitlines()
    module_line = next(
        (index for index, line in enumerate(lines) if TEST_MODULE.match(line)), None
    )
    # Dove il modulo **finisce**: un test scritto dopo la sua parentesi chiusa
    # non sta in `mod tests`, e attribuirgli quel percorso avrebbe fatto
    # pretendere a cargo un nome che non esiste.
    module_end = len(lines)
    if module_line is not None:
        depth = 0
        for index in range(module_line, len(lines)):
            depth += lines[index].count("{") - lines[index].count("}")
            if depth == 0 and index > module_line:
                module_end = index
                break
    found: list[MysqlTest] = []
    attributes: list[str] = []
    # La discovery e ricorsiva (`rglob`), ma il percorso si costruisce dal solo
    # `stem`: un file in una sottodirectory avrebbe un modulo intermedio che
    # nessuno rappresenta. Se ne comparisse uno, questo
    # controllo lo direbbe invece di lasciare che il gate chieda a cargo un
    # nome inesistente.
    if source.parent != SOURCE_DIR:
        raise RuntimeError(
            f"{source.relative_to(SOURCE_DIR)}: un sorgente in una "
            "sottodirectory ha un percorso di modulo che questo inventario non "
            "sa costruire"
        )
    for index, line in enumerate(lines):
        # Un attributo puo stare sulla stessa riga della firma: si stacca il
        # prefisso e si guarda cio che resta, invece di scartare la riga.
        while (inline := INLINE_ATTRIBUTE.match(line)) is not None:
            attributes.append(inline.group(0).strip())
            line = line[inline.end() :]
        if ATTRIBUTE.match(line):
            attributes.append(line)
            continue
        match = FUNCTION.match(line)
        if match is None:
            # Una riga vuota non spezza il blocco: e cio in cui si trasforma un
            # commento fra gli attributi e la firma, e spezzarlo toglieva dal
            # gate un test vero.
            if line.strip():
                attributes.clear()
            continue
        # Lo stesso predicato dell'inventario condiviso: riconosce anche
        # `#[cfg_attr(<pred>, tokio::test)]`, che la forma stretta perdeva.
        is_test = any(
            TEST_ATTRIBUTE.match(entry) for entry in attributes
        ) or live_inventory.declares_a_test("\n".join(attributes))
        ignored = any(IGNORE_ATTRIBUTE.match(entry) for entry in attributes)
        attributes.clear()
        if not is_test:
            continue
        name = match.group(1)
        if not name.isidentifier():
            # Lo stesso criterio dell'inventario condiviso: Rust usa le regole
            # XID, che `str.isidentifier()` segue. Un `\w` piu stretto perdeva
            # identificatori validi — e li perdeva **solo qui**, quindi il
            # confronto con cargo divergeva invece di restare coerente.
            continue
        if stem == LIVE_MODULE:
            if not name.startswith("live_"):
                raise RuntimeError(
                    f"{source.name}: il test {name} non ha il prefisso live_ "
                    "e sfuggirebbe al filtro del runner live"
                )
            found.append(MysqlTest(f"{LIVE_MODULE}::{name}", ignored, live=True))
            continue
        if name.startswith("live_"):
            raise RuntimeError(
                f"{source.name}: il test {name} usa il prefisso live_ fuori da "
                f"{LIVE_MODULE}.rs e verrebbe escluso dai runner offline"
            )
        nested = next(
            (
                nested_module_name(lines[position])
                for position in range(module_line + 1, module_end)
                if module_line is not None
                and nested_module_name(lines[position]) is not None
            ),
            None,
        )
        if nested is not None:
            raise RuntimeError(
                f"{source.name}: `mod {nested}` annidato dentro "
                "`mod tests`: il percorso stampato da cargo avrebbe un segmento "
                "in piu di quelli che questo inventario sa costruire"
            )
        if module_line is None or not module_line < index < module_end:
            raise RuntimeError(
                f"{source.name}: il test {name} non e dentro `mod tests`, "
                "il percorso stampato da cargo non sarebbe derivabile"
            )
        found.append(MysqlTest(f"{stem}::tests::{name}", ignored, live=False))
    return found


# File che dichiarano test ma non appartengono alla qualifica MySQL.
#
# `mariadb_evidence.rs` misura MariaDB attraverso il provider e ha un runner
# suo (`scripts/check_mariadb_driver.py`). Contarlo qui lo farebbe entrare
# negli inventari dei tre runner del gate, che dichiarano cosa il provider
# **MySQL** ha dimostrato: un test su un motore non qualificato non puo
# sostenere quell'affermazione, e il gate finirebbe per pretenderne
# l'esecuzione contro il riferimento sbagliato.
#
# `session_evidence.rs` misura la semantica di sessione sui riferimenti,
# con lo stesso runner parametrizzato (`scripts/check_session_matrix.py`).
# Vale la stessa ragione: due terzi delle sue corse avvengono su un motore che
# il gate non qualifica.
EXCLUDED_SOURCES = frozenset({"mariadb_evidence.rs", "session_evidence.rs"})


def collect() -> dict[str, frozenset[str]]:
    """Inventario reale della sorgente, per famiglia di runner.

    # Raises

    `RuntimeError` quando la sorgente viola la convenzione di posizione o di
    nome su cui i runner si appoggiano.
    """

    families: dict[str, set[str]] = {
        "unit": set(),
        "live_default": set(),
        "live_reference": set(),
    }
    # `rglob`: il docstring dice `src/**.rs` e la glob piatta si fermava al
    # primo livello. Un test in un sottomodulo sarebbe rimasto fuori
    # dall'inventario senza che niente lo dicesse.
    for source in sorted(SOURCE_DIR.rglob("*.rs")):
        if source.name in EXCLUDED_SOURCES:
            continue
        for test in _scan(source):
            if test.path in families[test.family]:
                raise RuntimeError(f"test duplicato nell'inventario: {test.path}")
            families[test.family].add(test.path)
    if not families["unit"] or not families["live_reference"]:
        raise RuntimeError("inventario MySQL incompleto: famiglia vuota")
    return {name: frozenset(paths) for name, paths in families.items()}


def difference(declared: frozenset[str], observed: frozenset[str]) -> str:
    missing = sorted(declared - observed)
    unexpected = sorted(observed - declared)
    return f"mancanti={missing}, inattesi={unexpected}"


if __name__ == "__main__":
    inventory = collect()
    for family in ("unit", "live_default", "live_reference"):
        print(f"# {family} ({len(inventory[family])})")
        for path in sorted(inventory[family]):
            print(f'    "{path}",')
