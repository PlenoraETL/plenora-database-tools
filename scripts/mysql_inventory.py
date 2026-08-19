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
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "crates" / "plenora-db-mysql" / "src"
LIVE_MODULE = "live_tests"

ATTRIBUTE = re.compile(r"^\s*#\[")
TEST_ATTRIBUTE = re.compile(r"^\s*#\[(tokio::)?test\]\s*$")
IGNORE_ATTRIBUTE = re.compile(r"^\s*#\[ignore\b")
FUNCTION = re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s+([A-Za-z0-9_]+)\s*\(")
TEST_MODULE = re.compile(r"^\s*mod\s+tests\s*\{\s*$")


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
    lines = source.read_text(encoding="utf-8").splitlines()
    module_line = next(
        (index for index, line in enumerate(lines) if TEST_MODULE.match(line)), None
    )
    found: list[MysqlTest] = []
    attributes: list[str] = []
    for index, line in enumerate(lines):
        if ATTRIBUTE.match(line):
            attributes.append(line)
            continue
        match = FUNCTION.match(line)
        if match is None:
            if line.strip():
                attributes.clear()
            continue
        is_test = any(TEST_ATTRIBUTE.match(entry) for entry in attributes)
        ignored = any(IGNORE_ATTRIBUTE.match(entry) for entry in attributes)
        attributes.clear()
        if not is_test:
            continue
        name = match.group(1)
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
        if module_line is None or index < module_line:
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
# `session_evidence.rs` misura la semantica di sessione sui tre riferimenti,
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
    for source in sorted(SOURCE_DIR.glob("*.rs")):
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
