#!/usr/bin/env python3
"""Genera `docs/STATO.md` leggendo il codice, e verifica che sia aggiornato.

Perche esiste
-------------

I documenti di questo repository ripetevano fatti che vivono nel codice: i
conteggi dei test, i sub-comandi del CLI, quali capability un provider
pubblica, quali write mode sono aperte. Un fatto scritto due volte diverge, e
diverge in silenzio — chi legge non ha modo di sapere quale delle due copie e
quella vera.

La difesa era diciotto guardie che rileggevano il Markdown cercando frasi e
numeri: `assertIn("staging + publish", row)`, `re.finditer(r"(\\d+) test
live")`. Funzionavano, ma presidiavano la **prosa**, e quindi presidiavano
anche la sua forma: riscrivere una frase in modo equivalente faceva rosso, e
aggiungere un documento nuovo apriva un buco che nessuno vedeva.

Qui il fatto si scrive una volta sola, nel codice, e il documento si genera.
La guardia diventa una: rigenerare non deve produrre differenze.

Uso
---

    python scripts/render_state.py            # riscrive il documento
    python scripts/render_state.py --check    # esce 1 se e disallineato
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from scripts.mysql_inventory import collect
    from scripts.phase0_validate import ACTIVE_MAJOR
except ModuleNotFoundError:  # esecuzione diretta: python scripts/...
    from mysql_inventory import collect
    from phase0_validate import ACTIVE_MAJOR

TARGET = ROOT / "docs" / "STATO.md"

# Dove vive la dichiarazione delle capability di ciascun provider pubblico.
# `impl` seleziona il blocco quando un file ne contiene piu d'uno: il profilo
# MySQL e quello MariaDB stanno nello stesso modulo, e il terzo profilo di quel
# file esiste solo per i test.
CAPABILITY_SOURCES = (
    ("PostgreSQL", "crates/plenora-db-postgres/src/catalog/capabilities.rs", None),
    ("MySQL", "crates/plenora-db-mysql/src/profile.rs", "impl ProductProfile for MysqlProfile"),
    ("MariaDB", "crates/plenora-db-mysql/src/profile.rs", "impl ProductProfile for MariadbProfile"),
    ("SQL Server", "crates/plenora-db-sqlserver/src/provider.rs", None),
)
CAPABILITY_GROUPS = (("reads", "ReadCapabilities"), ("writes", "WriteCapabilities"))


class RenderError(RuntimeError):
    pass


def braced_body(text: str, open_index: int) -> str:
    """Il contenuto della graffa che comincia a `open_index`."""
    depth = 0
    for index in range(open_index, len(text)):
        character = text[index]
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return text[open_index + 1 : index]
    raise RenderError("graffa non chiusa")


def strip_comments(body: str) -> Iterator[str]:
    for line in body.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith("//"):
            yield stripped


def capability_fields(
    source: str, field: str, kind: str, marker: str | None
) -> dict[str, str]:
    """I campi di un blocco capability, con il valore com'e scritto.

    I valori non letterali — `spatial` su PostgreSQL, dove la capability
    dipende dalla presenza di PostGIS — restano l'espressione sorgente. Non
    vengono risolti in `si`/`no`: sarebbe un'affermazione che il codice non fa.
    """
    text = (ROOT / source).read_text(encoding="utf-8")
    if marker is not None:
        start = text.index(marker)
        text = braced_body(text, text.index("{", start))
    needle = f"{field}: {kind} {{"
    if needle not in text:
        raise RenderError(f"blocco {needle.strip()} assente in {source}")
    body = braced_body(text, text.index(needle) + len(needle) - 1)
    fields: dict[str, str] = {}
    for line in strip_comments(body):
        match = re.fullmatch(r"([a-z_]+): (.+),", line)
        if match:
            fields[match.group(1)] = match.group(2)
    if not fields:
        raise RenderError(f"blocco {field} vuoto in {source}")
    return fields


def capability_matrix() -> dict[str, dict[str, dict[str, str]]]:
    matrix: dict[str, dict[str, dict[str, str]]] = {}
    for provider, source, marker in CAPABILITY_SOURCES:
        matrix[provider] = {
            name: capability_fields(source, name, kind, marker)
            for name, kind in CAPABILITY_GROUPS
        }
    return matrix


def workspace_version() -> str:
    text = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    match = re.search(r'(?m)^version = "([^"]+)"', text)
    if not match:
        raise RenderError("versione del workspace assente")
    return match.group(1)


def crates() -> list[tuple[str, str]]:
    """Nome e versione di ogni crate.

    Un crate puo ereditare la versione dal workspace con `version.workspace`:
    in quel caso la versione vera e quella della radice, e riportare qui
    "workspace" nasconderebbe il numero a chi legge.
    """
    shared = workspace_version()
    found = []
    for manifest in sorted((ROOT / "crates").glob("*/Cargo.toml")):
        text = manifest.read_text(encoding="utf-8")
        name = re.search(r'(?m)^name = "([^"]+)"', text)
        if not name:
            raise RenderError(f"nome assente: {manifest}")
        version = re.search(r'(?m)^version = "([^"]+)"', text)
        if version:
            declared = version.group(1)
        elif re.search(r"(?m)^version\.workspace = true", text):
            declared = shared
        else:
            raise RenderError(f"versione assente: {manifest}")
        found.append((name.group(1), declared))
    if not found:
        raise RenderError("nessun crate trovato")
    return found


def contract_messages() -> list[str]:
    root = ROOT / "contracts" / ACTIVE_MAJOR
    names = sorted(path.name for path in root.glob("*.schema.json"))
    if not names:
        raise RenderError(f"nessuno schema nella major attiva {ACTIVE_MAJOR}")
    return names


def cli_subcommands() -> list[str]:
    main = (ROOT / "crates" / "plenora-database-cli" / "src" / "main.rs").read_text(
        encoding="utf-8"
    )
    names = sorted(set(re.findall(r'\n\s+"([a-z][a-z0-9-]+)" => ', main)))
    if not names:
        raise RenderError("dispatch del CLI non riconosciuto")
    return names


def test_inventory() -> list[tuple[str, int]]:
    observed = collect()
    return [(family, len(observed[family])) for family in sorted(observed)]


def table(header: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |"]
    lines.append("|" + "|".join(" --- " for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def render() -> str:
    matrix = capability_matrix()
    providers = [name for name, _, _ in CAPABILITY_SOURCES]
    lines = [
        "# Stato del codice",
        "",
        "**Documento generato.** Non va modificato a mano: ogni riga qui sotto",
        "e letta dai sorgenti da `scripts/render_state.py`, e una guardia",
        "verifica che rigenerarlo non produca differenze. Se un numero e",
        "sbagliato, e sbagliato nel codice — oppure il documento e vecchio, e",
        "si aggiorna cosi:",
        "",
        "```powershell",
        "python scripts\\render_state.py",
        "```",
        "",
        "## Crate",
        "",
    ]
    lines += table(
        ["crate", "versione"], [[f"`{name}`", version] for name, version in crates()]
    )
    lines += [
        "",
        "## Contratto attivo",
        "",
        f"La major attiva e `contracts/{ACTIVE_MAJOR}/`, e contiene:",
        "",
    ]
    lines += [f"- `{name}`" for name in contract_messages()]
    lines += [
        "",
        "Le major precedenti restano leggibili ma sono ritirate: nessuno le",
        "referenzia, e il gate offline fallisce se la major attiva torna a",
        "farlo.",
        "",
        "## Capability pubblicate",
        "",
        "Cio che ciascun provider dichiara, letto dalla sua dichiarazione. Un",
        "valore che non e un letterale — `spatial` su PostgreSQL dipende dalla",
        "presenza di PostGIS — resta l'espressione sorgente: risolverla qui",
        "sarebbe un'affermazione che il codice non fa.",
        "",
    ]
    for group, _ in CAPABILITY_GROUPS:
        names: list[str] = []
        for provider in providers:
            for field in matrix[provider][group]:
                if field not in names:
                    names.append(field)
        lines += [f"### `{group}`", ""]
        lines += table(
            [group] + providers,
            [
                [f"`{field}`"]
                + [f"`{matrix[provider][group].get(field, '—')}`" for provider in providers]
                for field in names
            ],
        )
        lines.append("")
    lines += ["## Sub-comandi del CLI", ""]
    lines += [f"- `{name}`" for name in cli_subcommands()]
    lines += [
        "",
        "## Inventario dei test MySQL",
        "",
        "Le tre famiglie che il gate MySQL distingue, contate sulla sorgente.",
        "",
    ]
    lines += table(
        ["famiglia", "test"],
        [[f"`{family}`", str(count)] for family, count in test_inventory()],
    )
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="non scrive: esce 1 se il documento e disallineato",
    )
    arguments = parser.parse_args(argv if argv is not None else sys.argv[1:])
    try:
        rendered = render()
    except (RenderError, OSError, ValueError) as exc:
        print(f"render state: {exc}", file=sys.stderr)
        return 1
    if arguments.check:
        current = TARGET.read_text(encoding="utf-8") if TARGET.is_file() else ""
        if current != rendered:
            print(
                "render state: docs/STATO.md non e allineato al codice; "
                "rigeneralo con python scripts/render_state.py",
                file=sys.stderr,
            )
            return 1
        return 0
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(rendered, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
