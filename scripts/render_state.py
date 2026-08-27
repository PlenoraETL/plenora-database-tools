#!/usr/bin/env python3
"""Genera `docs/STATO.md` leggendo il codice, e verifica che sia aggiornato.

Conteggi dei test, comandi e capability vivono nel codice. Questo generatore
li rende senza duplicarli nel Markdown; in modalita `--check` pretende che una
rigenerazione non produca differenze.

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
    from scripts.live_inventory import annotated_tests
    from scripts.mysql_inventory import collect
    from scripts.phase0_validate import ACTIVE_MAJOR
except ModuleNotFoundError:  # esecuzione diretta: python scripts/...
    from live_inventory import annotated_tests
    from mysql_inventory import collect
    from phase0_validate import ACTIVE_MAJOR

TARGET = ROOT / "docs" / "STATO.md"

# Dove vive la dichiarazione delle capability di ciascun provider pubblico.
# `impl` seleziona il blocco quando un file ne contiene piu d'uno: il profilo
# MySQL e quello MariaDB stanno nello stesso modulo, e il terzo profilo di quel
# file esiste solo per i test.
# Ogni riga: come si chiama il prodotto, il crate, dove sta la dichiarazione
# di capability, come selezionarla se il file ne contiene piu d'una, il tipo
# provider che dovrebbe pubblicarla, e il profilo che quel tipo deve
# dichiarare — `None` per i crate che non hanno profili.
#
CAPABILITY_SOURCES = (
    (
        "PostgreSQL",
        "crates/plenora-db-postgres",
        "crates/plenora-db-postgres/src/catalog/capabilities.rs",
        None,
        "PostgresProvider",
        None,
    ),
    (
        "MySQL",
        "crates/plenora-db-mysql",
        "crates/plenora-db-mysql/src/profile.rs",
        "impl ProductProfile for MysqlProfile",
        "MysqlProvider",
        "MYSQL_PROFILE",
    ),
    (
        "MariaDB",
        "crates/plenora-db-mysql",
        "crates/plenora-db-mysql/src/profile.rs",
        "impl ProductProfile for MariadbProfile",
        "MariadbProvider",
        "MARIADB_PROFILE",
    ),
    (
        "SQL Server",
        "crates/plenora-db-sqlserver",
        "crates/plenora-db-sqlserver/src/provider.rs",
        None,
        "SqlServerProvider",
        None,
    ),
)
CAPABILITY_GROUPS = (
    ("reads", "ReadCapabilities"),
    ("writes", "WriteCapabilities"),
    ("transactions", "TransactionCapabilities"),
)
PROFILE_STATIC = r'\b([A-Z][A-Z0-9_]*_PROFILE)\b'
# Un tipo che implementa il trait dei provider.
PROVIDER_IMPL = r'\bimpl Provider for ([A-Za-z][A-Za-z0-9_]*)'
# Un `pub use` della radice del crate.
REEXPORT = r'pub use ([^;]+);'
# La dichiarazione del profilo pubblicato da un tipo provider, in due meta
# che si concatenano attorno al nome del tipo: la dichiarazione appartiene
# a lui, non al crate.
DECLARATION_HEAD = r"impl PublishedProfile for "
DECLARATION_TAIL = (
    r"\s*\{\s*const PROFILE: &\s*(?:'static\s*)?dyn "
    # Il profilo puo essere nominato per intero — `crate::profile::X` — e si
    # cattura comunque il nome, che e cio che identifica la dichiarazione.
    r"ProductProfile = &(?:[a-z_][a-z0-9_]*::)*([A-Z][A-Z0-9_]*);"
)
# Una voce del catalogo: nome, e la feature che lo compila se ce n'e una.
COMMAND_ENTRY = r'\("([a-z][a-z0-9-]+)", (?:Some\("([a-z]+)"\)|None)\)'
PROVIDER_TEST_CRATES = (
    ("PostgreSQL", "plenora-db-postgres"),
    ("MySQL + MariaDB", "plenora-db-mysql"),
    ("SQL Server", "plenora-db-sqlserver"),
)


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


def crate_sources(crate: str) -> list[tuple[str, str]]:
    return [
        (path.as_posix(), path.read_text(encoding="utf-8"))
        for path in sorted((ROOT / crate / "src").rglob("*.rs"))
    ]


def provider_types(crate: str) -> set[str]:
    """I tipi che implementano il trait dei provider."""
    found: set[str] = set()
    for _, source in crate_sources(crate):
        found.update(re.findall(PROVIDER_IMPL, source))
    return found


def exported_providers(crate: str) -> set[str]:
    """I provider che il crate esporta dalla propria radice.

    Non basta `pub struct`: un tipo pubblico dentro un modulo privato non
    esce dal crate, e chiamarlo esportato sarebbe la stessa promessa
    eccessiva di prima. La radice e `lib.rs`, e conta cio che ci passa —
    dichiarato li, oppure riesportato con un `pub use`.
    """
    root = (ROOT / crate / "src" / "lib.rs").read_text(encoding="utf-8")
    exported: set[str] = set()
    for name in provider_types(crate):
        declared = f"pub struct {name}" in root
        reexported = any(
            name in line
            for line in re.findall(REEXPORT, root, re.DOTALL)
        )
        if declared or reexported:
            exported.add(name)
    return exported


def declared_profile(crate: str, provider_type: str) -> str | None:
    """Il profilo che **quel tipo** dichiara pubblicato, se lo dichiara.

    Associata al tipo e non al crate, perche questo crate pubblichera due
    provider: una dichiarazione per crate ne descriverebbe uno solo, e due
    costanti in moduli diversi si sarebbero risolte prendendo la prima
    trovata — cioe a caso.

    La dichiarazione e una riga sola, e il renderer si limita a leggerla: che
    il costruttore la usi davvero lo prova un test Rust, non un'analisi del
    sorgente fatta da qui.
    """
    declaration = re.compile(
        DECLARATION_HEAD + re.escape(provider_type) + DECLARATION_TAIL
    )
    for _, source in crate_sources(crate):
        match = declaration.search(source)
        if match:
            return match.group(1)
    return None


def capability_matrix() -> dict[str, dict[str, object]]:
    matrix: dict[str, dict[str, object]] = {}
    for provider, crate, source, marker, provider_type, profile in CAPABILITY_SOURCES:
        exported = provider_type in exported_providers(crate)
        if profile is None:
            published = exported
        else:
            published = exported and profile == declared_profile(crate, provider_type)
        matrix[provider] = {
            "published": published,
            "groups": {
                name: capability_fields(source, name, kind, marker)
                for name, kind in CAPABILITY_GROUPS
            },
        }
    verify_every_exported_provider_declares_its_profile()
    return matrix


def verify_every_exported_provider_declares_its_profile() -> None:
    """Un provider esportato senza dichiarazione non e descrivibile.

    Il caso si presenta appena qualcuno esporta un provider nuovo e si
    dimentica l'`impl PublishedProfile`: senza questa riga il renderer lo
    tratterebbe come non pubblicato, cioe direbbe una cosa falsa in silenzio.
    Meglio fermarsi e chiedere la dichiarazione.
    """
    profiled = {
        crate for _, crate, _, _, _, profile in CAPABILITY_SOURCES if profile
    }
    known = {
        (crate, provider_type)
        for _, crate, _, _, provider_type, _ in CAPABILITY_SOURCES
    }
    for crate in sorted(profiled):
        for provider_type in sorted(exported_providers(crate)):
            if (crate, provider_type) not in known:
                raise RenderError(
                    f"{crate} esporta {provider_type}, che non compare fra i "
                    f"prodotti descritti"
                )
            if declared_profile(crate, provider_type) is None:
                raise RenderError(
                    f"{provider_type} e esportato ma non dichiara un "
                    f"PublishedProfile: quale profilo pubblichi non si indovina"
                )


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


def cli_subcommands() -> list[tuple[str, str]]:
    """I sub-comandi, dal catalogo che il CLI espone.

    Non da tutti i rami `"..." =>` del sorgente: quella lettura prendeva anche
    gli arm che traducono il nome di un provider, e produceva sei comandi che
    il binario non ha. `COMMAND_CATALOGUE` e l'elenco che l'aiuto stampa e che
    il dispatch riconosce, e porta con se la feature che li compila.
    """
    main = (ROOT / "crates" / "plenora-database-cli" / "src" / "main.rs").read_text(
        encoding="utf-8"
    )
    marker = "const COMMAND_CATALOGUE"
    if marker not in main:
        raise RenderError("catalogo dei comandi del CLI non trovato")
    body = main[main.index("[", main.index(marker)) :]
    body = body[: body.index("];") + 1]
    found = [
        (name, feature or "sempre")
        for name, feature in re.findall(COMMAND_ENTRY, body)
    ]
    if not found:
        raise RenderError("catalogo dei comandi del CLI vuoto")
    return sorted(found)


def test_inventory() -> list[tuple[str, int]]:
    observed = collect()
    return [(family, len(observed[family])) for family in sorted(observed)]


def provider_test_inventory() -> list[tuple[str, int]]:
    """Test Rust annotati nei crate che implementano i quattro prodotti.

    MySQL e MariaDB condividono intenzionalmente lo stesso crate e molte suite:
    separarli contando i nomi sarebbe un fatto inventato, quindi restano una
    riga sola. Il conteggio segue gli attributi Rust, non convenzioni sui nomi.
    """

    inventory: list[tuple[str, int]] = []
    for label, crate in PROVIDER_TEST_CRATES:
        root = ROOT / "crates" / crate
        count = sum(
            len(annotated_tests(path.read_text(encoding="utf-8")))
            for path in sorted(root.rglob("*.rs"))
        )
        inventory.append((label, count))
    return inventory


def table(header: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |"]
    lines.append("|" + "|".join(" --- " for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def render() -> str:
    matrix = capability_matrix()
    providers = [name for name, *_ in CAPABILITY_SOURCES]
    unpublished = [
        name for name in providers if not matrix[name]["published"]
    ]
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
        "E l'unica nel worktree: le major precedenti stanno in Git, e nessun",
        "file qui dentro le referenzia. Il gate offline fallisce se una di esse",
        "torna nell'albero di lavoro, o se un riferimento la nomina.",
        "",
        "## Capability dichiarate",
        "",
        "Cio che ciascuna dichiarazione di capability contiene, letto da dove",
        "e scritta. Un valore che non e un letterale — `spatial` su PostgreSQL",
        "dipende dalla presenza di PostGIS — resta l'espressione sorgente:",
        "risolverla qui sarebbe un'affermazione che il codice non fa.",
        "",
    ]
    if unpublished:
        lines += [
            "**Non tutte sono raggiungibili.** "
            + ", ".join(f"`{name}`" for name in unpublished)
            + " ha una dichiarazione nel crate ma nessun costruttore pubblico",
            "la seleziona: e un profilo interno, e non esiste un provider che",
            "un consumatore possa istanziare. La colonna e qui perche la",
            "dichiarazione esiste, non perche la si possa usare — ed e marcata",
            "nell'intestazione.",
            "",
        ]
    header = [
        name if matrix[name]["published"] else f"{name} (non pubblicato)"
        for name in providers
    ]
    for group, _ in CAPABILITY_GROUPS:
        names: list[str] = []
        for provider in providers:
            for field in matrix[provider]["groups"][group]:
                if field not in names:
                    names.append(field)
        lines += [f"### `{group}`", ""]
        lines += table(
            [group] + header,
            [
                [f"`{field}`"]
                + [
                    f"`{matrix[provider]['groups'][group].get(field, '—')}`"
                    for provider in providers
                ]
                for field in names
            ],
        )
        lines.append("")
    lines += [
        "## Sub-comandi del CLI",
        "",
        "Dal catalogo che il binario espone. La feature e quella che li",
        "compila: un comando la cui feature non e stata compilata esiste nel",
        "progetto ma non in quel binario, e il CLI lo dice invece di stampare",
        "l'aiuto.",
        "",
    ]
    lines += table(
        ["comando", "feature"],
        [[f"`{name}`", f"`{feature}`"] for name, feature in cli_subcommands()],
    )
    lines += [
        "",
        "## Inventario dei test dei provider",
        "",
        "Conteggio delle funzioni Rust annotate come test nei crate provider.",
        "MySQL e MariaDB condividono un crate e restano quindi una riga sola.",
        "",
    ]
    lines += table(
        ["provider", "test Rust"],
        [[label, str(count)] for label, count in provider_test_inventory()],
    )
    lines += [
        "",
        "### Famiglie del gate MySQL",
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
