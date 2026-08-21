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
    (
        "PostgreSQL",
        "crates/plenora-db-postgres",
        "crates/plenora-db-postgres/src/catalog/capabilities.rs",
        None,
        None,
    ),
    (
        "MySQL",
        "crates/plenora-db-mysql",
        "crates/plenora-db-mysql/src/profile.rs",
        "impl ProductProfile for MysqlProfile",
        "MYSQL_PROFILE",
    ),
    (
        "MariaDB",
        "crates/plenora-db-mysql",
        "crates/plenora-db-mysql/src/profile.rs",
        "impl ProductProfile for MariadbProfile",
        "MARIADB_PROFILE",
    ),
    (
        "SQL Server",
        "crates/plenora-db-sqlserver",
        "crates/plenora-db-sqlserver/src/provider.rs",
        None,
        None,
    ),
)
CAPABILITY_GROUPS = (("reads", "ReadCapabilities"), ("writes", "WriteCapabilities"))
PROFILE_STATIC = r'\b([A-Z][A-Z0-9_]*_PROFILE)\b'
# Il nome che segue `fn `.
NAME = r'([a-z_][a-z0-9_]*)'
# Una funzione pubblica dichiarata in un `impl`, nelle sue tre forme.
PUBLIC_FN = r'\bpub (?:async |const )?fn ([a-z_][a-z0-9_]*)'
# Un tipo che implementa il trait dei provider.
PROVIDER_IMPL = r'\bimpl Provider for ([A-Za-z][A-Za-z0-9_]*)'
# Una chiamata, qualunque sia il percorso con cui e scritta.
CALLED_FN = r'\b([a-z_][a-z0-9_]*)\s*\('
# Una voce del catalogo: nome, e la feature che lo compila se ce n'e una.
COMMAND_ENTRY = r'\("([a-z][a-z0-9-]+)", (?:Some\("([a-z]+)"\)|None)\)'


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


def function_bodies(source: str) -> dict[str, str]:
    """Nome e corpo di ogni funzione del file, qualunque sia la visibilita.

    Anche quelle private: e il punto. Un costruttore pubblico che delega a un
    helper privato raggiunge cio che l'helper raggiunge, e fermarsi al primo
    livello direbbe che non raggiunge niente.
    """
    bodies: dict[str, str] = {}
    start = 0
    while True:
        found = source.find("fn ", start)
        if found < 0:
            break
        start = found + 3
        if found > 0 and (source[found - 1].isalnum() or source[found - 1] == "_"):
            continue
        rest = source[found + 3 :]
        name = re.match(NAME, rest)
        if not name:
            continue
        opening = source.find("{", found)
        semicolon = source.find(";", found)
        if opening < 0 or (0 <= semicolon < opening):
            # Firma senza corpo: un metodo di trait.
            continue
        try:
            bodies[name.group(1)] = braced_body(source, opening)
        except RenderError:
            continue
    return bodies


def public_constructors(source: str, type_name: str) -> set[str]:
    """Le funzioni pubbliche dichiarate in un `impl` di quel tipo.

    `pub fn`, `pub async fn` e `pub const fn`: tutte e tre, perche tutte e tre
    sono chiamabili da fuori. `pub(crate)` no — quella e la porta di servizio,
    ed e esattamente la differenza che questo controllo deve vedere.
    """
    names: set[str] = set()
    for marker in (f"impl {type_name} ", f"impl {type_name}{{"):
        start = 0
        while True:
            found = source.find(marker, start)
            if found < 0:
                break
            start = found + 1
            opening = source.find("{", found)
            if opening < 0:
                break
            block = braced_body(source, opening)
            names.update(re.findall(PUBLIC_FN, block))
    return names


def exported_providers(crate: str) -> set[str]:
    """I tipi che implementano `Provider` e che il crate esporta davvero.

    Un tipo con un `impl Provider` ma dichiarato `pub(crate)` non e un
    provider che qualcuno possa istanziare: e un dettaglio interno.
    """
    exported: set[str] = set()
    for _, source in crate_sources(crate):
        for name in re.findall(PROVIDER_IMPL, source):
            declaration = f"pub struct {name}"
            if any(declaration in other for _, other in crate_sources(crate)):
                exported.add(name)
    return exported


def reachable_profiles(crate: str) -> set[str]:
    """I profili raggiungibili da un costruttore pubblico di un provider.

    I corpi si tengono **per file**, e una chiamata si risolve prima nel file
    da cui parte: `new` esiste in mezza dozzina di moduli di questo crate, e
    un dizionario unico per l'intero crate faceva vincere l'ultimo letto —
    la visita partiva dal corpo sbagliato e non trovava niente.

    La risoluzione resta un'approssimazione: non e il resolver di Rust, e un
    nome che esiste in piu file oltre a quello di partenza viene seguito in
    tutti. L'approssimazione allarga cio che si raggiunge, mai il contrario,
    quindi puo dire "pubblicato" di troppo e mai "interno" di troppo — ed e il
    verso giusto per una guardia che deve accorgersi di una superficie aperta
    per sbaglio.
    """
    sources = crate_sources(crate)
    bodies: dict[str, dict[str, str]] = {
        name: function_bodies(source) for name, source in sources
    }
    providers = exported_providers(crate)

    frontier: list[tuple[str, str]] = []
    for type_name in providers:
        for name, source in sources:
            frontier.extend(
                (name, function) for function in public_constructors(source, type_name)
            )

    seen: set[tuple[str, str]] = set()
    found: set[str] = set()
    while frontier:
        where, function = frontier.pop()
        if (where, function) in seen:
            continue
        seen.add((where, function))
        body = bodies.get(where, {}).get(function)
        if body is None:
            continue
        found.update(re.findall(PROFILE_STATIC, body))
        for called in re.findall(CALLED_FN, body):
            if called in bodies.get(where, {}):
                frontier.append((where, called))
                continue
            frontier.extend(
                (other, called) for other in bodies if called in bodies[other]
            )
    return found


def capability_matrix() -> dict[str, dict[str, object]]:
    matrix: dict[str, dict[str, object]] = {}
    for provider, crate, source, marker, profile in CAPABILITY_SOURCES:
        reachable = reachable_profiles(crate)
        published = bool(exported_providers(crate)) and (
            profile is None or profile in reachable
        )
        matrix[provider] = {
            "published": published,
            "groups": {
                name: capability_fields(source, name, kind, marker)
                for name, kind in CAPABILITY_GROUPS
            },
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


def table(header: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |"]
    lines.append("|" + "|".join(" --- " for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def render() -> str:
    matrix = capability_matrix()
    providers = [name for name, _, _, _, _ in CAPABILITY_SOURCES]
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
