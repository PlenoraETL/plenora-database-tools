#!/usr/bin/env python3
"""Ogni dipendenza esterna dei manifest Cargo ha un pin esatto e un perche.

Regola: una dipendenza da un registro entra con versione esatta `=x.y.z` e
con un commento di motivazione sulla riga stessa o immediatamente sopra
(un blocco di commento contiguo vale per la riga che lo segue). Le
dipendenze `path` e quelle ereditate con `workspace = true` sono escluse:
le prime non hanno una versione da fissare, le seconde la prendono da
`[workspace.dependencies]`, che questa guardia controlla.

Fuori perimetro, dichiarato: `vendor/oracle-rs/Cargo.toml` e il manifest
upstream con intervalli, le cui versioni effettive fissa `Cargo.lock` (il
motivo e scritto accanto a `[patch.crates-io]` nel manifest del workspace).

Controlla anche che `rust-version` del workspace coincida con la toolchain
di `rust-toolchain.toml`: la MSRV dichiarata e quella che la CI compila.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
EXACT = re.compile(r"^=\d+\.\d+\.\d+$")
SECTION = re.compile(r"^\[\[?(?P<name>[^\]]+)\]\]?\s*(#.*)?$")
TRAILING_COMMENT = re.compile(r'[}"\]]\s*#')
ENTRY = re.compile(r"^(?P<key>[A-Za-z0-9_-]+)(?P<dotted>\.[A-Za-z0-9_.-]+)?\s*=")


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    message: str

    def __str__(self) -> str:
        return f"{self.path.as_posix()}:{self.line}: {self.message}"


def manifests(root: Path = ROOT) -> list[Path]:
    return [
        root / "Cargo.toml",
        *sorted((root / "crates").glob("*/Cargo.toml")),
        root / "fuzz" / "Cargo.toml",
    ]


def is_dependency_section(name: str) -> bool:
    return name == "workspace.dependencies" or name.split(".")[-1] in {
        "dependencies",
        "dev-dependencies",
        "build-dependencies",
    }


def check_manifest(path: Path, root: Path = ROOT) -> list[Violation]:
    text = path.read_text(encoding="utf-8")
    relative = path.relative_to(root)
    violations: list[Violation] = []
    section = ""
    lines = text.splitlines()
    for index, raw in enumerate(lines):
        line = raw.strip()
        header = SECTION.match(line)
        if header:
            section = header.group("name")
            if section.startswith(("dependencies.", "dev-dependencies.")) or (
                ".dependencies." in section
            ):
                violations.append(
                    Violation(relative, index + 1, "tabella di dipendenza non supportata: usare una tabella inline")
                )
            continue
        if not is_dependency_section(section) or not line or line.startswith("#"):
            continue
        entry = ENTRY.match(line)
        if not entry:
            continue
        name = entry.group("key")
        if entry.group("dotted"):
            # `nome.workspace = true`: eredita dal workspace.
            continue
        value = tomllib.loads(line)[name]
        if isinstance(value, dict):
            if value.get("workspace") is True or "path" in value:
                continue
            version = value.get("version")
        else:
            version = value
        if not isinstance(version, str) or not EXACT.match(version):
            violations.append(
                Violation(relative, index + 1, f"{name}: versione non esatta (serve =x.y.z)")
            )
        commented = TRAILING_COMMENT.search(raw) is not None
        if not commented:
            previous = lines[index - 1].strip() if index else ""
            commented = previous.startswith("#")
        if not commented:
            violations.append(
                Violation(relative, index + 1, f"{name}: pin senza commento di motivazione")
            )
    return violations


def check_msrv(root: Path = ROOT) -> list[Violation]:
    workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    toolchain = tomllib.loads((root / "rust-toolchain.toml").read_text(encoding="utf-8"))
    declared = workspace["workspace"]["package"]["rust-version"]
    channel = toolchain["toolchain"]["channel"]
    if declared != channel:
        return [
            Violation(
                Path("Cargo.toml"),
                1,
                f"rust-version {declared} diverso dalla toolchain fissata {channel}",
            )
        ]
    return []


def scan(root: Path = ROOT) -> list[Violation]:
    violations = check_msrv(root)
    for path in manifests(root):
        violations.extend(check_manifest(path, root))
    return violations


def main() -> int:
    violations = scan()
    for violation in violations:
        print(violation, file=sys.stderr)
    if violations:
        return 1
    print(f"pin: {len(manifests())} manifest controllati, nessuna violazione")
    return 0


if __name__ == "__main__":
    sys.exit(main())
