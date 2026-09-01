#!/usr/bin/env python3
"""Assegna e verifica il build tag ``1db2`` del wheel runtime Db2 Linux."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path


UNTAGGED = re.compile(
    r"^(plenora_database)-([^-]+)-(cp310)-(abi3)-(linux_x86_64)\.whl$"
)
TAGGED = re.compile(
    r"^(plenora_database)-([^-]+)-(1db2)-(cp310)-(abi3)-(linux_x86_64)\.whl$"
)


def db2_wheel_name(name: str) -> str:
    """Restituisce il nome con build tag, rifiutando formati non qualificati."""

    match = UNTAGGED.fullmatch(name)
    if match is None:
        raise RuntimeError(f"wheel Db2 Linux con nome inatteso: {name}")
    distribution, version, python, abi, platform = match.groups()
    return f"{distribution}-{version}-1db2-{python}-{abi}-{platform}.whl"


def tagged_wheel(directory: Path) -> Path:
    """Trova l'unico wheel Db2 gia marcato nella directory."""

    wheels = sorted(directory.glob("*.whl"))
    tagged = [wheel for wheel in wheels if TAGGED.fullmatch(wheel.name)]
    if len(wheels) != 1 or len(tagged) != 1:
        raise RuntimeError(
            "directory Db2 deve contenere un solo wheel con build tag 1db2: "
            f"{[wheel.name for wheel in wheels]}"
        )
    return tagged[0]


def validate_release_tag(wheel: Path, *, event_name: str, ref_name: str) -> None:
    """Su una release, il tag deve dichiarare la versione nel filename."""

    if event_name != "release":
        return
    match = TAGGED.fullmatch(wheel.name)
    if match is None:
        raise RuntimeError(f"wheel Db2 senza build tag valido: {wheel.name}")
    expected = f"py-v{match.group(2)}"
    if ref_name != expected:
        raise RuntimeError(
            f"tag release Db2 inatteso: {ref_name or '<assente>'}; atteso {expected}"
        )


def tag_wheel(directory: Path) -> Path:
    """Rinomina l'unico wheel maturin aggiungendo il build tag Db2."""

    wheels = sorted(directory.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(
            f"atteso un wheel Db2, trovati {[wheel.name for wheel in wheels]}"
        )
    source = wheels[0]
    destination = source.with_name(db2_wheel_name(source.name))
    source.rename(destination)
    return tagged_wheel(directory)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verifica un wheel gia marcato senza rinominarlo",
    )
    arguments = parser.parse_args()
    wheel = tagged_wheel(arguments.directory) if arguments.check else tag_wheel(arguments.directory)
    validate_release_tag(
        wheel,
        event_name=os.environ.get("GITHUB_EVENT_NAME", ""),
        ref_name=os.environ.get("GITHUB_REF_NAME", ""),
    )
    print(wheel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
