#!/usr/bin/env python3
"""Campagna live della matrice di sessione: fixture, misura, verdetto.

La matrice e una prova permanente solo se qualcuno la riesegue. Il self-test
statico verifica il **giudizio** del runner su documenti costruiti a mano, e
questo e cio che serve su una pull request; ma se domani cambiasse
`SESSION_BOOTSTRAP_SQL` o `START TRANSACTION`, il documento resterebbe quello
di ieri e nessun controllo se ne accorgerebbe. Solo una corsa contro i tre
server reali lo scopre.

Il ciclo di vita sta qui e non nello YAML per due ragioni. La prima e che si
riproduce in locale con lo stesso comando che gira in CI, ed e la sola forma
in cui una campagna resta verificabile da chi non ha accesso ai runner. La
seconda e che le condizioni — quali fixture, in quale ordine, cosa aspettare —
sono decisioni, e le decisioni scritte in YAML non hanno ne test ne
revisione separata.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.check_session_matrix import EVIDENCE, markdown, verdict  # noqa: E402

# I due compose che accendono i tre riferimenti della matrice. L'ordine non
# conta — sono reti separate — ma l'elenco si: una campagna che ne avviasse
# uno solo misurerebbe due server su tre e il runner fallirebbe sul digest,
# che e il modo giusto di accorgersene ma non il piu chiaro.
COMPOSE_FILES = ("docker-compose.mysql.yml", "docker-compose.mariadb.yml")


def run(command: list[str], *, capture: bool = False) -> str:
    """Esegue un comando dalla radice del repository.

    # Raises

    `RuntimeError` se fallisce: la campagna non prosegue su una fixture che
    non e salita, perche cio che misurerebbe non sarebbe il riferimento.
    """

    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=capture,
        encoding="utf-8",
        errors="replace",
        timeout=1_800,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"comando fallito ({completed.returncode}): {' '.join(command)}")
    return completed.stdout if capture else ""


def compose(file: str, *arguments: str, capture: bool = False) -> str:
    return run(["docker", "compose", "-f", file, *arguments], capture=capture)


def start_fixtures() -> None:
    """Avvia i tre riferimenti e aspetta che siano sani.

    `--wait` e cio che rende la campagna deterministica: senza, la prima
    misura partirebbe contro un server che sta ancora inizializzando, e il
    fallimento somiglierebbe a una divergenza.
    """

    for file in COMPOSE_FILES:
        compose(file, "config", "--quiet")
        # Mai `--remove-orphans`: ogni compose dichiara il proprio progetto, e
        # su una macchina con piu riferimenti accesi quel flag cancella i
        # container degli altri provider.
        compose(file, "up", "-d", "--wait")


def stop_fixtures() -> None:
    for file in COMPOSE_FILES:
        compose(file, "down", "-v")


def diagnostics(destination: Path) -> None:
    """Cosa serve a capire un fallimento, salvato prima di spegnere tutto."""

    destination.mkdir(parents=True, exist_ok=True)
    (destination / "containers.txt").write_text(
        run(["docker", "ps", "-a"], capture=True), encoding="utf-8"
    )
    for file in COMPOSE_FILES:
        name = Path(file).stem.replace("docker-compose.", "")
        (destination / f"{name}.log").write_text(
            compose(file, "logs", "--no-color", capture=True), encoding="utf-8"
        )


def main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--keep-fixtures",
        action="store_true",
        help="lascia i container accesi (utile in locale fra due corse)",
    )
    parser.add_argument(
        "--diagnostics",
        type=Path,
        default=None,
        help="cartella in cui salvare log e stato dei container",
    )
    parsed = parser.parse_args(arguments)

    start_fixtures()
    try:
        document = verdict()
    except Exception:
        if parsed.diagnostics is not None:
            diagnostics(parsed.diagnostics)
        raise
    finally:
        if not parsed.keep_fixtures:
            stop_fixtures()

    EVIDENCE.write_text(markdown(document), encoding="utf-8", newline="\n")
    print(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=1))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except RuntimeError as error:
        print(f"session campaign FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
