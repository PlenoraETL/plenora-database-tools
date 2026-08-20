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

from scripts.check_session_matrix import (  # noqa: E402
    EVIDENCE,
    markdown,
    preflight,
    verdict,
)

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


def start_fixtures(started: list[str]) -> None:
    """Avvia i riferimenti, annotando via via quali sono partiti.

    L'elenco cresce mentre si avanza, e non alla fine: se il secondo compose
    fallisce, il chiamante deve sapere che il primo e acceso. Prima non lo
    sapeva, e un fallimento a meta lasciava container su senza diagnostica ne
    pulizia.

    `--wait` e cio che rende la campagna deterministica: senza, la prima
    misura partirebbe contro un server che sta ancora inizializzando, e il
    fallimento somiglierebbe a una divergenza.
    """

    for file in COMPOSE_FILES:
        compose(file, "config", "--quiet")
        started.append(file)
        # Prima si spegne, poi si accende. Non e prudenza: il compose genera
        # il materiale TLS con un container one-shot, e un `up` su uno stack
        # gia acceso lo rigenera **sotto** i server che lo stanno usando. La
        # prima corsa reale e morta cosi, con un `BadSignature` che somigliava
        # a un problema di rete. Da uno stato noto, invece, la campagna misura
        # il riferimento dichiarato e non l'accumulo di corse precedenti.
        compose(file, "down", "-v")
        # Mai `--remove-orphans`: ogni compose dichiara il proprio progetto, e
        # su una macchina con piu riferimenti accesi quel flag cancella i
        # container degli altri provider.
        compose(file, "up", "-d", "--wait")


def stop_fixtures(started: list[str]) -> list[str]:
    """Spegne cio che era stato avviato, senza fermarsi al primo errore.

    Restituisce i fallimenti invece di sollevarli: la pulizia non deve
    mascherare la ragione per cui la campagna e finita qui. Un `down` che
    fallisce e un problema, ma e il secondo.
    """

    failures = []
    for file in reversed(started):
        try:
            compose(file, "down", "-v")
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            # `OSError` sta nell'elenco perche `subprocess.run` lo solleva
            # quando l'eseguibile non c'e: senza, un `docker` sparito dal PATH
            # durante la corsa avrebbe fatto risalire quell'errore al posto
            # della ragione per cui la campagna e finita li. La stessa
            # famiglia che gia cattura la diagnostica, per la stessa ragione.
            failures.append(f"{file}: {error}")
    return failures


def diagnostics(destination: Path, started: list[str]) -> None:
    """Cosa serve a capire un fallimento, raccolto prima di spegnere tutto.

    Best-effort **per artefatto**, non in blocco: con un solo `try` attorno a
    tutto, un `docker ps` che fallisce portava via anche i log, e chi guardava
    il runner si ritrovava senza la cosa che stava cercando. Ogni pezzo si
    tenta per conto suo, e cio che non riesce lo dice.
    """

    def attempt(what: str, produce) -> None:
        try:
            produce()
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            print(f"diagnostica: {what} non raccolto ({error})", file=sys.stderr)

    attempt("cartella", lambda: destination.mkdir(parents=True, exist_ok=True))
    attempt(
        "stato dei container",
        lambda: (destination / "containers.txt").write_text(
            run(["docker", "ps", "-a"], capture=True), encoding="utf-8"
        ),
    )
    for file in started:
        name = Path(file).stem.replace("docker-compose.", "")
        attempt(
            f"log di {name}",
            lambda file=file, name=name: (destination / f"{name}.log").write_text(
                compose(file, "logs", "--no-color", capture=True), encoding="utf-8"
            ),
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

    # Il preflight sull'albero **prima** di Docker: scoprire un albero sporco
    # con tre server accesi e uno spreco, e l'affermazione che il controllo
    # precede Docker sarebbe stata falsa. `verdict` lo rifa — e giusto che lo
    # faccia, perche si usa anche da solo — ma qui e gia deciso.
    preflight()

    started: list[str] = []
    try:
        # L'avvio sta dentro il tentativo: se il secondo compose fallisce, il
        # primo e gia acceso e va spento. Prima restava su, senza nemmeno i
        # log per capire perche.
        start_fixtures(started)
        document = verdict()
    except BaseException:
        if parsed.diagnostics is not None:
            diagnostics(parsed.diagnostics, started)
        # Qui un errore c'e gia, ed e quello che il chiamante deve vedere: la
        # pulizia si tenta e i suoi fallimenti si stampano, ma non sostituiscono
        # la ragione per cui la campagna e finita qui.
        if not parsed.keep_fixtures:
            for failure in stop_fixtures(started):
                print(f"pulizia incompleta: {failure}", file=sys.stderr)
        raise

    # Sul percorso riuscito, invece, un `down` fallito **e** l'errore. Stamparlo
    # e proseguire faceva finire la campagna in verde lasciando tre server
    # accesi sul runner: il gate avrebbe dichiarato che tutto va bene mentre
    # lasciava dietro di se cio che la corsa successiva trovera.
    if not parsed.keep_fixtures:
        failures = stop_fixtures(started)
        if failures:
            raise RuntimeError(
                "la misura e riuscita ma la pulizia no: " + "; ".join(failures)
            )

    EVIDENCE.write_text(markdown(document), encoding="utf-8", newline="\n")
    print(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=1))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except RuntimeError as error:
        print(f"session campaign FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
