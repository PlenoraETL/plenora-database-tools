#!/usr/bin/env python3
"""Il ciclo di vita delle fixture di una campagna live, in un posto solo.

Due campagne accendono gli stessi tre riferimenti per misurare cose diverse —
la semantica di sessione e l'evidenza del driver — e il modo di accenderli e
lo stesso. Tenerlo qui non e simmetria: le condizioni sono decisioni, e ogni
riga di questo file e stata scritta dopo che una corsa reale e morta senza
quella riga.

* `down -v` **prima** di `up`: il compose genera il materiale TLS con un
  container one-shot, e un `up` su uno stack gia acceso lo rigenera sotto i
  server che lo stanno usando. La prima corsa e morta cosi, con un
  `BadSignature` che somigliava a un problema di rete.
* `--wait`: senza, la prima misura parte contro un server che sta ancora
  inizializzando, e il fallimento somiglia a una divergenza.
* nessun flag che tocchi container fuori dal proprio progetto: ogni compose
  dichiara il proprio `name:`, e su una macchina con piu riferimenti accesi
  spegnere gli "orfani" significa spegnere gli altri provider. Il commento
  accanto a `start_fixtures` nomina il flag vietato — la regola si documenta
  dove si applica, e una guardia del repository verifica che non compaia in
  nessuna riga eseguibile.
* l'elenco di cio che e partito cresce mentre si avanza: se il secondo compose
  fallisce, il primo e gia acceso e va spento.
* la pulizia e best-effort sul percorso di fallimento — non deve sostituire la
  ragione per cui la campagna e finita li — e **bloccante** su quello riuscito,
  perche una campagna verde che lascia tre server accesi mente alla corsa
  successiva.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import TypeVar

ROOT = Path(__file__).resolve().parents[1]

Measure = TypeVar("Measure")


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


def start_fixtures(compose_files: Sequence[str], started: list[str]) -> None:
    """Avvia i riferimenti, annotando via via quali sono partiti."""

    for file in compose_files:
        compose(file, "config", "--quiet")
        started.append(file)
        # Prima si spegne, poi si accende: da uno stato noto la campagna misura
        # il riferimento dichiarato e non l'accumulo di corse precedenti.
        compose(file, "down", "-v")
        # Mai `--remove-orphans`: ogni compose dichiara il proprio progetto, e
        # su una macchina con piu riferimenti accesi quel flag cancella i
        # container degli altri provider.
        compose(file, "up", "-d", "--wait")


def stop_fixtures(started: Iterable[str]) -> list[str]:
    """Spegne cio che era stato avviato, senza fermarsi al primo errore.

    Restituisce i fallimenti invece di sollevarli: la pulizia non deve
    mascherare la ragione per cui la campagna e finita qui. Un `down` che
    fallisce e un problema, ma e il secondo.
    """

    failures = []
    for file in reversed(list(started)):
        try:
            compose(file, "down", "-v")
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            # `OSError` sta nell'elenco perche `subprocess.run` lo solleva
            # quando l'eseguibile non c'e: senza, un `docker` sparito dal PATH
            # durante la corsa avrebbe fatto risalire quell'errore al posto
            # della ragione per cui la campagna e finita li.
            failures.append(f"{file}: {error}")
    return failures


def diagnostics(destination: Path, started: Iterable[str]) -> None:
    """Cosa serve a capire un fallimento, raccolto prima di spegnere tutto.

    Best-effort **per artefatto**, non in blocco: con un solo `try` attorno a
    tutto, un `docker ps` che fallisce portava via anche i log, e chi guardava
    il runner si ritrovava senza la cosa che stava cercando.
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


def confirm(ended_at: object, started_at: object) -> None:
    """Il repository non si e mosso durante la misura.

    # Cosa garantisce, e cosa no

    Due istantanee: una prima di accendere, una dopo l'ultima sonda. Vedono un
    `HEAD` diverso alla fine e una modifica **rimasta** nell'albero.

    Non vedono cio che e transitorio: un file modificato e poi rimesso identico,
    o un `HEAD` che va da A a B e torna ad A, danno due istantanee uguali. Su un
    runner pulito la distinzione non esiste — nessuno tocca l'albero mentre la
    campagna gira — e in locale la garanzia e quella scritta qui, non di piu.
    Chiuderla del tutto vorrebbe dire misurare da un checkout immutabile, cioe
    cambiare da dove la campagna legge il codice: e la mossa giusta, ed e
    grande abbastanza da meritare la propria.

    # Raises

    `RuntimeError` se le due istantanee differiscono.
    """

    if ended_at != started_at:
        raise RuntimeError(
            f"il repository e cambiato durante la misura: {started_at} -> {ended_at} "
            "— le sonde non parlano tutte dello stesso codice"
        )


def campaign(
    *,
    compose_files: Sequence[str],
    preflight: Callable[[], object],
    measure: Callable[[], Measure],
    diagnostics_directory: Path | None,
    keep_fixtures: bool,
) -> Measure:
    """Accende, misura, spegne — con l'ordine che conta.

    Il preflight sta **prima** di Docker: scoprire una precondizione violata
    con tre server accesi e uno spreco, e l'affermazione che il controllo
    precede Docker sarebbe falsa.

    # Raises

    Qualunque cosa sollevino `preflight`, `measure` o il postflight, dopo aver
    raccolto la diagnostica e tentato la pulizia. E `RuntimeError` se la misura
    riesce ma la pulizia no: li il `down` fallito **e** l'errore.

    Cosa il postflight garantisce, e cosa no, sta in [`confirm`].
    """

    started_at = preflight()

    started: list[str] = []
    try:
        # L'avvio sta dentro il tentativo: se il secondo compose fallisce, il
        # primo e gia acceso e va spento.
        start_fixtures(compose_files, started)
        measured = measure()
        # Il postflight sta **dentro** il tentativo. Fuori, un repository
        # cambiato durante la corsa faceva fallire la campagna saltando la
        # diagnostica e la pulizia: i tre server restavano accesi, ed e
        # esattamente la condizione in cui la corsa successiva misura un
        # accumulo invece di un riferimento.
        confirm(preflight(), started_at)
    except BaseException:
        if diagnostics_directory is not None:
            diagnostics(diagnostics_directory, started)
        if not keep_fixtures:
            for failure in stop_fixtures(started):
                print(f"pulizia incompleta: {failure}", file=sys.stderr)
        raise

    if not keep_fixtures:
        failures = stop_fixtures(started)
        if failures:
            raise RuntimeError("la misura e riuscita ma la pulizia no: " + "; ".join(failures))
    return measured
