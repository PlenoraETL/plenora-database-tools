#!/usr/bin/env python3
"""Da dove arriva il package che la suite sta per importare.

Il runner del SDK costruisce un wheel e lo installa: cio che pytest verifica
deve essere **quel** wheel. Non e garantito dal fatto di averlo installato.
Un `PYTHONPATH` verso `crates/plenora-database-py/python`, una `cwd` dentro il
source tree, o un `tests/__init__.py` che fa risalire l'inserimento di
`sys.path` di pytest fino alla directory del package: ognuna di queste tre
strade mette la copia sorgente davanti a quella installata, e la suite passa
descrivendo codice che il wheel non contiene.

La differenza non e visibile nel risultato — i test sono gli stessi e passano
uguale — quindi va osservata direttamente, prima che pytest parta: questo
modulo chiede a `importlib` da dove verrebbero `plenora_database` e il suo
modulo nativo, e fallisce se la risposta non e una directory di
`site-packages` di questo interprete.

Stampa una riga sola, che il runner rilegge per il verdetto:

    PLENORA_SDK_GATE_ORIGIN <dir del package> <path del nativo> <sha256>

Il digest e quello del `.so` **installato**: e il file che l'interprete
carica, ed e l'unica cosa che dice quale codice nativo ha risposto ai test.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import sys
import sysconfig
from pathlib import Path

ORIGIN_MARKER = "PLENORA_SDK_GATE_ORIGIN "
PACKAGE = "plenora_database"
NATIVE = f"{PACKAGE}._native"


def site_directories() -> list[Path]:
    """Le directory dove `pip install` deposita i pacchetti di questo Python.

    Sono due e non una: `purelib` per il Python puro, `platlib` per cio che
    porta codice compilato. In un'installazione tipica coincidono, ma darne
    per scontata una sola renderebbe la guardia dipendente dal layout
    dell'immagine invece che da `sysconfig`.
    """

    paths = sysconfig.get_paths()
    return sorted({Path(paths[key]).resolve() for key in ("purelib", "platlib")})


def module_origin(name: str) -> Path:
    """Il file da cui `import <name>` caricherebbe il modulo.

    `find_spec` risolve senza eseguire il modulo cercato — per `_native`
    importa il package che lo contiene, che e comunque cio che la suite fara.

    # Raises

    `RuntimeError` se il modulo non e importabile o non ha un file: senza il
    wheel installato la suite fallirebbe piu tardi, in collezione, con un
    `ImportError` che non nomina la causa.
    """

    specification = importlib.util.find_spec(name)
    if specification is None or not specification.origin:
        raise RuntimeError(
            f"modulo {name} non importabile: il wheel non risulta installato"
        )
    return Path(specification.origin).resolve()


def assert_installed(name: str, origin: Path, directories: list[Path]) -> None:
    """`origin` deve stare dentro una delle directory di installazione.

    # Raises

    `RuntimeError` nominando il percorso trovato: e l'unico dato che dice
    *quale* copia avrebbe vinto, e senza di esso il messaggio non basterebbe
    a distinguere un source tree in `sys.path` da un wheel mai installato.
    """

    if any(origin.is_relative_to(directory) for directory in directories):
        return
    raise RuntimeError(
        f"{name} non arriva da site-packages ma da {origin}: la suite "
        f"verificherebbe una copia sorgente invece del wheel costruito "
        f"(installazioni note: {[str(entry) for entry in directories]})"
    )


def assert_standard_wheel_excludes_db2_runtime() -> None:
    """Il wheel generale espone DB2 ma non deve collegare ODBC di nascosto.

    Un TLS mode volutamente invalido impedisce alla variante DB2 reale di
    arrivare alla rete. La variante standard deve invece fermarsi prima con
    l'errore pubblico `Unsupported`: e il contratto che rende distinguibile
    un wheel generale dall'artefatto DB2 dedicato.

    # Raises

    `RuntimeError` se la factory manca, accetta la chiamata o restituisce una
    categoria diversa.
    """

    package = importlib.import_module(PACKAGE)
    try:
        package.connect_db2(
            "localhost",
            "probe",
            "probe",
            "probe",
            tls_mode="sdk_wheel_probe_invalid",
        )
    except package.PlenoraUnsupportedError:
        return
    except package.PlenoraError as error:
        raise RuntimeError(
            "la factory DB2 del wheel standard non ha fallito come Unsupported"
        ) from error
    raise RuntimeError("la factory DB2 del wheel standard ha accettato la chiamata")


def main() -> int:
    directories = site_directories()
    # Il package si verifica prima di risolvere il nativo: risolvere `_native`
    # importa il package che lo contiene, e se quello e la copia sorgente
    # l'errore che ne esce parla di un `.so` mancante invece che della copia
    # sbagliata.
    package = module_origin(PACKAGE)
    assert_installed(PACKAGE, package, directories)
    native = module_origin(NATIVE)
    assert_installed(NATIVE, native, directories)
    assert_standard_wheel_excludes_db2_runtime()
    digest = hashlib.sha256(native.read_bytes()).hexdigest()
    print(f"{ORIGIN_MARKER}{package.parent} {native} {digest}")
    return 0


if __name__ == "__main__":
    try:
        status = main()
    except RuntimeError as error:
        print(f"sdk wheel probe: {error}", file=sys.stderr)
        status = 1
    raise SystemExit(status)
