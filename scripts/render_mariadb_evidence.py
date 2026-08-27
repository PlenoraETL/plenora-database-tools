#!/usr/bin/env python3
"""Genera l'inventario corrente delle prove MariaDB dai cataloghi eseguibili.

Il documento non conserva gli esiti di una corsa: quelli appartengono al
verdetto JSON e agli artifact del workflow. Conserva invece quali domande il
gate pone, su quali riferimenti e quali prove sono necessarie per sostenere le
capability pubblicate.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.check_mariadb_divergence import probes as sql_probes  # noqa: E402
from scripts.check_mariadb_driver import (  # noqa: E402
    EXPECTED_PROBES,
    OBSERVATION_ONLY_PROBES,
    QUALIFICATION_PROBES,
    REQUIRED_ACCEPTED_PROBES,
    REQUIRED_REJECTED_PROBES,
)
from scripts.mariadb_references import REFERENCES  # noqa: E402

TARGET = ROOT / "docs" / "mariadb" / "EVIDENCE.md"


def table(header: tuple[str, ...], rows: list[tuple[str, ...]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |"]
    lines.append("|" + "|".join(" --- " for _ in header) + "|")
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return lines


def proof_role(name: str) -> str:
    if name in REQUIRED_ACCEPTED_PROBES:
        return "richiesta: accepted"
    if name in REQUIRED_REJECTED_PROBES:
        return "richiesta: rejected"
    if name in QUALIFICATION_PROBES:
        return f"qualifica: {QUALIFICATION_PROBES[name][1]}"
    if name in OBSERVATION_ONLY_PROBES:
        return "osservativa"
    return "osservativa"


def render() -> str:
    sql = sql_probes()
    driver = tuple(EXPECTED_PROBES)
    lines = [
        "# Evidenza MariaDB",
        "",
        "**Documento generato.** Elenca i riferimenti e le sonde leggendo i",
        "cataloghi eseguibili. Si aggiorna con:",
        "",
        "```powershell",
        "python scripts\\render_mariadb_evidence.py",
        "```",
        "",
        "Il codice pubblica `MariadbProvider` come prodotto distinto dentro il",
        "crate condiviso con MySQL. La selezione resta esplicita: un provider",
        "MySQL rifiuta MariaDB e quello MariaDB rifiuta MySQL.",
        "",
        "Questo inventario **non equivale a un gate live passato**. Gli esiti, il",
        "commit e l'identita delle immagini appartengono al verdetto JSON della",
        "singola corsa. Se il gate non e stato eseguito, non e passato.",
        "",
        "## Come riprodurre",
        "",
        "```powershell",
        "docker compose -f docker-compose.mysql.yml up -d --wait",
        "docker compose -f docker-compose.mariadb.yml up -d --wait",
        "python scripts/check_mariadb_divergence.py",
        "python scripts/check_mariadb_driver.py",
        "python scripts/check_session_campaign.py",
        "```",
        "",
        "`check_mariadb_divergence.py` misura SQL e cataloghi direttamente;",
        "`check_mariadb_driver.py` attraversa driver e provider; la campagna di",
        "sessione rigenera `SESSION-MATRIX.md`. Le famiglie `raw` e `provider`",
        "restano distinte perche la prima misura il protocollo e la seconda il",
        "percorso realmente pubblicato.",
        "",
        "## Riferimenti",
        "",
    ]
    lines += table(
        ("ruolo", "riferimento", "versione", "digest"),
        [
            (
                f"`{reference.role}`",
                reference.label,
                reference.exact_version,
                f"`{reference.digest}`",
            )
            for reference in REFERENCES
        ],
    )
    lines += [
        "",
        "Versione, digest, container e porta hanno una sola fonte:",
        "`docker/mariadb/references.json`.",
        "",
        "## Sonde SQL e catalogo",
        "",
    ]
    lines += table(
        ("superficie", "sonda", "domanda"),
        [
            (probe.surface, f"`{probe.identifier}`", probe.question)
            for probe in sql
        ],
    )
    lines += [
        "",
        "## Sonde driver e provider",
        "",
        f"Il catalogo compilato contiene {len(driver)} sonde. Il ruolo e letto",
        "dagli inventari del gate: una prova richiesta che cambia esito rende la",
        "campagna rossa; una sonda osservativa registra invece una differenza.",
        "",
    ]
    lines += table(
        ("famiglia", "sonda", "ruolo nel gate"),
        [
            (name.split(".", 1)[0], f"`{name}`", proof_role(name))
            for name in driver
        ],
    )
    lines += [
        "",
        "## Prova critica: commit ambiguo",
        "",
        "`provider.ambiguous_commit` usa il seam `DelayedCommitResponse`: il",
        "server applica il commit e la risposta viene trattenuta. Il provider deve",
        "dichiarare `OutcomeUnknown` e la sonda rilegge `commit_contents` da una",
        "seconda connessione. Entrambe le meta sono necessarie: senza rilettura,",
        "l'esito ignoto non dimostrerebbe che il commit e realmente atterrato.",
        "",
        "## Cosa resta aperto",
        "",
        "Il documento non mantiene una roadmap parallela al codice. Le capability",
        "correnti sono generate in `docs/STATO.md`; le forme spatial non pubblicate",
        "restano chiuse nelle dichiarazioni di profilo e nei relativi inventari di",
        "prova. Il perche storico delle campagne precedenti resta in Git.",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args(argv if argv is not None else sys.argv[1:])
    rendered = render()
    if arguments.check:
        current = TARGET.read_text(encoding="utf-8") if TARGET.is_file() else ""
        if current != rendered:
            print(
                "mariadb evidence: documento non allineato; rigeneralo con "
                "python scripts/render_mariadb_evidence.py",
                file=sys.stderr,
            )
            return 1
        return 0
    TARGET.write_text(rendered, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
