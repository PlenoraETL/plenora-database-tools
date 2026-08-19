#!/usr/bin/env python3
"""Matrice della semantica di sessione sui tre riferimenti accesi.

La fase 1 ha lasciato fuori dal profilo il bootstrap di sessione, i livelli di
isolamento e `START TRANSACTION`. Un residuo dichiarato non e una decisione:
questa matrice misura se quelle superfici coincidono su MySQL 9.7, MariaDB
12.3 e MariaDB 11.8, perche la regola architetturale dipende dalla risposta.

Cio che il runner **non** fa: interpretare. Registra cosa i tre server hanno
fatto, e quali sonde divergono. La decisione su cosa spostare nel profilo si
prende leggendo la matrice, non generandola.

Riusa il runner della misura MariaDB — stessi riferimenti, stesso pin per
digest, stesso confronto — cambiando solo il test da eseguire e il marcatore
da cercare. Due runner con la stessa logica divergerebbero alla prima
correzione, e la seconda misura la erediterebbe rotta.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_mariadb_driver import (  # noqa: E402
    ROOT,
    compare,
    measure,
    repository_state,
    running_digest,
    servers,
)

MARKER = "PLENORA_SESSION_EVIDENCE "

TEST_COMMAND = (
    "cargo test --locked -p plenora-db-mysql --lib session_semantics_evidence "
    "-- --ignored --nocapture --test-threads=1"
)

# Nessuna sonda della sessione ha un dettaglio che dipende dal server per
# costruzione: il livello di isolamento, l'access mode e l'esito di
# commit/rollback si confrontano per testo. Se una sonda dovesse entrare qui,
# il motivo va scritto accanto — "diverge sempre" senza una ragione e il modo
# in cui una divergenza vera smette di essere vista.
OUTCOME_ONLY: frozenset[str] = frozenset()

EVIDENCE = ROOT / "docs" / "mariadb" / "SESSION-MATRIX.md"


def verdict() -> dict[str, object]:
    fleet = servers()
    for server in fleet:
        observed = running_digest(server.container)
        if observed != server.digest:
            raise RuntimeError(
                f"{server.label}: il container esegue {observed}, il documento "
                f"dichiara {server.digest} — la misura non riguarderebbe "
                "l'immagine dichiarata"
            )
    documents = {
        server.key: measure(server, MARKER, TEST_COMMAND) for server in fleet
    }

    # Il bootstrap misurato deve essere quello del pool, su tutti e tre: se
    # due server riportassero SQL diversi, staremmo confrontando due misure e
    # non due comportamenti.
    statements = {document["bootstrap_sql"] for document in documents.values()}
    if len(statements) != 1:
        raise RuntimeError(
            f"i server hanno misurato SESSION_BOOTSTRAP_SQL diversi: {statements}"
        )

    results = compare(documents, fleet, OUTCOME_ONLY)
    divergent = [entry for entry in results if entry["verdict"] == "differs"]

    # La matrice e una prova permanente, non una fotografia: da qui in poi
    # l'esecuzione **fallisce** se cio che ha giustificato la decisione
    # cambia. Sono due condizioni, e servono entrambe.
    #
    # La prima e ovvia: nessuna sonda deve divergere, perche su questo poggia
    # il fatto che il codice di sessione resti condiviso.
    #
    # La seconda meno: "coincidono" e vero anche quando tutti e tre
    # falliscono allo stesso modo. Una regressione che rompesse il bootstrap
    # ovunque passerebbe per accordo perfetto, ed e esattamente il verde
    # falso che questa misura esiste per escludere.
    unaccepted = [
        f"{entry['probe']}@{key}"
        for entry in results
        for key, observation in entry["observations"].items()
        if observation["outcome"] != "accepted"
    ]
    if unaccepted:
        raise RuntimeError(
            "sonde non accettate: " + ", ".join(unaccepted) + " — una matrice "
            "che coincide su un fallimento non giustifica nulla"
        )
    if divergent:
        raise RuntimeError(
            "sonde divergenti: "
            + ", ".join(f"{entry['probe']} ({', '.join(entry['divergent'])})" for entry in divergent)
            + " — la semantica di sessione non e piu condivisa, e la "
            "decisione che la lascia fuori dal profilo va rivista"
        )
    return {
        "schema_version": 1,
        "measure": "session-semantics",
        "repository": repository_state(),
        "bootstrap_sql": statements.pop(),
        "servers": {
            server.key: {
                "label": server.label,
                "declared_digest": server.digest,
                "running_digest": running_digest(server.container),
                "product_version": documents[server.key]["server"]["product_version"],
                "version_comment": documents[server.key]["server"]["version_comment"],
            }
            for server in fleet
        },
        "totals": {
            "probes": len(results),
            "same": len(results) - len(divergent),
            "differs": len(divergent),
        },
        "results": results,
    }


def markdown(document: dict[str, object]) -> str:
    servers_document = document["servers"]
    keys = list(servers_document)
    lines = [
        "# Matrice della semantica di sessione",
        "",
        "Misurata attraverso il driver e il percorso reale del pool, non con "
        "il client. Generata da `scripts/check_session_matrix.py`; non "
        "modificare a mano.",
        "",
        # Il commit e quello su cui la misura e girata, non quello che la
        # contiene: un artefatto generato precede per costruzione il commit
        # che lo porta. Dirlo evita di leggerlo come una contraddizione.
        f"Misurata su `{document['repository']['commit']}`"
        f"{', albero con modifiche non committate' if document['repository']['worktree_dirty'] else ', albero pulito'}"
        ". Il documento entra nel commit successivo.",
        "",
        "```sql",
        str(document["bootstrap_sql"]),
        "```",
        "",
        "## Riferimenti",
        "",
        "| chiave | riferimento | versione | digest osservato |",
        "| --- | --- | --- | --- |",
    ]
    for key in keys:
        entry = servers_document[key]
        lines.append(
            f"| `{key}` | {entry['label']} | {entry['product_version']} | "
            f"`{entry['running_digest'][:23]}` |"
        )
    totals = document["totals"]
    lines += [
        "",
        f"**{totals['probes']} sonde: {totals['same']} coincidono, "
        f"{totals['differs']} divergono.**",
        "",
        "## Sonde",
        "",
        "| sonda | superficie | " + " | ".join(f"`{key}`" for key in keys) + " | esito |",
        "| --- | --- | " + " | ".join("---" for _ in keys) + " | --- |",
    ]
    for entry in document["results"]:
        cells = []
        for key in keys:
            observation = entry["observations"][key]
            cells.append(f"{observation['outcome']} `{observation['digest']}`")
        lines.append(
            f"| `{entry['probe']}` | {entry['surface']} | "
            + " | ".join(cells)
            + f" | {entry['verdict']} |"
        )
    lines += ["", "## Dettagli", ""]
    for entry in document["results"]:
        lines.append(f"### `{entry['probe']}`")
        lines.append("")
        lines.append(str(entry["question"]))
        lines.append("")
        for key in keys:
            observation = entry["observations"][key]
            lines.append(
                f"* **{key}** — {observation['outcome']}: {observation['detail']}"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    document = verdict()
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(markdown(document), encoding="utf-8", newline="\n")
    print(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=1))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"session matrix FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
