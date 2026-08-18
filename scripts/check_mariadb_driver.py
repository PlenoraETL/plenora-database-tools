#!/usr/bin/env python3
"""Misura di evidenza MariaDB al livello del driver e del provider.

La prima tranche ha misurato dal **client**: SQL eseguito da `mariadb` e
`mysql`, che ha smentito tre delle cinque divergenze dichiarate e ne ha
trovate due che nessuno aveva nominato. Il client pero non vede il
protocollo — i metadata di `COM_STMT_PREPARE`, i tipi wire, il modo in cui il
provider classifica un esito — e quelle sono le superfici su cui si decide se
MariaDB possa condividere un profilo o serva un provider dedicato.

Questo runner esegue la misura **dentro il crate**, dove vive il bypass di
solo test sul rifiuto iniziale, e la ripete identica sui tre server: MySQL
9.7.2, MariaDB 12.3.2 e MariaDB 11.8.8. Stesse sonde, stesso schema, stesso
JSON.

Il verdetto separa due famiglie, perche rispondono a due domande diverse:

* `raw` — cosa offre il protocollo, con il driver `mysql_async` diretto;
* `provider` — cosa succede a **questo** provider quando attraversa quelle
  stesse superfici.

Una superficie che il protocollo offre e che il provider non raggiunge — per
`MAX_EXECUTION_TIME`, o per `information_schema.statistics.EXPRESSION` — non
e un difetto del motore: e codice che oggi non esiste, ed e esattamente cio
che la decisione deve pesare.

**Cosa non fa.** Non decide, non corregge e non aggira: una sonda rifiutata e
il risultato. Esce diverso da zero solo se la misura non e riuscita — un
server irraggiungibile, il crate che non compila, il marcatore assente — cioe
per un problema dell'harness, che va chiuso prima di leggere i numeri.

Uso:

    python scripts/check_mariadb_driver.py            # verdetto JSON
    python scripts/check_mariadb_driver.py --markdown # tabella leggibile
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compose_network import (  # noqa: E402
    compose_network_arguments,
    compose_volume,
    container_variable,
)
from scripts.mariadb_references import REFERENCES as MARIADB_REFERENCES  # noqa: E402
from scripts.mysql_references import BASELINE as MYSQL_BASELINE  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RUST_IMAGE = "rust:1.92"
MYSQL_CONTAINER = "dataflow-mysql"
MARKER = "PLENORA_MARIADB_EVIDENCE "

# Il test e `#[ignore]` perche pretende un riferimento vivo: qui lo si chiede
# per nome, con `--nocapture` perche il verdetto viaggia sullo stdout.
TEST_COMMAND = (
    "cargo test --locked -p plenora-db-mysql --lib mariadb_driver_evidence "
    "-- --ignored --nocapture --test-threads=1"
)

# Sonde il cui `detail` e per costruzione diverso su server diversi: la
# versione, il cifrario negoziato. Confrontarne il testo direbbe "divergono"
# di due server che si comportano allo stesso modo, quindi per queste vale
# solo l'esito.
OUTCOME_ONLY = frozenset({"raw.tls_cipher", "provider.test_connection"})


@dataclass(frozen=True)
class Server:
    """Un riferimento su cui ripetere la misura."""

    key: str
    label: str
    container: str
    digest: str
    password_variable: str


def servers() -> tuple[Server, ...]:
    entries = [
        Server(
            key="mysql",
            label=MYSQL_BASELINE.label,
            container=MYSQL_CONTAINER,
            digest=MYSQL_BASELINE.digest,
            password_variable="MYSQL_PASSWORD",
        )
    ]
    entries += [
        Server(
            key=f"mariadb-{reference.major}",
            label=reference.label,
            container=reference.container,
            digest=reference.digest,
            password_variable="MARIADB_PASSWORD",
        )
        for reference in MARIADB_REFERENCES
    ]
    return tuple(entries)


def running_digest(container: str) -> str:
    """Il digest dell'immagine che il container sta **davvero** eseguendo.

    Il documento dei riferimenti dice quale immagine dovrebbe girare; questo
    dice quale gira. Registrare solo il primo farebbe passare per misurata su
    12.3.2 una corsa fatta su un'immagine sostituita sotto lo stesso nome —
    ed e esattamente il caso che il pin per digest esiste per escludere.
    """

    return subprocess.run(
        ["docker", "inspect", "--format", "{{.Image}}", container],
        check=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    ).stdout.strip()


def repository_state() -> dict[str, object]:
    """Commit e stato dell'albero al momento della misura.

    Una misura e un'affermazione su del codice: senza il commit non si sa su
    quale, e con l'albero sporco il commit non lo descrive. Vale qui quanto
    vale per il gate del SDK.
    """

    def git(arguments: list[str]) -> str:
        return subprocess.run(
            ["git", *arguments],
            cwd=ROOT,
            check=True,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
        ).stdout

    dirty = [line for line in git(["status", "--porcelain", "-uall"]).splitlines() if line.strip()]
    return {
        "commit": git(["rev-parse", "HEAD"]).strip(),
        "worktree_dirty": bool(dirty),
        "worktree_changes": sorted(dirty),
    }


def run(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode:
        sys.stderr.write(completed.stdout[-4000:])
        sys.stderr.write(completed.stderr[-4000:])
        raise RuntimeError(f"comando fallito: {' '.join(command[:4])}")
    return completed.stdout


def measure(server: Server) -> dict[str, object]:
    """Esegue la misura contro un server e ne restituisce il documento.

    La CA arriva dal volume TLS del container, chiesto a Docker: il
    certificato che verifica quel server e emesso per il suo nome, e usare
    quello di un altro riferimento darebbe un errore di hostname che
    somiglia a una divergenza e non lo e.

    # Raises

    `RuntimeError` se il test non gira o non stampa il marcatore: sono
    problemi dell'harness, non misure.
    """

    tls_volume = compose_volume(server.container, "/etc/mysql/tls")
    environment = [
        "-e", f"PLENORA_MYSQL_HOST={server.container}",
        "-e", "PLENORA_MYSQL_PORT=3306",
        "-e", "PLENORA_MYSQL_CA=/tls/ca.pem",
        "-e",
        f"PLENORA_MYSQL_DATABASE="
        f"{container_variable(server.container, 'MYSQL_DATABASE' if server.key == 'mysql' else 'MARIADB_DATABASE')}",
        "-e",
        f"PLENORA_MYSQL_USER="
        f"{container_variable(server.container, 'MYSQL_USER' if server.key == 'mysql' else 'MARIADB_USER')}",
        "-e",
        f"PLENORA_MYSQL_PASSWORD="
        f"{container_variable(server.container, server.password_variable)}",
        "-e", f"PLENORA_EVIDENCE_LABEL={server.label}",
        "-e", f"PLENORA_EVIDENCE_DIGEST={server.digest}",
    ]
    command = [
        "docker", "run", "--rm",
        *compose_network_arguments(server.container),
        "-v", f"{ROOT}:/workspace",
        "-v", f"{tls_volume}:/tls:ro",
        "-v", "plenora_cargo_registry:/usr/local/cargo/registry",
        "-v", "plenora_cargo_git:/usr/local/cargo/git",
        "-v", "plenora_rustup:/usr/local/rustup",
        "-v", "pln_target_docker:/workspace/target-docker",
        "-w", "/workspace",
        "-e", "CARGO_TARGET_DIR=/workspace/target-docker",
        *environment,
        RUST_IMAGE, "sh", "-c", TEST_COMMAND,
    ]
    output = run(command)
    # Il marcatore non e a inizio riga: `cargo test` stampa "test nome ... "
    # e poi lascia scrivere al test, quindi il JSON arriva in coda alla riga
    # del risultato. Cercarlo con `startswith` non trovava niente.
    for line in reversed(output.splitlines()):
        position = line.find(MARKER)
        if position >= 0:
            return json.loads(line[position + len(MARKER) :])
    raise RuntimeError(
        f"{server.label}: la misura non ha stampato il marcatore {MARKER.strip()}"
    )


def compare(documents: dict[str, dict[str, object]], fleet: tuple[Server, ...]) -> list[dict[str, object]]:
    """Allinea le sonde dei tre server e nomina le divergenze."""

    reference = fleet[0].key
    by_server = {
        key: {entry["probe"]: entry for entry in document["observations"]}
        for key, document in documents.items()
    }
    probes = [entry["probe"] for entry in documents[reference]["observations"]]
    results = []
    for probe in probes:
        observations = {}
        for server in fleet:
            entry = by_server[server.key].get(probe)
            if entry is None:
                raise RuntimeError(
                    f"{server.label}: sonda {probe} assente — le sonde devono "
                    "essere le stesse su tutti i server"
                )
            observations[server.key] = {
                "outcome": entry["outcome"],
                "detail": entry["detail"],
                # Il digest e sul dettaglio **intero**: due server che hanno
                # decodificato lo stesso contenuto si riconoscono a colpo
                # d'occhio, e il confronto non dipende dal fatto che qualcuno
                # legga fino in fondo una riga di quattromila caratteri.
                "digest": hashlib.sha256(
                    entry["detail"].encode("utf-8")
                ).hexdigest()[:16],
                "server_code": entry["server_code"],
            }
        baseline = observations[reference]
        divergent = []
        for server in fleet[1:]:
            observed = observations[server.key]
            same_outcome = observed["outcome"] == baseline["outcome"]
            same_code = observed["server_code"] == baseline["server_code"]
            same_detail = (
                probe in OUTCOME_ONLY or observed["detail"] == baseline["detail"]
            )
            if not (same_outcome and same_code and same_detail):
                divergent.append(server.key)
        template = documents[reference]["observations"][probes.index(probe)]
        results.append(
            {
                "probe": probe,
                "family": template["family"],
                "surface": template["surface"],
                "question": template["question"],
                "observations": observations,
                "verdict": "differs" if divergent else "same",
                "divergent": divergent,
            }
        )
    return results


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
    documents = {server.key: measure(server) for server in fleet}
    results = compare(documents, fleet)
    families = sorted({entry["family"] for entry in results})
    differing = [entry for entry in results if entry["verdict"] == "differs"]
    not_measured = [
        entry
        for entry in results
        if any(
            observation["outcome"] == "not_measured"
            for observation in entry["observations"].values()
        )
    ]
    return {
        "schema_version": 1,
        "gate": "mariadb-driver-evidence",
        "status": "observed",
        "reference": fleet[0].key,
        "servers": [
            {
                "key": server.key,
                "label": server.label,
                "container": server.container,
                "declared_digest": server.digest,
                "running_digest": running_digest(server.container),
                "product_version": documents[server.key]["server"]["product_version"],
                "version_comment": documents[server.key]["server"]["version_comment"],
                "tls": next(
                    (
                        observation["detail"]
                        for observation in documents[server.key]["observations"]
                        if observation["probe"] == "raw.tls_cipher"
                    ),
                    "sconosciuto",
                ),
            }
            for server in fleet
        ],
        "repository": repository_state(),
        "families": families,
        "totals": {
            "probes": len(results),
            "same": len(results) - len(differing),
            "differs": len(differing),
            "not_measured": len(not_measured),
        },
        "results": results,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }


def markdown(document: dict[str, object]) -> str:
    servers_ = document["servers"]
    header = "| famiglia | superficie | sonda | " + " | ".join(
        entry["label"] for entry in servers_
    )
    lines = [f"{header} |", "|---" * (3 + len(servers_)) + "|"]
    for entry in document["results"]:
        cells = []
        for server in servers_:
            observation = entry["observations"][server["key"]]
            mark = {"accepted": "", "rejected": "**no** ", "not_measured": "— "}[
                observation["outcome"]
            ]
            cells.append(f"{mark}{truncate(observation['detail'])}")
        lines.append(
            f"| {entry['family']} | {entry['surface']} | `{entry['probe']}` | "
            + " | ".join(cells)
            + " |"
        )
    return "\n".join(lines)


def truncate(value: str, limit: int = 88) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="stampa la matrice come tabella invece del verdetto JSON",
    )
    arguments = parser.parse_args()
    try:
        document = verdict()
    except RuntimeError as error:
        print(f"mariadb driver evidence: {error}", file=sys.stderr)
        return 1

    if arguments.markdown:
        print(markdown(document))
    else:
        print(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
