#!/usr/bin/env python3
"""Matrice delle divergenze MariaDB, misurata invece che ricordata.

La matrice verifica le divergenze sulle sole superfici attraversate dal
provider, senza dedurle dalla documentazione generale dei motori.

**Cosa misura.** Non le differenze fra i due motori in generale — sarebbe un
elenco infinito e inutile — ma le superfici che il provider `mysql`
attraversa davvero: le sette variabili che legge alla probe, le istruzioni di
sessione che emette per ogni transazione, le colonne di `information_schema`
da cui deriva gli indici, cio che i piani di scrittura eseguono, e lo spatial.
Una divergenza su una superficie che il provider non tocca non e una
divergenza per noi.

**Come misura.** `docker exec` sul client di ogni container, cioe dal socket
locale: non attraversa `require-secure-transport` e non ha bisogno della CA,
quindi un errore osservato qui e del motore e non del trasporto. La password
resta dentro il container — la legge la shell dall'ambiente del processo, non
la riga di comando di docker.

**Cosa non fa.** Non decide. Una riga `differs` non e un difetto di MariaDB e
non e un verdetto sulla qualificazione: e un fatto registrato, con accanto
cio che i due motori hanno risposto. Lo script esce diverso da zero solo se
la misura stessa non e riuscita — un server irraggiungibile, una sonda che
non ha potuto girare da nessuna parte.

Uso:

    python scripts/check_mariadb_divergence.py            # verdetto JSON
    python scripts/check_mariadb_divergence.py --markdown # tabella leggibile
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.mariadb_references import REFERENCES as MARIADB_REFERENCES  # noqa: E402
from scripts.mysql_references import BASELINE as MYSQL_BASELINE  # noqa: E402

MYSQL_CONTAINER = "dataflow-mysql"
DATABASE = "dataflow_test"

# Prefisso di ogni oggetto che le sonde creano. Serve a due cose: rendere
# riconoscibile cio che questo script lascia sul server se muore a meta, e
# permettere alla pulizia di essere cieca — droppa per nome, non per memoria
# di cosa e stato creato.
SCRATCH = "plenora_div"


@dataclass(frozen=True)
class Server:
    """Un motore da interrogare, con il client che sa parlargli."""

    key: str
    label: str
    container: str
    client: str
    password_variable: str
    engine: str


@dataclass(frozen=True)
class Probe:
    """Una sonda: cosa si chiede, e a quale superficie del provider appartiene.

    `sql` finisce con l'istruzione che produce il valore osservato. Le sonde
    che creano oggetti li droppano prima e dopo: due corse di seguito devono
    dare lo stesso risultato, altrimenti la seconda misurerebbe i residui
    della prima.
    """

    identifier: str
    surface: str
    question: str
    sql: str


def probes() -> tuple[Probe, ...]:
    """Il catalogo, una sonda per superficie che il provider attraversa."""

    return (
        # --- cio che il provider legge alla probe -------------------------
        Probe(
            "probe.version",
            "probe",
            "cosa risponde VERSION()",
            "SELECT VERSION()",
        ),
        Probe(
            "probe.version_comment",
            "probe",
            "cosa risponde @@version_comment",
            "SELECT @@version_comment",
        ),
        Probe(
            "probe.lower_case_table_names",
            "probe",
            "come tratta il case dei nomi",
            "SELECT @@lower_case_table_names",
        ),
        Probe(
            "probe.sql_mode",
            "probe",
            "quale sql_mode dichiara",
            "SELECT @@sql_mode",
        ),
        Probe(
            "probe.transaction_isolation",
            "probe",
            "quale isolamento dichiara, con il nome che il provider usa",
            "SELECT @@transaction_isolation",
        ),
        # --- cio che il provider imposta a ogni transazione ---------------
        Probe(
            "session.max_execution_time",
            "sessione",
            "accetta il timeout di statement che il provider imposta",
            "SET SESSION MAX_EXECUTION_TIME = 1000; SELECT 'accettato'",
        ),
        Probe(
            "session.isolation_serializable",
            "sessione",
            "accetta SERIALIZABLE e lo rilegge uguale",
            "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
            " SELECT @@transaction_isolation",
        ),
        Probe(
            "session.context_variable",
            "sessione",
            "regge la chiave puntata del SessionContext",
            "SET @`plenora_ctx_app.tenant` = 'acme';"
            " SELECT @`plenora_ctx_app.tenant`",
        ),
        # --- cio da cui il provider deriva gli indici ----------------------
        Probe(
            "catalog.statistics_expression",
            "catalogo",
            "espone EXPRESSION in information_schema.statistics",
            "SELECT COUNT(EXPRESSION) FROM information_schema.statistics"
            f" WHERE TABLE_SCHEMA = '{DATABASE}'",
        ),
        Probe(
            "catalog.statistics_shape",
            "catalogo",
            "espone le colonne da cui il preflight Upsert ricostruisce gli indici",
            # La tabella se la crea la sonda: leggerne una della fixture
            # significherebbe confrontare due schemi diversi, e la differenza
            # sarebbe dell'harness invece che del motore.
            f"DROP TABLE IF EXISTS {SCRATCH}_idx;"
            f" CREATE TABLE {SCRATCH}_idx"
            " (id INT PRIMARY KEY, code INT, UNIQUE KEY code_uk (code));"
            " SELECT GROUP_CONCAT("
            "CONCAT_WS('/', INDEX_NAME, NON_UNIQUE, COLUMN_NAME, SEQ_IN_INDEX)"
            " ORDER BY INDEX_NAME, SEQ_IN_INDEX)"
            " FROM information_schema.statistics"
            f" WHERE TABLE_SCHEMA = '{DATABASE}' AND TABLE_NAME = '{SCRATCH}_idx';",
        ),
        # --- cio che i piani di scrittura eseguono ------------------------
        Probe(
            "write.on_duplicate_key_rowcount",
            "scrittura",
            "quante righe dichiara un ON DUPLICATE KEY che aggiorna",
            f"DROP TABLE IF EXISTS {SCRATCH}_odk;"
            f" CREATE TABLE {SCRATCH}_odk (id INT PRIMARY KEY, v INT);"
            f" INSERT INTO {SCRATCH}_odk VALUES (1, 1);"
            f" INSERT INTO {SCRATCH}_odk VALUES (1, 2)"
            " ON DUPLICATE KEY UPDATE v = VALUES(v);"
            " SELECT ROW_COUNT();",
        ),
        Probe(
            "write.on_duplicate_key_second_unique",
            "scrittura",
            "su quale indice unico scatta con due chiavi candidate",
            f"DROP TABLE IF EXISTS {SCRATCH}_two;"
            f" CREATE TABLE {SCRATCH}_two"
            " (id INT PRIMARY KEY, code INT, UNIQUE KEY code_uk (code));"
            f" INSERT INTO {SCRATCH}_two VALUES (1, 100), (2, 200);"
            f" INSERT INTO {SCRATCH}_two VALUES (3, 100)"
            " ON DUPLICATE KEY UPDATE code = 999;"
            f" SELECT GROUP_CONCAT(CONCAT(id, ':', code) ORDER BY id) FROM {SCRATCH}_two;",
        ),
        Probe(
            "write.truncate_survives_rollback",
            "scrittura",
            "se TRUNCATE sopravvive a un rollback",
            f"DROP TABLE IF EXISTS {SCRATCH}_trunc;"
            f" CREATE TABLE {SCRATCH}_trunc (id INT PRIMARY KEY);"
            f" INSERT INTO {SCRATCH}_trunc VALUES (1), (2);"
            f" START TRANSACTION; TRUNCATE TABLE {SCRATCH}_trunc; ROLLBACK;"
            f" SELECT COUNT(*) FROM {SCRATCH}_trunc;",
        ),
        Probe(
            "write.delete_survives_rollback",
            "scrittura",
            "se DELETE FROM — la Replace di MySQL — torna indietro",
            f"DROP TABLE IF EXISTS {SCRATCH}_del;"
            f" CREATE TABLE {SCRATCH}_del (id INT PRIMARY KEY) ENGINE = InnoDB;"
            f" INSERT INTO {SCRATCH}_del VALUES (1), (2);"
            f" START TRANSACTION; DELETE FROM {SCRATCH}_del; ROLLBACK;"
            f" SELECT COUNT(*) FROM {SCRATCH}_del;",
        ),
        # --- spatial -------------------------------------------------------
        Probe(
            "spatial.srid_column",
            "spatial",
            "accetta l'attributo SRID di colonna, che la fixture MySQL usa",
            f"DROP TABLE IF EXISTS {SCRATCH}_srid;"
            f" CREATE TABLE {SCRATCH}_srid (g GEOMETRY NOT NULL SRID 4326);"
            " SELECT 'accettato';",
        ),
        Probe(
            "spatial.geometrycollection",
            "spatial",
            "accetta una colonna GEOMETRYCOLLECTION",
            f"DROP TABLE IF EXISTS {SCRATCH}_gc;"
            f" CREATE TABLE {SCRATCH}_gc (g GEOMETRYCOLLECTION NOT NULL);"
            " SELECT 'accettato';",
        ),
        # --- prepared statement --------------------------------------------
        Probe(
            "prepared.instances_table",
            "prepared",
            "espone performance_schema.prepared_statements_instances",
            # Si chiede al catalogo se la tabella **esista**, non di leggerla:
            # un SELECT diretto sarebbe negato anche dove la tabella c'e ma il
            # grant manca, e la fixture MySQL quel grant lo dichiara mentre
            # quella MariaDB no. Misurerebbe le due fixture, non i due motori.
            "SELECT COUNT(*) FROM information_schema.tables"
            " WHERE TABLE_SCHEMA = 'performance_schema'"
            " AND TABLE_NAME = 'prepared_statements_instances'",
        ),
        # --- sequenze --------------------------------------------------------
        Probe(
            "sequence.create",
            "sequenze",
            "accetta CREATE SEQUENCE",
            f"DROP SEQUENCE IF EXISTS {SCRATCH}_seq;"
            f" CREATE SEQUENCE {SCRATCH}_seq START WITH 1;"
            " SELECT 'accettato';",
        ),
    )


def servers() -> tuple[Server, ...]:
    """I motori da confrontare: il riferimento MySQL e le righe MariaDB."""

    entries = [
        Server(
            key="mysql",
            label=MYSQL_BASELINE.label,
            container=MYSQL_CONTAINER,
            client="mysql",
            password_variable="MYSQL_PASSWORD",
            engine="mysql",
        )
    ]
    entries += [
        Server(
            key=f"mariadb-{reference.major}",
            label=reference.label,
            container=reference.container,
            client="mariadb",
            password_variable="MARIADB_PASSWORD",
            engine="mariadb",
        )
        for reference in MARIADB_REFERENCES
    ]
    return tuple(entries)


def ask(server: Server, sql: str) -> tuple[bool, str]:
    """Esegue `sql` sul server e restituisce `(riuscito, testo osservato)`.

    La password non passa dalla riga di comando di docker: la legge la shell
    dentro il container, dall'ambiente con cui il compose lo ha avviato.
    """

    script = (
        f'MYSQL_PWD="${server.password_variable}" '
        f"{server.client} -u dataflow -D {DATABASE} -N -B -e {shell_quote(sql)}"
    )
    completed = subprocess.run(
        ["docker", "exec", server.container, "sh", "-c", script],
        check=False,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )
    if completed.returncode:
        return False, condense(completed.stderr or completed.stdout)
    return True, condense(completed.stdout)


def shell_quote(value: str) -> str:
    """Quota per `sh -c`, che e l'unica shell in mezzo."""

    return "'" + value.replace("'", "'\\''") + "'"


def condense(text: str) -> str:
    """Una riga sola, e per gli errori la sola parte che dice qualcosa.

    Il client rieccheggia l'istruzione fallita prima del messaggio: tenerla
    riempie la cella di SQL che si conosce gia e spinge fuori il motivo, che
    e l'unica cosa che si stava misurando.
    """

    single = " ".join(text.split())
    marker = single.find("ERROR ")
    return single[marker:] if marker >= 0 else single


def cleanup(server: Server) -> None:
    """Toglie gli oggetti delle sonde, senza ricordarsi quali abbia creato."""

    ask(
        server,
        "; ".join(
            [
                f"DROP TABLE IF EXISTS {SCRATCH}_{name}"
                for name in ("odk", "two", "trunc", "del", "srid", "gc", "idx")
            ]
        )
        + "; SELECT 'pulito'",
    )
    # `DROP SEQUENCE` non esiste su MySQL: fallisce, ed e corretto che
    # fallisca — la pulizia non deve pretendere che l'oggetto sia creabile.
    ask(server, f"DROP SEQUENCE IF EXISTS {SCRATCH}_seq")


def observe() -> dict[str, object]:
    """Interroga ogni server con ogni sonda e confronta con il riferimento."""

    catalogue = probes()
    fleet = servers()
    reference = fleet[0]

    results: list[dict[str, object]] = []
    unreachable: list[str] = []
    for server in fleet:
        ok, text = ask(server, "SELECT 1")
        if not ok:
            unreachable.append(f"{server.container}: {text}")
    if unreachable:
        raise RuntimeError(f"server non interrogabili: {unreachable}")

    for probe in catalogue:
        observations: dict[str, dict[str, object]] = {}
        for server in fleet:
            ok, text = ask(server, probe.sql)
            observations[server.key] = {"accepted": ok, "observed": text}
        baseline = observations[reference.key]
        divergent = sorted(
            key
            for key, value in observations.items()
            if key != reference.key and value != baseline
        )
        results.append(
            {
                "probe": probe.identifier,
                "surface": probe.surface,
                "question": probe.question,
                "observations": observations,
                "verdict": "differs" if divergent else "same",
                "divergent": divergent,
            }
        )

    for server in fleet:
        cleanup(server)

    surfaces = sorted({probe.surface for probe in catalogue})
    differing = [entry for entry in results if entry["verdict"] == "differs"]
    return {
        "schema_version": 1,
        "gate": "mariadb-divergence",
        "status": "observed",
        "reference": {
            "key": reference.key,
            "label": reference.label,
            "container": reference.container,
        },
        "compared": [
            {"key": server.key, "label": server.label, "container": server.container}
            for server in fleet[1:]
        ],
        "surfaces": surfaces,
        "totals": {
            "probes": len(results),
            "same": len(results) - len(differing),
            "differs": len(differing),
        },
        "results": results,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }


def markdown(verdict: dict[str, object]) -> str:
    """La stessa osservazione, nella forma che si legge in un documento."""

    compared = verdict["compared"]
    header = ["| superficie | sonda | " + verdict["reference"]["label"]]
    header += [entry["label"] for entry in compared]
    lines = [
        "| superficie | sonda | "
        + " | ".join(
            [verdict["reference"]["label"]] + [entry["label"] for entry in compared]
        )
        + " |",
        "|---" * (2 + 1 + len(compared)) + "|",
    ]
    for entry in verdict["results"]:
        cells = []
        for key in [verdict["reference"]["key"]] + [item["key"] for item in compared]:
            observation = entry["observations"][key]
            value = observation["observed"]
            if not observation["accepted"]:
                value = f"**rifiutato** — {value}"
            cells.append(truncate(value))
        lines.append(
            f"| {entry['surface']} | `{entry['probe']}` | " + " | ".join(cells) + " |"
        )
    return "\n".join(lines)


def truncate(value: str, limit: int = 90) -> str:
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
        verdict = observe()
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"mariadb divergence: {error}", file=sys.stderr)
        return 1

    if arguments.markdown:
        print(markdown(verdict))
    else:
        print(json.dumps(verdict, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
