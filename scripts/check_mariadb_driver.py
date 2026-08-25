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
from collections.abc import Iterable
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


# Le sonde che sostengono una capability gia pubblicata, e cosa sostengono.
#
# Il resto della matrice **osserva**: una sonda che diventa `rejected` e una
# misura, e il verdetto la registra senza fallire — e il modo giusto di
# raccontare due motori diversi. Queste no. Il profilo dichiara `streaming`,
# `projection`, `filter` e `ordering` come `true`, e quella dichiarazione
# poggia su queste corse: se una smette di passare, la promessa resta
# pubblicata e la prova non c'e piu.
#
# Il runner esce percio diverso da zero, e la perturbazione che rende rossa
# una sonda rende rosso anche il gate — che e cio che la review ha chiesto,
# e che prima non succedeva: due rifiuti identici sui tre server erano `same`,
# e `same` usciva con zero.
REQUIRED_ACCEPTED_PROBES: dict[str, str] = {
    "provider.profile_probe": "riconoscimento del prodotto e qualifica della versione",
    "provider.profile_describe_object": "catalogo letto con le query del profilo",
    "provider.profile_read_schema": "lo schema che la lettura pubblica",
    "provider.profile_read_values": "i valori che la lettura decodifica",
    "provider.profile_read_namespace": "i metadata nel namespace del prodotto",
    "provider.profile_read_projection": "reads.projection",
    "provider.profile_read_filter_forms": "reads.filter, tutte e tredici le forme",
    "provider.profile_read_ordering_asc": "reads.ordering, verso ascendente",
    "provider.profile_read_ordering_desc": "reads.ordering, verso discendente",
    "provider.profile_read_streaming": "reads.streaming",
    # Punto 2 della fase 3: il contratto dell'indice. La descrizione deve
    # riuscire, e cio che ne esce deve corrispondere all'esito della DDL —
    # dove l'indice su espressione si crea deve risultare non confrontabile,
    # dove non si crea non deve comparire.
    "provider.profile_functional_index": "il catalogo descrive gli indici, e li descrive come sono",
    "provider.profile_generated_index": "il catalogo descrive la colonna generata e il suo indice",
}

# Le sonde che **osservano** e basta: nessuna capability pubblicata poggia su
# di loro, quindi il loro esito e una misura come le altre della matrice.
#
# Il terzo elenco esiste per non avere una terza categoria implicita. Una
# sonda che non sostiene niente e legittima — serve a sapere come si comporta
# il motore — ma dev'essere **dichiarata** tale, altrimenti "non e in nessun
# inventario" e indistinguibile da "qualcuno si e dimenticato di
# classificarla".
#
# Oggi e vuoto, ed e un buon segno: il punto 2 ha dato un contratto anche
# all'ultima sonda che ne era priva. Resta perche la prossima sonda senza
# contratto abbia dove stare **dichiarata**, invece di non stare da nessuna
# parte.
OBSERVATION_ONLY_PROBES: dict[str, str] = {}

# Le sonde il cui **rifiuto** e la prova.
#
# `filter = true` significa le tredici forme qualificate e non "qualunque
# filtro": se una di queste due passasse, il flag coprirebbe una superficie
# che nessuna misura sostiene. Un `not_measured` conta come violazione — e
# cio che la sonda registra quando il rifiuto arriva per un'altra ragione, e
# un fail-close verificato per la ragione sbagliata non e verificato.
REQUIRED_REJECTED_PROBES: dict[str, str] = {
    "provider.profile_read_filter_closed_like": "reads.filter esclude LIKE case-insensitive",
    "provider.profile_read_filter_closed_spatial": "reads.filter esclude il filtro spatial",
    # Su MariaDB `srs_id` arriva sempre nullo, quindi una geometry non e
    # descrivibile: `spatial.read_wkb` resta chiusa, e il rifiuto e la sua
    # prova. Su MySQL la stessa tabella e rifiutata perche la fixture non
    # dichiara un SRID — due ragioni diverse per lo stesso esito, ed e per
    # questo che la riga dice quale delle due sostiene cosa.
    "provider.profile_describe_geometry": "spatial.read_wkb resta chiusa su MariaDB",
    # Il timeout **deve** scattare: la sonda registra il rifiuto solo se la
    # quaterna e quella dichiarata, quindi un 1969 che tornasse generico non
    # arriverebbe qui come `rejected`.
    "provider.profile_timeout": "il timeout del profilo scatta ed e classificato come tale",
    # Le tre forme che una tabella con un indice unico su colonna generata
    # permette. Nessuna e sicura, e il rifiuto di ciascuna e la prova che
    # `writes.upsert` — quando si aprira — non le accettera per distrazione.
    "provider.profile_upsert_on_primary_key": "l'Upsert rifiuta un secondo indice unico",
    "provider.profile_upsert_on_generated_key": "l'Upsert rifiuta le keys che non ancorano da sole",
    "provider.profile_upsert_generated_anchor": "l'Upsert rifiuta di scrivere una colonna generata",
}


# L'inventario esatto delle sonde, nell'ordine in cui la misura le produce.
#
# Le violazioni di capability dicono se una prova necessaria ha cambiato esito;
# non dicono se una sonda e **sparita**. Se una `raw.*` o una osservativa
# smettesse di essere prodotta su tutti e tre i server, il totale scenderebbe
# da 74 a 73 e l'uscita resterebbe zero: la matrice
# racconterebbe una superficie in meno senza che nulla lo dica.
#
# L'ordine e parte del contratto perche e cio che rende leggibile il documento
# — le famiglie stanno insieme — e perche una sonda spostata e quasi sempre una
# sonda riscritta.
EXPECTED_PROBES: tuple[str, ...] = (
    "raw.tls_cipher",
    "raw.type_table",
    "raw.type_row",
    "raw.geometry_table",
    "raw.prepare_metadata_geometry",
    "raw.prepare_metadata",
    "raw.prepare_parameters",
    "raw.column_srid",
    "raw.spatial_functions",
    "raw.max_execution_time",
    "raw.statistics_expression",
    "provider.test_connection",
    "provider.capabilities",
    "provider.describe_object",
    "provider.query_schema",
    "provider.query_values",
    "provider.read",
    "provider.read_geometry",
    "provider.transaction",
    "provider.cancellation_inflight",
    "provider.session_quarantine",
    "provider.session_reuse",
    "provider.ambiguous_commit",
    "raw.error_unknown_column",
    "raw.error_unknown_table",
    "raw.error_unknown_database",
    "raw.error_duplicate_key",
    "raw.error_not_null",
    "raw.error_foreign_key",
    "raw.error_check_violation",
    "raw.error_privilege",
    "raw.error_statement_timeout",
    "raw.error_lock_wait",
    "raw.error_deadlock",
    "raw.error_access_denied",
    "provider.profile_probe",
    "provider.profile_describe_object",
    "provider.profile_describe_geometry",
    "raw.functional_index_ddl",
    "provider.profile_functional_index",
    "provider.profile_read_schema",
    "provider.profile_read_values",
    "provider.profile_read_namespace",
    "provider.profile_read_projection",
    "provider.profile_read_filter_forms",
    "provider.profile_read_filter_closed_like",
    "provider.profile_read_filter_closed_spatial",
    "provider.profile_read_ordering_asc",
    "provider.profile_read_ordering_desc",
    "provider.profile_read_streaming",
    "raw.generated_column_catalog",
    "provider.profile_generated_index",
    "provider.profile_upsert_on_primary_key",
    "provider.profile_upsert_on_generated_key",
    "provider.profile_upsert_generated_anchor",
    "provider.profile_write_append",
    "provider.profile_write_append_rollback",
    "provider.profile_write_append_cancellation",
    "provider.profile_write_create",
    "provider.profile_write_create_rollback",
    "provider.profile_write_create_cancellation",
    "provider.profile_write_update",
    "provider.profile_write_update_rollback",
    "provider.profile_write_update_cancellation",
    "provider.profile_write_upsert",
    "provider.profile_write_upsert_rollback",
    "provider.profile_write_upsert_cancellation",
    "provider.profile_write_replace",
    "provider.profile_write_replace_rollback",
    "provider.profile_write_replace_cancellation",
    "provider.profile_write_delete_by_keys",
    "provider.profile_write_delete_by_keys_rollback",
    "provider.profile_write_delete_by_keys_cancellation",
    "provider.profile_timeout",
)


def inventory_violations(document: dict[str, object]) -> list[str]:
    """Cosa manca, cosa avanza, e cosa si e spostato.

    Pura: prende il documento e ne guarda i nomi, cosi il giudizio si prova
    senza accendere un server.
    """

    observed = tuple(entry["probe"] for entry in document["results"])
    if observed == EXPECTED_PROBES:
        return []
    missing = [probe for probe in EXPECTED_PROBES if probe not in observed]
    unexpected = [probe for probe in observed if probe not in EXPECTED_PROBES]
    violations = [f"sonda sparita dalla misura: {probe}" for probe in missing]
    violations += [f"sonda non dichiarata nell'inventario: {probe}" for probe in unexpected]
    if not violations:
        violations.append(
            "le sonde sono quelle attese ma in un altro ordine: "
            f"atteso {EXPECTED_PROBES[:3]}..., osservato {observed[:3]}..."
        )
    return violations


def gate_violations(document: dict[str, object]) -> list[str]:
    """Tutto cio che rende rossa la campagna: inventario e prove necessarie."""

    return inventory_violations(document) + capability_violations(document)


def duplicate_probes(names: Iterable[str]) -> list[str]:
    """I nomi che compaiono piu di una volta, in ordine.

    Pura, e usata da entrambi i punti in cui un elenco di sonde diventa un
    dizionario: e li che un duplicato smette di essere visibile.
    """

    seen: set[str] = set()
    duplicated: list[str] = []
    for name in names:
        if name in seen and name not in duplicated:
            duplicated.append(name)
        seen.add(name)
    return sorted(duplicated)


# Le sonde che **qualificano** una superficie non ancora pubblicata.
#
# Sono bloccanti come le altre — una prova che cambia esito e una prova persa —
# ma dicono un'altra cosa: non "una capability dichiarata ha perso la sua
# prova", bensi "la qualifica di una superficie che non e ancora aperta non
# regge piu". La differenza conta perche `writes.append` e chiusa: attribuire
# queste tre a una capability pubblicata farebbe leggere al verdetto una
# promessa che il contratto non fa.
#
# La `chiave` e la superficie qualificata; il valore dice cosa la sonda deve
# rendere.
QUALIFICATION_PROBES: dict[str, tuple[str, str]] = {
    "provider.profile_write_append": (
        "Append",
        "accepted",
    ),
    "provider.profile_write_append_rollback": (
        "Append: il rollback annulla anche il primo batch",
        "rejected",
    ),
    "provider.profile_write_append_cancellation": (
        "Append: la cancellazione non lascia righe e il provider resta usabile",
        "rejected",
    ),
    "provider.profile_write_create": (
        "Create",
        "accepted",
    ),
    "provider.profile_write_create_rollback": (
        "Create: le righe tornano indietro e la tabella resta, dichiarate Partial",
        "rejected",
    ),
    "provider.profile_write_create_cancellation": (
        "Create: la cancellazione non lascia righe, lascia la tabella, e il provider resta usabile",
        "rejected",
    ),
    "provider.profile_write_update": (
        "Update: aggiorna cio che trova, salta cio che non trova, non inserisce",
        "accepted",
    ),
    "provider.profile_write_update_rollback": (
        "Update: il rollback rimette i valori di prima",
        "rejected",
    ),
    "provider.profile_write_update_cancellation": (
        "Update: la cancellazione lascia i valori di prima, e il provider resta usabile",
        "rejected",
    ),
    "provider.profile_write_upsert": (
        "Upsert: aggiorna cio che c'e, inserisce cio che non c'e, e non scompone cio che non sa",
        "accepted",
    ),
    "provider.profile_write_upsert_rollback": (
        "Upsert: il rollback annulla anche gli aggiornamenti del primo batch",
        "rejected",
    ),
    "provider.profile_write_upsert_cancellation": (
        "Upsert: la cancellazione non applica nulla, e il provider resta usabile",
        "rejected",
    ),
    "provider.profile_write_replace": (
        "Replace: svuota il target e ci mette le righe in ingresso",
        "accepted",
    ),
    "provider.profile_write_replace_rollback": (
        "Replace: un fallimento non lascia il target vuoto",
        "rejected",
    ),
    "provider.profile_write_replace_cancellation": (
        "Replace: la cancellazione non lascia il target vuoto",
        "rejected",
    ),
    "provider.profile_write_delete_by_keys": (
        "DeleteByKeys: cancella cio che trova e salta cio che non trova",
        "accepted",
    ),
    "provider.profile_write_delete_by_keys_rollback": (
        "DeleteByKeys: una chiave trattenuta fa tornare indietro l'intero batch",
        "rejected",
    ),
    "provider.profile_write_delete_by_keys_cancellation": (
        "DeleteByKeys: la cancellazione non toglie righe, e il provider resta usabile",
        "rejected",
    ),
}


def capability_violations(document: dict[str, object]) -> list[str]:
    """Le prove che una capability pubblicata non ha piu, o non ha mai avuto.

    Pura: prende il documento e ne guarda gli esiti, cosi il giudizio si prova
    senza accendere un server.
    """

    duplicated = duplicate_probes(entry["probe"] for entry in document["results"])
    if duplicated:
        raise RuntimeError(
            f"sonde duplicate nel verdetto — {', '.join(duplicated)}: il giudizio "
            "sulle capability guarderebbe una voce sola"
        )
    entries = {entry["probe"]: entry for entry in document["results"]}
    violations: list[str] = []
    # Una lista sola, con accanto a ogni sonda cosa deve rendere e cosa si
    # perde se non lo rende. Le prime due famiglie sostengono una capability
    # **pubblicata**; la terza qualifica una superficie che il contratto non
    # promette ancora, ed e altrettanto bloccante — ma dirlo allo stesso modo
    # farebbe leggere al verdetto una promessa che non esiste.
    checks: list[tuple[str, str, str, str]] = []
    for probe, capability in sorted(REQUIRED_ACCEPTED_PROBES.items()):
        checks.append((probe, "accepted", capability, "resta dichiarata senza prova"))
    for probe, capability in sorted(REQUIRED_REJECTED_PROBES.items()):
        checks.append((probe, "rejected", capability, "resta dichiarata senza prova"))
    for probe, (surface, expected) in sorted(QUALIFICATION_PROBES.items()):
        checks.append((probe, expected, surface, "perde la sua qualifica"))

    for probe, expected, subject, consequence in checks:
        entry = entries.get(probe)
        if entry is None:
            violations.append(
                f"{probe}: sonda assente dalla matrice — {subject} {consequence}"
            )
            continue
        for server, observation in sorted(entry["observations"].items()):
            if observation["outcome"] != expected:
                violations.append(
                    f"{probe} su {server}: atteso {expected}, osservato "
                    f"{observation['outcome']} — {subject} {consequence}"
                )
    return violations



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


def declares_image(identities: tuple[str, ...], digest: str) -> bool:
    """Se una di quelle identita e il digest dichiarato.

    Funzione pura, e separata per questo: e la parte che si puo sbagliare in
    silenzio, e l'unica verificabile senza un demone.
    """

    return any(
        identity == digest or identity.endswith(f"@{digest}") for identity in identities
    )


def image_identities(container: str) -> tuple[str, ...]:
    """I modi in cui il demone dice quale immagine sta girando.

    Il documento dei riferimenti dice quale immagine dovrebbe girare; questo
    dice quale gira. Registrare solo il primo farebbe passare per misurata su
    12.3.2 una corsa fatta su un'immagine sostituita sotto lo stesso nome — ed
    e esattamente il caso che il pin per digest esiste per escludere.

    Non basta pero `{{.Image}}`: quello e l'**ID** dell'immagine, e cosa
    contenga dipende dallo store del demone. Con containerd coincide con il
    digest del manifest; con il graph driver classico e il digest della
    *config*, un valore diverso. Confrontarlo con il pin passava in locale e
    falliva sul runner — verde dove non serviva, rossa dove serviva, che e il
    modo peggiore in cui una verifica puo sbagliare.
    #
    Si guardano quindi tutte e tre le risposte: il riferimento con cui il
    container e stato creato, l'ID dell'immagine, e i digest di manifest per
    cui quell'immagine e conosciuta. Il pin e un digest di manifest, e deve
    comparire fra queste.
    """

    def inspect(arguments: list[str]) -> str:
        return subprocess.run(
            ["docker", *arguments],
            check=True,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
        ).stdout.strip()

    configured = inspect(["inspect", "--format", "{{.Config.Image}}", container])
    image_id = inspect(["inspect", "--format", "{{.Image}}", container])
    repo_digests = json.loads(
        inspect(["image", "inspect", "--format", "{{json .RepoDigests}}", image_id])
        or "[]"
    )
    return (configured, image_id, *repo_digests)


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


def measure(
    server: Server,
    marker: str = MARKER,
    test_command: str = TEST_COMMAND,
) -> dict[str, object]:
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
        RUST_IMAGE, "sh", "-c", test_command,
    ]
    output = run(command)
    # Il marcatore non e a inizio riga: `cargo test` stampa "test nome ... "
    # e poi lascia scrivere al test, quindi il JSON arriva in coda alla riga
    # del risultato. Cercarlo con `startswith` non trovava niente.
    for line in reversed(output.splitlines()):
        position = line.find(marker)
        if position >= 0:
            return json.loads(line[position + len(marker) :])
    raise RuntimeError(
        f"{server.label}: la misura non ha stampato il marcatore {marker.strip()}"
    )


def compare(
    documents: dict[str, dict[str, object]],
    fleet: tuple[Server, ...],
    outcome_only: frozenset[str] = OUTCOME_ONLY,
    expected: tuple[str, ...] | None = None,
) -> list[dict[str, object]]:
    """Allinea le sonde dei tre server e nomina le divergenze.

    `expected` e l'inventario che ogni documento **grezzo** deve dichiarare,
    esatto e ordinato. E un parametro e non una costante perche questa
    funzione la usano due misure con due inventari: la matrice di sessione ne
    ha tredici, e passargli quello di MariaDB le faceva rifiutare ogni
    documento. Chi non lo passa ha un controllo suo — e la matrice di sessione
    ce l'ha, in `validate`.
    """

    reference = fleet[0].key
    for key, document in sorted(documents.items()):
        # Prima di costruire il dizionario, non dopo: due voci con lo stesso
        # nome ne producono una sola, e la seconda sparisce senza dire niente.
        # Su un server solo diventerebbe una divergenza inventata; su tutti e
        # tre, una sonda che non esiste piu ma continua a comparire.
        duplicated = duplicate_probes(entry["probe"] for entry in document["observations"])
        if duplicated:
            raise RuntimeError(
                f"{key}: sonde duplicate nella misura — {', '.join(duplicated)}"
            )
        # L'inventario si verifica **su ogni documento grezzo**, non sul
        # risultato del confronto. Dopo, l'informazione non c'e piu: l'elenco
        # delle sonde viene dal solo server di riferimento e gli altri passano
        # da un dizionario, quindi una sonda in piu — o spostata — su MariaDB
        # sparirebbe dall'allineamento e il documento finale conserverebbe le
        # sonde di MySQL, intatte.
        if expected is None:
            continue
        observed = tuple(entry["probe"] for entry in document["observations"])
        if observed != expected:
            missing = [probe for probe in expected if probe not in observed]
            unexpected = [probe for probe in observed if probe not in expected]
            difference = (
                f"mancano {missing}" if missing else ""
            ) + (
                f"{'; ' if missing else ''}in piu {unexpected}" if unexpected else ""
            ) or "stesso insieme, altro ordine"
            raise RuntimeError(
                f"{key}: l'inventario delle sonde non e quello dichiarato — {difference}"
            )
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
                probe in outcome_only or observed["detail"] == baseline["detail"]
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
        identities = image_identities(server.container)
        if not declares_image(identities, server.digest):
            raise RuntimeError(
                f"{server.label}: il container esegue {', '.join(identities)}, il "
                f"documento dichiara {server.digest} — la misura non riguarderebbe "
                "l'immagine dichiarata"
            )
    documents = {server.key: measure(server) for server in fleet}
    results = compare(documents, fleet, OUTCOME_ONLY, EXPECTED_PROBES)
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
    document: dict[str, object] = {
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
    document["violations"] = gate_violations(document)
    document["status"] = "regressed" if document["violations"] else "observed"
    return document


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

    # Il verdetto si stampa comunque — anche una corsa che fallisce e una
    # misura, e nasconderla renderebbe il fallimento illeggibile — ma l'uscita
    # dice se cio che il profilo dichiara ha ancora una prova sotto.
    violations = document["violations"]
    if violations:
        print(
            "mariadb driver evidence FAILED: l'inventario delle sonde o una prova "
            "necessaria non regge piu",
            file=sys.stderr,
        )
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
