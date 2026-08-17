#!/usr/bin/env python3
"""I riferimenti MariaDB: una fonte sola per versione, digest, container e porta.

`docker/mariadb/references.json` dichiara le immagini del ciclo di evidenza,
ognuna con la versione esatta e il digest immutabile del manifest index.
Questo modulo lo carica e ne deriva il resto, cosi che ne il compose ne uno
script possano affermare una versione diversa da quella effettivamente
avviata.

I ruoli si chiamano `evidence` e `compatibility`, mai `baseline`: MariaDB
**non** e un riferimento qualificato. Il provider `mysql` fa fail-close alla
probe quando la riconosce, e questa fixture esiste per produrre l'evidenza
che ADR 0014 chiede prima di decidere se qualificarla. Chiamarla baseline la
farebbe leggere come una piattaforma supportata, che e proprio l'equivoco che
il fail-close esiste per impedire.

Le righe sono due perche un fork ha una storia: la principale e l'attuale
LTS, la seconda la LTS precedente. L'evidenza raccolta su una versione non si
butta quando ne esce un'altra — semmai e il confronto fra le due a dire se la
divergenza dipende dalla versione.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REFERENCES_FILE = ROOT / "docker" / "mariadb" / "references.json"
COMPOSE_FILE = ROOT / "docker-compose.mariadb.yml"

EVIDENCE_ROLE = "evidence"
COMPATIBILITY_ROLE = "compatibility"
# Un nome risolvibile che nessun certificato copre: rende deterministica la
# prova TLS negativa senza mascherarla da errore DNS.
MISMATCH_ALIAS = "mariadb-hostname-mismatch"


@dataclass(frozen=True)
class MariadbReference:
    """Un'immagine del ciclo di evidenza, con il modo di raggiungerla."""

    role: str
    label: str
    exact_version: str
    digest: str
    container: str
    port: int

    @property
    def image(self) -> str:
        return f"mariadb@{self.digest}"

    @property
    def version_prefix(self) -> str:
        major_minor, _, _patch = self.exact_version.rpartition(".")
        return f"{major_minor}."

    @property
    def major(self) -> str:
        return self.exact_version.split(".", 1)[0]

    @property
    def tls_volume(self) -> str:
        """Volume del materiale TLS: uno per riferimento.

        I due server hanno hostname diversi, quindi certificati diversi.
        Condividere il volume darebbe a uno dei due un certificato emesso per
        l'altro, e il client lo rifiuterebbe per hostname mismatch — un
        errore che somiglia a un difetto del provider e non lo e.
        """

        return f"mariadb_{self.major}_tls"


def _load() -> tuple[MariadbReference, ...]:
    document = json.loads(REFERENCES_FILE.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise RuntimeError("schema dei riferimenti MariaDB non riconosciuto")
    entries = tuple(
        MariadbReference(
            role=entry["role"],
            label=entry["label"],
            exact_version=entry["exact_version"],
            digest=entry["digest"],
            container=entry["container"],
            port=int(entry["port"]),
        )
        for entry in document["references"]
    )
    if not entries:
        raise RuntimeError("riferimenti MariaDB vuoti")
    for entry in entries:
        # `sha256:` piu 64 esadecimali: un tag al posto del digest sarebbe
        # mutabile, e la riga smetterebbe di provare quale immagine e girata.
        if not entry.digest.startswith("sha256:") or len(entry.digest) != 71:
            raise RuntimeError(f"digest non immutabile per {entry.label}")
        if entry.exact_version.count(".") != 2:
            raise RuntimeError(f"versione non esatta per {entry.label}")
        if entry.role not in {EVIDENCE_ROLE, COMPATIBILITY_ROLE}:
            raise RuntimeError(
                f"ruolo '{entry.role}' per {entry.label}: MariaDB non e "
                "qualificata, e i soli ruoli ammessi sono 'evidence' e "
                "'compatibility'"
            )
    evidence = [entry for entry in entries if entry.role == EVIDENCE_ROLE]
    if len(evidence) != 1:
        raise RuntimeError("il ciclo deve dichiarare una sola riga 'evidence'")
    for field in ("exact_version", "digest", "container", "port"):
        values = [getattr(entry, field) for entry in entries]
        if len(set(values)) != len(values):
            raise RuntimeError(f"{field} duplicato fra i riferimenti MariaDB")
    return entries


REFERENCES: tuple[MariadbReference, ...] = _load()
EVIDENCE: MariadbReference = next(
    entry for entry in REFERENCES if entry.role == EVIDENCE_ROLE
)
COMPATIBILITY: tuple[MariadbReference, ...] = tuple(
    entry for entry in REFERENCES if entry.role == COMPATIBILITY_ROLE
)


def validate_compose_pins_the_references() -> None:
    """Il compose deve fissare esattamente i digest dichiarati.

    Due servizi per riferimento — certgen e server — con la **stessa**
    immagine: una fixture TLS generata da una versione diversa da quella che
    poi la serve non prova niente su quella versione.

    # Raises

    `RuntimeError` se il compose nomina un'immagine, un container o una porta
    che il documento non dichiara, o se li nomina per tag invece che per
    digest.
    """

    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    declared = [
        line.strip().removeprefix("image:").strip()
        for line in compose.splitlines()
        if line.strip().startswith("image:")
    ]
    expected = {reference.image for reference in REFERENCES}
    unknown = sorted(set(declared) - expected)
    if unknown:
        raise RuntimeError(f"{COMPOSE_FILE.name} fissa immagini ignote: {unknown}")
    for reference in REFERENCES:
        if declared.count(reference.image) != 2:
            raise RuntimeError(
                f"{COMPOSE_FILE.name}: {reference.label} compare "
                f"{declared.count(reference.image)} volte invece di due "
                "(certgen e server)"
            )
        if f"mariadb:{reference.exact_version}" in compose:
            raise RuntimeError(
                f"{COMPOSE_FILE.name} nomina un tag mutabile accanto al digest"
            )
        if f"container_name: {reference.container}" not in compose:
            raise RuntimeError(
                f"{COMPOSE_FILE.name} non dichiara il container "
                f"{reference.container}"
            )
        if f'"127.0.0.1:{reference.port}:3306"' not in compose:
            raise RuntimeError(
                f"{COMPOSE_FILE.name} non pubblica {reference.label} sulla "
                f"porta {reference.port}"
            )


if __name__ == "__main__":
    for reference in REFERENCES:
        print(
            f"{reference.role:14s} {reference.label:20s} "
            f"{reference.exact_version:8s} {reference.container:22s} "
            f"porta {reference.port}  {reference.image}"
        )
    validate_compose_pins_the_references()
