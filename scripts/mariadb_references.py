#!/usr/bin/env python3
"""Il riferimento MariaDB: una fonte sola per versione e digest.

`docker/mariadb/references.json` dichiara l'unica immagine del ciclo di
evidenza, con la versione esatta e il digest immutabile del manifest index.
Questo modulo lo carica e ne deriva il resto — nome del container, volumi,
alias — cosi che ne il compose ne uno script possano affermare una versione
diversa da quella effettivamente avviata.

Il ruolo si chiama `evidence` e non `baseline` di proposito: MariaDB **non**
e un riferimento qualificato. Il provider `mysql` fa fail-close alla probe
quando la riconosce, e questa fixture esiste per produrre l'evidenza che
ADR 0014 chiede prima di decidere se qualificarla. Chiamarla baseline la
farebbe leggere come una piattaforma supportata, che e proprio l'equivoco che
il fail-close esiste per impedire.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REFERENCES_FILE = ROOT / "docker" / "mariadb" / "references.json"
COMPOSE_FILE = ROOT / "docker-compose.mariadb.yml"

EVIDENCE_ROLE = "evidence"
CONTAINER = "dataflow-mariadb"
# Un nome risolvibile che il certificato non copre: rende deterministica la
# prova TLS negativa senza mascherarla da errore DNS.
MISMATCH_ALIAS = "mariadb-hostname-mismatch"


@dataclass(frozen=True)
class MariadbReference:
    """L'immagine su cui gira il ciclo di evidenza."""

    role: str
    label: str
    exact_version: str
    digest: str

    @property
    def image(self) -> str:
        return f"mariadb@{self.digest}"

    @property
    def version_prefix(self) -> str:
        major_minor, _, _patch = self.exact_version.rpartition(".")
        return f"{major_minor}."


def _load() -> tuple[MariadbReference, ...]:
    document = json.loads(REFERENCES_FILE.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise RuntimeError("schema del riferimento MariaDB non riconosciuto")
    entries = tuple(
        MariadbReference(
            role=entry["role"],
            label=entry["label"],
            exact_version=entry["exact_version"],
            digest=entry["digest"],
        )
        for entry in document["references"]
    )
    if not entries:
        raise RuntimeError("riferimento MariaDB vuoto")
    for entry in entries:
        # `sha256:` piu 64 esadecimali: un tag al posto del digest sarebbe
        # mutabile, e la riga smetterebbe di provare quale immagine e girata.
        if not entry.digest.startswith("sha256:") or len(entry.digest) != 71:
            raise RuntimeError(f"digest non immutabile per {entry.label}")
        if entry.exact_version.count(".") != 2:
            raise RuntimeError(f"versione non esatta per {entry.label}")
        if entry.role != EVIDENCE_ROLE:
            raise RuntimeError(
                f"ruolo '{entry.role}' per {entry.label}: MariaDB non e "
                "qualificata, e l'unico ruolo ammesso e 'evidence'"
            )
    versions = [entry.exact_version for entry in entries]
    if len(set(versions)) != len(versions):
        raise RuntimeError("versione duplicata nel riferimento MariaDB")
    return entries


REFERENCES: tuple[MariadbReference, ...] = _load()
EVIDENCE: MariadbReference = REFERENCES[0]


def validate_compose_pins_the_reference() -> None:
    """Il compose deve fissare il digest dichiarato, in entrambi i servizi.

    Certgen e server devono usare la stessa immagine: una fixture TLS
    generata da una versione diversa da quella che poi la serve non prova
    niente su quella versione.

    # Raises

    `RuntimeError` se il compose nomina un'immagine diversa, la nomina per
    tag invece che per digest, o non la nomina abbastanza volte.
    """

    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    declared = [
        line.strip()
        for line in compose.splitlines()
        if line.strip().startswith("image:")
    ]
    if len(declared) != 2:
        raise RuntimeError(
            f"{COMPOSE_FILE.name} dichiara {len(declared)} immagini invece di due"
        )
    for line in declared:
        image = line.removeprefix("image:").strip()
        if image != EVIDENCE.image:
            raise RuntimeError(
                f"{COMPOSE_FILE.name} fissa '{image}' invece di {EVIDENCE.image}"
            )
    if f"mariadb:{EVIDENCE.exact_version}" in compose:
        raise RuntimeError(
            f"{COMPOSE_FILE.name} nomina un tag mutabile accanto al digest"
        )


if __name__ == "__main__":
    print(f"{EVIDENCE.label}: {EVIDENCE.exact_version} ({EVIDENCE.image})")
    validate_compose_pins_the_reference()
