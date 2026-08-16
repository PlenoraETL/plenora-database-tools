#!/usr/bin/env python3
"""Matrice MySQL qualificata: unica fonte di verita per versione e digest.

Il file `docker/mysql/references.json` dichiara ogni riferimento con la
versione esatta e il digest immutabile del manifest index. Questo modulo lo
carica e ne deriva tutto il resto — nome container, volumi, prefisso di
versione atteso dai test — cosi che nessun gate, nessun compose e nessun test
possa affermare una versione diversa da quella effettivamente avviata.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REFERENCES_FILE = ROOT / "docker" / "mysql" / "references.json"
COMPOSE_FILE = ROOT / "docker-compose.mysql.yml"

BASELINE_ROLE = "baseline"
COMPATIBILITY_ROLE = "compatibility"


@dataclass(frozen=True)
class MysqlReference:
    """Un riferimento qualificato della matrice."""

    role: str
    label: str
    exact_version: str
    digest: str

    @property
    def image(self) -> str:
        return f"mysql@{self.digest}"

    @property
    def version_prefix(self) -> str:
        major_minor, _, _patch = self.exact_version.rpartition(".")
        return f"{major_minor}."

    @property
    def slug(self) -> str:
        return self.version_prefix.replace(".", "").strip()

    @property
    def container(self) -> str:
        return f"plenora-matrix-mysql-{self.slug}"

    @property
    def aliases(self) -> tuple[str, ...]:
        # Il certificato della fixture e emesso solo per dataflow-mysql.
        # L'alias risolvibile ma assente dai SAN rende deterministica la prova
        # TLS negativa senza introdurre un falso errore DNS.
        return ("dataflow-mysql", "mysql-hostname-mismatch")

    @property
    def ca_volume(self) -> str:
        return f"plenora-matrix-mysql-{self.slug}-ca"

    @property
    def tls_volume(self) -> str:
        return f"plenora-matrix-mysql-{self.slug}-tls"


def _load() -> tuple[MysqlReference, ...]:
    document = json.loads(REFERENCES_FILE.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise RuntimeError("schema della matrice MySQL non riconosciuto")
    entries = tuple(
        MysqlReference(
            role=entry["role"],
            label=entry["label"],
            exact_version=entry["exact_version"],
            digest=entry["digest"],
        )
        for entry in document["references"]
    )
    if not entries:
        raise RuntimeError("matrice MySQL vuota")
    for entry in entries:
        if not entry.digest.startswith("sha256:") or len(entry.digest) != 71:
            raise RuntimeError(f"digest non immutabile per {entry.label}")
        if entry.exact_version.count(".") != 2:
            raise RuntimeError(f"versione non esatta per {entry.label}")
    baselines = [entry for entry in entries if entry.role == BASELINE_ROLE]
    if len(baselines) != 1:
        raise RuntimeError("la matrice MySQL deve dichiarare una sola baseline")
    roles = {entry.role for entry in entries}
    if not roles <= {BASELINE_ROLE, COMPATIBILITY_ROLE}:
        raise RuntimeError("ruolo sconosciuto nella matrice MySQL")
    versions = [entry.exact_version for entry in entries]
    if len(set(versions)) != len(versions):
        raise RuntimeError("versione duplicata nella matrice MySQL")
    digests = [entry.digest for entry in entries]
    if len(set(digests)) != len(digests):
        raise RuntimeError("digest duplicato nella matrice MySQL")
    return entries


REFERENCES: tuple[MysqlReference, ...] = _load()
BASELINE: MysqlReference = next(
    entry for entry in REFERENCES if entry.role == BASELINE_ROLE
)
COMPATIBILITY: tuple[MysqlReference, ...] = tuple(
    entry for entry in REFERENCES if entry.role == COMPATIBILITY_ROLE
)


def validate_compose_pins_the_baseline() -> None:
    """Il compose del riferimento deve fissare il digest della baseline.

    I due servizi (certgen e server) devono usare la stessa immagine: una
    fixture TLS generata da una versione diversa da quella che poi la serve
    non e una prova della baseline.

    # Raises

    `RuntimeError` quando il compose non fissa esattamente due volte
    l'immagine della baseline o quando cita un altro digest MySQL.
    """

    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    if compose.count(BASELINE.image) != 2:
        raise RuntimeError(
            f"docker-compose.mysql.yml non fissa {BASELINE.image} sui due servizi"
        )
    for entry in COMPATIBILITY:
        if entry.digest in compose:
            raise RuntimeError(
                f"docker-compose.mysql.yml cita il digest di compatibilita {entry.label}"
            )


if __name__ == "__main__":
    validate_compose_pins_the_baseline()
    for reference in REFERENCES:
        print(f"{reference.role:14} {reference.label:24} {reference.exact_version:8} {reference.image}")
