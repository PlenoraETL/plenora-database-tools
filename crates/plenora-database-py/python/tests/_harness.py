"""Connessioni condivise per i test funzionali del SDK.

Sedici moduli avevano ciascuno la propria copia di `_dsn_or_skip`, e ciascuno
apriva la sessione con `p.connect(dsn)`. Dopo ADR-011 quel default significa
TLS obbligatorio con verifica WebPKI, mentre il riferimento di sviluppo
`dataflow-postgres` e **plaintext** per costruzione — il riferimento TLS e un
compose separato, `dataflow-postgres-tls`, con la sua CA privata. Il risultato
era che con la DSN impostata la suite non saltava piu i test: falliva in
`connect`, centocinquantatre volte, per una ragione che non riguardava nessuna
delle proprieta verificate.

L'interruttore vive qui, in un punto solo, e ha un nome che dice cosa fa. I
test che verificano il **default** sicuro non passano da questi helper: usano
`p.connect` / `p.aconnect` direttamente, perche il loro oggetto e proprio cio
che gli helper aggirano. Vederli in `test_tls_default.py` accanto a questa
nota e il modo in cui la deroga resta visibile.
"""

from __future__ import annotations

import os

import pytest

import plenora_database as p

POSTGRES_DSN_ENV = "PLENORA_TEST_POSTGRES_DSN"

# Il riferimento di sviluppo non parla TLS: chiederlo qui e l'unica differenza
# rispetto a un uso di produzione del SDK.
LOCAL_TLS_MODE = "insecure_local"


def postgres_dsn_or_skip() -> str:
    """La DSN del riferimento, o salta il test se non e configurata."""

    dsn = os.environ.get(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"live test: manca env {POSTGRES_DSN_ENV}")
    return dsn


def connect_postgres(dsn: str | None = None):
    """Sessione sync verso il riferimento plaintext.

    Salta il test se la DSN non e configurata.
    """

    return p.connect(dsn or postgres_dsn_or_skip(), LOCAL_TLS_MODE)


async def aconnect_postgres(dsn: str | None = None):
    """Sessione async verso il riferimento plaintext.

    Salta il test se la DSN non e configurata.
    """

    return await p.aconnect(dsn or postgres_dsn_or_skip(), LOCAL_TLS_MODE)
