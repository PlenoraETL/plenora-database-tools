"""Il default TLS del SDK resta sicuro (ADR-011).

Gli altri moduli aprono la sessione con `tls_mode="insecure_local"` tramite
`_harness`, perche il riferimento di sviluppo e plaintext. Quella deroga
renderebbe invisibile una regressione sul default — se `connect()` smettesse
di pretendere TLS, nessun test funzionale se ne accorgerebbe, perche nessuno
usa piu il default.

Questi test lo usano, e sono gli unici. Verificano la proprieta al contrario:
contro un riferimento che **non** parla TLS, il default deve fallire.
"""

from __future__ import annotations

import pytest

import plenora_database as p

from ._harness import postgres_dsn_or_skip


def test_the_default_refuses_a_plaintext_reference() -> None:
    """`connect(dsn)` senza tls_mode non parla con un server in chiaro."""

    dsn = postgres_dsn_or_skip()
    with pytest.raises(p.PlenoraError) as raised:
        p.engine_from_url(p.EngineConfig.from_postgres_dsn(dsn))
    # Il fallimento e nella negoziazione, non nell'autenticazione: la
    # distinzione conta, perche un errore di auth suggerirebbe credenziali
    # sbagliate invece di un canale rifiutato.
    assert raised.value.phase in {"connect", "probe"}, raised.value.phase


@pytest.mark.asyncio
async def test_the_async_default_refuses_a_plaintext_reference() -> None:
    """Stessa garanzia sul percorso async: il default non diverge."""

    dsn = postgres_dsn_or_skip()
    with pytest.raises(p.PlenoraError) as raised:
        await p.async_engine_from_url(p.EngineConfig.from_postgres_dsn(dsn))
    assert raised.value.phase in {"connect", "probe"}, raised.value.phase


def test_the_insecure_switch_has_to_be_named() -> None:
    """Non esiste un modo implicito di disattivare TLS.

    Un valore sconosciuto non degrada a "niente TLS": viene rifiutato. Se
    degradasse, un errore di battitura nel nome della modalita disattiverebbe
    la verifica in silenzio.
    """

    dsn = postgres_dsn_or_skip()
    with pytest.raises(Exception) as raised:
        p.engine_from_url(
            p.EngineConfig.from_postgres_dsn(dsn, tls_mode="insecure")
        )
    # Il messaggio elenca i valori ammessi: e cio che rende il rifiuto
    # correggibile invece che solo bloccante.
    message = str(raised.value)
    assert "tls_mode" in message, message
    assert "insecure_local" in message, message
