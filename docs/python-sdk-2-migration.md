# Migrazione al Python SDK 2.0

La 2.0 riduce la superficie pubblica a un solo lifecycle e rende esplicite le
decisioni che nella 1.x potevano cambiare semantica fra provider.

## Connessione e lifecycle

`connect*`, `aconnect*` e le factory `create_*_engine` per singolo provider non
fanno parte della superficie 2.0. Si costruisce un `EngineConfig` oppure si
passa un URL a `engine_from_url`; per asyncio si usa
`async_engine_from_url`. Una sessione nasce sempre dall'engine e non possiede
un pool indipendente.

`PoolConfig(max_connections=..., acquire_timeout_ms=...)` rende esplicita la
backpressure. Db2 rifiuta una configurazione pool finche il provider ODBC non
dispone di una prova riproducibile equivalente.

```python
from plenora_database import EngineConfig, engine_from_url

config = EngineConfig.from_url("postgresql://user:password@db/app")
with engine_from_url(config) as engine:
    with engine.session() as session:
        ...
```

## Risultati

`Result` itera e restituisce soltanto `Row`. I terminali `all`, `first`, `one`
e `one_or_none` non restituiscono piu `dict`; la conversione e deliberata con
`row.as_dict()`. I duplicati `rows`, `row_first`, `row_one` e
`row_one_or_none` sono rimossi. Le mutazioni restituiscono `MutationResult` e
richiedono di ispezionare esplicitamente `affected_rows`.

`execute` accetta soltanto statement portabili. Il SQL nativo passa da
`execute_sql`, le query native da `query_sql`; questa separazione rende
visibile il confine di portabilita anche nei type checker.

## Bind e bulk mapping

Ogni `bind` pubblico dichiara un `BindType`; non esiste inferenza dal valore
Python. Ogni operazione bulk dichiara inoltre `mapping_policy` fra `strict`,
`compatible`, `lossy` e `native`: l'assenza della policy e un errore prima di
contattare il database.

I loader ORM non accettano piu percorsi stringa: `selectinload` e `joinedload`
ricevono attributi `Relationship`, inclusi i percorsi annidati. I protocolli
`SessionProtocol` e `AsyncSessionProtocol` permettono a framework e adapter di
tipizzare una sessione senza dipendere dalla classe concreta.

## Errori

Gli errori pubblici derivano da `PlenoraError` e mantengono messaggi privi di
payload applicativi, DSN, token e SQL bindato. Il chiamante decide retry e
recovery usando gli attributi strutturati, non analizzando il testo.

## Migrazioni

La history 2.0 registra checksum e stato, rileva drift, serializza i runner con
un lock per provider e conserva uno stato recuperabile se una migrazione non
si conclude. Una revisione applicata con checksum diverso blocca il runner.
Gli stati `running` e `failed` richiedono una scelta esplicita tramite
`recover`: cancellazione per rieseguire oppure `assume_applied=True` dopo una
verifica operativa esterna.

## Distribuzione

La matrice Python supportata viene verificata staticamente in CI. Ogni wheel di
release e accompagnato da SBOM e attestazione di provenienza; la pubblicazione
su PyPI avviene solo dall'environment GitHub protetto del workflow di release.
La matrice dichiarata e provata copre CPython 3.10–3.14.
