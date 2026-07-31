# Protocollo errori CLI

Tutti i comandi `plenora-database` emettono gli errori su `stdout` come un
singolo oggetto JSON compatto e terminato da newline. `stderr` resta vuoto e
il processo termina con exit code non-zero.

La versione iniziale del protocollo è:

```json
{
  "status": "error",
  "protocol_version": 1,
  "error": {
    "category": "crs",
    "phase": "validate",
    "remote_effect": "none",
    "retry": {
      "kind": "never"
    },
    "provider": null,
    "execution_id": null,
    "message": "identificatore CRS e SRID numerico divergenti"
  }
}
```

Il campo `error` è la serializzazione `serde` diretta di
`plenora_database_core::DatabaseError`: tutti gli enum usano valori
`snake_case`. `retry` è un oggetto taggato; per `kind = "after"` contiene
anche `delay_ms`.

Gli errori prodotti da `plenora-database-core` conservano i quattro assi fino
al confine CLI. Gli errori locali di sintassi o invocazione sono
`invalid_plan/validate/none/never`; non vengono analizzati a partire da testo
libero.
