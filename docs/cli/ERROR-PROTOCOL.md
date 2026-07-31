# Protocollo errori CLI

Tutti i comandi `plenora-database` emettono gli errori su `stderr` come un
singolo oggetto JSON compatto e terminato da newline. `stdout` resta vuoto e
il processo termina con exit code non-zero.

La versione iniziale del protocollo è:

```json
{
  "status": "error",
  "protocol_version": 1,
  "error": {
    "category": "Crs",
    "phase": "Validate",
    "remote_effect": "none",
    "retry": "never",
    "message": "identificatore CRS e SRID numerico divergenti"
  }
}
```

`category` e `phase` mantengono i nomi canonici dei tipi Rust.
`remote_effect` e `retry` usano valori `snake_case`. Per
`retry = "after"` la busta contiene anche `retry_delay_ms`.

Gli errori prodotti da `plenora-database-core` conservano i quattro assi fino
al confine CLI. Gli errori locali di sintassi o invocazione sono
`InvalidPlan/Validate/none/never`; non vengono analizzati a partire da testo
libero.
