# Contratti v2 — capability

Questa cartella contiene **solo** il contratto che ha cambiato major:
`capabilities.schema.json`. Gli altri messaggi — piano, loss report, write
outcome, definizioni comuni, golden manifest — non sono cambiati e restano
quelli della `v1`, che questa versione referenzia per `$id` invece di
duplicarli: due copie della stessa definizione, con due namespace,
divergerebbero alla prima modifica.

La `v1` resta **immutabile**. Un consumatore che parla v1 continua a validare
i propri documenti contro `contracts/v1/capabilities.schema.json`, e quel file
non e stato toccato.

## Cosa cambia, e perche

`writes.truncate_insert` e un campo nuovo e **richiesto**.

Fino alla v1 non esisteva, e `writes.append` valeva per due mode: l'engine la
consultava sia per `Append` sia per `TruncateInsert`. Sono due promesse
diverse — `Append` lascia le righe che c'erano, `TruncateInsert` le toglie, e
il modo in cui le toglie decide se un fallimento e recuperabile — e tenerle
insieme faceva dire il falso al contratto:

* `MySQL` pubblicava `append = true` e rifiutava `TruncateInsert` in prepare,
  perche li `TRUNCATE TABLE` e DDL con commit implicito: le righe sparirebbero
  prima dell'INSERT e nessun rollback le riporterebbe indietro. Il contratto
  prometteva quindi una mode che il provider negava, e il consumatore lo
  scopriva a piano gia compilato.

Aggiungere un campo richiesto e una rottura: un documento v1 non lo porta, e un
validatore v1 rifiuta un documento che lo porta, perche i messaggi canonici non
ammettono proprieta impreviste. Per la regola di versionamento della `v1` —
"una modifica incompatibile richiede una nuova major di contratto" — questo e
esattamente il caso, e per questo esiste la v2 invece di una nota di rilascio.

## Migrazione

Da v1 a v2, per un documento capability:

1. `schema_version` passa da `1` a `2`;
2. si aggiunge `writes.truncate_insert`.

Il valore da mettere **non** e sempre quello di `append`. Sotto la v1 `append`
autorizzava entrambe le mode, quindi un documento che diceva `append: true`
prometteva anche `TruncateInsert`; la traduzione fedele di quel documento e
`truncate_insert: true`. Ma se il provider che lo ha prodotto rifiuta
`TruncateInsert` — come fanno `MySQL` e `MariaDB` — la traduzione fedele e
`false`, e la v2 e il momento in cui quella differenza smette di essere
invisibile.

Nel repository: PostgreSQL e SQL Server dichiarano `true`, MySQL e MariaDB
dichiarano `false`.

## Cosa non c'e piu

La v1 portava un esempio di capability ArcGIS. Qui non c'e, e non e una
dimenticanza: ArcGIS non e un motore di database, e la sua superficie —
autenticazione HTTP, pagination, rate limit, retry, Feature Service,
applyEdits, esiti per feature e per layer — appartiene a un altro dominio.
Tenerne un esempio in un contratto database-only lo farebbe leggere come un
provider fra gli altri.

L'esempio v1 resta dov'e, come materiale storico: la v1 e immutabile.
