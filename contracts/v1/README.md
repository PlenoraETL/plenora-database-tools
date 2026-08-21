# Contratti v1 — ritirati

**Questa major e ritirata.** La major attiva e `contracts/v2/`: e li che vive
il contratto che il codice emette e che i gate misurano. Questa cartella resta
per una ragione sola — chi si trova in mano un documento `schema_version: 1`
deve poterlo ancora interpretare — e non e piu referenziata da nessuno.

Nessun file qui dentro cambia piu. Se qualcosa va corretto, si corregge nella
major attiva.

Contiene:

- `plan.schema.json`: input validato per test, inspect, read e write;
- `capabilities.schema.json`: capability scoperte sul target;
- `loss-report.schema.json`: perdite di mapping esplicite;
- `write-outcome.schema.json`: committed, rolled back, partial e unknown;
- `common.schema.json`: identificatori, provider, geometria e policy;
- `golden-manifest.schema.json`: casi di compatibilità semantica.

Gli esempi validi sono registrati in `examples/index.json`. I `$id` usano il
namespace non instradabile `https://plenora.local/database-tools/v1/`; il
validatore costruisce un registry locale e non effettua accessi di rete.

## Perche e stata ritirata

Due rotture, arrivate una dopo l'altra:

1. `writes.append` valeva per due write mode, `Append` e `TruncateInsert`, che
   sono due promesse diverse su cosa succede alle righe che c'erano prima.
   Separarle ha richiesto un campo nuovo e richiesto;
2. questa versione modella **anche ArcGIS**, che non e un motore di database.
   `provider_kind` lo elenca, `plan.schema.json` porta otto operazioni
   `arcgis.*`, `object_ref` ha un `layer_id`, `write-outcome` ha gli esiti per
   layer, e le capability hanno `apply_edits` e `use_global_ids`. Quel dominio
   — autenticazione HTTP, pagination, rate limit, retry, Feature Service — non
   e questo, e continuera altrove.

La seconda rottura e la ragione per cui la v2 non referenzia piu nulla di qui:
un contratto database-only che prendesse `provider_kind` da questo file
tornerebbe ad avere ArcGIS fra i provider ammessi.

## Versionamento

- campi aggiuntivi non previsti sono rifiutati nei messaggi canonici;
- una modifica incompatibile richiede una nuova major di contratto;
- gli enum vengono trattati come chiusi;
- `schema_version` deve essere controllato prima dell’esecuzione;
- il JSON descrive control plane e metadata; i dati viaggiano come stream
  Arrow, non come array JSON.

Validazione:

```powershell
python scripts\phase0_validate.py
```
