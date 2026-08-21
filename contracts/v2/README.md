# Contratti v2 — la major attiva

Questa e la major che il codice emette e che i gate misurano. Contiene
l'insieme completo dei messaggi:

- `plan.schema.json`: input validato per test, inspect, read e write;
- `capabilities.schema.json`: capability scoperte sul target;
- `loss-report.schema.json`: perdite di mapping esplicite;
- `write-outcome.schema.json`: committed, rolled back, partial e unknown;
- `common.schema.json`: identificatori, provider, geometria e policy;
- `golden-manifest.schema.json`: casi di compatibilità semantica.

I `$id` usano il namespace non instradabile
`https://plenora.local/database-tools/v2/`, e nessuno di essi referenzia la
`v1`, che e ritirata. Gli esempi validi sono in `examples/index.json`.

## Cosa cambia rispetto alla v1

Due rotture indipendenti, arrivate insieme perche la seconda ha reso
inutilizzabile il compromesso con cui era nata la prima.

### 1. `append` e `truncate_insert` sono due promesse

`writes.truncate_insert` e un campo nuovo e **richiesto**.

Fino alla v1 non esisteva, e `writes.append` valeva per due mode: l'engine la
consultava sia per `Append` sia per `TruncateInsert`. Sono due promesse
diverse — `Append` lascia le righe che c'erano, `TruncateInsert` le toglie, e
il modo in cui le toglie decide se un fallimento e recuperabile — e tenerle
insieme faceva dire il falso al contratto: `MySQL` pubblicava `append = true` e
rifiutava `TruncateInsert` in prepare, perche li `TRUNCATE TABLE` e DDL con
commit implicito. Le righe sparirebbero prima dell'INSERT e nessun rollback le
riporterebbe indietro. Il contratto prometteva quindi una mode che il provider
negava, e il consumatore lo scopriva a piano gia compilato.

### 2. Il dominio sono i database

La v1 modellava anche ArcGIS. Non era un dettaglio di un esempio: era il
modello. Qui non c'e piu nulla di tutto questo.

| via dalla v1 | dove stava | cosa descriveva |
| --- | --- | --- |
| `arcgis` | `provider_kind` | un provider che non e un motore di database |
| otto operazioni `arcgis.*` | `plan.schema.json` | folders, items, services, layers |
| il ramo `allOf` sulle famiglie | `plan.schema.json` | teneva separate due famiglie di operazioni |
| `arcgis_apply_edits` | `transaction_profile` | il profilo transazionale di applyEdits |
| `layer_id` | `object_ref` | l'indirizzamento per layer di un Feature Service |
| `layer_outcomes` | `write-outcome.schema.json` | l'esito per layer di un applyEdits |
| `commit_or_edit_requested` | `recovery.last_certain_phase` | ora `commit_requested`: un database non richiede edit |
| `apply_edits`, `use_global_ids` | `capabilities.writes` | operazioni e identita di un Feature Service |
| `object_id_windows` | `capabilities.reads` | la paginazione per finestre di objectId |
| `max_record_count` | `capabilities.limits` | il `maxRecordCount` di un servizio |
| `edit_request`, `layer`, `service` | `transactions.scope` | scope transazionali che un database non ha |
| `feature_service` | `spatial_semantics` | una semantica spatial che il codice non ha mai avuto |
| `arcgis_features_v1`, categoria `arcgis` | `golden-manifest.schema.json` | dataset e casi di un altro dominio |

Nessuno di questi campi era popolato da un provider database: erano `false`,
`null`, vuoti o rifiutati in preflight. Il costo di tenerli non era a runtime,
era nel contratto — che descriveva un dominio piu largo di quello che questo
repository serve, e obbligava ogni provider nuovo a dichiarare di non essere
una cosa che nessuno gli aveva chiesto di essere.

L'elenco dei provider in `golden-manifest.schema.json` era una seconda copia di
`provider_kind`: ora lo referenzia. Due copie della stessa lista divergono alla
prima modifica, e qui erano gia divergenti.

## Migrazione

Da v1 a v2, per un documento capability:

1. `schema_version` passa da `1` a `2`;
2. si aggiunge `writes.truncate_insert`;
3. si tolgono `writes.apply_edits`, `writes.use_global_ids`,
   `reads.object_id_windows` e `limits.max_record_count`.

Il valore di `truncate_insert` **non** e sempre quello di `append`. Sotto la v1
`append` autorizzava entrambe le mode, quindi un documento che diceva
`append: true` prometteva anche `TruncateInsert`; la traduzione fedele di quel
documento e `truncate_insert: true`. Ma se il provider che lo ha prodotto
rifiuta `TruncateInsert` — come fanno `MySQL` e `MariaDB` — la traduzione
fedele e `false`, e la v2 e il momento in cui quella differenza smette di
essere invisibile. Nel repository: PostgreSQL e SQL Server dichiarano `true`,
MySQL e MariaDB `false`.

Per un piano: `schema_version` passa a `2`, e `object_ref` non porta piu
`layer_id`. Un piano con un'operazione `arcgis.*` non e migrabile — non e un
piano database.

Per un write outcome: `schema_version` passa a `2`, `layer_outcomes` non esiste
piu, e `commit_or_edit_requested` diventa `commit_requested`.

## Chi presidia il confine

`scripts/phase0_validate.py`, con il check `active-contract-domain`: legge il
testo degli schemi, degli esempi e della suite golden attivi e fallisce se
compare un termine di un altro dominio, o se un `$ref` della major attiva punta
a una major ritirata. Il secondo caso e quello che conta: un contratto puo
essere ripulito e continuare a referenziare, per una definizione comune, il
file da cui la superficie e stata tolta — e allora la superficie e ancora li.
