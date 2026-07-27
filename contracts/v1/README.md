# Contratti v1

Questa cartella contiene i contratti candidati indipendenti dal linguaggio:

- `plan.schema.json`: input validato per test, inspect, read e write;
- `capabilities.schema.json`: capability scoperte sul target;
- `loss-report.schema.json`: perdite di mapping esplicite;
- `write-outcome.schema.json`: committed, rolled back, partial e unknown;
- `common.schema.json`: identificatori, provider, geometria e policy;
- `golden-manifest.schema.json`: casi di compatibilità semantica.

Gli esempi validi sono registrati in `examples/index.json`. I `$id` usano il
namespace non instradabile `https://plenora.local/database-tools/v1/`; il
validatore costruisce un registry locale e non effettua accessi di rete.

## Versionamento

- campi aggiuntivi non previsti sono rifiutati nei messaggi canonici;
- una modifica incompatibile richiede una nuova major di contratto;
- gli enum vengono trattati come chiusi nella v1;
- `schema_version` deve essere controllato prima dell’esecuzione;
- il JSON descrive control plane e metadata; i dati viaggiano come stream
  Arrow, non come array JSON.

Validazione:

```powershell
python scripts\phase0_validate.py
```
