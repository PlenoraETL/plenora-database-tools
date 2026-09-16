# Contratti v2

Contratti del dominio database emessi e consumati dal componente.

- `plan.schema.json`: input validato per test, inspect, read e write;
- `capabilities.schema.json`: capability scoperte sul target;
- `read-checkpoint.schema.json`: checkpoint keyset persistente, qualificato per provider, sorgente e ordinamento;
- `age-capabilities.schema.json`: capability AGE v1, separate dal provider relazionale;
- `age-admin-capabilities.schema.json`: capability amministrative AGE additive;
- `loss-report.schema.json`: perdite di mapping esplicite;
- `public-operation-contracts.schema.json`: bundle immutabile degli input,
  output e attributi delle operazioni del profilo pubblico;
- `write-outcome.schema.json`: committed, rolled back, partial e unknown;
- `common.schema.json`: identificatori, provider, oggetti e policy;
- `golden-manifest.schema.json`: casi di compatibilità semantica.

La selezione del profilo comune e la revisione immutabile di
`plenora-contracts` sono dichiarate una sola volta in
[`../adoption-source.json`](../adoption-source.json).
La mappa operazione/export della superficie Rust e invece emessa dal catalogo
nel modulo pubblico
[`public_contract`](../../crates/plenora-database-core/src/public_contract.rs),
cosi non esiste una seconda lista da mantenere a mano.

I `$id` usano il namespace non instradabile
`https://plenora.local/database-tools/v2/`, e nessuno di essi referenzia
un'altra major. Gli esempi validi sono in `examples/index.json`.

## Semantica

`writes.append` e `writes.truncate_insert` autorizzano operazioni distinte:
append conserva le righe esistenti; truncate-insert le sostituisce. Il
consumatore deve interrogare la capability della modalita richiesta e il
profilo transazionale disponibile.

I riferimenti agli oggetti usano catalogo, schema e nome. Le operazioni sono
`database.*`, i profili descrivono transazioni SQL e gli esiti di scrittura
contano righe. Le dichiarazioni dei provider sono generate in
[`docs/STATO.md`](../../docs/STATO.md).

## Validazione

`scripts/phase0_validate.py` controlla schemi, esempi, golden e confini del
dominio. Il controllo `active-contract-domain` rifiuta riferimenti a major
diverse da quella attiva e operazioni estranee al dominio database.
