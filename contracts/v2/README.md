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

## Scritture Arrow: rapporto e consenso

Dalla release 10.0.0 l'esito e WriteOutcome v3, con `loss_report` del piano
eseguito (LossReport v2). Lo schema e in `contracts/v2/write-outcome-v3.schema.json`:
la cartella v2 identifica il bundle esistente, la versione dell'esito e esplicita
nel nome e nel documento. Il reader Rust accetta ancora v2 senza rapporto;
non equipara quell'assenza a `losses: []` di un piano preparato.

CLI `database-write-ipc`, `bulk-write`, `postgres-write-ipc` accettano
`--accept-loss`; la write canonica e il runtime accettano `accept_loss: true`
nel documento della richiesta. Senza consenso separato dalla policy, DataLoss
rifiuta l'esecuzione prima degli effetti. Information/Warning non richiedono
consenso. Il rapporto resta nei formati JSON, Markdown e JUnit, e negli esiti
parziali/incerti trasportati dall'errore. Un dry-run e una preparazione distinta,
non il rapporto della successiva esecuzione. SQL nativo e trasformazioni server
non osservate non sono coperti da un rapporto Arrow vuoto.

Il binding annuncia `database.write@2`, input/result v2; i consumer devono
negoziare questa versione e validare WriteOutcome v3. Le altre operazioni
conservano la loro versione. Vedere gli avvisi 8.x e 9.0.0–9.0.3 nel CHANGELOG.
