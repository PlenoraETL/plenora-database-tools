# Contratti v2

Contratti del dominio database emessi e consumati dal componente.

- `plan.schema.json`: input validato per test, inspect, read e write;
- `capabilities.schema.json`: capability scoperte sul target;
- `read-checkpoint.schema.json`: checkpoint keyset persistente, qualificato per provider, sorgente e ordinamento;
- `age-capabilities.schema.json`: capability AGE v1, separate dal provider relazionale;
- `age-admin-capabilities.schema.json`: capability amministrative AGE additive;
- `loss-report.schema.json`: rapporto legacy del rischio di mapping, senza misura esecutiva;
- `execution-loss-report-v3.schema.json`: rischio, valutazione e perdite effettive;
- `public-operation-contracts.schema.json`: bundle immutabile degli input,
  output e attributi delle operazioni del profilo pubblico;
- `write-outcome.schema.json`: esito legacy v2;
- `write-outcome-v3.schema.json`: committed, rolled back, partial e unknown con rapporto;
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

La 10.0.0 distingue il rischio del piano dalla perdita effettiva nei writer
PostgreSQL e SQL Server. Il rischio resta nel rapporto e, nelle policy
`compatible`, `lossy` e `native`, non richiede consenso: ogni valore consumato
viene controllato prima dell'invio. Se cambierebbe, serve consenso esplicito,
anche se la regola ha severità Warning. Il default nega: Rust puro usa
`LossConsent::Deny` (`LossConsent::Accept` per consentire), CLI `--accept-loss`,
SDK sync/async `accept_loss=True`, runtime `accept_loss: true`.
Strict, chiavi e rifiuti di formato, range, ammissione e capacità restano
vincolanti. Il consenso non apre altri provider. I profili `chunk_committed`
e `best_effort_ddl` non-strict sono rifiutati senza consenso prima degli effetti;
il consenso non rende disponibile un profilo non qualificato.

PostgreSQL non tronca mai testo, neppure spazi finali oltre capacità o con
consenso. Il bulk TDS di SQL Server rifiuta testo e binario oltre capacità anche
con consenso. SQL Server Prepared, nei percorsi ammessi e con consenso,
altera i valori e dichiara le trasformazioni nel rapporto.

Nei writer PostgreSQL/SQL Server, WriteOutcome v3 contiene in `loss_report`
ExecutionLossReport v3:
`risk` descrive il piano; `evaluation` registra i valori controllati e quelli
che cambierebbero; `effective` distingue `committed` da `uncertain`.
Le perdite confermate si pubblicano solo dopo commit confermato. Un rollback
confermato lascia `effective` vuoto e conserva `evaluation`; un esito ignoto
non diventa un commit. `basis: conservative` conta valori trattati come perdita,
non alterazioni provate. Il conteggio è per cella sorgente e regola: sommare
regole diverse non dà il numero di celle uniche. NULL e input vuoto non
richiedono consenso per il solo rischio, fatti salvi i rifiuti indipendenti.
Un dry-run descrive una preparazione distinta, non misura la futura scrittura.
Il perimetro è il mapping Arrow qualificato, non il confronto universale con
trigger, default, funzioni o SQL nativo.

### Schema e compatibilità

[WriteOutcome v3](write-outcome-v3.schema.json) trasporta
[ExecutionLossReport v3](execution-loss-report-v3.schema.json).
La cartella `v2` identifica il bundle, non la versione di ogni documento.
La 10.0.0 è una nuova major: reader chiusi e chiamanti Rust devono adottare
il nuovo esito e il consenso esplicito. Il reader conserva WriteOutcome v2
senza rapporto e LossReport v2 legacy: sono descrizioni senza misura esecutiva,
mai promosse a `effective` vuoto. Assente, null e rapporto vuoto non equivalgono.
Il ramo legacy nello schema v3 resta intenzionalmente leggibile.

Gli esempi nell'[indice](examples/index.json) comprendono rischio con valori
esatti, commit con perdita, rollback dopo rifiuto e conteggi incerti.
Lo schema struttura i campi; il validatore semantico condiviso impone chiavi
univoche, riferimenti al rischio, categorie coerenti e i vincoli sui conteggi.
Le invarianti e i vettori Rust/JSON sono eseguiti da phase0 e self-test.
Il [registro generato](../../docs/loss-registry.md) e l'enum `rule_id` dello
schema derivano da `loss_registry.rs`; non si modificano a mano.

Il binding `database.write@2` usa input/result v2. Il pin adottato resta quello
in [adoption-source.json](../adoption-source.json); la deviazione dal suo
catalogo write@1 è dichiarata e controllata da `check_public_contracts.py`.
Runtime-tools richiede adozione separata di catalogo, `accept_loss` e
WriteOutcome v3; non è attestata da questo repository.

La validazione completa di WriteOutcome v3 comprende lo schema e
`scripts.execution_loss_semantics.validate_outcome`, equivalente a
`WriteOutcome::validate` in Rust. Il confronto fra contatori non e esprimibile
in JSON Schema standard: per ogni campo/regola la somma di `committed` fra
le basi non supera `rows.confirmed`; campi e regole diversi non si sommano.
`rolled_back` non ammette effective positivi; `committed` non ammette
`uncertain`; gli stati parziale e ignoto ammettono incertezza. Resta il vincolo
preesistente che un esito ignoto non conferma righe. Gli esempi e i vettori
condivisi sono verificati dal gate offline. I limiti u64 dei contatori rows
sono espliciti nello schema v3; gli artefatti pubblicati v2 restano invariati.
