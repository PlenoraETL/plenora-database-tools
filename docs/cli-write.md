# Schede CLI delle scritture Arrow

Sintassi e routing derivano da `main.rs` e `write_cmd.rs` nel crate CLI;
l'help del binario costruito elenca provider compilati e opzioni disponibili.

## write

```text
plenora-database write --input REQUEST.json --data INPUT.arrow --format json
```

La richiesta canonica contiene `accept_loss: true` per concedere consenso;
assente o false nega. È disponibile anche `--accept-loss`. L'envelope canonico
resta distinto dall'output legacy di `database-write-ipc`.

## database-write-ipc

```text
plenora-database database-write-ipc <provider> <secret-env> <WRITE.json> <INPUT.arrow> --accept-loss --format json
```

Il flag concede consenso separatamente dalla policy nel documento di operazione.
Il provider e i suoi argomenti devono corrispondere al binario costruito.

## bulk-write e alias PostgreSQL

```text
plenora-database bulk-write <dsn-env> <WRITE.json> <INPUT.arrow> --accept-loss --format json
```

`bulk-write` è PostgreSQL, non un comando SQL Server. `postgres-write-ipc`
resta un alias con policy Strict: accettare il flag non indebolisce Strict.
Un dry-run è una preparazione distinta e non misura perdite avvenute.

## Rapporto e migrazione

JSON, Markdown e JUnit conservano `risk`, `evaluation`, `effective` e gli assi
dell'errore; selezionare il formato con `--format json|markdown|junit`.
Non usare il solo exit code per decidere un retry.

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

### Migrazione da 8.x e 9.x

**Utenti 8.x SQL Server:** la misura del 12/11/2026 su 8.0.4 ha trovato
nel percorso Prepared spazi finali testuali, zeri finali binari oltre capacità
e padding dei tipi fissi alterati senza dichiarazione, anche con `strict`.
La misura riguarda Rust diretto/Provider, CLI e SDK; non qualifica async,
runtime, ORM o portable 8.x. TDS bulk nella misura produce solo rifiuti o
valori esatti. Verificare i dati già scritti contro la sorgente, soprattutto
suffissi e lunghezze; il rapporto nuovo non ricostruisce le perdite storiche.
Aggiornare alla nuova major quando disponibile e riprovare Strict sui propri
casi prima di riabilitare scritture; non usare il vecchio Strict come prova
di conservazione. Il difetto Prepared è corretto dalla 9.0.0.

**Utenti 9.0.0–9.0.3:** CLI, SDK e runtime non rendevano visibili le perdite
ammesse del piano. La misura 9.0.3 rileva 72 casi CLI/SDK SQL Server Prepared
non-strict; Strict non presenta tali alterazioni nella misura. Verificare i
dati storici contro la sorgente, aggiornare producer e reader alla 10.0.0,
leggere `risk` / `evaluation` / `effective` e concedere consenso solo alle
trasformazioni volute. Non dedurre assenza di perdite da un rapporto mancante.
Prima di un retry leggere gli assi dell'errore e l'esito: un commit confermato
non va ripetuto; un esito incerto richiede verifica sul target.

Il binding `database.write@2` richiede adozione lato runtime-tools di catalogo,
`accept_loss` e WriteOutcome v3. Le [regole](loss-registry.md) sono generate dal
registro; [schema ed esempi](../contracts/v2/README.md) descrivono il formato.
