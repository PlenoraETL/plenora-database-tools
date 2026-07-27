# ADR 0003 — Outcome di scrittura e incertezza

Stato: **accettato**  
Data: 2026-07-26

## Contesto

Una connessione può interrompersi dopo che il client ha inviato `COMMIT` ma
prima della risposta. Lo stesso vale per una richiesta ArcGIS `applyEdits`.
Classificare sempre l'errore come rollback o ritentarlo può duplicare dati.

## Decisione

Outcome canonici:

```rust
enum WriteOutcome {
    Committed(CommitReport),
    RolledBack(RollbackReport),
    PartiallyCommitted(PartialCommitReport),
    OutcomeUnknown(RecoveryReport),
}
```

Stati minimi:

```text
NotStarted
→ SessionReady
→ TransactionOrEditPrepared
→ Writing
→ Finalizing
→ CommitOrEditRequested
→ Committed | RolledBack | PartiallyCommitted | OutcomeUnknown
```

Regole:

- dopo `CommitOrEditRequested`, senza prova dell'esito si restituisce
  `OutcomeUnknown`;
- non si ritenta automaticamente un'operazione incerta;
- una sessione SQL incerta non torna nel pool;
- ArcGIS riporta errori per feature, batch e layer;
- il profilo per-batch può produrre `PartiallyCommitted`;
- staging/service/layer e idempotency key entrano nel recovery report;
- errori e recovery non contengono segreti o valori.

## Conseguenze

- il nuovo modello è più preciso del `WriteStatus` Python;
- i caller devono gestire recovery;
- retry è capability/semantica, non proprietà generica dell'errore;
- test fault-injection sul confine commit/edit sono obbligatori.

## Alternative scartate

- trattare ogni timeout come rollback;
- trattare ogni timeout come successo;
- retry automatico indiscriminato;
- nascondere i commit parziali dietro un generico `PARTIAL`.

