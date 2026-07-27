# ADR 0008 — Gate per runtime e driver

Stato: **accettato**  
Data: 2026-07-27

## Contesto

La scelta di runtime e client Rust influenza pooling, streaming, cancellazione,
Oracle/Db2 client libraries e compatibilità delle licenze. Deciderla senza
spike contro server reali trasformerebbe ipotesi in architettura.

## Decisione

Il core è runtime-agnostic:

- contratti, schema, mapping, SQL AST e outcome non dipendono da un executor;
- gli stream espongono backpressure e cancellazione;
- ogni adapter può essere async nativo oppure isolare un client blocking in
  worker bounded;
- nessuna crate driver entra nell’API pubblica.

La selezione finale di runtime, pool e crate driver è un gate della Fase 1.
Per ogni provider sono obbligatori uno spike compilabile e una prova reale di:

1. streaming senza materializzazione completa;
2. bind di tipi critici e geometrie;
3. cancellazione/timeout;
4. perdita di connessione durante commit;
5. strategia bulk;
6. packaging delle eventuali librerie native;
7. licenza e supporto della versione server target.

Il risultato sarà registrato in un ADR provider-specifico. Fino a quel
momento i nomi/versioni di crate sono candidati, non impegni.

## Conseguenze

- lo scaffold può iniziare senza bloccare il dominio su un driver;
- Oracle e Db2 possono usare adapter blocking senza rendere blocking il core;
- le decisioni che richiedono database restano chiaramente separate dal lavoro
  offline.

## Alternative scartate

- scegliere tutti i driver solo dalla documentazione;
- esporre direttamente i tipi di un client nell’API pubblica;
- imporre che ogni provider sia async nativo.
