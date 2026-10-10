# Changelog

Le versioni seguono il versionamento semantico: una modifica incompatibile del
contratto pubblico (Rust, CLI, SDK Python, runtime) richiede una nuova major
(AGENTS.md, regola 2). Gli artefatti si distribuiscono solo come allegati delle
GitHub Release (tag `py-vX.Y.Z`). Le sezioni dalla 4.0.0 alla 6.0.0
riassumono le note di quelle release.

## Non rilasciato

### Interoperabilità

- **`write` della CLI accetta l'Arrow IPC stream**, oltre al file, come
  dichiara il catalogo. Il formato si riconosce dai primi byte (`ARROW1` per
  il file, il marcatore di continuazione per lo stream); un inizio diverso,
  compreso lo stream legacy senza marcatore, è un errore esplicito, e uno
  stream senza marcatore di fine è rifiutato come troncato. Vale anche per
  `inspect-dataset`, `database-write-ipc`, `bulk-write` e
  `postgres-write-ipc`.
  La fine dello stream è il messaggio di lunghezza zero letto al confine fra
  due messaggi, e dopo di essa non ci devono essere byte: due stream
  concatenati non si leggono più come il solo primo. L'input si legge in due
  passate: la prima verifica l'inquadramento e il limite di ogni messaggio
  (prima di allocarne il corpo) e conta le righe dichiarate, la seconda
  decodifica un batch alla volta. Prima si accumulavano tutti i batch in
  memoria prima del budget, anche per il formato file.
- **SDK Python: Arrow PyCapsule Interface.** `copy_from`/`acopy_from`
  accettano qualunque oggetto con `__arrow_c_stream__`; il `BatchReader` di
  `Session.read` lo espone, con `read_all()`. Una tabella letta dal database
  entra in `plenora_data.run` e la sua uscita torna in `copy_from` senza
  conversioni (`python/interop/test_plenora_data.py`).
- **`pyarrow>=25,<26`** è ora una dipendenza dichiarata dello SDK, nella
  stessa finestra di `plenora-data`.
- **Errori della sorgente tipizzati** (PYTHON-SDK §6): un errore Plenora
  della sorgente resta tale; ogni altro errore di conversione o di un
  produttore esterno diventa `PlenoraDataMappingError` (anche `ValueError`)
  con gli assi pubblici, senza il messaggio né la catena dell'originale, che
  possono contenere dati di riga. Vale anche per `read_all()`.
- **`acopy_from` consuma la sorgente in un executor**, non sul thread del
  loop asyncio (PYTHON-SDK §4): un `BatchReader` legge dal database. Il
  `BatchReader` sincrono non è più legato al thread che l'ha aperto.

### Correzioni

- **Il fork di `oracle-rs` vale anche per i consumatori.** Entrava con
  `[patch.crates-io]`, che Cargo applica solo dal workspace radice: chi usava
  database-tools come dipendenza (path o git) riceveva `oracle-rs` 0.1.7 da
  crates.io, senza la rinegoziazione TCPS, e senza errore. Ora è una
  dipendenza `path` con nome proprio (`plenora-oracle-rs`, libreria
  `oracle_rs`), e `plenora-db-oracle` legge in compilazione un marcatore che
  esiste solo nel fork. `scripts/check_consumer_fork.py` costruisce un
  consumatore esterno e lo verifica, in CI su Linux e Windows.
- **`thiserror` =2.0.21**, come data-tools2: con `=2.0.20` un grafo che usa
  entrambe le librerie non si risolveva.

## 7.0.0 — 2026-10-06

Major: alcuni documenti di controllo prima accettati ora vengono rifiutati, e
l'enum pubblico Rust `PublicSurface` ha una variante in più.

### Incompatibilità e migrazione

- **Chiavi sconosciute rifiutate nei documenti di controllo** (#103). L'AST
  portable (`Predicate`, `Expression`, `Projection`, `PortableStatement`),
  `ParameterValue` (letterali portable e `PARAMETERS.json`), `WindowFrameBound`
  e gli altri tipi letti da JSON di controllo rifiutano le chiavi che non
  conoscono. Prima venivano ignorate: `{"op": "is_null", "column": "a",
  "negate": true}` in un DELETE diventava `WHERE "a" IS NULL`.
  Migrazione: togliere dai documenti le chiavi che il contratto non dichiara.
- **`null` rifiutato dove nessun produttore lo manda** (#105).
  `limits.max_rows`, la `source` di `ReadCheckpoint` e `parameters_path` delle
  richieste canoniche della CLI scritti a `null` ora vengono rifiutati.
  Migrazione: omettere il campo invece di scriverlo `null`. `catalog: null`
  resta ammesso, perché lo schema lo prevede.
- **Chiavi ripetute rifiutate** in `REQUEST.json` e `PARAMETERS.json` della
  CLI (#111). Prima valeva l'ultima occorrenza.
  Migrazione: una chiave per oggetto.
- **Schema `target` applicato dalla CLI** (#111). `secret_environment` deve
  essere un identificatore di al massimo 128 caratteri e `provider_arguments`
  ha al massimo 32 elementi di 4096 caratteri, come dichiara
  `public-operation-contracts.schema.json`.
- **Richiesta canonica non conforme: `invalid_configuration`** (#111).
  Un `REQUEST.json` della CLI che non soddisfa il proprio input contract
  (JSON non leggibile, campi fuori schema, schema `target`) era
  `invalid_plan`; ora è `invalid_configuration`, come sul runtime. Il codice
  d'uscita resta 2. I documenti di operazione (READ.json, WRITE.json,
  PARAMETERS.json) restano `invalid_plan`.
- **`PublicSurface::Runtime`** (#111). Un `match` esaustivo su
  `PublicSurface` nel codice Rust consumer va esteso.
- **`arrow_field` di `OracleColumnSpec` e `Db2ColumnSpec` restituisce
  `Result`** (#110). Una colonna geometrica senza SRID ora è un errore `Crs`;
  prima andava in panico. Va adeguato chi chiama il metodo dall'API Rust.
- **NUMERIC PostgreSQL** (#110). Una cifra fuori da [0, 9999] è un errore.
  Prima produceva un numero plausibile e sbagliato.
- **MSRV 1.98.1** (#109). `rust-version` passa da `1.98` a `1.98.1`, la
  toolchain con cui il workspace è compilato e provato.
- **Messaggi d'errore** (#104). I comandi della CLI non ripetono più percorsi
  posizionali né il testo di `io::Error`: riportano lo slot e
  l'`io::ErrorKind`. Va adeguato chi confrontava il testo del messaggio.

### Novità

- **Superficie runtime** (#111). `plenora_database_engine::runtime_binding`
  implementa Runtime Binding 1.0 per `plenora.database-tools` sulle otto
  operazioni del catalogo: discovery, test di connessione, ispezioni, read,
  write e query. Connessioni e artefatti li risolve l'applicazione. I vettori
  `database-*` di `plenora-contracts` vengono eseguiti in CI. L'ammissione
  segue la matrice runtime comune alle librerie Plenora e i chiarimenti
  RT-016..RT-023 proposti in `plenora-contracts` #21, non ancora normativi;
  le 21 sonde `runtime-probes-v1` sono eseguite come test. Guida:
  `docs/runtime.md`.
- **Contratti** (#108). Il pin di `plenora-contracts` passa a `1e902dfa`. I
  vettori `arrow-v1` vengono eseguiti, e il manifest di adozione riporta le
  deviazioni dichiarate (null come assente nei piani v2, SURF-007, #106).
- **Robustezza** (#110). Nessuna primitiva di panico nel codice di libreria:
  63 siti convertiti in errori tipizzati, senza dati nei messaggi.
- **Gate**:
  - validatori semantici del pin su capability e manifest (#101);
  - il gate di coverage verifica i sorgenti misurati (#102);
  - campagne fuzz in CI con corpus seme versionato (#107);
  - motivazione obbligatoria accanto a ogni pin, con una guardia (#109);
  - CI anche su Windows e gate anti-panic sulle librerie (#110).

## 6.0.0 — 2026-09-18

- SQL Server adotta `tiberius-ng` 0.13.1 da crates.io, senza patch locali.
  È una major perché le API Rust di basso livello espongono i tipi del nuovo
  pacchetto: i consumer devono allineare la dipendenza
  (`docs/sqlserver-rust.md`).
- Il conteggio delle righe confermato dal server viene letto anche con
  `NOCOUNT ON`.
- Limiti: Azure SQL e PolyBase non sono stati eseguiti; il trasporto seleziona
  TLS 1.2.

## 5.0.1 — 2026-09-17

- SBOM di release dai grafi Cargo completi del workspace e del fuzz, dai
  requisiti Python pinnati e dai metadati dei wheel.
- Backport della migrazione rustls nel driver SQL Server; Oracle rifiuta le
  chiavi wallet invalide; `sqlparser` 0.63.0.

## 5.0.0 — 2026-09-16

- Arrow 60 nell'intero workspace e nel fuzz. È una major perché l'API Rust
  espone i tipi Arrow: i consumer allineano le dipendenze e usano `Metadata`
  (`docs/arrow-rust.md`).

## 4.3.0 — 2026-09-16

- Rust 1.98.1, Arrow 59.3.0, strumenti Python e action aggiornati e fissati
  per digest o commit; provenance e SBOM con `actions/attest`.

## 4.2.1 — 2026-09-16

- Oggetti aggiunti dai callback ORM durante il flush non più persi;
  aggiornamento di Rustls per RUSTSEC-2026-0285.

## 4.2.0 — 2026-09-14

- Governance SQL: batch T-SQL senza punto e virgola, `END`/`ABORT` riservati
  al transaction scope.
- Callback di flush e migrazioni non possono terminare la transazione; cleanup
  che conserva l'errore originale.

## 4.1.0 — 2026-09-13

- Governance SQL estesa a tutti gli statement, ai literal e ai commenti;
  errori URL e Arrow dello SDK senza valori dell'input; indici spatial Oracle
  sicuri per UTF-8.

## 4.0.0 — 2026-09-04

- Adozione del profilo pubblico `plenora-database-tools-profile-v1`,
  Capability Discovery 2.0, protocollo CLI 2 e allineamento di SDK e core al
  contratto pubblico.
