# Changelog

Le versioni seguono il versionamento semantico: una modifica incompatibile del
contratto pubblico (Rust, CLI, SDK Python, runtime) richiede una nuova major
(AGENTS.md, regola 2). Gli artefatti si distribuiscono solo come allegati delle
GitHub Release (tag `py-vX.Y.Z`). Le sezioni dalla 4.0.0 alla 6.0.0
riassumono le note di quelle release.

## 8.0.1 — non pubblicata

Patch: quattro correzioni, nessun cambiamento di contratto.

### Correzioni

- **Il `plenora.field_id` sopravvive al round-trip, su ogni superficie**
  (#126; rientro dei difetti #124 e #125 trovati dalla matrice interop,
  secondo la decision 0022 di plenora-contracts). La write registra l'id
  ricevuto con `COMMENT ON COLUMN` (formato riconoscibile, commenti
  preesistenti rispettati; serve il privilegio di COMMENT alla prima
  registrazione) e la read riemette quello, con l'hash di provenienza come
  assegnazione solo per le colonne nate nello store; un append con id
  divergente è un conflitto tipizzato. Per la #125, prove di uguaglianza
  dei metadati fra API Rust e CLI sulla stessa query.
- **Chiavi JSON duplicate rifiutate su ogni ingresso** (#123): chiusa la
  classe «chiavi duplicate collassate prima della validazione» su sette
  ingressi di documenti di controllo, con rifiuto ricorsivo al parse ed
  errori senza payload; comportamento dichiarato per i valori utente di
  `ParameterValue::Json`.
- **SBOM della wheel: la dipendenza runtime entra risolta nel grafo**
  (#122): `Requires-Dist` si mappa sul pin unico della qualifica, con
  l'arco verso il componente `pkg:pypi` risolto; le forme non gestite
  (extras, marker) si rifiutano invece di passare non verificate.

### Dipendenze

- **Advisory webpki** (#128): `rustls-webpki` 0.103.16
  (RUSTSEC-2026-0382) e `webpki-roots` 1.0.10 (RUSTSEC-2026-0381), lock
  del fuzz compreso. Lo shim 0.26.11 richiesto dal fork oracle-rs resta
  con eccezione mirata e motivata in `deny.toml`, rientro tracciato
  in #127.

## 8.0.0 — 2026-10-18

Major: l'adozione del catalogo e del profilo database-tools v2 cambia le
operazioni pubbliche, che passano alla versione 2 (#119).

### Incompatibilità e migrazione

- **Contratti v2** (#119). Il pin di `plenora-contracts` passa a `3aec9f7`,
  con Public Catalogs 2.0, Runtime Binding 2.0 e il catalogo e profilo
  database-tools v2. I consumer devono allineare le operazioni richieste e
  i contratti alla versione 2. La versione wire e i selettori del runtime
  restano 1: sono distinti dalla versione del contratto dichiarata in discovery.
- **Risultato query del runtime limitato a 8 MiB** (#119), misurati sul
  JSON UTF-8 compatto; l'host può restringere il limite. Il superamento è
  `resource_limit`, mai un successo troncato. Il manifest dichiara la
  deviazione DB-RT-003: il limite non è esprimibile nella discovery chiusa
  `attributes-v1`. Dichiara anche SURF-006 (query runtime con solo riepilogo
  JSON, senza percorso Arrow) e conserva le deviazioni SURF-007.
- **Esito incerto dopo l'invio del COMMIT** (#114). I consumer devono
  leggere l'envelope d'errore e gli assi di recovery invece di trattare
  l'esito come un risultato riuscito. La CLI usa i codici di CLI-2.0 per
  la causa osservata: 5 per `io`, `timeout` o `protocol`, 130 per
  `cancelled`, 70 per `internal`; non esiste un unico codice per ogni
  commit ignoto. Dettagli e migrazione sono nelle correzioni sotto.

### Novità

- **Discovery degli effetti locali** (#119). `database.read` e
  `database.query` della CLI dichiarano `plenora.surface_side_effects`
  con `{"cli":"local"}`; il gate black-box lo verifica sul binario.
  Una pulizia fallita dopo la pubblicazione riporta soltanto i token
  `cleanup`/`committed`/`never`, senza payload nel messaggio, e conserva
  l'output pubblicato.
- **Avvisi di licenza per ogni crate linkato** (#120), negli archivi CLI,
  nei wheel e nel sorgente di release. Raccolta offline e gate sugli
  artefatti entrano nel workflow di pubblicazione; i testi supplementari
  richiedono origine HTTPS e un blob Git verificato dai byte conservati.
  Guida: `docs/RELEASE.md`.
- **Interoperabilità dei formati** (#116) e **vocabolario delle geometrie**
  (#118): IPC file e stream in ingresso, PyCapsule nello SDK, query Arrow
  in uscita dalla CLI e metadati geometrici conformi, descritti sotto.
- **Dipendenze e protocollo Oracle** (#114, #115). `rustls` passa a
  0.23.46. Il fork `oracle-rs` corregge ricomposizione TNS, terminali TTC,
  bind e LOB, con scambi protetti dall'invio al completamento e fetch
  limitati; gli errori di fetch non diventano fine dati. La dipendenza
  diretta dal fork vale anche per i consumatori esterni.

### Correzioni Oracle

- **Oracle: prosecuzione del cursore e reset degli errori.** La prima pagina
  non dichiara piu la fine prima del completamento Oracle; la regressione live
  richiede 102 righe e poi ORA-01476. I parser di query, fetch, REF CURSOR e
  risultati impliciti rifiutano risposte incomplete o sconosciute; il fetch
  condivide la decodifica del completamento e include sequenza e token TTC.
  Il reset con header `large_sdu` corretto mantiene utilizzabile la sessione:
  il test di redazione verifica ORA-00942, riuso e rollback riuscito, senza
  richiedere la precedente chiusura del canale. Le verifiche live richiedono
  il fixture Oracle; i test offline del fork girano nel workflow Rust esistente.

### Interoperabilità

- **`query --output OUTPUT.arrows`**: il risultato di `query` come Arrow IPC
  stream, il content type che il catalogo dichiara per `database.query`.
  Pubblicazione atomica come per `read` (file temporaneo e hard link, mai su
  un output esistente); il risultato riporta `format: arrow_ipc_stream`,
  righe e batch. Senza `--output` resta il riepilogo JSON. Il binding runtime
  continua a restituire il riepilogo (vedi `docs/runtime.md`).
  Lo schema si valida contro il vocabolario Arrow 1.0 prima di scrivere il
  primo byte, per `query --output`, `read --output` e il sink runtime: uno
  schema non conforme non diventa un artefatto. La verifica guarda le sole
  chiavi canoniche: estensione `geoarrow.wkb` obbligatoria, chiavi legacy
  rifiutate, tipi geometrici case-sensitive, e un CRS `resolved` o
  `declared_unresolved` con identificatore e ordine degli assi (§4).
- **CRS conformi in lettura** in tutti gli adapter: uno SRID senza
  un'autorità risolta si pubblica `declared_unresolved` con
  l'identificatore nello spazio dei nomi del catalogo del database
  (`POSTGIS:n`, `MYSQL:n`, `SQLSERVER:n`, `ORACLE:n`, `DB2:n`) e
  `axis_order=unknown`; senza SRID il CRS è `missing`. L'identificatore è
  locale all'installazione: non viene mai promosso a `resolved` e non è
  confrontabile fra sorgenti diverse (semantica da ratificare in
  plenora-contracts). Lo SRID segue il dominio di ARROW-VOCABULARY §3,
  intero decimale signed 32-bit, in ingresso e in pubblicazione:
  `2147483648` è rifiutato, e `contract_schema` non lascia pubblicare uno
  SRID di catalogo fuori dominio (MySQL `SRS_ID` è senza segno). Deviazione
  dichiarata: un SRID negativo è nel contratto ma nessun provider lo
  rappresenta, ed è rifiutato come `unsupported`. Una definizione CRS vuota
  o di soli spazi non è un'identità ed è rifiutata. Oracle dichiarava
  `resolved` senza identificatore; MySQL, SQL Server e Db2
  `declared_unresolved` senza identificatore né ordine degli assi. Limiti dichiarati: una
  colonna `numeric` nel risultato di una query PostgreSQL e rifiutata come
  `unsupported` (non c'e ancora una mappatura esatta per il risultato di una
  query); senza un `order_by` totale l'ordine delle righe non e ripetibile.
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
  decodifica un batch alla volta. La seconda passata legge ogni messaggio
  intero (prefisso, metadati e corpo, nella lunghezza registrata), ne
  verifica l'impronta SHA-256 registrata nella prima passata e solo dopo lo
  consegna al decoder: un file cambiato fra le due passate è un errore, e
  nemmeno un prefisso alterato (una fine anticipata, una lunghezza diversa)
  arriva al decoder. Dopo un messaggio non verificato ogni lettura
  successiva fallisce, invece di riprendere dal segmento dopo.
  Il budget conta tutto ciò che la lettura tiene per l'input: ogni
  messaggio con il prefisso di otto byte, il messaggio più grande due volte
  (copia verificata e copia del decoder), l'indice dei segmenti per la
  capacità allocata (molti batch vuoti non lo fanno crescere oltre il
  limite) e, per il file, il footer con i blocchi che dichiara, mentre
  l'indice resta occupato per la sua capacità allocata. Il
  verificatore libera il messaggio precedente prima di allocare il
  successivo. Un messaggio che stava nel limite solo senza il prefisso, o
  la cui doppia copia non ci sta, ora è `resource_limit`. Il formato file si
  legge come lo stream che incapsula, e i blocchi del footer devono
  coincidere con i messaggi dello stream, e lo schema del footer con quello
  dello stream, metadati compresi: un lettore di file Arrow userebbe il
  primo, e lo stesso file non deve dare colonne diverse a seconda del
  lettore. Il confronto comprende le proprietà IPC che lo schema Arrow
  materializzato non porta (`Field::eq` ignora `dict_id` e
  `dict_is_ordered`): ordine dei byte, feature e, ricorsivamente per ogni
  campo, tipo IPC e codifica a dizionario (id, ordinamento, tipo degli
  indici). Un footer con gli id dei dizionari scambiati è un errore. Lo
  schema materializzato conta nel budget: quello del lettore dello stream
  per tutta la lettura, quello del footer finché è vivo, misurati dal
  flatbuffer prima di materializzarli (nomi, metadati, campi annidati, con
  una stima per eccesso del costo di ogni campo, di ogni mappa di metadati
  e di ogni voce: Arrow 60 costruisce una tabella hash temporanea e la
  converte in un `BTreeMap`, quindi anche una mappa con una sola voce ha
  un costo fisso). Contano anche i dizionari che il lettore dello stream
  tiene per id per tutta la lettura: il totale residente si aggiorna a ogni
  sostituzione (il nuovo prende il posto del precedente) e a ogni delta (si
  concatena), con il picco in cui precedente e nuovo sono vivi insieme.
  Limite dichiarato: un dizionario annidato nei valori di un altro
  dizionario si rifiuta prima del decoder, nella CLI (file e stream) e nel
  binding Python. Regola: nessun campo a dizionario, a qualunque
  profondità, nel tipo dei valori di un campo a dizionario; ambito: i
  dizionari affiancati o dentro struct e liste restano ammessi; hazard: il
  lettore di Arrow trattiene le generazioni sostituite del dizionario
  interno attraverso quello esterno, mentre il budget le sottrae; rientro:
  un conteggio delle dipendenze fra dizionari.
  Uno schema che dichiara un ordine dei byte diverso da quello della
  piattaforma si rifiuta: il lettore dello stream di Arrow non lo controlla,
  e i valori si leggerebbero con i byte invertiti (un Int32 con valore 1
  diventerebbe 16777216). Prima si accumulavano tutti i
  batch in memoria prima del budget, anche per il formato file.
- **SDK Python: Arrow PyCapsule Interface.** `copy_from`/`acopy_from`
  accettano qualunque oggetto con `__arrow_c_stream__`; il `BatchReader` di
  `Session.read` lo espone, con `read_all()`. Una tabella letta dal database
  entra in `plenora_data.run` e la sua uscita torna in `copy_from` senza
  conversioni (`python/interop/test_plenora_data.py`).
- **SDK: i batch di `copy_from` devono avere lo stesso schema del primo,
  metadati compresi** (`equals(check_metadata=True)`): un batch con un CRS
  diverso nel campo o nello schema si scriveva con i metadati del primo.
- **SDK: lo stream IPC di `copy_from` si verifica prima del decoder**: ogni
  messaggio con il marcatore di continuazione e con metadati e corpo dentro
  i byte ricevuti, la fine esplicita e nessun byte dopo. Prima uno stream
  troncato al confine fra due messaggi si scriveva più corto senza errore,
  e le lunghezze dichiarate governavano le allocazioni del decoder. Uno
  schema con un ordine dei byte diverso da quello della piattaforma si
  rifiuta anche qui.
- **`pyarrow>=25,<26`** è ora una dipendenza dichiarata dello SDK, nella
  stessa finestra di `plenora-data`.
- **Errori della sorgente tipizzati** (PYTHON-SDK §6): un errore Plenora
  della sorgente resta tale; ogni altro errore di conversione o di un
  produttore esterno diventa `PlenoraDataMappingError` (anche `ValueError`)
  con gli assi pubblici, senza il messaggio né la catena dell'originale, che
  possono contenere dati di riga. Vale anche per `read_all()`.
- La traduzione copre ogni accesso alla sorgente (`hasattr`, `iter`, il
  primo `next`, la serializzazione, e anche il riconoscimento dei `bytes`:
  una sottoclasse può ridefinire `__bytes__`); nessun messaggio dello SDK
  riporta il nome della classe del chiamante, che può essere costruito a
  runtime. Un `PlenoraError` sollevato dalla sorgente è esterno come ogni
  altro errore del chiamante: diventa `data_mapping` senza il suo
  messaggio. Limite dichiarato: l'errore di un `BatchReader` del database
  usato come sorgente attraversa lo stream C di pyarrow, che non ne conserva
  la classe, e arriva anch'esso come `data_mapping` (era già così).
- **Ogni eccezione dello SDK porta gli assi di PYTHON-SDK §6**
  (`category`, `phase`, `remote_effect`, `retry`, `message`), anche quelle
  definite in Python. Gli argomenti rifiutati da `copy_from` restano
  `TypeError`/`ValueError` e sono anche `PlenoraInvalidPlanError`;
  `NoResultFound`, `MultipleResultsFound`, `JsonInputError` e gli errori ORM
  ricevono gli assi della loro categoria. Gli errori ORM dichiarano la
  categoria anche nella gerarchia: `OrmMappingError` e `OrmStateError` sono
  `PlenoraInvalidPlanError`, `StaleObjectError` è
  `PlenoraConcurrentModificationError` con `remote_effect=unknown` (la
  transazione resta aperta e del chiamante), `OrmUnsupportedError` è
  `PlenoraUnsupportedError`. Limite dichiarato: i `TypeError`/`ValueError`
  built-in con cui il resto dello SDK valida gli argomenti delle proprie API
  non portano ancora gli assi.
- Il `BatchReader` non traduce un errore in Python tenendo il proprio
  mutex e non lo attende tenendo il GIL (un errore di lettura e
  `schema_bytes` da due thread potevano bloccarsi a vicenda). La prova forza
  la contesa in un processo separato con timeout esterno, e chiede lo
  schema solo quando `pg_stat_activity` mostra la query ferma in `pg_sleep`
  e il consumatore non avanza più: il mutex è del consumatore.
- Dichiarato: `python/interop/test_plenora_data.py` (database ↔
  `plenora_data`) si esegue a mano; la CI non installa `plenora-data` e non
  lo esegue.
- **`acopy_from` consuma la sorgente in un executor**, non sul thread del
  loop asyncio (PYTHON-SDK §4): un `BatchReader` legge dal database. Il
  `BatchReader` sincrono non è più legato al thread che l'ha aperto.

### Correzioni (vocabolario Arrow 1.0)

- **Più tipi geometrici dichiarati** (`plenora.geometry.types=point,polygon`,
  valido per ARROW-VOCABULARY §3) erano rifiutati da `write` PostgreSQL come
  `invalid_plan`. Ora la colonna creata è `geometry(Geometry,SRID)` e ogni
  valore deve essere di uno dei tipi dichiarati.
- **WKB ISO con SRID nei metadati** (`encoding=wkb`): `write` pretendeva lo
  SRID dentro il valore e rifiutava ogni ingresso IO con CRS. Ora lo SRID
  viene dai metadati del campo e il valore è riscritto in EWKB prima della
  scrittura; un SRID dentro un valore dichiarato `wkb` è un `data_mapping`.
- **Categoria conservata nel rollback**: un errore di mappatura scoperto
  mentre si riservano le risorse del batch usciva come `resource_limit`
  (exit 4). Ora resta della sua categoria.
- **`plenora.field_id` su ogni campo letto**, per tutti gli adapter
  (`protocol::contract_schema`). Un id dichiarato resta, qualunque intero
  decimale non negativo sia. PostgreSQL dichiara l'id dall'origine della
  colonna (`attrelid`, `attnum`), quindi una rinomina o un alias non lo
  cambiano. Limite dichiarato: per le espressioni, per gli adapter che non
  espongono l'origine (MySQL, SQL Server, Oracle, Db2) e per ogni colonna la
  cui origine compare più volte nella proiezione l'id si deriva dal nome, e
  una rinomina lo cambia. Aggiungere una seconda proiezione della stessa
  origine (anche con un auto-join) cambia quindi l'id anche della colonna
  rimasta invariata; per le viste l'equivalenza fra lettura e query non è
  garantita; l'unicità vale dentro lo schema, non fra schemi o nel tempo.
  Id duplicati — anche per collisione dell'hash — sono un errore `schema`.
  **`precision`** (`float64`), dimensioni e dichiarazione dei tipi sono
  sempre presenti sulle geometrie PostgreSQL (ARROW-VOCABULARY §2 e §4), e
  una geometria letta con `dimensions=unknown` (colonna senza typmod) si
  riscrive in una colonna `geometry` generica.
- **SRID annidati**: uno SRID dentro una geometria annidata (per esempio il
  figlio di una `GeometryCollection`) e un `data_mapping`, per `wkb` e
  `ewkb`: la verifica guarda tutto il valore, non solo la radice. La copia
  EWKB dei valori `wkb` e prenotata nel budget prima di essere allocata.

### Correzioni

- **Una scrittura dall'esito incerto non è più un successo.** `write` della
  CLI (e `database-write-ipc`, `bulk-write`, `postgres-write-ipc`),
  `database.write` del runtime e `copy_from`/`acopy_from` dello SDK Python
  consegnavano un esito `outcome_unknown` come risultato riuscito: la CLI con
  `status: ok` ed exit 0, lo SDK come valore di ritorno. Provato con un proxy
  che fa cadere la conferma del `COMMIT` di PostgreSQL. Ora è l'errore di
  commit ignoto (`commit`, `unknown`, `requires_recovery`;
  `PlenoraCommitOutcomeUnknownError`) con l'esito in `details.write_outcome`.
  La categoria è la causa che l'adapter ha osservato (ERR-001): `io` per un
  canale perso (exit 5, il caso del proxy), `timeout` (5), `cancelled`
  (130), `protocol` per una risposta non interpretabile (5), `internal`
  (70) quando la causa non è dimostrabile. La causa viaggia in
  `Recovery::cause`, solo Rust: lo schema `write-outcome` è chiuso e il
  documento JSON non la porta. In Python la classe resta
  `PlenoraCommitOutcomeUnknownError` (sottoclasse di `PlenoraInternalError`,
  per compatibilità) e l'attributo `category` porta la causa: è l'attributo,
  non la classe, a dire la categoria. `partially_committed` e `rolled_back` seguono la
  stessa regola con i propri assi (`execution`, exit 6). SURF-014.
- **`execute` con commit ignoto** stampava `status: ok` ed usciva con 1:
  ora è un envelope d'errore con il `CommitOutcome` in `details.commit`
  (CLI-2.0 §4: exit diverso da zero solo con `status: error`).
  Le due chiavi restano distinte perché portano documenti diversi:
  `details.write_outcome` è il `write-outcome` di una scrittura (conteggi e
  `recovery`), `details.commit` il `CommitOutcome` di una transazione.
- **Assi dopo un effetto remoto possibile**, su tutti gli adapter:
  - dopo l'invio del COMMIT, un errore che non è un rifiuto certo del server
    (Oracle senza codice ORA, PostgreSQL senza SQLSTATE) è un esito ignoto,
    non `protocol`/`none`; MySQL tratta allo stesso modo ogni errore che
    `driver_error` dichiara già di effetto ignoto;
  - un rollback non confermato porta `phase: rollback`, `remote_effect:
    unknown`, `requires_recovery` (ERR-003, ERR-014) in PostgreSQL, MySQL,
    SQL Server, Oracle e Db2;
  - Db2: una cancellazione all'ingresso di `commit` o `rollback` esegue il
    rollback in modo esplicito e ne riporta l'esito, invece di dichiarare
    `none` e lasciarlo al distruttore;
  - **regola unica dopo l'invio del COMMIT**, in un punto solo
    (`transaction::after_commit_sent` e `commit_failure_outcome`) usato da
    tutti gli adapter: ogni errore lascia l'esito ignoto, con la causa come
    categoria, salvo un elenco chiuso e per-adapter di codici che provano il
    rollback — PostgreSQL `40001`, `40002`, `40P01` e le violazioni di
    vincoli differiti `23502`, `23503`, `23505`, `23514`, `23P01`; MySQL
    `1213`, `3101`; MariaDB solo `1213` (`3101` è un codice MySQL che il
    catalogo MariaDB non definisce: senza prova resta ignoto); Oracle
    ORA-02091; SQL Server `1205`; Db2 nessuno. L'elenco MySQL/MariaDB è del
    profilo di prodotto. Uno SQLSTATE riconosciuto ma fuori elenco (per
    esempio `08006`, `08P01`) non porta più `none`. Il percorso PostgreSQL
    con diagnostica di riga usa lo stesso punto (`proven_commit_rollback`)
    della scrittura ordinaria;
  - il COMMIT del setup della creazione Oracle (metadati spaziali e indici
    dopo il `CREATE TABLE`) passa dalla stessa regola, con timeout e
    cancellazione; dopo la creazione un errore con effetto ignoto resta
    `unknown` invece di diventare `partial`, e un errore prima della
    creazione conserva i propri assi;
  - Oracle, ogni uscita dopo la creazione del target (apertura della
    transazione di carico, scrittura, COMMIT annullato, esito ignoto) passa
    da un punto solo, come `stamp_ddl_residue` di MySQL: il DDL Oracle fa
    commit implicito, quindi un effetto noto è `partial` e uno ignoto resta
    `unknown`. Prima l'apertura fallita della transazione e un COMMIT
    `rolled_back` dichiaravano `none`/`rolled_back` con la tabella creata;
  - Oracle, dopo l'invio di un comando mutante solo un **elenco chiuso** di
    codici ORA prova il rifiuto senza effetto
    (`CERTAIN_STATEMENT_REJECTIONS`: errori di analisi come ORA-00900–00957,
    00942, 01031, e rifiuti con rollback dell'istruzione come ORA-00001,
    01400, 02290–02292, 12899, ciascuno motivato nel codice). Ogni altro
    esito — canale chiuso, trasporto interrotto, codice non qualificato — dà
    effetto ignoto con recovery; i codici di connessione persa (ORA-03113,
    03114, 03135, 12537, 12547, 12570, 25408) hanno categoria `io`. Prima
    usciva `none` con `retry=safe`, anche per il `CREATE TABLE` del setup;
  - **SQL del chiamante dopo l'invio non dichiara mai `none`**, in un
    punto obbligato (`plenora_database_core::caller_sql`) per tutti i
    provider, PostgreSQL compreso. `Provider::begin_transaction` ed
    `execute_ddl` non si ridefiniscono più: avvolgono `open_transaction` e
    `run_ddl` dell'adapter, e qualunque errore di `execute`, `query`,
    `query_stream` con il suo stream, `execute_graph` e dell'update
    condizionale — invio, fetch, decodifica, cancellazione, timeout,
    worker fallito — passa da `DatabaseError::after_unproven_send`: un
    effetto `none` diventa ignoto con recovery, un rollback confermato
    resta. Il contesto non prova l'assenza di effetti: un DDL Oracle o
    MySQL fa commit implicito di ciò che lo precede, una procedura
    PostgreSQL chiamata in autocommit fa COMMIT e poi RAISE, un trigger
    autonomo fa commit da solo, un batch T-SQL `INSERT; RAISERROR` ha già
    scritto. Una prova d'inventario fallisce se un adapter ridefinisce i
    due metodi pubblici. Limite dichiarato — regola: un errore di fase
    `validate` precede l'invio e resta com'è; ambito: un errore locale
    dell'adapter in un'altra fase prima dell'invio (parametro non
    convertibile, transazione già chiusa) esce ignoto; hazard: nessuno per
    la correttezza, ma `requires_recovery` dove `none` sarebbe stato vero;
    rientro: un confine esplicito fra preparazione e invio nel trait degli
    adapter. Il difetto in SQL Server c'era già su `main`. Prove live in
    PostgreSQL (procedura che fa COMMIT e poi RAISE), SQL Server (batch
    `INSERT; RAISERROR`) e Oracle (INSERT poi CREATE TABLE con ORA-00955);
  - **scritture `create` abbandonate** (Oracle e MySQL): una guardia armata
    prima del `CREATE TABLE` registra il target nel registro del provider
    (`ddl_residues()`, modulo `ddl_residue`) se la scrittura non arriva a un
    esito — future lasciata cadere dal chiamante, panic del produttore dei
    batch. In Oracle anche il setup passa dal punto unico
    (`stamp_created_target`). MySQL esegue il DDL in fase `write`, così una
    risposta persa al `CREATE` lascia l'esito ignoto come per ogni comando
    mutante. Limite dichiarato — regola: un'operazione `create` abbandonata
    è osservabile dal registro del provider; ambito: il registro vive nel
    processo, e il crash o l'uscita del processo (anche `panic = "abort"`)
    lo perdono; hazard: dopo un crash una tabella creata può restare senza
    che nessun esito lo dica; rientro: un registro durevole delle intenzioni
    (journal su disco o tabella di servizio), che la libreria oggi non ha;
  - **regola unica dell'interruzione dopo l'invio**
    (`DatabaseError::after_interrupted_send`): un timeout o una cancellazione con un
    comando mutante in volo danno effetto ignoto e recovery, e in fase
    `rollback` un rollback non confermato, con la categoria della causa.
    Prima PostgreSQL dichiarava `none` per un `ROLLBACK TO SAVEPOINT`
    interrotto, Oracle per ogni comando interrotto in volo (scritture,
    savepoint, rollback), e la creazione o l'eliminazione di un grafo AGE
    interrotta dichiarava `none`;
  - ORA-03113, ORA-03114 e ORA-25408 dopo il COMMIT hanno categoria `io`;
  - ogni rollback inviato e non confermato — esplicito, `ROLLBACK TO
    SAVEPOINT`, pulizia dopo un `set_config` fallito in PostgreSQL — porta
    fase `rollback` ed effetto ignoto, mai `none`, in tutti gli adapter;
  - PostgreSQL, percorso con diagnostica di riga: la conferma di commit
    persa porta la categoria della causa (`io`), non sempre `protocol`;
  - dichiarato: il distruttore di una transazione Db2 abbandonata tenta
    ancora il rollback senza riportarlo, perché non c'è un errore da
    costruire; la connessione chiusa fa annullare il lavoro non confermato.
  - `WriteOutcome::settle` conserva la fase reale di un esito fuori
    contratto (la fase certa della `recovery`), invece di `finalize`.
- Dichiarato: PostgreSQL `append` con diagnostica di riga riporta una
  conferma di commit persa come `protocol`/`quarantine` (exit 5), mentre
  `create` usa gli assi del commit ignoto (`internal`/`requires_recovery`,
  exit 70). Entrambi hanno effetto `unknown` e nessun retry automatico
  (ERR-006 ammette `quarantine`).
- **Niente più exit code 1.** I comandi operativi che stampano un verdetto
  negativo (`doctor`, `diagnose`, `profile-check`, `test-*`,
  `conditional-update`, `pool-status`, …) uscivano con 1, che CLI-2.0 §8 non
  prevede. Ora escono con la proiezione della categoria: 70 per il commit
  ignoto, la categoria dell'errore quando c'è, 6 (`execution`) per una
  verifica non superata.

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

Chi leggeva `status` dal risultato di una scrittura, o l'exit 1, deve leggere
ora l'envelope d'errore e i codici di CLI-2.0.

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
