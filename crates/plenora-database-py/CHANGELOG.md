# Changelog — plenora-database Python SDK

Il changelog descrive l'impatto per chi aggiorna. La storia implementativa e le
review restano in Git. Ogni modifica incompatibile richiede una nuova major.

## [Unreleased]

## [1.1.0] — 2026-09-01

### Aggiunto

- Checkpoint keyset persistenti e provider-qualified per letture Arrow sync e
  async. Il token verifica provider, sorgente, proiezione, ordinamento, filtro,
  parametri e CRS; PostgreSQL, MySQL, MariaDB, SQL Server e Db2 hanno prove live
  separate di ripresa senza duplicati o salti.
- Diagnostica row-scoped Db2 con indici sorgente assoluti e cause redatte,
  scritture bulk mediante parameter array e capability `array_binding`
  qualificata sul riferimento Db2 LUW 12.1.
- Migrazioni ORM sync/async ordinate come DAG, con branch, merge, validazione
  della history e runner Db2 idempotente e reversibile.
- DML relazionale portabile con righe restituite: `RETURNING` sui provider
  qualificati e lowering `OUTPUT` per SQL Server; i dialetti privi della forma
  richiesta restano fail-closed.
- `joinedload` delle collezioni con deduplicazione delle entita root,
  relationship e many-to-many con chiavi composite, mixin astratti ed
  ereditarieta concrete che conserva colonne, vincoli e relazioni.
- Superficie Apache AGE per mapping dichiarativo di vertex/edge, inserimenti
  bulk sync/async a batch, risoluzione degli endpoint per business key e DDL
  di indici o vincoli unique sulle proprieta. La prova live usa AGE 1.7.0 su
  PostgreSQL 18.

### Corretto

- Gli identificatori Cypher e gli accessi alle mappe dei batch AGE sono
  quotati anche quando coincidono con parole chiave come `end`.
- Le capability `truncate_insert` e `staged_swap` restano aperte soltanto dove
  la semantica e realmente atomica; una sequenza `DELETE` + `INSERT` non viene
  piu descritta come swap di un oggetto staging.
- I messaggi pubblici dei nuovi percorsi non includono valori di riga, token,
  DSN o SQL bindato.

## [1.0.0] — 2026-09-01

### Breaking

- La distribuzione ufficiale non produce piu wheel macOS ARM. La matrice
  standard resta Linux x86_64 e Windows x86_64; la modifica appartiene alla
  major 1.0.

### Aggiunto

- Il runtime Db2 Linux x86_64 viene distribuito come wheel distinto con build
  tag `1db2`, solo dopo la qualifica live completa contro Db2 LUW 12.1.
- La campagna SDK schedulata include dieci cicli ripetuti di cancellazione,
  recupero della sessione, rollback e concorrenza async sul wheel installato.

### Manutenzione

- Le JavaScript action GitHub sono aggiornate alle release basate su Node 24.

## [0.14.0] — 2026-09-01

### Aggiunto

- `Geometry` ORM e ora qualificata live anche su MySQL, MariaDB, SQL Server e
  Db2. MySQL/MariaDB coprono le geometrie OGC in XY; SQL Server e Db2 coprono
  Point, LineString e Polygon in XY/XYZ. SQL Server qualifica entrambe le
  semantiche `geometry` e `geography`, mentre Db2 resta `geometry`-only.

### Corretto

- I round trip Geometry ORM conservano separatamente WKB e SRID sui provider
  che non trasportano il frame EWKB, inclusi valori `NULL`, insert e update.
- Il percorso transazionale Db2 riconosce il discriminator `SQL_BLOB` del
  driver IBM CLI e decodifica in modo redatto la rappresentazione esadecimale
  restituita da `ST_ASBINARY`.
- Il fixture Db2 prepara uno spazio pagina adatto alle righe con piu colonne
  `ST_GEOMETRY`, cosi il gate live esercita lo stesso DDL pubblicato dall'ORM.

## [0.13.0] — 2026-08-31

### Aggiunto

- API `cypher()` sync/async su sessione e transazione per Apache AGE, con
  parametri separati dal testo, risultati `Vertex`/`Edge`/`Path` tipizzati e
  capability fail-closed qualificate per AGE 1.7.0 su PostgreSQL 18.
- Amministrazione graph sync/async con `list_graphs()`, `create_graph()` e
  `drop_graph()`, protetta da un capability document additivo che non modifica
  il contratto AGE v1.
- `cypher(..., max_rows=...)` limita i risultati prima della materializzazione;
  il gate live copre l'intera matrice di clausole AGE documentate, percorsi
  variabili, concorrenza, cancellazione e timeout.

## [0.12.0] — 2026-08-31

La versione interna `0.11.0` non e stata pubblicata: questa release consolida
il relativo lavoro in un unico artefatto canonico e verificabile.

### Aggiunto

- Helper pubblici `int32()` e `int64()` per dichiarare esplicitamente la
  larghezza dei bind interi; PostgreSQL adatta inoltre gli interi Python al
  tipo preparato `smallint`/`integer`/`bigint` senza frame binari incoerenti.
- Gli errori di bind PostgreSQL incompatibili espongono posizione, tipo
  portabile e tipo target come attributi strutturati, senza includere il valore
  del parametro nel messaggio pubblico.
- `create_engine` e `create_async_engine` per PostgreSQL: un pool condiviso,
  sessione per unita di lavoro, lifecycle e cancellazione governati dal Core
  v3, con parita sync/async.
- `connect_mariadb` e `aconnect_mariadb`, con prodotto esplicito e nessuna
  selezione automatica rispetto a MySQL.
- `connect_sqlserver` e `aconnect_sqlserver` nella superficie comune del SDK.
- `capabilities`, `inspect`, `execute_ddl`, lettura Arrow e scrittura Arrow
  sulle sessioni dei quattro provider, nelle rispettive forme sync e async.
- Expression language immutabile (`table`, `select`, `bind`, predicati, join,
  ordinamento e paginazione) compilata dall'IR relazionale canonico per tutti i
  provider pubblici, con `Result` uniforme su sessioni e transazioni sync/async.
- Expression language avanzata con funzioni scalar e aggregate, grouping,
  window, subquery, CTE e set operation, qualificata live sui cinque provider.
- `Row` immutabile e terminali tipizzati additivi su `Result`, con accesso per
  posizione, nome e descrittore di colonna.
- `BindType` e `bind(..., type_=...)` aggiungono hint logici chiusi e portabili,
  compresa la projection tipizzata richiesta da Db2 quando manca una colonna
  da cui inferire il tipo.
- Verticale ORM-like sync e async: mapping dichiarativo con `DeclarativeBase`,
  `Mapped` e `mapped_column`, registry, identity map, stati delle istanze e
  unit of work transazionale con autoflush, merge, expire, expunge, cascade e
  rollback del grafo; `AsyncOrmSession` condivide mapper e invarianti.
- Query di entita tramite `OrmSession.query`, con composizione
  join, filtri relazionali, eager loading, proiezioni e tuple multi-entita,
  terminali a cardinalita esplicita e `refresh` delle istanze persistenti.
- Chiavi primarie semplici o composite, vincoli unique/FK compositi, tipi
  Python verificati ed ereditarieta concrete esplicita.
- Chiavi `generated=True` e colonne `server_default=True`: idratazione dallo
  statement su PostgreSQL, MariaDB e SQL Server; recupero dell'identita locale
  e SELECT nella stessa transazione su MySQL e Db2.
- Relazioni many-to-one, one-to-many, one-to-one e many-to-many senza lazy I/O,
  con `back_populates`, cascade esplicite e planner dei cicli FK nullable.
- Versioning ottimistico integrato nel flush tramite una colonna dichiarata
  `version=True`; un conteggio diverso da una riga produce `StaleObjectError`.
- Tipo ORM `Geometry` con validazione EWKB, SRID, dimensioni e semantica;
  validazione del tipo concreto, predicati/funzioni ORM e proiezione/bind
  canonici qualificati per PostgreSQL. Gli altri provider restano fail-closed.
- `OrmMetadata`, `ServerDefault`, runner di migrazioni lineari sync/async e hook
  locali della sessione completano il perimetro schema/lifecycle.

### Breaking

- `execute_scalar` impone la cardinalita dichiarata: al massimo una riga ed
  esattamente una colonna. Zero righe restituiscono `None`; risultati piu
  larghi devono essere letti con `execute_returning_rows`.

### Corretto

- La chiusura di una transazione Python sgancia subito l'oggetto nativo
  thread-affine, evitando che un traceback trasferito tra thread ne rimandi la
  distruzione al thread sbagliato.
- Su Db2 una projection con parametro privo di contesto di tipo viene ora
  rifiutata in prepare, invece di inviare al server SQL che Db2 non puo tipizzare.
- Le tabelle Db2 qualificate con schema e senza alias esplicito ricevono una
  correlazione deterministica, cosi i descrittori `table.c.column` restano
  validi anche nel dialect Db2.
- Il decoder MySQL conserva come byte i `BLOB` ASCII invece di dedurne il tipo
  dall'aspetto del contenuto.
- Una cella assente nel protocollo non viene convertita in `NULL` SQL.
- `SpatialReference.validated()` fallisce quando il modulo nativo non e
  disponibile.
- Un errore durante l'avvio del modulo nativo diventa `ImportError` classificato
  invece di un panic.

## [0.10.0] — 2026-08-17

### Breaking

- PostgreSQL `Replace` richiede un target esistente e usa `DELETE` + bulk
  insert nella stessa transazione. Identity, indici, vincoli, trigger, grant e
  opzioni del target vengono preservati.
- MySQL `Replace` usa `DELETE` + insert bulk transazionale, non staging con
  `RENAME TABLE`.
- I comandi legacy MySQL applicano TLS fail-closed come la superficie comune.
- Piani con campi accettati ma ignorati vengono ora rifiutati in prepare.

### Aggiunto

- MySQL espone sei write mode su sette. `TruncateInsert` resta esclusa per il
  commit implicito di `TRUNCATE`.
- `SessionContext`, `NativeQueryPolicy`, introspezione degli indici e diagnostica
  degli effetti rimasti sul server per MySQL.

### Corretto

- Upsert MySQL fail-closed quando una riga puo collidere con piu indici unici.
- Upsert keys-only reso idempotente senza aggiornamenti fittizi.
- Normalizzazione Arrow applicata anche ai tipi annidati.
- Configurazione TLS raccolta una volta e propagata senza riletture intermedie.

## [0.9.2] — 2026-08-15

- Corretto il default MySQL di `copy_from`: `mapping_policy="strict"` non viene
  piu imposto quando il chiamante non lo richiede.
- Le factory MySQL top-level non sono piu stub.
- La documentazione delle write mode MySQL segue le capability effettive.
- La probe MySQL rifiuta MariaDB senza adattarsi automaticamente.

## [0.9.1] — 2026-08-15

### Breaking

- `Replace` e `TruncateInsert` MySQL sono state temporaneamente chiuse finche la
  loro semantica transazionale non fosse dimostrata; `Replace` e stata riaperta
  con contratto corretto in 0.10.0.
- TLS MySQL e secure-by-default. Per fixture locali senza TLS serve una scelta
  esplicita del chiamante.

### Corretto

- Una transazione MySQL colpita da deadlock viene invalidata e non riutilizzata.

## [0.9.0] — 2026-08-15

### Breaking

- TLS PostgreSQL e secure-by-default; la modalita senza verifica richiede una
  scelta esplicita.

### Aggiunto

- `SessionContext` transaction-local.
- `NativeQueryPolicy` configurabile all'apertura della transazione.
- `PlenoraCommitOutcomeUnknownError` per commit dal risultato ambiguo.
- Validazione EWKB e policy spatial fail-closed.
- Cancellazione client-side in-flight e gerarchia errori tipizzata.

### Corretto

- Parametri JSON non finiti, SRID fuori dominio e identificatori invalidi non
  vengono piu normalizzati o accettati silenziosamente.
- Budget, quoting, SRID geografici e costruzione degli errori commit-unknown
  sono stati consolidati nelle rispettive fonti comuni.

## [0.8.1] — 2026-08-14

- Verificati su MySQL gli helper tipizzati `uuid`, `decimal`, `date`,
  `timestamp`, `timestamptz` e `null`.
- Documentati in modo esplicito i gap MySQL ancora presenti nella release.

## [0.8.0] — snapshot interno, non rilasciato

- Sviluppo della sessione MySQL async e di `aread`/`acopy_from`, confluito nella
  successiva release pubblicata.

## [0.7.0] — snapshot interno, non rilasciato

- Sviluppo iniziale dello streaming Arrow MySQL con `MysqlSession.read`,
  confluito nella successiva release pubblicata.

## [0.6.0] — snapshot interno, non rilasciato

- Sviluppo iniziale di `MysqlSession.copy_from`, confluito nella successiva
  release pubblicata.

## [0.5.0] — 2026-08-14

- Aggiunta `MysqlSession` sincrona con execute, scalar, transazioni e probe.
- Aggiunti TLS MySQL, parametri tipizzati e mapping degli errori.

## [0.4.0] — 2026-08-14

- Creato lo scaffold pubblico MySQL e stabilita la forma delle factory.
- Aggiunti wheel Linux, macOS arm64 e Windows x86_64.

## [0.3.0] — 2026-08-14

- `copy_from` aggiunge `update`, `upsert` e `delete_by_keys` con chiavi
  esplicite.
- `read` aggiunge projection, ordering e limit.
- Gli input bulk accettano anche `list[dict]` e `pandas.DataFrame`.

## [0.2.0] — 2026-08-14

- `copy_from(mode="create")` crea il target dallo schema Arrow.
- Aggiunte le forme async di lettura e scrittura bulk.

## [0.1.3] — 2026-08-14

- Corretto il default `mapping_policy` di `copy_from` da `strict` a
  `compatible`.

## [0.1.2] — 2026-08-14

### Aggiunto

- `copy_from` sync e async via contratto `WriteOutcome`.
- Esempi di osservabilita basati su `Session.metrics()` e sugli attributi
  strutturati delle eccezioni.

### Breaking

- Rimosso `batch_rows` da `read`/`aread`: era accettato ma ignorato.

## [0.1.1] — 2026-08-14

- `p.version()` usa la versione del package Python invece di quella del
  workspace Rust.

## [0.1.0] — 2026-08-14

- Prima release del SDK in-process PostgreSQL/PostGIS.
- Sessioni sync e async, transazioni, savepoint, builder SQL portabile,
  parametri tipizzati, predicati spatial, lettura Arrow e gerarchia errori.
