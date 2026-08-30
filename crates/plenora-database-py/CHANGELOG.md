# Changelog — plenora-database Python SDK

Il changelog descrive l'impatto per chi aggiorna. La storia implementativa e le
review restano in Git. Fino alla 1.0, una minor puo contenere modifiche
incompatibili, sempre indicate come **breaking**.

## [0.11.0] — non rilasciata

### Aggiunto

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

### Breaking

- `execute_scalar` impone la cardinalita dichiarata: al massimo una riga ed
  esattamente una colonna. Zero righe restituiscono `None`; risultati piu
  larghi devono essere letti con `execute_returning_rows`.

### Corretto

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
