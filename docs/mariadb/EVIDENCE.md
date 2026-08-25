# MariaDB — matrice delle divergenze

Questo documento registra **cio che e stato misurato**, non cio che si
ricorda. ADR 0014 apre il ciclo MariaDB con una regola: evidenza prima della
scelta fra provider dedicato e qualificazione sotto il provider MySQL. Questa
e la prima evidenza.

Non e una decisione. MariaDB resta non qualificata e il provider `mysql`
continua a rifiutarla alla probe, come prima e per le stesse ragioni: una
riga `differs` qui sotto e un fatto, non un verdetto.

## Come riprodurla

```bash
docker compose -f docker-compose.mysql.yml up -d --wait
docker compose -f docker-compose.mariadb.yml up -d --wait
python scripts/check_mariadb_divergence.py            # verdetto JSON
python scripts/check_mariadb_divergence.py --markdown # la tabella qui sotto
```

Le sonde interrogano i tre server dal **socket locale** di ciascun container:
non attraversano `require-secure-transport` e non hanno bisogno della CA,
quindi un errore osservato e del motore e non del trasporto. Ogni sonda che
crea oggetti li droppa prima e dopo, cosi due corse di seguito danno lo stesso
risultato.

Il catalogo copre le superfici che il provider `mysql` **attraversa davvero**:
le variabili che legge alla probe, le istruzioni che emette a ogni
transazione, le colonne di `information_schema` da cui deriva gli indici, cio
che i piani di scrittura eseguono, lo spatial, i prepared statement e le
sequenze. Una divergenza su una superficie che il provider non tocca non e una
divergenza per noi.

## La matrice

| superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|
| probe | `probe.version` | 9.7.2 | 12.3.2-MariaDB-ubu2404 | 11.8.8-MariaDB-ubu2404 |
| probe | `probe.version_comment` | MySQL Community Server - GPL | mariadb.org binary distribution | mariadb.org binary distribution |
| probe | `probe.lower_case_table_names` | 0 | 0 | 0 |
| probe | `probe.sql_mode` | STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION | STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION | STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION |
| probe | `probe.transaction_isolation` | REPEATABLE-READ | REPEATABLE-READ | REPEATABLE-READ |
| sessione | `session.max_execution_time` | accettato | **rifiutato** — ERROR 1193 (HY000) at line 1: Unknown system variable 'MAX_EXECUTION_TIME' | **rifiutato** — ERROR 1193 (HY000) at line 1: Unknown system variable 'MAX_EXECUTION_TIME' |
| sessione | `session.isolation_serializable` | SERIALIZABLE | SERIALIZABLE | SERIALIZABLE |
| sessione | `session.context_variable` | acme | acme | acme |
| catalogo | `catalog.statistics_expression` | 0 | **rifiutato** — ERROR 1054 (42S22) at line 1: Unknown column 'EXPRESSION' in 'SELECT' | **rifiutato** — ERROR 1054 (42S22) at line 1: Unknown column 'EXPRESSION' in 'SELECT' |
| catalogo | `catalog.statistics_shape` | code_uk/0/code/1,PRIMARY/0/id/1 | code_uk/0/code/1,PRIMARY/0/id/1 | code_uk/0/code/1,PRIMARY/0/id/1 |
| scrittura | `write.on_duplicate_key_rowcount` | 2 | 2 | 2 |
| scrittura | `write.on_duplicate_key_second_unique` | 1:999,2:200 | 1:999,2:200 | 1:999,2:200 |
| scrittura | `write.truncate_survives_rollback` | 0 | 0 | 0 |
| scrittura | `write.delete_survives_rollback` | 2 | 2 | 2 |
| spatial | `spatial.srid_column` | accettato | **rifiutato** — ERROR 1064 (42000) at line 1: You have an error in your SQL syntax; check… | **rifiutato** — ERROR 1064 (42000) at line 1: You have an error in your SQL syntax; check… |
| spatial | `spatial.geometrycollection` | accettato | accettato | accettato |
| prepared | `prepared.instances_table` | 1 | 0 | 0 |
| sequenze | `sequence.create` | **rifiutato** — ERROR 1064 (42000) at line 1: You have an error in your SQL syntax; check… | accettato | accettato |

## Cosa dice

**Tre delle cinque divergenze dichiarate dal messaggio di fail-close non sono
divergenze.**

* **`INSERT ... ON DUPLICATE KEY`** si comporta allo stesso modo sui due
  motori, incluso il caso pericoloso: con due indici unici, una riga in
  ingresso collide sull'indice sbagliato e aggiorna la riga sbagliata —
  `1:999,2:200` su tutti e tre i server. Non e una differenza fra MySQL e
  MariaDB, e un comportamento di entrambi, ed e gia chiuso dal preflight
  Upsert fail-closed introdotto in 0.10.0. Vale identico su MariaDB.
* **`GEOMETRYCOLLECTION`** e accettata da entrambi.
* **La semantica di isolamento** coincide: stesso nome di variabile
  (`@@transaction_isolation`, non il vecchio `@@tx_isolation`), stesso default
  `REPEATABLE-READ`, e `SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE`
  accettato e riletto uguale.

**Una e vera ma rovesciata.** Le **sequenze** ci sono su MariaDB e non su
MySQL: `CREATE SEQUENCE` e accettato dal primo e rifiutato dal secondo con un
errore di sintassi. E una capability in piu, non una in meno, e il provider
non emette mai `CREATE SEQUENCE` — quindi non e un rischio, e un pezzo di
superficie che un eventuale provider dedicato potrebbe usare.

**Una e vera.** `performance_schema.prepared_statements_instances` non esiste
su MariaDB. Il gate `QueryOperation` la interroga per osservare `COUNT_EXECUTE`
senza esporre hook nel provider di produzione: su MariaDB quella osservazione
andrebbe rifatta in un altro modo.

**Due che nessuno aveva nominato, e sono quelle che romperebbero il provider
in produzione.**

* **`SET SESSION MAX_EXECUTION_TIME`** — `ERROR 1193: Unknown system variable`.
  Il provider la emette per **ogni** transazione che dichiara un timeout
  (`transaction.rs`). Su MariaDB l'equivalente e `max_statement_time`, che
  prende secondi invece di millisecondi: non e una variabile mancante, e una
  variabile diversa con un'unita diversa.
* **`information_schema.statistics.EXPRESSION`** — `ERROR 1054: Unknown
  column`. Il preflight Upsert legge quella colonna per riconoscere gli indici
  funzionali, che non sa confrontare e che deve rifiutare. Su MariaDB la query
  fallisce prima di arrivarci.

Il resto della forma di `information_schema.statistics` coincide:
`INDEX_NAME`, `NON_UNIQUE`, `COLUMN_NAME`, `SEQ_IN_INDEX` restituiscono le
stesse righe per la stessa tabella.

**Le due versioni di MariaDB non divergono fra loro.** Su tutte e diciotto le
sonde, 11.8.8 e 12.3.2 rispondono identico. Per quanto misurato finora, le
differenze appartengono al fork e non a una sua release — il che e una
ragione in piu per tenere entrambe le righe: e la prossima divergenza trovata
a dover dimostrare il contrario, non questa a doverlo assumere.

## Cosa resta aperto

La matrice copre le superfici che il provider attraversa oggi, non tutte
quelle che attraverserebbe. Restano da misurare, prima che una ADR possa
decidere:

* il **protocollo** dei prepared statement — `COM_STMT_PREPARE` e i metadati
  da cui il path query deriva lo schema, che si osservano solo con il driver
  e non con il client;
* le **funzioni spatial** portabili (`ST_*`) e il modo in cui MariaDB
  dichiara l'SRID di colonna, visto che l'attributo `SRID` non esiste;
* i **tipi wire** riga per riga, cioe il mapping su cui il provider costruisce
  gli Arrow batch;
* il comportamento sotto **cancellazione** e sotto errore ambiguo dopo il
  commit, dove il contratto Plenora e piu stretto del protocollo.

Fino ad allora il fail-close resta, e questo documento resta cio che e: una
misura, con la data della corsa e il comando per rifarla.

---

## Seconda tranche: driver e provider

La prima tranche ha misurato dal **client**. Il client non vede il protocollo:
i metadata di `COM_STMT_PREPARE`, i tipi wire, e cosa succede quando e il
provider ad attraversare quelle superfici. Questa tranche misura li, e tiene
separate due famiglie perche rispondono a due domande diverse — `raw`, cioe
cosa offre il protocollo con il driver diretto, e `provider`, cioe cosa
succede a **questo** codice.

### Come riprodurla

```bash
docker compose -f docker-compose.mysql.yml up -d --wait
docker compose -f docker-compose.mariadb.yml up -d --wait
python scripts/check_mariadb_driver.py            # verdetto JSON
python scripts/check_mariadb_driver.py --markdown # la tabella qui sotto
```

La misura gira **dentro il crate**, dove vive un bypass di solo test sul
rifiuto iniziale di MariaDB. Il bypass e `#[cfg(test)]`: non e una feature,
non e una variabile d'ambiente, non e un parametro, e nel binario pubblico
non esiste. Supera **solo** il rifiuto: SQL, mapping, timeout, transazioni e
classificazione degli errori restano quelli di oggi, ed e il punto — cio che
si osserva dopo e il comportamento reale del provider, non di una sua
variante indulgente.

Le connessioni sono TLS verificate contro la CA privata di ciascuna fixture;
il verdetto registra il cifrario negoziato accanto a versione e digest.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | protocollo | `raw.tls_cipher` | TLS_AES_128_GCM_SHA256 | TLS_AES_256_GCM_SHA384 | TLS_AES_256_GCM_SHA384 |
| raw | wire | `raw.type_table` | creata | creata | creata |
| raw | wire | `raw.type_row` | riga inserita | riga inserita | riga inserita |
| raw | spatial | `raw.geometry_table` | creata | creata | creata |
| raw | spatial | `raw.prepare_metadata_geometry` | shape:MYSQL_TYPE_GEOMETRY | shape:MYSQL_TYPE_GEOMETRY | shape:MYSQL_TYPE_GEOMETRY |
| raw | wire | `raw.prepare_metadata` | id:MYSQL_TYPE_LONGLONG, small_signed:MYSQL_TYPE_SHORT, big_unsigned:MYSQL_TYPE_LONGLONG… | id:MYSQL_TYPE_LONGLONG, small_signed:MYSQL_TYPE_SHORT, big_unsigned:MYSQL_TYPE_LONGLONG… | id:MYSQL_TYPE_LONGLONG, small_signed:MYSQL_TYPE_SHORT, big_unsigned:MYSQL_TYPE_LONGLONG… |
| raw | protocollo | `raw.prepare_parameters` | 1 | 1 | 1 |
| raw | spatial | `raw.column_srid` | Some(None) | **no** Server error: `ERROR 1054 (42S22): Unknown column 'SRS_ID' in 'SELECT'' | **no** Server error: `ERROR 1054 (42S22): Unknown column 'SRS_ID' in 'SELECT'' |
| raw | spatial | `raw.spatial_functions` | POINT srid=4326 wkb=21 byte | POINT srid=4326 wkb=21 byte | POINT srid=4326 wkb=21 byte |
| raw | sessione | `raw.max_execution_time` | accettato | **no** Server error: `ERROR 1193 (HY000): Unknown system variable 'MAX_EXECUTION_TIME'' | **no** Server error: `ERROR 1193 (HY000): Unknown system variable 'MAX_EXECUTION_TIME'' |
| raw | catalogo | `raw.statistics_expression` | presente | **no** Server error: `ERROR 1054 (42S22): Unknown column 'EXPRESSION' in 'SELECT'' | **no** Server error: `ERROR 1054 (42S22): Unknown column 'EXPRESSION' in 'SELECT'' |
| provider | protocollo | `provider.test_connection` | server_version=9.7.2 | server_version=12.3.2-MariaDB-ubu2404 | server_version=11.8.8-MariaDB-ubu2404 |
| provider | protocollo | `provider.capabilities` | create=true append=true upsert=true replace=true bulk=true spatial=26 | create=true append=true upsert=true replace=true bulk=true spatial=26 | create=true append=true upsert=true replace=true bulk=true spatial=26 |
| provider | catalogo | `provider.describe_object` | colonne=14 indici=1 | **no** Schema: colonna MySQL non valida (codice 1054) | **no** Schema: colonna MySQL non valida (codice 1054) |
| provider | wire | `provider.query_schema` | id:Int64/false/{plenora.mysql.native_type=bigint}, small_signed:Int16/false/{plenora.my… | id:Int64/false/{plenora.mysql.native_type=bigint}, small_signed:Int16/false/{plenora.my… | id:Int64/false/{plenora.mysql.native_type=bigint}, small_signed:Int16/false/{plenora.my… |
| provider | wire | `provider.query_values` | PrimitiveArray<Int64> [ 1, ] | PrimitiveArray<Int16> [ -7, ] | PrimitiveArray<UInt64> [… | PrimitiveArray<Int64> [ 1, ] | PrimitiveArray<Int16> [ -7, ] | PrimitiveArray<UInt64> [… | PrimitiveArray<Int64> [ 1, ] | PrimitiveArray<Int16> [ -7, ] | PrimitiveArray<UInt64> [… |
| provider | wire | `provider.read` | schema=[id:Int64/false, small_signed:Int16/false, big_unsigned:UInt64/false, exact_deci… | — dipende da provider.describe_object, che non ha raggiunto il catalogo: la superficie no… | — dipende da provider.describe_object, che non ha raggiunto il catalogo: la superficie no… |
| provider | spatial | `provider.read_geometry` | **no** Crs: colonna spatial MySQL senza SRID dichiarato | — dipende da provider.describe_object, che non ha raggiunto il catalogo: la superficie no… | — dipende da provider.describe_object, che non ha raggiunto il catalogo: la superficie no… |
| provider | sessione | `provider.transaction` | commit Committed | **no** Execution: errore server MySQL redatto (codice 1193) | **no** Execution: errore server MySQL redatto (codice 1193) |
| provider | sessione | `provider.cancellation_inflight` | Cancelled/None/retry=Never | Cancelled/None/retry=Never | Cancelled/None/retry=Never |
| provider | sessione | `provider.session_quarantine` | stato=Quarantined riusabile=false | stato=Quarantined riusabile=false | stato=Quarantined riusabile=false |
| provider | sessione | `provider.session_reuse` | connessione rimpiazzata | connessione rimpiazzata | connessione rimpiazzata |
| provider | commit | `provider.ambiguous_commit` | — richiede fault injection deterministica sul COMMIT — uccidere la connessione a meta com… | — richiede fault injection deterministica sul COMMIT — uccidere la connessione a meta com… | — richiede fault injection deterministica sul COMMIT — uccidere la connessione a meta com… |

### Cosa dice

**Il protocollo e quasi lo stesso.** Dodici colonne su quattordici hanno lo
stesso tipo wire nei metadata del prepare, `ST_*` risponde identico, il numero
di parametri dichiarati coincide, e la riga di dati e accettata da tutti e
tre. Due colonne no, e dal client non si vedevano:

* **`JSON` viaggia come `MYSQL_TYPE_BLOB`** su MariaDB e come
  `MYSQL_TYPE_JSON` su MySQL. Su MariaDB `JSON` e un alias di `LONGTEXT` con
  un CHECK, e il protocollo lo dice;
* **`TIMESTAMP` porta il flag `unsigned`** su MariaDB e non su MySQL.

**Il mapper piega le due divergenze sullo stesso tipo, e ne lascia passare
una nei metadata.** Lo si vede solo attraverso `QueryOperation`, che deriva lo
schema dai metadata di `COM_STMT_PREPARE` — `statement.columns()`, non il
catalogo — e quindi raggiunge il mapper anche dove `information_schema` non
risponde.

Su tutti e tre i server `document` diventa `Utf8` e `moment_timestamp`
diventa `Timestamp(Microsecond)`: il `DataType` coincide perche il mapper
sceglie il `kind` dal tipo wire **piu** i flag e il charset, e `JSON` e un
`BLOB` con charset non binario finiscono sullo stesso ramo. I **valori**
decodificati coincidono per intero: stesso digest sulle quattordici colonne,
calcolato sul contenuto completo e non su una rappresentazione troncata.

Lo schema Arrow completo pero **non** coincide, e la differenza sta dove il
`DataType` non guarda:

| campo | MySQL | MariaDB |
|---|---|---|
| `document` | `Utf8/true/{plenora.mysql.native_type=json}` | `Utf8/true/{plenora.mysql.native_type=text}` |

Tredici campi su quattordici sono identici; questo no. E una **divergenza
vera del contratto pubblicato**, non un dettaglio interno: `MYSQL_NATIVE_TYPE`
e cio che il consumer legge per sapere cosa fosse la colonna, e dalla stessa
DDL — `document JSON` su entrambi i motori — escono due annotazioni diverse.

Ed e anche un'annotazione **legittima**: su MariaDB `JSON` e un alias di
`LONGTEXT`, e il protocollo dice davvero `text`. Le due letture non si
escludono, ed e proprio per questo che la decisione appartiene al profilo:

* se `MYSQL_NATIVE_TYPE` descrive **il wire**, i due valori sono entrambi
  corretti e il contratto va documentato come tale — oggi non lo dice;
* se descrive **la DDL**, come un consumer che decide se parsare il JSON si
  aspetta, allora il profilo deve normalizzarlo, e per farlo serve una regola
  per prodotto: sul path query il catalogo non viene letto, quindi la
  normalizzazione dovrebbe vivere nel mapping wire.

In entrambi i casi e il profilo a doverla possedere. Registrata come
divergenza, non risolta qui.

Senza questa sonda l'evidenza si sarebbe fermata al driver: `read` passa da
`describe_object`, quindi su MariaDB non raggiunge mai il mapper, e i tipi
sarebbero rimasti verificati solo in forma grezza. E senza i metadata nel
confronto, la sonda avrebbe detto "schema identico" osservando meta schema.

**`SRS_ID` non esiste** in `information_schema.columns` su MariaDB — errore
1054 — quindi non c'e modo di sapere se una geometry abbia un sistema di
riferimento vincolato. E la controparte, al livello del catalogo,
dell'attributo `SRID` di colonna che la prima tranche aveva gia visto
rifiutare.

**Dove il provider si ferma, e cosa servirebbe.** Due sonde falliscono su
MariaDB, e nessuna delle due per un limite del motore: sono istruzioni che
**questo** provider emette sempre.

| superficie | cosa emette oggi | cosa chiederebbe un profilo |
|---|---|---|
| catalogo | `information_schema.statistics.EXPRESSION` (1054) | query di catalogo per prodotto, e un modo diverso di riconoscere gli indici funzionali |
| timeout | `SET SESSION MAX_EXECUTION_TIME` (1193) | l'istruzione del prodotto e la conversione dell'unita — `max_statement_time` prende secondi, non millisecondi |
| mapping wire | stesso `DataType` e stessi valori, ma `native_type` divergente su `JSON` | decidere se `MYSQL_NATIVE_TYPE` annoti il wire o la DDL, e normalizzarlo per prodotto se e la seconda |
| spatial / SRID | `SRID` di colonna e `SRS_ID` di catalogo | una strategia SRID per prodotto, e capability spatial dichiarate di conseguenza |

Non sono "due righe da cambiare": sono quattro superfici, di cui due gia
rotte, una che diverge nei metadata pubblicati e una non ancora misurata. E la misura di
quanto costerebbe condividere il codice, non la prova che sia gratis.

**Cio che il provider fa uguale.** Probe, capability, mapper — e, questo e il
risultato meno atteso, **tutta la sessione**: una query cancellata mentre il
server risponde e `Cancelled` con `remote_effect: none` e `retry: never` su
tutti e tre, la sessione finisce in `Quarantined` e smette di essere
riusabile su tutti e tre, e il provider la rimpiazza su tutti e tre. La
macchina a stati, che e la parte piu delicata del provider, non distingue i
due motori.

`provider.read` e `provider.read_geometry` restano `not_measured` su MariaDB:
dipendono dal catalogo, e registrarle come rifiuti autonomi conterebbe tre
divergenze dove ce n'e una. `provider.read_geometry` su MySQL e rifiutato
dalla regola del provider — una geometry senza SRID dichiarato — non dal
motore: la fixture non puo dichiarare l'SRID perche MariaDB non accetta
quell'attributo, e tabelle diverse non sarebbero confrontabili.

**Le due versioni di MariaDB non divergono fra loro** su nessuna delle
ventitre sonde, come nella prima tranche.

### Cosa resta not_measured

`provider.ambiguous_commit`. Un commit di esito ignoto si osserva solo con
fault injection deterministica: uccidere la connessione a meta `COMMIT` da
una seconda sessione e una corsa, non un esperimento ripetibile, e un esito
ottenuto cosi non distingue il comportamento del provider dal momento in cui
e arrivato il colpo. Resta `not_measured`, senza inferenze: un esito assente
non e un esito negativo.

## Terza tranche: la semantica di sessione

Le prime due tranche hanno misurato catalogo, protocollo e superfici del
provider. Restava fuori cio che sta **prima**: il bootstrap che il pool
applica a ogni connessione, e le opzioni con cui una transazione viene
aperta. La fase 1 li aveva lasciati fuori dal profilo dichiarandoli residui,
che non e una decisione finche nessuno ha guardato.

`scripts/check_session_matrix.py` li misura sui tre riferimenti gia accesi,
attraverso il driver **e** attraverso il percorso reale del pool — dove il
bootstrap arriva come `setup` e viene applicato prima di qualunque probe.
Tredici sonde: bootstrap eseguito a mano, bootstrap ricevuto dal pool,
bootstrap dopo il rientro di una sessione sporca, i quattro livelli di
isolamento, le tre modalita di accesso, il session context, commit e
rollback. La matrice, con i dettagli osservati, e in
`docs/mariadb/SESSION-MATRIX.md`.

**Tredici sonde su tredici coincidono sui tre server, e tutte e tredici
soddisfano il proprio contratto.** Le due cose sono distinte, e la seconda e
quella che conta: `accepted` significa che l'osservato coincide con l'atteso,
non che la misura sia riuscita. Senza questa distinzione sarebbero passati un
READ ONLY che accetta scritture ovunque, quattro livelli che riportano tutti
lo stesso valore sbagliato, un rollback che non annulla.

### Cosa ne segue

Il bootstrap di sessione, i livelli di isolamento e `START TRANSACTION`
**restano codice condiviso**. Non entrano nel profilo, e la ragione e la
misura: spostarli sarebbe simmetria, non una decisione, e ADR 0014 chiede il
contrario — nel profilo entra cio che diverge.

La matrice e una prova permanente, non una fotografia. Il runner rifiuta un
albero sporco prima di avviare Docker e verifica che HEAD non sia cambiato a
misura finita; poi fallisce se l'inventario delle sonde non e esattamente
quello dichiarato, se una sonda non e accettata, o se una diverge.

Ed e **eseguita**: la campagna `session-matrix` accende i tre riferimenti a
cadenza settimanale e su richiesta, riesegue la misura e confronta il
documento rigenerato con quello committato. Senza quella corsa il self-test
statico verificherebbe il giudizio del runner e nient'altro: un cambio di
`SESSION_BOOTSTRAP_SQL` lascerebbe il documento di ieri e la CI verde. La seconda
condizione non e ridondante rispetto alla terza: "coincidono" e vero anche
quando tutti e tre falliscono allo stesso modo, ed e il verde falso che questa
misura esiste per escludere.

### Cosa questa tranche non prova

Tre sonde hanno dovuto essere corrette prima di leggere i numeri, e la terza
correzione ha ristretto cio che si puo affermare.

`access_mode` leggeva `@@transaction_read_only`, che riflette `SET
TRANSACTION` e non `START TRANSACTION READ ONLY`: dava lo stesso valore per
tutte e tre le modalita. Ora si osserva l'effetto — una scrittura dentro la
transazione — e i tre casi si distinguono: ammessa, rifiutata, ammessa. Il
rifiuto dev'essere **quello del read-only**, per intero: codice 1792,
categoria `Execution`, effetto `None`, retry `Never`, identici sui tre
server. Il solo codice non basterebbe — una regressione che classificasse
quel codice come autenticazione, o ne dichiarasse l'effetto ignoto, o lo
rendesse ritentabile, resterebbe accettata ovunque, e sono le tre cose che
decidono cosa il chiamante fa dopo.

Il contesto rileggeva isolamento e autocommit, cioe nulla che lo riguardasse.
Ora rilegge la variabile utente che il provider imposta.

La sonda sul riuso pretendeva che il pool riconsegnasse la **stessa**
connessione dopo la restituzione. Fallisce su tutti e tre: `mysql_async` ne
apre una nuova. Cio che resta provato e che ogni sessione consegnata dal pool
e bootstrappata — la proprieta su cui il provider poggia — mentre la
riapplicazione del bootstrap su una connessione **riusata** non e esercitata
da questa configurazione. Resta non misurata, ed e scritto qui invece di
essere sottinteso da una sonda verde.

## Quarta tranche: i codici di errore, e il profilo attraversato davvero

Le prime tre tranche hanno misurato cosa i motori fanno. Questa misura due
cose che nessuna delle tre poteva vedere, e che una review ha nominato per
prima: **quali codici** arrivano dalle superfici che la classificazione
traduce in categoria, retry ed effetto remoto; e cosa succede quando le stesse
superfici vengono attraversate con il **profilo di MariaDB** invece che con
quello di MySQL.

La seconda domanda non e teorica. Fino a qui il profilo nuovo era esercitato
solo da test offline: le sue query di catalogo compilavano, nessuno le aveva
mai mandate a un server. La prima corsa di questa tranche le ha bocciate.

Gira con lo stesso comando della seconda tranche, che ora include anche queste
sonde:

    python scripts/check_mariadb_driver.py --markdown

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | errori | `raw.error_access_denied` | 1045 | 1045 | 1045 |
| raw | errori | `raw.error_unknown_column` | 1054 | 1054 | 1054 |
| raw | errori | `raw.error_unknown_table` | 1146 | 1146 | 1146 |
| raw | errori | `raw.error_unknown_database` | 1142 — `SELECT command denied` | 1142 — `SELECT command denied` | 1142 — `SELECT command denied` |
| raw | errori | `raw.error_duplicate_key` | 1062 | 1062 | 1062 |
| raw | errori | `raw.error_not_null` | 1048 | 1048 | 1048 |
| raw | errori | `raw.error_foreign_key` | 1452 | 1452 | 1452 |
| raw | errori | `raw.error_check_violation` | **3819** | **4025** | **4025** |
| raw | errori | `raw.error_privilege` | 1142 | 1142 | 1142 |
| raw | errori | `raw.error_lock_wait` | 1205 | 1205 | 1205 |
| raw | errori | `raw.error_deadlock` | 1213 — effetto=vittima annullata | 1213 — effetto=vittima annullata | 1213 — effetto=vittima annullata |
| raw | errori | `raw.error_statement_timeout` | **3024** | **1969** | **1969** |
| raw | errori | `raw.functional_index_ddl` | accettato | **rifiutato** — ERROR 1064: sintassi | **rifiutato** — ERROR 1064: sintassi |
| provider | profilo | `provider.profile_probe` | versione=9.7.2, qualificata=nessun elenco dichiarato | versione=12.3.2-MariaDB-ubu2404, qualificata=11.8 12.3 | versione=11.8.8-MariaDB-ubu2404, qualificata=11.8 12.3 |
| provider | profilo | `provider.profile_describe_object` | colonne=14 indici=1 | colonne=14 indici=1 | colonne=14 indici=1 |
| provider | profilo | `provider.profile_describe_geometry` | **no** Crs: colonna spatial MySQL senza SRID dichiarato | **no** Crs: colonna spatial MariaDB senza SRID dichiarato | **no** Crs: colonna spatial MariaDB senza SRID dichiarato |
| provider | profilo | `provider.profile_functional_index` | `plenora_idx_expression`:colonne=0 confrontabile=false, `PRIMARY`:colonne=1 confrontabile=true | `PRIMARY`:colonne=1 confrontabile=true | `PRIMARY`:colonne=1 confrontabile=true |
| provider | profilo | `provider.profile_timeout` | Timeout/Never/None — timeout MySQL (codice 3024) | Timeout/Never/None — timeout MariaDB (codice 1969) | Timeout/Never/None — timeout MariaDB (codice 1969) |

### Cosa dice

**Il timeout diverge due volte, e la seconda era invisibile.** Che
`max_statement_time` sostituisca `MAX_EXECUTION_TIME` lo sapeva gia la prima
tranche. Che lo statement interrotto arrivi come **1969** invece che come
**3024** non lo sapeva nessuno, e la tabella dei codici non conosce 1969: il
limite scattava davvero e il chiamante leggeva "errore server redatto" invece
di un timeout, cioe non poteva distinguere un limite che aveva fatto il suo
lavoro da un guasto. Un'istruzione corretta con una classificazione ereditata
e peggio di un'istruzione sbagliata, perche non fallisce.

`SELECT SLEEP(1)` non serviva a misurarlo. Su MySQL 9.7 il timer non
interrompe la `SLEEP`, e la prima corsa registrava "nessun errore" dove ci si
aspettava un codice. La sonda usa ora una scansione incrociata di
`information_schema`, che i due motori interrompono entrambi.

**Otto codici su undici coincidono**, e per quelli la tabella condivisa regge:
1045, 1048, 1054, 1062, 1146, 1205, 1213, 1452 sono arrivati dagli stessi
tentativi sui tre server. Il piu importante e 1213, l'unico della tabella che
dichiara `retry: Safe` e `remote_effect: RolledBack` — cioe autorizza a
rifare l'operazione e afferma che non c'e nulla da ripulire. La sonda non si
ferma al codice: dopo il deadlock verifica, dal commit del superstite, che
della transazione della vittima non sia rimasto niente. `vittima annullata`
sui tre server e cio che rende quelle due promesse una misura invece di una
citazione.

**Il CHECK diverge:** lo stesso `INSERT` che viola lo stesso vincolo arriva
come 3819 da MySQL e come 4025 da MariaDB. Sono i due codici che la
diagnostica di riga traduce in "vincolo violato", e ciascun profilo attribuisce
il proprio.

**Due codici della tabella non sono mai arrivati.** 1044 e 1049 — autorizzazione
e schema inesistente — non si osservano da questa configurazione: entrambi i
tentativi ricevono **1142** prima di arrivarci, perche il permesso manca prima
che il nome venga risolto. Restano `not_measured` e finiscono nel verdetto
generico del profilo MariaDB, che e `Execution`/`Never`.

**1142 invece era una lacuna, e la misura l'ha scoperta.** Non era nella
tabella di nessuno dei due prodotti: anche su MySQL, fino a questa tranche, un
errore di privilegio si classificava come esecuzione generica — un guasto,
invece che un permesso mancante, che sono due cose con due rimedi diversi.
Arriva identico dai tre riferimenti, quindi e ora `Authorization`/`Never` per
entrambi. E un cambio al comportamento di un provider qualificato, e sta in un
commit suo per questo.

**Su MariaDB un indice su espressione non si crea nemmeno.** `CREATE INDEX ...
((LOWER(col)))` e un errore di sintassi (1064). Il profilo dichiara di non
pubblicare le parti funzionali e rifiuta una parte senza colonna ne
espressione: quel rifiuto resta la risposta giusta — una parte che non si sa
leggere non e un indice su nessuna colonna — ma va letto per quello che e, una
difesa contro un caso che questi due server non producono per quella via.
Come MariaDB indicizzi una colonna generata, e come `statistics` la descriva,
non e misurato.

**La qualifica della versione e attraversata, non solo dichiarata.** La probe
con il profilo del prodotto e l'unico punto in cui riconoscimento e qualifica
vengono eseguiti: le altre sonde partono da una sessione gia aperta e li
salterebbero. Il bypass di test — che questa misura accende sempre — supera il
rifiuto del prodotto, e **solo** quello: la qualifica sta fuori dal blocco,
altrimenti l'unica corsa che deve dimostrarla sarebbe anche l'unica a non
attraversarla. I due riferimenti passano la propria lista (11.8, 12.3); MySQL
non dichiara elenco, e la sonda lo registra invece di lasciarlo intendere.

**Il profilo, attraversato davvero, funziona — dopo una correzione.** Le query
di catalogo che dichiarano `NULL AS srs_id` e `NULL AS expression` girano sui
due riferimenti e descrivono l'oggetto come su MySQL: colonne=14, indici=1. La
prima corsa pero falliva, con `DataMapping: campo catalogo
generation_expression non convertibile`: su MariaDB `GENERATION_EXPRESSION` e
**NULL** per le colonne non generate, dove MySQL manda la stringa vuota. Il
profilo la normalizza ora con `COALESCE`, ed e una divergenza che nessun test
offline poteva trovare — la query compilava, ed era sbagliata.

E la geometria si ferma dove deve: `Crs: colonna spatial MariaDB senza SRID
dichiarato`, con il nome del prodotto che ha rifiutato. Su MariaDB quel
rifiuto e strutturale, perche `srs_id` arriva sempre nullo.

### Cosa resta not_measured

* **1044 e 1049** su tutti e tre i server, per la ragione detta sopra: il
  privilegio manca prima del nome. Servirebbe un utente con grant diversi.
* ~~**1142** e misurato ma non classificato da nessun profilo.~~ **Chiuso**:
  arriva identico dai tre riferimenti, ed e ora `Authorization`/`Never` per
  entrambi i prodotti. Il cambio tocca anche il provider MySQL qualificato —
  un errore di privilegio si classificava come esecuzione generica, cioe come
  un guasto invece che come un permesso mancante — ed e stato fatto in un
  commit suo, con il gate completo rieseguito.
* **Come MariaDB pubblica un indice su colonna generata**, che e l'unica forma
  di indice non su colonna semplice che quel motore accetta.
* **Le scritture attraverso il profilo**: nessun piano di scrittura e stato
  eseguito con `MARIADB_PROFILE`. Le capability di scrittura restano chiuse.

## Quinta tranche: la lettura, dall'inizio alla fine

Il punto 1 della fase 3: `read` attraversato con il profilo del prodotto sui
due riferimenti qualificati, e verificato su tre cose separate — lo schema che
pubblica, i valori che decodifica, il namespace con cui li annota. Tre
osservazioni e non una, perche una riga verde sola direbbe "la lettura
funziona" senza dire quale parte regge.

La sonda storica `provider.read` resta dov'e e continua a misurare il provider
`MySQL`: su MariaDB si ferma al catalogo che non risponde, ed e giusto che
continui a dirlo.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_read_schema` | 14 campi, namespace `plenora.mysql.*` | 14 campi, namespace `plenora.mariadb.*` | 14 campi, namespace `plenora.mariadb.*` |
| provider | profilo | `provider.profile_read_values` | digest `8501c769…` | **identico** | **identico** |
| provider | profilo | `provider.profile_read_namespace` | annotate=14 estranee=0 su 14 campi | annotate=14 estranee=0 su 14 campi | annotate=14 estranee=0 su 14 campi |
| provider | profilo | `provider.profile_read_projection` | tre colonne nell'ordine dichiarato | idem | idem |
| provider | profilo | `provider.profile_read_filter_forms` | tredici forme, conteggi e prima riga attesi | idem | idem |
| provider | profilo | `provider.profile_read_filter_closed_like` | **rifiutato** — LIKE case-insensitive | **rifiutato** | **rifiutato** |
| provider | profilo | `provider.profile_read_filter_closed_spatial` | **rifiutato** — filtro spatial | **rifiutato** | **rifiutato** |
| provider | profilo | `provider.profile_read_ordering_asc` | primo=1 | primo=1 | primo=1 |
| provider | profilo | `provider.profile_read_ordering_desc` | primo=8193 | primo=8193 | primo=8193 |
| provider | profilo | `provider.profile_read_streaming` | batch=2 righe=8193 digest `21b5b708…` | **identico** | **identico** |

### Cosa dice

**Le sonde verificano un contratto, non l'assenza di errore.** E la
correzione che ha cambiato la tranche: la prima stesura registrava `accepted`
per qualunque `Ok`, quindi una projection ignorata, un ordinamento che non
ordina o uno stream consegnato in un colpo solo sarebbero finiti verdi — su
tutti e tre i server, con l'aria di una convergenza. Ogni sonda dichiara ora
righe attese, colonne attese, batch attesi e primo valore atteso; il
confronto e una funzione pura, e i suoi sei modi di fallire hanno un self-test
offline ciascuno.

**I valori coincidono per intero.** Quattordici famiglie di tipo, stesso
digest sui tre server — e il digest copre **tutti** i batch, non il primo:
sulle 8193 righe dello stream lungo i tre server rendono `21b5b708…` senza
differenze.

**Lo streaming e misurato al taglio, non per abbondanza.** La tabella ha
`DEFAULT_BATCH_ROWS + 1` righe, cioe la piu piccola che non puo stare in un
batch solo: due batch su 8193 righe significa che il lettore rispetta il
proprio limite, e uno solo avrebbe significato che lo ignora. La prima
stesura provava a spezzare il batch con un budget di memoria stretto e ha
misurato altro — i budget sono **cumulativi**, non per batch, quindi la
lettura moriva di `ResourceLimit` a meta invece di consegnare piu batch.

**Le tredici forme di filtro qualificate rendono cio che devono.**
`filter = true` copriva una superficie dedotta da `id = ?`: ora ogni forma che
il renderer qualifica — `Eq`, `Ne`, `Lt`, `Lte`, `Gt`, `Gte`, `IsNull`,
`IsNotNull`, `In`, `Between`, `Like`, `And`, `Or` — ha il proprio conteggio e
la propria prima riga, e i tre server rendono la stessa riga di esiti:
`eq=1/7 ne=8192/1 lt=99/1 lte=100/1 gt=3/8191 gte=4/8190 is_null=2731/3
is_not_null=5462/1 in=3/1 between=11/10 like=1/7 and=2/8191 or=2/1`. Il primo
valore accanto al conteggio non e ridondante: novantanove righe le rende anche
`id > 8093`.

Le due forme che il renderer **non** qualifica — `LIKE` case-insensitive e il
filtro spatial — restano rifiutate su tutti e tre, e hanno una sonda propria:
senza, `filter = true` si leggerebbe come "tutte le forme", che e la lettura
che il flag non sostiene.

Quelle due sonde verificano il rifiuto **per intero** — categoria, fase,
effetto remoto, retry e causa — e non "ha dato errore". Non e pedanteria: la
prima stesura nominava una colonna che quella tabella non ha, e il rifiuto
arrivava da `NotFound` invece che dalla regola sorvegliata. Il fail-close
sembrava verificato e non lo era, e a scoprirlo e stato il contratto. Lo
stesso vale per il timeout, che ora e `Timeout/Read/Never/None` verificato
nella quaterna e non un `Err` qualunque.

**Il namespace regge fino ai metadata pubblicati.** Quattordici campi
annotati con la chiave del proprio prodotto, zero con quella dell'altro, su
tutti e tre. E la verifica che la scelta del namespace non si fermi al profilo
ma arrivi allo schema che il consumatore riceve.

**Tre divergenze nuove nello schema pubblicato**, tutte nel campo
`native_declaration` o nella collation, nessuna nel `DataType` e nessuna nei
valori:

| campo | MySQL 9.7 | MariaDB |
|---|---|---|
| `id` | `bigint` | `bigint(20)` |
| `small_signed` | `smallint` | `smallint(6)` |
| `text_utf8` (collation) | `utf8mb4_0900_ai_ci` | `utf8mb4_uca1400_ai_ci` |
| `document` (DDL `JSON`) | `json` | `longtext`, collation `utf8mb4_bin` |

La larghezza di visualizzazione — `(20)`, `(6)` — MySQL l'ha tolta dalla 8.0.19,
MariaDB la pubblica ancora. Le collation sono i default dei due server, non
una scelta del provider. Entrambe sono fedeli a cio che il catalogo ha
risposto, ed e cio che il contratto dichiara di annotare.

**`JSON` diverge una seconda volta, e in un modo che MySQL non ha.** Sul path
query MariaDB annota `text`, sul path catalogo annota `longtext`: due nomi
diversi per la stessa colonna, a seconda della strada. Su MySQL le due strade
dicono entrambe `json`. Non e una violazione del contratto — ogni annotazione
descrive cio che quella strada ha osservato, e sul filo `LONGTEXT` e `TEXT`
non si distinguono oltre la lunghezza massima — ma e un'asimmetria che il
prodotto qualificato non ha, e che un consumer che alterna le due strade
vedrebbe. Registrata, non risolta: normalizzarla richiederebbe di decidere
quale delle due strade abbia ragione, e nessuna delle due ha torto.

### Il gate, e cosa lo rende rosso

Una sonda che diventa `rejected` e, per il resto della matrice, una misura: e
il modo giusto di raccontare due motori diversi, e il runner la registra senza
fallire. Per le sonde su cui poggia una capability **gia pubblicata** no. Il
runner porta ora due inventari — `REQUIRED_ACCEPTED_PROBES` e
`REQUIRED_REJECTED_PROBES` — e esce diverso da zero se una prova necessaria
manca, cambia esito o sparisce dalla matrice.

Serviva perche prima non era cosi: due rifiuti identici sui tre server erano
`same`, e `same` usciva con zero. La perturbazione dimostrava che
l'osservazione cambiava, non che la regressione venisse fermata — mentre il
profilo continuava a pubblicare le quattro bandiere come `true`.

Gli inventari sono tre, non due: `OBSERVATION_ONLY_PROBES` raccoglie le sonde
che **osservano e basta**, e nessuna capability pubblicata poggia su di loro —
oggi `provider.profile_functional_index`, che racconta come il catalogo
descrive gli indici mentre il contratto semantico dell'indice arriva al punto
2. Il terzo elenco esiste per non avere una terza categoria implicita: "non e
in nessun inventario" e indistinguibile da "qualcuno si e dimenticato di
classificarla". Una guardia pretende che ogni sonda `provider.profile_*` stia
in **esattamente** uno dei tre, senza sovrapposizioni.

Per le sonde il cui rifiuto e la prova, `rejected` significa **quel** rifiuto.
Una sessione che non si apre, una transazione che non comincia, un catalogo
che non risponde: sono tutti `Err`, e registrarli come rifiuti direbbe che la
regola sorvegliata ha fatto il suo lavoro quando la sonda non ci e mai
arrivata. Diventano `not_measured`, e il gate li conta come prova mancante —
verificato perturbando il checkout del timeout, che finisce sulla porta
sbagliata, e la causa del rifiuto geometrico, che diventa un oggetto assente:
sei violazioni, tre per sonda.

E i nomi duplicati vengono rifiutati **prima** che una lista diventi un
dizionario, in entrambi i punti in cui succede. Due voci con lo stesso nome ne
producono una sola: su un server sarebbe una divergenza inventata, su tutti e
tre una sonda che non esiste piu e continua a comparire.

### Cosa ne segue per le capability

Quattro bandiere di lettura si aprono per MariaDB — `streaming`, `projection`,
`filter`, `ordering` — e ciascuna ha le proprie sonde, con attese esatte.
`filter` copre le tredici forme qualificate e nessuna delle due chiuse. Le altre quattro
restano chiuse perche il crate **non le offre a nessuno dei due prodotti**:
`server_cursor`, `pagination` e `resumable` sono false anche per MySQL, quindi
qui non c'e niente da qualificare. La quarta di allora indirizzava per finestre
di objectId e non appartiene piu al contratto.

Le scritture restano chiuse per intero: nessun piano di scrittura e mai stato
eseguito con questo profilo. E lo spatial resta chiuso in lettura, perche su
MariaDB `srs_id` arriva sempre nullo e ogni colonna geometrica viene rifiutata
alla descrizione — la lettura funziona su tutto tranne che li.

## Sesta tranche: l'indice su colonna generata, e cosa ne fa l'Upsert

Il punto 2 della fase 3. La domanda non e se MariaDB sappia indicizzare
un'espressione — sa farlo, in un modo solo — ma **come si presenta** al
catalogo, perche da li discendono due decisioni: se la colonna sia scrivibile,
e se l'indice sia confrontabile con le keys di un Upsert.

La prima ha un rischio che questa tranche esisteva per escludere, e riguarda
una correzione fatta nella quinta. Su MariaDB `GENERATION_EXPRESSION` e NULL
dove MySQL manda la stringa vuota, e il profilo la normalizza con
`COALESCE(..., '')` perche il lettore pretende una stringa. Se fosse NULL
**anche** per le colonne generate, quella normalizzazione trasformerebbe una
colonna non scrivibile in una scrivibile: un fail-open introdotto da un fix.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | catalogo | `raw.generated_column_catalog` | extra=VIRTUAL GENERATED, espressione=`lower(\`name\`)`, indice_su=lname, non_unique=0 | idem, espressione=`lcase(\`name\`)` | idem, espressione=`lcase(\`name\`)` |
| provider | profilo | `provider.profile_generated_index` | espressione_non_vuota=true, `PRIMARY`:id unico confrontabile, `uq_lname`:lname unico confrontabile | **identico** | **identico** |
| provider | profilo | `provider.profile_upsert_on_primary_key` | **rifiutato** — Unsupported/Prepare: altro PK/UNIQUE index (`uq_lname`) | **identico** | **identico** |
| provider | profilo | `provider.profile_upsert_on_generated_key` | **rifiutato** — Unsupported/Prepare: altro PK/UNIQUE index (`PRIMARY`) | **identico** | **identico** |
| provider | profilo | `provider.profile_upsert_generated_anchor` | **rifiutato** — DataMapping/Prepare: non puo scrivere una colonna generata | **identico**, con il proprio nome | **identico**, con il proprio nome |

### Cosa dice

**Il fail-open non c'e.** Su MariaDB `GENERATION_EXPRESSION` e NULL soltanto
per le colonne **non** generate; quella generata porta la sua espressione. La
normalizzazione con `COALESCE` coincide percio esattamente con la semantica di
MySQL — vuota significa scrivibile, non vuota significa rifiutata — e la sonda
lo verifica attraverso il profilo: `espressione_non_vuota=true` su tutti e tre.

**L'indice su colonna generata e un indice ordinario.** `COLUMN_NAME` porta il
nome della colonna, `NON_UNIQUE` e zero, e il profilo lo descrive
confrontabile per colonne su tutti e tre i riferimenti. Ne segue che il
rifiuto per "parte senza colonna ne espressione", che il profilo di MariaDB
puo produrre perche dichiara `NULL AS expression`, difende da una forma che
questi due server non producono: la sola via all'indice su espressione, li, e
la colonna generata, e quella via passa dal catalogo come qualunque colonna.

**L'espressione e scritta in due modi.** `lower(\`name\`)` su MySQL,
`lcase(\`name\`)` su MariaDB, dalla stessa DDL: e la forma canonica con cui
ciascun motore riscrive la funzione. Finisce nel token di schema, che serve a
riconoscere un cambio di schema fra prepare ed esecuzione **sullo stesso
server**, quindi la differenza non attraversa nulla. Registrata perche non
venga scambiata per un difetto la prima volta che qualcuno confronta due token.

**L'Upsert chiude tutte e tre le forme, e non sempre con la stessa guardia.**
Una tabella con un indice unico su colonna generata ne permette tre, e nessuna
e sicura:

| forma | chi la ferma |
|---|---|
| keys = chiave primaria, con un secondo indice unico sulla generata | il preflight: `ON DUPLICATE KEY UPDATE` scatterebbe anche su quell'altro indice |
| keys = colonna generata, con una chiave primaria | il preflight, per lo stesso motivo visto dall'altro lato |
| keys = colonna generata, **senza** chiave primaria | non il preflight — l'indice coincide con le keys ed e confrontabile — ma la guardia che vieta di scrivere una colonna generata |

La terza riga e la piu istruttiva: il preflight sugli indici non ha niente da
obiettare, e cio che salva la scrittura e una regola diversa, piu avanti nel
piano. Se domani cadesse quella, il preflight direbbe ancora di si. Per questo
le tre sonde stanno fra le prove necessarie del gate, con la categoria e la
causa del rifiuto verificate: sono la rete sotto `writes.upsert`, che oggi e
chiusa e che il punto 3 aprira una mode alla volta.

**Cosa verifica ciascuna sonda, e cosa distingue.** Le tre sonde di questa
tranche registravano cio che vedevano; ora pretendono cio che serve, ed e la
differenza fra un dato e una prova:

* `raw.generated_column_catalog` separa tre esiti e non due. La riga c'e, la
  riga non c'e, oppure **la domanda non e arrivata**: un privilegio mancante o
  una query incompatibile finivano fra le assenze, e un'assenza inventata e un
  fatto registrato che nessuno ha osservato. L'errore diventa `not_measured`,
  con il codice del server.
* `provider.profile_generated_index` confronta la struttura intera — colonna
  presente, espressione non vuota, indice sulla sola colonna generata, unico e
  confrontabile. Da quella forma dipendono due decisioni, e una descrizione
  che perdesse `lname` o rendesse l'indice non unico le cambierebbe entrambe
  restando verde.
* `provider.profile_functional_index` pretende che la DDL abbia dato **l'esito
  misurato**: accettata su MySQL, rifiutata con 1064 su MariaDB. Prima
  qualunque errore diventava "l'indice non c'e", e un catalogo senza indice
  passava per la conferma di un rifiuto mai avvenuto. Un esito diverso rende
  la sonda `not_measured`: senza la premessa, il catalogo non ha una forma
  attesa da confrontare.

**Il contratto dell'indice funzionale, che mancava.** La sonda su
`plenora_idx_expression` registrava cio che vedeva senza pretendere niente.
Ora l'esito della DDL decide cosa il catalogo deve dire: dove l'indice su
espressione si crea deve comparire **non** confrontabile per colonne — se
comparisse confrontabile, la regola che rifiuta un Upsert su un indice non
confrontabile non sarebbe mai raggiungibile — e dove la DDL viene rifiutata
quell'indice non deve esserci affatto. E l'ultima sonda del profilo che era
senza contratto: `OBSERVATION_ONLY_PROBES` oggi e vuoto.

### Cosa resta not_measured

* **la scrittura vera**: tutte e tre le forme sono state fermate in `prepare`,
  quindi nessuna riga e mai arrivata al server. Cosa succeda eseguendo un
  Upsert qualificato — e come si comporti sotto rollback e cancellazione — e
  il punto 3.
* **come MariaDB descriva un indice su colonna generata `PERSISTENT`**: qui e
  `VIRTUAL`, che e la forma piu comune e la sola misurata.

## Settima tranche: la prima write mode, Append

Il punto 3 procede una mode alla volta. `Append` e la piu semplice — nessun
DDL, nessuna keys — e proprio per questo e quella su cui si decide **come** si
misura una scrittura. Tre paletti, che valgono anche per le mode successive:

* la riuscita si verifica **rileggendo da un'altra sessione**, dopo il commit:
  cio che il provider dichiara di aver scritto e cio che la tabella contiene
  sono due affermazioni diverse, e la seconda si legge solo da fuori;
* il rollback pretende **due batch**, di cui il primo arrivato davvero al
  server: un errore di mapping o di preflight non proverebbe niente, perche
  non avrebbe mai scritto nulla da annullare;
* la cancellazione pretende una **barriera dichiarata**, non un timeout: il
  token si annulla quando il provider chiede il secondo batch, cioe con il
  primo gia sul server e la transazione ancora aperta. Un timeout cadrebbe
  ogni volta in un punto diverso, e una sonda che misura un punto diverso ogni
  volta non misura niente.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_write_append` | dichiarate=6, righe=6, contenuto verificato | **identico** | **identico** |
| provider | profilo | `provider.profile_write_append_rollback` | **rifiutato** — DataMapping/Write/RolledBack/Never, e righe=0 | **identico** | **identico** |
| provider | profilo | `provider.profile_write_append_cancellation` | **rifiutato** — Cancelled/Write/Unknown/RequiresRecovery, righe=0, ripresa righe=2 | **identico**, con il proprio nome | **identico**, con il proprio nome |

### Cosa dice

**Le righe arrivano, e si rileggono da fuori.** Due batch, sei righe, stesso
contenuto sui tre riferimenti — letto da una connessione che non ha visto la
transazione, dopo il commit. E l'esito pubblicato si verifica per intero:
`status`, il prodotto che ha scritto, ricevute, confermate, inserite, fallite,
saltate, e l'assenza di un recovery. Due campi su otto lascerebbero passare un
`Committed` che dichiara zero righe ricevute, o un esito attribuito all'altro
prodotto — che sono le cose su cui il chiamante costruisce la propria
contabilita.

**Il rollback annulla anche il primo batch.** La chiave duplicata del secondo
batch fa abortire la transazione, e la tabella resta vuota: `righe=0`. Il
codice 1062 non arriva pero come conflitto ma come **rifiuto di riga** —
`DataMapping`, effetto `RolledBack`, retry `Never` — perche e il piano di
scrittura a classificarlo, e la diagnostica di riga e la strada che quel
codice prende. La quaterna e stata misurata, non prevista: la prima stesura si
aspettava `Conflict` e la sonda ha detto che non era quello.

**La cancellazione dichiara l'effetto ignoto, ed e la risposta onesta.**
`Cancelled/Write/Unknown/RequiresRecovery`: da quel lato il provider non puo
sapere se il server avesse applicato, e dichiarare `RolledBack` sarebbe una
promessa che non e in grado di mantenere. Cosa sia successo davvero lo dice la
rilettura — `righe=0` — ed e per questo che la sonda la fa. `RequiresRecovery`
e la conseguenza per il chiamante: ripulire, non ritentare.

E il provider resta usabile: la scrittura successiva sullo stesso provider
scrive le sue due righe, e la tabella contiene quelle **e nient'altro** —
confrontata riga per riga, perche una sessione riusata con una transazione
residua committerebbe anche le righe di prima e il solo conteggio dichiarato
non lo direbbe.

Che la connessione sia stata chiusa e sostituita, invece, questa tranche non
lo **osserva**: lo dichiara il messaggio del provider, che e cosa afferma e non
cosa e successo. Osservarlo vorrebbe dire guardare l'identita della sessione in
`information_schema.processlist`, come fa il test live sulla quarantena del
pool MySQL. Finche non lo fa, la sonda dice quello che vede.

**Nessuna delle tre distingue i tre server.** L'unica differenza e il nome del
prodotto nel messaggio della cancellazione, che e attribuzione corretta e non
divergenza.

### Cosa ne segue per le capability

`writes.append` si apre: e la prima capability di scrittura di MariaDB, e la
sostengono le tre sonde di questa tranche.

Non e stato immediato, e la ragione vale la pena di restare scritta. La
bandiera non significava "Append": l'engine la consultava anche per
`TruncateInsert` — `validate_write_capability` mappava le due mode sullo
stesso flag — e `TruncateInsert` questo crate la rifiuta di proposito, perche
su MySQL e MariaDB `TRUNCATE` e DDL con commit implicito. Aprirla avrebbe
autorizzato una mode deliberatamente non qualificata.

Non era un difetto di questa tranche: c'era prima, e riguardava MySQL, che
pubblica `append = true` e poi rifiuta `TruncateInsert` in prepare. Il
contratto le ha separate — `writes.truncate_insert` e ora un campo suo,
`true` su PostgreSQL e SQL Server, `false` sui due motori dove `TRUNCATE`
commette da solo — e questo ha chiuso l'incoerenza per tutti.

Le tre sonde restano bloccanti in un inventario loro: il runner distingue le
prove che sostengono una capability **pubblicata** da quelle che
**qualificano** una superficie. Ora che `append` e aperta la distinzione conta
meno per queste tre, ma resta per le mode che verranno: ciascuna avra le
proprie prove prima della propria bandiera.

`rollback_on_failure` **e aperta**, e per un po' non lo era per un argomento
sbagliato. Il flag parla delle righe di qualunque scrittura che questo profilo
ammette, e ne ammette una: `Append`. Le tre sonde qui sopra girano con
`allow_partial: false` — il piano che la bandiera governa — e misurano l'esito
che promette: il secondo batch rifiutato annulla anche il primo, l'effetto
dichiarato e `RolledBack`, e la rilettura da un'altra sessione lo conferma. Il
residuo DDL non c'entra: lo descrive `transactional_ddl`, che resta `false`.

L'obiezione che avevo scritto — la cancellazione dichiara l'effetto remoto
`Unknown` — era fuori bersaglio: `Unknown` e l'esito di una **cancellazione**,
non di un fallimento, e su quel percorso nessun provider promette nulla.
PostgreSQL pubblica `rollback_on_failure = true` e ha lo stesso esito ignoto a
commit interrotto. Tenendola chiusa, MariaDB rifiutava in `prepare` proprio il
piano su cui queste prove sono state raccolte: l'unica mode aperta, con
l'unico `allow_partial` misurato.

### Cosa resta not_measured

* **le altre cinque write mode**: `Create`, `Update`, `Upsert`, `Replace`,
  `DeleteByKeys`. Ciascuna aggiunge una superficie che `Append` non ha — DDL
  da ripulire, keys da confrontare, una tabella che puo sopravvivere a un
  fallimento — e ciascuna arriva con le proprie tre sonde. La prima, `Create`,
  e l'ottava tranche.
* **il commit ambiguo**, che resta il punto 4 e non si deduce da qui: la
  cancellazione dichiara l'effetto ignoto **prima** del commit, che e una cosa
  diversa da un commit interrotto a meta.
* **`allow_partial`**: tutte le sonde di questa tranche usano il default, cioe
  il fallimento totale. Cosa succeda quando il chiamante accetta un esito
  parziale non e stato osservato.
* **la quarantena della connessione**, per la ragione detta sopra: serve
  l'identita della sessione, non il messaggio che la dichiara.

## Ottava tranche: la seconda write mode, Create

`Append` ha deciso **come** si misura una scrittura. `Create` aggiunge una
superficie sola, e da quella discende tutto il resto: il **DDL**. Su MySQL e
su MariaDB `CREATE TABLE` fa commit implicito, quindi la tabella che la mode
costruisce nella preparazione non appartiene alla transazione che segue, e
nessun `ROLLBACK` la annulla.

Ne segue che un fallimento qui non e il fallimento di un Append. Le righe
tornano indietro, lo schema no, e la differenza fra le due cose e la
differenza fra «il server e come prima» e «il server ha una tabella vuota in
piu». Le tre sonde verificano percio, ciascuna, anche **cosa e rimasto**: non
solo il conteggio delle righe, ma la forma della tabella letta dal catalogo.

Gira con lo stesso comando delle tranche precedenti:

    python scripts/check_mariadb_driver.py --markdown

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_write_create` | dichiarate=6, righe=6, `colonne=id/NO,payload/NO pk=id`, `tipi=id:int,payload:text` | **identico**, salvo `tipi=id:int(11)` | **identico**, salvo `tipi=id:int(11)` |
| provider | profilo | `provider.profile_write_create_rollback` | **rifiutato** — Conflict/Write/**Partial**/RequiresRecovery, righe=0, tabella rimasta | **identico**, con il proprio nome | **identico**, con il proprio nome |
| provider | profilo | `provider.profile_write_create_cancellation` | **rifiutato** — Cancelled/Write/Unknown/RequiresRecovery, righe=0, tabella rimasta, ripresa righe=2 | **identico**, con il proprio nome | **identico**, con il proprio nome |

### Cosa dice

**La tabella che nasce e la stessa sui tre server.** Nomi, ordine,
nullability e chiave primaria coincidono — `colonne=id/NO,payload/NO pk=id` —
e le sei righe si rileggono da un'altra connessione dopo il commit, con lo
stesso contenuto. La sonda guarda la forma **e** il contenuto, non uno dei
due: una `Create` che scrivesse le righe giuste in una tabella sbagliata
sarebbe verde su un conteggio.

**I tipi nativi divergono, e non e una novita.** Dalla stessa `CREATE TABLE`
il catalogo rende `id:int` su MySQL e `id:int(11)` su MariaDB: e la larghezza
di visualizzazione che MySQL ha tolto dalla 8.0.19 e MariaDB pubblica ancora,
gia registrata dalla quinta tranche su `bigint(20)`. Sta nel dettaglio della
sonda ma non nel suo contratto: farne una condizione renderebbe rossa una
prova per una differenza gia misurata e capita, toglierla dal dettaglio la
nasconderebbe.

**Il rollback annulla le righe e lascia la tabella, e il provider lo
dichiara.** `righe=0` letto da un'altra connessione, la tabella ancora li con
la sua forma, e l'effetto remoto **`Partial`** invece di `RolledBack`, con
`RequiresRecovery`. E' la risposta corretta: dichiarare `RolledBack` direbbe
al chiamante che il server e come prima mentre una tabella vuota e rimasta, e
il messaggio lo scrive per esteso — «la tabella creata da mode='create' e
rimasta: il DDL fa commit implicito e non e annullato dal rollback». La sonda
non si fida del messaggio: rilegge il catalogo e verifica che quella tabella
ci sia davvero, perche un messaggio e cosa il provider afferma, non cosa e
successo.

**Lo stesso duplicato arriva in due categorie diverse a seconda della mode.**
Nell'Append il 1062 e un rifiuto di riga — `DataMapping`, «riga sorgente
rifiutata» — e qui e un `Conflict`, «vincolo univoco violato (codice 1062)».
La quaterna e stata misurata, non prevista: la prima stesura di questa sonda
si aspettava la categoria della settima tranche, e la misura ha detto di no.

La ragione non e del motore — i tre server si comportano identici — ma di
questo crate, ed e scritta nel punto in cui si decide: la diagnostica per riga
si attiva **solo** per `Append`, perche per le altre mode il conteggio
per-riga non regge (l'Upsert MySQL rende `affected_rows=2` per un UPDATE) e il
bulk INSERT e preferibile. Fuori dall'Append il codice server arriva al
chiamante come verdetto del profilo.

Registrata, non risolta: un consumer che ramifica sulla categoria vede due
cose diverse per la stessa causa, e decidere quale delle due sia quella giusta
non e una domanda su MariaDB.

**La cancellazione dichiara l'effetto ignoto, e il residuo non lo declassa.**
`Cancelled/Write/Unknown/RequiresRecovery`: `Unknown` resta `Unknown` e non
diventa `Partial`, perche non sapere se le righe siano state applicate e piu
grave che sapere che lo schema e rimasto — il provider non declassa la prima
incertezza per annunciare la seconda. La rilettura dice cosa e successo
davvero: `righe=0`, tabella presente.

E il provider resta usabile. La ripresa qui non puo essere un secondo
`Create` — la tabella c'e, e la mode fallirebbe per una ragione che non
riguarda la quarantena della sessione: si toglie di mezzo dall'altra
connessione, **come farebbe chi recupera**, e poi si rifa. Le due righe della
seconda scrittura sono le uniche che la tabella contiene, confrontate una per
una: una sessione riusata con una transazione residua committerebbe anche le
righe di prima, e il conteggio dichiarato non lo direbbe.

**Nessuna delle tre distingue i tre server.** Le uniche differenze sono il
nome del prodotto nei messaggi, che e attribuzione corretta, e la larghezza di
visualizzazione dei tipi.

### Cosa ne segue per le capability

`writes.create` si apre per MariaDB: e la seconda capability di scrittura, e
la sostengono le tre sonde di questa tranche, bloccanti nella campagna come
quelle dell'Append.

`transactional_ddl` resta `false`, e questa tranche e la misura che lo
sostiene invece di lasciarlo dichiarato: la tabella sopravvive al rollback su
tutti e tre i server, che e esattamente cio che quel flag nega.

`rollback_on_failure` resta `true` e non cambia significato. Il flag parla
delle **righe**, e le righe tornano indietro anche qui — `righe=0` dopo un
fallimento a meta. Il residuo dello schema e cio che `transactional_ddl`
descrive, e le due bandiere dicono due cose che questa tranche ha visto
insieme senza confonderle.

### Cosa resta not_measured

* **`Create` su un target che esiste gia**. Il piano non lo incontra qui,
  perche ogni sonda parte da una tabella assente, e non e una lacuna di
  MariaDB: il provider MySQL qualificato pubblica `create = true` con la
  stessa superficie non misurata.
* ~~**le altre quattro write mode**~~ **Chiuso**: `Update`, `Upsert`,
  `Replace` e `DeleteByKeys` sono la nona tranche, qui sotto.
  `TruncateInsert` resta fuori per decisione, non per assenza di misura.
* **`allow_partial`**, come nella settima tranche: tutte le sonde usano il
  default, cioe il fallimento totale.
* **il commit ambiguo**, che resta il punto 4.

## Nona tranche: le ultime quattro write mode

`Append` ha deciso come si misura una scrittura, `Create` cosa succede quando
il DDL non torna indietro. Restavano `Update`, `Upsert`, `Replace` e
`DeleteByKeys`, e arrivano insieme: dodici sonde, tre per mode, con le proprie
tabelle, la propria contabilita attesa e la propria domanda. Insieme perche
condividono l'unica cosa che le lega — il modo di provocare un rifiuto del
**server**, che e cio che una sonda di rollback deve fare: un errore di
mapping o di preflight non proverebbe niente, perche non avrebbe mai scritto
nulla da annullare.

Gira con lo stesso comando delle tranche precedenti:

    python scripts/check_mariadb_driver.py --markdown

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_write_update` | aggiornate=5, saltate=1, contenuto atteso | **identico** | **identico** |
| provider | profilo | `provider.profile_write_update_rollback` | **rifiutato** — Conflict/Write/**RolledBack**/Never (1062), valori di prima tornati | **identico** | **identico** |
| provider | profilo | `provider.profile_write_update_cancellation` | **rifiutato** — Cancelled/Write/Unknown/RequiresRecovery, valori di prima, ripresa 2 | **identico**, col proprio nome | **identico**, col proprio nome |
| provider | profilo | `provider.profile_write_upsert` | confermate=6, `inserite=None`, `aggiornate=None` | **identico** | **identico** |
| provider | profilo | `provider.profile_write_upsert_rollback` | **rifiutato** — Execution/Write/RolledBack/Never (1406), righe di prima tornate | **identico** | **identico** |
| provider | profilo | `provider.profile_write_upsert_cancellation` | **rifiutato** — Cancelled/…/RequiresRecovery, nulla applicato, ripresa 1 | **identico**, col proprio nome | **identico**, col proprio nome |
| provider | profilo | `provider.profile_write_replace` | inserite=2, `cancellate=Some(0)`, target sostituito | **identico** | **identico** |
| provider | profilo | `provider.profile_write_replace_rollback` | **rifiutato** — Execution/Write/RolledBack/Never (1406), **target non vuoto** | **identico** | **identico** |
| provider | profilo | `provider.profile_write_replace_cancellation` | **rifiutato** — Cancelled/…/RequiresRecovery, target intatto, ripresa 1 | **identico**, col proprio nome | **identico**, col proprio nome |
| provider | profilo | `provider.profile_write_delete_by_keys` | cancellate=2, saltate=1 | **identico** | **identico** |
| provider | profilo | `provider.profile_write_delete_by_keys_rollback` | **rifiutato** — batch intero tornato indietro | **identico** | **identico** |
| provider | profilo | `provider.profile_write_delete_by_keys_cancellation` | **rifiutato** — nessuna riga tolta, ripresa 1 | **identico**, col proprio nome | **identico**, col proprio nome |

### Cosa dice

**Il rollback di `Update` e pieno, e la differenza da `Create` e misurata.**
Stesso vincolo violato, stessa mode di fallimento, ma qui l'effetto remoto e
`RolledBack` e non `Partial`: `Update` accumula le righe in una `CREATE
TEMPORARY TABLE` di staging, e su questi motori una temporary non provoca
commit implicito. Il residuo di `Create` non c'e, e il contratto lo dice
invece di lasciarlo dedurre dalla mode.

**Una chiave che non trova riscontro e saltata, non fallita.** Sei righe in
ingresso, cinque aggiornate e una — l'id che nella tabella non c'e — contata
in `skipped`. E la promessa di idempotenza, e la sonda la verifica su tre
fronti: la contabilita la distingue, il contenuto mostra che le altre cinque
sono cambiate, e la tabella ha ancora sei righe — la chiave assente non e
stata inserita. `DeleteByKeys` fa la stessa cosa dall'altro lato: due
cancellate, una saltata.

**`Upsert` dichiara di non sapere, e fa bene.** `inserted` e `updated`
arrivano a `None` — «non pertinente» — perche su questi motori
`affected_rows` vale 1 per un inserimento e 2 per un aggiornamento, e il
totale non si scompone senza una seconda interrogazione. La sonda pretende
quel `None`: un numero inventato sarebbe peggio di un'assenza dichiarata, e
`Some(0)` avrebbe l'aria di una misura.

**Il rollback di `Replace` e la prova piu importante della fase.** `Replace`
svuota il target e lo riempie nella stessa transazione, quindi quando la
scrittura fallisce il `DELETE` e gia passato. Se non tornasse indietro, un
`Replace` fallito lascerebbe il target **vuoto**: distruggerebbe i dati che
doveva sostituire. Le tre righe di partenza sono tutte li, su tutti e tre i
riferimenti, e lo stesso vale sotto cancellazione.

**`Replace` pubblica pero `deleted = 0`** avendo svuotato il target. Non e un
difetto del motore ne una divergenza: e la forma che il contratto dichiara per
quella mode — le righe finali sono esattamente quelle in ingresso, e il
`DELETE` e considerato parte della sostituzione e non una cancellazione a se.
Registrata perche un consumatore che sommasse `deleted` fra le mode
conterebbe zero dove il target e stato svuotato.

**`DeleteByKeys` vuole uno schema di sole chiavi**, e l'abbiamo scoperto
venendo rifiutati: «colonna 'payload' non e una key — schema Arrow deve
contenere solo le colonne key». Il rifiuto e giusto — una cancellazione non ha
nulla da fare dei valori — ma le prime tre sonde ponevano una domanda diversa
da quella che credevano, e sono state riscritte con lo schema che la mode
chiede.

**Il valore fuori misura arriva come errore generico.** 1406, «Data too long»,
non e in nessuna tabella di classificazione: il profilo gli attribuisce il
verdetto dei codici non qualificati — `Execution`/`Never`, messaggio redatto —
identico sui tre server. Non e sbagliato: l'operazione e fallita sul server e
ritentarla non ha ragione di riuscire. E pero la stessa forma della lacuna che
la quarta tranche ha chiuso su 1142, dove un permesso mancante si presentava
come guasto generico: un dato troppo lungo lo corregge chi chiama, un guasto
no, e sono due rimedi diversi. Registrata qui e chiusa a parte, perche tocca
anche il provider MySQL qualificato.

**Nessuna delle dodici distingue i tre server.** Le uniche differenze sono i
nomi dei prodotti nei messaggi.

### Cosa ne segue per le capability

Si aprono `writes.update`, `writes.upsert`, `writes.replace` e
`writes.delete_by_keys`. Con `append` e `create` sono sei mode su sette: resta
chiusa `truncate_insert`, e non per assenza di misura — su questi motori
`TRUNCATE` e DDL con commit implicito, quindi le righe sparirebbero prima
dell'`INSERT` e nessun rollback le riporterebbe indietro. E' la stessa
chiusura permanente di MySQL.

`writes.bulk` **non** si apre, e la ragione merita di essere scritta: nessun
codice la consulta. `validate_write_capability` mappa sette mode su sette
bandiere, e `bulk` non e fra quelle; nessun altro punto del workspace la
legge. Aprirla significherebbe pubblicare una promessa che nessuna misura
sostiene e che nessun controllo fa rispettare — cioe l'opposto della regola 1.
Sta insieme a `array_binding`, `returning`, `server_cursor`, `pagination` e
`resumable`, che sono nella stessa condizione.

### Cosa resta not_measured

* **`allow_partial`**, come nelle due tranche precedenti: tutte le sonde
  usano il default, cioe il fallimento totale.
* **il commit ambiguo**, che resta il punto 4.
* **la quarantena della connessione**, che si osserva solo dall'identita
  della sessione in `information_schema.processlist`.
