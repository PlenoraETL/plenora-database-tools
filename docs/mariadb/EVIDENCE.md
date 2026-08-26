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
| raw | scrittura | `raw.returning_forms` | insert=1064 replace=1064 update=1064 delete=1064 upsert_insert=1064 upsert_update=1064 | insert=`[1]` replace=`[2]` update=1064 delete=`[2]` upsert_insert=`[3]` upsert_update=`[3]` | **identico a MariaDB 11** |
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

~~`provider.ambiguous_commit`~~ **Chiuso dall'undicesima tranche.** La ragione
scritta qui — che un commit di esito ignoto si osserva solo con fault
injection deterministica, e che uccidere la connessione a meta `COMMIT` da una
seconda sessione e una corsa e non un esperimento — era giusta, e escludeva
**quel** metodo, non la misura. La forma deterministica esisteva gia nel
provider SQL Server di questo repository, e vale identica qui. Il seguito e in
fondo al documento.

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
| provider | profilo | `provider.profile_portable_returning` | **rifiutato** — `Unsupported`: RETURNING non esiste su MySQL | righe=1 valori=`I64(1)` | righe=1 valori=`I64(1)` |

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
| provider | profilo | `provider.transaction_row_stream` | batch=[4096, 4096, 1] commit=Committed | **identico** | **identico** |
| provider | profilo | `provider.transaction_row_stream_abandoned` | commit=Committed — righe=1 | **identico** | **identico** |

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

**Lo stream in transazione e una seconda superficie sotto la stessa
bandiera.** `provider.profile_read_streaming` misura il percorso Arrow — un
lettore che riceve piu di un batch. `TransactionScope::query_stream` fa un'altra
cosa: apre un result set sul filo e lo fa scorrere mentre la transazione e
aperta. L'implementazione e condivisa con MySQL, e per questo la misura non lo
e: «condivide il codice» non e un argomento che questo documento accetta per
nessun'altra bandiera, e non c'e ragione di accettarlo qui. I batch si contano
**uno per uno** — `[4096, 4096, 1]` sui tre server — perche un totale giusto
uscirebbe anche da uno stream che consegna tutto in un colpo.

**Una sonda che smentisce una dichiarazione, invece di sostenerla.** La prima
stesura di `query_stream` affermava che abbandonare un result set a meta lascia i
pacchetti in coda e rende la connessione inservibile: c'era una bandiera di
stato, e la transazione rifiutava con `RequiresRecovery` ogni operazione
successiva. Il riferimento MySQL ha risposto `Committed`, perche `mysql_async`
drena i pacchetti pendenti prima dello statement dopo. La regola 1 non distingue
fra il dedurre una capability e il dedurre un guasto — nessuna delle due si
dichiara senza misura, e questa era dedotta da come funziona il protocollo sul
filo, che e vero e irrilevante quando in mezzo c'e un driver che se ne occupa.

`provider.transaction_row_stream_abandoned` tiene onesta la ritrattazione su
questo prodotto: dopo un batch su mille lo stream viene lasciato andare, la
transazione scrive, committa, e la riga si rilegge **da un'altra connessione** —
che il commit dica `Committed` e cio che il provider crede, la riga sul server e
cio che e successo. `commit=Committed — righe=1` sui tre riferimenti, nessuna
divergenza. Senza questa sonda, una divergenza di MariaDB su questo punto si
scoprirebbe quando un chiamante esce da un ciclo con un `break` in produzione.

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
| provider | profilo | `provider.profile_write_upsert_rollback` | **rifiutato** — DataMapping/Write/RolledBack/Never (1406), righe di prima tornate | **identico** | **identico** |
| provider | profilo | `provider.profile_write_upsert_cancellation` | **rifiutato** — Cancelled/…/RequiresRecovery, nulla applicato, ripresa 1 | **identico**, col proprio nome | **identico**, col proprio nome |
| provider | profilo | `provider.profile_write_replace` | inserite=2, `cancellate=Some(0)`, target sostituito | **identico** | **identico** |
| provider | profilo | `provider.profile_write_replace_rollback` | **rifiutato** — DataMapping/Write/RolledBack/Never (1406), **target non vuoto** | **identico** | **identico** |
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

~~**Il valore fuori misura arriva come errore generico.**~~ **Chiuso.** 1406,
«Data too long», e 1451, la cancellazione trattenuta da un vincolo
referenziale, non erano in nessuna tabella di classificazione: il profilo gli
attribuiva il verdetto dei codici non qualificati — `Execution`/`Never`,
messaggio redatto — identico sui tre server. Non era sbagliato, ma era la
stessa forma della lacuna che la quarta tranche ha chiuso su 1142: un dato
troppo lungo lo corregge chi chiama, un guasto no, e sono due rimedi diversi.

La classificazione condivisa conosce ora quattro codici in piu, e ciascuno
arriva identico dai tre riferimenti: **1048** (colonna non nullable senza
valore) e **1406** (valore oltre la larghezza) come `DataMapping`, **1451** e
**1452** — i due lati dello stesso vincolo referenziale — come `Conflict`. La
seconda scelta ha la stessa ragione di 1062: non e la riga a essere
malformata, e lo stato del database a non ammetterla.

Fuori dall'`Append` la diagnostica per riga non si attiva, quindi era proprio
in queste cinque mode che quei codici perdevano il proprio nome. Il cambio
tocca anche il provider MySQL qualificato, ed e in un commit suo — le sonde di
rollback di questa tranche verificano ora le due quaterne al posto di quella
generica.

E la campagna ha aggiunto la seconda meta della correzione, che la sola
tabella non conteneva. Su `MariaDB` un codice eredita la classificazione
condivisa **solo** se e in `MEASURED_SERVER_CODES`, cioe se e stato osservato
su quel motore: la prima stesura aggiungeva i quattro codici alla tabella e non
all'inventario, quindi MySQL cambiava e MariaDB restava generica. La guardia ha
fatto esattamente cio per cui esiste — non ereditare una promessa a nome di un
motore che nessuno ha interrogato — e la misura c'era: 1048 e 1452 dalla quarta
tranche, 1406 e 1451 da questa. Due dei quattro erano misurati da mesi senza
essere in elenco, perche fino ad allora non c'era niente da ereditare.

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

## Decima tranche: l'SRID di colonna, cercato dove poteva stare

Le tranche precedenti avevano chiuso lo spatial di `MariaDB` con una frase che
sembrava definitiva: `information_schema.columns.SRS_ID` non esiste, errore
1054, quindi non c'e modo di sapere se una geometry abbia un sistema di
riferimento. La frase era vera e la conclusione sbagliata, perche l'assenza
era stata osservata **da una parte sola**.

Questa tranche guarda nelle altre due, e corregge il documento.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | spatial | `raw.geometry_columns_registry` | **no** `GEOMETRY_COLUMNS` assente, `ST_GEOMETRY_COLUMNS.SRS_ID`=NULL | `GEOMETRY_COLUMNS.SRID`=0 | `GEOMETRY_COLUMNS.SRID`=0 |
| raw | spatial | `raw.declared_column_srid` | `SRID`=accettato, registro rende 4326; `REF_SYSTEM_ID`=rifiutato 1064 | **no** entrambe rifiutate 1064 | **no** entrambe rifiutate 1064 |

### Cosa dice

**Il registro OGC su `MariaDB` c'e, ed espone un SRID di colonna.**
`information_schema.GEOMETRY_COLUMNS` esiste su entrambi i riferimenti e porta
una colonna `SRID`. Su `MySQL` quella tabella non esiste affatto — la 8.0
l'ha sostituita con `ST_GEOMETRY_COLUMNS` — quindi i due prodotti hanno **due
registri diversi**, non uno solo che a uno dei due manca. E' una divergenza di
prodotto come le altre, e appartiene al profilo.

**Ma nessuna colonna puo essere vincolata a un SRID.** `SRID 4326` nella DDL e
rifiutato da `MariaDB` con 1064, come la prima tranche aveva gia misurato; e
lo e anche `REF_SYSTEM_ID=4326`, che e l'attributo che quel prodotto
documenta al posto suo. Due sintassi, due rifiuti, su entrambe le versioni. Su
`MySQL` la prima e accettata e il registro rende 4326, la seconda no.

Ne segue che `GEOMETRY_COLUMNS.SRID` su `MariaDB` vale **sempre zero** —
l'«indefinito» OGC — perche non esiste una DDL che lo faccia diventare altro.

**La ragione della chiusura cambia, e diventa piu stretta.** Non «il catalogo
non risponde»: risponde, e dice zero. Non «l'SRID e sconosciuto»: e
**assente**, perche il motore non permette di dichiararlo. Lo spatial di
`MariaDB` resta chiuso, e ora si sa esattamente cosa servirebbe per aprirlo —
non una query di catalogo diversa, ma un CRS **dichiarato dal chiamante** e
verificato valore per valore, che e la forma che il path di scrittura ha gia
con `srid_policy`.

### Come la sonda ha quasi registrato il contrario

La prima stesura interrogava `WHERE TABLE_NAME`, e ha preso 1054 da entrambe
le `MariaDB`. Stava per chiudere il capitolo con «il registro non ha un SRID»,
e sarebbe stato falso in modo credibile: due server, tre rifiuti coerenti, un
codice d'errore che sembrava confermare.

A smentirlo e stata la riga aggiunta **per completezza** e non per dubbio —
quella che chiede al catalogo la forma del registro. La forma diceva
`[..., MAX_PPR, SRID]`: la colonna c'era. Il 1054 riguardava il predicato,
perche nel registro OGC il nome della tabella sta in `F_TABLE_NAME`.

Una sonda che chiede la cosa sbagliata non rende «nessuna risposta»: rende
**una risposta a un'altra domanda**, e le due si leggono identiche. Il
predicato si ricava ora dalla forma del registro invece di essere immaginato,
e per questo la sonda prova entrambi i nomi di ciascuna generazione OGC.

### Cosa resta not_measured

* **il comportamento sotto un `SRID` di riga eterogeneo**: se una colonna
  senza vincolo contenga valori con SRID diversi, e cosa il provider dovrebbe
  pubblicare come CRS in quel caso. E' la domanda che una strategia dichiarata
  dal chiamante dovrebbe risolvere, e non si pone finche quella strategia non
  esiste.
* **le versioni di `MariaDB` oltre la 12.3**: il rifiuto e misurato su 11.8 e
  12.3, e come tutte le righe di questo documento vale per cio che e stato
  acceso.

## Undicesima tranche: il commit che e atterrato e non l'ha detto

Era l'ultima superficie `not_measured` di questo documento, e ci e rimasta a
lungo per una ragione che vale la pena rileggere: «un esito ottenuto cosi non
distingue il comportamento del provider dal momento in cui e arrivato il
colpo». Vero — di quel metodo. Uccidere la connessione a meta `COMMIT` da una
seconda sessione e una corsa, e da una corsa non esce un contratto.

La forma deterministica era in casa da tempo, in un provider che nessuno
stava guardando: `SqlServerSession::commit_with_delayed_response` esegue
`COMMIT TRANSACTION; WAITFOR DELAY`, cioe fa **atterrare** il commit e poi
trattiene la risposta. La finestra in cui cancellare e larga, ripetibile e
sempre nello stesso punto. Su questi due motori l'equivalente e
`COMMIT; DO SLEEP(5)`.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | commit | `provider.ambiguous_commit` | `OutcomeUnknown`, fase certa `commit_requested`, e la riga **c'e** | **identico** | **identico** |

### Cosa dice

**`OutcomeUnknown` e onesto, e questa e la parte che conta.** Che il provider
dichiari un esito ignoto e meta della prova; l'altra meta e che quella
dichiarazione corrisponda a cio che il server ha fatto. `Unknown` non vuol
dire «non e successo niente»: vuol dire «non lo so», e le due si distinguono
solo rileggendo da un'altra connessione. La riga inserita nella transazione
**c'e**, su tutti e tre i riferimenti: il commit era andato a buon fine, e il
provider non poteva saperlo.

E' il caso per cui `OutcomeUnknown` esiste, ed e il piu pericoloso da
sbagliare in entrambe le direzioni. Un `Committed` qui direbbe a
un'automazione che la mutazione e confermata quando nessuno l'ha vista
tornare; un `RolledBack` autorizzerebbe un retry che **raddoppia** la riga.
La sonda rifiuta entrambi, e non per costruzione: se il commit non fosse
atterrato registrerebbe `not_measured`, perche avrebbe misurato un altro caso
e chiamarlo con questo nome direbbe una cosa per un'altra.

**Il percorso attraversato e quello di produzione.** L'interruttore cambia il
testo dello statement, non la logica che ne classifica l'esito: la
cancellazione, la quarantena della sessione e la mappatura verso
`OutcomeUnknown` sono le stesse righe che un consumatore incontra. E' un
`#[cfg(test)]` con guardia che si spegne al `Drop` — non una feature, non una
variabile d'ambiente — per la stessa ragione del bypass del rifiuto: un
interruttore che si accende e basta lascerebbe ogni commit successivo del
binario in attesa di cinque secondi.

**I tre server non si distinguono.** Come per tutta la macchina a stati della
sessione, che resta la parte del provider su cui MySQL e MariaDB non hanno mai
divergiuto.

### Cosa resta not_measured

Niente, in questo documento. Le superfici che il provider `MySQL` puntato su
`MariaDB` non raggiunge — `provider.read` e `provider.read_geometry` —
restano quello che sono per costruzione: dipendono da un catalogo che quel
percorso non sa leggere, e il percorso qualificato passa dal profilo, dove
sono misurate.

## Dodicesima tranche: `RETURNING`, e un intero strato che non si poteva raggiungere

Questa tranche e cominciata da un commento. `compile_returning` rifiutava
`RETURNING` su tutto il dialetto `Mysql` con questa motivazione: «MySQL non ha
RETURNING universale (solo 8.0.20+ per INSERT)». Poche funzioni piu in la, il
facade ne dava un'altra: «MySQL 8.0.31+ supporta INSERT ... RETURNING per
singola riga».

Due numeri di versione diversi per la stessa funzionalita, ed e questo il
dettaglio da cui vale la pena partire: se venissero entrambi da una misura,
sarebbero lo stesso numero. Nessuno dei due ci veniva. `MySQL` non ha
`RETURNING` — a nessuna versione, in nessuna forma.

A rendere plausibile la confusione e che `MariaDB` ce l'ha. E i due prodotti
condividevano un solo `DialectKind`.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | scrittura | `raw.returning_forms` | 1064 su tutte e sei | `insert=[1] replace=[2] update=1064 delete=[2] upsert_insert=[3] upsert_update=[3]` | **identico a MariaDB 12** |
| provider | profilo | `provider.profile_portable_returning` | **rifiutato** — `Unsupported` | righe=1 valori `I64(1)` | **identico** |

### Cosa dice

**`MariaDB` ammette `RETURNING` ovunque tranne che su `UPDATE`.** `INSERT`,
`REPLACE`, `DELETE` e **entrambi** i rami dell'upsert rendono le righe; l'
`UPDATE` risponde `1064`, errore di sintassi. Le due major danno la stessa riga
di esiti, senza differenze.

L'upsert e il caso in cui la misura serviva di piu: la documentazione di
`MariaDB` lascia intendere che `RETURNING` non sopravviva a `ON DUPLICATE KEY
UPDATE`, e il server lo esegue — su tutti e due i rami, quello che inserisce e
quello che aggiorna. Questo file segue il server.

**La sonda legge le righe, non l'assenza di errore.** La prima stesura usava
`query_drop`: se il parser non rifiutava la sintassi, la forma risultava
disponibile. E' meta della domanda. `RETURNING` esiste per i valori che
consegna, e una forma accettata che non consegnasse niente sarebbe una
capability aperta su una promessa vuota — e sarebbe stato proprio sull'upsert,
dove il server smentisce la documentazione, che fermarsi all'accettazione
avrebbe pesato di piu.

**Due sonde, due domande che sembrano una.** `raw.returning_forms` interroga il
**server** con lo statement scritto a mano. `provider.profile_portable_returning`
interroga il **percorso**: `execute_portable_returning` compila per
`tx.provider_kind()` ed esegue, quindi attraversa la tabella dialetto-forma e il
decoder delle righe.

La differenza non e teorica, e si vedeva benissimo il giorno in cui sono state
scritte: il server `MariaDB` accettava `INSERT ... RETURNING` mentre
`compile_portable` rifiutava `ProviderKind::Mariadb` **per intero**. Ogni
`execute_portable` e ogni `query_portable` su `MariaDB` fallivano in prepare con
«compile_portable non supportato per Mariadb»: l'intero strato facade era
irraggiungibile su un provider che questo repository pubblica. Una sonda sul
solo server avrebbe registrato «disponibile» di una superficie che nessun
chiamante poteva toccare.

Quel rifiuto non era una decisione. Era il ramo di scarto di un `match` scritto
quando il provider `MariaDB` non esisteva, e il messaggio diceva «non
supportato» di una cosa che nessuno aveva deciso di non supportare.

### Cosa ne segue per il codice

`DialectKind::Mariadb` e una variante, non una bandiera dentro `Mysql`. Oggi la
divergenza e una sola, e una bandiera sarebbe bastata a scrivere il codice — non
a leggerlo: la seconda divergenza si presenterebbe come una seconda bandiera
dentro un dialetto che nel nome dichiara di essere di qualcun altro. Dove i due
prodotti si somigliano — segnaposto `?`, quoting a backtick, `ON DUPLICATE KEY
UPDATE`, `INSERT IGNORE`, spatial via `ST_GeomFromWKB` — l'arm resta condiviso,
e ogni riga condivisa e una riga in cui si somigliano davvero.

`compile_returning` riceve la **forma**, non solo il dialetto. Su `MariaDB`
`RETURNING` non e una proprieta del prodotto: e una proprieta della coppia
prodotto-forma, e il solo dialetto avrebbe costretto a scegliere fra aprire
l'`UPDATE`, che fallisce sul server, e chiudere le altre tre, che funzionano.

### Perche le due sonde sono osservative

Il loro esito atteso **diverge per prodotto**: su `MySQL` il rifiuto e la misura
giusta, su `MariaDB` lo sono le righe. Gli inventari del runner esprimono un
esito solo, uguale per tutti i riferimenti — chiedere `accepted` renderebbe rossa
la matrice su `MySQL`, chiedere `rejected` su `MariaDB`, e ciascuna direbbe il
falso su meta di essa.

L'elenco delle osservative esisteva vuoto, con una nota che diceva di tenerlo
per la prossima sonda senza contratto. Sono le prime due ad abitarlo, e la
seconda ha allargato il perimetro anche alla famiglia `raw`: «questa sonda non
sostiene niente, e lo dice apposta» non e una proprieta della famiglia.

### Cosa resta not_measured

Niente di nuovo. `writes.returning` resta `false` e **descrittiva**, e non per
mancanza di misura: parla di un'altra superficie ancora — l'esito del percorso
di piano, che conta righe e non le trasporta. Aprirla vorrebbe dire far
trasportare righe a un documento di esito, che e la forma sbagliata per una
scrittura bulk; il `RETURNING` che questa tranche apre vive nel percorso
portable, dove le righe hanno gia dove stare.

## Tredicesima tranche: lo spatial si apre, e con la condizione accanto

La decima tranche aveva chiuso lo spatial di `MariaDB` con una ragione precisa e
una ricetta: «non una query di catalogo diversa, ma un CRS **dichiarato dal
chiamante** e verificato valore per valore, che e la forma che il path di
scrittura ha gia con `srid_policy`». Questa tranche misura quella forma, e apre
la capability.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_crs_undeclared` | **rifiutato** — `Crs`: il catalogo tace e il piano non lo dichiara | **identico** | **identico** |
| provider | profilo | `provider.profile_crs_declared` | righe=2 batch=1 | **identico** | **identico** |
| provider | profilo | `provider.profile_crs_mismatched` | **rifiutato** — `Crs`: dichiarata SRID 3003, la riga 0 porta SRID 4326 | **identico** | **identico** |

### Cosa dice

**Tre domande, e la terza e quella che rende vere le altre due.** Senza
dichiarazione la colonna resta rifiutata — la dichiarazione apre una porta, non
ne toglie una chiusa. Con la dichiarazione giusta le righe arrivano. Con una
dichiarazione che i valori smentiscono, la lettura fallisce **alla riga che la
smentisce**, e nomina i due SRID.

Senza la terza, la seconda non proverebbe niente: una dichiarazione creduta
sulla parola darebbe lo stesso esito verde, e `geometry: true` significherebbe
«il provider ripete cio che il chiamante gli ha detto». La verifica sta nel ciclo
delle righe e non nel prepare, e non e pedanteria: la colonna che richiede una
dichiarazione e proprio quella che nessuna DDL vincola, quindi due righe della
stessa colonna possono portare SRID diversi. E' il caso lasciato `not_measured`
dalla decima tranche, e ora e misurato — non come domanda separata, ma come la
forma stessa del controllo.

**`MySQL` si comporta identico, e non era la risposta attesa.** La tranche era
nata come una divergenza di `MariaDB`: li `SRS_ID` non esiste e nessuna DDL puo
vincolare una geometry, quindi sembrava che il problema fosse suo. La sonda gira
su una colonna `GEOMETRY` che la DDL **non** vincola — l'unica forma che
`MariaDB` ammette — e su `MySQL` quella stessa colonna ha `SRS_ID` nullo: e
rifiutata senza dichiarazione, letta con una, e fallisce sui valori quando la
dichiarazione e smentita. Tre esiti su tre, identici sui tre riferimenti.

Il fatto non e «`MariaDB` ha un problema che `MySQL` non ha». E' che su
`MariaDB` la colonna non vincolata e l'unica possibile, mentre su `MySQL` e una
possibilita che qualcuno puo scegliere — e in quel caso i due prodotti si
comportano allo stesso modo.

### Cosa ne segue per le capability

`MariaDB` apre `SpatialCapabilities::read_wkb` e `SpatialCapabilities::geometry`,
con `dimensions: [xy]` — che e cio che le sonde hanno attraversato — e con
`SpatialCapabilities::requires_declared_crs` a `true` accanto.

I nomi qui sono scritti per intero e non come `spatial.<campo>` per una ragione
che vale la pena dire: `spatial` e gia il prefisso di una **superficie di
sonde** in questo documento — `spatial.srid_column`, `spatial.geometrycollection`
— e una guardia del self-test controlla che ogni identificatore con quel
prefisso corrisponda a una sonda esistente. Due nomi con la stessa forma e due
significati diversi sono esattamente cio che quella guardia esiste per non
lasciar passare.

Le due bandiere vanno lette **insieme**, ed e la ragione per cui la seconda
esiste. `geometry` da sola non sa dire la verita su questo prodotto: `false`
negherebbe una lettura che funziona, `true` prometterebbe che una lettura
semplice basti. `geometry: true, requires_declared_crs: true` dice la cosa
giusta, e un chiamante che ignora la seconda riceve un rifiuto in prepare invece
di un CRS inventato.

`MySQL` pubblica anche lui `requires_declared_crs` a `true`, per la stessa misura.
La bandiera dice «leggere una geometria **puo** richiedere un CRS dichiarato»,
non «lo richiede sempre».

Restano chiuse `geography` — che su questo prodotto non esiste, e non e una
lacuna di misura — `spatial_index`, `mixed_geometry_types`, che nessuna sonda ha
letto, e la lista `functions`, che non si eredita da `MySQL`.

### Perche le tre sonde sono osservative

Per la stessa ragione delle due del `RETURNING`, e la campagna lo registra come
divergenza su due delle tre: gli inventari del runner esprimono un esito solo
per tutti i riferimenti, e qui l'esito atteso e per meta un rifiuto — che nella
matrice si legge `rejected` — e per meta una lettura. Non c'e un unico valore che
li descriva tutti e tre senza dire il falso su qualcuno.

La divergenza che la campagna segnala e nel **testo**, non nel comportamento: il
messaggio nomina il prodotto che ha rifiutato, quindi `MySQL` e `MariaDB` non
possono rendere la stessa stringa. Gli esiti coincidono.

### Cosa resta not_measured

* **i tipi geometrici misti**: la tabella delle sonde porta soltanto punti, e una
  colonna con geometrie di tipo diverso non e mai stata letta. `mixed_geometry_types`
  resta chiusa.
* **le dimensioni oltre XY**: la proiezione condivisa produce WKB XY, e Z e M non
  hanno attraversato nulla.
* **le funzioni spatial**: nessuna sonda le ha eseguite su questo prodotto, e la
  lista verified di `MySQL` — che questa stessa sessione ha dovuto accorciare da
  ventisei a quindici — e la prova che ereditarla sarebbe un errore.

## Quattordicesima tranche: i savepoint, chiusi per non essere stati provati

`transactions.savepoints` era `false` su questo profilo da sempre, e la ragione
scritta accanto alla bandiera non diceva «il prodotto non li ha». Diceva:
«nessuna sonda li ha toccati, e un savepoint dichiarato e non provato e
esattamente il genere di promessa che si scopre rotta durante un rollback
parziale».

E' la forma di chiusura piu scomoda da leggere, perche non si distingue da
un'assenza vera guardando solo il documento. Questa tranche la scioglie.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_savepoint_partial_rollback` | commit=Committed — righe=1 contenuto `1:p1` | **identico** | **identico** |
| provider | profilo | `provider.profile_savepoint_unknown_name` | **rifiutato** — `Execution`/`Write`, codice 1305 | **identico** | **identico** |

### Cosa dice

**Il rollback parziale annulla cio che e venuto dopo, e nient'altro.** Una riga,
il savepoint, altre due righe, `ROLLBACK TO`, `RELEASE`, `COMMIT`: alla fine
resta la sola riga scritta prima. La rilettura arriva da un'altra connessione,
perche l'esito che il provider dichiara e cio che crede, e la tabella sul server
e cio che e successo — le due si confondono proprio nel caso in cui si vuole
distinguerle.

**E il nome conta.** La seconda sonda chiede un rollback a un savepoint mai
creato, e il server risponde 1305 su tutti e tre. E' la sonda che rende vera la
prima: senza, un motore che dicesse di si a qualunque `ROLLBACK TO` supererebbe
comunque il controllo sul conteggio — le due righe successive verrebbero
annullate dal `ROLLBACK` finale della transazione, e il conteggio tornerebbe lo
stesso. Il chiamante crederebbe di aver annullato qualcosa, e nessuno lo
smentirebbe.

**Nessuna divergenza.** Non era scontato che ci fosse — il crate implementa i
savepoint una volta sola per i due prodotti — ma non era nemmeno scontato che
non ci fosse: dodici tranche di questo documento esistono perche codice
condiviso non significa comportamento condiviso, e la lista spatial di `MySQL`
ne e la dimostrazione piu recente.

### Cosa ne segue per le capability

`transactions.savepoints` passa a `true`, e la guardia del profilo verifica che
coincida con quella di `MySQL`: una divergenza inventata su una superficie
condivisa e il difetto che ADR 0010 ha nominato, e va escluso dove il codice e
lo stesso.

`transactional_ddl` e `staged_swap` restano chiusi, e non per mancanza di
misura: l'ottava tranche ha mostrato che su questi motori `CREATE TABLE` fa
commit implicito, quindi il DDL sopravvive al rollback.

### Cosa resta not_measured

* **il codice 1305 non ha un verdetto condiviso.** Non compare in
  `MEASURED_SERVER_CODES`, quindi cade nella classificazione generica e arriva
  al chiamante come `Execution`. Ora e misurato due volte in questo repository,
  e con due significati diversi: «savepoint inesistente» qui, «funzione
  inesistente» nella sonda spatial di `MySQL`. Entrambi sono «il nome non
  esiste», e un verdetto condiviso e difendibile — ma deciderlo riguarda
  entrambi i significati insieme, e non e la domanda di questa tranche.
* **la fase del rifiuto e `Write`**, per un `ROLLBACK TO` che una scrittura non
  e. Misurata uguale sui tre riferimenti, quindi non e una divergenza: e una
  classificazione da rivedere, non una differenza fra prodotti.

## Quindicesima tranche: la colonna non porta il CRS, lo portano i valori

La tredicesima ha aperto la lettura spatial con un CRS dichiarato dal chiamante
e verificato valore per valore. Restava l'altra meta: scrivere. Ed era chiusa
per la ragione giusta — «nessuna geometria e mai stata scritta attraverso il
crate, e leggere un WKB che il server produce non dice nulla su cosa accetti in
ingresso».

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | scrittura | `raw.spatial_write_forms` | ddl=accettata, bind=accettato, srid=4326; legato[nudo=ok cast=ok senza_srid=3643] | ddl=**1064**, bind=accettato, srid=4326; legato[nudo=**4079** cast=ok senza_srid=**4079**] | **identico a MariaDB 12** |
| provider | profilo | `provider.profile_write_spatial_create` | Committed — righe=2 srid=4326 | **identico** | **identico** |
| provider | profilo | `provider.profile_write_spatial_append` | Committed — righe=4 srid=4326 | **identico** | **identico** |

### Cosa dice

**Su `MariaDB` nessuna DDL puo vincolare una colonna a un sistema di
riferimento.** `GEOMETRY SRID 4326` risponde 1064, come `REF_SYSTEM_ID` che la
prima tranche aveva gia misurato. Non e una lacuna di questa misura: e la forma
del prodotto.

**Ma l'SRID non si perde: lo porta il valore.** `ST_GeomFromWKB(..., 4326)`
memorizza una geometria il cui `ST_SRID` rende 4326, su entrambe le major. Ed e
la meta che incastra con la tredicesima tranche: la lettura verifica il CRS
valore per valore, quindi una colonna non vincolata resta descrivibile con
onesta. Se la scrittura avesse perso il CRS, la lettura di questo stesso crate
avrebbe rifiutato le righe appena scritte — e le due tranche si sarebbero
contraddette a distanza di ore.

**Un segnaposto non e un'espressione tipata.** E' la scoperta che ha richiesto
due sonde per essere vista. `raw.spatial_write_forms` usava
`ST_GeomFromWKB(ST_AsBinary(...), 4326)` — un valore il cui tipo il server
conosce — e passava su tutti e tre. Il piano di scrittura lega un parametro, e
li `MariaDB` risponde `4079`,
`ER_ILLEGAL_PARAMETER_DATA_TYPE_FOR_OPERATION`.

Delle tre varianti misurate, `CAST(? AS BINARY)` e l'unica che entrambi i
prodotti accettano, e su entrambi conserva l'SRID. Il rendering e percio
**condiviso** e non una decisione del profilo: una forma sola che vale su tutti
e due e meglio di due che divergono senza doverlo.

### Cosa ne segue per il codice

Due decisioni si spostano nel profilo, e una terza no.

`geometry_column_ddl` rende la colonna vincolata dove si puo e nuda dove non si
puo. Stava dentro una tabella di tipi, che e l'ultimo posto dove qualcuno
cercherebbe una divergenza di prodotto.

`geometry_target_srid_is_compatible` sostituisce il confronto `catalogo ==
dichiarato`. Non erano la stessa domanda: dove la colonna e vincolata il
catalogo deve portare **quell'**SRID; dove non puo esserlo, il catalogo tace e
non c'e niente con cui confrontare. Il confronto secco falliva sempre sul
secondo — `None` non e mai uguale a `Some(4326)` — e teneva chiusa la scrittura
spatial con una riga di codice, prima ancora che con una bandiera.

Il `CAST` invece resta condiviso, per la ragione detta sopra.

### Sull'ordine, che qui non e quello solito

`write_spatial_is_qualified` e insieme la capability pubblicata **e** il
cancello che il piano consulta: una sola origine, per scelta dichiarata, cosi
nessun profilo puo negare `write_wkb` e accettare comunque la compilazione. La
conseguenza e che finche era `false` nessuna sonda poteva attraversare il
percorso — `compile_write_column` si ferma prima.

Prove e apertura sono percio arrivate insieme, e la campagna e cio che le ha
rese vere. Non e una formalita: nei primi tre giri e stata rossa, e ogni volta
per un difetto diverso — lo schema della sonda senza `plenora.contract.version`,
il preflight che applicava alla scrittura la regola della lettura, e il
segnaposto non tipato. Se fosse rimasta rossa, la bandiera sarebbe tornata giu
con lei.

### Cosa resta not_measured

* **la dichiarazione `exact`**: le sonde girano su geometrie `mixed`, dove il
  tipo geometrico non compare. `writable_geometry_type` rinvia all'insieme di
  `MySQL` — sono nomi OGC, non una tabella di prodotto — ma nessuna sonda ha
  scritto una colonna dichiarata `exact` su questo prodotto, e la nota sta
  accanto al metodo che lo deciderebbe.
* **`create_spatial_index`**: `SpatialCapabilities::spatial_index` resta chiusa
  su entrambi i profili, e nessuna sonda ha creato un indice spaziale. Il nome
  e scritto per intero per la stessa ragione della tredicesima tranche:
  `spatial` e gia il prefisso di una superficie di sonde, e una guardia del
  self-test pretende che ogni identificatore con quel prefisso corrisponda a
  una sonda esistente.
* **le dimensioni oltre XY**: invariato dalla tredicesima.

## Sedicesima tranche: quattordici, non quindici — e le due major non coincidono

`SpatialCapabilities::functions` era una lista vuota, e la ragione accanto era che nessuna
sonda le aveva eseguite. Non era prudenza generica: la lista di `MySQL` e scesa
da ventisei a quindici il giorno in cui qualcuno l'ha attraversata davvero, e
undici delle bocciate erano li per analogia con `PostgreSQL`. Ereditarla su un
secondo prodotto sarebbe stato lo stesso errore, un prodotto piu in la.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_spatial_functions` | eseguite=15/15 | eseguite=15/15 | **eseguite=14/15**, `IsValid` risponde 1305 |

### Cosa dice

**E' la prima divergenza fra le due major di `MariaDB` di tutto questo
documento.** Quindici tranche hanno misurato tipi, catalogo, sessione, errori,
scritture, streaming, `RETURNING`, CRS e savepoint, e su ogni riga le due
versioni avevano risposto identico. Qui no: `ST_IsValid` esiste sulla 12.3 e
non sulla 11.8 LTS, che risponde 1305 — «la funzione non esiste».

**La lista pubblicata e quattordici, cioe l'intersezione.** Non e una
sottodichiarazione prudente: e la sola forma onesta quando il profilo e uno solo
per il prodotto. Una capability e una promessa fatta a chi **non sa** su quale
minor atterrera, e pubblicarne quindici funzionerebbe sulla 12.3 e romperebbe
sulla LTS — cioe sulla versione con piu installazioni.

Il giorno in cui il profilo si sdoppiasse per major, `IsValid` tornerebbe sulla
12, e la costante e il posto in cui dirlo.

**La sonda attraversa, non apre.** Costruisce per ogni funzione una
`QueryOperation` con l'arieta che il contratto dichiara, la manda al provider e
consuma il primo batch: il prepare non e l'esecuzione, e un errore che arriva
dopo verrebbe buttato via da un `drop`. Due geometrie, una lineare e una areale,
e basta che una passi — `ST_Area` su una linea risponde 3516, e chiudere la
funzione per quello sarebbe una falsa assenza.

### Cosa ne segue per il codice

**Il cancello e la promessa leggono ora la stessa lista.** Erano separati:
`render_query` consultava la costante di `MySQL` qualunque fosse il prodotto,
mentre la capability ne pubblicava un'altra. La conseguenza e concreta — su
`MariaDB 11.8` un piano con `ST_IsValid` sarebbe passato dal renderer e sarebbe
morto sul server con 1305, mentre la capability diceva giustamente che quella
funzione non c'e. Una delle due mentiva, e non era la capability.

Il profilo espone ora `verified_spatial_functions()`, e sia il renderer sia la
tabella delle capability lo chiamano. Il messaggio del rifiuto nomina di
conseguenza il **prodotto** invece di rinviare a una costante: su due prodotti
quella costante non e piu una.

### Cosa resta not_measured

* **i tipi geometrici misti**: invariato: la tabella delle sonde porta un punto,
  una linea e un poligono, ma mai due tipi diversi nella stessa colonna.
* **`SpatialCapabilities::spatial_index`**: nessuna sonda ha creato un indice
  spaziale, su nessuno dei due prodotti.
* **le funzioni oltre le quindici di `MySQL`**: la sonda attraversa la lista di
  `MySQL`, quindi misura al piu quella. Se `MariaDB` avesse funzioni che `MySQL`
  non ha, questo documento non lo saprebbe — ed e una domanda diversa, che
  comincia dal catalogo del prodotto e non dalla lista dell'altro.

## Diciassettesima tranche: i tipi misti, e un indice che il server accetta e il provider no

Due superfici che il documento non aveva mai toccato, ed erano le ultime dello
spatial.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_write_spatial_mixed` | Committed — `7:POINT,8:POLYGON` | **identico** | **identico** |
| raw | scrittura | `raw.spatial_index_forms` | senza_srid=ok, con_srid=ok | senza_srid=ok, **con_srid=1064** | **identico a MariaDB 12** |

### Cosa dice

**Una colonna `GEOMETRY` regge tipi diversi, su tutti e tre.** Un punto e un
poligono scritti nella stessa colonna, riletti per tipo da un'altra connessione:
`7:POINT,8:POLYGON`, identico ovunque.

Perche serviva una sonda apposta: le due scritture spatial della quindicesima
tranche portavano **soltanto punti**. `mixed` era una dichiarazione che nessuna
misura attraversava, e la colonna avrebbe retto identica anche se il prodotto
avesse ammesso un tipo solo. Una capability sostenuta da una prova che non la
distingue dal suo contrario non e sostenuta.

**`SPATIAL INDEX` funziona su tutti e tre**, sulla forma di colonna che ciascun
prodotto ammette. Il `1064` di `MariaDB` sulla variante `con_srid` non riguarda
l'indice: e il vincolo `SRID 4326` che quel prodotto non accetta comunque, e che
la quindicesima tranche aveva gia misurato. Tolto il vincolo, l'indice si crea.

### Cosa ne segue per le capability

`SpatialCapabilities::mixed_geometry_types` passa a `true` su `MariaDB`, e
coincide con `MySQL`: e la stessa colonna che regge tipi diversi, misurata con
lo stesso punto e lo stesso poligono.

`SpatialCapabilities::spatial_index` resta chiusa su **entrambi** i profili, e
qui la ragione cambia forma rispetto a prima. Non e piu «non misurata»: il
server lo accetta, ed e scritto qui sopra. E' che il piano di scrittura rifiuta
`create_spatial_index` in prepare — «non ancora qualificata» — e una capability
descrive cio che il **provider** sa fare, non cio che il server saprebbe.

La distinzione conta perche cambia cosa costerebbe aprirla: non una campagna
nuova, ma l'emissione della clausola nella DDL e una sonda che la attraversi.

### Cosa resta not_measured

* **l'indice spaziale attraverso il provider**: vedi sopra. Il fatto del server
  c'e, il percorso no.
* **la dichiarazione `exact` in scrittura**: invariata dalla quindicesima — le
  sonde girano su `mixed`, ed e proprio la dichiarazione che questa tranche ha
  finito di qualificare.
* **le dimensioni oltre XY**: invariata.

## Diciottesima tranche: l'indice spaziale attraversa il percorso

La diciassettesima aveva lasciato l'indice chiuso con una ragione che non era
«non misurata»: il server lo accetta, ed era scritto nel documento. Mancava il
percorso — il piano rifiutava `create_spatial_index` in prepare — e una
capability descrive cio che il **provider** sa fare.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_write_spatial_index` | Committed — indice `SPATIAL` su `shape` | **identico** | **identico** |

### Cosa dice

**Il piano emette la clausola, e il catalogo la conferma.** La sonda chiede
l'indice alla mode `Create` e poi interroga `information_schema.statistics`: che
il piano abbia emesso la clausola e cio che il provider crede, l'indice sul
server e cio che e successo. Le due si confondono proprio nel caso in cui si
vuole distinguerle.

**La clausola e la stessa sui due prodotti.** Diverge il vincolo di SRID sulla
colonna — `GEOMETRY SRID <n>` dove si puo, `GEOMETRY` dove non si puo — e
l'indice si attacca a entrambe. Non era scontato: il `1064` che `MariaDB`
risponde alla variante `con_srid` di `raw.spatial_index_forms` poteva far
sembrare che l'indice fosse il problema, e non lo e.

### Cosa ne segue per il codice

Tre rifiuti nuovi, e ciascuno ne evita uno peggiore.

L'indice vale **solo sulla mode `Create`**: si crea con la tabella, e su una
mode senza DDL aggiungerlo con un `ALTER` separato sarebbe una seconda
istruzione con un secondo commit implicito. Un fallimento a meta lascerebbe la
tabella con l'indice e senza le righe, o il contrario, e l'esito non saprebbe
dirlo.

Non vale su una **colonna nullable**: entrambi i motori la rifiutano, e
scoprirlo dal server significherebbe averlo scoperto con la tabella gia in piedi
— la `CREATE TABLE` fa commit implicito e non torna indietro.

Non vale su uno **schema senza geometrie**: eseguire senza indice sarebbe un
piano onorato a meta, e il chiamante crederebbe di avere un indice che non ha.

`SpatialCapabilities::spatial_index` si apre su **entrambi** i profili. Su
`MySQL` restava chiusa per la stessa ragione, non per una differenza fra i due.

### Cosa resta not_measured

* **la dichiarazione `exact` in scrittura**: le sonde girano su `mixed`.
  `writable_geometry_type` rinvia all'insieme di `MySQL` — sono nomi OGC — ma
  nessuna sonda ha scritto una colonna dichiarata `exact` su questo prodotto.
* **le dimensioni oltre XY**: la proiezione condivisa produce WKB XY, e Z e M
  non hanno attraversato nulla.
* **le funzioni spatial che `MySQL` non ha**: la sonda attraversa la lista
  dell'altro prodotto, quindi misura al piu quella. E' una domanda che comincia
  dal catalogo di `MariaDB`, e questo documento non l'ha ancora posta.

## Diciannovesima tranche: ventisei mai chieste, nove presenti

La lista verified ne portava quindici su `MySQL` e quattordici qui. Il contratto
ne dichiara settantadue, e quarantuno non restituiscono geometria — cioe
quarantuno che il mapper del result set saprebbe consegnare. Le ventisei di
mezzo non erano state **rifiutate**: non erano mai state chieste.

La differenza non e sfumatura. Una capability chiusa perche misurata assente e
una promessa che il prodotto non puo mantenere; una chiusa perche nessuno ha
guardato e una promessa che il prodotto forse mantiene gia, e che il consumatore
non puo usare.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | spatial | `raw.spatial_candidate_functions` | presenti 9/26 | presenti 9/26 | presenti **8/26** |

> La sonda si chiama oggi `raw.scalar_function_forms` e chiede tutte e
> quarantuno le scalari, non piu le sole mai provate: vedi la ventisettesima
> tranche. Il nome qui sopra e quello che portava quando questa misura fu
> eseguita, e il gate lo dichiara fra i nomi rinominati perche resti
> citabile senza che nessuno lo scambi per una sonda corrente.
| provider | profilo | `provider.profile_spatial_functions` | eseguite 24/24 | eseguite 21/21 | eseguite 21/21 |

### Cosa dice

**Nove funzioni su ventisei esistono, diciassette no.** Le diciassette assenti
sono `PostGIS` — clustering, MVT, geobuf, azimuth, distanze 3D — e non sono
lavoro rimasto: sono un fatto del prodotto.

**Le due liste non sono piu una il sottoinsieme dell'altra.** `Relate` e
`CoveredBy` esistono su `MariaDB` e non su `MySQL`; `HausdorffDistance` e
`FrechetDistance` il contrario. La guardia del profilo verifica ora entrambe le
direzioni: cercare solo cio che manca a `MariaDB` lascerebbe passare in silenzio
il giorno in cui `MySQL` perdesse qualcosa che qui c'e.

**`CoveredBy` e la seconda divergenza fra le due major**, dopo `IsValid`: la
12.3 ce l'ha, la 11.8 LTS no. Vale la stessa regola — la lista e
l'intersezione.

**`Relate` esiste e non e utilizzabile**, che sono due cose diverse. La sonda
delle candidate lo aveva trovato presente ed era entrato nella lista; il gate lo
ha bocciato con `1582`, numero di parametri sbagliato, perche `MariaDB` lo vuole
a tre argomenti — le due geometrie e il pattern DE-9IM — mentre il contratto ne
ammette anche due. Una funzione e qualificata quando lo e a **ogni** arieta che
il piano ammette, non quando ne esiste una che funziona: e la stessa regola che
tolse `Union` dalla lista di `MySQL`.

E' anche il caso che mostra perche le due sonde servono entrambe. Quella raw dice
«il server ce l'ha» e basta — e il primo filtro, non il verdetto. Quella sul
percorso dice se un piano scritto secondo il contratto arriva a destinazione.

### Cosa ne segue per le capability

`MySQL` passa a **ventiquattro** funzioni, `MariaDB` a **ventuno**. Il nome che
la sonda chiede al server e quello che il renderer emetterebbe — glielo da
`plenora_database_sql::spatial_function_name`, esposto per questo. Ricavarlo dal
catalogo o a mano misurerebbe una funzione che il crate non scrive mai, ed e
l'errore che aveva lasciato `ST_NDims` e `ST_NPoints` fra le verified di
`MySQL`.

### Cosa resta not_measured

Niente di nuovo: le trentuno geometriche restano il blocco, ed e la tranche
successiva a dire cosa le tiene chiuse.

## Ventesima tranche: la leva che non c'era

Il documento aveva contato trentuno funzioni del contratto che restituiscono
geometria, chiuse tutte dalla stessa riga: il mapper del result set rifiuta
`MYSQL_TYPE_GEOMETRY`. Sembrava una causa sola, e quindi la leva piu grossa
rimasta — il percorso di lettura aveva appena risolto lo stesso problema con un
CRS dichiarato dal chiamante e verificato valore per valore, e portare quella
forma anche qui avrebbe aperto trentuno superfici in un colpo.

La misura dice che quella leva non esiste.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | spatial | `raw.geometry_result_forms` | geo: envelope/centroid/buffer = **3618**; cart: 0 ovunque | geo: envelope=4326 centroid=4326 **buffer=0**; cart: 0 ovunque | **identico a MariaDB 12** |

### Cosa dice

**Su `MySQL`, in un sistema di riferimento geografico, quelle funzioni non
esistono.** `ST_Envelope`, `ST_Centroid` e `ST_Buffer` su una geometria 4326
rispondono `3618`: non sono implementate per SRS geografici. Non c'e un CRS da
verificare perche non c'e un risultato — e 4326 e il caso comune, non un angolo.

**Su `MariaDB` funzionano, ma il CRS sopravvive a seconda della funzione.**
`ST_Envelope` e `ST_Centroid` conservano 4326; `ST_Buffer` rende **0**. Non e un
errore: e che il risultato di un buffer, per quel motore, non appartiene piu a
quel sistema di riferimento.

**In cartesiano entrambi rendono 0 ovunque**, che e l'indefinito OGC.
Pubblicarlo come CRS direbbe una cosa che nessuno ha dichiarato.

E diverge perfino la **forma** del risultato: l'envelope di un punto occupa 93
byte su `MariaDB` — un poligono degenere — e 21 su `MySQL`, che rende il punto.

### Cosa ne segue

La differenza col percorso di lettura e netta, e va detta perche e la ragione
per cui la stessa soluzione non si applica. Li il CRS e di una **colonna**: il
chiamante lo dichiara, i valori lo portano, e la verifica valore per valore lo
conferma o lo smentisce. Qui la geometria e **calcolata**, e cio che ne esce non
porta un CRS confrontabile: su un prodotto non esce affatto, sull'altro dipende
dalla funzione.

Aprire questa superficie richiederebbe una regola di CRS per **funzione e per
tipo di sistema di riferimento**, misurata una funzione alla volta — trentuno
per due — e su `MySQL` il ramo geografico resterebbe comunque chiuso dal
prodotto.

Il rifiuto resta, e cambia ragione. Diceva «richiede il preflight SRID non
ancora qualificato», che suonava come una cosa da fare; ora dice «geometria
calcolata senza un CRS dimostrabile», che e cio che e stato misurato.

### Cosa resta not_measured

* **le altre ventotto funzioni geometriche**: la sonda ne ha attraversate tre —
  envelope, centroide, buffer — scelte perche coprono i tre esiti possibili.
  Misurarle tutte servirebbe soltanto a costruire la regola per funzione, ed e
  lavoro che ha senso il giorno in cui qualcuno decida di volerla.
* **la dichiarazione `exact` in scrittura** e **le dimensioni oltre XY**:
  invariate.

## Ventunesima tranche: le ultime due, e una DDL che diceva meno del contratto

Restavano due bandiere chiuse per ragioni diverse, e nessuna delle due era stata
guardata.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| raw | scrittura | `raw.exact_geometry_column` | point=ok polygon=ok, tipo sbagliato **1416** | point=ok polygon=ok, tipo sbagliato **1366** | **identico a MariaDB 12** |
| raw | spatial | `raw.geometry_dimensions` | xy=0, xyz/xym **3037** | xy=0, xyz/xym **null** | **identico a MariaDB 12** |

### Cosa dice

**La colonna tipata funziona, e rifiuta il tipo sbagliato.** Una colonna `POINT`
accetta un punto e respinge un poligono su tutti e tre — con codici diversi,
1416 e 1366, e lo stesso comportamento. E' la terza riga della sonda quella che
conta: una colonna `POINT` che accettasse un poligono non sarebbe `exact` in
nessun senso utile.

**Le dimensioni oltre XY non esistono su questi prodotti.** Il parser rifiuta il
WKT `POINT Z(1 2 3)` in entrambe le sintassi: `MySQL` con 3037 — WKT non valido
— e `MariaDB` parsando a `NULL`. Sommato a `ST_Z` e `ST_M` gia risultate
assenti, la chiusura smette di essere «non misurata» e diventa un **fatto del
prodotto**: non c'e una terza dimensione da scrivere, non che non sia stata
provata.

### Il difetto che la misura ha fatto emergere

Il piano supporta la dichiarazione `exact` da sempre, e il preflight la fa
rispettare: un contratto `mixed` non puo scrivere in una colonna tipata. Ma la
**DDL** della mode `Create` emetteva `GEOMETRY` anche per un contratto `exact`.

Cioe: creava una colonna che accetta qualunque geometria per dati che ne
contengono una sola. Il contratto diceva una cosa piu forte di quella che la
tabella faceva rispettare, e il primo a scriverci un poligono dentro — con un
altro strumento, o con un piano `mixed` su una colonna che il preflight
lasciava passare — non avrebbe trovato nessuno a fermarlo.

Ora la DDL emette il tipo dichiarato, e il vincolo di SRID gli si attacca
accanto dove il prodotto lo ammette: `POINT SRID 4326` su `MySQL`, `POINT` su
`MariaDB`.

### Cosa resta not_measured

* **le ventotto funzioni geometriche non caratterizzate**: la ventesima tranche
  ha misurato le tre che coprono i tre esiti possibili e ha stabilito quanto
  costerebbe la regola per funzione. Il resto ha senso il giorno in cui qualcuno
  decida di volerla.

E nient'altro. Ogni altra bandiera di questo profilo e aperta con una misura o
chiusa con una ragione misurata.

## Ventiduesima tranche: la contesa, che non era spatial

Le ventuno tranche precedenti hanno misurato tipi, catalogo, sessione, errori,
scritture, streaming, `RETURNING`, CRS, savepoint e l'intero spatial. Nessuna
aveva chiesto a questo provider di servire **piu lettori insieme**.

Non era una lacuna di contratto: era che un pool che sotto contesa mescolasse le
righe, o ne perdesse, non avrebbe fatto fallire nessuna prova di questo
documento. `PostgreSQL` ha una prova di contesa da tempo; `MySQL` l'ha avuta lo
stesso giorno di questa.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_concurrent_readers` | lettori=12 pool=4 righe=60 | **identico** | **identico** |

### Cosa dice

**Dodici lettori su quattro connessioni, e ciascuno vede la propria fetta.** Il
pool e volutamente piu piccolo del numero di lettori: uno abbondante non misura
la contesa, e la contesa e cio che la sonda esiste per attraversare. Il batch e
da due righe su cinque, cosi lo stream resta aperto per piu giri — e li che una
connessione condivisa per sbaglio si farebbe sentire.

**Il conteggio totale non basta, ed e la parte che vale la pena spiegare.** Un
pool che consegnasse a due lettori la **stessa** connessione a meta stream
renderebbe comunque sessanta righe, con quelle di uno finite nell'altro. Ogni
worker chiede percio una fetta di id **disgiunta** dalle altre e verifica di
aver visto la propria: il totale coglie una perdita, la fetta coglie uno
scambio.

### Cosa resta not_measured

* **la contesa in scrittura**: dodici lettori, non dodici scrittori. Un pool
  puo sbagliare in modo diverso quando le connessioni portano transazioni che
  scrivono, e questa sonda non lo dice.
* **la durata**: la sonda dura secondi. Non e un soak, e non pretende di
  esserlo.

## Ventitreesima tranche: gli scrittori e la tenuta

La ventiduesima aveva lasciato scritte due cose come non misurate: la contesa in
**scrittura** e la durata. Sono le due che la sonda dei lettori non poteva dire.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS |
|---|---|---|---|---|---|
| provider | profilo | `provider.profile_concurrent_writers` | scrittori=12 pool=4 righe=48 attribuzioni_errate=0 | **identico** | **identico** |
| provider | profilo | `provider.profile_pool_endurance` | giri=150 pool=3 connessioni=2→3 tetto=6 | **identico** | **identico** |

### Cosa dice

**Una connessione condivisa per sbaglio sbaglia in modo diverso a seconda di
cosa ci passa.** Fra due letture mescola righe, e la sonda precedente lo
coglieva. Fra due scritture fa altro: un commit su un filo che non e il suo,
righe attribuite alla transazione sbagliata, un rollback che ne annulla una che
non gli appartiene. Nessuna di quelle cose la sonda di lettura poteva vederla.

Dodici scrittori su un pool di quattro, ciascuno con la propria fetta di chiavi
e un payload che porta il **proprio** numero. La rilettura verifica due cose
diverse: il conteggio coglie una perdita, il payload coglie un'attribuzione
sbagliata — una riga scritta da un worker e finita sotto il nome di un altro
renderebbe il totale giusto e questo confronto no.

**Il pool regge centocinquanta cicli senza lasciarsi dietro niente.** Non e un
soak — dura secondi — ma misura la cosa che un soak cerca: che il numero di
connessioni non **cresca**, su abbastanza cicli perche una perdita di una ogni
giro diventi visibile. Con centocinquanta giri su un pool da tre, una perdita
sistematica sarebbe centocinquanta connessioni.

Il conteggio arriva da `Threads_connected` del **server**, letto da un'altra
connessione: e cio che il motore vede, non cio che il pool crede. Le due
divergono esattamente nel caso che la sonda cerca — una sessione che il pool ha
dimenticato e che il server tiene ancora aperta.

Il tetto lascia il margine del pool stesso: pretendere l'uguaglianza esatta
misurerebbe la velocita con cui il sistema operativo chiude un socket invece
della tenuta del pool.

### Cosa resta not_measured

* **un soak vero**: ore, non secondi. Questa sonda coglie una perdita
  sistematica, non una lenta — una connessione persa ogni mille giri le
  sfuggirebbe.
* **la contesa fra letture e scritture insieme**: le due sonde separano i due
  carichi, e un pool puo sbagliare proprio dove si mescolano.

---

## Ventiquattresima tranche: il carico misto, la durata, e una riga in piu nella matrice

La ventitreesima aveva lasciato scritte due cose come non misurate: **un soak
vero** e **la contesa fra letture e scritture insieme**. Questa tranche le
chiude, e chiude con esse l'elenco: non resta piu nulla di aperto su questo
profilo che non sia una decisione dichiarata.

Nel farlo la matrice guadagna un quarto riferimento, e quel riferimento ha
prodotto subito un difetto.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS | MariaDB 10.11 LTS |
|---|---|---|---|---|---|---|
| provider | profilo | `provider.profile_mixed_load` | giri=12 lettori=6 scrittori=6 pool=4 lette=540 scritte=288 attribuzioni_errate=0 connessioni=2→2 tetto=7 | **identico** | **identico** | **identico** |
| provider | profilo | `provider.profile_probe` | versione=9.7.2 qualificata=nessun elenco dichiarato | versione=12.3.2 qualificata=10.11 11.8 12.3 | versione=11.8.8 **identico** | versione=10.11.19 **identico** |

### Il carico misto, e perche e anche il soak

Le due sonde precedenti separano i carichi: dodici lettori in una, dodici
scrittori nell'altra. Un pool puo sbagliare proprio dove si mescolano, e
nessuna delle due poteva dirlo — una connessione che torna dal path di
scrittura con la transazione non chiusa e **innocua** fra scrittori, che ne
aprono un'altra subito, ed e velenosa per un lettore che la trova con un
`BEGIN` implicito addosso.

Sei lettori e sei scrittori partono insieme, a ogni giro, sullo **stesso** pool
da quattro. Che sia lo stesso e il punto: la transazione dei lettori si apre
dal provider e non da un pool costruito nella sonda, perche `pool_for` e
cachato per segreto e con lo stesso segreto degli scrittori la lettura esce
dalla medesima riserva di connessioni. Due pool separati riprodurrebbero le due
sonde che gia esistono.

**Le fette dei lettori hanno lunghezze diverse**, e non e un dettaglio. Con
fette uguali, due lettori che si scambiassero la connessione a meta stream
renderebbero comunque il conteggio giusto. Cinque righe per uno, sei per il
successivo, sette per quello dopo: lo scambio cambia il totale, e il conteggio
diventa da solo la prova che ciascuno ha visto la propria.

Per gli scrittori la stessa domanda ha un'altra forma e serve un'altra
risposta. Le chiavi non si ripetono fra i giri — un `Append` che trovasse la
propria riga gia scritta fallirebbe sul primario, e la sonda leggerebbe come
contesa cio che e aritmetica — e il payload di ogni riga porta il numero di chi
l'ha scritta, cosi il conteggio coglie una perdita e il confronto coglie
un'attribuzione sbagliata.

Il secondo residuo era la durata, e si chiude con lo **stesso codice**. Non e
un risparmio: e la ragione per cui il soak vale qualcosa. Un soak che
esercitasse un percorso diverso da quello del gate misurerebbe la tenuta di
codice che nessuno attraversa mai. Qui la corsa lunga e la corsa breve sono la
stessa, e cambia solo `PLENORA_MIXED_ROUNDS`. Il gate ne fa pochi, perche un
gate che dura ore non lo esegue nessuno — e una sonda che nessuno esegue non e
una sonda.

La corsa lunga e stata fatta, e il documento ne porta la riga accanto a quella
del gate:

| corsa | giri | letture | scritture | attribuzioni errate | connessioni |
|---|---|---|---|---|---|
| gate | 12 | 540 | 288 | 0 | 2 → 2 |
| soak | 6000 | 270.000 | 144.000 | 0 | 2 → 2 |

**Duecentosettantamila letture e centoquarantaquattromila scritture in contesa,
per riferimento**, e il numero di connessioni che il server vede alla fine e lo
stesso che vedeva all'inizio. E' cio che la sonda corta non poteva dire: coglie
una perdita sistematica — una connessione ogni giro — e non una lenta. Una
perdita di una connessione ogni mille giri sarebbe invisibile a dodici giri e
qui varrebbe sei connessioni sopra il tetto.

Il conteggio arriva da `Threads_connected` del **server**, letto da un'altra
connessione: e cio che il motore vede, non cio che il pool crede. Le due
divergono esattamente nel caso che la sonda cerca.

### La riga in piu: 10.11 LTS

Le tre righe che c'erano rispondevano a una domanda sola — se una divergenza
appartiene al fork o a una sua release — confrontando versioni vicine. 10.11 ne
pone un'altra: e la LTS supportata fino al 2028, quella che piu gente ha
davvero in produzione, e piu vecchia della riga di evidenza di due cicli. Cio
che le altre misurano vale per il server piu recente, non per quello che il
lettore di questo documento ha sotto le mani.

Ha prodotto un fatto al primo giro, e non era quello che sembrava.

**La probe rifiutava 10.11 con il codice 1193 redatto.** Il primo sospetto era
una divergenza di prodotto. La misura diretta dice altro: `transaction_isolation`
non esiste su MariaDB prima della 11.1 — fino a li si chiama `tx_isolation` — e
la probe la chiedeva nella **stessa query** di `VERSION()`. La query moriva
prima di arrivare al riconoscimento del prodotto e alla qualifica della
versione.

Il difetto non e la variabile mancante. Il profilo dichiara di essere
qualificato su un elenco, e accanto a quell'elenco c'era scritto che «la riga
che rende il rifiuto onesto e il messaggio: dice che la versione non e stata
misurata, non che non funzioni». Quel messaggio era **irraggiungibile
esattamente sulle versioni per cui era stato scritto**: chi arrivava con una
10.11 leggeva un errore server redatto e andava a cercare un guasto che non
c'era, mentre il repository sapeva benissimo cosa rispondergli e non riusciva a
dirlo.

Da qui la regola: **una query di capability puo fallire per la stessa ragione
che l'identita avrebbe spiegato, quindi l'identita si stabilisce prima.** La
probe fa ora due query — `VERSION()` e `@@version_comment`, poi le variabili di
sessione — con il riconoscimento e la qualifica in mezzo. Il costo e un
round-trip per probe, ed e il prezzo di un rifiuto che si sa leggere.

### Da rifiuto onesto a qualifica

Con il messaggio giusto, la ragione era chiara — ed era **nostra**, non sua.

`tx_isolation` risponde su tutte e tre le versioni MariaDB misurate;
`transaction_isolation` solo dalla 11.1 in su. Su MySQL vale l'opposto:
`tx_isolation` e stata rimossa nella 8.0. Non c'e un nome che vada bene per
entrambi i prodotti, e dentro MariaDB ce n'e uno solo che vada bene per tutte
le versioni. Il nome appartiene percio al profilo, esattamente come vi
appartiene gia quello del timeout di statement.

Fra i due non si sceglie il piu moderno: si sceglie quello che copre l'intera
matrice, perche un nome che copre meta delle righe fa fallire l'altra meta
prima ancora che la probe possa spiegarsi.

Con quella riga al suo posto, **cento sonde su 10.11 danno lo stesso esito che
danno su 11.8 e 12.3** — protocollo, tipi wire, valori decodificati, catalogo,
scritture, spatial, savepoint, contesa, carico misto. Le sole tre in cui 10.11
e l'unica a divergere sono `provider.test_connection`,
`provider.capabilities` e `provider.session_reuse`, cioe le sonde del provider
**MySQL** puntato su MariaDB: la misura del fail-close, non del prodotto.
Divergono perche il profilo MySQL chiede una variabile che 10.11 non ha, ed e
esattamente il comportamento che deve avere.

Restava dunque una cosa sola a tenerla fuori dall'elenco qualificato, ed era
una lista scritta prima che qualcuno la accendesse. L'elenco resta chiuso su
cio che e stato misurato — una major che non c'era quando la misura e stata
fatta continua a non essere coperta — e cambia solo che le versioni misurate
ora sono tre.

### Cosa resta not_measured

* **le ventotto funzioni geometriche non caratterizzate**: la ventesima tranche
  ha misurato le tre che coprono i tre esiti possibili e ha stabilito quanto
  costerebbe la regola per funzione. Il resto ha senso il giorno in cui
  qualcuno decida di volerla. E' una decisione dichiarata, non una lacuna.

E nient'altro. I due residui che la ventitreesima aveva lasciati aperti sono
chiusi, ogni bandiera di questo profilo e aperta con una misura o chiusa con
una ragione misurata, e le versioni su cui quelle misure valgono sono scritte
nell'elenco qualificato invece di essere sottintese.

## Venticinquesima tranche: tre funzioni che c'erano, e un campione che non le aveva chieste

La ventesima tranche si chiama «la leva che non c'era», e la sua conclusione era
che le trentuno funzioni che restituiscono geometria restano chiuse perche il
risultato non porta un CRS dimostrabile. Su `MySQL` la ragione era piu netta
ancora: quelle funzioni **non esistono**, e il server risponde `3618`.

Non esistono per i sistemi di riferimento **geografici**. Il campione della
ventesima tranche ne conteneva due — 4326, geografico, e 0, l'indefinito OGC —
e la categoria in mezzo non l'aveva chiesta.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS | MariaDB 10.11 LTS |
|---|---|---|---|---|---|---|
| raw | spatial | `raw.crs_rule_check` | geo 4326: **3618** ovunque; proiettati 3857 e 3003: envelope/centroid/buffer conservano l'SRID, stessoposto=1 | tutti e tre i sistemi: envelope e centroid conservano l'SRID, **buffer rende 0**, stessoposto=1 ovunque | **identico a MariaDB 12** | **identico a MariaDB 12** |

### Cosa dice

**Su `MySQL` quelle tre funzioni ci sono, in un sistema proiettato.** In 3857 e
in 3003 `ST_Envelope`, `ST_Centroid` e `ST_Buffer` girano tutte e tre e
restituiscono l'SRID dell'ingresso. Il `3618` non dice «non implementata», dice
«non implementata per i sistemi di riferimento geografici» — ed e una condizione
sul sistema, non sul prodotto. La ventesima tranche aveva letto la prima frase e
scritto la seconda.

**Su `MariaDB` girano ovunque, e su tutte e tre le major.** L'etichetta cade solo
per il buffer, che rende SRID 0 partendo da 4326 come da 3003.

**Le coordinate non si spostano mai.** E' la misura nuova, e quella che decide:
`ST_Contains(ST_Envelope(area), area)`, `ST_Within(ST_Centroid(area), area)` e
`ST_Contains(ST_Buffer(area, 1), area)` rispondono `1` in tutte e dodici le
combinazioni misurate. Un motore che riproiettasse in silenzio renderebbe un
SRID plausibile e una geometria altrove; qui il buffer di `MariaDB` lascia
cadere l'etichetta e **non** la geometria.

Quello zero, quindi, non significa «non si sa dove sta il risultato»: significa
che il motore non ha propagato il frame. E cio che il provider pubblica non e
quell'etichetta — e il CRS dichiarato per la colonna d'ingresso, propagato dalla
regola della funzione e confermato riga per riga sull'ingresso stesso, con la
stessa forma che il percorso di lettura usa da tempo.

### Cosa ne segue

Tre funzioni entrano nelle liste qualificate di **entrambi** i prodotti:
`Envelope`, `Centroid` e `Buffer`. `provider.profile_spatial_functions` le
attraversa con il resto — 27 su 27 per `MySQL`, 24 su 24 per `MariaDB` — e il
gate del riferimento le esegue una per una contro un server vero.

Restano chiuse per `MySQL` le colonne in un sistema **geografico**: un piano che
dichiari 4326 rendera SQL valido e il server rispondera `3618`. E' un limite del
prodotto e non una scelta della lista, che dichiara cosa il renderer sa scrivere
e non quale sistema di riferimento ogni funzione ammette — la stessa distinzione
per cui `ST_Area` su una `LINESTRING` risponde `3516` senza che `Area` esca
dall'elenco.

La ragione scritta accanto al rifiuto e cambiata due volte in due tranche, ed e
il segno che era una ragione e non una formula: diceva «richiede il preflight
SRID», poi «geometria calcolata senza un CRS dimostrabile», e ora copre cio che
davvero copre — una colonna geometrica proiettata **senza involucro** nel path
query, che arriva nel formato interno del prodotto e di cui nessuna posizione
nel piano dice niente.

### Cosa resta not_measured

* **le altre ventotto funzioni geometriche**: il meccanismo ora c'e e la regola
  e dichiarata nel catalogo per tutte e trentuno. Aprirne una in piu e diventato
  lavoro meccanico — una misura che *verifica* la regola invece di scoprirla —
  invece che una decisione da prendere trentuno volte. Restano not_measured, ma
  la ragione non e piu «costerebbe una regola per funzione»: e che nessuno le ha
  ancora chieste.
* **il ramo geografico di `MySQL`**: il provider potrebbe rifiutarlo al prepare
  invece di lasciare rispondere il server, e per farlo dovrebbe chiedere a
  `INFORMATION_SCHEMA.ST_SPATIAL_REFERENCE_SYSTEMS` se l'SRID dichiarato e
  geografico. Non e stato fatto: e un giro in piu su un percorso caldo, e
  l'errore del server e chiaro e attribuito. E' una decisione dichiarata.

## Ventiseiesima tranche: le ventotto, chieste

La tranche precedente si chiude dicendo che ventotto funzioni geometriche
restano `not_measured` e che la ragione non e piu il costo di una regola per
funzione: e che nessuno le ha chieste. Questa le chiede.

`raw.geometry_function_forms` attraversa tutte e **trentuno** — le tre gia
aperte comprese, come controllo che la sonda concordi con cio che gia si sa — su
tre sistemi di riferimento e tre forme geometriche. Il nome lo da
`spatial_function_name`, cioe lo stesso che il renderer emetterebbe; l'arieta e
i ruoli degli argomenti li danno `accepts_argument_count` e `takes_geometry_at`,
cioe il contratto. Una sonda che deducesse la firma misurerebbe una funzione che
il crate non scrive mai.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS | MariaDB 10.11 LTS |
|---|---|---|---|---|---|---|
| raw | spatial | `raw.geometry_function_forms` | proiettati: 13/31 presenti, tutte conservano l'SRID; 18 assenti | 13/31 presenti — 7 conservano, 6 perdono l'etichetta; 18 assenti | 12/31 presenti — 6 conservano, 6 perdono; 19 assenti | **identico a MariaDB 11.8** |

### Cosa dice

**Diciotto delle trentuno non esistono, su entrambi i prodotti.** `1305`, che e
il codice dell'assenza. `ST_MakeValid`, `ST_Reverse`, `ST_SnapToGrid`,
`ST_LineMerge`, `ST_OrientedEnvelope`, `ST_Subdivide`, `ST_OffsetCurve`,
`ST_UnaryUnion`, `ST_SimplifyPreserveTopology`, `ST_AsMvtGeom` e i quattro
`ST_Force*` non ci sono. Non e una lacuna di questo progetto: e cio che i due
motori hanno.

**`ST_Union` c'e, e risponde `1582`.** Numero di parametri sbagliato: entrambi
lo vogliono binario, e il contratto ammette anche la forma unaria. E' la stessa
regola che aveva gia tenuto fuori `Relate` — una funzione e qualificata quando lo
e a **ogni** arieta che il piano ammette, non quando ne esiste una che funziona.

**Delle tredici presenti, cinque restano fuori per una ragione che non e del
prodotto.** `Transform` porta l'SRID di destinazione in un argomento;
`Intersection`, `Difference` e `SymDifference` prendono due geometrie, e il
frame del risultato non e derivabile dal contratto; `Collect` e un aggregato,
che la macchina dei gruppi di questo crate non modella. Sono regole di CRS che
il provider non sa ancora propagare, ed e una chiusura del **nostro** lato,
scritta come tale.

**Due divergenze nuove fra i prodotti, e vanno in direzioni opposte.**
`ST_Boundary` e `ST_PointOnSurface` esistono su `MariaDB` e rispondono `1305` su
`MySQL`; `ST_Transform` e `ST_SetSrid` esistono su `MySQL` e non su `MariaDB`.
Le prime due entrano nella lista di `MariaDB` e non in quella di `MySQL` — ed e
la prima volta che la divergenza va in quella direzione per una funzione che
rende geometria. Le altre due non entrano da nessuna parte, per la regola di CRS.

**`ST_Simplify` e la terza divergenza fra prodotti**, dopo `IsValid` e le due
distanze: c'e su `MySQL`, e su `MariaDB` risponde `4212` sulla 12.3 e `1305`
sulle due LTS. Nemmeno dove il server risponde qualcosa e utilizzabile.

**Nessuna funzione ha spostato le coordinate.** La colonna `altrove` — un SRID
in uscita diverso sia dall'ingresso sia da zero — e vuota in tutte e dodici le
misure. L'unica intersezione mancante e quella del buffer su `MySQL` in 4326, ed
e un artefatto della sonda: li la funzione risponde `3618` e non c'e risultato
da intersecare.

### Cosa ne segue

Le liste qualificate crescono di cinque su `MySQL` — `StartPoint`, `EndPoint`,
`PointN`, `ConvexHull`, `Simplify` — e di sei su `MariaDB`, dove al posto di
`Simplify` entrano `PointOnSurface` e `Boundary`. Il gate del riferimento le
esegue una per una, e il gate della matrice ripete la prova su `MySQL` 8.4 e
8.0: una capability e una promessa a chi non sa su quale minor atterrera.

### Cosa resta not_measured

* **niente, di questa superficie.** Tutte e trentuno le funzioni che rendono
  geometria sono ora caratterizzate: diciannove assenti dal motore, cinque
  chiuse da una regola di CRS che questo provider non propaga, e sette aperte
  con la misura in mano. Il conteggio delle «ventotto non caratterizzate» esce
  dal documento perche non descrive piu niente.
* **le regole `argument` e `undefined`**: aprire `Transform` significa
  propagare l'SRID che il piano nomina; aprire `Intersection` significa
  dimostrare a runtime che le due geometrie condividono il frame. Sono due
  meccanismi, non due misure, e non sono stati costruiti. E' una decisione
  dichiarata.

## Ventisettesima tranche: il censimento scalare, e una misura che scadeva

Le trentuno funzioni che rendono geometria sono caratterizzate. Restano le
quarantuno che rendono uno scalare, ed erano gia chiuse — ogni bandiera aperta
con una misura o chiusa con una ragione misurata. Il difetto non era li: era in
**come** quella misura veniva ripetuta.

`raw.spatial_candidate_functions` chiedeva al server le funzioni «mai provate»,
cioe quelle che non stavano in nessuna delle due liste pubblicate. Il filtro
sembra ovvio — chiedere cio che si sa gia e spreco — e ha un effetto che non si
vede subito: **una funzione aperta su `MySQL` smette di essere chiesta a
`MariaDB`**, dove non e aperta.

`HausdorffDistance` e `FrechetDistance` sono entrate nella lista di `MySQL` nella
diciannovesima tranche. Da quel momento nessuna campagna le ha piu chieste a
`MariaDB`, e il documento ha continuato a dire che li non ci sono ripetendo una
misura vecchia di sette tranche. Era ancora vero, e non era piu verificato.

La sonda si chiama ora `raw.scalar_function_forms`, in simmetria con quella
geometrica, e chiede tutte e quarantuno le scalari a ogni server.

### La matrice

| famiglia | superficie | sonda | MySQL 9.7 | MariaDB 12.3 | MariaDB 11.8 LTS | MariaDB 10.11 LTS |
|---|---|---|---|---|---|---|
| raw | spatial | `raw.scalar_function_forms` | presenti **24/41** | presenti **24/41** | presenti **22/41** | **identico a MariaDB 11.8** |

### Cosa dice

**Su `MySQL` il conto torna esatto: 24 presenti, 24 pubblicate.** Non c'e una
sola funzione scalare che il server possiede e che la lista non offre. Le
diciassette assenti — clustering, MVT, geobuf, azimuth, distanze 3D, `ST_Z` e
`ST_M` — sono `PostGIS` che il motore non ha.

**Su `MariaDB` la differenza fra presenti e pubblicate e tre, e ciascuna ha gia
il suo nome.** `IsValid` e `CoveredBy` esistono sulla 12.3 e non sulle due LTS,
e la lista e l'intersezione perche una capability e una promessa a chi non sa su
quale minor atterrera. `Relate` esiste su tutte e tre e risponde `1582`
all'arieta che il contratto ammette: esiste e non e utilizzabile, che sono due
cose diverse.

**Le due assenze che erano ricordate ora sono rimisurate.**
`HausdorffDistance` e `FrechetDistance` rispondono `1305` su tutte e tre le major
di `MariaDB`. Il fatto non cambia; cambia che da oggi lo dice una campagna e non
la memoria di una campagna.

### Cosa ne segue

Niente si apre, ed e il risultato giusto: la superficie era chiusa bene, e cio
che mancava era la ripetibilita della prova. Una guardia impedisce al filtro di
tornare — e stata falsificata rimettendolo in due forme, su entrambe le liste e
su una sola, e morde in tutte e due.

Con questa tranche il catalogo e coperto per intero. Delle settantadue funzioni:
trentuno rendono geometria e sono caratterizzate dalla tranche precedente,
quarantuno rendono uno scalare e sono censite qui. Nessuna resta «mai chiesta»,
su nessuno dei due prodotti.

### Cosa resta not_measured

* **le regole `argument` e `undefined`**, invariate dalla tranche precedente:
  sono due meccanismi da costruire, non due misure da fare.
* **il profilo per major**: `IsValid` e `CoveredBy` tornerebbero sulla 12.3 il
  giorno in cui il profilo di `MariaDB` si sdoppiasse. E' una decisione
  dichiarata da sedici tranche, e questa misura ne conferma il prezzo esatto —
  due funzioni.
