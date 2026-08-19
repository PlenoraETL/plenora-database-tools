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
quello dichiarato, se una sonda non e accettata, o se una diverge. La seconda
condizione non e ridondante rispetto alla terza: "coincidono" e vero anche
quando tutti e tre falliscono allo stesso modo, ed e il verde falso che questa
misura esiste per escludere.

### Cosa questa tranche non prova

Tre sonde hanno dovuto essere corrette prima di leggere i numeri, e la terza
correzione ha ristretto cio che si puo affermare.

`access_mode` leggeva `@@transaction_read_only`, che riflette `SET
TRANSACTION` e non `START TRANSACTION READ ONLY`: dava lo stesso valore per
tutte e tre le modalita. Ora si osserva l'effetto — una scrittura dentro la
transazione — e i tre casi si distinguono: ammessa, rifiutata, ammessa.

Il contesto rileggeva isolamento e autocommit, cioe nulla che lo riguardasse.
Ora rilegge la variabile utente che il provider imposta.

La sonda sul riuso pretendeva che il pool riconsegnasse la **stessa**
connessione dopo la restituzione. Fallisce su tutti e tre: `mysql_async` ne
apre una nuova. Cio che resta provato e che ogni sessione consegnata dal pool
e bootstrappata — la proprieta su cui il provider poggia — mentre la
riapplicazione del bootstrap su una connessione **riusata** non e esercitata
da questa configurazione. Resta non misurata, ed e scritto qui invece di
essere sottinteso da una sonda verde.
