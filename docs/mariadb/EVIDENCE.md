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
