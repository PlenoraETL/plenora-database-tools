# ADR 0012 - Provider MySQL di riferimento

## Stato

Accettata per l'implementazione iniziale. La baseline di riferimento e MySQL
8.4 LTS; MySQL 8.0 e la seconda riga della matrice. MariaDB non viene dedotta
come compatibile e richiedera una campagna separata.

## Decisione

Il provider usa `mysql_async` 0.37 con protocollo nativo Tokio, TLS rustls e
provider crittografico `ring`. Non viene introdotto un ORM. Query e valori
restano separati e il renderer comune conserva i placeholder `?`.

La verifica TLS e il default. `TrustServerCertificate` e un opt-out esplicito
che mantiene la cifratura ma disabilita verifica della CA e del nome; non e una
policy implicita. Il gate conserva la chiave di firma CA in un volume privato
montato solo nel certgen, pubblica al server soltanto CA e identità server,
rigenera l'intero set se un artefatto è incoerente, verifica SAN DNS/IP e prova
il rifiuto di un alias non presente nel certificato. `LOCAL
INFILE`, cleartext authentication e socket locale non sono abilitati.

Ogni sessione applica un bootstrap deterministico: autocommit attivo fuori
dalle write, UTC e SQL mode strict. Timeout o cancellazione quarantinano la
connessione; un errore di trasporto durante write/commit non puo diventare
successo o rollback confermato.

Il bootstrap usa le query `setup` del driver, non soltanto `init`: viene quindi
eseguito sia sulla nuova connessione sia dopo `COM_RESET_CONNECTION` o
`change_user`. Il gate live altera deliberatamente autocommit, timezone e SQL
mode, esegue `COM_RESET_CONNECTION` e verifica il ripristino sulla stessa
`CONNECTION_ID()`.

Ogni checkout applica due budget distinti: prima un semaforo di pool fail-closed
entro `acquire_timeout`, poi l'attesa della connessione entro `connect_timeout`;
il timeout operazione resta separato. Timeout e cancellazione prima della
creazione della sessione non dichiarano quarantena, perche non esiste ancora
una connessione da isolare. Timeout e cancellazione in-flight sono provati live
e rendono la sessione quarantinata e non riusabile.

## Divergenza DDL

MySQL 8.4 offre DDL atomico InnoDB ma non DDL transazionale: una DDL termina
implicitamente la transazione attiva. Di conseguenza `transactional_ddl` e
`staged_swap` restano `false` finche non esiste un protocollo MySQL specifico
con journal, nomi deterministici, recovery idempotente e fault injection in
ogni finestra di pubblicazione. Non si copia lo swap PostgreSQL/SQL Server.

## Spatial

MySQL espone un'unica gerarchia `GEOMETRY`; il comportamento cartesiano o
geodetico dipende dallo SRS. SRID e ordine degli assi devono essere profilati
dal catalogo e dalle conversioni WKB. La baseline non dichiara Z, M o ZM: ogni
input non XY deve fallire chiuso, senza appiattimento. `ST_Transform` verra
pubblicata soltanto dopo prove su SRS risolti e axis order esplicito.

Nel contratto Arrow una colonna `GEOMETRY` dichiara `mixed`; una colonna
tipizzata (`POINT`, `LINESTRING`, `POLYGON` e analoghe) dichiara `exact` e il
tipo canonico. Il valore non canonico `homogeneous` non viene emesso.

Il worker legge una riga soltanto dopo una richiesta esplicita del consumer,
mentre il batch corrente detiene gia la lease di memoria: non esistono righe
prefetch o sender bloccati fra batch. Prima di richiedere una riga ulteriore il
consumer conserva nel residuo il massimo costo conservativo di una riga valida,
derivato da `cell_bytes`, numero di colonne e crescita dei buffer. Il gate live
esercita righe di dimensione crescente e verifica che attraversino il confine
di batch senza consumo anticipato o perdita. Il drop anticipato cancella il
worker e avvia un reaper bounded.

## Criterio di promozione

Una capability diventa `true` soltanto nella revisione che contiene API
pubblica, limiti, test negativi, prova live e aggiornamento del gate. Il mero
supporto sintattico del server non costituisce una capability Plenora.
