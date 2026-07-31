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
policy implicita. `LOCAL INFILE`, cleartext authentication e socket locale non
sono abilitati.

Ogni sessione applica un bootstrap deterministico: autocommit attivo fuori
dalle write, UTC e SQL mode strict. Timeout o cancellazione quarantinano la
connessione; un errore di trasporto durante write/commit non puo diventare
successo o rollback confermato.

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

## Criterio di promozione

Una capability diventa `true` soltanto nella revisione che contiene API
pubblica, limiti, test negativi, prova live e aggiornamento del gate. Il mero
supporto sintattico del server non costituisce una capability Plenora.
