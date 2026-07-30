# SQL Server — percorso del provider

SQL Server è il secondo provider di `plenora-database-tools`. PostgreSQL resta
il riferimento comportamentale, non un modello da copiare alla cieca.

## Fasi 0–3

| Fase | Deliverable | Stato |
|---|---|---|
| 0 | ADR, baseline, matrice capability e gate di evidenza | offline |
| 1 | renderer T-SQL fail-closed e limiti strutturali | testabile offline |
| 2 | crate provider e confini architetturali | testabile offline |
| 3 | configurazione TDS/TLS, bootstrap, pool e recovery state machine | verificato sul riferimento SQL Server 2022 |
| 4 | probe, catalogo, vincoli, indici e schema token | verificato sul riferimento SQL Server 2022 |
| 5 | mapping SQL Server→Arrow e read stream bounded | verificato sul riferimento SQL Server 2022 |
| 6 | prepared write, transazione, schema guard e rollback | verificato sul riferimento SQL Server 2022 |
| 7 | trait comune `Provider`, capability e testkit di conformità | verificato sul riferimento SQL Server 2022 |
| 8 | read comune e `QueryOperation` a singola source | verificato sul riferimento SQL Server 2022 |
| 9 | TDS bulk opt-in, differential e rollback multi-batch | verificato sul riferimento SQL Server 2022 |
| 10 | `QueryOperation` relazionale, schema output server-authoritative e codec nativi | verificato sul riferimento SQL Server 2022 |

Il crate usa un client TDS diretto. Non introduce un ORM: AST, mapping Arrow,
policy di perdita e outcome restano nei contratti Plenora.

## Invarianti

- nessuna credenziale in `Debug`, errori o metriche;
- TLS con verifica certificato per default;
- un opt-out TLS deve essere esplicito;
- una sola operazione attiva per connessione; MARS disabilitato;
- tutti i result stream devono essere drenati prima del riuso;
- cancellazione o dubbio sullo stato TDS implicano quarantena;
- una sessione riusabile deve essere fuori transazione e nello stato `Ready`;
- errori di commit con esito non dimostrabile restano `Unknown`;
- parametri e identificatori sono validati prima dell'I/O;
- funzionalità non provate falliscono in preparazione.
- il contratto Arrow canonico è validato dallo stesso giudice nel core usato
  da PostgreSQL; il crate SQL Server interpreta solo il proprio namespace
  nativo.

## Lavoro che richiede il database

La campagna live su SQL Server 2022 copre handshake con certificato
self-signed, probe, catalogo, mapping Arrow e streaming bounded di scalari,
`geometry` e `geography`, oltre a prepared write `append`/`truncate_insert` in
singola transazione. Il codec `TdsBulk` è opt-in, bounded e ammesso solo per
tutte le colonne scrivibili nell'ordine del catalogo e per tipi verificati;
spatial e conversioni non native restano sul percorso prepared. Include fault
deterministici pre-commit, sul trasporto e
tagli fisici del socket durante write e prima della conferma commit, con
verifica da una sessione indipendente. Copre inoltre blackhole durante read e
dopo rollback server.
La stessa campagna attraversa ora il trait comune `Provider`: test connection,
capability, catalogo, projection/filter/order/limit bindati, round-trip
prepared write e `QueryOperation` relazionali con CTE, join, group/having,
aggregati, window, set operation e offset/fetch. Lo schema Arrow dell'output è
derivato dal server prima dell'esecuzione e confrontato con i metadati TDS
effettivi; questo copre anche risultati vuoti senza rieseguire la query. I
risultati spatial non convertiti esplicitamente in WKB, i lateral join, il
locking e le forme prive di un nome output deterministico restano fail-closed.
L'evidenza è in [LIVE-REFERENCE.md](LIVE-REFERENCE.md). Servono ancora
certificati verificabili e campagne dedicate per altre modalità di write,
latenza/packet loss su read e rollback, oltre ai profili spatial avanzati. La
matrice completa è in [CAPABILITY-MATRIX.md](CAPABILITY-MATRIX.md).
La campagna cumulativa di coverage è descritta in
[COVERAGE.md](COVERAGE.md).
Il gate autonomo con evidenza riproducibile è descritto in
[ASSURANCE.md](ASSURANCE.md).
