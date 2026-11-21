# Connessione prestata: requisito L-DB1

Decisione del titolare del 21/11/2026, per la major 13.0.0 non rilasciata.
Il documento definisce il requisito e descrive la superficie che lo
soddisfa (sezione «Superficie»); la qualifica live e quella della sezione
«Prove».

Il contratto richiesto e: funziona dietro **PgBouncer >= 1.21 in modo
transazione con `max_prepared_statements > 0`**. Sono ammesse le istruzioni
preparate a livello di protocollo, anche nominate. Non e richiesto un fork di
tokio-postgres. Il requisito precedente di usare soltanto Parse unnamed e
ritirato. Versione e configurazione del pooler sono responsabilita del
deployment; il supporto non puo essere dedotto dalla sola versione PostgreSQL.

La superficie prestata deve lasciare al chiamante apertura, ruolo, timeout,
commit, rollback e chiusura. Non deve introdurre SET persistenti, LISTEN,
advisory lock di sessione o cursori WITH HOLD. Eventuali impostazioni locali
alla transazione devono essere dichiarate nel contratto della superficie.
Un esito prima del commit del chiamante non puo dichiarare `Committed`.

Il mancato supporto delle istruzioni preparate deve produrre un errore
tipizzato e riconoscibile, senza messaggi vendor, SQL, parametri o dati.
Non e una condizione per riprovare automaticamente la scrittura sulla stessa
transazione. Dopo un errore PostgreSQL il chiamante resta responsabile del
rollback della transazione prestata.

La qualifica richiesta comprende PostgreSQL reale, commit e rollback del
chiamante, identita dei codec e del rapporto delle perdite, annullamento della
COPY, superamento del budget e assenza di stato di sessione. La prova tramite
PgBouncer deve usare la configurazione sopra; una prova sul solo PostgreSQL
non la sostituisce.

## Superficie

In `plenora_db_postgres` (riesportate dalla radice del crate):

- `scrivi_arrow(tx, &WritePlan, stream, &budget, &annulla)` scrive un flusso
  di `RecordBatch` dentro la `Transaction` del chiamante con gli stessi codec
  del provider (COPY testo o binaria, WKB con SRID) e restituisce righe
  ricevute, righe applicate e il rapporto delle perdite provvisorio. Non
  apre, conferma o annulla transazioni e non esegue `SET`: un `search_path`
  che mette prima di `pg_catalog` uno schema con funzioni, operatori o tipi
  omonimi si rifiuta prima di ogni effetto (regola Q), e le query di catalogo
  usano operatori qualificati. Con `session_replication_role` diverso da
  `origin` la scrittura si rifiuta invece di cambiarlo. In errore il
  rapporto resta allegato all'errore, l'effetto remoto e `Unknown` e il
  rollback spetta al chiamante.
- `leggi_arrow(client, &ReadPlan, &budget, &annulla)` legge Arrow sul
  `Client` del chiamante, anche fuori transazione, con gli stessi codec e
  limiti di `ReadPlan::new`. Non modifica impostazioni; interrompere lo
  stream non invia un `CancelRequest` su una connessione nuova.

Il rapporto provvisorio coincide, per rischio e valutazione, con quello della
scrittura posseduta sullo stesso input; non dichiara `Committed`.

## Prove

In `crates/plenora-db-postgres/src/borrowed_live_tests.rs`, eseguite dal gate
`scripts/check_postgres_reference.py` (workflow `postgres-assurance`):

- `live_borrowed_commit_rollback_and_owned_parity`: commit e rollback del
  chiamante, lavoro precedente nella stessa transazione preservato, nessun
  commit implicito, `pg_settings` invariato prima, durante e dopo, parita
  binaria e di rapporto con la scrittura e la lettura possedute, COPY testo e
  binaria;
- `live_borrowed_budget_and_cancelled_input_leave_caller_in_control` e
  `live_borrowed_cancel_in_copy`: budget superato e annullamento durante la
  COPY;
- `live_borrowed_through_pgbouncer_transaction_mode`: PgBouncer del compose
  del riferimento (`docker/pgbouncer/`, 1.26.1, `pool_mode = transaction`,
  `max_prepared_statements = 100`, due connessioni server). La prova legge
  modo, versione e `max_prepared_statements` dalla console; quattro client
  scrivono tre volte ciascuno (COPY binaria e testo, un rollback) e leggono
  fuori transazione, cosi le istruzioni nominate passano fra backend
  diversi; i batch letti coincidono con quelli della lettura diretta. Il gate
  la richiede per nome (`PLENORA_REQUIRE_LIVE_PGBOUNCER`).

Il rifiuto con `max_prepared_statements = 0` non ha una prova live: dipende
da quale backend riceve il Bind, quindi non e deterministico. La sua
classificazione (`PG_PREPARED_STATEMENTS_REQUIRED`) e provata sui messaggi
riconosciuti nei test unitari di `prepared_rejection_tests.rs`.
