# Connessione prestata: requisito L-DB1

Decisione del titolare del 21/11/2026, per la major 13.0.0 non rilasciata.
Questo documento definisce il requisito; non costituisce una qualifica live
e non apre capability. Le API prestate sono ancora da implementare.

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
