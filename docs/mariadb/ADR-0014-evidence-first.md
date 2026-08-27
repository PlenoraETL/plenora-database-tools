# ADR 0014 - Confine MySQL/MariaDB basato su evidenza

## Stato

Accettata e implementata.

Questa ADR conserva soltanto la decisione ancora referenziata dal codice. La
sequenza delle campagne, le ipotesi scartate e gli stati intermedi restano in
Git; l'inventario corrente delle prove e generato in `EVIDENCE.md`.

## Contesto

MySQL e MariaDB parlano lo stesso protocollo e condividono gran parte del data
path, ma non sono lo stesso prodotto. Le campagne hanno misurato differenze
sulle superfici realmente attraversate dal provider:

- `MAX_EXECUTION_TIME` e rifiutato da MariaDB con codice 1193; il profilo usa
  `max_statement_time` e converte i millisecondi in secondi;
- `information_schema.statistics.EXPRESSION` manca su MariaDB con codice 1054;
- dalla stessa DDL JSON i metadata possono pubblicare `native_type=json` su
  MySQL e un tipo testuale su MariaDB;
- MySQL espone `SRS_ID` nel proprio catalogo, mentre MariaDB richiede una
  strategia diversa e la verifica del CRS sui valori.

Duplicare l'intero crate per isolare queste differenze avrebbe duplicato
protocollo, pool, transazioni, mapping Arrow e lifecycle di scrittura.
Nasconderle dietro il riconoscimento automatico del server avrebbe invece
trasformato un errore di configurazione in una scelta silenziosa.

## Decisione architetturale

1. Esiste **un solo crate**, `plenora-db-mysql`, con implementazione condivisa.
2. Esistono **due provider pubblici distinti**, `MysqlProvider` e
   `MariadbProvider`.
3. Ogni provider seleziona un profilo di prodotto esplicito. Il profilo
   possiede riconoscimento e versioni ammesse, SQL del timeout, query di
   catalogo, metadata nativi, comportamento spatial e classificazione degli
   errori che divergono.
4. **Nessuna selezione automatica.** `MysqlProvider` rifiuta MariaDB e
   `MariadbProvider` rifiuta MySQL. CLI e SDK espongono nomi e factory distinti.
5. Il **fail-close resta** per ogni capability non sostenuta da una prova
   riproducibile. Un esito `not_measured` non apre una bandiera.

La configurazione di trasporto resta condivisa: i due prodotti usano lo stesso
protocollo e duplicare tipi TLS equivalenti moltiplicherebbe i punti in cui un
default di sicurezza potrebbe divergere.

## Evidenza e riproduzione

Le versioni e i digest dei riferimenti vivono esclusivamente in
`docker/mariadb/references.json`. L'inventario generato delle sonde e i comandi
sono in [`EVIDENCE.md`](EVIDENCE.md); gli esiti appartengono al verdetto JSON
della singola corsa live.

La decisione non trasforma l'inventario in un risultato: una corsa saltata non
e una corsa passata, e una versione non elencata non viene dedotta compatibile.

## Conseguenze

- Le correzioni al data path comune si applicano a entrambi i prodotti.
- Le differenze hanno un proprietario leggibile nel profilo invece di bandiere
  sparse nel provider.
- Il consumer sceglie il prodotto prima della connessione e riceve un rifiuto
  se il server non corrisponde.
- Nuove capability o nuove versioni entrano soltanto insieme alla prova live e
  al gate che la esegue.
