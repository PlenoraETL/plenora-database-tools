# Evidenza live — SQL Server 2022

Data: 2026-07-29

## Ambiente

- immagine: `mcr.microsoft.com/mssql/server:2022-latest`;
- image id osservato:
  `sha256:e07b9699a2b749969f19d86563ceeea22bd3a69f7f1db85a8d1ac4bdaf0c6f56`;
- prodotto: `16.0.4255.1`;
- livello: `RTM`;
- edition: `Developer Edition (64-bit)`;
- database: `dataflow_test`;
- compatibility level: `160`;
- collation: `SQL_Latin1_General_CP1_CI_AS`;
- container: `running`, healthcheck `healthy`.

L'ambiente è riproducibile tramite `docker-compose.sqlserver.yml`. Il servizio
`sqlserver-init` applica in modo idempotente gli script dopo che il database è
healthy; non dipende da un entrypoint init che l'immagine Microsoft non offre.
Una seconda esecuzione completa sulla fixture già popolata è terminata con
exit code `0`.

## Prove completate

La suite live seriale ha verificato:

1. connessione TCP/TDS e bootstrap completo della sessione;
2. accesso con opt-out `TrustServerCertificate` esplicito per la CA self-signed
   della fixture;
3. rifiuto della stessa istanza con la policy predefinita `Verify`;
4. rilevazione di versione, edition, compatibility level e impostazioni
   snapshot;
5. presenza dei tipi nativi `geometry` e `geography`;
6. enumerazione degli schemi e degli oggetti visibili;
7. introspezione deterministica di 13 colonne, inclusi identity, computed,
   rowversion, XML, geometry e geography;
8. introspezione di primary key, unique/check constraint e indici;
9. stabilità del token sullo schema invariato;
10. variazione del token dopo `ALTER TABLE`, con cleanup della fixture;
11. mapping Arrow di booleani, interi signed/unsigned, float, decimal/money,
    testo, binari, UUID/XML proiettati e tipi temporali;
12. lettura di cinque righe in tre batch bounded `2/2/1`;
13. `geometry` e `geography` XY come GeoArrow WKB con SRID 4326;
14. rifiuto fail-closed di SRID misti e geometrie Z;
15. quarantena della connessione dopo drop di uno stream non drenato;
16. prepared write TDS di tutti i tipi della fixture, inclusi UUID/XML,
    decimal, temporali, geometry e geography;
17. differential SQL bidirezionale read/write con zero righe differenti;
18. rollback di `TRUNCATE` e di un batch già inserito dopo duplicate key nel
    batch successivo;
19. rilevazione del cambio schema tra prepare ed execute, senza mutazioni;
20. rifiuto live di `time(7)`/`datetime2(7)` a 100 ns non rappresentabili
    esattamente dal profilo Arrow a microsecondi;
21. conservazione di `datetimeoffset` come RFC3339, incluso offset originale
    per riga e settima cifra;
22. fault prima del commit con rollback verificato, target invariato ed effetto
    pubblico `RolledBack`;
23. perdita del trasporto TDS dopo la prima conferma `OUTPUT`, senza falso
    successo, classificata `Unknown/RequiresRecovery` e con rollback osservato
    da una sessione indipendente;
24. perdita della conferma del commit dopo l'applicazione server: outcome
    `OutcomeUnknown`, zero righe confermate, retry automatico vietato e riga
    osservabile dalla sessione di riconciliazione;
25. taglio fisico del socket TDS, tramite proxy TCP locale, dopo un `INSERT`
    non committato: errore `Unknown/RequiresRecovery`, nessuna riga residua e
    sentinel preservato;
26. commit server seguito da finestra di risposta TDS ritardata, riga osservata
    da una sessione indipendente e socket fisicamente chiuso prima della
    conferma client: `OutcomeUnknown` e retry automatico vietato;
27. blackhole con socket mantenuto aperto mentre SQL Server espone una richiesta
    read attiva: timeout, nessun effetto remoto e sessione non riusabile;
28. rollback server osservato da una sessione indipendente seguito da blackhole
    della risposta TDS: errore `Unknown/RequiresRecovery`, mai falso rollback
    confermato.

Comando della prova:

```text
cargo test -p plenora-db-sqlserver live_ -- --ignored --test-threads=1
```

Esito: **17 superati, 0 falliti**.

## Limiti dell'evidenza

Questa prova non dimostra ancora:

- catena TLS privata verificata positivamente e hostname matching;
- bulk write e modalità create/replace/update/upsert/delete-by-keys;
- latenza finita e packet loss durante read e rollback;
- profili spatial Z/M e `FullGlobe` (oggi rifiutati);
- tipi `sql_variant`, CLR/UDT e famiglie non incluse nel profilo read;
- altre build SQL Server e Azure SQL.

La suite combina fault deterministici interni e un proxy TCP capace di chiudere
materialmente entrambi i socket oppure mantenerli aperti senza inoltrare byte.
Le barriere sono legate alle fasi transazionali o a richieste osservate sul
server, non a sleep probabilistici. Gli hook restano compilati solo nei test e
non ampliano l'API pubblica.

Tali proprietà restano non pubblicizzate.
