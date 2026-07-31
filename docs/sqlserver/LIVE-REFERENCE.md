# Evidenza live — SQL Server 2022

Data: 2026-07-31

## Ambiente

- immagine:
  `mcr.microsoft.com/mssql/server@sha256:e07b9699a2b749969f19d86563ceeea22bd3a69f7f1db85a8d1ac4bdaf0c6f56`;
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
13. `geometry` e `geography` XY, XYZ, XYM e XYZM come GeoArrow WKB con SRID
    e dimensioni preservati;
14. rifiuto fail-closed di SRID o profili dimensionali misti e di geography
    FullGlobe;
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
29. contratto comune `Provider` verificato dal testkit per connessione,
    capability, cataloghi, schemi, oggetti, describe e rifiuto ArcGIS, seguito
    da read/write completi attraverso il trait;
30. projection `id,label`, filtro TDS bindato `id >= 3`, ordinamento
    discendente e `TOP (2)` verificati sia come `ReadOperation` sia come
    `QueryOperation`, con risultato deterministico `[5, 4]`.
31. TDS bulk opt-in attraversato anche tramite il trait comune `Provider`, con
    capability pubblicata e conferma del conteggio server;
32. differenziale prepared/TDS bulk su 100 righe in quattro batch, con zero
    differenze bidirezionali;
33. duplicate key nel secondo request bulk con rollback confermato anche per
    le righe già finalizzate dal primo request.
34. round-trip bulk dei tipi ammessi (`bit`, interi, `real`/`float`,
    `decimal`, `nvarchar`, `varbinary`) con confronto SQL bidirezionale a zero
    differenze.
35. `QueryOperation` relazionale con CTE, inner join, `COUNT_BIG`,
    group-by/having e parametri TDS ripetibili;
36. funzione window `ROW_NUMBER`, `OFFSET/FETCH` e `UNION ALL`, con valori e
    ordinamento deterministici;
37. schema output derivato tramite
    `sys.dm_exec_describe_first_result_set` prima dell'esecuzione e verificato
    contro il primo `COLMETADATA` TDS, incluso un result set a zero righe;
38. decodifica nativa esatta di `decimal`, `date`, `time`, `datetime2`,
    `datetimeoffset`, `uniqueidentifier` e `xml`, con rifiuto fail-closed di
    proiezioni calcolate senza nome.
39. `update`, `upsert` e `delete_by_keys` parametrizzati con chiave univoca
    verificata da catalogo, contabilizzazione distinta insert/update/delete,
    chiavi assenti come `skipped`, rifiuto pre-mutation di target non univoci
    e rollback del primo batch se un vincolo fallisce nel batch successivo.
40. `create` atomico da schema Arrow con grammatica DDL chiusa, primary key
    esplicita, rifiuto del target esistente e nessun residuo lifecycle.
41. `replace` su staging con target originale leggibile durante il caricamento,
    lock/fingerprint/dipendenze ricontrollati alla pubblicazione e swap
    transazionale senza finestra di assenza.
42. rollback sia dopo errore nel caricamento sia dopo i rename pre-commit,
    cleanup verificato, FK entranti rifiutate e commit incerto pubblicato come
    `OutcomeUnknown` senza retry automatico.
43. `create` e `replace` sui 19 tipi del profilo di riferimento, inclusi
    decimal/money, temporali, UUID/XML, collation, geometry e geography, con
    dichiarazioni native rilette dal catalogo identiche a quelle sorgenti.
44. roundtrip WKB ISO Z, M e ZM per geometry e geography con metadati
    dimensionali e byte WKB preservati;
45. CA privata accettata con hostname matching, mismatch rifiutato, rotazione
    del certificato server con CA stabile e riconnessione verificata.
46. `QueryOperation` spatial tipizzata con WKB bindato e `STIntersects`,
    inclusi rifiuti pre-query di SRID e semantica geometry/geography
    discordanti.
47. indici `GEOMETRY_AUTO_GRID` e `GEOGRAPHY_AUTO_GRID` creati sia in
    `create` sia in staged `replace`, metadati di tassellazione e bounding box
    riletti dal catalogo, query geometry eseguita forzando l'access path
    pubblicato e create senza extent rifiutata con rollback completo.
48. quattordici metodi spatial nativi con output scalare eseguiti su
    `geometry` e `geography`: accessori, validazione, cinque predicati e
    misure; i predicati in projection restano `bit` nativi, mentre
    filtri, WKB bindato e preflight SRID/semantica restano nativi.
49. `StartPoint`, `EndPoint` e `PointN` su `geometry` e `geography` XYZM,
    convertiti con `AsBinaryZM()` e verificati byte per byte; semantica, SRID e
    dimensioni Arrow derivano dal profilo del risultato effettivo e il token
    strutturale della sorgente viene ricontrollato prima dello stream.
50. `Buffer`, `Intersection`, `Difference`, `SymDifference`, `Union` e
    `ConvexHull` eseguiti dal vivo su `geometry` e `geography`, con argomenti
    bindati, WKB risultante ispezionato e contratto Arrow del risultato
    verificato.
51. join spaziale fra due tabelle fisiche su colonne `geometry` e `geography`,
    con predicato colonna-colonna, output `Intersection`, alias risolti,
    semantica/SRID verificati su entrambi i lati e token strutturale conservato
    per ogni sorgente.
52. output e predicati spatial attraverso CTE non ricorsive, derived table e
    subquery non correlate, su `geometry` e `geography`; tipo nativo, SRID e
    token di tutte le tabelle fisiche sottostanti sono verificati, incluso il
    rifiuto live di un parametro con SRID divergente. La stessa fixture prova
    Point+Polygon nella medesima colonna per entrambe le semantiche e rende
    operativo il claim `mixed_geometry_types`.
53. catalogo avanzato su temporal system-versioned, graph node/edge e tabella
    partizionata: history table, colonne del periodo, tipo graph, partition
    scheme/function/column e numero di partizioni sono osservati. Il token
    strutturale cambia quando il versioning viene disabilitato senza modificare
    l'insieme delle colonne.
54. owner effettivo, predicato RLS schema-bound e permessi object/column
    `GRANT`/`DENY` sono osservati dal catalogo. Il token strutturale cambia
    quando la policy viene disabilitata senza modificare colonne o indici.
55. definizione, schema binding e opzioni `ANSI_NULLS`/`QUOTED_IDENTIFIER` di
    una view sono osservati. Il token cambia dopo `ALTER VIEW` a colonne
    invariate.
56. scope spatial ricorsivo e annidato su `geometry` e `geography`: CTE
    ricorsiva top-level, `UNION ALL`, `CROSS APPLY` e subquery correlata su
    chiave scalare con operando spatial locale. Una CTE dichiarata dentro una
    derived table viene rifiutata fail-closed perché SQL Server 2022 non ne
    ammette la sintassi.
57. locking query su una riga realmente contesa: `UPDLOCK,NOWAIT` produce
    errore server 1222 classificato `Timeout`, `retry=Never`, effetto `None`;
    dopo rollback la stessa query spatial legge la riga e `STDimension()`
    restituisce il valore atteso.

Comando della prova:

```text
cargo test -p plenora-db-sqlserver live_ -- --ignored --test-threads=1
```

Esito post-RC1: **41 superati, 0 falliti**. Il valore RC1 resta storicamente
**28/28** sulla revisione taggata e non viene riscritto retroattivamente.

## Limiti dell'evidenza

Questa prova non dimostra ancora:

- TDS bulk per spatial/UDT e per le modalità create/replace;
- CTE dichiarate dentro derived table, `OUTER APPLY`, riferimenti spatial
  esterni dentro subquery correlate, `SkipLocked` e forme calcolate senza
  alias deterministico;
- latenza finita e packet loss durante read e rollback;
- supporto lossless a `FullGlobe` (il rifiuto è provato);
- tipi `sql_variant`, CLR/UDT e famiglie non incluse nel profilo read;
- external table con data source e file format reali;
- altre build SQL Server e Azure SQL.

La suite combina fault deterministici interni e un proxy TCP capace di chiudere
materialmente entrambi i socket oppure mantenerli aperti senza inoltrare byte.
Le barriere sono legate alle fasi transazionali o a richieste osservate sul
server, non a sleep probabilistici. Gli hook restano compilati solo nei test e
non ampliano l'API pubblica.

Tali proprietà restano non pubblicizzate.

Il bulk non è una sostituzione implicita del codec prepared. La selezione usa
`SqlServerInsertMode::TdsBulk`; il preflight rifiuta input parziali, colonne
riordinate, temporali, `money`, `geometry`/`geography`, XML/UUID e varianti
native per cui Tiberius non espone una codifica bulk dimostrata. Ogni batch
Arrow viene validato e materializzato
in descrittori borrowed prima di aprire il request TDS; i descrittori sono
conteggiati nel budget memoria. Un errore non classificabile dopo l'avvio del
request mette la connessione in quarantena e non dichiara rollback.
