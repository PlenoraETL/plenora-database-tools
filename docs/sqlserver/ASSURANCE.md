# Assurance dedicata SQL Server

Il provider SQL Server dispone di un gate autonomo, separato dalla coverage
generale del workspace:

```powershell
python scripts\check_sqlserver_reference.py
```

La qualifica external table è separata perché richiede un'istanza con PolyBase
installato e la fixture `plenora_test.external_probe`:

```powershell
python scripts\check_sqlserver_polybase.py
```

Questo gate fallisce esplicitamente se `IsPolyBaseInstalled` non vale `1`; il
riferimento 2022 standard fissato per digest vale `0` e non produce quindi un
claim external artificiale.

Il gate usa SQL Server 2022 fissato per digest, verifica identità e stato del
server, esegue Clippy con warning negati, unit test e l'intera matrice live in
serie. La baseline post-RC1 aggiunge una CA privata, il controllo positivo e
negativo del nome host, la rotazione con impronta server diversa e CA stabile,
e il roundtrip lossless Z/M/ZM con rifiuto di profili misti e FullGlobe. Il
conteggio dei test live è un cricchetto deliberato: un'aggiunta o una rimozione
richiede l'aggiornamento esplicito del gate e della relativa evidenza.

In CI il risultato machine-readable, il log completo e le identità di
toolchain/container sono conservati per 90 giorni. Un fallimento conserva anche
i log Compose e lo stato dei container.

Il provider imposta sempre `EncryptionLevel::Required`. La fixture non forza
la cifratura per client estranei (`forceencryption = 0`): il gate qualifica la
policy della libreria e non pretende di qualificare l'hardening globale
dell'istanza SQL Server. La combinazione server-forced/Tiberius viene tenuta
fuori dal claim finché non dispone di una prova di interoperabilità separata.

## Scope dimostrato

- bootstrap TDS/TLS, pool bounded e recovery;
- read Arrow bounded, `geometry` e `geography` XY/XYZ/XYM/XYZM;
- prepared write e TDS bulk differenziale, incluso il profilo wire temporale
  `time`/`datetime2`/`datetimeoffset` e `uniqueidentifier`;
- update/upsert/delete-by-keys con chiavi univoche e conteggi distinti;
- QueryOperation relazionali ricche e schema dei risultati vuoti;
- 24 metodi AST spatial nativi comuni a `geometry` e `geography` su
  source fisiche singole o join fisici; nove output geometrici (`StartPoint`, `EndPoint`,
  `PointN`, `Buffer`, overlay booleani, `Union`, `ConvexHull`) escono come WKB
  Z/M-safe con contratto profilato sul risultato, argomenti WKB e numerici
  bindati, predicati projection mantenuti come `bit` nativo e rifiuto live di
  semantica, SRID o valori numerici non validi;
- join spatial fra colonne con risoluzione obbligatoria degli alias, verifica
  di semantica/SRID su entrambi gli operandi e token strutturale ricontrollato
  per ogni tabella coinvolta;
- CTE non ricorsive e ricorsive top-level, derived table, set operation,
  `CROSS APPLY` e subquery correlate su valori scalari con operando spatial
  locale, con
  descrizione autoritativa del tipo nativo, profilo SRID prima del predicato e
  token strutturale di ogni sorgente fisica sottostante;
- schema drift fail-closed;
- schema evolution additiva opt-in, senza mutazioni in prepare e con rollback
  congiunto di DDL e dati;
- rollback e outcome di commit incerto;
- create atomico e replace con staged swap transazionale;
- profilo create/replace completo sui 19 tipi scalari, temporali e spatial
  della fixture di riferimento;
- indici `GEOMETRY_AUTO_GRID` e `GEOGRAPHY_AUTO_GRID` creati durante
  create/replace, riletti da `sys.spatial_indexes` e
  `sys.spatial_index_tessellations`, con access path geometry forzato live;
- bounding box geometry derivato dopo il caricamento nella stessa transazione;
  dataset senza extent rifiutato con rollback e senza oggetti residui;
- rollback dello staging e dei rename, cleanup, dipendenze fail-closed e
  leggibilita del vecchio target durante il caricamento;
- taglio e blackhole fisici del trasporto TDS.
- catena TLS privata, hostname match/mismatch e rotazione;
- roundtrip WKB ISO di geometry/geography Z, M e ZM; rifiuto pre-stream di
  dimensioni miste e FullGlobe;
- lettura bounded di `CircularString`, `CompoundCurve` e `CurvePolygon` su
  geometry/geography e scrittura lossless di `CircularString`;
- colonne `geometry` e `geography` con tipi geometrici misti Point+Polygon,
  preservati come WKB e dichiarati `mixed` nel contratto Arrow;
- catalogo avanzato temporal, graph e partizionato, più view, owner, predicati
  RLS e permessi espliciti object/column; le proprietà semantiche entrano nel
  token strutturale;
- gate prestazionale separato su read, prepared, TDS bulk, create e replace.

## Gap non coperti dal gate v1

- SQL Server 2019, 2025 e Azure SQL;
- supporto lossless `FullGlobe`; Z/M/ZM e i tipi curvi pubblicati sono coperti
  come WKB ISO;
- CTE dichiarate dentro una derived table (rifiutate nativamente da SQL Server
  2022), `OUTER APPLY` e riferimenti spatial esterni dentro subquery correlate;
- esecuzione del gate PolyBase separato con data source e file format reali;
  il percorso `ET` e il feature probe sono implementati ma il riferimento
  standard dichiara esplicitamente la feature assente.

Questi gap non sono capability implicite: restano non pubblicizzati finché una
prova dedicata non viene aggiunta al gate.
