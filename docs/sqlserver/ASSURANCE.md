# Assurance dedicata SQL Server

Il provider SQL Server dispone di un gate autonomo, separato dalla coverage
generale del workspace:

```powershell
python scripts\check_sqlserver_reference.py
```

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
- prepared write e TDS bulk differenziale;
- update/upsert/delete-by-keys con chiavi univoche e conteggi distinti;
- QueryOperation relazionali ricche e schema dei risultati vuoti;
- ventitré metodi AST spatial nativi comuni a `geometry` e `geography` su
  source fisiche singole o join fisici; nove output geometrici (`StartPoint`, `EndPoint`,
  `PointN`, `Buffer`, overlay booleani, `Union`, `ConvexHull`) escono come WKB
  Z/M-safe con contratto profilato sul risultato, argomenti WKB e numerici
  bindati, predicati projection mantenuti come `bit` nativo e rifiuto live di
  semantica, SRID o valori numerici non validi;
- join spatial fra colonne con risoluzione obbligatoria degli alias, verifica
  di semantica/SRID su entrambi gli operandi e token strutturale ricontrollato
  per ogni tabella coinvolta;
- CTE non ricorsive, derived table e subquery spatial non correlate con
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
- colonne `geometry` e `geography` con tipi geometrici misti Point+Polygon,
  preservati come WKB e dichiarati `mixed` nel contratto Arrow;
- catalogo avanzato temporal, graph e partizionato, più view, owner, predicati
  RLS e permessi espliciti object/column; le proprietà semantiche entrano nel
  token strutturale;
- gate prestazionale separato su read, prepared, TDS bulk, create e replace.

## Gap non coperti dal gate v1

- SQL Server 2019, 2025 e Azure SQL;
- supporto lossless `FullGlobe`; Z/M/ZM sono coperti come WKB ISO;
- CTE spatial ricorsive o annidate in derived table, subquery spatial
  correlate, lateral/APPLY e set operation spatial;
- catalogo external table con data source e file format reali.

Questi gap non sono capability implicite: restano non pubblicizzati finché una
prova dedicata non viene aggiunta al gate.
