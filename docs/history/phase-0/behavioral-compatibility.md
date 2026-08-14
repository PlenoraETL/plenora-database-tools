# Compatibilità comportamentale Python ↔ Rust

## 1. Obiettivo

Il port non confronta dettagli di implementazione. Confronta:

- schema e metadata;
- righe e valori;
- ordering dichiarato;
- tipi e precisione;
- geometrie e CRS;
- conteggi;
- mutazioni remote;
- stato transazionale;
- error category e redazione;
- risorse/staging rimasti.

Ogni caso ha un manifest:

```yaml
id: write.postgres.upsert.decimal_geometry
source: python-backend
classification: normative
providers: [postgres]
operation: upsert
fixture: fixtures/mixed_types_v1
comparison:
  rows: unordered_by_key
  decimal: exact
  geometry: semantic
  outcome: committed
```

## 2. Classificazione

### Normativo

Preservare:

- write mode e precondizioni;
- identificatori quotati;
- valori bindati;
- test connessione sanitizzati;
- read bounded;
- decimal senza conversione float;
- append per-batch non sicuro solo con opt-in;
- conflitti append `skip/force/abort`;
- transazione all-or-nothing vs per-batch;
- stage/swap e Oracle redefinition come strategie esplicite;
- indice spatial quando richiesto e supportato;
- ArcGIS auth/discovery/read/write/multi-layer.

### Da correggere

Il Rust diventa il nuovo comportamento normativo:

- SRID sconosciuto non diventa 4326;
- tipi sconosciuti non diventano silenziosamente stringa;
- precisione/scala/timezone restano nel contratto;
- nessun errore pubblico contiene `str(e)` non redatto;
- outcome dopo richiesta commit/edit può essere `Unknown`;
- batch è limitato in byte oltre che righe;
- ArcGIS risposta edit persa non viene automaticamente ritentata;
- geometry/geography e Z/M sono espliciti.

### Legacy compatibile

- alias `postgresql → postgres`;
- nomi dei write mode;
- schema default esistenti solo tramite migrazione esplicita;
- tipo canvas semplificato `int/real/str/bool/date/geometry`;
- lowercase Oracle solo come opzione legacy;
- vecchi campi `source/dest` dei column mapping.

## 3. Golden dataset

### `empty_v1`

- schema non vuoto, zero righe;
- verifica create/no-op/append/read.

### `scalars_v1`

- min/max interi;
- unsigned quando disponibile;
- decimal con scale 0, 2, 18 e precisione alta;
- float, `NaN`, infinito dove supportato;
- bool;
- null;
- Unicode, emoji, combining characters, NUL policy;
- binary;
- UUID;
- JSON;
- date/time/timestamp con timezone e DST boundary.

### `wide_rows_v1`

- 128 colonne;
- nomi riservati;
- spazi, quote/backtick/bracket;
- case collision;
- nomi vicini ai limiti vendor;
- duplicate output names da query.

### `geometry_v1`

- Point, LineString, Polygon con hole;
- MultiPoint/MultiLineString/MultiPolygon;
- GeometryCollection;
- empty e null;
- endian LE/BE;
- SRID noto/sconosciuto/mismatch;
- XY/XYZ/XYM/XYZM;
- geometria invalida;
- payload grande.

### `keys_conflicts_v1`

- chiavi uniche;
- duplicati interni;
- conflitti remoti;
- null nelle chiavi;
- composite key;
- conflict rate 0/50/100%.

### `arcgis_features_v1`

- ObjectID e GlobalID;
- domain coded/range;
- subtype;
- Point/Multipoint/Polyline/Polygon;
- wkid/latestWkid/WKT;
- Z/M;
- feature con errore parziale;
- più layer;
- pagina vuota/intermedia/finale.

## 4. Confronti

### Valori tabellari

- integer/decimal: esatti;
- float: bitwise quando possibile, altrimenti policy NaN/tolleranza dichiarata;
- timestamp: istante + timezone semantics;
- string/binary: esatti;
- JSON: semantico se il database normalizza.

### Geometrie

Non confrontare WKB byte per byte. Confrontare:

- tipo;
- empty/null;
- coordinate e dimensioni;
- CRS/SRID;
- equivalenza topologica o strutturale secondo operazione;
- tolleranza solo se il provider introduce una conversione documentata;
- `LossReport`.

### Ordering

- query con `ORDER BY`: sequenza esatta;
- senza `ORDER BY`: multiset/chiave, nessuna promessa;
- ArcGIS: ordine solo se `orderByFields` supportato e richiesto;
- per-batch non cambia la semantica globale.

### Outcome

Categorie canoniche:

- `Committed`;
- `RolledBack`;
- `PartiallyCommitted`;
- `OutcomeUnknown`;
- read complete/incomplete;
- ArcGIS per-feature/per-layer results.

Il vecchio `WriteStatus` viene mappato senza perdere informazione; quando il
Python non distingue un caso, il manifest lo marca `legacy_ambiguous`.

## 5. Fault matrix minima

Per ogni write:

1. errore prima della connessione;
2. errore nel preflight;
3. errore prima del primo batch;
4. errore a metà batch;
5. errore tra batch;
6. errore durante finalizzazione;
7. connessione persa prima di commit/edit;
8. connessione persa dopo invio commit/edit;
9. rollback fallito;
10. cleanup fallito.

Verificare:

- righe remote;
- oggetti staging/layer;
- outcome;
- retryability;
- nessun segreto;
- sessione non riusata se incerta.

ArcGIS aggiunge:

- token scaduto;
- HTTP 429;
- risposta `success=false` per una feature;
- `rollbackOnFailure` ignorato/non supportato;
- pagina duplicata/mancante;
- timeout dopo body edit inviato;
- service creato ma item response persa.

## 6. Oracle differenziale

Il runner Python e quello Rust eseguono la stessa fixture su un target isolato.
L'oracle è lo stato remoto normalizzato, non il formato di risposta interno.

```text
setup fixture
  → snapshot before
  → execute Python
  → inspect state/outcome
  → reset
  → execute Rust
  → inspect state/outcome
  → semantic diff
```

Per test distruttivi ogni run usa schema/database/service dedicato e nomi
derivati dal test ID.

## 7. Manifest risultati

Output JSON stabile:

```json
{
  "schema_version": 1,
  "case_id": "write.postgres.append.scalars",
  "implementation": "python",
  "provider": "postgres",
  "server_version": "...",
  "contract": {},
  "outcome": {},
  "rows_digest": "...",
  "geometry_digest": "...",
  "remote_state": {},
  "redaction_passed": true
}
```

Il digest accelera il confronto ma non sostituisce il diff dettagliato quando
fallisce.

