# PostgreSQL/PostGIS safety case

Stato: **profilo ingegneristico safety-critical, non certificato**.

Questo documento definisce gli invarianti applicati al provider
PostgreSQL/PostGIS. Non costituisce una certificazione DO-178C, ED-12C o
equivalente: una certificazione richiede autorità, processo organizzativo,
indipendenza della verifica, tool qualification e configurazione controllata
esterni a questo repository.

## Obiettivo

Per input non fidati, stato remoto incoerente, cancellazioni e guasti di rete,
la libreria deve:

- fallire in modo esplicito e classificato;
- non produrre SQL ambiguo o interpolare valori;
- non consumare risorse senza limite;
- non causare panic per dati Arrow, catalogo o risposte PostgreSQL;
- non dichiarare `Committed` quando l'esito non è dimostrabile;
- conservare la fedeltà di tipo, SRID e dimensionalità spaziale.

## Invarianti e prove

| ID | Pericolo controllato | Invariante | Evidenza automatica |
|---|---|---|---|
| PG-SC-001 | AST avversario esaurisce lo stack | visita iterativa, massimo 64 livelli e 4.096 nodi | test profondità/nodi in `plenora-database-core` |
| PG-SC-002 | query non booleana usata come filtro | filtro, join e having accettano solo predicati riconosciuti | test KNN non booleano |
| PG-SC-003 | SQL avanzato ambiguo | cardinalità funzioni, frame window, source, set operation, `DISTINCT ON`, offset e locking validati fail-closed | test negativi core e golden renderer |
| PG-SC-004 | parametro interpolato nel SQL | tutti i valori restano bind ordinali | test ordine bind SQL |
| PG-SC-005 | EWKB incompatibile corrompe una write | header, byte order, tipo, XY/XYZ/XYM/XYZM e SRID verificati prima della write | test contratto EWKB |
| PG-SC-006 | mapping temporale causa panic | estremi Arrow producono `DataMapping` | test Date32/Timestamp MIN/MAX |
| PG-SC-007 | memoria non bounded | batch, cella WKB, pool, cache e AST hanno limiti espliciti | gate hardening e fault matrix |
| PG-SC-008 | sessione contaminata o incerta viene riusata | reset singolo e invalidazione dopo cancellazione/esito incerto | test pool/cancellation live |
| PG-SC-009 | poisoning rende il provider indisponibile | pool e cache recuperano `PoisonError` | test poisoning intenzionale |
| PG-SC-010 | schema cache obsoleta | token OID e fingerprint includono enum, domain, collation e generated | test DDL esterno |
| PG-SC-011 | indice spaziale atteso non usato | bbox/KNN tipizzati e `EXPLAIN` deve osservare GiST | test live e gate spatial performance |
| PG-SC-012 | introspezione incompleta | output include constraint, indici/opclass, identity/generated, enum/domain, partizioni, viste materializzate, RLS, policy e ACL | fixture avanzata e test live |
| PG-SC-013 | commit ambiguo dichiarato certo | outcome `Unknown` con recovery esplicita dopo perdita di certezza | fault injection commit |
| PG-SC-014 | codice non verificabile introdotto | `unsafe_code = "forbid"` e Clippy warnings-as-errors | gate workspace |
| PG-SC-015 | retry pericoloso dopo un effetto remoto incerto | errore a quattro assi; `Unknown` è effetto e impone recovery | test error envelope e outcome |
| PG-SC-016 | polling o race nella cancellazione | token concreto, wake senza polling, registrazione check-register-recheck | test token, cancellazione live e recovery pool |
| PG-SC-017 | CRS/tipo PostGIS reinterpretato durante il passaggio Arrow | metadata canonici, versione schema e divergenza legacy rifiutata | test metadata e suite PostGIS live |
| PG-SC-018 | cancellazione di write dichiarata senza conoscere il rollback | `RolledBack` solo dopo conferma; altrimenti `Unknown` e recovery | test semantico e fault injection live |
| PG-SC-019 | limiti applicati dopo l'allocazione o aggirati cambiando contesto | budget unico obbligatorio, lease atomiche reserve/commit e identità verificata | test core, test sostituzione e budget righe live |
| PG-SC-020 | esaurimento risorse a metà transazione dichiarato privo di effetti | rollback esplicito; `RolledBack` solo se confermato, altrimenti recovery | test semantico resource failure e suite write live |
| PG-SC-021 | conteggi EWKB avversari causano ricorsione, allocazioni o traversal illimitato | scanner iterativo, stack limitato dalla profondità e contatore componenti checked | test EWKB bomb core e budget geometrico live |
| PG-SC-022 | SRID reinterpretato come CRS diverso durante una write | authority ID risolto da `spatial_ref_sys`, incluso nel token schema e verificato prima del preflight | assert metadata live e test negativo CRS mismatch |
| PG-SC-023 | `duration_ms` dichiarato ma non operativo, o timeout che lascia effetti remoti ambigui | deadline monotona attiva; cancel backend; rollback verificato, commit timeout `Unknown` | test deadline token/budget, read backend e write rollback live |
| PG-SC-024 | errore SQL, trigger, producer o DDL dopo `BEGIN` affidato al drop implicito | ogni uscita fallibile pre-commit consuma la transazione con rollback esplicito; execution ID nell'errore | trigger failure e fault before-commit live |
| PG-SC-025 | parametro testuale avversario causa panic o coercizione ambigua | UUID decodificato solo da byte ASCII esadecimali; decimal richiede una grammatica non vuota e deterministica | test negativi UTF-8, segno isolato, punti multipli e caratteri non ASCII |
| PG-SC-026 | drift dell'API o capability contraddittorie viene scoperto soltanto dal chiamante | freeze compile-time degli export v0.1 e suite comune fail-closed su connessione, capability, inspection, cancellation e unsupported | `public_api_v0_1.rs` e `verify_provider_contract` live |
| PG-SC-027 | una primitiva interna viene pubblicizzata come funzione disponibile | capability opzionali vere solo con superficie pubblica e prova; catalogo spatial, nomi wire e schema JSON devono coincidere esattamente | assert capability live, test wire/catalogo e `phase0_validate.py` |

## Regole di cambiamento

Ogni nuova funzione o variante pubblica deve:

1. essere presente nel catalogo versionato;
2. dichiarare cardinalità e posizione degli argomenti geometrici;
3. avere un test positivo e uno negativo quando esistono combinazioni invalide;
4. superare il renderer deterministico senza SQL raw;
5. superare PostgreSQL/PostGIS live se modifica il provider;
6. aggiornare questa matrice se introduce un nuovo pericolo.

Una modifica al percorso read/write non può aggiornare una baseline
prestazionale se prima non supera correttezza, hardening e fault injection.

## Gate

```powershell
python scripts\check_pre_database.py
python scripts\check_postgres_reference.py
python scripts\check_postgres_hardening.py
python scripts\check_postgres_spatial_performance.py
python scripts\check_postgres_matrix.py
```

Il gate spatial esegue 50 campioni su 100 righe, verifica l'indice GiST nel
piano e applica budget fail-closed a mediana e p95.

## Limiti residui

- dipendenze Rust, kernel, runtime container e server PostgreSQL non sono
  formalmente verificati dal progetto;
- i servizi PostgreSQL gestiti richiedono campagne separate su privilegi,
  failover, TLS ed estensioni;
- la validazione EWKB controlla il contratto dell'header; la validità
  topologica completa resta responsabilità di `ST_IsValid`/`ST_MakeValid`;
- Raster, Topology e SFCGAL non fanno parte di questo profilo;
- replica logica, WAL e amministrazione cluster non fanno parte del data path.

Questi limiti devono essere esposti come capability o dichiarati fuori
perimetro; non sono ammessi fallback silenziosi.
