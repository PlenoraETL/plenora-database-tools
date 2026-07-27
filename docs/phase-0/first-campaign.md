# Prima campagna di caratterizzazione

Data: 2026-07-26  
Stato: **smoke completato**

## Ambiente

- Windows 11;
- Python 3.12.3;
- Docker Desktop;
- PostgreSQL 17.5;
- PostGIS 3.5.2;
- fake REST ArcGIS del backend Plenora;
- dataset `test-e2e-v2`.

I valori temporali sono un singolo smoke locale, non ancora una baseline
statistica.

## Inventario statico

Risultato:

- 91 file Python nei moduli database/provider selezionati;
- 787 file `test_*.py` nel backend;
- digest simboli:
  `0f16115738a88783021860059d5cce793c4f4562b13c91e2be81f612a796f0a4`.

Raw: `benchmarks/raw/phase0-inventory.jsonl`.

## PostgreSQL/PostGIS

| Caso | Risultato | Tempo smoke |
|---|---:|---:|
| connection + version probe | PostgreSQL 17.5, PostGIS 3.5.2 | 5,258 ms |
| introspezione | 12 tabelle, 73 colonne | 3,341 ms |
| fixture preflight | 10.000 eventi, 18 geometrie | 1,674 ms |
| read server-side cursor | 10.000 righe, 10 fetch × 1.000 | 38,365 ms |
| spatial EWKB | 18 geometrie, 450 byte, SRID 4326 | 0,702 ms |

Digest:

- introspezione:
  `c0412f6eda09046f28f4be4d7aa68538ad7a4686b9ce852c900fba9c6b8c19ab`;
- righe:
  `9c8aa77ce526b91b7ee9c4bed897a2da7c673b000b510adb9101a09b34fc7220`;
- EWKB:
  `eca0a484eab61d63193940cdf1688f98012e395110e9f36d3adc426fcf516818`.

Raw: `benchmarks/raw/phase0-postgres-smoke.jsonl`.

### Problema ambientale trovato

Il volume E2E preesistente conteneva PostGIS ma non le fixture applicative.
Inoltre `postgres/init/01_load.sh` aveva fine-riga Windows e non era eseguibile
direttamente con `sh` nel container. La campagna ha normalizzato lo script in
memoria e ricaricato esclusivamente il database fixture `plenora_test`.

È stato quindi aggiunto un controllo preflight dell'harness:

- verificare tabella e row count attesi;
- fallire con `fixture_not_ready`;
- non includere run incomplete nella baseline.

## ArcGIS fake REST

| Caso | Risultato | Tempo smoke |
|---|---:|---:|
| portal probe | portale e utente presenti | 28,832 ms |
| layer introspection | 5 campi, Point, ObjectID | 16,809 ms |
| read features | 5 feature, nessun transfer limit | 15,949 ms |
| applyEdits | add riuscito, count temporaneo 6 | 3,539 ms |

Lo stato è stato resettato a fine caso.

Digest:

- layer:
  `6915fa20c9f459a5fd7352fcaad27bf6178d9277cddcfc6cc16e72bca0939154`;
- feature:
  `3c61a5ed25a98e0d799203dfd252fe75ff92d02958fb4073a1a6c90b53841f0b`.

Raw: `benchmarks/raw/phase0-arcgis-smoke.jsonl`.

## Test Python caratterizzati

Due gruppi, 95 test totali, tutti passati:

1. 50 test:
   - SQL injection guard DDL;
   - stage/swap;
   - schema PostgreSQL;
   - protocollo provider ArcGIS;
   - REST write ArcGIS.
2. 45 test:
   - uncertain recovery;
   - unsafe append gate;
   - batch loop;
   - catena upsert;
   - promozione Oracle CLOB;
   - credential leak nei logger.

Warning non bloccanti:

- secret JWT random perché l'ambiente di test non configura quello applicativo;
- installazioni CuPy multiple nell'ambiente Python host, non coinvolte nei casi.

## Decisioni confermate

- ArcGIS deve restare provider distinto dal SQL.
- Cursor PostgreSQL server-side richiede transazione anche per letture.
- Il controllo fixture precede qualunque misura.
- Digest e conteggi sono necessari oltre al tempo.
- Gli outcome edit ArcGIS devono essere per-feature.
- I test esistenti costituiscono già un oracle sostanziale per Fase 0.

## Prossimo incremento

1. ripetizioni/warmup e report mediana/p95;
2. benchmark Python backend, non solo driver smoke;
3. avviare MySQL, SQL Server e Oracle del profilo cross-db;
4. caratterizzare paginazione/rate limit/errore parziale ArcGIS;
5. produrre golden manifest per type mapping e write modes.
