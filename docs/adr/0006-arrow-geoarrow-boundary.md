# ADR 0006 — Confine Arrow e GeoArrow-WKB

Stato: **accettato con verifica dipendenze rinviata**  
Data: 2026-07-27

## Contesto

La black box deve scambiare batch senza dipendere da DataFrame Python e deve
supportare geometrie di tutti i provider.

## Decisione

Il confine dati pubblico v1 è:

- stream bounded di Apache Arrow `RecordBatch`;
- geometria fisica `Binary`/`LargeBinary` con metadata GeoArrow-WKB;
- schema invariato per stream, salvo protocollo esplicito di schema evolution;
- limiti sia per righe sia per byte;
- ownership dei buffer esplicita, senza mantenere connessioni dentro i batch.

La versione Arrow sarà unica in tutto il workspace e resa visibile nel
manifest di build. Il candidato iniziale è la stessa major già usata negli
altri progetti Plenora locali; la versione esatta verrà confermata durante lo
scaffold con un test di compatibilità IPC/C Data Interface.

EWKB o formati nativi sono dettagli del driver. Prima di attraversare il
confine pubblico vengono normalizzati in WKB più metadata SRID/CRS. Il
round-trip nativo può conservare metadata aggiuntivi, ma non cambiare il
contratto base.

## Conseguenze

- backpressure e memoria sono misurabili;
- PostGIS, Oracle Spatial, Db2 Spatial e ArcGIS convergono sullo stesso
  contratto geometrico;
- l’upgrade Arrow richiede test ABI/IPC e non è una modifica invisibile;
- Z/M, empty, null e SRID hanno casi golden dedicati.

## Alternative scartate

- GeoJSON come formato interno;
- oggetti geometry Rust come unica API pubblica;
- dipendenza diretta dai DataFrame Python.
