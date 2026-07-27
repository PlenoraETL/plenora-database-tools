# Decisioni aperte che richiedono i target

Queste decisioni non devono essere chiuse prima di avere accesso ai database o
alle istruzioni di provisioning concordate con Marco.

| ID | Decisione | Evidenza richiesta | Output |
|---|---|---|---|
| DB-01 | client/runtime PostgreSQL | cursor, COPY, cancel, PostGIS EWKB | ADR driver PostgreSQL |
| DB-02 | protocollo bulk MySQL/MariaDB | prepared batch, LOAD DATA policy, geometry SRID | ADR driver MySQL |
| DB-03 | client SQL Server | streaming TDS, bulk copy, geometry/geography | ADR driver SQL Server |
| DB-04 | Oracle client e packaging | array binding, LOB, SDO_GEOMETRY, redefinition | ADR driver Oracle |
| DB-05 | Db2 client e packaging | CLI/native libs, bulk, ST_Geometry | ADR driver Db2 |
| DB-06 | SQLite/SpatiaLite | extension loading policy, transaction/replace | ADR driver SQLite |
| DB-07 | DuckDB Spatial | extension availability, Arrow zero-copy, write spatial | ADR driver DuckDB |
| GIS-01 | ArcGIS matrix | Online/Enterprise version, pagination, applyEdits, GlobalID | ADR provider ArcGIS |
| PERF-01 | batch defaults | throughput/RSS/p95 su LAN e caso realistico | profili default |

## Informazioni da raccogliere al momento del gate

Per ciascun target:

- prodotto, edizione, versione e architettura;
- estensione spatial e versione;
- endpoint isolato e credenziali con privilegi di test;
- limiti di rete/proxy/TLS;
- possibilità di creare schema/database/service temporanei;
- modalità di cleanup;
- versioni minime e massime che Plenora deve supportare.

I segreti non entrano nei manifest: verranno forniti tramite variabili
d’ambiente o secret resolver. Nessun database viene installato o avviato in
questa fase.
