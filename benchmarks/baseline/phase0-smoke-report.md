# Report aggregato Fase 0

Generato: 2026-07-27T01:19:17.316189+00:00

| Caso | Provider | Campioni | Pass/Fail | Mediana ms | p95 ms | Summary stabile |
|---|---|---:|---:|---:|---:|---:|
| arcgis.connection.portal | arcgis | 1 | 1/0 | 28.832 | 28.832 | sì |
| arcgis.introspection.layer | arcgis | 1 | 1/0 | 16.809 | 16.809 | sì |
| arcgis.read.features | arcgis | 1 | 1/0 | 15.949 | 15.949 | sì |
| arcgis.write.apply_edits | arcgis | 1 | 1/0 | 3.538 | 3.538 | sì |
| backend.static_inventory | backend-python | 1 | 1/0 | 173.558 | 173.558 | sì |
| postgres.connection.version | postgres | 1 | 1/0 | 5.258 | 5.258 | sì |
| postgres.fixture.preflight | postgres | 1 | 1/0 | 1.674 | 1.674 | sì |
| postgres.introspection.columns | postgres | 1 | 1/0 | 3.341 | 3.341 | sì |
| postgres.read.events_stream | postgres | 1 | 1/0 | 38.365 | 38.365 | sì |
| postgres.spatial.ewkb_read | postgres | 1 | 1/0 | 0.702 | 0.702 | sì |

## Totali

- casi: 10;
- campioni: 10;
- passati: 10;
- falliti: 0;
- summary instabili: 0.

Il p95 usa il metodo nearest-rank.
