# Readiness della metadata candidate 1.1.0

La candidate `1.1.0` introduce la superficie MySQL relazionale e write già
qualificata sul commit funzionale
`ee4fa7470d48f152aa40d86fb911e780bc75a908`. Questo documento non autorizza il
tag: il commit metadata deve ancora superare CI same-SHA e le catene
cross-library PostgreSQL/MySQL devono essere rieseguite sui nuovi artefatti.

## Perimetro MySQL qualificato

- MySQL `8.0.46` e `8.4.11`, immagini fissate per digest;
- TLS obbligatorio con CA privata e verifica hostname sul data path;
- query relazionale e lifecycle prepare/query;
- scrittura `Append` e `SingleTransaction` con transazione unica sullo stream;
- rollback per errori certi e quarantine per outcome ambiguo;
- `GEOMETRY`/WKB XY con SRID; Z, M e ZM fail-closed;
- `LOCAL INFILE` disabilitato e massimo `65535` placeholder;
- drain completo, readback esatto e differenziale zero nei gate qualificati.

MariaDB, geography, spatial index e dimensioni superiori non sono dedotti come
supportati.

## Evidence base same-SHA

Sette workflow sul commit funzionale esatto sono passati:

- Release manifest: `30744606166`;
- MySQL assurance: `30744606163`;
- MySQL 8.0/8.4 version matrix: `30746069178`;
- Workspace coverage: `30746112820`;
- EWKB parser fuzzing: `30746113685`;
- SQL Server assurance: `30744606173`;
- PostgreSQL/PostGIS assurance: `30744606189`.

Queste evidenze non si trasferiscono automaticamente al futuro commit metadata.
Il workflow `Release manifest` valida sia il record storico `1.0.0` sia
`release/1.1.0.json`; i workflow provider devono poi passare sul commit effettivo
di `main` destinato al tag.

## Blocchi prima del tag

1. CI PR e post-merge same-SHA del commit metadata `1.1.0`;
2. release Data Tools e IO Tools candidate qualificate;
3. catene PostgreSQL/MySQL Database → Data → IO sui nuovi artefatti;
4. comparativa Plenora separata, senza modificare Plenora prima del freeze delle
   tre librerie.

La candidate mantiene il claim conservativo `verified_internally`: le review
esterne Claude/Kimi guidano il gate pre-commit, ma non vengono promosse a una
certificazione autoreferenziale dentro il commit non ancora esaminato. I claim
`system_rc` e certificazione avionica restano falsi.
