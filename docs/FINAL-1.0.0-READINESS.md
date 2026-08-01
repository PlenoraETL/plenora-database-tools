# Readiness della metadata candidate 1.0.0

**Data:** 2026-08-01  
**Stato:** pre-tag; nessun commit, tag, merge o publish autorizzato

La baseline funzionale `16248a904f19062403ea3f0215e5c7b620ed9b72` ha
superato otto workflow same-SHA: PostgreSQL/PostGIS, MySQL 8.4, SQL Server,
coverage con i tre riferimenti, EWKB fuzz, le due matrix e Release manifest.
Il bump versione e i nuovi record di assurance costituiscono però un nuovo diff
e richiedono una CI completa sulla propria revisione prima di qualsiasi tag.

## Perimetro provider 1.0.0

- PostgreSQL 16/PostGIS 3.4: profilo di riferimento read/write/spatial;
- SQL Server 2022: profilo qualificato secondo la maturity matrix;
- MySQL 8.4 LTS: superficie stabile e verificata **read-only**, con TLS,
  catalogo, streaming bounded, spatial XY, pooling/reset, timeout,
  cancellazione e quarantena;
- MySQL query relazionale, `prepare_write` e `write`: `Unsupported` fail-closed;
- MySQL 8.0, MariaDB, Z/M/ZM e Azure SQL: nessuna compatibilità implicita.

Questa scelta non dichiara parità funzionale fra provider. Dichiara una
superficie MySQL deliberatamente limitata e verificata.

## Provenienza Contracts

Il candidato cita `plenora-contracts` alla revisione esatta
`e81c3ce7941bacbdb0e083f03c512ae22a6b924a`, versione documentale
`2.0-rc16`, senza inventare un tag. R4.3.2 resta proposta ed è adottata dal
componente come policy più stretta; `system_rc` resta falso.

## Immutabilità storica

Restano invariati:

- `release/rc1-readiness.json`;
- `release/development.json`;
- `docs/RC1-READINESS.md`;
- tag ed evidenze `v0.1.0-rc.1`.

Il checker RC1 e il relativo test ricevono soltanto la separazione necessaria
fra verifica della candidate corrente e verifica del tag RC1 immutabile: un
record `component_rc_tagged` non viene confrontato con la versione del checkout
post-RC, mentre una candidate pre-tag continua a fallire chiuso sul drift.

## Gate prima del tag

1. checker storico RC1 ancora verde;
2. checker finale e mutation test verdi;
3. workspace e lockfile coerenti a `1.0.0`;
4. otto workflow same-SHA nuovamente verdi sul commit metadata;
5. autorizzazione owner separata per eventuale tag `v1.0.0`.
