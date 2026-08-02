# Matrice di maturità dei provider di riferimento

Questa matrice descrive il codice corrente e i gate riproducibili. Non sostituisce
il manifesto di release e non estende implicitamente le capability pubblicate da
`probe_capabilities`.

Legenda:

- **live**: comportamento esercitato contro il database di riferimento;
- **offline**: coperto da test senza database;
- **fail-closed**: capability non pubblicata e chiamata rifiutata;
- **aperto**: necessario per la parità indicata.

## Riferimenti

| Provider | Riferimento | Gate |
| --- | --- | --- |
| PostgreSQL/PostGIS | PostgreSQL 16 / PostGIS 3.4 | `python scripts/check_postgres_reference.py` |
| SQL Server | SQL Server 2022, compatibility level 160, immagine fissata per digest | `python scripts/check_sqlserver_reference.py` |
| MySQL | MySQL 8.0.46 e 8.4.11, immagini fissate per digest | `python scripts/check_mysql_matrix.py` e `python scripts/check_mysql_reference.py` |

MariaDB non è dedotto come compatibile con MySQL e non appartiene al riferimento
MySQL.

## Capability e assurance

| Area | PostgreSQL/PostGIS | SQL Server | MySQL 8.4 LTS |
| --- | --- | --- | --- |
| Connessione e identità server | live | live | live |
| TLS sul data path | live | live, inclusa CA privata/hostname/rotazione | live; CA privata, hostname positivo/negativo e `require-secure-transport=ON` |
| Pool bounded | live | live | live |
| Bootstrap dopo reset | live | live | live; stessa `CONNECTION_ID()` dopo reset, `setup` riapplicato |
| Timeout acquire/connect | live | live | offline/live; lease acquire e apertura connessione hanno budget distinti |
| Timeout operazione/deadline e quarantena | live | live | live; envelope `Timeout` distinto da cancellazione richiesta |
| Cancellazione in-flight e quarantena | live | live | live |
| Redazione credenziali | offline/live | offline/live | offline |
| Catalogo e describe | live | live | live |
| Lettura Arrow bounded/streaming | live | live | live; allocazione pre-bounded e drop anticipato con cleanup bounded |
| Projection/filter/order/limit bind-safe | live | live | live |
| Query relazionale pubblica | live | live | live; lifecycle prepare/query e drain completo |
| Scrittura | live | live | live; Append e SingleTransaction, rollback o quarantine |
| Tipi scalari di riferimento | live, profilo esteso | live, profilo esteso | live: integer, decimal, bool, UTF-8, binary, date, datetime, JSON |
| Spatial generico | live | live | live: `GEOMETRY -> mixed` |
| Spatial tipizzato | live | live | live: `POINT` e `GEOMETRYCOLLECTION -> exact` |
| Dimensioni spatial | XY/XYZ/XYM/XYZM secondo gate | geometry/geography secondo gate | XY; Z/M/ZM non pubblicate |
| Contratto schema canonico | offline/live | offline/live | offline/live tramite `validate_schema_contract` |
| Gate fmt + Clippy `-D warnings` | sì | sì | sì |
| Gate live corrente | suite reference | 44 test live attesi | 23 test live attesi per nome su 8.0.46 e 8.4.11 |

## Esito di parità

MySQL ha raggiunto disciplina di assurance e parità per le capability pubblicate:
connessione TLS, introspezione, query relazionale, lettura streaming bounded,
scrittura Append/SingleTransaction, tipi dichiarati, spatial XY/SRID, reset,
timeout, cancellazione, rollback e quarantena. La matrice live copre sia 8.0.46
sia 8.4.11.

La superficie non deduce capability non provate: Z, M e ZM continuano a fallire
chiuso; MariaDB non è qualificato; geography e spatial index non sono pubblicati.
Questi limiti non possono essere nascosti tramite feature flag né descritti come
supportati.

## Decisione della metadata candidate 1.1.0

La minor release espone la nuova superficie MySQL relazionale e write già
qualificata live. Non dichiara equivalenza oltre il profilo pubblicato e non
promuove claim di sistema: le catene cross-library PostgreSQL/MySQL restano un
gate separato prima del tag.
