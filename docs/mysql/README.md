# Provider MySQL

Baseline di riferimento: MySQL 8.4 LTS con matrice MySQL 8.0 ancora aperta. Il provider usa
protocollo nativo asincrono e TLS rustls; MariaDB resta fuori scope fino a una
qualifica indipendente.

## Stato qualificato

- configurazione strutturata e credenziali redatte;
- TLS obbligatorio; CA privata isolata dal server, rigenerazione completa su
  artefatti parziali e hostname DNS/IP positivo/negativo provati live;
- budget connect/operazione/acquire configurabili; il checkout acquisisce prima
  un permit del semaforo di pool entro `acquire_timeout`, poi attende la
  connessione entro `connect_timeout`;
- bootstrap UTC, strict SQL mode e autocommit deterministico;
- bootstrap applicato sia all'apertura sia dopo il reset della connessione;
- quarantena su timeout, cancellazione ed errori fatali di trasporto;
- errori pubblici redatti e outcome write ambiguo predisposto;
- introspezione e lettura Arrow streaming bounded provate live, incluso drop
  anticipato del consumer con cleanup bounded;
- `GEOMETRY -> mixed` e tipi spatial concreti, inclusi `POINT` e
  `GEOMETRYCOLLECTION -> exact`, validati contro il contratto canonico;
- query relazionale e scritture restano fail-closed e non sono capability
  pubblicate.

Il gate riproducibile è `python scripts/check_mysql_reference.py`. Esegue fmt,
Clippy con warning negati, dodici test fixture, 34 test offline e dodici test live
identificati per nome sul riferimento fissato per digest. La matrice comparativa è in
[`docs/PROVIDER-MATURITY-MATRIX.md`](../PROVIDER-MATURITY-MATRIX.md).

Il DDL atomico MySQL non viene equiparato a DDL transazionale. Create/replace
richiederanno un lifecycle specifico e una campagna di recovery dedicata.
