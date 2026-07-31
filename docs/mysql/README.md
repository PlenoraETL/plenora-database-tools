# Provider MySQL

Baseline in costruzione: MySQL 8.4 LTS con matrice MySQL 8.0. Il provider usa
protocollo nativo asincrono e TLS rustls; MariaDB resta fuori scope fino a una
qualifica indipendente.

## Stato iniziale

- configurazione strutturata e credenziali redatte;
- TLS obbligatorio, verifica certificato predefinita;
- timeout di connect/operazione/acquire separati;
- bootstrap UTC, strict SQL mode e autocommit deterministico;
- quarantena su timeout, cancellazione ed errori fatali di trasporto;
- errori pubblici redatti e outcome write ambiguo predisposto;
- nessuna capability read/write/spatial dichiarata prima della prova live.

Il DDL atomico MySQL non viene equiparato a DDL transazionale. Create/replace
richiederanno un lifecycle specifico e una campagna di recovery dedicata.
