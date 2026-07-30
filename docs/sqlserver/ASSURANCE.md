# Assurance dedicata SQL Server

Il provider SQL Server dispone di un gate autonomo, separato dalla coverage
generale del workspace:

```powershell
python scripts\check_sqlserver_reference.py
```

Il gate usa SQL Server 2022 fissato per digest, verifica identità e stato del
server, esegue Clippy con warning negati, unit test e l'intera matrice live in
serie. Il conteggio dei test live è un cricchetto deliberato: un'aggiunta o una
rimozione richiede l'aggiornamento esplicito del gate e della relativa
evidenza.

In CI il risultato machine-readable, il log completo e le identità di
toolchain/container sono conservati per 90 giorni. Un fallimento conserva anche
i log Compose e lo stato dei container.

## Scope dimostrato

- bootstrap TDS/TLS, pool bounded e recovery;
- read Arrow bounded, `geometry` e `geography` XY;
- prepared write e TDS bulk differenziale;
- update/upsert/delete-by-keys con chiavi univoche e conteggi distinti;
- QueryOperation relazionali ricche e schema dei risultati vuoti;
- schema drift fail-closed;
- rollback e outcome di commit incerto;
- create atomico e replace con staged swap transazionale;
- profilo create/replace completo sui 19 tipi scalari, temporali e spatial
  della fixture di riferimento;
- rollback dello staging e dei rename, cleanup, dipendenze fail-closed e
  leggibilita del vecchio target durante il caricamento;
- taglio e blackhole fisici del trasporto TDS.

## Gap non coperti dal gate v1

- SQL Server 2019, 2025 e Azure SQL;
- TLS positivo con CA privata e hostname matching;
- spatial Z/M, `FullGlobe` e AST spatial tipizzato;
- catalogo temporal/graph/external e partizioni.

Questi gap non sono capability implicite: restano non pubblicizzati finché una
prova dedicata non viene aggiunta al gate.
