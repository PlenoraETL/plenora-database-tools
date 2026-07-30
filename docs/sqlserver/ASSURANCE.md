# Assurance dedicata SQL Server

Il provider SQL Server dispone di un gate autonomo, separato dalla coverage
generale del workspace:

```powershell
python scripts\check_sqlserver_reference.py
```

Il gate usa SQL Server 2022 fissato per digest, verifica identità e stato del
server, esegue Clippy con warning negati, unit test e l'intera matrice live in
serie. La baseline post-RC1 aggiunge una CA privata, il controllo positivo e
negativo del nome host, la rotazione con impronta server diversa e CA stabile,
e il rifiuto live di Z/M/ZM/FullGlobe. Il conteggio dei test live è un
cricchetto deliberato: un'aggiunta o una
rimozione richiede l'aggiornamento esplicito del gate e della relativa
evidenza.

In CI il risultato machine-readable, il log completo e le identità di
toolchain/container sono conservati per 90 giorni. Un fallimento conserva anche
i log Compose e lo stato dei container.

Il provider imposta sempre `EncryptionLevel::Required`. La fixture non forza
la cifratura per client estranei (`forceencryption = 0`): il gate qualifica la
policy della libreria e non pretende di qualificare l'hardening globale
dell'istanza SQL Server. La combinazione server-forced/Tiberius viene tenuta
fuori dal claim finché non dispone di una prova di interoperabilità separata.

## Scope dimostrato

- bootstrap TDS/TLS, pool bounded e recovery;
- read Arrow bounded, `geometry` e `geography` XY;
- prepared write e TDS bulk differenziale;
- update/upsert/delete-by-keys con chiavi univoche e conteggi distinti;
- QueryOperation relazionali ricche e schema dei risultati vuoti;
- schema drift fail-closed;
- schema evolution additiva opt-in, senza mutazioni in prepare e con rollback
  congiunto di DDL e dati;
- rollback e outcome di commit incerto;
- create atomico e replace con staged swap transazionale;
- profilo create/replace completo sui 19 tipi scalari, temporali e spatial
  della fixture di riferimento;
- rollback dello staging e dei rename, cleanup, dipendenze fail-closed e
  leggibilita del vecchio target durante il caricamento;
- taglio e blackhole fisici del trasporto TDS.
- catena TLS privata, hostname match/mismatch e rotazione;
- rifiuto pre-stream/pre-mutation di geometry/geography Z, M, ZM e FullGlobe;
- gate prestazionale separato su read, prepared, TDS bulk, create e replace.

## Gap non coperti dal gate v1

- SQL Server 2019, 2025 e Azure SQL;
- supporto lossless spatial Z/M/ZM e `FullGlobe` (il rifiuto è provato);
- AST spatial tipizzato;
- catalogo temporal/graph/external e partizioni.

Questi gap non sono capability implicite: restano non pubblicizzati finché una
prova dedicata non viene aggiunta al gate.
