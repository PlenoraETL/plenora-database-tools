# ADR 0004 — Provider SQL e ArcGIS

Stato: **accettato**  
Data: 2026-07-27

## Contesto

I database relazionali e ArcGIS Feature Service condividono discovery,
lettura/scrittura geospaziale, limiti, osservabilità e outcome. Non condividono
però protocollo, transazioni o linguaggio di query.

## Decisione

Il core espone un `Provider` capability-driven con quattro famiglie di
operazioni:

- test connessione;
- introspezione;
- read a stream di `RecordBatch`;
- write da stream di `RecordBatch`.

Sotto questa interfaccia:

- `SqlProvider` usa dialect, bind parameter, transazioni e strategie bulk;
- `ArcGisProvider` usa REST, query pagination/ObjectID windows e
  `applyEdits`;
- le capability sono scoperte a runtime e salvate insieme alla versione del
  server;
- una capability assente produce un errore `Unsupported`, mai un fallback
  silenzioso.

ArcGIS non implementa il trait interno `SqlDialect`. Le sue query vengono
costruite da un modello tipizzato e serializzate nei parametri REST.

## Conseguenze

- l’API pubblica rimane uniforme senza fingere semantica identica;
- transaction scope e atomicità restano dichiarati per provider;
- nuove fonti non SQL possono essere aggiunte senza contaminare l’AST SQL;
- i test comuni verificano garanzie, quelli provider-specifici verificano il
  protocollo.

## Alternative scartate

- trattare ArcGIS come dialect SQL;
- esporre API pubbliche completamente separate;
- dedurre le capability soltanto dal nome del provider.
