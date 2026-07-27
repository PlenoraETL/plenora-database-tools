# ADR 0002 — Validazione locale e preflight remoto

Stato: **accettato**  
Data: 2026-07-26

## Contesto

La validazione di un piano deve essere riusabile e sicura senza credenziali,
ma capability, schema e privilegi esistono solo sul provider reale.

## Decisione

Tre stadi:

```text
validate (puro)
  → prepare (sessione + probe, nessuna mutazione)
  → execute (read o mutation protocol)
```

`validate`:

- limita/parsa il JSON;
- valida contratti e policy;
- produce `ValidatedDatabasePlan`;
- non apre rete e non legge segreti.

`prepare`:

- risolve endpoint/segreti runtime;
- connette;
- prova server/provider e capability;
- introspeziona;
- verifica privilegi;
- prepara SQL/REST request layout;
- produce `PhysicalDatabasePlan`;
- non muta quando una prova read-only è possibile.

`execute` è l'unico stadio autorizzato alla mutazione.

## Conseguenze

- dry-run locale stabile;
- errori di piano precedono quelli di rete;
- nessuna credenziale nel piano validato;
- schema drift dopo prepare resta possibile e viene rilevato;
- il piano fisico non è portabile tra sessioni/versioni;
- ArcGIS usa lo stesso lifecycle con probe REST al posto del catalogo SQL.

## Invariante

Nessun piano non validato raggiunge rete; nessuna mutazione precede il remote
preflight.

