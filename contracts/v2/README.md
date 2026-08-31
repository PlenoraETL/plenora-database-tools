# Contratti v2

L'unica major nel worktree, e quella che il codice emette. Le precedenti stanno
in Git, che e dove sta la storia.

- `plan.schema.json`: input validato per test, inspect, read e write;
- `capabilities.schema.json`: capability scoperte sul target;
- `age-capabilities.schema.json`: capability AGE v1, separate dal provider relazionale;
- `age-admin-capabilities.schema.json`: capability amministrative AGE additive;
- `loss-report.schema.json`: perdite di mapping esplicite;
- `write-outcome.schema.json`: committed, rolled back, partial e unknown;
- `common.schema.json`: identificatori, provider, oggetti e policy;
- `golden-manifest.schema.json`: casi di compatibilità semantica.

I `$id` usano il namespace non instradabile
`https://plenora.local/database-tools/v2/`, e nessuno di essi referenzia
un'altra major. Gli esempi validi sono in `examples/index.json`.

## Perche la major e cambiata

Due rotture indipendenti.

### `append` e `truncate_insert` sono due promesse

`writes.truncate_insert` e un campo nuovo e **richiesto**.

Prima non esisteva, e `writes.append` valeva per due mode: l'engine la
consultava sia per `Append` sia per `TruncateInsert`. Sono due promesse diverse
— `Append` lascia le righe che c'erano, `TruncateInsert` le toglie, e il modo
in cui le toglie decide se un fallimento e recuperabile — e tenerle insieme
faceva dire il falso al contratto: `MySQL` pubblicava `append = true` e
rifiutava `TruncateInsert` in prepare, perche li `TRUNCATE TABLE` e DDL con
commit implicito. Le righe sparirebbero prima dell'INSERT e nessun rollback le
riporterebbe indietro. Il contratto prometteva quindi una mode che il provider
negava, e il consumatore lo scopriva a piano gia compilato.

### Il dominio sono i database

La major precedente modellava anche un provider REST, e non come dettaglio di
un esempio: come modello. Portava con se un'operazione per famiglia, un
profilo transazionale dedicato, l'indirizzamento per layer nel riferimento a un
oggetto, gli esiti per layer nel write outcome, e mezza dozzina di capability
che nessun provider database ha mai popolato — erano `false`, `null`, vuote o
rifiutate in preflight.

Il costo non era a runtime: era un contratto piu largo del dominio, che
obbligava ogni provider nuovo a dichiarare di non essere una cosa che nessuno
gli aveva chiesto di essere.

Qui il riferimento a un oggetto ha catalog, schema e oggetto; le operazioni
sono `database.*`; i profili transazionali sono quelli di una transazione SQL;
l'esito di una scrittura conta righe, non layer; e `recovery.last_certain_phase`
arriva a `commit_requested`, perche un database non richiede altro.

## Migrazione

Per un documento capability:

1. `schema_version` passa a `2`;
2. si aggiunge `writes.truncate_insert`;
3. si tolgono le capability che non appartengono a un motore SQL.

Il valore di `truncate_insert` **non** e sempre quello di `append`. Prima
`append` autorizzava entrambe le mode, quindi un documento che diceva
`append: true` prometteva anche `TruncateInsert`; la traduzione fedele dipende
pero dal comportamento realmente sostenuto dal provider. Le dichiarazioni
correnti non vengono duplicate qui: sono generate in
[`docs/STATO.md`](../../docs/STATO.md).

Per un piano e per un write outcome: `schema_version` passa a `2`, e cade
quanto elencato sopra. Un piano la cui operazione non e `database.*` non e
migrabile — non e un piano database.

## Chi presidia il confine

`scripts/phase0_validate.py`, con il check `active-contract-domain`: legge il
testo di contratti, suite golden, benchmark, cataloghi e documenti, e fallisce
se compare un termine di un altro dominio o un riferimento a una major diversa
da questa. Il secondo caso e quello che conta: un contratto puo essere ripulito
e continuare a referenziare, per una definizione comune, il file da cui la
superficie e stata tolta — e allora la superficie e ancora li.
