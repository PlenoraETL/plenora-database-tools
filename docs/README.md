# docs

Pochi documenti, ciascuno con un ruolo esplicito.

| documento | cos'e |
| --- | --- |
| [`STATO.md`](STATO.md) | generato dal codice: crate, capability, sub-comandi, inventario dei test |
| [`operativo.md`](operativo.md) | cio che i file Compose non dicono da soli |
| [`database-core-v3-roadmap.md`](database-core-v3-roadmap.md) | piano di evoluzione additivo verso Core, unit of work e migrazioni |
| [`mariadb/EVIDENCE.md`](mariadb/EVIDENCE.md) | inventario generato delle prove che sostengono MariaDB |
| [`mariadb/SESSION-MATRIX.md`](mariadb/SESSION-MATRIX.md) | esito live generato della semantica di sessione |
| [`mariadb/ADR-0014-evidence-first.md`](mariadb/ADR-0014-evidence-first.md) | decisione ancora citata dal codice, ridotta al suo vincolo attuale |
| [`sdk/VM-EVIDENCE.md`](sdk/VM-EVIDENCE.md) | ultimo verdetto SDK autorevole eseguito nella VM |

## Perche cosi poco

Qui c'erano venticinque documenti che descrivevano lo stato corrente in prosa:
quali write mode fossero aperte, quante funzioni spatial fossero verificate,
quanti test avesse ciascuna famiglia, quali sub-comandi esponesse il CLI. Ogni
fatto viveva in due posti — il codice e la frase — e i due divergevano.

La difesa erano diciotto guardie che rileggevano il Markdown cercando frasi e
numeri. Presidiavano la prosa, quindi ne presidiavano anche la forma:
riscrivere una frase in modo equivalente faceva rosso, e un documento nuovo
apriva un buco che nessuno vedeva.

Ora un fatto sta in un posto solo. Se un documento deve mostrarlo, lo genera:
`scripts/render_state.py` scrive `STATO.md` e `--check` fallisce se e vecchio.
Se non puo generarlo, non lo dice.

`operativo.md` e l'eccezione utile: descrive scelte operative che nessun
sorgente esprime da solo — la separazione dei progetti Compose, la migrazione
una tantum dai vecchi container. I nomi che cita sono presidiati dai Compose.

## Dove sta la storia

In Git. Non qui.

Non c'e un archivio: i documenti che descrivevano lo stato di fasi passate, i
readiness dei rilasci e le review non restano nel worktree. Anche l'evidenza
MariaDB espone l'inventario corrente delle prove, non il diario delle campagne
che lo hanno prodotto.

Chi cerca il perche di una decisione lo trova nel commit che l'ha presa:
`git log`, `git show`, e i messaggi di commit che quelle decisioni le
spiegano.
