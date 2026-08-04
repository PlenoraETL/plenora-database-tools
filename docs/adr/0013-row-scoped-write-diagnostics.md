# ADR 0013 - Diagnostica row-scoped delle scritture

## Stato

Proposta. Introduce una **rottura sorgente** del contratto pubblico di
`plenora-database-core` ed e percio candidata a **Database Tools 2.0.0**. La
1.1.0 congelata non viene toccata: `release/1.1.0.json`, `contracts/v1/` e il
manifest di rilascio restano invariati in questo ramo.

## Problema

Quando una scrittura fallisce, l'errore dice *che cosa* e andato storto ma non
*quale riga sorgente* e stata rifiutata, ne *che fine ha fatto* il resto
dell'input. Le due domande operative — quale riga correggere, e che cosa e
rimasto sul server — oggi si rispondono leggendo il messaggio del vendor, cioe
indovinando.

Due deduzioni sono particolarmente pericolose:

1. **L'indice per riga.** Un INSERT multi-riga rifiutato dice che il batch ha
   violato un vincolo, non quale riga. Ricostruire l'indice dalla posizione
   nel batch o dal testo del messaggio produce un numero plausibile e falso.
2. **L'esito dell'annullamento.** Se il `ROLLBACK` viene inviato ma il suo
   acknowledgement non arriva, le righe applicate non sono ne annullate ne
   presenti: sono ignote. Dichiararle annullate e la deduzione che trasforma un
   incidente recuperabile in una perdita silenziosa di dati.

## Decisione

Adottare il contratto `plenora-row-diagnostics-v1` come carrier tipizzato
dell'errore, con quattro vincoli non negoziabili.

**L'indice e provato, non dedotto.** Il percorso diagnostico applica *uno
statement per riga sorgente*. L'indice pubblicato e l'offset assoluto nella
sorgente, zero-based, e attraversa i batch: `WriteDiagnosticsTracker` rifiuta
un indice che non sia quello della prima riga non ancora applicata.

**Il totale dichiarato viene verificato fino a EOF.** Dopo l'ultima riga
dichiarata il seam deve attestare che lo stream sia esaurito. Righe residue nel
batch corrente o in batch successivi fanno fallire la scrittura e provocano il
rollback: non possono essere ignorate prima del commit.

**La causa nasce da un codice, mai da un messaggio.** La classificazione legge
il codice del server (`plenora_db_mysql::error::row_rejection_cause`). Il testo
e vendor, localizzato e contiene valori di riga: interpretarlo violerebbe sia
l'attribuzione certa sia la redazione.

Il codice prova la classe del vincolo, non la colonna. `constraint_column`
appartiene al contratto dichiarato dalla sorgente: il provider MySQL la
pubblica soltanto se il campo esiste nello schema preparato, verificandolo
prima della transazione. Non viene mai ricavata dal testo del server. Una
dichiarazione sorgente falsa e un contratto invalido, come uno schema Arrow
falso, e fallisce chiusa prima dell'I/O di scrittura.

**Applicata significa una riga confermata dall'OK packet.** Il successo dello
statement row-scoped richiede `affected_rows == 1`. Zero o piu di una riga
rendono l'effetto remoto ignoto, quarantinano la sessione e impediscono il
commit. Se la quarantena ha gia disconnesso la sessione, il tentativo di
cleanup non puo degradare la disposizione a `RequiresRecovery`: l'errore
pubblico resta `Quarantine` e non retryable. Totale dichiarato e policy vengono
validati prima di aprire la transazione.

**Certo e ignoto sono partizioni distinte.** `write_outcome` separa
`certainly_rejected`, `certainly_not_attempted`, `certainly_rolled_back` e
`effect_unknown`, e ogni quantita dichiara esplicitamente se e nota. Un
acknowledgement di rollback perso lascia `certainly_rolled_back` e
`effect_unknown` in stato `unknown`, non a zero: assi d'errore fase `Rollback`,
effetto `Unknown`, disposizione `Quarantine`.

**Nessun valore di riga lascia il processo.** La chiave configurata viaggia
redatta (campo identificabile, contenuto no) e i messaggi d'errore restano
privi di indici, chiavi e payload. Il documento non e pubblicabile finche non
supera `RowDiagnostics::validate`: sia `to_json` sia l'implementazione pubblica
di `Serialize` validano prima di emettere, senza bypass via `serde_json`.

L'aritmetica e interamente `checked_*`; gli esempi sono limitati da
`examples_limit` con `examples_truncated` che dichiara l'omissione. I limiti
JSON Schema `maxLength` sono misurati in caratteri Unicode, non byte UTF-8.

## Rottura sorgente

`DatabaseError` acquisisce il campo `diagnostics: Option<Box<RowDiagnostics>>`.
Le classi di rottura osservate nel workspace sono due, entrambe di sorgente e
nessuna di comportamento a runtime:

| Classe | Sito | Effetto |
| --- | --- | --- |
| Literal di struct `DatabaseError` | 40 occorrenze in `core`, `cli`, `db-mysql`, `db-postgres`, `db-sqlserver` | `E0063: missing field diagnostics` |
| Literal di `RowDiagnosticsPolicy` | costruzioni della policy row-scoped | campo `constraint_column` mancante |

Nessun consumatore che costruisca errori tramite i costruttori
(`DatabaseError::invalid_plan`, `resource_limit`, `cancelled`, `provider_*`) e
interessato: la rottura riguarda solo chi usa i literal.

Si aggiunge inoltre la variante `RetryDisposition::Quarantine`, che estende un
enum pubblico: un `match` esaustivo a valle non compila piu. La variante non e
ritentabile (`is_retryable() == false`), quindi il default di sicurezza resta
conservativo anche per chi la ignora.

## Alternative scartate

**Campo privato con accessore.** Avrebbe evitato la rottura dei literal ma
introdotto un costruttore obbligatorio per ogni errore del workspace: rottura
piu ampia, non minore.

**Diagnostica come stringa nel messaggio.** Rimette il consumatore a fare
parsing, cioe esattamente il problema che l'ADR chiude.

**Attribuzione statistica sul batch.** Un indice probabile non e un indice: il
contratto ammette solo `complete`, `partial` o `unknown` dichiarati, mai una
stima presentata come certezza.

## Conseguenze

Il percorso diagnostico costa uno statement per riga ed e quindi attivo solo
quando la sorgente dichiara `declared_input_rows()`: senza quel numero non
esiste `input_total` da partizionare e il costo non sarebbe giustificato. Il
percorso a batch resta il default soltanto quando la cardinalita non e
dichiarata. Una cardinalita dichiarata richiede invece semantiche row-scoped:
mode, profilo transazionale o codec che non consentono attribuzione certa
falliscono prima del checkout, senza fallback silenzioso al percorso a batch.

Il seam `RowScopedWriter` e provider-agnostico: `plenora-database-testkit`
offre `ScriptedRowWriter` e i provider PostgreSQL e SQL Server possono
adottarlo senza duplicare la contabilita.
