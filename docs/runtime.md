# Binding runtime

[`plenora_database_engine::runtime_binding`](../crates/plenora-database-engine/src/runtime_binding.rs)
implementa Runtime Binding 1.0 (`plenora-runtime-binding-v1`) per la
capability `plenora.database-tools`, versione 1. È un confine
transport-neutral: l'applicazione finale lo avvolge nel proprio handler di
capability, per esempio il `CapabilityHandler` di runtime-tools. Il crate non
dipende da runtime-tools (RT-015). Trasporto, autorizzazione, segreti e
artefatti appartengono all'applicazione, dietro due trait:

| trait | responsabilità |
| --- | --- |
| `ConnectionResolver` | autorizza il riferimento protetto (`secret_environment`) e apre il provider richiesto |
| `ArtifactResolver` | risolve i documenti indicati da riferimenti opachi, consegna i dati Arrow di `database.write` e apre il sink del risultato di `database.read` |

## Operazioni e discovery

Le operazioni sono quelle che il catalogo del pin lega alla superficie
`runtime` (`bindings/runtime-v1.json`): `database.test_connection`,
`database.list_catalogs`, `database.list_schemas`, `database.list_objects`,
`database.describe_object`, `database.read`, `database.write` e
`database.query`. `RuntimeBinding::capabilities` (entrypoint
`plenora.database-tools#capabilities@1`) restituisce il documento Capability
Discovery 2.0 della superficie. Lo stesso documento decide l'instradamento,
quindi una richiesta non può raggiungere un'operazione che il documento non
dichiara (RT-004).

## Richieste

`RuntimeInvocation` porta `content_type`, i metadati (valori JSON di
qualunque tipo: una chiave riservata che non è una stringa, `null` compreso, è
malformata) e il payload JSON. `RuntimeBinding::invoke_json` legge il
messaggio serializzato rifiutando le chiavi ripetute. Un messaggio che non si
legge affatto produce comunque un risultato d'errore `protocol`, senza valori
riflessi.

Le regole di ammissione seguono la matrice runtime comune alle quattro
librerie e i chiarimenti RT-016..RT-023 proposti in `plenora-contracts` #21.
Ogni rifiuto avviene prima di qualunque resolver, con `phase: validate`,
`remote_effect: none` e `retry: never` (RT-016). La categoria è la prima che
si applica (RT-018): `protocol` se una chiave riservata è assente, non è una
stringa o è fuori grammatica (`"01"`, `"+1"`, `1`, UUID maiuscoli);
`unsupported` se tutto è ben formato ma non corrisponde a un'operazione
annunciata dalla discovery, al content type o ai controlli; `timeout` se la
deadline è già trascorsa. Una chiave `plenora.*` che il binding non riserva
viene ignorata, come ogni membro facoltativo sconosciuto (RB §9). In
dettaglio:

| caso | esito |
| --- | --- |
| capability, versione del binding, operazione, versione d'operazione, input contract | `unsupported` se ben formati ma diversi da quelli annunciati, `protocol` altrimenti |
| identità (`message.id`, correlazione, causazione) assenti o non canoniche | `protocol` (RT-012) |
| content type | `application/json`; un media type ben formato diverso è `unsupported`, uno malformato è `protocol` |
| `plenora.execution.idempotency_key` | `null`, vuota o oltre 256 byte: `protocol`; presente, poiché nessuna operazione la accetta: `unsupported` (RT-006, RT-022) |
| `plenora.execution.deadline` | ogni grafia RFC 3339 di UTC (`Z` o `z`, `+00:00`, `T` o `t`, frazioni); un offset diverso da zero, `-00:00` o un valore che non è RFC 3339: `protocol`. Già scaduta (`deadline <= now`): `timeout` (RT-021) |
| payload che non soddisfa l'input contract, deadline nel payload compresa | `invalid_configuration` |

Il payload è il documento `plenora-database-*-input-v1` della CLI, letto dagli
stessi tipi di
[`public_ops`](../crates/plenora-database-engine/src/public_ops.rs). Ne seguono
la stessa validazione, gli stessi risultati e gli stessi assi d'errore
(SURF-017): anche la CLI classifica una richiesta non conforme come
`invalid_configuration`. Sul runtime, `operation_path` e `parameters_path`
sono riferimenti opachi ad artefatti nella forma `schema://...`. Un percorso
locale (assoluto, relativo, `file:` o con `..`) viene rifiutato prima di
essere risolto (RT-013). `secret_environment` resta il nome del riferimento
protetto: lo risolve il `ConnectionResolver`, e il binding non legge mai
l'ambiente.

`database.write` porta il documento JSON della richiesta. I dati Arrow, che il
catalogo dichiara fra i content type d'ingresso, li consegna
`ArtifactResolver::open_write_input`, come sulla CLI arrivano da `--data`.

## Risultati

`RuntimeResult` porta content type, metadati e corpo.

- `plenora.message.id` è sempre nuovo: un UUID versione 8 derivato dai byte
  dell'invocazione, quindi deterministico.
- `plenora.message.causation_id` è il `message.id` della richiesta, se
  canonico.
- `plenora.capability.operation`, `plenora.operation.version` e
  `plenora.trace.correlation_id` sono i valori della richiesta, copiati byte
  per byte solo se canonici. Altrimenti la chiave viene omessa: mai
  normalizzata, mai inventata, mai `"0"`.

Il contenuto per esito:

- Le operazioni JSON restituiscono il documento della CLI: `verified` e la
  discovery runtime per `test_connection`; il documento d'ispezione; l'esito
  `plenora-database-write-result-v1`; il riepilogo di `database.query`
  (batch, righe e campi, senza valori).
- `database.read` scrive lo stream Arrow IPC nel sink dell'host, con lo
  stesso budget della CLI ristretto alla deadline. Il risultato ha content
  type `application/vnd.apache.arrow.stream` e porta numero di byte e SHA-256
  dei byte scritti (RT-009, RT-014). Se un errore arriva dopo che il sink ha
  ricevuto byte, l'effetto è `partial` con `retry: never`. Se la
  pubblicazione dell'host fallisce, l'esito è `unknown` con
  `requires_recovery` in fase `commit`.
- Un errore è `application/vnd.plenora.error+json` con contratto
  `plenora-error-v1` e tutti gli assi comuni (RT-010). Di un errore dei
  resolver dell'applicazione passano categoria, fase, effetto e retry; il
  testo viene sostituito.

Seguono regole proposte e non ancora normative al pin `1e902df`, ratificate
come chiarimenti in `plenora-contracts` #21: l'ordine fra `protocol`,
`unsupported` e `timeout` (RT-017, RT-018), che cosa riflettere nel risultato
(RT-019), `retry: never` per i rifiuti (RT-016), la causazione del risultato
(RT-020), `invalid_configuration` per il payload (RT-023) e gli assi dopo un
effetto sul sink (ERR-014).

## Limiti dichiarati

- L'esito di `database.write` è il documento `write-outcome` della CLI e
  dello SDK solo quando la scrittura è `committed`. Uno stato
  `outcome_unknown`, `partially_committed` o `rolled_back` non è un successo
  (SURF-014): arriva come envelope d'errore con gli assi dell'esito — per il
  commit ignoto quelli del vettore `database-write-error` — e con il
  documento, compresa la `recovery`, in `details.write_outcome`. La CLI e lo
  SDK Python fanno lo stesso (`WriteOutcome::settle`).
- `database.query` restituisce il riepilogo JSON della CLI. Lo stream Arrow,
  dichiarato fra i content type d'uscita, non è prodotto da questo binding.
- I documenti risolti da un riferimento hanno un limite di 8 MiB
  (`MAX_DOCUMENT_BYTES`).

## Prove

[`tests/runtime_vectors.rs`](../crates/plenora-database-engine/tests/runtime_vectors.rs)
esegue i tre vettori `database-*` di `vectors/runtime-v1` del pin, copiati
byte per byte in [`contracts/upstream`](../contracts/upstream/source.json),
con un provider scriptato:

- `database-read-request`: l'instradamento viene ammesso. Il payload
  illustrativo viene rifiutato dallo schema del componente prima di qualunque
  resolver. Con un payload conforme, la stessa richiesta legge e consegna lo
  stream. Ogni caso applicabile della matrice runtime è provato: mutazioni di
  capability, operazione, versioni, input contract, content type, deadline,
  chiave di idempotenza e identità; metadati assenti o `null`; valori
  riflessi solo se canonici; sink che fallisce dopo i primi byte o alla
  pubblicazione.
- `database-query-success`: stessa operazione, versione, output contract,
  content type e correlazione; il riepilogo descrive i dati del vettore.
- `database-write-error`: un commit dall'esito ignoto produce gli assi del
  vettore (`internal`, `commit`, `unknown`, `requires_recovery`).

Il test confronta inoltre la discovery con il catalogo e con i binding del
pin. Esegue anche le 21 sonde `vectors/runtime-probes-v1` di
`plenora-contracts` #21, copiate in
[`contracts/upstream/proposed`](../contracts/upstream/proposed/source.json) e
fissate per SHA-256 al commit `4890d27`. Sono proposte, non ancora
normative. Le sonde su altri componenti si eseguono con la stessa mutazione
sulla richiesta database. Nel job `public-contract`, i risultati prodotti vengono validati con gli
schemi del pin da `python scripts/check_runtime_evidence.py`.
