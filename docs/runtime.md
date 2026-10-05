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

`RuntimeInvocation` porta `content_type`, i metadati riservati e il payload
JSON. `RuntimeInvocation::from_json` rifiuta le chiavi ripetute: un `Value`
terrebbe l'ultima occorrenza. I metadati opzionali
(`plenora.execution.deadline`, `plenora.execution.idempotency_key`,
`plenora.message.causation_id`) si omettono quando sono assenti; scritti a
`null` vengono rifiutati.

Prima di qualunque resolver, il binding rifiuta con `remote_effect: none`:

- identità che non sono UUID canonici minuscoli (RT-012): `protocol`;
- capability, versione del binding o versione d'operazione non nella forma
  decimale canonica, oppure diverse da quelle dichiarate: `protocol` o
  `unsupported`;
- un input contract diverso da quello dell'operazione: `protocol`;
- un content type diverso da `application/json`: `protocol`;
- una chiave di idempotenza, che nessuna operazione accetta (RT-006):
  `unsupported`;
- una deadline che non sia un istante RFC 3339 in UTC: `protocol`. Se la
  deadline è già trascorsa: `timeout`.

Il payload è il documento `plenora-database-*-input-v1` della CLI, letto dagli
stessi tipi di
[`public_ops`](../crates/plenora-database-engine/src/public_ops.rs). Ne
seguono la stessa validazione, gli stessi risultati e gli stessi assi
d'errore (SURF-017). Sul runtime, `operation_path` e `parameters_path` sono
riferimenti opachi ad artefatti nella forma `schema://...`. Un percorso locale
(assoluto, relativo, `file:` o con `..`) viene rifiutato prima di essere
risolto (RT-013). `secret_environment` resta il nome del riferimento
protetto: lo risolve il `ConnectionResolver`, e il binding non legge mai
l'ambiente.

`database.write` porta il documento JSON della richiesta. I dati Arrow, che
il catalogo dichiara fra i content type d'ingresso, li consegna
`ArtifactResolver::open_write_input`, come sulla CLI arrivano da `--data`.

## Risultati

`RuntimeResult` porta content type, metadati e corpo. Il messaggio di
risultato ha un'identità propria: un UUID versione 8 derivato da quella della
richiesta, quindi deterministico. `causation_id` è la richiesta e
`correlation_id` è quello della richiesta. Un'identità della richiesta che
non è canonica non viene riflessa: al suo posto il risultato porta l'UUID nil.

- Le operazioni JSON restituiscono il documento della CLI: `verified` e la
  discovery runtime per `test_connection`; il documento d'ispezione; l'esito
  `plenora-database-write-result-v1`; il riepilogo di `database.query`
  (batch, righe e campi, senza valori).
- `database.read` scrive lo stream Arrow IPC nel sink dell'host, con lo
  stesso budget della CLI, ristretto alla deadline. Il risultato ha content
  type `application/vnd.apache.arrow.stream` e porta numero di byte e SHA-256
  dei byte scritti (RT-009, RT-014). Su un errore il sink riceve `abort` e
  l'host scarta i byte parziali.
- Un errore è `application/vnd.plenora.error+json` con contratto
  `plenora-error-v1` e tutti gli assi comuni (RT-010). Di un errore dei
  resolver dell'applicazione passano categoria, fase, effetto e retry; il
  testo viene sostituito, perché potrebbe nominare ciò a cui un riferimento
  si è risolto.

## Limiti dichiarati

- L'esito di `database.write` è il documento `write-outcome` della CLI e
  dello SDK. Uno stato `outcome_unknown` o `partially_committed` arriva come
  risultato con quello stato, come sulle altre superfici, e non come envelope
  d'errore. Il consumatore legge `status`. Un errore del provider con effetto
  ignoto arriva invece come envelope d'errore (vettore
  `database-write-error`).
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
  stream. Le mutazioni di capability, operazione, versioni, input contract,
  deadline, chiave di idempotenza e identità vengono rifiutate prima
  dell'invocazione.
- `database-query-success`: stessa operazione, versione, output contract,
  content type e correlazione; il riepilogo descrive i dati del vettore.
- `database-write-error`: un commit dall'esito ignoto produce gli assi del
  vettore (`internal`, `commit`, `unknown`, `requires_recovery`).

Il test confronta inoltre la discovery con il catalogo e con i binding del
pin. Nel job `public-contract`, i risultati prodotti vengono validati con gli
schemi del pin da `python scripts/check_runtime_evidence.py`.
