# Binding runtime

[`plenora_database_engine::runtime_binding`](../crates/plenora-database-engine/src/runtime_binding.rs)
implementa Runtime Binding 2.0 (`plenora-runtime-binding-v2`) per la
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

Le regole di ammissione e di identita sono normative al pin adottato.
Il contratto pubblicato in discovery e distinto dalla versione wire:
vedere RT-025 nel [testo adottato](../contracts/upstream/specs/runtime/RUNTIME-BINDING-2.0.md).
Negoziazione, inoltri e acceptance record (RT-026, RT-027) appartengono
al trasporto ospitante: questo binding e soltanto una destinazione.

## Limiti dichiarati

- L'esito di `database.write` è il documento `write-outcome` della CLI e
  dello SDK solo quando la scrittura è `committed`. Uno stato
  `outcome_unknown`, `partially_committed` o `rolled_back` non è un successo
  (SURF-014): arriva come envelope d'errore con gli assi dell'esito — per il
  commit ignoto quelli della causa osservata (ERR-016) — e con il
  documento, compresa la `recovery`, in `details.write_outcome`. La CLI e lo
  SDK Python fanno lo stesso (`WriteOutcome::settle`).
- `database.query` restituisce il riepilogo JSON. Lo stream Arrow, dichiarato
  fra i content type d'uscita, lo produce la CLI (`query --output`), non
  questo binding: Runtime Binding 2.0 non ha un modo per chiedere uno dei due
  content type, e l'input contract di `database.query` è chiuso, quindi una
  scelta richiede un cambio di contratto.
- I documenti risolti da un riferimento hanno un limite di 8 MiB
  (`MAX_DOCUMENT_BYTES`).

## Prove

[`tests/runtime_vectors.rs`](../crates/plenora-database-engine/tests/runtime_vectors.rs)
esegue i vettori database e le sonde normative della revisione adottata,
verificandone gli hash. Il job `public-contract` confronta le copie con il
checkout e valida le evidenze prodotte contro gli schemi del pin.
Le cause del commit ignoto e i limiti di risultato sono esercitati con un
provider scriptato: non costituiscono una qualifica live dei provider.

Stati dei requisiti, ambito, limiti e deviazioni sono nel
[registro di adozione](../contracts/adoption-source.json). Il limite del
risultato query e applicato prima della consegna; l'host puo restringerlo con
`RuntimeBinding::with_query_payload_limit` e dichiara il proprio limite nel
suo manifest. Lo schema degli attributi v1 resta invariato: la mancata
pubblicazione del limite in discovery e una deviazione esplicita.
