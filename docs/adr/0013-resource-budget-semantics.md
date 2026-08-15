# ADR 0013 — Semantica dei limiti `ResourceBudget`

Stato: **accettato**
Data: 2026-08-15
Target release: `py-v0.9.0`, core `1.2.0`.

## Contesto

`ResourceLimits` espone quattro limiti principali:
- `rows: u64`
- `memory_bytes: u64`
- `output_bytes: u64`
- `cell_bytes: u64`

Al 2026-08-15 la semantica applicata era **inconsistente**:
- CLI `--max-output-bytes` era interpretato come "memoria Arrow
  stimata" (riservata al pre-decode) — NON come byte serializzati.
- `memory_bytes` in `write.rs::enforce_input_limits` misurava
  `array.get_array_memory_size()` che è già "memoria pool Arrow".
- Nessun budget contava i byte realmente scritti su file/stream IPC.

Consumer che imposta `--max-output-bytes=1GB` si aspetta di limitare
il file emesso, non la memoria transient del decode. La confusione
rende il budget imprevedibile e la CI non può gate su size reale.

## Decisione

Semantica per limite:

| Campo           | Cosa misura                                        | Enforced dove                        |
|-----------------|----------------------------------------------------|--------------------------------------|
| `rows`          | Righe totali processate                            | Batch counter per stream reader/writer |
| `memory_bytes`  | Memoria/reservation stimata (pool Arrow, staging)  | `get_array_memory_size()` sui batch  |
| `max_batch_bytes` | Singolo batch (soft target)                      | Pre-decode/pre-write ogni batch      |
| `cell_bytes`    | Singola cella (WKB, string long, ecc.)             | Per-cell scan                        |
| `output_bytes`  | **Byte serializzati sul sink (IPC/file/stream)**   | Counting writer sul canale output    |

**`output_bytes` diventa realmente enforced**:
- Introdurre wrapper `CountingWriter<W: Write>` che incrementa un
  contatore atomico ad ogni `write_all`.
- Path CLI `read-ipc` / `write-arrow` / `--output` usano
  `CountingWriter` invece del writer nudo.
- Superato il limite → `ResourceLimit` error, il writer viene chiuso
  e il file parziale rimosso (nessun file parziale in circolazione).

**`memory_bytes` resta memoria stimata** — è metric semanticamente
diversa (peak Arrow memory), utile per prevenire OOM. Documentare
chiaramente la distinzione.

## Conseguenze

**Positive**:
- CLI `--max-output-bytes=1GB` fa quello che dice.
- Consumer può fare `bash -c 'plenora ... --max-output-bytes=100M ||
  echo too-big'` in modo affidabile.
- Documentazione allineata al codice.

**Negative**:
- Refactor tocca tutti i path che scrivono su sink: CLI `read-ipc`,
  `write-arrow`, `explain` con `--output`, benchmark output.
- SDK Python: `BatchReader` deve esporre un writer counter o
  wrapper simile per garantire il limite lato consumer.

**Non copre**:
- Compression: se il consumer serializza gzip/zstd, `output_bytes`
  misura il byte compresso. Documentare.
- Metadata/schema overhead: il counter parte dal primo byte scritto
  incluso header IPC.

## Migrazione

- Doc `--max-output-bytes` aggiornata con nuova semantica.
- CHANGELOG con sezione "BREAKING: max_output_bytes ora conta byte
  serializzati". Consumer che si affidava al vecchio comportamento
  (memoria Arrow) deve migrare a `memory_bytes`.
- Non è un rename ma cambia il comportamento — versione minor 1.2.0.

## Alternative considerate

- **Rinominare i limiti**: `output_bytes` è già il nome semanticamente
  corretto. La bug era nell'implementazione, non nel naming.
- **Rimuovere `memory_bytes`**: alcuni consumer lo usano per pre-flight
  check OOM. Meglio mantenere entrambi con distinzione documentata.
