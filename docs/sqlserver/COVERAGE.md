# SQL Server coverage assurance

Data della prova locale pre-push: 2026-07-29.

La coverage usa Rust `1.92.0` e `cargo-llvm-cov 0.8.7`. Il profilo cumulativo
esegue prima tutti i target del workspace con PostgreSQL/PostGIS live e poi i
test SQL Server ignorati esplicitamente contro il riferimento SQL Server 2022.
I risultati vengono uniti prima di applicare le soglie production-focused.

## Gate cumulativo

| Metrica | Risultato | Soglia | Esito |
|---|---:|---:|---|
| regioni | 80,41% | 77% | superata |
| funzioni | 80,98% | 79% | superata |
| linee | 82,12% | 80% | superata |

## Percorsi SQL Server principali

| Unità | Linee |
|---|---:|
| `arrow.rs` | 92,14% |
| `catalog/schema.rs` | 99,12% |
| `config.rs` | 94,34% |
| `connection.rs` | 77,91% |
| `read.rs` | 77,23% |
| `types.rs` | 90,07% |
| `write/codec.rs` | 83,52% |
| `write/mod.rs` | 79,47% |

Le soglie sono cumulative, non minimi per singolo file. I valori locali
dimostrano che il workflow aggiornato è eseguibile; l'artefatto GitHub Actions
con hash, report LCOV/JSON e ambiente resta l'evidenza autoritativa della
revisione pubblicata.

## Sequenza riproducibile

```text
cargo llvm-cov clean --workspace
cargo llvm-cov --workspace --all-targets --locked --no-report
cargo llvm-cov --package plenora-db-sqlserver --lib --locked --no-report -- --ignored --test-threads=1
cargo llvm-cov report --summary-only --ignore-filename-regex <production-ignore> --fail-under-lines 80 --fail-under-functions 79 --fail-under-regions 77
```
