# SQL Server coverage assurance

Data della prova locale pre-push: 2026-07-29.

La coverage usa Rust `1.92.0` e `cargo-llvm-cov 0.8.7`. Il profilo cumulativo
esegue prima tutti i target del workspace con PostgreSQL/PostGIS live e poi i
test SQL Server ignorati esplicitamente contro il riferimento SQL Server 2022.
I risultati vengono uniti prima di applicare le soglie production-focused.

## Gate cumulativo

| Metrica | Risultato | Soglia | Esito |
|---|---:|---:|---|
| regioni | 79,96% | 77% | superata |
| funzioni | 81,12% | 79% | superata |
| linee | 81,80% | 80% | superata |

## Percorsi SQL Server principali

| Unità | Linee |
|---|---:|
| `arrow.rs` | 92,14% |
| `catalog/schema.rs` | 99,12% |
| `config.rs` | 94,34% |
| `connection.rs` | 77,91% |
| `parameter.rs` | 61,60% |
| `provider.rs` | 90,26% |
| `query.rs` | 72,20% |
| `read.rs` | 79,70% |
| `types.rs` | 78,38% |
| `write/codec.rs` | 83,52% |
| `write/mod.rs` | 78,22% |

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
