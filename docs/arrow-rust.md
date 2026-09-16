# Tipi Arrow nell'API Rust

I batch e gli schemi pubblici usano i tipi riesportati da
[`plenora_database_core::arrow`](../crates/plenora-database-core/src/lib.rs).
Usare questi export nel codice applicativo evita di introdurre una seconda
major Arrow incompatibile con il provider:

```rust
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::arrow::schema::{Field, Metadata};
```

Quando servono dipendenze dirette Arrow, allinearle ai pin del
[`manifest workspace`](../Cargo.toml). Un `RecordBatch` di un'altra major
Rust non e lo stesso tipo, anche se ha lo stesso nome e rappresenta gli
stessi dati. Ricompilare i consumer contro la stessa famiglia di dipendenze.

I metadati restituiti da campi e schemi sono `Metadata`. Negli helper che
li prendono in prestito usare `&Metadata`; evitare conversioni in mappe
temporanee solo per leggere una chiave. I costruttori accettano anche
`HashMap<String, String>` tramite conversione. Se un `collect()` alimenta
un costruttore generico, specificare `collect::<Metadata>()`.

La serializzazione dei metadati in JSON conserva oggetti chiave/valore.
L'iterazione e ordinata, ma i confronti dei documenti e gli scambi IPC
devono verificare contenuto e schema, non l'identita dei byte serializzati.

Il confine Python usa IPC: non richiede che il numero di versione PyArrow
coincida con quello dei crate Rust. Le versioni qualificate sono nei
[`requisiti SDK`](../requirements-sdk-tests.txt); i test di interoperabilita
verificano schema, null e metadati. I vincoli GeoArrow e le capability dei
provider rimangono quelli del contratto pubblico.
