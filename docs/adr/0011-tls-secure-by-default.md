# ADR 0011 — TLS secure-by-default per PostgreSQL

Stato: **accettato**
Data: 2026-08-15
Target release: `py-v0.9.0`, driver `1.2.0` (bump minor per breaking).

## Contesto

Al 2026-08-15 (commit `b8a822e`), `PostgresProvider::default()` imposta
`PostgresTlsMode::Disabled`. Il rationale storico era rimuovere friction
per setup dev/staging con `docker-compose` (Postgres senza TLS).

**Impatto negativo**: la review 2026-08-15 ha identificato che i probe
di conformance PFM (`probe_pfm_core_v1`, `probe_pfm_gis_v1`) usano
setup con TLS attivato via `.with_tls_mode(Require)`, mentre i comandi
operativi CLI (`pfm.rs`, `ops_cmd.rs`, ecc.) usano il default `Disabled`.
Risultato: **inconsistenza silente** — un endpoint che passa il probe
può essere connesso senza TLS dai comandi write. Confusione
particolarmente pericolosa perché il comportamento cambia in base al
comando invece che al DSN.

## Decisione

`PostgresProvider::default()` diventa `PostgresTlsMode::Require` (breaking).

Aggiungiamo un costruttore esplicito con nome che comunica il rischio:

```rust
impl PostgresProvider {
    /// Provider senza TLS. **Solo per test/dev locali**. Rifiuta
    /// silenziosamente in produzione: usare `default()` + configurazione
    /// TLS esplicita.
    pub fn insecure_local() -> Self { ... }
}
```

**Migrazione**:
- `docker-compose.yml` / CI usano `insecure_local()`.
- `test_suite` live usa `insecure_local()`.
- `PostgresCommandContext::for_pfm(dsn_env)` usa `default()`
  (era `Disabled` → ora `Require`).
- Rimosso `PostgresCommandContext::for_pfm_secure`: coincide con
  `for_pfm`.
- Aggiungiamo `PostgresCommandContext::for_pfm_insecure_local` per
  test/dev con nome esplicito.

## Conseguenze

**Positive**:
- Consumer PFM non può connettersi in produzione a plaintext per errore.
- Comando `doctor` / `diagnose` non falsifica più il TLS status.
- Coerenza fra probe e path operativi.

**Negative**:
- Breaking release: ogni consumer che dipende da `PostgresProvider::default()`
  senza materiale TLS smette di funzionare finché non passa a
  `insecure_local()` o fornisce material TLS.
- CI dev-env `docker-compose` richiede aggiornamento config code.

**Non copre**:
- La modalità `Prefer` opportunistic (fallback plaintext se server non
  supporta TLS) non è aggiunta — richiederebbe rivedere il connection
  path `tokio-postgres-rustls` e non è priorità.

## Contratto migrazione

- Bump versione major driver (`plenora-db-postgres`) da 1.1.x → 1.2.0.
- Bump SDK Python `plenora-database-py` da 0.8.x → 0.9.0.
- `CHANGELOG.md` con sezione dedicata "BREAKING: TLS default".
- Note migrazione con snippet before/after e checklist verifica.

## Alternative considerate

- **Mantenere `Disabled` + warning tracing**: non risolve il problema
  perché il warning non blocca la connessione plaintext in produzione.
- **Aggiungere modalità `Prefer`**: refactor più grosso, effort
  sproporzionato rispetto al valore (99% dei setup produzione richiede
  Require esplicito).
- **Rimuovere `default()` e forzare costruzione esplicita**: rompe più
  consumer del necessario; `Require` con webpki è il default ragionevole
  per il 90% dei casi.
