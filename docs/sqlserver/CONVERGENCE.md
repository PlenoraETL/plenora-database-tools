# Convergenza con PostgreSQL/PostGIS

Questa matrice impedisce di dichiarare parità sulla sola presenza di
un'interfaccia comune. Una voce è chiusa soltanto quando implementazione,
capability, prova live, documentazione e gate concordano.

| Area | PostgreSQL/PostGIS di riferimento | SQL Server | Condizione di uscita |
|---|---|---|---|
| Contratto Provider e data path | completo | completo | mantenere suite comune e gate live |
| Capability opzionali | cursor nominato, returning e savepoint non esposti | cursor nominato, returning e savepoint non esposti | **allineata**: i tre flag restano `false` finché esistono superficie pubblica e prova live |
| Write atomica e recovery | completo | completo | mantenere fault injection e staged swap |
| Schema evolution additiva | nullable, opt-in | nullable, opt-in | **chiusa**: DDL+dati atomici e rollback live |
| Spatial XY | completo | completo | mantenere roundtrip geometry/geography |
| Spatial Z/M/ZM | lossless | lossless | **chiusa**: WKB ISO e differenziale live |
| Tipi geometrici misti | supportati | Point+Polygon su geometry/geography | **chiusa**: metadata Arrow `mixed` e roundtrip live senza coercizione |
| AST spatial | catalogo tipizzato | 23 metodi nativi comuni tipizzati | **chiusa per il sottoinsieme comune pubblicato**: accessori, validazione, predicati, misure e processing su source fisiche, join fisici, CTE non ricorsive, derived e subquery non correlate; i nove output geometrici sono WKB Z/M-safe con profilo del risultato e prova live geometry/geography |
| Indice spatial | GiST+bbox/KNN | auto-grid `geometry`/`geography` | **chiusa per create/replace**: creazione atomica, catalogo, access path forzato e rollback fail-closed live; nessun claim di equivalenza KNN |
| Catalogo avanzato | partizioni, viste, RLS/ACL | temporal/graph/partizioni live-proven; external implementato | fixture external live e poi RLS/ACL |
| Matrice versioni | PostgreSQL 14-18 | SQL Server 2022 | campagne 2019/2025/Azure separate |
| TLS | CA privata+mTLS | CA privata+rotazione | policy server-forced e autenticazione certificato, se applicabile |

## Regole

- Nessun fallback silenzioso o appiattimento dimensionale.
- Una capability viene alzata solo nella revisione che aggiunge la prova live.
- Le differenze native documentate non sono gap se il contratto pubblico
  dichiara una semantica equivalente e verificata.
- `inspect_dataset.rs`, il validatore dei contratti e il pin di conformità
  restano congelati durante questa convergenza.
