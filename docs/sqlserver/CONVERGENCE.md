# Convergenza con PostgreSQL/PostGIS

Questa matrice impedisce di dichiarare parità sulla sola presenza di
un'interfaccia comune. Una voce è chiusa soltanto quando implementazione,
capability, prova live, documentazione e gate concordano.

| Area | PostgreSQL/PostGIS di riferimento | SQL Server | Condizione di uscita |
|---|---|---|---|
| Contratto Provider e data path | completo | completo | mantenere suite comune e gate live |
| Write atomica e recovery | completo | completo | mantenere fault injection e staged swap |
| Schema evolution additiva | nullable, opt-in | nullable, opt-in | **chiusa**: DDL+dati atomici e rollback live |
| Spatial XY | completo | completo | mantenere roundtrip geometry/geography |
| Spatial Z/M/ZM | lossless | lossless | **chiusa**: WKB ISO e differenziale live |
| AST spatial | catalogo tipizzato | fail-closed | sottoinsieme SQL Server tipizzato e capability esatta |
| Indice spatial | GiST+bbox/KNN | non pubblicizzato | create/introspection/plan proof live |
| Catalogo avanzato | partizioni, viste, RLS/ACL | parziale | temporal/graph/external/partizioni osservati |
| Matrice versioni | PostgreSQL 14-18 | SQL Server 2022 | campagne 2019/2025/Azure separate |
| TLS | CA privata+mTLS | CA privata+rotazione | policy server-forced e autenticazione certificato, se applicabile |

## Regole

- Nessun fallback silenzioso o appiattimento dimensionale.
- Una capability viene alzata solo nella revisione che aggiunge la prova live.
- Le differenze native documentate non sono gap se il contratto pubblico
  dichiara una semantica equivalente e verificata.
- `inspect_dataset.rs`, il validatore dei contratti e il pin di conformità
  restano congelati durante questa convergenza.
