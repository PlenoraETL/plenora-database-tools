# ADR 0012 — Unità di misura esplicite per predicati spaziali portable

Stato: **accettato**
Data: 2026-08-15
Target release: `py-v0.9.0`, core `1.2.0`.

## Contesto

`SpatialPredicate::DWithin { distance_meters: f64 }` promette metri
ma il significato dipende dalla combinazione
`SpatialReference.semantics × srid`:

| Semantics  | SRID                                          | Unità reali              |
|------------|-----------------------------------------------|--------------------------|
| Geography  | qualsiasi                                     | metri (garantito PostGIS)|
| Geometry   | geografico (4326, 4269, …)                    | **gradi** ⚠️              |
| Geometry   | proiettato metri (3857, 25832)                | metri                    |
| Geometry   | proiettato altre unità (piedi US, chains)     | **CRS units** ⚠️          |

Il fix attuale (`spatial_policy::validate_predicate`) rifiuta la
combinazione `Geometry + SRID geografico` per prevenire silent wrong
result. Ma per `Geometry + SRID proiettato in unità non-metriche`
(EPSG:2229 piedi US, EPSG:27700, ecc.) il compiler non ha catalogo
EPSG lato client per detectare — la query passa e produce risultati
sbagliati numericamente.

## Decisione

Sostituire `distance_meters: f64` con enum tipizzato:

```rust
pub enum SpatialPredicate {
    // ...
    DWithin {
        distance: f64,
        unit: DistanceUnit,
    },
}

pub enum DistanceUnit {
    /// Distanza in metri veri. Richiede SpatialSemantics::Geography.
    Meters,
    /// Distanza in unità del CRS della colonna. Richiede
    /// SpatialSemantics::Geometry. Consumer è responsabile di
    /// conoscere l'unità del CRS.
    CrsUnits,
}
```

**Validazione policy** (`spatial_policy::validate_predicate`):
- `unit=Meters` + `Geography` → OK.
- `unit=Meters` + `Geometry` → `InvalidPlan`.
- `unit=CrsUnits` + `Geometry` → OK (consumer sa cosa fa).
- `unit=CrsUnits` + `Geography` → `InvalidPlan`.

Nessuna conversione implicita. Il compiler non fa aritmetica su unità.

## Conseguenze

**Positive**:
- Impossibile scrivere `DWithin(100, Meters)` su una colonna geometry
  proiettata in piedi — la policy rifiuta.
- Semantica esplicita nel codice: `DistanceUnit::Meters` vs
  `CrsUnits` comunica intent al lettore.
- Rimozione della dipendenza dalla lista `GEOGRAPHIC_SRIDS` (che era
  best-effort e non copriva tutti i geografici EPSG).

**Negative**:
- Breaking change: `SpatialPredicate::DWithin` struct-variant cambia
  campi. Tutti i consumer (Rust + Python + JSON serializzati) devono
  migrare.
- Serialization JSON cambia: `{"kind":"d_within","distance_meters":100}`
  → `{"kind":"d_within","distance":100,"unit":"meters"}`. Consumer
  che persiste query serializzate deve gestire migration.

**Non copre**:
- Detection automatica dell'unità CRS (richiederebbe catalogo EPSG
  embedded — ~2 MB, out of scope).
- `Miles`, `Feet`, ecc. — solo `Meters` e `CrsUnits`, minimalismo.

## Migrazione Python

```python
# Before
p.spatial_predicate.DWithin(distance_meters=100)

# After
p.spatial_predicate.DWithin(distance=100, unit="meters")   # Geography
p.spatial_predicate.DWithin(distance=100, unit="crs_units")  # Geometry
```

## Alternative considerate

- **Doc-only warning** (attuale, doc-fix in `b8a822e`): non risolve il
  silent wrong result per `Geometry` + SRID proiettato non-metri —
  chiude solo il caso `Geometry` + geografico.
- **Rename `distance_meters` → `distance` senza enum**: chiude
  l'ambiguità nel nome ma il consumer resta senza segnale di intent.
  Non è sufficiente.
- **Enum con `Meters | Degrees | Feet | ...`**: over-engineering per
  il caso corrente. `Meters` + `CrsUnits` (delega al CRS) copre il
  99% dei casi PFM.
