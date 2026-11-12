# Registro delle perdite

Generato da `loss_registry.rs` con `scripts/render_loss_registry.py`.

| ID | Applicabilità | Categoria | Severità | Predicato | Modalità | Alias PostgreSQL |
| --- | --- | --- | --- | --- | --- | --- |
| pg.add_nullable | Postgres | NativeType | Information | NoChange | NoChange | None |
| pg.arrow_mismatch | Postgres | NativeType | DataLoss | Conservative | Conservative | None |
| pg.crs | Postgres | Crs | DataLoss | Conservative | Conservative | None |
| pg.jsonb | Postgres | NativeType | Warning | PgJson | Mixed | None |
| pg.missing_column | Postgres | NativeType | DataLoss | Conservative | Conservative | None |
| pg.nullability | Postgres | Nullability | DataLoss | NoChange | NoChange | None |
| pg.numeric | Postgres | Precision | DataLoss | PgNumeric | Mixed | None |
| pg.pad_text | Postgres | NativeType | DataLoss | PadText | Exact | None |
| pg.srid | Postgres | Srid | DataLoss | Conservative | Conservative | None |
| pg.temporal | Postgres | Precision | DataLoss | PgTemporal | Mixed | None |
| pg.text_limit | Postgres | Precision | DataLoss | PgText | HardReject | None |
| ss.add_nullable | Sqlserver | NativeType | Information | NoChange | NoChange | None |
| ss.binary_limit | Sqlserver | Precision | DataLoss | BinaryLimit | Exact | None |
| ss.encoding | Sqlserver | Encoding | Warning | SsEncoding | Mixed | None |
| ss.offset | Sqlserver | Precision | Warning | SsOffset | Exact | None |
| ss.temporal | Sqlserver | Precision | DataLoss | SsTemporal | Exact | None |
| ss.text_limit | Sqlserver | Precision | DataLoss | SsText | Mixed | None |
| ss.xml | Sqlserver | NativeType | Warning | Conservative | Conservative | None |
| store.empty_to_null | Shared | Nullability | DataLoss | EmptyToNull | Exact | None |
| store.every | Shared | NativeType | DataLoss | Conservative | Conservative | None |
| store.excess_spaces | Shared | NativeType | DataLoss | Conservative | Conservative | None |
| store.fractional_seconds | Shared | Precision | DataLoss | FractionalSeconds | Exact | None |
| store.json_numbers | Shared | Precision | DataLoss | JsonNumbers | Mixed | None |
| store.negative_zero | Shared | Precision | DataLoss | NegativeZero | Exact | None |
| store.non_ascii | Shared | Encoding | DataLoss | NonAscii | Conservative | None |
| store.pad_binary | Shared | NativeType | DataLoss | PadBinary | Exact | None |
| store.pad_text | Shared | NativeType | DataLoss | PadText | Mixed | Some(RuleId::PgPadText) |
| store.strip_trailing | Shared | NativeType | DataLoss | StripTrailing | Exact | None |
| store.underflow | Shared | Precision | DataLoss | Underflow | Exact | None |
