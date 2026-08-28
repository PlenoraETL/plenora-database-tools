#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::limits::Limits;
use plenora_database_core::relational::{validate_query_operation, QueryOperation};
use plenora_database_sql::{Dialect, DialectCapabilities, RenderedSql, Renderer};

const DIALECTS: [Dialect; 7] = [
    Dialect::Postgres,
    Dialect::Mysql,
    Dialect::SqlServer,
    Dialect::Oracle,
    Dialect::Db2,
    Dialect::Sqlite,
    Dialect::Duckdb,
];

fn check_rendered(rendered: &RenderedSql) {
    assert!(!rendered.sql.is_empty());
    // Nessun testo SQL renderizzato può trasportare un NUL: finirebbe nel
    // protocollo di rete del driver.
    assert!(!rendered.sql.contains('\0'));
    for (index, bind) in rendered.binds.iter().enumerate() {
        // Gli ordinali dei bind sono densi e a base 1: il driver li usa per
        // posizionare i valori senza rileggere il testo SQL.
        assert_eq!(bind.ordinal, index + 1);
        assert!(!bind.name.is_empty());
        assert!(!bind.name.contains('\0'));
    }
}

fuzz_target!(|input: &[u8]| {
    let Ok(query) = serde_json::from_slice::<QueryOperation>(input) else {
        return;
    };

    // La validazione portabile precede ogni dialect: se fallisce, nessun
    // renderer può accettare la stessa query.
    let portable = validate_query_operation(&query, &Limits::default());

    for dialect in DIALECTS {
        for spatial_intersects in [false, true] {
            let renderer = Renderer::new(dialect, DialectCapabilities { spatial_intersects });

            let Ok(rendered) = renderer.render_query(&query) else {
                continue;
            };
            // SQL Server restringe ulteriormente i limiti portabili, quindi un
            // render accettato implica sempre una query portabile valida.
            assert!(portable.is_ok());
            check_rendered(&rendered);

            // Il rendering è deterministico a parità di dialect e capability.
            let again = renderer
                .render_query(&query)
                .expect("rendering deterministico");
            assert_eq!(again, rendered);
            assert_eq!(again.fingerprint(), rendered.fingerprint());

            // Le parti native devono ricomporre esattamente il testo nativo:
            // i preflight provider si appoggiano a questa separazione senza
            // riscrivere lessicalmente il SQL.
            if let Ok(parts) = renderer.render_query_native_spatial_parts(&query) {
                let native = renderer
                    .render_query_native_spatial(&query)
                    .expect("variante nativa coerente con le sue parti");
                assert_eq!(parts.binds, native.binds);
                assert_eq!(format!("{}{}", parts.with_clause, parts.body), native.sql);
                check_rendered(&native);
            }
        }
    }
});
