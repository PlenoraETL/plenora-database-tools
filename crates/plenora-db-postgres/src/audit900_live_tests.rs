//! L3, L5 e L6 dell'audit; nessuna prova server viene dedotta dai test offline.
use crate::PostgresProvider;
use plenora_database_core::provider::{ParameterBag, ParameterValue as V, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;

fn fixture() -> (PostgresProvider, SecretString) {
    (
        PostgresProvider::insecure_local(),
        SecretString::new(
            std::env::var("PLENORA_TEST_POSTGRES_DSN").expect("DSN fixture PostgreSQL richiesta"),
        ),
    )
}

#[tokio::test]
#[ignore = "richiede PostgreSQL con PostGIS"]
async fn live_audit900_l3_typed_wkb() {
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    let (provider, secret) = fixture();
    let cancel = CancellationToken::new();
    for sql in [
        "DROP TABLE IF EXISTS public.audit900_spatial",
        "CREATE TABLE public.audit900_spatial(id integer PRIMARY KEY)",
        "INSERT INTO public.audit900_spatial VALUES (1)",
    ] {
        provider
            .execute_ddl(&secret, sql, &cancel)
            .await
            .expect("fixture L3");
    }
    // Il risultato scalare ST_AsGeoJSON evita di scambiare la qualifica di un
    // decoder geometrico con la prova del bind WKB qui sotto.
    let geometry = serde_json::json!({"kind":"spatial_value","expression":{"kind":"typed_parameter","name":"p","parameter_type":"binary"},"srid":4326,"semantics":"geometry"});
    let query = serde_json::from_value(serde_json::json!({
        "source":{"object":{"catalog":null,"schema":"public","object":"audit900_spatial"},"alias":null},
        "projection":[{"expression":{"kind":"spatial","function":"as_geo_json","arguments":[geometry]},"alias":"v"},{"expression":{"kind":"spatial","function":"srid","arguments":[geometry]},"alias":"srid"}]
    })).expect("query L3");
    let mut bytes = vec![1, 1, 0, 0, 0];
    bytes.extend(1_f64.to_le_bytes());
    bytes.extend(2_f64.to_le_bytes());
    for value in [
        V::Wkb {
            bytes: bytes.clone(),
            srid: Some(4326),
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geometry,
        },
        V::Bytes(bytes),
    ] {
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let parameters = ParameterBag::new([("p".into(), value)].into());
        let mut stream = provider
            .query(&secret, &query, &parameters, &budget, &cancel)
            .await
            .expect("WKB tipizzato");
        let batch = stream
            .next_batch(&cancel)
            .await
            .expect("batch")
            .expect("riga");
        assert_eq!(batch.num_rows(), 1);
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<plenora_database_core::arrow::array::StringArray>()
            .expect("GeoJSON")
            .value(0);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(text).expect("GeoJSON valido"),
            serde_json::json!({"type":"Point","coordinates":[1,2]})
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<plenora_database_core::arrow::array::Int32Array>()
                .expect("SRID")
                .value(0),
            4326
        );
        assert!(stream.next_batch(&cancel).await.expect("EOF").is_none());
    }
    let mut tx = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &ResourceBudget::new(ResourceLimits::default()).expect("budget raw"),
            &cancel,
        )
        .await
        .expect("begin raw L3");
    let bytes = [
        vec![1, 1, 0, 0, 0],
        1_f64.to_le_bytes().to_vec(),
        2_f64.to_le_bytes().to_vec(),
    ]
    .concat();
    let rows=tx.query(&Statement::new("SELECT ST_X(ST_GeomFromWKB($1,4326)), ST_Y(ST_GeomFromWKB($1,4326)), ST_SRID(ST_GeomFromWKB($1,4326))").with_params(vec![V::Bytes(bytes)]),&cancel).await.expect("testimone raw L3");
    assert_eq!(rows[0].values(), &[V::F64(1.0), V::F64(2.0), V::I32(4326)]);
    tx.rollback(&cancel).await.expect("rollback raw L3");
    provider
        .execute_ddl(&secret, "DROP TABLE public.audit900_spatial", &cancel)
        .await
        .expect("pulizia L3");
}

#[tokio::test]
#[ignore = "richiede PostgreSQL"]
#[allow(
    clippy::too_many_lines,
    reason = "sequenza completa dei controlli name e dei due percorsi L6"
)]
async fn live_audit900_l6_name_bytes() {
    use plenora_database_core::facade::execute_portable;
    use plenora_database_core::portable::{
        Expression, InsertStatement, PortableStatement, TableRef,
    };
    let (provider, secret) = fixture();
    let cancel = CancellationToken::new();
    for sql in [
        "DROP TABLE IF EXISTS public.audit900_name",
        "CREATE TABLE public.audit900_name(id integer PRIMARY KEY,n name)",
    ] {
        provider
            .execute_ddl(&secret, sql, &cancel)
            .await
            .expect("fixture L6");
    }
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = TransactionOptions::default();
    let mut control = provider
        .begin_transaction(&secret, &options, &budget, &cancel)
        .await
        .expect("begin controlli L6");
    let limits = control
        .query(&Statement::new("SHOW max_identifier_length"), &cancel)
        .await
        .expect("limite name");
    assert_eq!(limits[0].values()[0], V::String("63".into()));
    let cast = control
        .query(
            &Statement::new("SELECT octet_length($1::text::name::text)")
                .with_params(vec![V::String("x".repeat(64))]),
            &cancel,
        )
        .await
        .expect("controllo CAST, distinto dal binder NAME");
    assert_eq!(cast[0].values()[0], V::I32(63));
    control
        .rollback(&cancel)
        .await
        .expect("rollback controlli L6");
    for text in [
        "x".repeat(63),
        "x".repeat(64),
        "x".repeat(65),
        "\u{e9}".repeat(32),
    ] {
        for native in [false, true] {
            let mut tx = provider
                .begin_transaction(&secret, &options, &budget, &cancel)
                .await
                .expect("begin L6");
            let inserted = if native {
                tx.execute(
                    &Statement::new("INSERT INTO public.audit900_name VALUES (1,$1)")
                        .with_params(vec![V::String(text.clone())]),
                    &cancel,
                )
                .await
            } else {
                let operation = PortableStatement::Insert(InsertStatement {
                    table: TableRef {
                        schema: Some("public".into()),
                        name: "audit900_name".into(),
                    },
                    columns: vec!["id".into(), "n".into()],
                    values: vec![vec![
                        Expression::literal(V::I32(1)),
                        Expression::literal(V::String(text.clone())),
                    ]],
                    returning: vec![],
                });
                execute_portable(tx.as_mut(), &operation, &cancel).await
            };
            if !native && text.len() > 63 {
                assert!(inserted.is_err(), "name oltre limite ammesso");
            } else if text.len() == 63 {
                inserted.as_ref().expect("controllo positivo 63 byte");
            }
            match inserted {
                Ok(_) => {
                    let rows = tx
                        .query(
                            &Statement::new("SELECT n::text, octet_length(n::text) FROM public.audit900_name WHERE id=1"),
                            &cancel,
                        )
                        .await
                        .expect("lettura name");
                    let exact = rows[0].values()[0] == V::String(text.clone());
                    println!(
                        "L6 native={native} bytes={} stored_bytes={:?} exact={exact}",
                        text.len(),
                        rows[0].values()[1]
                    );
                    // Il percorso nativo preesiste: registra il sospetto senza
                    // attribuirlo al cancello introdotto nella 9.0.0.
                    if !native {
                        assert!(exact, "perdita nel percorso ammesso");
                    }
                }
                Err(error) => println!(
                    "L6 native={native} bytes={} rifiuto={:?}/{:?}",
                    text.len(),
                    error.category,
                    error.phase
                ),
            }
            tx.rollback(&cancel).await.expect("rollback L6");
        }
    }
    provider
        .execute_ddl(&secret, "DROP TABLE public.audit900_name", &cancel)
        .await
        .expect("pulizia L6");
}

#[tokio::test]
#[ignore = "richiede PostgreSQL e privilegio CREATE EVENT TRIGGER"]
async fn live_audit900_l5_comment_search_path() {
    use crate::write_visibility_live_tests::{operation, write_stream};
    use plenora_database_core::arrow::{array::Int32Array, DataType, Field, RecordBatch, Schema};
    use plenora_database_core::provider::{BatchStream, ProviderFuture};
    use std::sync::Arc;
    struct One(Arc<Schema>, Option<RecordBatch>);
    impl BatchStream for One {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.0)
        }
        fn next_batch<'a>(
            &'a mut self,
            _: &'a CancellationToken,
        ) -> ProviderFuture<'a, Option<RecordBatch>> {
            Box::pin(std::future::ready(Ok(self.1.take())))
        }
    }
    let (provider, secret) = fixture();
    let client = PostgresProvider::connect(&secret)
        .await
        .expect("admin fixture");
    client.batch_execute("DROP EVENT TRIGGER IF EXISTS audit900_comment_event; DROP SCHEMA IF EXISTS audit900_path CASCADE; CREATE SCHEMA audit900_path; CREATE TABLE audit900_path.audit_log(tag text NOT NULL); CREATE FUNCTION audit900_path.log_comment() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() WHERE schema_name='audit900_path') THEN INSERT INTO audit_log(tag) VALUES (TG_TAG); END IF; END; $$; CREATE EVENT TRIGGER audit900_comment_event ON ddl_command_end WHEN TAG IN ('COMMENT') EXECUTE FUNCTION audit900_path.log_comment(); SET search_path=audit900_path,public; CREATE TABLE audit900_path.dest(id integer); COMMENT ON COLUMN audit900_path.dest.id IS 'baseline';").await.expect("controllo positivo COMMENT");
    assert_eq!(
        client
            .query("SELECT tag FROM audit900_path.audit_log", &[])
            .await
            .expect("log baseline")
            .len(),
        1
    );
    let dsn = secret.expose();
    let scoped = if dsn.contains("://") {
        format!(
            "{dsn}{}options=-csearch_path%3Daudit900_path%2Cpublic",
            if dsn.contains('?') { "&" } else { "?" }
        )
    } else {
        format!("{dsn} options='-c search_path=audit900_path,public'")
    };
    let scoped = SecretString::new(scoped);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)
        .with_metadata(std::collections::HashMap::from([(
            plenora_database_core::protocol::FIELD_ID.into(),
            "1".into(),
        )]))]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .expect("batch L5");
    let mut op = operation(
        "dest_writer",
        plenora_database_core::plan::WriteMode::Create,
    );
    op.target.schema = Some("audit900_path".into());
    let outcome = write_stream(&provider, &scoped, &op, Box::new(One(schema, Some(batch)))).await;
    client
        .batch_execute(
            "DROP EVENT TRIGGER audit900_comment_event; DROP SCHEMA audit900_path CASCADE",
        )
        .await
        .expect("pulizia L5");
    // La scelta COMMENT resta invariata: il test registra la misura e
    // accetta solo successo o lo specifico NotFound in Write del sospetto.
    if let Err(error) = &outcome {
        println!(
            "L5 categoria={:?} fase={:?} diagnostico={:?}",
            error.category, error.phase, error.message
        );
    }
    if let Err(error) = outcome {
        assert_eq!(error.phase, plenora_database_core::ErrorPhase::Write);
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::NotFound,
            "L5: rifiuto diverso dal sospetto COMMENT"
        );
    }
}
