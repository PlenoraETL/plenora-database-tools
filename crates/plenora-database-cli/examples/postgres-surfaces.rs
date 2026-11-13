//! Qualification host only: real Provider / `RuntimeBinding`, no metadata decisions.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use arrow_array::RecordBatch;
use arrow_ipc::{reader::FileReader, writer::StreamWriter};
use arrow_schema::SchemaRef;
use plenora_database_core::provider::{
    BatchStream, ParameterBag, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::{CancellationToken, Result};
use plenora_db_postgres::PostgresProvider;
use serde_json::{json, Value};
use std::{collections::VecDeque, fs::File, sync::Arc};

#[path = "postgres_surfaces/host.rs"]
mod host;

struct Input(SchemaRef, VecDeque<RecordBatch>);
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async { Ok(self.1.pop_front()) })
    }
}

fn input(path: &str) -> Input {
    let reader =
        FileReader::try_new(File::open(path).expect("input file"), None).expect("IPC file");
    Input(
        reader.schema(),
        reader.map(|b| b.expect("IPC batch")).collect(),
    )
}

async fn direct(
    request: &Value,
    provider: &PostgresProvider,
    secret: &SecretString,
) -> Result<Value> {
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let token = CancellationToken::new();
    if request["action"] == "preflight" {
        let operation =
            serde_json::from_value(request["operation"].clone()).expect("write operation");
        let stream = input(request["input"].as_str().expect("input"));
        let report = provider
            .dry_run_write_with_consent(
                None,
                &operation,
                Box::new(stream),
                &budget,
                &token,
                plenora_database_core::loss::LossConsent::from_accept_loss(
                    request["accept_loss"].as_bool().expect("consent"),
                ),
            )
            .await?;
        return Ok(json!({"execution_loss": report.execution_loss}));
    }
    if request["action"] == "write" {
        let operation =
            serde_json::from_value(request["operation"].clone()).expect("write operation");
        let stream = input(request["input"].as_str().expect("input"));
        let prepared = provider
            .prepare_write(secret, &operation, stream.schema(), &budget, &token)
            .await?;
        let outcome = provider
            .write_with_consent(
                secret,
                prepared,
                Box::new(stream),
                &budget,
                &token,
                request["accept_loss"].as_bool().expect("consent"),
            )
            .await?;
        outcome.validate()?;
        Ok(serde_json::to_value(outcome).expect("outcome"))
    } else {
        let operation =
            serde_json::from_value(request["operation"].clone()).expect("read operation");
        let mut stream = provider
            .read(
                secret,
                &operation,
                &ParameterBag::default(),
                &budget,
                &token,
            )
            .await?;
        let file = File::create(request["output"].as_str().expect("output")).expect("output file");
        let mut writer = StreamWriter::try_new(file, &stream.schema()).expect("IPC writer");
        while let Some(batch) = stream.next_batch(&token).await? {
            writer.write(&batch).expect("IPC batch");
        }
        writer.finish().expect("IPC finish");
        Ok(json!({"read_loss": stream.read_loss()}))
    }
}

#[tokio::main]
async fn main() {
    let path = std::env::args().nth(1).expect("request path");
    let request: Value =
        serde_json::from_slice(&std::fs::read(path).expect("request file")).expect("request JSON");
    let provider = Arc::new(PostgresProvider::insecure_local());
    let secret = SecretString::new(std::env::var("PLENORA_TEST_POSTGRES_DSN").expect("runner DSN"));
    let result = if request["surface"] == "runtime" {
        host::invoke(&request, provider, secret).await
    } else {
        match direct(&request, &provider, &secret).await {
            Ok(document) => json!({"ok": true, "document": document}),
            Err(error) => json!({"ok": false, "document": error.public_projection()}),
        }
    };
    println!("{}", serde_json::to_string(&result).expect("result"));
}
