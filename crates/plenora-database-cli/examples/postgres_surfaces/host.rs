use super::{
    input, Arc, BatchStream, CancellationToken, File, PostgresProvider, ProviderFuture, Result,
    SecretString,
};
use plenora_database_engine::runtime_binding::{
    ArrowSink, ArtifactResolver, ConnectionResolver, RuntimeBinding, RuntimeBody,
    RuntimeConnection, RuntimeInvocation, RuntimeRequest, RuntimeTarget, CAPABILITY_NAME,
    CAPABILITY_NAME_KEY, CAPABILITY_VERSION, INPUT_CONTRACT, JSON_CONTENT_TYPE, MESSAGE_ID,
    OPERATION, OPERATION_VERSION,
};
use serde_json::{json, Value};
use std::io::Write;

struct Host<'a> {
    request: &'a Value,
    provider: Arc<PostgresProvider>,
    secret: SecretString,
}
impl ConnectionResolver for Host<'_> {
    fn open<'a>(&'a self, _: &'a RuntimeTarget) -> ProviderFuture<'a, RuntimeConnection> {
        Box::pin(async {
            Ok(RuntimeConnection {
                provider: self.provider.clone(),
                secret: self.secret.clone(),
            })
        })
    }
}
struct Sink(File);
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
impl ArrowSink for Sink {
    fn finish(self: Box<Self>) -> Result<()> {
        Ok(())
    }
    fn abort(self: Box<Self>) {}
}
impl ArtifactResolver for Host<'_> {
    fn read_document<'a>(&'a self, reference: &'a str, max: usize) -> ProviderFuture<'a, Vec<u8>> {
        Box::pin(async move {
            assert_eq!(reference, "artifact://qualification/operation");
            let bytes = serde_json::to_vec(&self.request["operation"]).expect("operation");
            assert!(bytes.len() <= max);
            Ok(bytes)
        })
    }
    fn open_write_input<'a>(
        &'a self,
        _: &'a RuntimeRequest,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async {
            Ok(
                Box::new(input(self.request["input"].as_str().expect("input")))
                    as Box<dyn BatchStream>,
            )
        })
    }
    fn open_read_sink<'a>(
        &'a self,
        _: &'a RuntimeRequest,
    ) -> ProviderFuture<'a, Box<dyn ArrowSink>> {
        Box::pin(async {
            Ok(Box::new(Sink(
                File::create(self.request["output"].as_str().expect("output"))
                    .expect("output file"),
            )) as Box<dyn ArrowSink>)
        })
    }
}

pub async fn invoke(
    request: &Value,
    provider: Arc<PostgresProvider>,
    secret: SecretString,
) -> Value {
    let write = request["action"] == "write";
    let operation = if write {
        "database.write"
    } else {
        "database.read"
    };
    let contract = if write {
        "plenora-database-write-input-v2"
    } else {
        "plenora-database-read-input-v1"
    };
    let mut payload = json!({"provider":"postgres", "secret_environment":"qualification", "provider_arguments":[], "operation_path":"artifact://qualification/operation"});
    if write {
        payload["accept_loss"] = request["accept_loss"].clone();
    }
    let invocation = RuntimeInvocation {
        content_type: JSON_CONTENT_TYPE.to_owned(),
        metadata: serde_json::from_value(json!({
            (MESSAGE_ID): "018f3d84-7b2c-7f00-8000-000000000101",
            (CAPABILITY_NAME_KEY): CAPABILITY_NAME,
            (CAPABILITY_VERSION): "1", (OPERATION): operation,
            (OPERATION_VERSION): if write {"2"} else {"1"}, (INPUT_CONTRACT): contract,
        }))
        .expect("runtime metadata"),
        payload,
    };
    let host = Host {
        request,
        provider,
        secret,
    };
    let result = RuntimeBinding::new(&host, &host)
        .invoke(&invocation, &CancellationToken::new())
        .await;
    let ok = !result.is_error();
    let document = match result.body {
        RuntimeBody::Json(value) => value,
        RuntimeBody::ArrowStream(delivered) => json!({"read_loss":delivered.read_loss}),
    };
    json!({"ok":ok, "document":document})
}
