//! Un commit senza conferma, provocato davvero, sulle superfici pubbliche.
//!
//! Un proxy TCP sta fra la CLI e il riferimento PostgreSQL. Inoltra tutto
//! finche la connessione che ha toccato la tabella di prova non invia
//! `COMMIT`: allora chiude il lato client **prima** di inoltrare il comando,
//! cosi il server riceve il commit ma la sua risposta non arriva mai. E il
//! caso che il contratto chiama esito ignoto, ed e lo stesso che produce una
//! rete che cade fra invio e conferma.
//!
//! La CLI deve dirlo con un envelope d'errore e un exit code diverso da zero
//! (SURF-014 e CLI-2.0 §4): un `status: ok`, o un exit 0, direbbe
//! all'orchestratore di proseguire su uno stato che nessuno ha verificato.
//!
//! `#[ignore]` per default; esegui con `python scripts/check_postgres_reference.py`.

#![cfg(test)]

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use serde_json::{json, Value};

const BIN: &str = env!("CARGO_BIN_EXE_plenora-database");
const PROXIED_SECRET: &str = "PLENORA_TEST_PROXIED_DSN";
const DIRECT_SECRET: &str = "PG_DSN";

/// La DSN del riferimento, come la legge il resto delle suite CLI.
fn dsn() -> String {
    std::env::var("PLENORA_TEST_POSTGRES_DSN")
        .or_else(|_| std::env::var("PG_DSN"))
        .expect("impostare PLENORA_TEST_POSTGRES_DSN o PG_DSN per la fixture live")
}

/// Host e porta della DSN `chiave=valore`, e la stessa DSN puntata altrove.
///
/// Una DSN con valori quotati non si riscrive per sostituzione di token: il
/// test la rifiuta invece di provare contro un indirizzo che non ha capito.
fn redirect(dsn: &str, port: u16) -> (String, String) {
    assert!(
        !dsn.contains('\''),
        "DSN con valori quotati non supportata dal proxy di prova"
    );
    let mut host = "localhost".to_owned();
    let mut upstream_port = "5432".to_owned();
    let mut rewritten = Vec::new();
    for token in dsn.split_whitespace() {
        if let Some(value) = token.strip_prefix("host=") {
            value.clone_into(&mut host);
        } else if let Some(value) = token.strip_prefix("port=") {
            value.clone_into(&mut upstream_port);
        } else {
            rewritten.push(token.to_owned());
        }
    }
    rewritten.push("host=127.0.0.1".to_owned());
    rewritten.push(format!("port={port}"));
    (format!("{host}:{upstream_port}"), rewritten.join(" "))
}

/// Proxy che interrompe il primo `COMMIT` di una connessione marcata.
///
/// Una connessione e marcata quando nel suo traffico compare `marker`, il
/// nome della tabella di prova: le connessioni di servizio — probe,
/// capability, introspezione — passano intatte.
struct CommitCutter {
    port: u16,
    cuts: Arc<AtomicUsize>,
}

impl CommitCutter {
    fn start(upstream: String, marker: &'static [u8]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
        let port = listener.local_addr().expect("indirizzo proxy").port();
        let cuts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&cuts);
        thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { return };
                let Ok(server) = TcpStream::connect(&upstream) else {
                    return;
                };
                let counter = Arc::clone(&counter);
                thread::spawn(move || relay(client, server, marker, &counter));
            }
        });
        Self { port, cuts }
    }

    fn cuts(&self) -> usize {
        self.cuts.load(Ordering::SeqCst)
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Riconosce il `COMMIT` della transazione marcata nel traffico del client,
/// anche quando marcatore o comando sono spezzati fra due letture TCP.
struct CommitScanner {
    marker: Vec<u8>,
    touched: bool,
    tail: Vec<u8>,
}

impl CommitScanner {
    fn new(marker: &[u8]) -> Self {
        Self {
            marker: marker.to_vec(),
            touched: false,
            tail: Vec::new(),
        }
    }

    /// `true` quando questa lettura completa il `COMMIT` dopo il marcatore.
    fn feed(&mut self, chunk: &[u8]) -> bool {
        // Il marcatore o il `COMMIT` possono arrivare spezzati su due
        // letture: si cerca nella coda della lettura precedente piu questa.
        let mut window = std::mem::take(&mut self.tail);
        window.extend_from_slice(chunk);
        self.touched |= contains(&window, &self.marker);
        let cut = self.touched && contains(&window, b"COMMIT");
        let keep = self.marker.len().max(b"COMMIT".len()).saturating_sub(1);
        self.tail = window[window.len().saturating_sub(keep)..].to_vec();
        cut
    }
}

/// Il proxy conserva la coda fra le letture: un marcatore e un `COMMIT`
/// spezzati su piu chunk vengono comunque riconosciuti.
#[test]
fn the_scanner_finds_a_marker_and_a_commit_split_across_reads() {
    let mut scanner = CommitScanner::new(b"plenora-marker");
    assert!(!scanner.feed(b"INSERT ... plenora-ma"));
    assert!(!scanner.feed(b"rker ... CO"));
    assert!(scanner.feed(b"MMIT"));
    let mut unmarked = CommitScanner::new(b"plenora-marker");
    assert!(!unmarked.feed(b"COMMIT"));
}

fn relay(mut client: TcpStream, mut server: TcpStream, marker: &[u8], cuts: &AtomicUsize) {
    let (Ok(mut client_back), Ok(mut server_back)) = (client.try_clone(), server.try_clone())
    else {
        return;
    };
    // Dal server al client: termina da solo quando il client viene chiuso.
    thread::spawn(move || {
        let _ = std::io::copy(&mut server_back, &mut client_back);
    });
    let mut scanner = CommitScanner::new(marker);
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = match client.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        let chunk = &buffer[..read];
        if scanner.feed(chunk) {
            // Prima si chiude il client, poi si inoltra: la conferma del
            // server non ha piu una strada per tornare indietro.
            let _ = client.shutdown(Shutdown::Both);
            let _ = server.write_all(chunk);
            cuts.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_secs(2));
            let _ = server.shutdown(Shutdown::Both);
            return;
        }
        if server.write_all(chunk).is_err() {
            break;
        }
    }
    let _ = server.shutdown(Shutdown::Both);
}

/// La CLI con il riferimento diretto in `PG_DSN` e quello passante dal proxy
/// in [`PROXIED_SECRET`].
fn cli(args: &[&str], proxied: &str) -> Command {
    let mut command = Command::new(BIN);
    command
        .args(args)
        .env(DIRECT_SECRET, dsn())
        .env(PROXIED_SECRET, proxied)
        .env("PLENORA_TLS_INSECURE_LOCAL", "1");
    for name in [
        "PLENORA_PG_CA_PATH",
        "PLENORA_PG_CLIENT_CERT_PATH",
        "PLENORA_PG_CLIENT_KEY_PATH",
    ] {
        command.env_remove(name);
    }
    command
}

fn scratch(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "plenora-outcome-unknown-{}-{name}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).expect("cartella di prova");
    directory
}

fn write_json(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec(value).expect("JSON")).expect("scrittura JSON");
}

fn ddl(sql: &str) {
    let output = cli(
        &["database-execute-ddl", "postgres", DIRECT_SECRET, sql],
        "",
    )
    .output()
    .expect("spawn CLI");
    assert!(
        output.status.success(),
        "DDL di preparazione fallito: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

/// Un solo documento JSON su stdout, niente su stderr (CLI-2.0 §4).
fn envelope(output: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.stderr.is_empty(),
        "stderr non vuoto: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = stdout.lines().filter(|line| !line.is_empty()).count();
    assert_eq!(lines, 1, "atteso un solo documento JSON: {stdout}");
    serde_json::from_str(&stdout).unwrap_or_else(|error| panic!("JSON: {error}: {stdout}"))
}

/// Gli assi che un commit ignoto per canale perso deve portare, e l'exit code
/// che li proietta.
fn assert_unknown_commit(output: &Output, command: &str) -> Value {
    let document = envelope(output);
    assert_eq!(
        document["status"], "error",
        "un commit ignoto riportato come successo: {document}"
    );
    assert_eq!(document["command"], command);
    let error = &document["error"];
    // ERR-001: la conferma e persa perche il canale e stato chiuso, quindi
    // la categoria e `io`, con il suo exit code (CLI-2.0 §8).
    assert_eq!(error["category"], "io", "{document}");
    assert_eq!(error["phase"], "commit", "{document}");
    assert_eq!(error["remote_effect"], "unknown", "{document}");
    assert_eq!(error["retry"]["kind"], "requires_recovery", "{document}");
    assert_eq!(error["provider"], "postgres", "{document}");
    assert_eq!(output.status.code(), Some(5), "{document}");
    document
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_write_with_a_lost_commit_acknowledgement_is_an_error_not_success() {
    const TABLE: &str = "plenora_cli_outcome_unknown_write";
    let (upstream, _) = redirect(&dsn(), 0);
    let cutter = CommitCutter::start(upstream, TABLE.as_bytes());
    let (_, proxied) = redirect(&dsn(), cutter.port);
    ddl(&format!("DROP TABLE IF EXISTS public.{TABLE}"));

    let directory = scratch("write");
    let data = directory.join("input.arrow");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch");
    let file = std::fs::File::create(&data).expect("file Arrow");
    let mut writer = FileWriter::try_new(file, &schema).expect("writer");
    writer.write(&batch).expect("batch scritto");
    writer.finish().expect("file chiuso");

    let operation = directory.join("write.json");
    write_json(
        &operation,
        &json!({
            "target": {"schema": "public", "object": TABLE},
            "mode": "create",
            "mapping_policy": "compatible",
            "transaction_profile": "single_transaction",
        }),
    );
    let request = directory.join("request.json");
    write_json(
        &request,
        &json!({
            "provider": "postgres",
            "secret_environment": PROXIED_SECRET,
            "operation_path": operation.to_str().expect("percorso UTF-8"),
        }),
    );
    let output = cli(
        &[
            "write",
            "--input",
            request.to_str().expect("percorso UTF-8"),
            "--data",
            data.to_str().expect("percorso UTF-8"),
        ],
        &proxied,
    )
    .output()
    .expect("spawn CLI");
    assert_eq!(cutter.cuts(), 1, "il proxy non ha interrotto il commit");
    let document = assert_unknown_commit(&output, "write");
    let outcome = &document["error"]["details"]["write_outcome"];
    assert_eq!(outcome["status"], "outcome_unknown", "{document}");
    assert_eq!(
        outcome["recovery"]["automatic_retry_allowed"], false,
        "{document}"
    );
    assert_eq!(
        document["error"]["execution_id"], outcome["execution_id"],
        "{document}"
    );

    ddl(&format!("DROP TABLE IF EXISTS public.{TABLE}"));
    let _ = std::fs::remove_dir_all(directory);
}

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_execute_with_a_lost_commit_acknowledgement_is_an_error_not_success() {
    const TABLE: &str = "plenora_cli_outcome_unknown_execute";
    let (upstream, _) = redirect(&dsn(), 0);
    let cutter = CommitCutter::start(upstream, TABLE.as_bytes());
    let (_, proxied) = redirect(&dsn(), cutter.port);
    ddl(&format!("DROP TABLE IF EXISTS public.{TABLE}"));
    ddl(&format!("CREATE TABLE public.{TABLE} (id bigint)"));

    let directory = scratch("execute");
    let request = directory.join("request.json");
    write_json(
        &request,
        &json!({
            "provider": "postgres",
            "secret_environment": PROXIED_SECRET,
            "sql": format!("INSERT INTO public.{TABLE} (id) VALUES (1)"),
            "allow_raw": true,
        }),
    );
    let output = cli(
        &[
            "execute",
            "--input",
            request.to_str().expect("percorso UTF-8"),
        ],
        &proxied,
    )
    .output()
    .expect("spawn CLI");
    assert_eq!(cutter.cuts(), 1, "il proxy non ha interrotto il commit");
    let document = assert_unknown_commit(&output, "execute");
    assert_eq!(
        document["error"]["details"]["commit"]["status"], "outcome_unknown",
        "{document}"
    );

    ddl(&format!("DROP TABLE IF EXISTS public.{TABLE}"));
    let _ = std::fs::remove_dir_all(directory);
}

/// Lo stesso commit perso sul percorso `append`, che in PostgreSQL passa
/// dalla diagnostica di riga: la categoria e la causa (`io`, ERR-016), con
/// effetto ignoto e nessun retry automatico. La disposizione di quel
/// percorso e `quarantine`, ammessa da ERR-006.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[test]
fn live_append_with_a_lost_commit_acknowledgement_is_an_io_error() {
    const TABLE: &str = "plenora_cli_outcome_unknown_append";
    let (upstream, _) = redirect(&dsn(), 0);
    let cutter = CommitCutter::start(upstream, TABLE.as_bytes());
    let (_, proxied) = redirect(&dsn(), cutter.port);
    ddl(&format!("DROP TABLE IF EXISTS public.{TABLE}"));
    ddl(&format!("CREATE TABLE public.{TABLE} (id bigint NOT NULL)"));

    let directory = scratch("append");
    let data = directory.join("input.arrow");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch");
    let file = std::fs::File::create(&data).expect("file Arrow");
    let mut writer = FileWriter::try_new(file, &schema).expect("writer");
    writer.write(&batch).expect("batch scritto");
    writer.finish().expect("file chiuso");
    let operation = directory.join("write.json");
    write_json(
        &operation,
        &json!({
            "target": {"schema": "public", "object": TABLE},
            "mode": "append",
            "mapping_policy": "compatible",
            "transaction_profile": "single_transaction",
        }),
    );
    let request = directory.join("request.json");
    write_json(
        &request,
        &json!({
            "provider": "postgres",
            "secret_environment": PROXIED_SECRET,
            "operation_path": operation.to_str().expect("percorso UTF-8"),
        }),
    );
    let output = cli(
        &[
            "write",
            "--input",
            request.to_str().expect("percorso UTF-8"),
            "--data",
            data.to_str().expect("percorso UTF-8"),
        ],
        &proxied,
    )
    .output()
    .expect("spawn CLI");
    assert_eq!(cutter.cuts(), 1, "il proxy non ha interrotto il commit");
    let document = envelope(&output);
    assert_eq!(document["status"], "error", "{document}");
    let error = &document["error"];
    assert_eq!(error["category"], "io", "{document}");
    assert_eq!(error["phase"], "commit", "{document}");
    assert_eq!(error["remote_effect"], "unknown", "{document}");
    assert_ne!(error["retry"]["kind"], "safe", "{document}");
    assert_eq!(output.status.code(), Some(5), "{document}");

    ddl(&format!("DROP TABLE IF EXISTS public.{TABLE}"));
    let _ = std::fs::remove_dir_all(directory);
}
