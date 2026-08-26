use std::ffi::{OsStr, OsString};
use std::process::{Command, Output};

const CHILD_SECRET_ENV: &str = "PLENORA_CLI_LIVE_SECRET";
const CHILD_CA_PATH_ENV: &str = "PLENORA_CLI_LIVE_CA_PATH";
const CHILD_CLIENT_CERT_PATH_ENV: &str = "PLENORA_CLI_LIVE_CLIENT_CERT_PATH";
const CHILD_CLIENT_KEY_PATH_ENV: &str = "PLENORA_CLI_LIVE_CLIENT_KEY_PATH";

fn required(name: &str) -> OsString {
    std::env::var_os(name).unwrap_or_else(|| panic!("{name} required for live CLI probe"))
}

fn required_text(name: &str) -> String {
    required(name)
        .into_string()
        .unwrap_or_else(|_| panic!("{name} must be valid Unicode"))
}

fn run_probe(arguments: &[String], environment: &[(&str, &OsStr)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_plenora-database"));
    command.args(arguments);
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().expect("run live plenora-database probe")
}

/// L'uscita di un comando riuscito, verificata prima di essere letta.
///
/// Le tre condizioni valgono per ogni comando del CLI, e sono separate dal
/// contenuto perche il contenuto cambia da comando a comando: uscita zero,
/// nessun segreto stampato, e `stderr` vuoto — un comando riuscito che scrive
/// su `stderr` sta dicendo qualcosa che nessuno legge.
fn assert_clean_output(output: &Output, forbidden: &[&OsStr]) -> String {
    let stdout = String::from_utf8(output.stdout.clone()).expect("UTF-8 stdout");
    let stderr = String::from_utf8(output.stderr.clone()).expect("UTF-8 stderr");
    for value in forbidden {
        if let Some(value) = value.to_str() {
            assert!(!stdout.contains(value), "segreto trapelato su stdout");
            assert!(!stderr.contains(value), "segreto trapelato su stderr");
        }
    }
    assert!(
        output.status.success(),
        "comando fallito: stdout={stdout:?}, stderr={stderr:?}"
    );
    assert!(
        stderr.is_empty(),
        "un comando riuscito ha scritto su stderr"
    );
    stdout
}

fn assert_successful_probe(output: &Output, provider: &str, forbidden: &[&OsStr]) {
    let stdout = String::from_utf8(output.stdout.clone()).expect("UTF-8 stdout");
    let stderr = String::from_utf8(output.stderr.clone()).expect("UTF-8 stderr");
    for value in forbidden {
        if let Some(value) = value.to_str() {
            assert!(!stdout.contains(value), "sensitive value leaked to stdout");
            assert!(!stderr.contains(value), "sensitive value leaked to stderr");
        }
    }
    assert!(
        output.status.success(),
        "live CLI probe failed: stdout={stdout:?}, stderr={stderr:?}"
    );
    assert!(stderr.is_empty(), "successful probe wrote stderr");
    let envelope: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("JSON success envelope");
    assert_eq!(envelope["schema_version"], 1);
    assert_eq!(envelope["connection"]["provider"], provider);
    assert!(envelope["connection"]["server_version"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
}

#[test]
#[ignore = "requires the pinned MySQL reference fixture"]
fn live_database_probe_mysql_private_ca() {
    let host = required_text("PLENORA_MYSQL_HOST");
    let database = required_text("PLENORA_MYSQL_DATABASE");
    let username = required_text("PLENORA_MYSQL_USER");
    let password = required("PLENORA_MYSQL_PASSWORD");
    let ca = required("PLENORA_MYSQL_CA");
    let arguments = [
        "database-probe".to_owned(),
        "mysql".to_owned(),
        CHILD_SECRET_ENV.to_owned(),
        host,
        database,
        username,
        "--tls-ca-path-env".to_owned(),
        CHILD_CA_PATH_ENV.to_owned(),
    ];
    let output = run_probe(
        &arguments,
        &[(CHILD_SECRET_ENV, &password), (CHILD_CA_PATH_ENV, &ca)],
    );
    assert_successful_probe(&output, "mysql", &[&password, &ca]);
}

#[test]
#[ignore = "requires the pinned SQL Server reference fixture"]
fn live_database_probe_sqlserver_private_ca() {
    let host = required_text("PLENORA_SQLSERVER_HOST");
    let database = required_text("PLENORA_SQLSERVER_DATABASE");
    let username = required_text("PLENORA_SQLSERVER_USER");
    let password = required("PLENORA_SQLSERVER_PASSWORD");
    let ca = required("PLENORA_SQLSERVER_PRIVATE_CA");
    let arguments = [
        "database-probe".to_owned(),
        "sqlserver".to_owned(),
        CHILD_SECRET_ENV.to_owned(),
        host,
        database,
        username,
        "--tls-ca-path-env".to_owned(),
        CHILD_CA_PATH_ENV.to_owned(),
    ];
    let output = run_probe(
        &arguments,
        &[(CHILD_SECRET_ENV, &password), (CHILD_CA_PATH_ENV, &ca)],
    );
    assert_successful_probe(&output, "sqlserver", &[&password, &ca]);
}

/// I due comandi generici di esecuzione, su un motore che prima non ne aveva
/// nessuno.
///
/// # Cosa cambia
///
/// `PostgreSQL` aveva `execute-sql` ed `execute-scalar`, `MySQL` aveva i propri
/// con il prefisso, `MariaDB` e SQL Server **niente**: dal CLI erano provider
/// che si potevano interrogare e non usare.
///
/// La famiglia generica esisteva gia per la probe e l'introspezione, e la
/// ragione scritta allora vale identica qui: gli adapter implementano lo stesso
/// contratto, e una quarta copia degli stessi comandi diverge alla prima
/// correzione applicata a una sola.
///
/// # Perche non prima
///
/// Il contratto che serve e `TransactionScope`, e SQL Server pubblicava
/// `scope: Transaction` senza implementarlo: un comando generico avrebbe
/// risposto `Unsupported` su un quarto dei provider che accetta. Quello scope
/// e arrivato con la transazione applicativa, e questa prova e la ragione per
/// cui si vede.
///
/// # Cosa attraversa
///
/// Lo scalare, che deve rendere il valore; e la scrittura, che deve rendere il
/// numero di righe toccate e committare. La seconda usa una tabella temporanea
/// di sessione — `#`, che SQL Server elimina alla chiusura — cosi la prova non
/// lascia niente dietro anche se cade a meta.
#[test]
#[ignore = "richiede SQL Server live esplicito e la CA privata materializzata"]
fn live_database_execute_sqlserver_private_ca() {
    let host = required_text("PLENORA_SQLSERVER_HOST");
    let database = required_text("PLENORA_SQLSERVER_DATABASE");
    let username = required_text("PLENORA_SQLSERVER_USER");
    let password = required("PLENORA_SQLSERVER_PASSWORD");
    let ca = required("PLENORA_SQLSERVER_PRIVATE_CA");

    let scalar = [
        "database-execute-scalar".to_owned(),
        "sqlserver".to_owned(),
        CHILD_SECRET_ENV.to_owned(),
        "SELECT 42".to_owned(),
        host.clone(),
        database.clone(),
        username.clone(),
        "--tls-ca-path-env".to_owned(),
        CHILD_CA_PATH_ENV.to_owned(),
    ];
    let output = run_probe(
        &scalar,
        &[(CHILD_SECRET_ENV, &password), (CHILD_CA_PATH_ENV, &ca)],
    );
    let stdout = assert_clean_output(&output, &[&password, &ca]);
    assert!(
        stdout.contains("\"value\":42"),
        "lo scalare deve rendere il valore: {stdout}"
    );

    // La scrittura: una tabella di sessione, creata ed esaurita nello stesso
    // statement, cosi non resta niente sul server.
    let write = [
        "database-execute-sql".to_owned(),
        "sqlserver".to_owned(),
        CHILD_SECRET_ENV.to_owned(),
        "CREATE TABLE #cli_probe (n int NOT NULL); INSERT INTO #cli_probe VALUES (1), (2), (3)"
            .to_owned(),
        // La forma CRUD del profilo PFM non ammette una DDL: qui serve il
        // consenso esplicito, ed e la ragione per cui il flag esiste.
        "--allow-raw".to_owned(),
        host,
        database,
        username,
        "--tls-ca-path-env".to_owned(),
        CHILD_CA_PATH_ENV.to_owned(),
    ];
    let output = run_probe(
        &write,
        &[(CHILD_SECRET_ENV, &password), (CHILD_CA_PATH_ENV, &ca)],
    );
    let stdout = assert_clean_output(&output, &[&password, &ca]);
    assert!(
        stdout.contains("\"affected_rows\":3"),
        "la scrittura deve dichiarare le righe toccate: {stdout}"
    );
    assert!(
        stdout.contains("\"status\":\"committed\""),
        "la transazione deve committare: {stdout}"
    );
}

fn run_postgres_private_ca_mtls(command: &str) {
    let dsn = required("PLENORA_TEST_POSTGRES_TLS_DSN");
    let ca = required("PLENORA_TEST_POSTGRES_TLS_CA");
    let certificate = required("PLENORA_TEST_POSTGRES_TLS_CLIENT_CERT");
    let key = required("PLENORA_TEST_POSTGRES_TLS_CLIENT_KEY");
    let mut arguments = vec![command.to_owned()];
    if command == "database-probe" {
        arguments.push("postgres".to_owned());
    }
    arguments.extend([
        CHILD_SECRET_ENV.to_owned(),
        "--tls-ca-path-env".to_owned(),
        CHILD_CA_PATH_ENV.to_owned(),
        "--tls-client-cert-path-env".to_owned(),
        CHILD_CLIENT_CERT_PATH_ENV.to_owned(),
        "--tls-client-key-path-env".to_owned(),
        CHILD_CLIENT_KEY_PATH_ENV.to_owned(),
    ]);
    let output = run_probe(
        &arguments,
        &[
            (CHILD_SECRET_ENV, &dsn),
            (CHILD_CA_PATH_ENV, &ca),
            (CHILD_CLIENT_CERT_PATH_ENV, &certificate),
            (CHILD_CLIENT_KEY_PATH_ENV, &key),
        ],
    );
    assert_successful_probe(&output, "postgres", &[&dsn, &ca, &certificate, &key]);
}

#[test]
#[ignore = "requires the pinned PostgreSQL private-CA/mTLS fixture"]
fn live_database_probe_postgres_private_ca_mtls() {
    run_postgres_private_ca_mtls("database-probe");
}

#[test]
#[ignore = "requires the pinned PostgreSQL private-CA/mTLS fixture"]
fn live_legacy_postgres_probe_private_ca_mtls() {
    run_postgres_private_ca_mtls("postgres-probe");
}
