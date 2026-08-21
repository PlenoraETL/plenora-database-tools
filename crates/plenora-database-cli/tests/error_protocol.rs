use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use std::fs;
#[cfg(all(feature = "mysql", feature = "sqlserver"))]
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const SECRET_SENTINEL: &str = "HERMES_SENTINEL_DATABASE_SECRET_7F3C91";

/// Variabili TLS che la CLI legge: azzerate prima di ogni invocazione.
///
/// Il figlio eredita l'ambiente del runner, quindi un test che ne dichiara
/// solo alcune misura anche quelle che chi esegue ha esportato. Per i test di
/// coerenza e fatale: "certificato client senza CA" troverebbe una CA
/// ereditata e passerebbe per il motivo sbagliato.
const TLS_ENVIRONMENT: [&str; 4] = [
    "PLENORA_PG_CA_PATH",
    "PLENORA_PG_CLIENT_CERT_PATH",
    "PLENORA_PG_CLIENT_KEY_PATH",
    "PLENORA_TLS_INSECURE_LOCAL",
];

/// Comando CLI con l'ambiente TLS azzerato: il test dichiara cio che vuole.
fn cli(arguments: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_plenora-database"));
    command.args(arguments);
    for name in TLS_ENVIRONMENT {
        command.env_remove(name);
    }
    command
}

fn run(arguments: &[&str]) -> Output {
    cli(arguments).output().expect("run plenora-database")
}

fn run_with_secret(arguments: &[&str], secret: &str) -> Output {
    cli(arguments)
        .env("PLENORA_TEST_DATABASE_SECRET", secret)
        .output()
        .expect("run plenora-database with isolated secret")
}

// I due helper e `fs` servono solo ai test delle rotte multi-provider, che
// sono gia sotto questo `cfg`: senza, con le sole feature di default
// risultano codice morto e clippy lo rifiuta.
#[cfg(all(feature = "mysql", feature = "sqlserver"))]
fn run_without_secret(arguments: &[&str]) -> Output {
    cli(arguments)
        .env_remove("PLENORA_TEST_MISSING_SECRET")
        .output()
        .expect("run plenora-database without secret")
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
fn run_without_secret_with_tls_path(
    arguments: &[&str],
    tls_environment: &str,
    tls_path: &Path,
) -> Output {
    cli(arguments)
        .env_remove("PLENORA_TEST_MISSING_SECRET")
        .env(tls_environment, tls_path)
        .output()
        .expect("run plenora-database without secret and with isolated TLS path")
}

fn unique_tls_path(suffix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "plenora-cli-tls-{}-{timestamp}-{suffix}",
        std::process::id()
    ))
}

fn pem_certificate(der: &[u8]) -> Vec<u8> {
    let mut pem = b"-----BEGIN CERTIFICATE-----\n".to_vec();
    for line in BASE64_STANDARD.encode(der).as_bytes().chunks(64) {
        pem.extend_from_slice(line);
        pem.push(b'\n');
    }
    pem.extend_from_slice(b"-----END CERTIFICATE-----\n");
    pem
}

fn error_envelope(output: &Output) -> serde_json::Value {
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout.clone()).expect("UTF-8 stdout");
    serde_json::from_str(stdout.trim()).expect("JSON error envelope")
}

fn assert_secret_absent(output: &Output, secret: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains(secret), "secret leaked to stdout");
    assert!(!stderr.contains(secret), "secret leaked to stderr");
}

#[test]
fn cli_errors_are_canonical_json_on_stdout_with_nonzero_exit() {
    let output = run(&["unknown-command"]);
    let envelope = error_envelope(&output);
    assert_eq!(envelope["status"], "error");
    assert_eq!(envelope["protocol_version"], 1);
    assert_eq!(envelope["error"]["category"], "invalid_plan");
    assert_eq!(envelope["error"]["phase"], "validate");
    assert_eq!(envelope["error"]["remote_effect"], "none");
    assert_eq!(envelope["error"]["retry"]["kind"], "never");
    assert!(envelope["error"]["message"]
        .as_str()
        .is_some_and(|message| message.starts_with("uso:")));
}

#[test]
fn postgres_routes_share_private_ca_environment_contract() {
    for arguments in [
        vec![
            "database-probe",
            "postgres",
            "PLENORA_TEST_DATABASE_SECRET",
            "--tls-ca-path-env",
            "PLENORA_TEST_DELIBERATELY_MISSING_TLS_CA_PATH_7219",
        ],
        vec![
            "postgres-probe",
            "PLENORA_TEST_DATABASE_SECRET",
            "--tls-ca-path-env",
            "PLENORA_TEST_DELIBERATELY_MISSING_TLS_CA_PATH_7219",
        ],
    ] {
        let output = run_with_secret(&arguments, SECRET_SENTINEL);
        assert_eq!(output.status.code(), Some(1));
        assert_secret_absent(&output, SECRET_SENTINEL);
        let envelope = error_envelope(&output);
        assert_eq!(envelope["error"]["category"], "invalid_plan");
        assert_eq!(envelope["error"]["message"], "variabile path TLS assente");
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn tls_path_resolution_precedes_secret_resolution_for_every_probe_route() {
    let routes: &[&[&str]] = &[
        &[
            "database-probe",
            "postgres",
            "PLENORA_TEST_MISSING_SECRET",
            "--tls-ca-path-env",
            "PLENORA_TEST_MISSING_TLS_PATH",
        ],
        &[
            "postgres-probe",
            "PLENORA_TEST_MISSING_SECRET",
            "--tls-ca-path-env",
            "PLENORA_TEST_MISSING_TLS_PATH",
        ],
        &[
            "database-probe",
            "mysql",
            "PLENORA_TEST_MISSING_SECRET",
            "host",
            "database",
            "username",
            "--tls-ca-path-env",
            "PLENORA_TEST_MISSING_TLS_PATH",
        ],
        &[
            "database-probe",
            "sqlserver",
            "PLENORA_TEST_MISSING_SECRET",
            "host",
            "database",
            "username",
            "--tls-ca-path-env",
            "PLENORA_TEST_MISSING_TLS_PATH",
        ],
    ];
    for arguments in routes {
        let output = Command::new(env!("CARGO_BIN_EXE_plenora-database"))
            .args(*arguments)
            .env_remove("PLENORA_TEST_MISSING_SECRET")
            .env_remove("PLENORA_TEST_MISSING_TLS_PATH")
            .output()
            .expect("run probe without secret or TLS path");
        let envelope = error_envelope(&output);
        assert_eq!(envelope["error"]["message"], "variabile path TLS assente");
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn bounded_ca_validation_precedes_secret_resolution_for_every_probe_route() {
    let fixture = std::env::temp_dir().join(format!(
        "plenora-cli-oversized-ca-{}.pem",
        std::process::id()
    ));
    std::fs::write(&fixture, vec![b'x'; 1_048_577]).expect("write oversized CA fixture");
    assert_ca_error_before_secret(&fixture, "materiale TLS vuoto o oltre 1 MiB");
    std::fs::write(&fixture, b"not-a-certificate").expect("write invalid CA fixture");
    assert_ca_error_before_secret(&fixture, "materiale CA TLS non valido");
    std::fs::remove_file(&fixture).expect("remove CA fixture");
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
fn assert_ca_error_before_secret(path: &Path, expected_message: &str) {
    for arguments in [
        vec![
            "database-probe",
            "postgres",
            "PLENORA_TEST_MISSING_SECRET",
            "--tls-ca-path-env",
            "PLENORA_TEST_CA_PATH",
        ],
        vec![
            "postgres-probe",
            "PLENORA_TEST_MISSING_SECRET",
            "--tls-ca-path-env",
            "PLENORA_TEST_CA_PATH",
        ],
        vec![
            "database-probe",
            "mysql",
            "PLENORA_TEST_MISSING_SECRET",
            "host",
            "database",
            "username",
            "--tls-ca-path-env",
            "PLENORA_TEST_CA_PATH",
        ],
        vec![
            "database-probe",
            "sqlserver",
            "PLENORA_TEST_MISSING_SECRET",
            "host",
            "database",
            "username",
            "--tls-ca-path-env",
            "PLENORA_TEST_CA_PATH",
        ],
    ] {
        let output = run_without_secret_with_tls_path(&arguments, "PLENORA_TEST_CA_PATH", path);
        let envelope = error_envelope(&output);
        assert_eq!(envelope["error"]["message"], expected_message);
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn invalid_non_secret_provider_fields_precede_secret_resolution() {
    let cases: &[(&[&str], &str)] = &[
        (
            &[
                "database-probe",
                "mysql",
                "PLENORA_TEST_MISSING_SECRET",
                "",
                "warehouse",
                "loader",
            ],
            "configurazione MySQL: host vuoto o contenente NUL",
        ),
        (
            &[
                "database-probe",
                "mysql",
                "PLENORA_TEST_MISSING_SECRET",
                "mysql.example.test",
                "",
                "loader",
            ],
            "configurazione MySQL: database vuoto o contenente NUL",
        ),
        (
            &[
                "database-probe",
                "mysql",
                "PLENORA_TEST_MISSING_SECRET",
                "mysql.example.test",
                "warehouse",
                "",
            ],
            "configurazione MySQL: username vuoto o contenente NUL",
        ),
        (
            &[
                "database-probe",
                "sqlserver",
                "PLENORA_TEST_MISSING_SECRET",
                "",
                "warehouse",
                "loader",
            ],
            "configurazione SQL Server: host vuoto o contenente NUL",
        ),
        (
            &[
                "database-probe",
                "sqlserver",
                "PLENORA_TEST_MISSING_SECRET",
                "sql.example.test",
                "",
                "loader",
            ],
            "configurazione SQL Server: database vuoto o contenente NUL",
        ),
        (
            &[
                "database-probe",
                "sqlserver",
                "PLENORA_TEST_MISSING_SECRET",
                "sql.example.test",
                "warehouse",
                "",
            ],
            "configurazione SQL Server: username vuoto o contenente NUL",
        ),
    ];
    for (arguments, expected) in cases {
        let output = run_without_secret(arguments);
        let envelope = error_envelope(&output);
        assert_eq!(envelope["error"]["message"], *expected);
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn valid_der_and_crt_der_ca_preparation_precedes_secret_resolution() {
    const TEST_CA_DER: &[u8] = include_bytes!("fixtures/cli-test-ca.der");
    let routes: &[&[&str]] = &[
        &["database-probe", "postgres", "PLENORA_TEST_MISSING_SECRET"],
        &["postgres-probe", "PLENORA_TEST_MISSING_SECRET"],
        &[
            "database-probe",
            "mysql",
            "PLENORA_TEST_MISSING_SECRET",
            "mysql.example.test",
            "warehouse",
            "loader",
        ],
        &[
            "database-probe",
            "sqlserver",
            "PLENORA_TEST_MISSING_SECRET",
            "sql.example.test",
            "warehouse",
            "loader",
        ],
    ];
    for extension in ["der", "crt"] {
        let path = unique_tls_path(&format!("valid-ca.{extension}"));
        fs::write(&path, TEST_CA_DER).expect("write public DER CA fixture");
        for route in routes {
            let mut arguments = route.to_vec();
            arguments.extend(["--tls-ca-path-env", "PLENORA_TEST_CA_PATH"]);
            let output =
                run_without_secret_with_tls_path(&arguments, "PLENORA_TEST_CA_PATH", &path);
            let envelope = error_envelope(&output);
            assert_eq!(envelope["error"]["message"], "variabile DSN assente");
        }
        fs::remove_file(path).expect("remove public DER CA fixture copy");
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn sqlserver_rejects_multi_certificate_ca_before_secret_resolution() {
    const TEST_CA_DER: &[u8] = include_bytes!("fixtures/cli-test-ca.der");
    let path = unique_tls_path("multi-ca.pem");
    let certificate = pem_certificate(TEST_CA_DER);
    let mut bundle = certificate.clone();
    bundle.extend_from_slice(&certificate);
    fs::write(&path, bundle).expect("write two-certificate public CA bundle");
    let output = run_without_secret_with_tls_path(
        &[
            "database-probe",
            "sqlserver",
            "PLENORA_TEST_MISSING_SECRET",
            "sql.example.test",
            "warehouse",
            "loader",
            "--tls-ca-path-env",
            "PLENORA_TEST_CA_PATH",
        ],
        "PLENORA_TEST_CA_PATH",
        &path,
    );
    fs::remove_file(path).expect("remove public CA bundle");
    let envelope = error_envelope(&output);
    assert_eq!(
        envelope["error"]["message"],
        "configurazione SQL Server: CA privata deve contenere esattamente un certificato"
    );
}

#[test]
fn public_usage_documents_the_provider_neutral_probe_boundary() {
    let output = run(&["unknown-command"]);
    let envelope = error_envelope(&output);
    let message = envelope["error"]["message"]
        .as_str()
        .expect("usage message");
    // Post-F5.14: la usage() è ristrutturata per gruppi. Verifica che i
    // token essenziali per il contratto database-probe siano documentati.
    assert!(message.contains("database-probe"));

    // I provider elencati sono quelli **compilati**. Questo test pretendeva
    // `postgres | mysql | sqlserver` sempre: era vero finché l'aiuto elencava
    // tutto a prescindere dalle feature, cioè finché mentiva in tre
    // configurazioni su quattro. La riga si legge intera, non per
    // sottostringa: `portable-compile <postgres|mysql|sqlserver>` nomina i tre
    // provider anche in un binario che ne ha compilato uno solo.
    let providers = message
        .lines()
        .find(|line| line.contains("provider compilati in questo binario"))
        .expect("riga dei provider compilati");
    for (compiled, name) in [
        (cfg!(feature = "postgres"), "postgres"),
        (cfg!(feature = "mysql"), "mysql"),
        (cfg!(feature = "sqlserver"), "sqlserver"),
    ] {
        assert_eq!(
            providers.contains(name),
            compiled,
            "la riga dei provider non riflette le feature compilate: {providers}"
        );
    }

    // Le variabili TLS del probe vivono nella sezione PostgreSQL.
    #[cfg(feature = "postgres")]
    {
        assert!(message.contains("--tls-ca-path-env"));
        assert!(message.contains("--tls-client-cert-path-env"));
        assert!(message.contains("--tls-client-key-path-env"));
    }

    // Sezione flag globali con --format, --allow-write-tests, --session-context.
    assert!(message.contains("--format"));
    assert!(message.contains("--allow-write-tests"));
    assert!(message.contains("--session-context"));
}

#[test]
fn declared_providers_without_adapters_fail_closed_at_the_public_cli() {
    for provider in ["mariadb", "oracle", "db2", "sqlite", "duckdb"] {
        let output = run(&["database-probe", provider]);
        let envelope = error_envelope(&output);
        assert_eq!(envelope["error"]["category"], "unsupported");
        assert_eq!(envelope["error"]["phase"], "prepare");
        assert_eq!(envelope["error"]["remote_effect"], "none");
        assert_eq!(envelope["error"]["retry"]["kind"], "never");
        assert_eq!(envelope["error"]["provider"], provider);
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn malformed_provider_arguments_fail_before_secret_resolution() {
    let malformed_configurations: &[(&[&str], &str)] = &[
        (&[], "manca host provider"),
        (&["db.example.test"], "manca database provider"),
        (&["db.example.test", "warehouse"], "manca username provider"),
        (
            &["db.example.test", "warehouse", "loader", "0"],
            "porta provider non valida",
        ),
        (
            &["db.example.test", "warehouse", "loader", "-1"],
            "porta provider non valida",
        ),
        (
            &["db.example.test", "warehouse", "loader", "65536"],
            "porta provider non valida",
        ),
        (
            &["db.example.test", "warehouse", "loader", "not-a-port"],
            "porta provider non valida",
        ),
        (
            &["db.example.test", "warehouse", "loader", "3306", "trailing"],
            "troppi argomenti",
        ),
    ];
    for provider in ["mysql", "sqlserver"] {
        for (configuration, expected_message) in malformed_configurations {
            let mut arguments = vec!["database-probe", provider, "PLENORA_TEST_MISSING_SECRET"];
            arguments.extend_from_slice(configuration);
            let output = run_without_secret(&arguments);
            let envelope = error_envelope(&output);
            assert_eq!(envelope["error"]["category"], "invalid_plan");
            assert_eq!(envelope["error"]["message"], *expected_message);
        }
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn malformed_tls_arguments_fail_before_secret_resolution() {
    let cases: &[(&[&str], &str)] = &[
        (
            &[
                "database-probe",
                "postgres",
                "PLENORA_TEST_MISSING_SECRET",
                "--tls-unknown",
                "ENV",
            ],
            "opzione TLS provider sconosciuta",
        ),
        (
            &[
                "database-probe",
                "postgres",
                "PLENORA_TEST_MISSING_SECRET",
                "--tls-ca-path-env",
                "CA_ENV",
                "--tls-ca-path-env",
                "OTHER_CA_ENV",
            ],
            "opzione TLS provider duplicata",
        ),
        (
            &[
                "database-probe",
                "postgres",
                "PLENORA_TEST_MISSING_SECRET",
                "--tls-ca-path-env",
                "INVALID=ENV",
            ],
            "nome variabile ambiente TLS non valido",
        ),
        (
            &[
                "database-probe",
                "postgres",
                "PLENORA_TEST_MISSING_SECRET",
                "--tls-ca-path-env",
                "CA_ENV",
                "--tls-client-cert-path-env",
                "CERT_ENV",
            ],
            "certificato e chiave client TLS devono essere forniti insieme",
        ),
        (
            &[
                "database-probe",
                "postgres",
                "PLENORA_TEST_MISSING_SECRET",
                "--tls-client-cert-path-env",
                "CERT_ENV",
                "--tls-client-key-path-env",
                "KEY_ENV",
            ],
            "identità client TLS richiede una CA privata",
        ),
        (
            &[
                "database-probe",
                "mysql",
                "PLENORA_TEST_MISSING_SECRET",
                "host",
                "database",
                "username",
                "--tls-ca-path-env",
                "CA_ENV",
                "--tls-client-cert-path-env",
                "CERT_ENV",
                "--tls-client-key-path-env",
                "KEY_ENV",
            ],
            "identità client TLS supportata solo per PostgreSQL",
        ),
    ];
    for (arguments, expected) in cases {
        let output = run_without_secret(arguments);
        let envelope = error_envelope(&output);
        assert_eq!(envelope["error"]["category"], "invalid_plan");
        assert_eq!(envelope["error"]["message"], *expected);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("PLENORA_TEST_MISSING_SECRET"));
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn implemented_providers_reach_their_public_probe_configuration_without_network_io() {
    let cases: &[(&str, &[&str], &str)] = &[
        (
            "postgres",
            &[],
            "not-a-postgres-dsn-HERMES_SENTINEL_DATABASE_SECRET_7F3C91",
        ),
        ("mysql", &["", "warehouse", "loader"], SECRET_SENTINEL),
        ("sqlserver", &["", "warehouse", "loader"], SECRET_SENTINEL),
    ];
    for (provider, configuration, secret) in cases {
        let mut arguments = vec!["database-probe", *provider, "PLENORA_TEST_DATABASE_SECRET"];
        arguments.extend_from_slice(configuration);
        let output = run_with_secret(&arguments, secret);
        assert_secret_absent(&output, SECRET_SENTINEL);
        let envelope = error_envelope(&output);
        assert_eq!(envelope["error"]["category"], "invalid_configuration");
        assert_eq!(envelope["error"]["remote_effect"], "none");
        assert_eq!(envelope["error"]["retry"]["kind"], "never");
        assert_eq!(envelope["error"]["provider"], *provider);
    }
}

#[test]
fn secrets_are_redacted_from_transport_errors_for_every_public_probe_route() {
    let postgres_dsn = format!(
        "host=127.0.0.1 port=1 user=probe password={SECRET_SENTINEL} dbname=probe \
         connect_timeout=1 sslmode=disable"
    );
    for arguments in [
        vec!["database-probe", "postgres", "PLENORA_TEST_DATABASE_SECRET"],
        vec!["postgres-probe", "PLENORA_TEST_DATABASE_SECRET"],
    ] {
        let output = run_with_secret(&arguments, &postgres_dsn);
        assert_secret_absent(&output, SECRET_SENTINEL);
        let _envelope = error_envelope(&output);
    }

    for provider in ["mysql", "sqlserver"] {
        let output = run_with_secret(
            &[
                "database-probe",
                provider,
                "PLENORA_TEST_DATABASE_SECRET",
                "127.0.0.1",
                "probe",
                "probe",
                "1",
            ],
            SECRET_SENTINEL,
        );
        assert_secret_absent(&output, SECRET_SENTINEL);
        let _envelope = error_envelope(&output);
    }
}

// ============================================================================
//  Configurazione TLS dei sottocomandi: nessuna variabile ignorata
// ============================================================================

/// Senza alcuna variabile TLS il provider e quello sicuro di default e arriva
/// a tentare la connessione.
///
/// La regressione che questo test avrebbe colto: leggere le variabili con
/// l'helper di `database-probe`, che tratta "nome indicato ma variabile
/// assente" come errore del chiamante, faceva fallire ogni sottocomando in
/// fase `validate` con "variabile path TLS assente" — cioe la configurazione
/// di produzione, quella senza CA privata, era l'unica che non funzionava.
#[test]
fn the_plain_secure_default_reaches_the_connection_attempt() {
    let output = cli(&["execute-scalar", "PG_DSN", "SELECT 1", "--type=i64"])
        .env("PG_DSN", "host=127.0.0.1 port=1 user=nobody dbname=nothing")
        .output()
        .expect("run plenora-database");
    let envelope = error_envelope(&output);
    assert_eq!(
        envelope["error"]["phase"], "connect",
        "il default sicuro non deve fallire prima di provare a connettersi: {envelope}"
    );
}

/// Una configurazione TLS che il provider non puo onorare deve fallire, non
/// essere ignorata.
///
/// Il certificato client senza CA e il caso concreto: il provider userebbe i
/// root pubblici e il certificato resterebbe inutilizzato, quindi il consumer
/// crederebbe di avere mTLS senza averlo. Stessa logica per l'interruttore
/// insicuro insieme a del materiale TLS: le due richieste si contraddicono.
#[test]
fn incoherent_tls_configuration_fails_instead_of_being_ignored() {
    /// Un caso: cosa si imposta, e cosa il messaggio deve contenere.
    struct Case {
        label: &'static str,
        paths: Vec<(&'static str, PathBuf)>,
        flags: Vec<&'static str>,
        expected: &'static str,
    }

    // CA reale: il caso "certificato senza chiave" si raggiunge solo dopo che
    // la CA e stata validata, quindi un PEM fittizio verrebbe respinto prima.
    const TEST_CA_DER: &[u8] = include_bytes!("fixtures/cli-test-ca.der");
    let ca = unique_tls_path("coherence-ca.pem");
    fs::write(&ca, pem_certificate(TEST_CA_DER)).expect("write ca");
    let client = unique_tls_path("coherence-client.pem");
    fs::write(&client, pem_certificate(TEST_CA_DER)).expect("write client");

    let cases = [
        Case {
            label: "certificato client senza CA",
            paths: vec![("PLENORA_PG_CLIENT_CERT_PATH", client.clone())],
            flags: vec![],
            expected: "richiede anche PLENORA_PG_CA_PATH",
        },
        Case {
            label: "certificato senza chiave",
            paths: vec![
                ("PLENORA_PG_CA_PATH", ca.clone()),
                ("PLENORA_PG_CLIENT_CERT_PATH", client.clone()),
            ],
            flags: vec![],
            expected: "insieme",
        },
        Case {
            label: "insicuro con materiale TLS",
            paths: vec![("PLENORA_PG_CA_PATH", ca.clone())],
            flags: vec!["PLENORA_TLS_INSECURE_LOCAL"],
            expected: "non puo convivere con materiale TLS",
        },
    ];

    for Case {
        label,
        paths,
        flags,
        expected,
    } in cases
    {
        let mut command = cli(&["execute-scalar", "PG_DSN", "SELECT 1", "--type=i64"]);
        command.env("PG_DSN", "host=127.0.0.1 user=nobody dbname=nothing");
        for (name, value) in &paths {
            command.env(name, value);
        }
        for name in &flags {
            command.env(name, "1");
        }
        let output = command.output().expect("run plenora-database");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            !output.status.success(),
            "{label}: uscita zero
{stdout}"
        );
        assert!(
            stdout.contains(expected),
            "{label}: messaggio inatteso
{stdout}"
        );
    }

    let _ = fs::remove_file(&ca);
    let _ = fs::remove_file(&client);
}
