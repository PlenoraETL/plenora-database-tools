use std::process::Command;

/// Usa la connessione del runner e ne eredita la configurazione TLS esplicita.
pub fn postgres_cli(args: &[&str]) -> Command {
    let dsn = std::env::var("PLENORA_TEST_POSTGRES_DSN")
        .or_else(|_| std::env::var("PG_DSN"))
        .expect("impostare PLENORA_TEST_POSTGRES_DSN o PG_DSN per la fixture live");
    let mut command = Command::new(env!("CARGO_BIN_EXE_plenora-database"));
    command.args(args).env("PG_DSN", dsn);
    command
}
