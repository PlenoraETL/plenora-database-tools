//! Formattatori di output per la CLI: json (default), markdown (human),
//! junit (CI on-premise). Attivi tramite flag globale `--format` letto da
//! `strip_output_format` prima del dispatch.

use crate::CliResult;
use serde_json::Value;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};

const FMT_JSON: u8 = 0;
const FMT_MARKDOWN: u8 = 1;
const FMT_JUNIT: u8 = 2;

static ACTIVE_FORMAT: AtomicU8 = AtomicU8::new(FMT_JSON);
static ACTIVE_COMMAND: OnceLock<Mutex<String>> = OnceLock::new();

fn command_store() -> &'static Mutex<String> {
    ACTIVE_COMMAND.get_or_init(|| Mutex::new("output".to_owned()))
}

pub(crate) fn set_active_command(name: &str) {
    if let Ok(mut guard) = command_store().lock() {
        name.clone_into(&mut guard);
    }
}

fn active_command() -> String {
    command_store()
        .lock()
        .map_or_else(|_| "output".to_owned(), |g| g.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputFormat {
    Json,
    Markdown,
    Junit,
}

impl OutputFormat {
    pub(crate) fn set_active(self) {
        ACTIVE_FORMAT.store(
            match self {
                Self::Json => FMT_JSON,
                Self::Markdown => FMT_MARKDOWN,
                Self::Junit => FMT_JUNIT,
            },
            Ordering::Relaxed,
        );
    }

    pub(crate) fn active() -> Self {
        match ACTIVE_FORMAT.load(Ordering::Relaxed) {
            FMT_MARKDOWN => Self::Markdown,
            FMT_JUNIT => Self::Junit,
            _ => Self::Json,
        }
    }
}

/// Rimuove `--format <value>` dalla coda di `args`, se presente, e imposta
/// il formato attivo per la sessione. Il resto degli argomenti resta invariato
/// (l'estrazione mantiene l'ordine relativo).
pub(crate) fn strip_output_format(args: Vec<String>) -> CliResult<Vec<String>> {
    let mut out = Vec::with_capacity(args.len());
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--format" {
            let value = iter
                .next()
                .ok_or("--format richiede un valore (json|markdown|junit)")?;
            let fmt = match value.as_str() {
                "json" => OutputFormat::Json,
                "markdown" | "md" => OutputFormat::Markdown,
                "junit" | "junit-xml" => OutputFormat::Junit,
                other => {
                    return Err(format!(
                        "--format sconosciuto: {other} (json|markdown|junit)"
                    )
                    .into());
                }
            };
            fmt.set_active();
        } else {
            out.push(arg);
        }
    }
    Ok(out)
}

/// Stampa `value` nel formato attivo, usando il comando registrato con
/// `set_active_command` come titolo (markdown) / classname (junit).
pub(crate) fn print_active(value: &Value) -> CliResult<()> {
    let cmd = active_command();
    print_result(&cmd, value)
}

/// Stampa `value` nel formato attivo. Sostituisce `print_json` come punto di
/// uscita canonico.
pub(crate) fn print_result(command: &str, value: &Value) -> CliResult<()> {
    match OutputFormat::active() {
        OutputFormat::Json => {
            let s = serde_json::to_string(value).map_err(|_| "output non serializzabile".to_owned())?;
            println!("{s}");
        }
        OutputFormat::Markdown => {
            println!("{}", render_markdown(command, value));
        }
        OutputFormat::Junit => {
            println!("{}", render_junit(command, value));
        }
    }
    Ok(())
}

fn render_markdown(command: &str, value: &Value) -> String {
    let mut buf = String::new();
    buf.push_str("# ");
    buf.push_str(command);
    buf.push_str("\n\n");
    render_markdown_value(&mut buf, value, 0);
    buf
}

fn render_markdown_value(buf: &mut String, value: &Value, depth: usize) {
    match value {
        Value::Object(map) => {
            // Se tutte le foglie sono scalari, render come lista `**k**: v`.
            for (k, v) in map {
                for _ in 0..depth {
                    buf.push_str("  ");
                }
                buf.push_str("- **");
                buf.push_str(k);
                if v.is_object() || v.is_array() {
                    buf.push_str("**:\n");
                    render_markdown_value(buf, v, depth + 1);
                } else {
                    buf.push_str("**: ");
                    buf.push_str(&scalar_to_string(v));
                    buf.push('\n');
                }
            }
        }
        Value::Array(items) => {
            if items.iter().all(Value::is_object) && !items.is_empty() {
                render_markdown_table(buf, items);
            } else {
                for item in items {
                    for _ in 0..depth {
                        buf.push_str("  ");
                    }
                    buf.push_str("- ");
                    if item.is_object() || item.is_array() {
                        buf.push('\n');
                        render_markdown_value(buf, item, depth + 1);
                    } else {
                        buf.push_str(&scalar_to_string(item));
                        buf.push('\n');
                    }
                }
            }
        }
        other => buf.push_str(&scalar_to_string(other)),
    }
}

fn render_markdown_table(buf: &mut String, items: &[Value]) {
    // Colonne = unione ordinata delle chiavi (ordine del primo oggetto).
    let mut cols: Vec<String> = Vec::new();
    for item in items {
        if let Value::Object(map) = item {
            for k in map.keys() {
                if !cols.iter().any(|c| c == k) {
                    cols.push(k.clone());
                }
            }
        }
    }
    if cols.is_empty() {
        return;
    }
    buf.push_str("| ");
    for c in &cols {
        buf.push_str(c);
        buf.push_str(" | ");
    }
    buf.push('\n');
    buf.push('|');
    for _ in &cols {
        buf.push_str("---|");
    }
    buf.push('\n');
    for item in items {
        buf.push_str("| ");
        for c in &cols {
            let v = item.get(c).cloned().unwrap_or(Value::Null);
            let cell = if v.is_object() || v.is_array() {
                serde_json::to_string(&v).unwrap_or_default()
            } else {
                scalar_to_string(&v)
            };
            buf.push_str(&cell.replace('|', "\\|").replace('\n', " "));
            buf.push_str(" | ");
        }
        buf.push('\n');
    }
}

fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}

fn render_junit(command: &str, value: &Value) -> String {
    let status = value.get("status").and_then(Value::as_str).unwrap_or("ok");
    let is_failure = matches!(
        status,
        "fail" | "unhealthy" | "wrong_category" | "unexpected_ok" | "leaked" | "row_count_mismatch",
    );
    let payload_text = serde_json::to_string_pretty(value).unwrap_or_default();
    let escaped = xml_escape(&payload_text);
    let (failures, body) = if is_failure {
        (
            1,
            format!(
                "    <failure message=\"{}\">{}</failure>",
                xml_escape(status),
                escaped
            ),
        )
    } else {
        (0, format!("    <system-out>{escaped}</system-out>"))
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <testsuite name=\"plenora-database-cli\" tests=\"1\" failures=\"{failures}\">\n\
         \x20 <testcase classname=\"{cls}\" name=\"{name}\">\n\
         {body}\n\
         \x20 </testcase>\n\
         </testsuite>",
        cls = xml_escape(command),
        name = xml_escape(command),
        failures = failures,
        body = body,
    )
}

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    static FORMAT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn strip_format_extracts_value_and_returns_remaining_args() {
        let _guard = FORMAT_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let out = strip_output_format(vec![
            "dsn-env".into(),
            "--format".into(),
            "markdown".into(),
            "extra".into(),
        ])
        .expect("parse");
        assert_eq!(out, vec!["dsn-env", "extra"]);
        assert_eq!(OutputFormat::active(), OutputFormat::Markdown);
        OutputFormat::Json.set_active();
    }

    #[test]
    fn strip_format_rejects_unknown_value() {
        let err = strip_output_format(vec!["--format".into(), "yaml".into()])
            .expect_err("must fail");
        assert!(format!("{err:?}").contains("format"));
    }

    #[test]
    fn markdown_object_renders_key_value_bullets() {
        let md = render_markdown("test-x", &json!({"a": 1, "b": "ciao"}));
        assert!(md.contains("# test-x"));
        assert!(md.contains("- **a**: 1"));
        assert!(md.contains("- **b**: ciao"));
    }

    #[test]
    fn markdown_array_of_objects_renders_table() {
        let md = render_markdown(
            "list",
            &json!([{"name": "a", "n": 1}, {"name": "b", "n": 2}]),
        );
        // BTreeMap alfabetico: colonne "n" prima di "name".
        assert!(md.contains("| n | name |"));
        assert!(md.contains("|---|---|"));
        assert!(md.contains("| 1 | a |"));
        assert!(md.contains("| 2 | b |"));
    }

    #[test]
    fn junit_wraps_status_ok_as_system_out() {
        let x = render_junit("test-x", &json!({"status": "ok"}));
        assert!(x.contains("<testsuite name=\"plenora-database-cli\""));
        assert!(x.contains("failures=\"0\""));
        assert!(x.contains("<system-out>"));
    }

    #[test]
    fn junit_wraps_failure_status_as_failure_element() {
        let x = render_junit("test-x", &json!({"status": "unhealthy"}));
        assert!(x.contains("failures=\"1\""));
        assert!(x.contains("<failure message=\"unhealthy\""));
    }
}
