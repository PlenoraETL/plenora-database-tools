use odbc_api::handles::{DiagnosticResult, Diagnostics, SqlChar, State};
use plenora_database_core::{ErrorCategory, ErrorPhase};

struct Diagnostic(Option<State>);

impl Diagnostics for Diagnostic {
    fn diagnostic_record(&self, number: i16, text: &mut [SqlChar]) -> Option<DiagnosticResult> {
        assert_eq!(number, 1);
        assert!(
            text.is_empty(),
            "il payload diagnostico non viene richiesto"
        );
        self.0.map(|state| DiagnosticResult {
            state,
            native_error: 0,
            text_length: 100,
        })
    }
}

struct Records(Vec<State>);

impl Diagnostics for Records {
    fn diagnostic_record(&self, number: i16, text: &mut [SqlChar]) -> Option<DiagnosticResult> {
        assert!(text.is_empty());
        self.0
            .get(usize::try_from(number - 1).unwrap())
            .map(|state| DiagnosticResult {
                state: *state,
                native_error: 100,
                text_length: 100,
            })
    }
}

#[test]
fn utf8_probe_survives_end_of_cursor_but_not_lossy_reads() {
    use crate::connection::{codepage_probe_result, CODEPAGE_PROBE};
    use crate::exact_read::checked_batch;
    let row = vec![
        Some("C3A8F09F9880".into()),
        Some("2".into()),
        Some(CODEPAGE_PROBE.into()),
    ];
    let batch = vec![row];
    for phase in [ErrorPhase::Probe, ErrorPhase::Read] {
        let mut rows = checked_batch(Some(&batch), &Records(vec![]), phase)
            .unwrap()
            .unwrap()
            .clone();
        let end = checked_batch::<Vec<Vec<Option<String>>>>(
            None,
            &Records(vec![State(*b"02000")]),
            phase,
        )
        .unwrap();
        assert!(end.is_none());
        codepage_probe_result(Ok(std::mem::take(&mut rows))).unwrap();
        assert!(checked_batch(Some(&batch), &Records(vec![State(*b"02000")]), phase).is_err());
        for state in [*b"01517", *b"01004", *b"01S07", *b"01000"] {
            for records in [vec![State(state)], vec![State(*b"02000"), State(state)]] {
                assert!(checked_batch(Some(&batch), &Records(records.clone()), phase).is_err());
                assert!(checked_batch::<()>(None, &Records(records), phase).is_err());
            }
        }
        assert!(checked_batch::<()>(None, &Records(vec![]), phase)
            .unwrap()
            .is_none());
    }
}

#[test]
fn codepage_probe_remains_fail_closed_and_preserves_read_errors() {
    use crate::connection::{codepage_probe_result, CODEPAGE_PROBE};
    let valid = vec![
        Some("C3A8F09F9880".into()),
        Some(" 2 ".into()),
        Some(CODEPAGE_PROBE.into()),
    ];
    codepage_probe_result(Ok(vec![valid.clone()])).unwrap();
    for (column, value) in [
        (0, "C383C2A8F09F9880"),
        (0, "C3A8F09F9880 "),
        (1, "6"),
        (2, "??"),
        (2, "è😀 "),
    ] {
        let mut row = valid.clone();
        row[column] = Some(value.into());
        let error = codepage_probe_result(Ok(vec![row])).unwrap_err();
        assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
        assert_eq!(error.phase, ErrorPhase::Connect);
    }
    for column in 0..3 {
        let mut row = valid.clone();
        row[column] = None;
        assert!(codepage_probe_result(Ok(vec![row])).is_err());
    }
    for rows in [vec![], vec![vec![]], vec![valid.clone(), valid]] {
        assert!(codepage_probe_result(Ok(rows)).is_err());
    }
    let read_error =
        crate::exact_read::check(&Diagnostic(Some(State(*b"01517"))), ErrorPhase::Probe)
            .unwrap_err();
    let error = codepage_probe_result(Err(read_error.clone())).unwrap_err();
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, ErrorPhase::Connect);
    assert_eq!(error.message, read_error.message);
}

#[test]
fn codepage_probe_refuses_latin1_even_when_the_round_trip_is_identical() {
    use crate::connection::{catalog_text, codepage_probe_result, CODEPAGE_PROBE};
    use odbc_api::DataType;
    // SQL_C_CHAR UTF-8 interpretato come Latin-1: il ritorno puo essere
    // identico, ma i byte sul server e il numero di caratteri sono diversi.
    for (hex, count, accepted) in [
        ("C3A8F09F9880", "2", true),
        ("C383C2A8C3B0C29FC298C280", "6", false),
        ("C383C2A8C3B0C29FC298C280", "2", false),
        ("C3A8F09F9880", "6", false),
    ] {
        for kind in [
            DataType::Varchar { length: None },
            DataType::Char { length: None },
        ] {
            let rows = vec![vec![
                Some(catalog_text(hex, kind).into()),
                Some(catalog_text(count, DataType::Integer).into()),
                Some(catalog_text(CODEPAGE_PROBE, kind).into()),
            ]];
            assert_eq!(codepage_probe_result(Ok(rows)).is_ok(), accepted);
        }
    }
}

#[test]
fn reads_refuse_conversion_truncation_and_unknown_diagnostics_without_payloads() {
    for phase in [ErrorPhase::Read, ErrorPhase::Probe] {
        crate::exact_read::check(&Diagnostic(None), phase).unwrap();
        for state in [*b"01517", *b"01004", *b"01S07", *b"01S01", *b"01000"] {
            let error =
                crate::exact_read::check(&Diagnostic(Some(State(state))), phase).unwrap_err();
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert_eq!(error.phase, phase);
            assert_eq!(
                error.message,
                format!(
                    "lettura Db2 non esatta (SQLSTATE {}, codice 0)",
                    State(state).as_str()
                )
            );
        }
    }
}
