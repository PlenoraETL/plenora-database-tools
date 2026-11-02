use odbc_api::handles::{DiagnosticResult, Diagnostics, SqlChar, State};
use plenora_database_core::{ErrorCategory, ErrorPhase};

struct Diagnostic(Option<State>);

#[test]
fn raw_returns_capture_prepare_and_fetch_before_diagnostics_are_cleared() {
    use odbc_api::handles::{ExtSqlReturn, SqlResult};
    use odbc_api::sys::SqlReturn;
    for function in [
        "SQLPrepare",
        "SQLFetch",
        "SQLSetStmtAttr",
        "SQLDescribeCol",
        "SQLColAttribute",
    ] {
        for state in [None, Some(State(*b"01S02")), Some(State(*b"01ZZZ"))] {
            let captured =
                SqlReturn::SUCCESS_WITH_INFO.into_sql_result_strict(&Diagnostic(state), function);
            assert!(captured.is_err());
            // Simulate SQLNumParams clearing the driver's diagnostic area.
            let error = captured
                .into_result_strict(&Diagnostic(None), "SQLNumParams")
                .unwrap_err();
            match error {
                odbc_api::Error::Diagnostics {
                    function: actual,
                    record,
                } => {
                    assert_eq!(actual, function);
                    assert_eq!(Some(record.state), state);
                    assert!(record.message.is_empty());
                }
                odbc_api::Error::NoDiagnostics { function: actual } => assert_eq!(actual, function),
                other => panic!("unexpected {other:?}"),
            }
        }
        let accepted = SqlReturn::SUCCESS_WITH_INFO
            .into_sql_result_strict(&Diagnostic(Some(State(*b"01003"))), function);
        assert_eq!(accepted, SqlResult::Success(()));
        let mut completed = Vec::new();
        accepted
            .on_success(|| completed.extend(["counter", "bind", "resize"]))
            .into_result_strict(&Diagnostic(None), function)
            .unwrap();
        assert_eq!(completed, ["counter", "bind", "resize"]);
    }
}

impl Diagnostics for Diagnostic {
    fn diagnostic_record(&self, number: i16, text: &mut [SqlChar]) -> Option<DiagnosticResult> {
        if number != 1 {
            return None;
        }
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

#[test]
fn strict_odbc_results_preserve_each_warning_and_its_function_without_payload() {
    use odbc_api::handles::SqlResult;
    for function in [
        "SQLExecute",
        "SQLExecDirect",
        "SQLPutData",
        "SQLParamData",
        "SQLColAttribute",
        "SQLDescribeParam",
        "SQLBindParameter",
        "SQLBindCol",
    ] {
        for state in [*b"01004", *b"01S07", *b"01517", *b"01000"] {
            let diagnostics = Diagnostic(Some(State(state)));
            let error = SqlResult::SuccessWithInfo(17)
                .into_result_strict(&diagnostics, function)
                .unwrap_err();
            match &error {
                odbc_api::Error::Diagnostics {
                    record,
                    function: actual,
                } => {
                    assert_eq!(*actual, function);
                    assert_eq!(record.state, State(state));
                    assert!(record.message.is_empty());
                }
                other => panic!("diagnostico inatteso: {other:?}"),
            }
            let public = crate::error::driver_error(&error, ErrorPhase::Write);
            assert_eq!(public.category, ErrorCategory::DataMapping);
            assert_eq!(
                public.remote_effect,
                plenora_database_core::RemoteEffect::Unknown
            );
            // A later successful call cannot turn the saved failure into success.
            assert_eq!(
                SqlResult::Success(23)
                    .into_result_strict(&Diagnostic(None), function)
                    .unwrap(),
                23
            );
            assert!(matches!(error, odbc_api::Error::Diagnostics { .. }));
        }
        assert!(SqlResult::SuccessWithInfo(17)
            .into_result_strict(&Diagnostic(None), function)
            .is_err());
    }
}

#[test]
fn informational_warnings_remain_diagnostic_but_do_not_refuse_exact_results() {
    use odbc_api::handles::SqlResult;
    for state in [
        *b"01003", *b"01006", *b"01007", *b"0100C", *b"0100D", *b"01504", *b"01508", *b"01509",
        *b"01516", *b"01521", *b"01522", *b"01523", *b"01526", *b"01543", *b"01545", *b"01550",
        *b"01560", *b"01589",
    ] {
        let info = State(state);
        let records = Records(vec![info]);
        crate::exact_read::check(&records, ErrorPhase::Write).unwrap();
        assert_eq!(
            SqlResult::SuccessWithInfo(17)
                .into_result_strict(&records, "SQLExecute")
                .unwrap(),
            17
        );
        assert_eq!(records.diagnostic_record(1, &mut []).unwrap().state, info);
        for states in [vec![info, State(*b"02000")], vec![State(*b"02000"), info]] {
            let records = Records(states);
            assert!(
                crate::exact_read::checked_batch::<()>(None, &records, ErrorPhase::Read)
                    .unwrap()
                    .is_none()
            );
            assert!(
                crate::exact_read::checked_batch(Some(&17), &records, ErrorPhase::Read).is_err()
            );
        }
        for unsafe_state in [
            *b"01005", *b"0100E", *b"01503", *b"01S01", *b"01ZZZ", *b"01004", *b"01S07", *b"01506",
            *b"01507", *b"01515", *b"01517", *b"01519", *b"01520", *b"01524", *b"01539", *b"01544",
            *b"01547", *b"01564", *b"01565", *b"01658", *b"01665", *b"01H51", *b"22001", *b"01H99",
            *b"01000", *b"01S02",
        ] {
            for states in [
                vec![info, State(unsafe_state)],
                vec![State(unsafe_state), info],
            ] {
                let records = Records(states);
                assert!(crate::exact_read::check(&records, ErrorPhase::Read).is_err());
                assert!(SqlResult::SuccessWithInfo(17)
                    .into_result_strict(&records, "SQLExecute")
                    .is_err());
            }
        }
    }
}
