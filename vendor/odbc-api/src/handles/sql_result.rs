use super::{Diagnostics, State};
use odbc_sys::SqlReturn;

/// Result of an ODBC function call. Variants hold the same meaning as the constants associated with
/// [`SqlReturn`]. This type may hold results, but it is still the responsibility of the user to
/// fetch and handle the diagnostics in case of an Error.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SqlResult<T> {
    /// The function has been executed successfully.
    Success(T),
    /// The function has been executed successfully. There have been warnings.
    SuccessWithInfo(T),
    /// Meaning depends on the function emitting `NoData`.
    NoData,
    /// Diagnostic captured before another driver call can overwrite it.
    Rejected {
        function: &'static str,
        diagnostic: Option<(State, i32)>,
    },
    /// Emmitted by execute in case delayed parameters have been bound and their input values are
    /// now required.
    NeedData,
    /// The function was started asynchronously and is still executing.
    StillExecuting,
    /// The function returned an error state. Check diagnostics.
    Error {
        /// Name of the ODBC Api call which caused the error. This might help interpreting
        /// associated ODBC diagnostics if the error is bubbled all the way up to the
        /// end users output, but the context is lost.
        function: &'static str,
    },
}

impl SqlResult<()> {
    /// Append a return value a successful to Result
    pub fn on_success<F, T>(self, f: F) -> SqlResult<T>
    where
        F: FnOnce() -> T,
    {
        self.map(|()| f())
    }
}

impl<T> SqlResult<T> {
    /// `True` if variant is [`SqlResult::Error`].
    pub fn is_err(&self) -> bool {
        matches!(self, SqlResult::Error { .. } | SqlResult::Rejected { .. })
    }

    /// Applies `f` to any value wrapped in `Success` or `SuccessWithInfo`.
    pub fn map<U, F>(self, f: F) -> SqlResult<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            SqlResult::Success(v) => SqlResult::Success(f(v)),
            SqlResult::SuccessWithInfo(v) => SqlResult::SuccessWithInfo(f(v)),
            SqlResult::Error { function } => SqlResult::Error { function },
            SqlResult::Rejected {
                function,
                diagnostic,
            } => SqlResult::Rejected {
                function,
                diagnostic,
            },
            SqlResult::StillExecuting => SqlResult::StillExecuting,
            SqlResult::NoData => SqlResult::NoData,
            SqlResult::NeedData => SqlResult::NeedData,
        }
    }

    /// Maps [`Self::NoData`] to [`Self::Success`] with the given value. This makes it easy to chain
    /// calls of to [`SqlResult::on_no_data`] after calls to [`SqlResult::on_success`].
    pub fn on_no_data<F>(self, f: F) -> SqlResult<T>
    where
        F: FnOnce() -> T,
    {
        match self {
            Self::NoData => SqlResult::Success(f()),
            other => other,
        }
    }

    /// Maps [`Self::NeedData`] to [`Self::Success`] with the given value. This makes it easy to
    /// chain calls of to [`SqlResult::on_need_data`] after calls to [`SqlResult::on_success`].
    pub fn on_need_data<F>(self, f: F) -> SqlResult<T>
    where
        F: FnOnce() -> T,
    {
        match self {
            Self::NeedData => SqlResult::Success(f()),
            other => other,
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            SqlResult::Success(v) => v,
            _ => panic!("Invalid unwrapping of SqlResult"),
        }
    }
}

/// The only SQLRETURN conversion. Diagnostics are consumed before any state transition.
pub trait ExtSqlReturn {
    fn into_sql_result_strict(
        self,
        handle: &(impl Diagnostics + ?Sized),
        function: &'static str,
    ) -> SqlResult<()>;
}

impl ExtSqlReturn for SqlReturn {
    fn into_sql_result_strict(
        self,
        handle: &(impl Diagnostics + ?Sized),
        function: &'static str,
    ) -> SqlResult<()> {
        match self {
            SqlReturn::SUCCESS => SqlResult::Success(()),
            SqlReturn::SUCCESS_WITH_INFO => {
                SqlResult::SuccessWithInfo(()).check_strict(handle, function)
            }
            SqlReturn::ERROR => SqlResult::Error { function }.check_strict(handle, function),
            SqlReturn::NO_DATA => SqlResult::NoData,
            SqlReturn::NEED_DATA => SqlResult::NeedData,
            SqlReturn::STILL_EXECUTING => SqlResult::StillExecuting,
            _ => SqlResult::Rejected {
                function,
                diagnostic: None,
            },
        }
    }
}

impl<T> SqlResult<T> {
    /// Normalize warnings immediately; admitted warnings continue through the success path.
    pub fn check_strict(
        self,
        handle: &(impl Diagnostics + ?Sized),
        function: &'static str,
    ) -> Self {
        match self {
            Self::SuccessWithInfo(value) => {
                let mut found = false;
                for number in 1..=i16::MAX {
                    let Some(record) = handle.diagnostic_record(number, &mut []) else {
                        return if found {
                            Self::Success(value)
                        } else {
                            Self::Rejected {
                                function,
                                diagnostic: None,
                            }
                        };
                    };
                    found = true;
                    if record.state.prevents_exact_result() {
                        return Self::Rejected {
                            function,
                            diagnostic: Some((record.state, record.native_error)),
                        };
                    }
                    log::warn!(
                        "{}: SQLSTATE {}, native code {}",
                        function,
                        record.state.as_str(),
                        record.native_error
                    );
                }
                // The complete diagnostic chain could not be proved safe.
                Self::Rejected {
                    function,
                    diagnostic: None,
                }
            }
            Self::Error { function } => Self::Rejected {
                function,
                diagnostic: handle
                    .diagnostic_record(1, &mut [])
                    .map(|r| (r.state, r.native_error)),
            },
            other => other,
        }
    }
}

/// Calls without a diagnostic handle must fail closed on warnings.
pub(crate) struct NoDiagnostics;
impl Diagnostics for NoDiagnostics {
    fn diagnostic_record(
        &self,
        _: i16,
        _: &mut [super::SqlChar],
    ) -> Option<super::DiagnosticResult> {
        None
    }
}
