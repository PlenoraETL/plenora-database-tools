use std::mem::transmute;

use crate::{
    CursorImpl, CursorPolling, Error, ParameterCollectionRef, Sleep,
    handles::{AsStatementRef, SqlText, State, Statement, StatementRef},
    parameter::Blob,
    sleep::wait_for,
};

/// Shared implementation for executing a query with parameters between [`crate::Connection`],
/// [`crate::Preallocated`] and [`crate::Prepared`].
///
/// # Parameters
///
/// * `lazy_statement`: Factory for statement handle used to execute the query. We pass the pass the
///   statement lazily in order to avoid unnecessary allocating a statement handle in case the
///   parameter set is empty.
/// * `query`: SQL query to be executed. If `None` it is a assumed a prepared query is to be
///   executed.
/// * `params`: The parameters bound to the statement before query execution.
pub fn execute_with_parameters<S>(
    mut statement: S,
    query: Option<&SqlText<'_>>,
    params: impl ParameterCollectionRef,
) -> Result<Option<CursorImpl<S>>, Error>
where
    S: Statement,
{
    unsafe {
        bind_parameters(&mut statement, params)?;
        execute(statement, query)
    }
}

/// Execute while rejecting diagnostics before result-set discovery clears them.
pub fn execute_with_parameters_strict<S: Statement>(
    mut statement: S,
    query: Option<&SqlText<'_>>,
    params: impl ParameterCollectionRef,
) -> Result<Option<CursorImpl<S>>, Error> {
    unsafe {
        bind_parameters(&mut statement, params)?;
        execute_checked(statement, query, true)
    }
}

/// Asynchronous sibiling of [`execute_with_parameters`]
pub async fn execute_with_parameters_polling<S>(
    mut statement: S,
    query: Option<&SqlText<'_>>,
    params: impl ParameterCollectionRef,
    sleep: impl Sleep,
) -> Result<Option<CursorPolling<S>>, Error>
where
    S: Statement,
{
    unsafe {
        bind_parameters(&mut statement, params)?;
        execute_polling(statement, query, sleep).await
    }
}

unsafe fn bind_parameters(
    statement: &mut impl Statement,
    mut params: impl ParameterCollectionRef,
) -> Result<(), Error> {
    unsafe {
        let parameter_set_size = params.parameter_set_size();

        // Reset parameters so we do not dereference stale once by mistake if we call
        // `exec_direct`.
        statement
            .reset_parameters()
            .into_result_strict(statement, "SQLFreeStmt")?;
        let result = statement
            .set_paramset_size(parameter_set_size)
            .into_result_strict(statement, "SQLSetStmtAttr");
        // If setting parameter size is not supported, we can ignore the error in case we want to
        // set it to 1, because 1 is the default and it could not have been set to something else.
        //
        // This occurred using mdbtools. See PR: https://github.com/pacman82/odbc-api/pull/903
        //
        // mdbtools claims to support ODBC 3, but zeroes out the SQLSetStmtAttr explicitly in the
        // dispatch table handed to the unixODBC driver manager. UnixODBC does applay the fallback
        // behavior, and finds that seting the parameter size is not supported by ODBC2's
        // SQLSetStmtOption.
        if !matches!(
            &result,
            Err(Error::Diagnostics { record, .. })
                if record.state == State::INVALID_ATTRIBUTE_OR_OPTION_IDENTIFIER
                    && parameter_set_size == 1
        ) {
            result?;
        }
        // Bind new parameters passed by caller.
        params.bind_parameters_to(statement)?;
        Ok(())
    }
}

/// # Safety
///
/// * Execute may dereference pointers to bound parameters, so these must guaranteed to be valid
///   then calling this function.
/// * Furthermore all bound delayed parameters must be of type `*mut &mut dyn Blob`.
pub unsafe fn execute<S>(
    statement: S,
    query: Option<&SqlText<'_>>,
) -> Result<Option<CursorImpl<S>>, Error>
where
    S: Statement,
{
    unsafe { execute_checked(statement, query, false) }
}

pub(crate) unsafe fn execute_checked<S: Statement>(
    mut statement: S,
    query: Option<&SqlText<'_>>,
    strict: bool,
) -> Result<Option<CursorImpl<S>>, Error> {
    unsafe {
        let result = if let Some(sql) = query {
            // We execute an unprepared "one shot query"
            statement.exec_direct(sql)
        } else {
            // We execute a prepared query
            statement.execute()
        };

        // If delayed parameters (e.g. input streams) are bound we might need to put data in order
        // to execute.
        let result = result
            .on_success(|| false)
            .on_no_data(|| false)
            .on_need_data(|| true);
        let need_data = if strict {
            result.into_result_strict(
                &statement,
                if query.is_some() {
                    "SQLExecDirect"
                } else {
                    "SQLExecute"
                },
            )?
        } else {
            result.into_result(&statement)?
        };

        let mut stmt = statement.as_stmt_ref();
        if need_data {
            // Check if any delayed parameters have been bound which stream data to the database at
            // statement execution time. Loops over each bound stream.
            while let Some(blob_ref) = next_blob_param(&mut stmt, strict)? {
                // Loop over all batches within each blob
                while let Some(batch) = blob_ref.next_batch().map_err(Error::FailedReadingInput)? {
                    let result = stmt.put_binary_batch(batch);
                    if strict {
                        result.into_result_strict(&stmt, "SQLPutData")?;
                    } else {
                        result.into_result(&stmt)?;
                    }
                }
            }
        }

        // Discover results only after each execution/streaming diagnostic was checked.
        let result = statement.num_result_cols();
        let columns = if strict {
            result.into_result_strict(&statement, "SQLNumResultCols")?
        } else {
            result.into_result(&statement)?
        };
        if columns == 0 {
            Ok(None)
        } else {
            // Safe: `statement` is in cursor state.
            let cursor = CursorImpl::new(statement);
            Ok(Some(cursor))
        }
    }
}

/// # Safety
///
/// * Execute may dereference pointers to bound parameters, so these must guaranteed to be valid
///   then calling this function.
/// * Furthermore all bound delayed parameters must be of type `*mut &mut dyn Blob`.
pub async unsafe fn execute_polling<S>(
    mut statement: S,
    query: Option<&SqlText<'_>>,
    mut sleep: impl Sleep,
) -> Result<Option<CursorPolling<S>>, Error>
where
    S: Statement,
{
    unsafe {
        let result = if let Some(sql) = query {
            // We execute an unprepared "one shot query"
            wait_for(|| statement.exec_direct(sql), &mut sleep).await
        } else {
            // We execute a prepared query
            wait_for(|| statement.execute(), &mut sleep).await
        };

        // If delayed parameters (e.g. input streams) are bound we might need to put data in order
        // to execute.
        let need_data = result
            .on_success(|| false)
            .on_no_data(|| false)
            .on_need_data(|| true)
            .into_result(&statement)?;

        let mut stmt = statement.as_stmt_ref();
        if need_data {
            // Check if any delayed parameters have been bound which stream data to the database at
            // statement execution time. Loops over each bound stream.
            while let Some(blob_ref) = next_blob_param(&mut stmt, false)? {
                // Loop over all batches within each blob
                while let Some(batch) = blob_ref.next_batch().map_err(Error::FailedReadingInput)? {
                    let result = wait_for(|| stmt.put_binary_batch(batch), &mut sleep).await;
                    result.into_result(&stmt)?;
                }
            }
        }

        // Check if a result set has been created.
        let num_result_cols = wait_for(|| statement.num_result_cols(), &mut sleep)
            .await
            .into_result(&statement)?;
        if num_result_cols == 0 {
            Ok(None)
        } else {
            // Safe: `statement` is in cursor state.
            let cursor = CursorPolling::new(statement);
            Ok(Some(cursor))
        }
    }
}

unsafe fn next_blob_param<'a>(
    stmt: &mut StatementRef<'a>,
    strict: bool,
) -> Result<Option<&'a mut dyn Blob>, Error> {
    let result = stmt.param_data();
    let maybe_ptr = if strict {
        result.into_result_strict(stmt, "SQLParamData")?
    } else {
        result.into_result(stmt)?
    };
    if let Some(blob_ptr) = maybe_ptr {
        // The safe interfaces currently exclusively bind pointers to `Blob` trait objects
        let blob_ptr: *mut &mut dyn Blob = unsafe { transmute(blob_ptr) };
        let blob_ref = unsafe { &mut *blob_ptr };
        Ok(Some(*blob_ref))
    } else {
        Ok(None)
    }
}
