"""Mutazioni che devono fallire prima di poter perdere un avviso ODBC."""
import unittest
from check_odbc_strict import inspect, ROOT, EXCEPTION_SOURCES, STRICT_FILES


class StrictTests(unittest.TestCase):
    def test_immediate_boundary_and_bypasses(self):
        for path in ('vendor/odbc-api/src/handles/statement.rs',
                     'crates/plenora-db-db2/src/read.rs'):
            good = 'unsafe { SQLFetch(self.as_sys()) }.into_sql_result_strict(self, "SQLFetch")'
            self.assertEqual(bool(inspect(path, good)[0]), path not in STRICT_FILES)
            for bad in (
                good.replace('into_sql_result_strict(self, ', 'into_sql_result('),
                'match SQLFetch(handle) { _ => Ok(()) }',
                'let result = SQLPrepare(h, sql); next_call();',
                'result.into_result(&statement)',
                'result.into_result_without_logging(&statement)',
                'match result { SqlResult::SuccessWithInfo(_) => Ok(()), _ => fail() }',
                'use odbc_sys::SQLFetch as fetch; fetch(handle);',
            ):
                self.assertTrue(inspect(path, bad)[0], bad)

    def test_all_function_references_require_closed_boundary(self):
        for path in ('new.rs', 'crates/plenora-db-db2/src/read.rs',
                     'crates/another-provider/src/lib.rs', 'new_tests.rs'):
            for source in (
                'let f = odbc_sys::SQLFetch; f(h);',
                '(odbc_sys::SQLFetch)(h);',
                'macro_rules! call { ($f:path, $h:expr) => { $f($h) }; } '
                'call!(odbc_sys::SQLFetch, h);',
                'use odbc_sys::SQLFetch as fetch;',
                'use odbc_sys::{SQLFetch as fetch}; let f = fetch;',
                'use odbc_sys::*;',
                'use odbc_sys::{self, *};',
                'use odbc_sys as ffi; use ffi::*;',
                'extern crate odbc_sys as ffi; use ffi::*;',
                'use odbc_api::{sys as ffi}; use ffi::*;',
                'use odbc_sys as ffi; let f = ffi::SQLFetch;',
                'let f = ::odbc_sys::SQLFetch;',
                'pub use odbc_sys::SQLFetch;',
                'SQLFetch(h).into_sql_result_strict(h, "fetch");',
            ):
                with self.subTest(path=path, source=source):
                    self.assertTrue(inspect(path, source)[0])

    def test_boundary_does_not_allow_indirect_references(self):
        path = 'vendor/odbc-api/src/handles/statement.rs'
        for source in ('let f = odbc_sys::SQLFetch; f(h);',
                       '(odbc_sys::SQLFetch)(h);',
                       'call!(odbc_sys::SQLFetch, h);',
                       'use odbc_sys::SQLFetch as fetch; let f = fetch;',
                       'pub use odbc_sys::SQLFetch as fetch;',
                       'pub(crate) use odbc_sys::SQLFetch as fetch;',
                       'use odbc_sys::*;'):
            with self.subTest(source=source):
                self.assertTrue(inspect(path, source)[0])

    def test_types_and_constants_are_not_functions(self):
        source = '''
            use odbc_sys::{SqlReturn, HStmt, SQLSTATE_SIZE, SQLSTATE_SIZEW};
            use odbc_sys::SQLSTATE_SIZE as state_size;
            let result = SqlReturn::SUCCESS;
            let handle: HStmt = todo!();
            let state = [0; SQLSTATE_SIZE];
        '''
        self.assertFalse(inspect('new.rs', source)[0])

    def test_current_sources(self):
        for folder in ('vendor/odbc-api/src', 'crates'):
            for path in (ROOT / folder).rglob('*.rs'):
                with self.subTest(path=path):
                    self.assertFalse(inspect(path.relative_to(ROOT).as_posix(),
                                             path.read_text(encoding='utf-8'))[0])

    def test_exception_sources_are_closed(self):
        for name in EXCEPTION_SOURCES:
            source = (ROOT / name).read_text(encoding="utf-8")
            self.assertFalse(inspect(name, source)[0])
            self.assertTrue(inspect(name, source + "\nSQLFreeHandle(t, h);")[0])

    def test_comments_do_not_create_exceptions(self):
        self.assertTrue(inspect('new.rs', 'SQLFetch(h); // .into_sql_result_strict(h, "fetch")')[0])
        self.assertFalse(inspect('new.rs', '// SQLFetch(h);\nlet text = "SuccessWithInfo";')[0])
        self.assertTrue(inspect('new.rs', 'SQLFreeHandle(t, h);')[0])

    def test_rust_literals_and_nested_comments_preserve_references(self):
        for prefix in ('let s = r#"quoted " text"#;',
                       'let s = br#"quoted " text"#;',
                       "let c = '\"';", '/* outer /* inner */ " */'):
            with self.subTest(prefix=prefix):
                self.assertTrue(inspect('new.rs', prefix + '\nlet f = odbc_sys::SQLFetch;')[0])
        self.assertFalse(inspect('new.rs', 'let s = r##"" SQLFetch ""##; '
                                 '/* outer /* SQLFetch */ SQLFetch */')[0])


if __name__ == '__main__':
    unittest.main()
