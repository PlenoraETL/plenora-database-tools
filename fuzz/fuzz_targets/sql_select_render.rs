#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::plan::{ComparisonOperator, SortDirection};
use plenora_database_core::query::SpatialFunction;
use plenora_database_sql::{
    Dialect, DialectCapabilities, Expression, Identifier, ObjectName, Ordering, RenderedSql,
    Renderer, Select,
};

const DIALECTS: [Dialect; 7] = [
    Dialect::Postgres,
    Dialect::Mysql,
    Dialect::SqlServer,
    Dialect::Oracle,
    Dialect::Db2,
    Dialect::Sqlite,
    Dialect::Duckdb,
];

/// Cursore sui byte non fidati: nessuna lettura può fallire o allocare oltre
/// il residuo dell'input.
struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn byte(&mut self) -> u8 {
        let value = self.bytes.get(self.position).copied().unwrap_or_default();
        self.position = self.position.saturating_add(1);
        value
    }

    fn exhausted(&self) -> bool {
        self.position >= self.bytes.len()
    }

    /// Estrae una stringa di lunghezza dichiarata dal byte successivo,
    /// tollerando UTF-8 non valido.
    fn text(&mut self) -> String {
        let length = usize::from(self.byte()) % 24;
        let start = self.position.min(self.bytes.len());
        let end = start.saturating_add(length).min(self.bytes.len());
        self.position = end;
        String::from_utf8_lossy(&self.bytes[start..end]).into_owned()
    }

    fn identifier(&mut self) -> Option<Identifier> {
        Identifier::new(self.text()).ok()
    }
}

fn comparison(selector: u8) -> ComparisonOperator {
    match selector % 6 {
        0 => ComparisonOperator::Eq,
        1 => ComparisonOperator::Ne,
        2 => ComparisonOperator::Lt,
        3 => ComparisonOperator::Lte,
        4 => ComparisonOperator::Gt,
        _ => ComparisonOperator::Gte,
    }
}

fn spatial_function(selector: u8) -> SpatialFunction {
    match selector % 8 {
        0 => SpatialFunction::Intersects,
        1 => SpatialFunction::Contains,
        2 => SpatialFunction::Within,
        3 => SpatialFunction::Overlaps,
        4 => SpatialFunction::Touches,
        5 => SpatialFunction::Crosses,
        6 => SpatialFunction::DWithin,
        _ => SpatialFunction::IsValid,
    }
}

/// Costruisce un'espressione limitata in profondità: l'AST è già protetto dai
/// limiti del core, qui interessa la superficie di rendering.
fn expression(cursor: &mut Cursor<'_>, depth: usize) -> Option<Expression> {
    if depth == 0 || cursor.exhausted() {
        return None;
    }
    let selector = cursor.byte();
    let field = cursor.identifier()?;
    match selector % 9 {
        0 | 1 => {
            let count = usize::from(cursor.byte()) % 4;
            let mut args = Vec::with_capacity(count);
            for _ in 0..count {
                args.push(expression(cursor, depth - 1)?);
            }
            if selector % 9 == 0 {
                Some(Expression::And(args))
            } else {
                Some(Expression::Or(args))
            }
        }
        2 => Some(Expression::Compare {
            field,
            operator: comparison(cursor.byte()),
            parameter: cursor.text(),
        }),
        3 => Some(Expression::IsNull(field)),
        4 => Some(Expression::IsNotNull(field)),
        5 => {
            let count = usize::from(cursor.byte()) % 5;
            let parameters = (0..count).map(|_| cursor.text()).collect();
            Some(Expression::In { field, parameters })
        }
        6 => Some(Expression::Between {
            field,
            lower_parameter: cursor.text(),
            upper_parameter: cursor.text(),
        }),
        7 => Some(Expression::Like {
            field,
            parameter: cursor.text(),
            case_insensitive: cursor.byte() % 2 == 0,
        }),
        _ => {
            let function = spatial_function(cursor.byte());
            let geometry_parameter = (cursor.byte() % 2 == 0).then(|| cursor.text());
            let distance_parameter = (cursor.byte() % 2 == 0).then(|| cursor.text());
            if function == SpatialFunction::Intersects && geometry_parameter.is_some() {
                Some(Expression::SpatialIntersects {
                    field,
                    wkb_parameter: cursor.text(),
                })
            } else {
                Some(Expression::SpatialPredicate {
                    function,
                    field,
                    geometry_parameter,
                    distance_parameter,
                })
            }
        }
    }
}

fn object_name(cursor: &mut Cursor<'_>) -> Option<ObjectName> {
    let flags = cursor.byte();
    Some(ObjectName {
        catalog: if flags & 1 == 0 {
            None
        } else {
            Some(cursor.identifier()?)
        },
        schema: if flags & 2 == 0 {
            None
        } else {
            Some(cursor.identifier()?)
        },
        object: cursor.identifier()?,
    })
}

fn select(cursor: &mut Cursor<'_>) -> Option<Select> {
    let source = object_name(cursor)?;
    let projection_count = usize::from(cursor.byte()) % 6;
    let mut projection = Vec::with_capacity(projection_count);
    for _ in 0..projection_count {
        projection.push(cursor.identifier()?);
    }
    let filter = (cursor.byte() % 2 == 0)
        .then(|| expression(cursor, 5))
        .flatten();
    let order_count = usize::from(cursor.byte()) % 4;
    let mut order_by = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        order_by.push(Ordering {
            field: cursor.identifier()?,
            direction: if cursor.byte() % 2 == 0 {
                SortDirection::Asc
            } else {
                SortDirection::Desc
            },
        });
    }
    let limit = (cursor.byte() % 2 == 0).then(|| u64::from(cursor.byte()));
    Some(Select {
        source,
        projection,
        filter,
        order_by,
        limit,
    })
}

/// Il delimitatore di chiusura deve comparire solo raddoppiato: è l'unica
/// difesa contro l'iniezione via identificatore.
fn assert_quoting_round_trip(quoted: &str, original: &str, open: char, close: char) {
    let mut characters = quoted.chars();
    assert_eq!(characters.next(), Some(open));
    assert_eq!(characters.next_back(), Some(close));
    let inner: String = characters.collect();
    let doubled = close.to_string().repeat(2);
    assert_eq!(inner.replace(&doubled, &close.to_string()), original);
}

fn check_rendered(rendered: &RenderedSql) {
    assert!(!rendered.sql.contains('\0'));
    for (index, bind) in rendered.binds.iter().enumerate() {
        assert_eq!(bind.ordinal, index + 1);
    }
}

fuzz_target!(|input: &[u8]| {
    let mut cursor = Cursor::new(input);
    let Some(statement) = select(&mut cursor) else {
        return;
    };

    for dialect in DIALECTS {
        for spatial_intersects in [false, true] {
            let renderer = Renderer::new(dialect, DialectCapabilities { spatial_intersects });

            let (open, close) = match dialect {
                Dialect::Mysql => ('`', '`'),
                Dialect::SqlServer => ('[', ']'),
                _ => ('"', '"'),
            };
            for identifier in statement
                .projection
                .iter()
                .chain(std::iter::once(&statement.source.object))
            {
                assert_quoting_round_trip(
                    &renderer.quote_identifier(identifier),
                    identifier.as_str(),
                    open,
                    close,
                );
            }

            if let Ok(rendered) = renderer.render_select(&statement) {
                check_rendered(&rendered);
                assert!(rendered.sql.starts_with("SELECT "));
                let again = renderer
                    .render_select(&statement)
                    .expect("rendering deterministico");
                assert_eq!(again, rendered);
            }

            if let Some(filter) = &statement.filter {
                if let Ok(rendered) = renderer.render_filter(filter) {
                    check_rendered(&rendered);
                    assert!(!rendered.sql.is_empty());
                }
            }
        }
    }
});
