//! `Affinity` checked against the bundled SQLite: every value is stored in a
//! column of each declared type, read back, and compared with `Affinity::apply`.

#![cfg(feature = "testing")]

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, Statement};
use sqlite_diff_rs::{Affinity, Value};

type Val = Value<String, Vec<u8>>;

const DECLARED_TYPES: &[&str] = &[
    "",
    "INTEGER",
    "int",
    "BIGINT",
    "FLOATING POINT",
    "CHARINT",
    "VARCHAR(10)",
    "NATIVE CHARACTER(70)",
    "CLOB",
    "text",
    "BLOB",
    "BLOBINT",
    "REAL",
    "DOUBLE PRECISION",
    "FLOAT",
    "REAL BLOB",
    "NUMERIC",
    "DECIMAL(10,2)",
    "BOOLEAN",
    "DATETIME",
    "STRING",
    "TEXTBLOB",
];

fn values() -> Vec<Val> {
    let integers = [
        0,
        1,
        -1,
        5,
        i64::MAX,
        i64::MIN,
        123_456_789_012_345_678,
        1 << 53,
        (1 << 53) + 1,
    ];
    let reals = [
        0.0,
        -0.0,
        5.0,
        -5.0,
        5.5,
        0.1,
        49.47,
        1e5,
        1e-4,
        1e-5,
        1e15,
        1e16,
        1e17,
        1e20,
        // 2^63, -2^63, and the largest double below 2^63
        9_223_372_036_854_775_808.0,
        -9_223_372_036_854_775_808.0,
        f64::from_bits(0x43DF_FFFF_FFFF_FFFF),
        1.5e300,
        f64::MAX,
        f64::MIN_POSITIVE,
        5e-324,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1.0 / 3.0,
    ];
    let texts = [
        "5",
        " 5 ",
        "\t5\n",
        "+7",
        "00012",
        "5.",
        "3.0e+5",
        ".5",
        "-.5",
        "5.0",
        "5.5",
        "9223372036854775807",
        "-9223372036854775808",
        "9223372036854775808",
        "-9223372036854775809",
        "09223372036854775807",
        "12345678901234567890",
        "123456789012345678901234567890",
        "3500000000000000.2500001",
        "1e400",
        "-1e400",
        "1.5e-310",
        "1.0e18",
        "1e19",
        "-0",
        "-0.0",
        "00",
        "0x10",
        "abc",
        "5abc",
        "5 abc",
        "",
        " ",
        "+",
        "-",
        ".",
        "1e",
        "1e+",
        "1_000",
        "\u{663}",
        "nan",
        "inf",
        "Infinity",
        "5\u{0}abc",
    ];
    integers
        .into_iter()
        .map(Value::Integer)
        .chain(reals.into_iter().map(Value::Real))
        .chain(texts.into_iter().map(|t| Value::Text(t.to_owned())))
        .chain([Value::Blob(vec![0x35]), Value::Null])
        .collect()
}

fn to_sql(value: &Val) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(i) => SqlValue::Integer(*i),
        Value::Real(r) => SqlValue::Real(*r),
        Value::Text(t) => SqlValue::Text(t.clone()),
        Value::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

fn from_sql(value: SqlValue) -> Val {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::Integer(i),
        SqlValue::Real(r) => Value::Real(r),
        SqlValue::Text(t) => Value::Text(t),
        SqlValue::Blob(b) => Value::Blob(b),
    }
}

/// A one-row table whose single column has a given declared type.
struct Column<'c> {
    insert: Statement<'c>,
    select: Statement<'c>,
}

impl<'c> Column<'c> {
    fn new(conn: &'c Connection, table: usize, declared: &str) -> Self {
        conn.execute_batch(&format!(
            "CREATE TABLE t{table} (k INTEGER PRIMARY KEY, c {declared})"
        ))
        .unwrap();
        Self {
            insert: conn
                .prepare(&format!("INSERT OR REPLACE INTO t{table} VALUES (1, ?1)"))
                .unwrap(),
            select: conn.prepare(&format!("SELECT c FROM t{table}")).unwrap(),
        }
    }

    fn store(&mut self, value: &Val) -> Val {
        self.insert.execute([to_sql(value)]).unwrap();
        from_sql(self.select.query_row([], |row| row.get(0)).unwrap())
    }
}

#[test]
fn apply_matches_sqlite_storage_for_every_declared_type() {
    let conn = Connection::open_in_memory().unwrap();
    let mut mismatches = Vec::new();
    for (table, declared) in DECLARED_TYPES.iter().enumerate() {
        let affinity = Affinity::from_declared_type(declared);
        let mut column = Column::new(&conn, table, declared);
        for value in values() {
            let expected = column.store(&value);
            let actual = affinity.apply(value.clone());
            if actual != expected {
                mismatches.push(format!(
                    "{declared:?} ({affinity:?}) {value:?}: SQLite {expected:?}, apply {actual:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// SplitMix64, so the sweep is the same on every run.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `2^e` built from its bit pattern, exact down to the smallest subnormal.
fn power_of_two(e: i32) -> f64 {
    if e >= -1022 {
        f64::from_bits(u64::try_from(e + 1023).unwrap() << 52)
    } else {
        f64::from_bits(1 << u32::try_from(e + 1074).unwrap())
    }
}

fn real_sweep() -> Vec<f64> {
    let mut reals: Vec<f64> = values()
        .into_iter()
        .filter_map(|v| match v {
            Value::Real(r) => Some(r),
            _ => None,
        })
        .collect();
    reals.extend([
        9_007_199_254_740_992.0,
        9_007_199_254_740_994.0,
        9_007_199_254_740_996.0,
        0.300_000_000_000_000_04,
        123_456_789.123_456_79,
        1.234_567_890_123_456_7e-7,
    ]);
    reals.extend((-1074..=1023).map(power_of_two));
    reals.extend((-323..=308).map(|e| format!("1e{e}").parse::<f64>().unwrap()));
    let mut state = 0x5EED_u64;
    reals.extend(
        (0..200_000)
            .map(|_| f64::from_bits(splitmix64(&mut state)))
            .filter(|r| !r.is_nan()),
    );
    let negated: Vec<f64> = reals.iter().map(|r| -r).collect();
    reals.extend(negated);
    reals
}

#[test]
fn real_to_text_matches_sqlite_cast() {
    let conn = Connection::open_in_memory().unwrap();
    let mut cast = conn.prepare("SELECT CAST(?1 AS TEXT)").unwrap();
    let mut mismatches = Vec::new();
    for real in real_sweep() {
        let expected: String = cast.query_row([real], |row| row.get(0)).unwrap();
        let actual: Val = Affinity::Text.apply(Value::Real(real));
        if actual != Value::Text(expected.clone()) {
            mismatches.push(format!("{real:e}: SQLite {expected:?}, apply {actual:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} mismatches, first: {}",
        mismatches.len(),
        mismatches
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn text_to_number_matches_sqlite_parse() {
    let conn = Connection::open_in_memory().unwrap();
    let mut real_column = Column::new(&conn, 0, "REAL");
    let mut numeric_column = Column::new(&conn, 1, "NUMERIC");
    let mut mismatches = Vec::new();
    for real in real_sweep() {
        for rendered in [
            format!("{real:e}"),
            format!("{real:.16e}"),
            format!("{real}"),
        ] {
            let value: Val = Value::Text(rendered.clone());
            for (affinity, column) in [
                (Affinity::Real, &mut real_column),
                (Affinity::Numeric, &mut numeric_column),
            ] {
                let expected = column.store(&value);
                let actual = affinity.apply(value.clone());
                if actual != expected {
                    mismatches.push(format!(
                        "{rendered} ({affinity:?}): SQLite {expected:?}, apply {actual:?}"
                    ));
                }
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} mismatches, first: {}",
        mismatches.len(),
        mismatches
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn apply_is_idempotent() {
    for affinity in [
        Affinity::Integer,
        Affinity::Text,
        Affinity::Blob,
        Affinity::Real,
        Affinity::Numeric,
    ] {
        for value in values() {
            let once = affinity.apply(value);
            assert_eq!(affinity.apply(once.clone()), once, "{affinity:?}");
        }
    }
}
