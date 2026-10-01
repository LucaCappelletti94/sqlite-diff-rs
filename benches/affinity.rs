//! Cost of each column affinity conversion `Affinity::apply` performs.

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use sqlite_diff_rs::{Affinity, Value};
use std::hint::black_box;

type Val = Value<String, Vec<u8>>;

/// Doubles whose text SQLite renders short, long, and in exponent form.
const REALS: &[f64] = &[
    0.1,
    49.47,
    1.0 / 3.0,
    1e5,
    1e-5,
    1e17,
    9_223_372_036_854_775_808.0,
    1.234_567_890_123_456_7e-300,
    5e-324,
];

fn texts(values: &[&str]) -> Vec<Val> {
    values.iter().map(|&t| Value::Text(t.to_owned())).collect()
}

fn apply_all(c: &mut Criterion, name: &str, affinity: Affinity, inputs: &[Val]) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || inputs.to_vec(),
            |values| {
                for value in values {
                    black_box(affinity.apply(black_box(value)));
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_affinity(c: &mut Criterion) {
    apply_all(
        c,
        "affinity/text_to_integer",
        Affinity::Integer,
        &texts(&["5", " 42 ", "-9223372036854775808", "00012", "3.0e+5"]),
    );
    apply_all(
        c,
        "affinity/text_to_real",
        Affinity::Real,
        &texts(&["0.1", "49.47", "1e-5", "1.7976931348623157e308", ".5"]),
    );
    apply_all(
        c,
        "affinity/text_stays_text",
        Affinity::Numeric,
        &texts(&["abc", "5abc", "0x10", "", "1e"]),
    );
    let integers: Vec<Val> = [0, -1, 42, i64::MIN, i64::MAX]
        .into_iter()
        .map(Value::Integer)
        .collect();
    apply_all(c, "affinity/integer_to_text", Affinity::Text, &integers);
    let reals: Vec<Val> = REALS.iter().copied().map(Value::Real).collect();
    apply_all(c, "affinity/real_to_text", Affinity::Text, &reals);

    c.bench_function("affinity/real_to_text_rust_format_reference", |b| {
        b.iter(|| {
            for &real in REALS {
                black_box(format!("{:e}", black_box(real)));
            }
        });
    });
}

criterion_group!(benches, bench_affinity);
criterion_main!(benches);
