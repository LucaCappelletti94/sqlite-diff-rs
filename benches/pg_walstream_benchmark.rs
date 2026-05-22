//! Benchmark for `pg_walstream` event conversion to `SQLite` changesets.
//!
//! Measures the performance of converting `pg_walstream` `EventType`
//! to `SQLite` changeset operations.
//!
//! Note: `pg_walstream` uses binary protocol internally, so we only benchmark
//! the conversion step (not parsing, which happens at the protocol level).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pg_walstream::{ChangeEvent, ColumnValue, EventType, Lsn, ReplicaIdentity, RowData};
use sqlite_diff_rs::{ChangeDelete, ChangesetFormat, Insert, PatchDelete, SimpleTable, Update};
use std::hint::black_box;
use std::sync::Arc;

/// Build a `RowData` from `(name, ColumnValue)` pairs.
fn row(pairs: &[(&str, ColumnValue)]) -> RowData {
    let mut data = RowData::with_capacity(pairs.len());
    for (name, value) in pairs {
        data.push(Arc::from(*name), value.clone());
    }
    data
}

fn key_cols(names: &[&str]) -> Vec<Arc<str>> {
    names.iter().map(|s| Arc::from(*s)).collect()
}

/// Create a simple INSERT event
fn insert_simple() -> EventType {
    EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 12345,
        data: row(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
            ("email", ColumnValue::text("alice@example.com")),
        ]),
    }
}

/// Create a large INSERT event with many columns
fn insert_large() -> EventType {
    EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("orders"),
        relation_oid: 12346,
        data: row(&[
            ("id", ColumnValue::text("12345")),
            ("customer_id", ColumnValue::text("9876")),
            ("order_date", ColumnValue::text("2024-01-15T10:30:00Z")),
            ("status", ColumnValue::text("pending")),
            ("total_amount", ColumnValue::text("1234.56")),
            (
                "shipping_address",
                ColumnValue::text("123 Main Street, Anytown, ST 12345, USA"),
            ),
            (
                "billing_address",
                ColumnValue::text("456 Oak Avenue, Somewhere, ST 67890, USA"),
            ),
            (
                "notes",
                ColumnValue::text("Please deliver between 9am and 5pm. Ring doorbell twice."),
            ),
            ("created_at", ColumnValue::text("2024-01-15T10:30:00Z")),
            ("updated_at", ColumnValue::text("2024-01-15T10:30:00Z")),
            ("is_express", ColumnValue::text("t")),
            ("discount_code", ColumnValue::text("SAVE20")),
            ("tax_amount", ColumnValue::text("98.76")),
            ("shipping_cost", ColumnValue::text("15.99")),
            ("tracking_number", ColumnValue::Null),
        ]),
    }
}

/// Create an UPDATE event
fn update_event() -> EventType {
    EventType::Update {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 12345,
        old_data: Some(row(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
            ("email", ColumnValue::text("alice@example.com")),
        ])),
        new_data: row(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Bob")),
            ("email", ColumnValue::text("bob@example.com")),
        ]),
        replica_identity: ReplicaIdentity::Full,
        key_columns: key_cols(&["id"]),
    }
}

/// Create a DELETE event
fn delete_event() -> EventType {
    EventType::Delete {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 12345,
        old_data: row(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
            ("email", ColumnValue::text("alice@example.com")),
        ]),
        replica_identity: ReplicaIdentity::Full,
        key_columns: key_cols(&["id"]),
    }
}

/// Create an INSERT event for batch testing
fn insert_for_batch(i: usize) -> EventType {
    let id_text = ColumnValue::text(&i.to_string());
    let name_text = ColumnValue::text(&format!("User{i}"));
    let email_text = ColumnValue::text(&format!("user{i}@example.com"));
    EventType::Insert {
        schema: Arc::from("public"),
        table: Arc::from("users"),
        relation_oid: 12345,
        data: row(&[("id", id_text), ("name", name_text), ("email", email_text)]),
    }
}

/// Create a batch of events for throughput testing
fn create_batch(count: usize) -> Vec<EventType> {
    (0..count).map(insert_for_batch).collect()
}

/// Create the table schema for users
fn users_table() -> SimpleTable {
    SimpleTable::new("users", &["id", "name", "email"], &[0])
}

/// Create the table schema for orders (large table)
fn orders_table() -> SimpleTable {
    SimpleTable::new(
        "orders",
        &[
            "id",
            "customer_id",
            "order_date",
            "status",
            "total_amount",
            "shipping_address",
            "billing_address",
            "notes",
            "created_at",
            "updated_at",
            "is_express",
            "discount_code",
            "tax_amount",
            "shipping_cost",
            "tracking_number",
        ],
        &[0],
    )
}

fn benchmark_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_walstream_conversion");

    group.bench_function("insert_simple_to_changeset", |b| {
        b.iter_batched(
            || (insert_simple(), users_table()),
            |(event, table)| {
                let _: Insert<_, String, Vec<u8>> = black_box((event, table)).try_into().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("insert_large_to_changeset", |b| {
        b.iter_batched(
            || (insert_large(), orders_table()),
            |(event, table)| {
                let _: Insert<_, String, Vec<u8>> = black_box((event, table)).try_into().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("update_to_changeset", |b| {
        b.iter_batched(
            || (update_event(), users_table()),
            |(event, table)| {
                let _: Update<_, ChangesetFormat, String, Vec<u8>> =
                    black_box((event, table)).try_into().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("delete_to_changeset", |b| {
        b.iter_batched(
            || (delete_event(), users_table()),
            |(event, table)| {
                let _: ChangeDelete<_, String, Vec<u8>> =
                    black_box((event, table)).try_into().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("delete_to_patchset", |b| {
        b.iter_batched(
            || (delete_event(), users_table()),
            |(event, table)| {
                let _: PatchDelete<_, String, Vec<u8>> =
                    black_box((event, table)).try_into().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn benchmark_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_walstream_batch_throughput");

    for batch_size in [10, 100, 1000] {
        // Estimate bytes: each event has ~100 bytes of data
        let estimated_bytes = batch_size * 100;
        group.throughput(Throughput::Bytes(estimated_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("convert_batch", batch_size),
            &batch_size,
            |b, &size| {
                b.iter_batched(
                    || create_batch(size),
                    |batch| {
                        let table = users_table();
                        for event in batch {
                            // Clone table for each conversion since we take ownership
                            let _: Insert<_, String, Vec<u8>> =
                                black_box((event, table.clone())).try_into().unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn benchmark_change_event_wrapper(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_walstream_change_event");

    group.bench_function("change_event_insert", |b| {
        b.iter_batched(
            || {
                let event = ChangeEvent {
                    event_type: insert_simple(),
                    lsn: Lsn::from(0x1234_5678_u64),
                    metadata: None,
                };
                (event, users_table())
            },
            |(event, table)| {
                let _: Insert<_, String, Vec<u8>> = black_box((event, table)).try_into().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn benchmark_serde_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_walstream_serde");

    // Benchmark serialization (EventType implements Serialize)
    group.bench_function("serialize_insert", |b| {
        let event = insert_simple();
        b.iter(|| {
            black_box(serde_json::to_string(black_box(&event)).unwrap());
        });
    });

    group.bench_function("serialize_large_insert", |b| {
        let event = insert_large();
        b.iter(|| {
            black_box(serde_json::to_string(black_box(&event)).unwrap());
        });
    });

    // Benchmark deserialization
    group.bench_function("deserialize_insert", |b| {
        let event = insert_simple();
        let json = serde_json::to_string(&event).unwrap();
        b.iter(|| {
            black_box(serde_json::from_str::<EventType>(black_box(&json)).unwrap());
        });
    });

    group.bench_function("deserialize_large_insert", |b| {
        let event = insert_large();
        let json = serde_json::to_string(&event).unwrap();
        b.iter(|| {
            black_box(serde_json::from_str::<EventType>(black_box(&json)).unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_conversion,
    benchmark_batch_throughput,
    benchmark_change_event_wrapper,
    benchmark_serde_roundtrip,
);
criterion_main!(benches);
