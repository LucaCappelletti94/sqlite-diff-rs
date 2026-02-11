//! Benchmark for `pg_walstream` event conversion to `SQLite` changesets.
//!
//! Measures the performance of converting `pg_walstream` `EventType`
//! to `SQLite` changeset operations.
//!
//! Note: `pg_walstream` uses binary protocol internally, so we only benchmark
//! the conversion step (not parsing, which happens at the protocol level).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pg_walstream::{ChangeEvent, EventType, Lsn, ReplicaIdentity};
use sqlite_diff_rs::{ChangeDelete, ChangesetFormat, Insert, PatchDelete, SimpleTable, Update};
use std::collections::HashMap;
use std::hint::black_box;

/// Create a simple INSERT event
fn insert_simple() -> EventType {
    let mut data = HashMap::new();
    data.insert("id".into(), serde_json::json!(1));
    data.insert("name".into(), serde_json::json!("Alice"));
    data.insert("email".into(), serde_json::json!("alice@example.com"));

    EventType::Insert {
        schema: "public".into(),
        table: "users".into(),
        relation_oid: 12345,
        data,
    }
}

/// Create a large INSERT event with many columns
fn insert_large() -> EventType {
    let mut data = HashMap::new();
    data.insert("id".into(), serde_json::json!(12345));
    data.insert("customer_id".into(), serde_json::json!(9876));
    data.insert(
        "order_date".into(),
        serde_json::json!("2024-01-15T10:30:00Z"),
    );
    data.insert("status".into(), serde_json::json!("pending"));
    data.insert("total_amount".into(), serde_json::json!(1234.56));
    data.insert(
        "shipping_address".into(),
        serde_json::json!("123 Main Street, Anytown, ST 12345, USA"),
    );
    data.insert(
        "billing_address".into(),
        serde_json::json!("456 Oak Avenue, Somewhere, ST 67890, USA"),
    );
    data.insert(
        "notes".into(),
        serde_json::json!("Please deliver between 9am and 5pm. Ring doorbell twice."),
    );
    data.insert(
        "created_at".into(),
        serde_json::json!("2024-01-15T10:30:00Z"),
    );
    data.insert(
        "updated_at".into(),
        serde_json::json!("2024-01-15T10:30:00Z"),
    );
    data.insert("is_express".into(), serde_json::json!(true));
    data.insert("discount_code".into(), serde_json::json!("SAVE20"));
    data.insert("tax_amount".into(), serde_json::json!(98.76));
    data.insert("shipping_cost".into(), serde_json::json!(15.99));
    data.insert("tracking_number".into(), serde_json::Value::Null);

    EventType::Insert {
        schema: "public".into(),
        table: "orders".into(),
        relation_oid: 12346,
        data,
    }
}

/// Create an UPDATE event
fn update_event() -> EventType {
    let mut old_data = HashMap::new();
    old_data.insert("id".into(), serde_json::json!(1));
    old_data.insert("name".into(), serde_json::json!("Alice"));
    old_data.insert("email".into(), serde_json::json!("alice@example.com"));

    let mut new_data = HashMap::new();
    new_data.insert("id".into(), serde_json::json!(1));
    new_data.insert("name".into(), serde_json::json!("Bob"));
    new_data.insert("email".into(), serde_json::json!("bob@example.com"));

    EventType::Update {
        schema: "public".into(),
        table: "users".into(),
        relation_oid: 12345,
        old_data: Some(old_data),
        new_data,
        replica_identity: ReplicaIdentity::Full,
        key_columns: vec!["id".into()],
    }
}

/// Create a DELETE event
fn delete_event() -> EventType {
    let mut old_data = HashMap::new();
    old_data.insert("id".into(), serde_json::json!(1));
    old_data.insert("name".into(), serde_json::json!("Alice"));
    old_data.insert("email".into(), serde_json::json!("alice@example.com"));

    EventType::Delete {
        schema: "public".into(),
        table: "users".into(),
        relation_oid: 12345,
        old_data,
        replica_identity: ReplicaIdentity::Full,
        key_columns: vec!["id".into()],
    }
}

/// Create an INSERT event for batch testing
fn insert_for_batch(i: usize) -> EventType {
    let mut data = HashMap::new();
    data.insert("id".into(), serde_json::json!(i));
    data.insert("name".into(), serde_json::json!(format!("User{i}")));
    data.insert(
        "email".into(),
        serde_json::json!(format!("user{i}@example.com")),
    );

    EventType::Insert {
        schema: "public".into(),
        table: "users".into(),
        relation_oid: 12345,
        data,
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
