//! Benchmark for Debezium parsing and conversion to `SQLite` changesets.
//!
//! Measures the performance of:
//! 1. Parsing Debezium envelope JSON format
//! 2. Converting parsed envelopes to `SQLite` changeset operations

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sqlite_diff_rs::debezium::{Envelope, Op, parse};
use sqlite_diff_rs::{ChangeDelete, ChangeUpdate, Insert, SimpleTable};
use std::hint::black_box;

/// Real Debezium CREATE (insert) event from `PostgreSQL` connector documentation.
/// Source: <https://debezium.io/documentation/reference/stable/connectors/postgresql.html>
const CREATE_EVENT_JSON: &str = r#"{
    "before": null,
    "after": {
        "id": 1,
        "first_name": "Anne",
        "last_name": "Kretchmar",
        "email": "annek@noanswer.org"
    },
    "source": {
        "version": "2.7.4.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "ts_ms": 1559033904863,
        "db": "postgres",
        "schema": "public",
        "table": "customers",
        "txId": 555,
        "lsn": 24023128
    },
    "op": "c",
    "ts_ms": 1559033904863
}"#;

/// Real Debezium UPDATE event from `PostgreSQL` connector documentation.
const UPDATE_EVENT_JSON: &str = r#"{
    "before": {
        "id": 1,
        "first_name": "Anne",
        "last_name": "Kretchmar",
        "email": "annek@noanswer.org"
    },
    "after": {
        "id": 1,
        "first_name": "Anne Marie",
        "last_name": "Kretchmar",
        "email": "annek@noanswer.org"
    },
    "source": {
        "version": "2.7.4.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "ts_ms": 1559033905123,
        "db": "postgres",
        "schema": "public",
        "table": "customers",
        "txId": 556,
        "lsn": 24023256
    },
    "op": "u",
    "ts_ms": 1559033905123
}"#;

/// Real Debezium DELETE event from `PostgreSQL` connector documentation.
const DELETE_EVENT_JSON: &str = r#"{
    "before": {
        "id": 1,
        "first_name": "Anne Marie",
        "last_name": "Kretchmar",
        "email": "annek@noanswer.org"
    },
    "after": null,
    "source": {
        "version": "2.7.4.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "ts_ms": 1559033906456,
        "db": "postgres",
        "schema": "public",
        "table": "customers",
        "txId": 557,
        "lsn": 24023384
    },
    "op": "d",
    "ts_ms": 1559033906456
}"#;

/// Compact CREATE event (minified JSON like Kafka messages)
const CREATE_EVENT_COMPACT: &str = r#"{"before":null,"after":{"id":1,"first_name":"Anne","last_name":"Kretchmar","email":"annek@noanswer.org"},"source":{"version":"2.7.4.Final","connector":"postgresql","name":"PostgreSQL_server","ts_ms":1559033904863,"db":"postgres","schema":"public","table":"customers","txId":555,"lsn":24023128},"op":"c","ts_ms":1559033904863}"#;

/// Large Debezium event with many columns (orders table)
const LARGE_INSERT_JSON: &str = r#"{
    "before": null,
    "after": {
        "id": 12345,
        "customer_id": 9876,
        "order_date": "2024-01-15T10:30:00Z",
        "status": "pending",
        "total_amount": 1234.56,
        "shipping_address": "123 Main Street, Anytown, ST 12345, USA",
        "billing_address": "456 Oak Avenue, Somewhere, ST 67890, USA",
        "notes": "Please deliver between 9am and 5pm. Ring doorbell twice.",
        "created_at": "2024-01-15T10:30:00Z",
        "updated_at": "2024-01-15T10:30:00Z",
        "is_express": true,
        "discount_code": "SAVE20",
        "tax_amount": 98.76,
        "shipping_cost": 15.99,
        "tracking_number": null
    },
    "source": {
        "version": "2.7.4.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "ts_ms": 1705318200000,
        "db": "ecommerce",
        "schema": "public",
        "table": "orders",
        "txId": 12345,
        "lsn": 98765432
    },
    "op": "c",
    "ts_ms": 1705318200000
}"#;

/// Read/snapshot event (initial data load)
const READ_EVENT_JSON: &str = r#"{
    "before": null,
    "after": {
        "id": 1001,
        "first_name": "Sally",
        "last_name": "Thomas",
        "email": "sally.thomas@acme.com"
    },
    "source": {
        "version": "2.7.4.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "ts_ms": 1559033900000,
        "snapshot": "true",
        "db": "postgres",
        "schema": "public",
        "table": "customers"
    },
    "op": "r",
    "ts_ms": 1559033900000
}"#;

/// Event with transaction metadata
const CREATE_WITH_TRANSACTION_JSON: &str = r#"{
    "before": null,
    "after": {
        "id": 1,
        "first_name": "Anne",
        "last_name": "Kretchmar",
        "email": "annek@noanswer.org"
    },
    "source": {
        "version": "2.7.4.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "ts_ms": 1559033904863,
        "db": "postgres",
        "schema": "public",
        "table": "customers",
        "txId": 555,
        "lsn": 24023128
    },
    "op": "c",
    "ts_ms": 1559033904863,
    "transaction": {
        "id": "555:24023128",
        "total_order": 1,
        "data_collection_order": 1
    }
}"#;

/// Create the table schema for customers
fn customers_table() -> SimpleTable {
    SimpleTable::new(
        "customers",
        &["id", "first_name", "last_name", "email"],
        &[0],
    )
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

/// Create a batch of Debezium messages for throughput testing
fn create_batch(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            format!(
                r#"{{"before":null,"after":{{"id":{},"first_name":"User{}","last_name":"Test","email":"user{}@example.com"}},"source":{{"version":"2.7.4.Final","connector":"postgresql","name":"test","ts_ms":1559033904863,"db":"test","schema":"public","table":"customers","txId":{},"lsn":{}}},"op":"c","ts_ms":1559033904863}}"#,
                i, i, i, i, i * 100
            )
        })
        .collect()
}

fn benchmark_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("debezium_parsing");

    group.bench_function("create_event", |b| {
        b.iter(|| black_box(parse::<serde_json::Value>(black_box(CREATE_EVENT_JSON)).unwrap()));
    });

    group.bench_function("create_event_compact", |b| {
        b.iter(|| black_box(parse::<serde_json::Value>(black_box(CREATE_EVENT_COMPACT)).unwrap()));
    });

    group.bench_function("update_event", |b| {
        b.iter(|| black_box(parse::<serde_json::Value>(black_box(UPDATE_EVENT_JSON)).unwrap()));
    });

    group.bench_function("delete_event", |b| {
        b.iter(|| black_box(parse::<serde_json::Value>(black_box(DELETE_EVENT_JSON)).unwrap()));
    });

    group.bench_function("large_insert", |b| {
        b.iter(|| black_box(parse::<serde_json::Value>(black_box(LARGE_INSERT_JSON)).unwrap()));
    });

    group.bench_function("read_snapshot", |b| {
        b.iter(|| black_box(parse::<serde_json::Value>(black_box(READ_EVENT_JSON)).unwrap()));
    });

    group.bench_function("with_transaction", |b| {
        b.iter(|| {
            black_box(parse::<serde_json::Value>(black_box(CREATE_WITH_TRANSACTION_JSON)).unwrap())
        });
    });

    group.finish();
}

fn benchmark_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("debezium_conversion");

    let customers = customers_table();
    let orders = orders_table();

    // Pre-parse envelopes for conversion-only benchmarks
    let create_envelope: Envelope<serde_json::Value> = parse(CREATE_EVENT_JSON).unwrap();
    let update_envelope: Envelope<serde_json::Value> = parse(UPDATE_EVENT_JSON).unwrap();
    let delete_envelope: Envelope<serde_json::Value> = parse(DELETE_EVENT_JSON).unwrap();
    let large_envelope: Envelope<serde_json::Value> = parse(LARGE_INSERT_JSON).unwrap();
    let read_envelope: Envelope<serde_json::Value> = parse(READ_EVENT_JSON).unwrap();

    group.bench_function("create_to_insert", |b| {
        b.iter(|| {
            let env = black_box(&create_envelope);
            let table = black_box(&customers);
            let _: Insert<_, String, Vec<u8>> = (env, table).try_into().unwrap();
        });
    });

    group.bench_function("update_to_changeset", |b| {
        b.iter(|| {
            let env = black_box(&update_envelope);
            let table = black_box(&customers);
            let _: ChangeUpdate<_, String, Vec<u8>> = (env, table).try_into().unwrap();
        });
    });

    group.bench_function("delete_to_changeset", |b| {
        b.iter(|| {
            let env = black_box(&delete_envelope);
            let table = black_box(&customers);
            let _: ChangeDelete<_, String, Vec<u8>> = (env, table).try_into().unwrap();
        });
    });

    group.bench_function("large_insert_to_changeset", |b| {
        b.iter(|| {
            let env = black_box(&large_envelope);
            let table = black_box(&orders);
            let _: Insert<_, String, Vec<u8>> = (env, table).try_into().unwrap();
        });
    });

    group.bench_function("read_to_insert", |b| {
        b.iter(|| {
            let env = black_box(&read_envelope);
            let table = black_box(&customers);
            let _: Insert<_, String, Vec<u8>> = (env, table).try_into().unwrap();
        });
    });

    group.finish();
}

fn benchmark_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("debezium_end_to_end");

    let customers = customers_table();
    let orders = orders_table();

    group.bench_function("parse_and_convert_create", |b| {
        b.iter(|| {
            let json = black_box(CREATE_EVENT_JSON);
            let table = black_box(&customers);
            let env: Envelope<serde_json::Value> = parse(json).unwrap();
            let _: Insert<_, String, Vec<u8>> = (&env, table).try_into().unwrap();
        });
    });

    group.bench_function("parse_and_convert_update", |b| {
        b.iter(|| {
            let json = black_box(UPDATE_EVENT_JSON);
            let table = black_box(&customers);
            let env: Envelope<serde_json::Value> = parse(json).unwrap();
            let _: ChangeUpdate<_, String, Vec<u8>> = (&env, table).try_into().unwrap();
        });
    });

    group.bench_function("parse_and_convert_delete", |b| {
        b.iter(|| {
            let json = black_box(DELETE_EVENT_JSON);
            let table = black_box(&customers);
            let env: Envelope<serde_json::Value> = parse(json).unwrap();
            let _: ChangeDelete<_, String, Vec<u8>> = (&env, table).try_into().unwrap();
        });
    });

    group.bench_function("parse_and_convert_large", |b| {
        b.iter(|| {
            let json = black_box(LARGE_INSERT_JSON);
            let table = black_box(&orders);
            let env: Envelope<serde_json::Value> = parse(json).unwrap();
            let _: Insert<_, String, Vec<u8>> = (&env, table).try_into().unwrap();
        });
    });

    group.finish();
}

fn benchmark_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("debezium_batch_throughput");

    let customers = customers_table();

    for batch_size in [10, 100, 1000] {
        let batch = create_batch(batch_size);
        let total_bytes: usize = batch.iter().map(String::len).sum();

        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("parse_only", batch_size),
            &batch,
            |b, batch| {
                b.iter(|| {
                    for json in batch {
                        black_box(parse::<serde_json::Value>(json).unwrap());
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parse_and_convert", batch_size),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let table = black_box(&customers);
                    for json in batch {
                        let env: Envelope<serde_json::Value> = parse(json).unwrap();
                        if env.op == Op::Create {
                            let _: Insert<_, String, Vec<u8>> = (&env, table).try_into().unwrap();
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_parsing,
    benchmark_conversion,
    benchmark_end_to_end,
    benchmark_batch_throughput,
);
criterion_main!(benches);
