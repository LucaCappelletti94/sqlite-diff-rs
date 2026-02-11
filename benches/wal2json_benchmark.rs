//! Benchmark for wal2json parsing and conversion to `SQLite` changesets.
//!
//! Measures the performance of:
//! 1. Parsing wal2json v1 and v2 JSON formats
//! 2. Converting parsed messages to `SQLite` changeset operations

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sqlite_diff_rs::wal2json::{
    Action, ChangeV1, Column, MessageV2, OldKeys, TransactionV1, parse_v1, parse_v2,
};
use sqlite_diff_rs::{ChangeDelete, Insert, SimpleTable};
use std::hint::black_box;

/// Create a simple v2 INSERT message
fn v2_insert_simple() -> MessageV2 {
    MessageV2 {
        action: Action::I,
        schema: Some("public".into()),
        table: Some("users".into()),
        columns: Some(vec![
            Column {
                name: "id".into(),
                type_name: "integer".into(),
                value: serde_json::json!(1),
            },
            Column {
                name: "name".into(),
                type_name: "text".into(),
                value: serde_json::json!("Alice"),
            },
            Column {
                name: "email".into(),
                type_name: "text".into(),
                value: serde_json::json!("alice@example.com"),
            },
        ]),
        identity: None,
    }
}

/// Create a large v2 INSERT message with many columns
fn v2_insert_large() -> MessageV2 {
    MessageV2 {
        action: Action::I,
        schema: Some("public".into()),
        table: Some("orders".into()),
        columns: Some(vec![
            Column {
                name: "id".into(),
                type_name: "integer".into(),
                value: serde_json::json!(12345),
            },
            Column {
                name: "customer_id".into(),
                type_name: "integer".into(),
                value: serde_json::json!(9876),
            },
            Column {
                name: "order_date".into(),
                type_name: "timestamp".into(),
                value: serde_json::json!("2024-01-15T10:30:00Z"),
            },
            Column {
                name: "status".into(),
                type_name: "text".into(),
                value: serde_json::json!("pending"),
            },
            Column {
                name: "total_amount".into(),
                type_name: "numeric".into(),
                value: serde_json::json!(1234.56),
            },
            Column {
                name: "shipping_address".into(),
                type_name: "text".into(),
                value: serde_json::json!("123 Main Street, Anytown, ST 12345, USA"),
            },
            Column {
                name: "billing_address".into(),
                type_name: "text".into(),
                value: serde_json::json!("456 Oak Avenue, Somewhere, ST 67890, USA"),
            },
            Column {
                name: "notes".into(),
                type_name: "text".into(),
                value: serde_json::json!(
                    "Please deliver between 9am and 5pm. Ring doorbell twice."
                ),
            },
            Column {
                name: "created_at".into(),
                type_name: "timestamp".into(),
                value: serde_json::json!("2024-01-15T10:30:00Z"),
            },
            Column {
                name: "updated_at".into(),
                type_name: "timestamp".into(),
                value: serde_json::json!("2024-01-15T10:30:00Z"),
            },
            Column {
                name: "is_express".into(),
                type_name: "boolean".into(),
                value: serde_json::json!(true),
            },
            Column {
                name: "discount_code".into(),
                type_name: "text".into(),
                value: serde_json::json!("SAVE20"),
            },
            Column {
                name: "tax_amount".into(),
                type_name: "numeric".into(),
                value: serde_json::json!(98.76),
            },
            Column {
                name: "shipping_cost".into(),
                type_name: "numeric".into(),
                value: serde_json::json!(15.99),
            },
            Column {
                name: "tracking_number".into(),
                type_name: "text".into(),
                value: serde_json::Value::Null,
            },
        ]),
        identity: None,
    }
}

/// Create a v2 UPDATE message
fn v2_update() -> MessageV2 {
    MessageV2 {
        action: Action::U,
        schema: Some("public".into()),
        table: Some("users".into()),
        columns: Some(vec![
            Column {
                name: "id".into(),
                type_name: "integer".into(),
                value: serde_json::json!(1),
            },
            Column {
                name: "name".into(),
                type_name: "text".into(),
                value: serde_json::json!("Bob"),
            },
            Column {
                name: "email".into(),
                type_name: "text".into(),
                value: serde_json::json!("bob@example.com"),
            },
        ]),
        identity: Some(vec![Column {
            name: "id".into(),
            type_name: "integer".into(),
            value: serde_json::json!(1),
        }]),
    }
}

/// Create a v2 DELETE message
fn v2_delete() -> MessageV2 {
    MessageV2 {
        action: Action::D,
        schema: Some("public".into()),
        table: Some("users".into()),
        columns: None,
        identity: Some(vec![
            Column {
                name: "id".into(),
                type_name: "integer".into(),
                value: serde_json::json!(1),
            },
            Column {
                name: "name".into(),
                type_name: "text".into(),
                value: serde_json::json!("Alice"),
            },
            Column {
                name: "email".into(),
                type_name: "text".into(),
                value: serde_json::json!("alice@example.com"),
            },
        ]),
    }
}

/// Create a v1 transaction with multiple changes
fn v1_transaction() -> TransactionV1 {
    TransactionV1 {
        change: vec![
            ChangeV1 {
                kind: "insert".into(),
                schema: "public".into(),
                table: "users".into(),
                columnnames: vec!["id".into(), "name".into(), "email".into()],
                columntypes: vec!["integer".into(), "text".into(), "text".into()],
                columnvalues: vec![
                    serde_json::json!(1),
                    serde_json::json!("Alice"),
                    serde_json::json!("alice@example.com"),
                ],
                oldkeys: None,
            },
            ChangeV1 {
                kind: "insert".into(),
                schema: "public".into(),
                table: "users".into(),
                columnnames: vec!["id".into(), "name".into(), "email".into()],
                columntypes: vec!["integer".into(), "text".into(), "text".into()],
                columnvalues: vec![
                    serde_json::json!(2),
                    serde_json::json!("Bob"),
                    serde_json::json!("bob@example.com"),
                ],
                oldkeys: None,
            },
            ChangeV1 {
                kind: "update".into(),
                schema: "public".into(),
                table: "users".into(),
                columnnames: vec!["id".into(), "name".into(), "email".into()],
                columntypes: vec!["integer".into(), "text".into(), "text".into()],
                columnvalues: vec![
                    serde_json::json!(1),
                    serde_json::json!("Alice Updated"),
                    serde_json::json!("alice@example.com"),
                ],
                oldkeys: Some(OldKeys {
                    keynames: vec!["id".into()],
                    keytypes: vec!["integer".into()],
                    keyvalues: vec![serde_json::json!(1)],
                }),
            },
            ChangeV1 {
                kind: "delete".into(),
                schema: "public".into(),
                table: "users".into(),
                columnnames: vec![],
                columntypes: vec![],
                columnvalues: vec![],
                oldkeys: Some(OldKeys {
                    keynames: vec!["id".into()],
                    keytypes: vec!["integer".into()],
                    keyvalues: vec![serde_json::json!(2)],
                }),
            },
        ],
    }
}

/// Create a v2 INSERT message for batch testing
fn v2_insert_for_batch(i: usize) -> MessageV2 {
    MessageV2 {
        action: Action::I,
        schema: Some("public".into()),
        table: Some("users".into()),
        columns: Some(vec![
            Column {
                name: "id".into(),
                type_name: "integer".into(),
                value: serde_json::json!(i),
            },
            Column {
                name: "name".into(),
                type_name: "text".into(),
                value: serde_json::json!(format!("User{i}")),
            },
            Column {
                name: "email".into(),
                type_name: "text".into(),
                value: serde_json::json!(format!("user{i}@example.com")),
            },
        ]),
        identity: None,
    }
}

/// Create a batch of v2 messages serialized to JSON for throughput testing
fn create_v2_batch(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| serde_json::to_string(&v2_insert_for_batch(i)).unwrap())
        .collect()
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

fn benchmark_v2_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal2json_v2_parsing");

    // Serialize structs to JSON at setup time
    let insert_simple_json = serde_json::to_string(&v2_insert_simple()).unwrap();
    let insert_large_json = serde_json::to_string(&v2_insert_large()).unwrap();
    let update_json = serde_json::to_string(&v2_update()).unwrap();
    let delete_json = serde_json::to_string(&v2_delete()).unwrap();

    group.bench_function("insert_simple", |b| {
        b.iter(|| black_box(parse_v2(black_box(&insert_simple_json)).unwrap()));
    });

    group.bench_function("insert_large", |b| {
        b.iter(|| black_box(parse_v2(black_box(&insert_large_json)).unwrap()));
    });

    group.bench_function("update", |b| {
        b.iter(|| black_box(parse_v2(black_box(&update_json)).unwrap()));
    });

    group.bench_function("delete", |b| {
        b.iter(|| black_box(parse_v2(black_box(&delete_json)).unwrap()));
    });

    group.finish();
}

fn benchmark_v1_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal2json_v1_parsing");

    let transaction_json = serde_json::to_string(&v1_transaction()).unwrap();

    group.bench_function("transaction_4_changes", |b| {
        b.iter(|| black_box(parse_v1(black_box(&transaction_json)).unwrap()));
    });

    group.finish();
}

fn benchmark_v2_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal2json_v2_conversion");

    let users = users_table();
    let orders = orders_table();

    // Use structs directly for conversion benchmarks (no parsing overhead)
    let insert_msg = v2_insert_simple();
    let insert_large_msg = v2_insert_large();
    let delete_msg = v2_delete();

    group.bench_function("insert_simple_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&insert_msg);
            let table = black_box(&users);
            let _: Insert<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.bench_function("insert_large_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&insert_large_msg);
            let table = black_box(&orders);
            let _: Insert<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.bench_function("delete_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&delete_msg);
            let table = black_box(&users);
            let _: ChangeDelete<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.finish();
}

fn benchmark_v1_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal2json_v1_conversion");

    let users = users_table();

    // Use struct directly
    let tx = v1_transaction();

    group.bench_function("transaction_all_changes", |b| {
        b.iter(|| {
            let tx = black_box(&tx);
            let table = black_box(&users);

            for change in &tx.change {
                match change.kind.as_str() {
                    "insert" => {
                        let _: Insert<_, String, Vec<u8>> = (change, table).try_into().unwrap();
                    }
                    "delete" => {
                        let _: ChangeDelete<_, String, Vec<u8>> =
                            (change, table).try_into().unwrap();
                    }
                    _ => {}
                }
            }
        });
    });

    group.finish();
}

fn benchmark_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal2json_batch_throughput");

    let users = users_table();

    for batch_size in [10, 100, 1000] {
        let batch = create_v2_batch(batch_size);
        let total_bytes: usize = batch.iter().map(String::len).sum();

        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("parse_only", batch_size),
            &batch,
            |b, batch| {
                b.iter(|| {
                    for json in batch {
                        black_box(parse_v2(json).unwrap());
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parse_and_convert", batch_size),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let table = black_box(&users);
                    for json in batch {
                        let msg = parse_v2(json).unwrap();
                        if msg.action == Action::I {
                            let _: Insert<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

fn benchmark_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal2json_end_to_end");

    let users = users_table();

    // Serialize to JSON for end-to-end benchmarks
    let insert_json = serde_json::to_string(&v2_insert_simple()).unwrap();
    let delete_json = serde_json::to_string(&v2_delete()).unwrap();

    group.bench_function("parse_convert_insert", |b| {
        b.iter(|| {
            let json = black_box(&insert_json);
            let table = black_box(&users);

            let msg = parse_v2(json).unwrap();
            let _: Insert<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
        });
    });

    group.bench_function("parse_convert_delete", |b| {
        b.iter(|| {
            let json = black_box(&delete_json);
            let table = black_box(&users);

            let msg = parse_v2(json).unwrap();
            let _: ChangeDelete<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_v2_parsing,
    benchmark_v1_parsing,
    benchmark_v2_conversion,
    benchmark_v1_conversion,
    benchmark_batch_throughput,
    benchmark_end_to_end,
);
criterion_main!(benches);
