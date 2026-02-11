//! Benchmark for Maxwell parsing and conversion to `SQLite` changesets.
//!
//! Measures the performance of:
//! 1. Parsing Maxwell JSON format
//! 2. Converting parsed messages to `SQLite` changeset operations

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sqlite_diff_rs::maxwell::{Message, OpType, parse};
use sqlite_diff_rs::{ChangeDelete, ChangeUpdate, Insert, SimpleTable};
use std::hint::black_box;

/// Real Maxwell INSERT event
const INSERT_JSON: &str = r#"{
    "database": "mydb",
    "table": "products",
    "type": "insert",
    "ts": 1477053217,
    "xid": 23396,
    "commit": true,
    "data": {
        "id": 111,
        "name": "scooter",
        "description": "Big 2-wheel scooter",
        "weight": 5.15
    }
}"#;

/// Compact INSERT (minified JSON)
const INSERT_COMPACT: &str = r#"{"database":"mydb","table":"products","type":"insert","ts":1477053217,"xid":23396,"commit":true,"data":{"id":111,"name":"scooter","description":"Big 2-wheel scooter","weight":5.15}}"#;

/// Real Maxwell UPDATE event with old values
const UPDATE_JSON: &str = r#"{
    "database": "mydb",
    "table": "products",
    "type": "update",
    "ts": 1477053218,
    "xid": 23397,
    "data": {
        "id": 111,
        "name": "scooter",
        "description": "Big 2-wheel scooter",
        "weight": 5.18
    },
    "old": {
        "weight": 5.15
    }
}"#;

/// Real Maxwell DELETE event
const DELETE_JSON: &str = r#"{
    "database": "mydb",
    "table": "products",
    "type": "delete",
    "ts": 1477053219,
    "xid": 23398,
    "data": {
        "id": 111,
        "name": "scooter",
        "description": "Big 2-wheel scooter",
        "weight": 5.18
    }
}"#;

/// Large INSERT with many columns
const LARGE_INSERT_JSON: &str = r#"{
    "database": "ecommerce",
    "table": "orders",
    "type": "insert",
    "ts": 1705318200,
    "xid": 12345,
    "commit": true,
    "position": "master.000006:800911",
    "server_id": 23042,
    "thread_id": 108,
    "data": {
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
    }
}"#;

/// INSERT with binlog position metadata
const INSERT_WITH_POSITION_JSON: &str = r#"{
    "database": "test",
    "table": "users",
    "type": "insert",
    "ts": 1477053217,
    "xid": 23396,
    "commit": true,
    "position": "master.000006:800911",
    "server_id": 23042,
    "thread_id": 108,
    "primary_key": [1],
    "primary_key_columns": ["id"],
    "data": {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com"
    }
}"#;

fn products_table() -> SimpleTable {
    SimpleTable::new("products", &["id", "name", "description", "weight"], &[0])
}

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

fn users_table() -> SimpleTable {
    SimpleTable::new("users", &["id", "name", "email"], &[0])
}

/// Create a batch of Maxwell messages for throughput testing
fn create_batch(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            format!(
                r#"{{"database":"mydb","table":"users","type":"insert","ts":1477053217,"xid":{i},"data":{{"id":{i},"name":"User{i}","email":"user{i}@example.com"}}}}"#
            )
        })
        .collect()
}

fn benchmark_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("maxwell_parsing");

    group.bench_function("insert", |b| {
        b.iter(|| black_box(parse(black_box(INSERT_JSON)).unwrap()));
    });

    group.bench_function("insert_compact", |b| {
        b.iter(|| black_box(parse(black_box(INSERT_COMPACT)).unwrap()));
    });

    group.bench_function("update", |b| {
        b.iter(|| black_box(parse(black_box(UPDATE_JSON)).unwrap()));
    });

    group.bench_function("delete", |b| {
        b.iter(|| black_box(parse(black_box(DELETE_JSON)).unwrap()));
    });

    group.bench_function("large_insert", |b| {
        b.iter(|| black_box(parse(black_box(LARGE_INSERT_JSON)).unwrap()));
    });

    group.bench_function("with_position", |b| {
        b.iter(|| black_box(parse(black_box(INSERT_WITH_POSITION_JSON)).unwrap()));
    });

    group.finish();
}

fn benchmark_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("maxwell_conversion");

    let products = products_table();
    let orders = orders_table();
    let users = users_table();

    // Pre-parse messages for conversion-only benchmarks
    let insert_msg: Message = parse(INSERT_JSON).unwrap();
    let update_msg: Message = parse(UPDATE_JSON).unwrap();
    let delete_msg: Message = parse(DELETE_JSON).unwrap();
    let large_msg: Message = parse(LARGE_INSERT_JSON).unwrap();
    let users_msg: Message = parse(INSERT_WITH_POSITION_JSON).unwrap();

    group.bench_function("insert_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&insert_msg);
            let table = black_box(&products);
            let _: Insert<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.bench_function("update_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&update_msg);
            let table = black_box(&products);
            let _: ChangeUpdate<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.bench_function("delete_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&delete_msg);
            let table = black_box(&products);
            let _: ChangeDelete<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.bench_function("large_insert_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&large_msg);
            let table = black_box(&orders);
            let _: Insert<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.bench_function("users_insert_to_changeset", |b| {
        b.iter(|| {
            let msg = black_box(&users_msg);
            let table = black_box(&users);
            let _: Insert<_, String, Vec<u8>> = (msg, table).try_into().unwrap();
        });
    });

    group.finish();
}

fn benchmark_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("maxwell_end_to_end");

    let products = products_table();
    let orders = orders_table();

    group.bench_function("parse_and_convert_insert", |b| {
        b.iter(|| {
            let json = black_box(INSERT_JSON);
            let table = black_box(&products);
            let msg = parse(json).unwrap();
            let _: Insert<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
        });
    });

    group.bench_function("parse_and_convert_update", |b| {
        b.iter(|| {
            let json = black_box(UPDATE_JSON);
            let table = black_box(&products);
            let msg = parse(json).unwrap();
            let _: ChangeUpdate<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
        });
    });

    group.bench_function("parse_and_convert_delete", |b| {
        b.iter(|| {
            let json = black_box(DELETE_JSON);
            let table = black_box(&products);
            let msg = parse(json).unwrap();
            let _: ChangeDelete<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
        });
    });

    group.bench_function("parse_and_convert_large", |b| {
        b.iter(|| {
            let json = black_box(LARGE_INSERT_JSON);
            let table = black_box(&orders);
            let msg = parse(json).unwrap();
            let _: Insert<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
        });
    });

    group.finish();
}

fn benchmark_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("maxwell_batch_throughput");

    let users = users_table();

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
                        black_box(parse(json).unwrap());
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
                        let msg = parse(json).unwrap();
                        if msg.op_type == OpType::Insert {
                            let _: Insert<_, String, Vec<u8>> = (&msg, table).try_into().unwrap();
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
