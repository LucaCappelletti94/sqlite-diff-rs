//! Benchmark comparing changeset/patchset generation methods.
//!
//! Compares three approaches:
//! 1. Using rusqlite's native session API
//! 2. Using our builder API directly (programmatic construction)
//! 3. Using our SQL parser to build from SQL statements

use criterion::{Criterion, criterion_group, criterion_main};
use rusqlite::Connection;
use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangeUpdate, Insert, PatchSet, PatchUpdate, TableSchema, Value,
};
use std::hint::black_box;
use std::string::String;

/// Large realistic schema for benchmarking
const SCHEMA: &str = "
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    last_login INTEGER,
    is_active INTEGER NOT NULL DEFAULT 1,
    profile_data BLOB
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER,
    view_count INTEGER NOT NULL DEFAULT 0,
    is_published INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    parent_id INTEGER,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (parent_id) REFERENCES comments(id)
);

CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE post_tags (
    post_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (post_id, tag_id),
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (tag_id) REFERENCES tags(id)
);
";

/// SQL operations to perform on the database
const OPERATIONS: &[&str] = &[
    // Insert users
    "INSERT INTO users (id, username, email, created_at, is_active) VALUES (1, 'alice', 'alice@example.com', 1000000, 1)",
    "INSERT INTO users (id, username, email, created_at, is_active) VALUES (2, 'bob', 'bob@example.com', 1000100, 1)",
    "INSERT INTO users (id, username, email, created_at, is_active) VALUES (3, 'charlie', 'charlie@example.com', 1000200, 1)",
    "INSERT INTO users (id, username, email, created_at, is_active) VALUES (4, 'diana', 'diana@example.com', 1000300, 0)",
    "INSERT INTO users (id, username, email, created_at, is_active) VALUES (5, 'eve', 'eve@example.com', 1000400, 1)",
    // Insert posts
    "INSERT INTO posts (id, user_id, title, content, created_at, is_published) VALUES (1, 1, 'First Post', 'Hello World!', 1000500, 1)",
    "INSERT INTO posts (id, user_id, title, content, created_at, is_published) VALUES (2, 1, 'Second Post', 'More content', 1000600, 1)",
    "INSERT INTO posts (id, user_id, title, content, created_at, is_published) VALUES (3, 2, 'Bob''s Post', 'My thoughts', 1000700, 1)",
    "INSERT INTO posts (id, user_id, title, content, created_at, is_published) VALUES (4, 3, 'Draft', 'Work in progress', 1000800, 0)",
    "INSERT INTO posts (id, user_id, title, content, created_at, is_published) VALUES (5, 5, 'Eve''s Post', 'Latest news', 1000900, 1)",
    // Insert tags
    "INSERT INTO tags (id, name) VALUES (1, 'rust')",
    "INSERT INTO tags (id, name) VALUES (2, 'database')",
    "INSERT INTO tags (id, name) VALUES (3, 'tutorial')",
    "INSERT INTO tags (id, name) VALUES (4, 'news')",
    "INSERT INTO tags (id, name) VALUES (5, 'discussion')",
    // Insert post_tags
    "INSERT INTO post_tags (post_id, tag_id) VALUES (1, 1)",
    "INSERT INTO post_tags (post_id, tag_id) VALUES (1, 3)",
    "INSERT INTO post_tags (post_id, tag_id) VALUES (2, 1)",
    "INSERT INTO post_tags (post_id, tag_id) VALUES (3, 5)",
    "INSERT INTO post_tags (post_id, tag_id) VALUES (5, 4)",
    // Insert comments
    "INSERT INTO comments (id, post_id, user_id, content, created_at) VALUES (1, 1, 2, 'Great post!', 1001000)",
    "INSERT INTO comments (id, post_id, user_id, content, created_at) VALUES (2, 1, 3, 'Thanks for sharing', 1001100)",
    "INSERT INTO comments (id, post_id, user_id, content, created_at) VALUES (3, 2, 2, 'Interesting', 1001200)",
    "INSERT INTO comments (id, post_id, user_id, content, created_at) VALUES (4, 3, 1, 'Nice work', 1001300)",
    "INSERT INTO comments (id, post_id, user_id, content, created_at) VALUES (5, 1, 5, 'Reply to comment 1', 1001400)",
    // Update operations
    "UPDATE users SET last_login = 1002000 WHERE id = 1",
    "UPDATE users SET last_login = 1002100 WHERE id = 2",
    "UPDATE posts SET view_count = 10 WHERE id = 1",
    "UPDATE posts SET view_count = 5 WHERE id = 2",
    "UPDATE posts SET updated_at = 1002200, content = 'Updated content' WHERE id = 2",
    // Delete operations
    "DELETE FROM comments WHERE id = 5",
    "DELETE FROM post_tags WHERE post_id = 3 AND tag_id = 5",
    "DELETE FROM users WHERE id = 4",
];

/// Create table schemas programmatically.
fn create_schemas() -> (
    TableSchema<&'static str>,
    TableSchema<&'static str>,
    TableSchema<&'static str>,
    TableSchema<&'static str>,
    TableSchema<&'static str>,
) {
    // users: id(PK), username, email, created_at, last_login, is_active, profile_data
    let users = TableSchema::new("users", 7, vec![1, 0, 0, 0, 0, 0, 0]);
    // posts: id(PK), user_id, title, content, created_at, updated_at, view_count, is_published
    let posts = TableSchema::new("posts", 8, vec![1, 0, 0, 0, 0, 0, 0, 0]);
    // comments: id(PK), post_id, user_id, content, created_at, parent_id, is_deleted
    let comments = TableSchema::new("comments", 7, vec![1, 0, 0, 0, 0, 0, 0]);
    // tags: id(PK), name
    let tags = TableSchema::new("tags", 2, vec![1, 0]);
    // post_tags: post_id(PK1), tag_id(PK2)
    let post_tags = TableSchema::new("post_tags", 2, vec![1, 2]);

    (users, posts, comments, tags, post_tags)
}

/// Get user data rows for inserts
fn get_user_rows() -> Vec<Vec<Value<String, Vec<u8>>>> {
    vec![
        vec![
            1i64.into(),
            "alice",
            "alice@example.com",
            1000000i64.into(),
            Value::Null,
            1i64.into(),
            Value::Null,
        ],
        vec![
            2i64.into(),
            "bob",
            "bob@example.com",
            1000100i64.into(),
            Value::Null,
            1i64.into(),
            Value::Null,
        ],
        vec![
            3i64.into(),
            "charlie",
            "charlie@example.com",
            1000200i64.into(),
            Value::Null,
            1i64.into(),
            Value::Null,
        ],
        vec![
            4i64.into(),
            "diana",
            "diana@example.com",
            1000300i64.into(),
            Value::Null,
            0i64.into(),
            Value::Null,
        ],
        vec![
            5i64.into(),
            "eve",
            "eve@example.com",
            1000400i64.into(),
            Value::Null,
            1i64.into(),
            Value::Null,
        ],
    ]
}

/// Get post data rows for inserts
fn get_post_rows() -> Vec<Vec<Value<String, Vec<u8>>>> {
    vec![
        vec![
            1i64.into(),
            1i64.into(),
            "First Post",
            "Hello World!",
            1000500i64.into(),
            Value::Null,
            0i64.into(),
            1i64.into(),
        ],
        vec![
            2i64.into(),
            1i64.into(),
            "Second Post",
            "More content",
            1000600i64.into(),
            Value::Null,
            0i64.into(),
            1i64.into(),
        ],
        vec![
            3i64.into(),
            2i64.into(),
            "Bob's Post",
            "My thoughts",
            1000700i64.into(),
            Value::Null,
            0i64.into(),
            1i64.into(),
        ],
        vec![
            4i64.into(),
            3i64.into(),
            "Draft",
            "Work in progress",
            1000800i64.into(),
            Value::Null,
            0i64.into(),
            0i64.into(),
        ],
        vec![
            5i64.into(),
            5i64.into(),
            "Eve's Post",
            "Latest news",
            1000900i64.into(),
            Value::Null,
            0i64.into(),
            1i64.into(),
        ],
    ]
}

/// Get tag data rows for inserts
fn get_tag_rows() -> Vec<Vec<Value<String, Vec<u8>>>> {
    vec![
        vec![1i64.into(), "rust"],
        vec![2i64.into(), "database"],
        vec![3i64.into(), "tutorial"],
        vec![4i64.into(), "news"],
        vec![5i64.into(), "discussion"],
    ]
}

/// Get post-tag data rows for inserts
fn get_post_tag_rows() -> Vec<Vec<Value<String, Vec<u8>>>> {
    vec![
        vec![1i64.into(), 1i64.into()],
        vec![1i64.into(), 3i64.into()],
        vec![2i64.into(), 1i64.into()],
        vec![3i64.into(), 5i64.into()],
        vec![5i64.into(), 4i64.into()],
    ]
}

/// Get comment data rows for inserts
fn get_comment_rows() -> Vec<Vec<Value<String, Vec<u8>>>> {
    vec![
        vec![
            1i64.into(),
            1i64.into(),
            2i64.into(),
            "Great post!",
            1001000i64.into(),
            Value::Null,
            0i64.into(),
        ],
        vec![
            2i64.into(),
            1i64.into(),
            3i64.into(),
            "Thanks for sharing",
            1001100i64.into(),
            Value::Null,
            0i64.into(),
        ],
        vec![
            3i64.into(),
            2i64.into(),
            2i64.into(),
            "Interesting",
            1001200i64.into(),
            Value::Null,
            0i64.into(),
        ],
        vec![
            4i64.into(),
            3i64.into(),
            1i64.into(),
            "Nice work",
            1001300i64.into(),
            Value::Null,
            0i64.into(),
        ],
        vec![
            5i64.into(),
            1i64.into(),
            5i64.into(),
            "Reply to comment 1",
            1001400i64.into(),
            Value::Null,
            0i64.into(),
        ],
    ]
}

/// Generate changeset using rusqlite's session API
fn rusqlite_changeset_with(schema: &str, operations: &[&str]) -> Vec<u8> {
    let conn = Connection::open_in_memory().unwrap();

    // Set up session
    conn.execute_batch(schema).unwrap();
    let mut session = rusqlite::session::Session::new(&conn).unwrap();
    session.attach::<&str>(None).unwrap();

    // Execute operations
    for op in operations {
        conn.execute(op, []).unwrap();
    }

    // Generate changeset
    let mut bytes = Vec::new();
    session.changeset_strm(&mut bytes).unwrap();
    bytes
}

/// Generate patchset using rusqlite's session API
fn rusqlite_patchset_with(schema: &str, operations: &[&str]) -> Vec<u8> {
    let conn = Connection::open_in_memory().unwrap();

    // Set up session
    conn.execute_batch(schema).unwrap();
    let mut session = rusqlite::session::Session::new(&conn).unwrap();
    session.attach::<&str>(None).unwrap();

    // Execute operations
    for op in operations {
        conn.execute(op, []).unwrap();
    }

    // Generate patchset
    let mut bytes = Vec::new();
    session.patchset_strm(&mut bytes).unwrap();
    bytes
}

/// Add insert operations for all tables to the changeset builder
fn add_inserts_to_changeset(
    mut builder: ChangeSet<TableSchema<&'static str>, &'static str, Vec<u8>>,
    users: &TableSchema<&'static str>,
    posts: &TableSchema<&'static str>,
    comments: &TableSchema<&'static str>,
    tags: &TableSchema<&'static str>,
    post_tags: &TableSchema<&'static str>,
) -> ChangeSet<TableSchema<&'static str>, &'static str, Vec<u8>> {
    // User inserts
    for row in get_user_rows() {
        let mut insert = Insert::from(users);
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post inserts
    for row in get_post_rows() {
        let mut insert = Insert::from(posts);
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Tag inserts
    for row in get_tag_rows() {
        let mut insert = Insert::from(tags);
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post-tag inserts
    for row in get_post_tag_rows() {
        let mut insert = Insert::from(post_tags);
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Comment inserts
    for row in get_comment_rows() {
        let mut insert = Insert::from(comments);
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val).unwrap();
        }
        builder = builder.insert(insert);
    }

    builder
}

/// Add user update operations to the changeset builder
fn add_user_updates_to_changeset(
    mut builder: ChangeSet<TableSchema<&'static str>, &'static str, Vec<u8>>,
    users: &TableSchema<&'static str>,
) -> ChangeSet<TableSchema<&'static str>, &'static str, Vec<u8>> {
    // User updates: [(old_values, new_values)]
    let user_updates: &[(
        &[Value<&'static str, Vec<u8>>],
        &[Value<&'static str, Vec<u8>>],
    )] = &[
        (
            &[
                1i64.into(),
                "alice",
                "alice@example.com",
                1000000i64.into(),
                Value::Null,
                1i64.into(),
                Value::Null,
            ],
            &[
                1i64.into(),
                "alice",
                "alice@example.com",
                1000000i64.into(),
                1002000i64.into(),
                1i64.into(),
                Value::Null,
            ],
        ),
        (
            &[
                2i64.into(),
                "bob",
                "bob@example.com",
                1000100i64.into(),
                Value::Null,
                1i64.into(),
                Value::Null,
            ],
            &[
                2i64.into(),
                "bob",
                "bob@example.com",
                1000100i64.into(),
                1002100i64.into(),
                1i64.into(),
                Value::Null,
            ],
        ),
    ];
    for (old, new) in user_updates {
        let mut update = ChangeUpdate::from(users.clone());
        for (i, (o, n)) in old.iter().zip(new.iter()).enumerate() {
            update = update.set(i, o.clone(), n.clone()).unwrap();
        }
        builder = builder.update(update);
    }

    builder
}

/// Add post update operations to the changeset builder
fn add_post_updates_to_changeset(
    mut builder: ChangeSet<TableSchema<&'static str>, &'static str, Vec<u8>>,
    posts: &TableSchema<&'static str>,
) -> ChangeSet<TableSchema<&'static str>, &'static str, Vec<u8>> {
    // Post updates
    let post_updates: &[(
        &[Value<&'static str, Vec<u8>>],
        &[Value<&'static str, Vec<u8>>],
    )] = &[
        (
            &[
                1i64.into(),
                1i64.into(),
                "First Post".into(),
                "Hello World!".into(),
                1000500i64.into(),
                Value::Null,
                0i64.into(),
                1i64.into(),
            ],
            &[
                1i64.into(),
                1i64.into(),
                "First Post".into(),
                "Hello World!".into(),
                1000500i64.into(),
                Value::Null,
                10i64.into(),
                1i64.into(),
            ],
        ),
        (
            &[
                2i64.into(),
                1i64.into(),
                "Second Post".into(),
                "More content".into(),
                1000600i64.into(),
                Value::Null,
                0i64.into(),
                1i64.into(),
            ],
            &[
                2i64.into(),
                1i64.into(),
                "Second Post".into(),
                "More content".into(),
                1000600i64.into(),
                Value::Null,
                5i64.into(),
                1i64.into(),
            ],
        ),
        (
            &[
                2i64.into(),
                1i64.into(),
                "Second Post".into(),
                "More content".into(),
                1000600i64.into(),
                Value::Null,
                5i64.into(),
                1i64.into(),
            ],
            &[
                2i64.into(),
                1i64.into(),
                "Second Post".into(),
                "Updated content".into(),
                1000600i64.into(),
                1002200i64.into(),
                5i64.into(),
                1i64.into(),
            ],
        ),
    ];
    for (old, new) in post_updates {
        let mut update = ChangeUpdate::from(posts.clone());
        for (i, (o, n)) in old.iter().zip(new.iter()).enumerate() {
            update = update.set(i, o.clone(), n.clone()).unwrap();
        }
        builder = builder.update(update);
    }

    builder
}

/// Add update operations to the changeset builder
fn add_updates_to_changeset(
    builder: ChangeSet<TableSchema<&'static str>, &'static str, &'static [u8]>,
    users: &TableSchema<&'static str>,
    posts: &TableSchema<&'static str>,
) -> ChangeSet<TableSchema<&'static str>, &'static str, &'static [u8]> {
    let builder = add_user_updates_to_changeset(builder, users);
    add_post_updates_to_changeset(builder, posts)
}

/// Add delete operations to the changeset builder
fn add_deletes_to_changeset(
    mut builder: ChangeSet<TableSchema<&'static str>, &'static str, &'static [u8]>,
    users: &TableSchema<&'static str>,
    comments: &TableSchema<&'static str>,
    post_tags: &TableSchema<&'static str>,
) -> ChangeSet<TableSchema<&'static str>, &'static str, &'static [u8]> {
    // Delete comment
    let mut delete = ChangeDelete::from(comments.clone());
    for (i, val) in [
        5i64.into(),
        1i64.into(),
        5i64.into(),
        "Reply to comment 1".into(),
        1001400i64.into(),
        Value::Null,
        0i64.into(),
    ]
    .iter()
    .enumerate()
    {
        delete = delete.set(i, val.clone()).unwrap();
    }
    builder = builder.delete(delete);

    // Delete post-tag
    let delete_post_tags: [Value<&'static str, &'static [u8]>; 2] = [3i64.into(), 5i64.into()];
    let mut delete = ChangeDelete::from(post_tags.clone());
    for (i, val) in delete_post_tags.iter().enumerate() {
        delete = delete.set(i, val.clone()).unwrap();
    }
    builder = builder.delete(delete);

    // Delete user
    let mut delete = ChangeDelete::from(users.clone());
    for (i, val) in [
        4i64.into(),
        "diana".into(),
        "diana@example.com".into(),
        1000300i64.into(),
        Value::Null,
        0i64.into(),
        Value::Null,
    ]
    .iter()
    .enumerate()
    {
        delete = delete.set(i, val.clone()).unwrap();
    }
    builder = builder.delete(delete);

    builder
}

/// Generate changeset using builder API (programmatic construction)
#[allow(clippy::too_many_lines)]
fn builder_changeset() -> Vec<u8> {
    let (users, posts, comments, tags, post_tags) = black_box(create_schemas());

    let mut builder = ChangeSet::new();

    // Add all operations
    builder = add_inserts_to_changeset(builder, &users, &posts, &comments, &tags, &post_tags);
    builder = add_updates_to_changeset(builder, &users, &posts);
    builder = add_deletes_to_changeset(builder, &users, &comments, &post_tags);

    builder.build()
}

/// Add insert operations for all tables to the patchset builder
fn add_inserts_to_patchset(
    mut builder: PatchSet<TableSchema<String>, String, Vec<u8>>,
    users: &TableSchema<String>,
    posts: &TableSchema<String>,
    comments: &TableSchema<String>,
    tags: &TableSchema<String>,
    post_tags: &TableSchema<String>,
) -> PatchSet<TableSchema<String>, String, Vec<u8>> {
    // User inserts
    for row in get_user_rows() {
        let mut insert = Insert::from(users.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post inserts
    for row in get_post_rows() {
        let mut insert = Insert::from(posts.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Tag inserts
    for row in get_tag_rows() {
        let mut insert = Insert::from(tags.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post-tag inserts
    for row in get_post_tag_rows() {
        let mut insert = Insert::from(post_tags.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Comment inserts
    for row in get_comment_rows() {
        let mut insert = Insert::from(comments.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    builder
}

/// Add update operations to the patchset builder
fn add_updates_to_patchset(
    mut builder: PatchSet<TableSchema<String>, String, Vec<u8>>,
    users: &TableSchema<String>,
    posts: &TableSchema<String>,
) -> PatchSet<TableSchema<String>, String, Vec<u8>> {
    // User updates: [(col_idx, new_value), ...] - only PK and changed columns
    // UPDATE users SET last_login = 1002000 WHERE id = 1
    // UPDATE users SET last_login = 1002100 WHERE id = 2
    let user_updates: &[&[(usize, Value<String, Vec<u8>>)]] = &[
        &[(0, 1i64.into()), (4, 1002000i64.into())],
        &[(0, 2i64.into()), (4, 1002100i64.into())],
    ];
    for cols in user_updates {
        let mut update = PatchUpdate::from(users.clone());
        for (i, val) in *cols {
            update = update.set(*i, val.clone()).unwrap();
        }
        builder = builder.update(update);
    }

    // UPDATE posts SET view_count = 10 WHERE id = 1
    // UPDATE posts SET view_count = 5 WHERE id = 2
    // UPDATE posts SET updated_at = 1002200, content = 'Updated content' WHERE id = 2
    let post_updates: &[&[(usize, Value<String, Vec<u8>>)]] = &[
        &[(0, 1i64.into()), (6, 10i64.into())],
        &[(0, 2i64.into()), (6, 5i64.into())],
        &[
            (0, 2i64.into()),
            (3, "Updated content"),
            (5, 1002200i64.into()),
        ],
    ];
    for cols in post_updates {
        let mut update = PatchUpdate::from(posts.clone());
        for (i, val) in *cols {
            update = update.set(*i, val.clone()).unwrap();
        }
        builder = builder.update(update);
    }

    builder
}

/// Add delete operations to the patchset builder
fn add_deletes_to_patchset(
    mut builder: PatchSet<TableSchema<String>, String, Vec<u8>>,
    users: &TableSchema<String>,
    comments: &TableSchema<String>,
    post_tags: &TableSchema<String>,
) -> PatchSet<TableSchema<String>, String, Vec<u8>> {
    // Deletes for patchset (only need PK values)
    builder = builder.delete(comments, &[5i64.into()]);
    builder = builder.delete(post_tags, &[3i64.into(), 5i64.into()]);
    builder = builder.delete(users, &[4i64.into()]);

    builder
}

/// Generate patchset using builder API (programmatic construction)
#[allow(clippy::too_many_lines)]
fn builder_patchset() -> Vec<u8> {
    let (users, posts, comments, tags, post_tags) = black_box(create_schemas());

    let mut builder = PatchSet::new();

    // Add all operations
    builder = add_inserts_to_patchset(builder, &users, &posts, &comments, &tags, &post_tags);
    builder = add_updates_to_patchset(builder, &users, &posts);
    builder = add_deletes_to_patchset(builder, &users, &comments, &post_tags);

    builder.build()
}

/// Generate changeset using SQL parser
fn parser_changeset_with(schema: &str, operations: &[&str]) -> Vec<u8> {
    // Combine schema and operations into one SQL string
    let mut sql = String::from(schema);
    for op in operations {
        sql.push('\n');
        sql.push_str(op);
        sql.push(';');
    }

    let builder = ChangeSet::try_from(sql.as_str()).unwrap();
    builder.build()
}

/// Generate patchset using SQL parser
fn parser_patchset_with(schema: &str, operations: &[&str]) -> Vec<u8> {
    // Combine schema and operations into one SQL string
    let mut sql = String::from(schema);
    for op in operations {
        sql.push('\n');
        sql.push_str(op);
        sql.push(';');
    }

    let builder = PatchSet::try_from(sql.as_str()).unwrap();
    builder.build()
}

fn benchmark_changeset(c: &mut Criterion) {
    let mut group = c.benchmark_group("changeset_generation");

    group.bench_function("rusqlite", |b| {
        b.iter(|| {
            // black_box the schema and operations to prevent compile-time optimization
            let schema = black_box(SCHEMA);
            let ops = black_box(OPERATIONS);
            black_box(rusqlite_changeset_with(schema, ops))
        });
    });

    group.bench_function("builder_api", |b| {
        b.iter(|| black_box(builder_changeset()));
    });

    group.bench_function("sql_parser", |b| {
        b.iter(|| {
            let schema = black_box(SCHEMA);
            let ops = black_box(OPERATIONS);
            black_box(parser_changeset_with(schema, ops))
        });
    });

    group.finish();
}

fn benchmark_patchset(c: &mut Criterion) {
    let mut group = c.benchmark_group("patchset_generation");

    group.bench_function("rusqlite", |b| {
        b.iter(|| {
            let schema = black_box(SCHEMA);
            let ops = black_box(OPERATIONS);
            black_box(rusqlite_patchset_with(schema, ops))
        });
    });

    group.bench_function("builder_api", |b| {
        b.iter(|| black_box(builder_patchset()));
    });

    group.bench_function("sql_parser", |b| {
        b.iter(|| {
            let schema = black_box(SCHEMA);
            let ops = black_box(OPERATIONS);
            black_box(parser_patchset_with(schema, ops))
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_changeset, benchmark_patchset);
criterion_main!(benches);
