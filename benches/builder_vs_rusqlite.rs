//! Benchmark comparing changeset/patchset generation methods.
//!
//! Compares three approaches:
//! 1. Using rusqlite's native session API
//! 2. Using our builder API directly (programmatic construction)
//! 3. Using our SQL parser to build from SQL statements

use criterion::{Criterion, criterion_group, criterion_main};
use rusqlite::Connection;
use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangeUpdate, DiffOps, Insert, PatchDelete, PatchSet, PatchUpdate,
    SimpleTable, TableSchema, Value,
};
use std::hint::black_box;
use std::string::String;

/// Type alias for borrowed values (clone-less)
type Val = Value<&'static str, &'static [u8]>;

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

/// Static user data rows for inserts (no allocations)
static USER_ROWS: &[&[Val]] = &[
    &[
        Val::Integer(1),
        Val::Text("alice"),
        Val::Text("alice@example.com"),
        Val::Integer(1000000),
        Val::Null,
        Val::Integer(1),
        Val::Null,
    ],
    &[
        Val::Integer(2),
        Val::Text("bob"),
        Val::Text("bob@example.com"),
        Val::Integer(1000100),
        Val::Null,
        Val::Integer(1),
        Val::Null,
    ],
    &[
        Val::Integer(3),
        Val::Text("charlie"),
        Val::Text("charlie@example.com"),
        Val::Integer(1000200),
        Val::Null,
        Val::Integer(1),
        Val::Null,
    ],
    &[
        Val::Integer(4),
        Val::Text("diana"),
        Val::Text("diana@example.com"),
        Val::Integer(1000300),
        Val::Null,
        Val::Integer(0),
        Val::Null,
    ],
    &[
        Val::Integer(5),
        Val::Text("eve"),
        Val::Text("eve@example.com"),
        Val::Integer(1000400),
        Val::Null,
        Val::Integer(1),
        Val::Null,
    ],
];

/// Static post data rows for inserts (no allocations)
static POST_ROWS: &[&[Val]] = &[
    &[
        Val::Integer(1),
        Val::Integer(1),
        Val::Text("First Post"),
        Val::Text("Hello World!"),
        Val::Integer(1000500),
        Val::Null,
        Val::Integer(0),
        Val::Integer(1),
    ],
    &[
        Val::Integer(2),
        Val::Integer(1),
        Val::Text("Second Post"),
        Val::Text("More content"),
        Val::Integer(1000600),
        Val::Null,
        Val::Integer(0),
        Val::Integer(1),
    ],
    &[
        Val::Integer(3),
        Val::Integer(2),
        Val::Text("Bob's Post"),
        Val::Text("My thoughts"),
        Val::Integer(1000700),
        Val::Null,
        Val::Integer(0),
        Val::Integer(1),
    ],
    &[
        Val::Integer(4),
        Val::Integer(3),
        Val::Text("Draft"),
        Val::Text("Work in progress"),
        Val::Integer(1000800),
        Val::Null,
        Val::Integer(0),
        Val::Integer(0),
    ],
    &[
        Val::Integer(5),
        Val::Integer(5),
        Val::Text("Eve's Post"),
        Val::Text("Latest news"),
        Val::Integer(1000900),
        Val::Null,
        Val::Integer(0),
        Val::Integer(1),
    ],
];

/// Static tag data rows for inserts (no allocations)
static TAG_ROWS: &[&[Val]] = &[
    &[Val::Integer(1), Val::Text("rust")],
    &[Val::Integer(2), Val::Text("database")],
    &[Val::Integer(3), Val::Text("tutorial")],
    &[Val::Integer(4), Val::Text("news")],
    &[Val::Integer(5), Val::Text("discussion")],
];

/// Static post-tag data rows for inserts (no allocations)
static POST_TAG_ROWS: &[&[Val]] = &[
    &[Val::Integer(1), Val::Integer(1)],
    &[Val::Integer(1), Val::Integer(3)],
    &[Val::Integer(2), Val::Integer(1)],
    &[Val::Integer(3), Val::Integer(5)],
    &[Val::Integer(5), Val::Integer(4)],
];

/// Static comment data rows for inserts (no allocations)
static COMMENT_ROWS: &[&[Val]] = &[
    &[
        Val::Integer(1),
        Val::Integer(1),
        Val::Integer(2),
        Val::Text("Great post!"),
        Val::Integer(1001000),
        Val::Null,
        Val::Integer(0),
    ],
    &[
        Val::Integer(2),
        Val::Integer(1),
        Val::Integer(3),
        Val::Text("Thanks for sharing"),
        Val::Integer(1001100),
        Val::Null,
        Val::Integer(0),
    ],
    &[
        Val::Integer(3),
        Val::Integer(2),
        Val::Integer(2),
        Val::Text("Interesting"),
        Val::Integer(1001200),
        Val::Null,
        Val::Integer(0),
    ],
    &[
        Val::Integer(4),
        Val::Integer(3),
        Val::Integer(1),
        Val::Text("Nice work"),
        Val::Integer(1001300),
        Val::Null,
        Val::Integer(0),
    ],
    &[
        Val::Integer(5),
        Val::Integer(1),
        Val::Integer(5),
        Val::Text("Reply to comment 1"),
        Val::Integer(1001400),
        Val::Null,
        Val::Integer(0),
    ],
];

/// Static user update data: (old_row, new_row)
static USER_UPDATES: &[(&[Val], &[Val])] = &[
    (
        &[
            Val::Integer(1),
            Val::Text("alice"),
            Val::Text("alice@example.com"),
            Val::Integer(1000000),
            Val::Null,
            Val::Integer(1),
            Val::Null,
        ],
        &[
            Val::Integer(1),
            Val::Text("alice"),
            Val::Text("alice@example.com"),
            Val::Integer(1000000),
            Val::Integer(1002000),
            Val::Integer(1),
            Val::Null,
        ],
    ),
    (
        &[
            Val::Integer(2),
            Val::Text("bob"),
            Val::Text("bob@example.com"),
            Val::Integer(1000100),
            Val::Null,
            Val::Integer(1),
            Val::Null,
        ],
        &[
            Val::Integer(2),
            Val::Text("bob"),
            Val::Text("bob@example.com"),
            Val::Integer(1000100),
            Val::Integer(1002100),
            Val::Integer(1),
            Val::Null,
        ],
    ),
];

/// Static post update data: (old_row, new_row)
static POST_UPDATES: &[(&[Val], &[Val])] = &[
    (
        &[
            Val::Integer(1),
            Val::Integer(1),
            Val::Text("First Post"),
            Val::Text("Hello World!"),
            Val::Integer(1000500),
            Val::Null,
            Val::Integer(0),
            Val::Integer(1),
        ],
        &[
            Val::Integer(1),
            Val::Integer(1),
            Val::Text("First Post"),
            Val::Text("Hello World!"),
            Val::Integer(1000500),
            Val::Null,
            Val::Integer(10),
            Val::Integer(1),
        ],
    ),
    (
        &[
            Val::Integer(2),
            Val::Integer(1),
            Val::Text("Second Post"),
            Val::Text("More content"),
            Val::Integer(1000600),
            Val::Null,
            Val::Integer(0),
            Val::Integer(1),
        ],
        &[
            Val::Integer(2),
            Val::Integer(1),
            Val::Text("Second Post"),
            Val::Text("More content"),
            Val::Integer(1000600),
            Val::Null,
            Val::Integer(5),
            Val::Integer(1),
        ],
    ),
    (
        &[
            Val::Integer(2),
            Val::Integer(1),
            Val::Text("Second Post"),
            Val::Text("More content"),
            Val::Integer(1000600),
            Val::Null,
            Val::Integer(5),
            Val::Integer(1),
        ],
        &[
            Val::Integer(2),
            Val::Integer(1),
            Val::Text("Second Post"),
            Val::Text("Updated content"),
            Val::Integer(1000600),
            Val::Integer(1002200),
            Val::Integer(5),
            Val::Integer(1),
        ],
    ),
];

/// Static delete rows
static COMMENT_DELETE: &[Val] = &[
    Val::Integer(5),
    Val::Integer(1),
    Val::Integer(5),
    Val::Text("Reply to comment 1"),
    Val::Integer(1001400),
    Val::Null,
    Val::Integer(0),
];
static POST_TAG_DELETE: &[Val] = &[Val::Integer(3), Val::Integer(5)];
static USER_DELETE: &[Val] = &[
    Val::Integer(4),
    Val::Text("diana"),
    Val::Text("diana@example.com"),
    Val::Integer(1000300),
    Val::Null,
    Val::Integer(0),
    Val::Null,
];

/// Static patchset update data: (column_index, new_value)
static PATCH_USER_UPDATES: &[&[(usize, Val)]] = &[
    &[(0, Val::Integer(1)), (4, Val::Integer(1002000))],
    &[(0, Val::Integer(2)), (4, Val::Integer(1002100))],
];

static PATCH_POST_UPDATES: &[&[(usize, Val)]] = &[
    &[(0, Val::Integer(1)), (6, Val::Integer(10))],
    &[(0, Val::Integer(2)), (6, Val::Integer(5))],
    &[
        (0, Val::Integer(2)),
        (3, Val::Text("Updated content")),
        (5, Val::Integer(1002200)),
    ],
];

/// Static patchset delete PK values
static PATCH_COMMENT_DELETE_PK: &[Val] = &[Val::Integer(5)];
static PATCH_POST_TAG_DELETE_PK: &[Val] = &[Val::Integer(3), Val::Integer(5)];
static PATCH_USER_DELETE_PK: &[Val] = &[Val::Integer(4)];

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
fn add_inserts_to_changeset<'a>(
    mut builder: ChangeSet<TableSchema<&'a str>, &'a str, &'a [u8]>,
    users: &TableSchema<&'a str>,
    posts: &TableSchema<&'a str>,
    comments: &TableSchema<&'a str>,
    tags: &TableSchema<&'a str>,
    post_tags: &TableSchema<&'a str>,
) -> ChangeSet<TableSchema<&'a str>, &'a str, &'a [u8]> {
    // User inserts
    for row in USER_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(users.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post inserts
    for row in POST_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(posts.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Tag inserts
    for row in TAG_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(tags.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post-tag inserts
    for row in POST_TAG_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(post_tags.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Comment inserts
    for row in COMMENT_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(comments.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    builder
}

/// Add update operations to the changeset builder
fn add_updates_to_changeset<'a>(
    mut builder: ChangeSet<TableSchema<&'a str>, &'a str, &'a [u8]>,
    users: &TableSchema<&'a str>,
    posts: &TableSchema<&'a str>,
) -> ChangeSet<TableSchema<&'a str>, &'a str, &'a [u8]> {
    // User updates
    for (old, new) in USER_UPDATES {
        let mut update: ChangeUpdate<TableSchema<&'a str>, &'a str, &'a [u8]> =
            ChangeUpdate::from(users.clone());
        for (i, (o, n)) in old.iter().zip(new.iter()).enumerate() {
            update = update.set(i, *o, *n).unwrap();
        }
        builder = builder.update(update);
    }

    // Post updates
    for (old, new) in POST_UPDATES {
        let mut update: ChangeUpdate<TableSchema<&'a str>, &'a str, &'a [u8]> =
            ChangeUpdate::from(posts.clone());
        for (i, (o, n)) in old.iter().zip(new.iter()).enumerate() {
            update = update.set(i, *o, *n).unwrap();
        }
        builder = builder.update(update);
    }

    builder
}

/// Add delete operations to the changeset builder
fn add_deletes_to_changeset<'a>(
    mut builder: ChangeSet<TableSchema<&'a str>, &'a str, &'a [u8]>,
    users: &TableSchema<&'a str>,
    comments: &TableSchema<&'a str>,
    post_tags: &TableSchema<&'a str>,
) -> ChangeSet<TableSchema<&'a str>, &'a str, &'a [u8]> {
    // Delete comment
    let mut delete: ChangeDelete<_, &str, &[u8]> = ChangeDelete::from(comments.clone());
    for (i, val) in COMMENT_DELETE.iter().enumerate() {
        delete = delete.set(i, val.clone()).unwrap();
    }
    builder = builder.delete(delete);

    // Delete post-tag
    let mut delete: ChangeDelete<_, &str, &[u8]> = ChangeDelete::from(post_tags.clone());
    for (i, val) in POST_TAG_DELETE.iter().enumerate() {
        delete = delete.set(i, val.clone()).unwrap();
    }
    builder = builder.delete(delete);

    // Delete user
    let mut delete: ChangeDelete<_, &str, &[u8]> = ChangeDelete::from(users.clone());
    for (i, val) in USER_DELETE.iter().enumerate() {
        delete = delete.set(i, val.clone()).unwrap();
    }
    builder = builder.delete(delete);

    builder
}

/// Generate changeset using builder API (programmatic construction)
fn builder_changeset() -> Vec<u8> {
    let (users, posts, comments, tags, post_tags) = black_box(create_schemas());

    let mut builder: ChangeSet<TableSchema<&str>, &str, &[u8]> = ChangeSet::new();

    // Add all operations
    builder = add_inserts_to_changeset(builder, &users, &posts, &comments, &tags, &post_tags);
    builder = add_updates_to_changeset(builder, &users, &posts);
    builder = add_deletes_to_changeset(builder, &users, &comments, &post_tags);

    builder.build()
}

/// Add insert operations for all tables to the patchset builder
fn add_inserts_to_patchset<'a>(
    mut builder: PatchSet<TableSchema<&'a str>, &'a str, &'a [u8]>,
    users: &TableSchema<&'a str>,
    posts: &TableSchema<&'a str>,
    comments: &TableSchema<&'a str>,
    tags: &TableSchema<&'a str>,
    post_tags: &TableSchema<&'a str>,
) -> PatchSet<TableSchema<&'a str>, &'a str, &'a [u8]> {
    // User inserts
    for row in USER_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(users.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post inserts
    for row in POST_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(posts.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Tag inserts
    for row in TAG_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(tags.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Post-tag inserts
    for row in POST_TAG_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(post_tags.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    // Comment inserts
    for row in COMMENT_ROWS {
        let mut insert: Insert<_, &str, &[u8]> = Insert::from(comments.clone());
        for (i, val) in row.iter().enumerate() {
            insert = insert.set(i, val.clone()).unwrap();
        }
        builder = builder.insert(insert);
    }

    builder
}

/// Add update operations to the patchset builder
fn add_updates_to_patchset<'a>(
    mut builder: PatchSet<TableSchema<&'a str>, &'a str, &'a [u8]>,
    users: &TableSchema<&'a str>,
    posts: &TableSchema<&'a str>,
) -> PatchSet<TableSchema<&'a str>, &'a str, &'a [u8]> {
    // User updates
    for cols in PATCH_USER_UPDATES {
        let mut update: PatchUpdate<_, &str, &[u8]> = PatchUpdate::from(users.clone());
        for (i, val) in *cols {
            update = update.set(*i, val.clone()).unwrap();
        }
        builder = builder.update(update);
    }

    // Post updates
    for cols in PATCH_POST_UPDATES {
        let mut update: PatchUpdate<_, &str, &[u8]> = PatchUpdate::from(posts.clone());
        for (i, val) in *cols {
            update = update.set(*i, val.clone()).unwrap();
        }
        builder = builder.update(update);
    }

    builder
}

/// Add delete operations to the patchset builder
fn add_deletes_to_patchset<'a>(
    mut builder: PatchSet<TableSchema<&'a str>, &'a str, &'a [u8]>,
    users: &TableSchema<&'a str>,
    comments: &TableSchema<&'a str>,
    post_tags: &TableSchema<&'a str>,
) -> PatchSet<TableSchema<&'a str>, &'a str, &'a [u8]> {
    // Deletes for patchset (only need PK values)
    builder = builder.delete(PatchDelete::new(
        comments.clone(),
        PATCH_COMMENT_DELETE_PK.to_vec(),
    ));
    builder = builder.delete(PatchDelete::new(
        post_tags.clone(),
        PATCH_POST_TAG_DELETE_PK.to_vec(),
    ));
    builder = builder.delete(PatchDelete::new(
        users.clone(),
        PATCH_USER_DELETE_PK.to_vec(),
    ));

    builder
}

/// Generate patchset using builder API (programmatic construction)
fn builder_patchset() -> Vec<u8> {
    let (users, posts, comments, tags, post_tags) = black_box(create_schemas());

    let mut builder: PatchSet<TableSchema<&str>, &str, &[u8]> = PatchSet::new();

    // Add all operations
    builder = add_inserts_to_patchset(builder, &users, &posts, &comments, &tags, &post_tags);
    builder = add_updates_to_patchset(builder, &users, &posts);
    builder = add_deletes_to_patchset(builder, &users, &comments, &post_tags);

    builder.build()
}

/// Create SimpleTable schemas for use with the SQL parser.
fn create_simple_table_schemas() -> (
    SimpleTable,
    SimpleTable,
    SimpleTable,
    SimpleTable,
    SimpleTable,
) {
    let users = SimpleTable::new(
        "users",
        &[
            "id",
            "username",
            "email",
            "created_at",
            "last_login",
            "is_active",
            "profile_data",
        ],
        &[0],
    );
    let posts = SimpleTable::new(
        "posts",
        &[
            "id",
            "user_id",
            "title",
            "content",
            "created_at",
            "updated_at",
            "view_count",
            "is_published",
        ],
        &[0],
    );
    let comments = SimpleTable::new(
        "comments",
        &[
            "id",
            "post_id",
            "user_id",
            "content",
            "created_at",
            "parent_id",
            "is_deleted",
        ],
        &[0],
    );
    let tags = SimpleTable::new("tags", &["id", "name"], &[0]);
    let post_tags = SimpleTable::new("post_tags", &["post_id", "tag_id"], &[0, 1]);
    (users, posts, comments, tags, post_tags)
}

/// Generate patchset using SQL parser
fn parser_patchset_with(operations: &[&str]) -> Vec<u8> {
    let (users, posts, comments, tags, post_tags) = create_simple_table_schemas();

    let mut builder = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    builder.add_table(&users);
    builder.add_table(&posts);
    builder.add_table(&comments);
    builder.add_table(&tags);
    builder.add_table(&post_tags);

    // Combine operations into one SQL string
    let mut sql = String::new();
    for op in operations {
        sql.push_str(op);
        sql.push(';');
        sql.push('\n');
    }

    builder.digest_sql(sql.as_str()).unwrap();
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
            let ops = black_box(OPERATIONS);
            black_box(parser_patchset_with(ops))
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_changeset, benchmark_patchset);
criterion_main!(benches);
