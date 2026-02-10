//! Browser-side integration tests using wasm-bindgen-test.
//!
//! These tests verify that the SQLite session extension FFI works correctly
//! in a wasm32 environment via sqlite-wasm-rs.

#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

use std::ffi::CString;
use std::os::raw::{c_int, c_void};

use sqlite_wasm_rs::*;

/// Helper: open an in-memory SQLite database via sqlite-wasm-rs FFI.
/// Memory VFS is the default in sqlite-wasm-rs — no init required.
fn open_memory_db() -> *mut sqlite3 {
    let mut db: *mut sqlite3 = std::ptr::null_mut();
    let name = CString::new(":memory:").unwrap();
    let rc = unsafe { sqlite3_open(name.as_ptr(), &mut db) };
    assert_eq!(rc, SQLITE_OK, "sqlite3_open failed: {rc}");
    db
}

/// Helper: execute a SQL statement on a raw db pointer.
fn exec_sql(db: *mut sqlite3, sql: &str) {
    let sql_c = CString::new(sql).unwrap();
    let rc = unsafe {
        sqlite3_exec(
            db,
            sql_c.as_ptr(),
            None,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, SQLITE_OK, "sqlite3_exec failed for: {sql}");
}

/// Helper: count rows returned by a query.
fn count_rows(db: *mut sqlite3, sql: &str) -> i32 {
    let sql_c = CString::new(sql).unwrap();
    let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
    let rc = unsafe { sqlite3_prepare_v2(db, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    assert_eq!(rc, SQLITE_OK);

    let mut count = 0;
    loop {
        let rc = unsafe { sqlite3_step(stmt) };
        if rc == SQLITE_ROW {
            count += 1;
        } else {
            break;
        }
    }
    unsafe { sqlite3_finalize(stmt) };
    count
}

/// Helper: read a TEXT column from the first row of a query.
fn query_text(db: *mut sqlite3, sql: &str, col: i32) -> String {
    let sql_c = CString::new(sql).unwrap();
    let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
    let rc = unsafe { sqlite3_prepare_v2(db, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    assert_eq!(rc, SQLITE_OK);

    let text = if unsafe { sqlite3_step(stmt) } == SQLITE_ROW {
        let ptr = unsafe { sqlite3_column_text(stmt, col) };
        if ptr.is_null() {
            String::new()
        } else {
            unsafe { std::ffi::CStr::from_ptr(ptr as *const i8) }
                .to_string_lossy()
                .into_owned()
        }
    } else {
        String::new()
    };

    unsafe { sqlite3_finalize(stmt) };
    text
}

/// Default conflict handler for changeset_apply: omit all conflicts.
unsafe extern "C" fn omit_conflicts(
    _ctx: *mut c_void,
    _conflict: c_int,
    _iter: *mut sqlite3_changeset_iter,
) -> c_int {
    0 // SQLITE_CHANGESET_OMIT
}

/// Test: capture a patchset from an INSERT and apply it to another database.
///
/// This is the core round-trip that the chat application relies on.
#[wasm_bindgen_test]
fn test_session_capture_and_apply() {
    let db1 = open_memory_db();
    exec_sql(db1, chat_shared::ddl::INIT_DDL);

    // Start a session on db1
    let mut session: *mut sqlite3_session = std::ptr::null_mut();
    let main_name = CString::new("main").unwrap();
    let rc = unsafe { sqlite3session_create(db1, main_name.as_ptr(), &mut session) };
    assert_eq!(rc, SQLITE_OK);

    // Attach all tables
    let rc = unsafe { sqlite3session_attach(session, std::ptr::null()) };
    assert_eq!(rc, SQLITE_OK);

    // Insert a user
    let user_id_hex = "0102030405060708090a0b0c0d0e0f10";
    exec_sql(
        db1,
        &format!(
            "INSERT INTO users (id, name, created_at) VALUES (X'{user_id_hex}', 'Alice', '2025-01-01T00:00:00Z')"
        ),
    );

    // Extract patchset
    let mut n_patchset: c_int = 0;
    let mut p_patchset: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { sqlite3session_patchset(session, &mut n_patchset, &mut p_patchset) };
    assert_eq!(rc, SQLITE_OK);
    assert!(n_patchset > 0, "Patchset should not be empty");

    let patchset_bytes = unsafe {
        std::slice::from_raw_parts(p_patchset as *const u8, n_patchset as usize).to_vec()
    };
    unsafe { sqlite3_free(p_patchset) };
    unsafe { sqlite3session_delete(session) };

    // Verify the patchset can be parsed by sqlite-diff-rs
    let parsed = sqlite_diff_rs::ParsedDiffSet::parse(&patchset_bytes);
    assert!(parsed.is_ok(), "ParsedDiffSet::parse should succeed");

    // Open a second database and apply the patchset
    let db2 = open_memory_db();
    exec_sql(db2, chat_shared::ddl::INIT_DDL);

    let rc = unsafe {
        sqlite3changeset_apply(
            db2,
            patchset_bytes.len() as c_int,
            patchset_bytes.as_ptr() as *mut c_void,
            None,
            Some(omit_conflicts),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, SQLITE_OK, "sqlite3changeset_apply should succeed");

    // Verify the user exists in db2
    let count = count_rows(db2, "SELECT * FROM users");
    assert_eq!(count, 1, "db2 should have exactly one user");

    let name = query_text(db2, "SELECT name FROM users", 0);
    assert_eq!(name, "Alice");

    // Clean up
    unsafe {
        sqlite3_close(db1);
        sqlite3_close(db2);
    }
}

/// Test: multiple INSERTs in a single session produce a combined patchset.
#[wasm_bindgen_test]
fn test_multi_insert_session() {
    let db1 = open_memory_db();
    exec_sql(db1, chat_shared::ddl::INIT_DDL);

    let mut session: *mut sqlite3_session = std::ptr::null_mut();
    let main_name = CString::new("main").unwrap();
    let rc = unsafe { sqlite3session_create(db1, main_name.as_ptr(), &mut session) };
    assert_eq!(rc, SQLITE_OK);
    let rc = unsafe { sqlite3session_attach(session, std::ptr::null()) };
    assert_eq!(rc, SQLITE_OK);

    // Insert two users and a message
    let alice_hex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let bob_hex = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let msg_hex = "cccccccccccccccccccccccccccccccc";

    exec_sql(
        db1,
        &format!("INSERT INTO users VALUES (X'{alice_hex}', 'Alice', '2025-01-01T00:00:00Z')"),
    );
    exec_sql(
        db1,
        &format!("INSERT INTO users VALUES (X'{bob_hex}', 'Bob', '2025-01-01T00:00:01Z')"),
    );
    exec_sql(
        db1,
        &format!(
            "INSERT INTO messages VALUES (X'{msg_hex}', X'{alice_hex}', X'{bob_hex}', 'Hello!', '2025-01-01T00:00:02Z')"
        ),
    );

    // Extract patchset
    let mut n_patchset: c_int = 0;
    let mut p_patchset: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { sqlite3session_patchset(session, &mut n_patchset, &mut p_patchset) };
    assert_eq!(rc, SQLITE_OK);

    let patchset_bytes = unsafe {
        std::slice::from_raw_parts(p_patchset as *const u8, n_patchset as usize).to_vec()
    };
    unsafe { sqlite3_free(p_patchset) };
    unsafe { sqlite3session_delete(session) };

    // Parse with sqlite-diff-rs
    let parsed = sqlite_diff_rs::ParsedDiffSet::parse(&patchset_bytes)
        .expect("should parse multi-table patchset");

    // Apply to fresh db
    let db2 = open_memory_db();
    exec_sql(db2, chat_shared::ddl::INIT_DDL);

    let rc = unsafe {
        sqlite3changeset_apply(
            db2,
            patchset_bytes.len() as c_int,
            patchset_bytes.as_ptr() as *mut c_void,
            None,
            Some(omit_conflicts),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, SQLITE_OK);

    assert_eq!(count_rows(db2, "SELECT * FROM users"), 2);
    assert_eq!(count_rows(db2, "SELECT * FROM messages"), 1);

    let msg_body = query_text(db2, "SELECT body FROM messages", 0);
    assert_eq!(msg_body, "Hello!");

    unsafe {
        sqlite3_close(db1);
        sqlite3_close(db2);
    }
}

/// Test: builder-produced patchset can be applied via `sqlite3changeset_apply`.
///
/// This tests the opposite direction: sqlite-diff-rs builds a patchset and
/// the SQLite session extension applies it.
#[wasm_bindgen_test]
fn test_builder_patchset_apply() {
    use sqlite_diff_rs::{DiffOps, Insert, PatchSet, TableSchema, Value};

    let schema = chat_shared::ddl::users_table_schema();

    let user_id = vec![
        0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA,
        0xBB,
    ];

    let insert = Insert::from(schema.clone())
        .set(0, Value::Blob(user_id.clone()))
        .unwrap()
        .set(1, Value::Text("Charlie".into()))
        .unwrap()
        .set(2, Value::Text("2025-06-01T12:00:00Z".into()))
        .unwrap();

    let patchset: PatchSet<TableSchema> = PatchSet::new().insert(insert);
    let bytes: Vec<u8> = patchset.into();

    assert!(!bytes.is_empty(), "Builder patchset should produce bytes");

    // Apply to a fresh SQLite DB
    let db = open_memory_db();
    exec_sql(db, chat_shared::ddl::INIT_DDL);

    let rc = unsafe {
        sqlite3changeset_apply(
            db,
            bytes.len() as c_int,
            bytes.as_ptr() as *mut c_void,
            None,
            Some(omit_conflicts),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(
        rc, SQLITE_OK,
        "Builder-produced patchset should apply cleanly"
    );

    assert_eq!(count_rows(db, "SELECT * FROM users"), 1);
    let name = query_text(db, "SELECT name FROM users", 0);
    assert_eq!(name, "Charlie");

    unsafe { sqlite3_close(db) };
}

/// Test: full round-trip — builder builds a patchset, session captures the same
/// INSERT independently, and both parse identically via `ParsedDiffSet`.
#[wasm_bindgen_test]
fn test_builder_vs_session_roundtrip() {
    use sqlite_diff_rs::{DiffOps, Insert, PatchSet, TableSchema, Value};

    let schema = chat_shared::ddl::users_table_schema();
    let user_id = vec![0x01; 16];
    let name = "RoundTripUser";
    let ts = "2025-01-15T10:30:00Z";

    // Builder-produced patchset
    let builder_insert = Insert::from(schema.clone())
        .set(0, Value::Blob(user_id.clone()))
        .unwrap()
        .set(1, Value::Text(name.into()))
        .unwrap()
        .set(2, Value::Text(ts.into()))
        .unwrap();
    let builder_bytes: Vec<u8> = PatchSet::new().insert(builder_insert).into();

    // Session-captured patchset
    let db = open_memory_db();
    exec_sql(db, chat_shared::ddl::INIT_DDL);

    let mut session: *mut sqlite3_session = std::ptr::null_mut();
    let main_name = CString::new("main").unwrap();
    unsafe {
        sqlite3session_create(db, main_name.as_ptr(), &mut session);
        sqlite3session_attach(session, std::ptr::null());
    }

    let hex_id: String = user_id.iter().map(|b| format!("{b:02x}")).collect();
    exec_sql(
        db,
        &format!("INSERT INTO users VALUES (X'{hex_id}', '{name}', '{ts}')"),
    );

    let mut n: c_int = 0;
    let mut p: *mut c_void = std::ptr::null_mut();
    unsafe {
        sqlite3session_patchset(session, &mut n, &mut p);
    }
    let session_bytes = unsafe { std::slice::from_raw_parts(p as *const u8, n as usize).to_vec() };
    unsafe {
        sqlite3_free(p);
        sqlite3session_delete(session);
    }

    // Both should parse successfully
    let parsed_builder = sqlite_diff_rs::ParsedDiffSet::parse(&builder_bytes)
        .expect("builder patchset should parse");
    let parsed_session = sqlite_diff_rs::ParsedDiffSet::parse(&session_bytes)
        .expect("session patchset should parse");

    // Both should apply to fresh DBs and produce the same result
    let db_from_builder = open_memory_db();
    exec_sql(db_from_builder, chat_shared::ddl::INIT_DDL);
    unsafe {
        sqlite3changeset_apply(
            db_from_builder,
            builder_bytes.len() as c_int,
            builder_bytes.as_ptr() as *mut c_void,
            None,
            Some(omit_conflicts),
            std::ptr::null_mut(),
        );
    }

    let db_from_session = open_memory_db();
    exec_sql(db_from_session, chat_shared::ddl::INIT_DDL);
    unsafe {
        sqlite3changeset_apply(
            db_from_session,
            session_bytes.len() as c_int,
            session_bytes.as_ptr() as *mut c_void,
            None,
            Some(omit_conflicts),
            std::ptr::null_mut(),
        );
    }

    // Both databases should have the same user
    assert_eq!(count_rows(db_from_builder, "SELECT * FROM users"), 1);
    assert_eq!(count_rows(db_from_session, "SELECT * FROM users"), 1);
    assert_eq!(
        query_text(db_from_builder, "SELECT name FROM users", 0),
        query_text(db_from_session, "SELECT name FROM users", 0),
    );

    unsafe {
        sqlite3_close(db);
        sqlite3_close(db_from_builder);
        sqlite3_close(db_from_session);
    }
}
