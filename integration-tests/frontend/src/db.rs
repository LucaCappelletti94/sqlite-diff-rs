//! Local SQLite database operations via sqlite-wasm-rs FFI.
//!
//! The database is opened once using `sqlite3_open` on the memory VFS (default).
//! All operations (reads, writes, patchset capture) use the raw `sqlite3` handle.

use std::ffi::CString;
use std::sync::Arc;

use crate::session;

/// Handle to the local SQLite database.
///
/// Wraps a raw `*mut sqlite3` pointer obtained from sqlite-wasm-rs.
/// All operations go through this handle.
#[derive(Clone)]
pub struct LocalDb {
    /// Raw SQLite handle. Shared via Arc so the db can be cloned into closures.
    /// The pointer itself is not Send/Sync, but on wasm32-unknown-unknown
    /// there's only one thread so this is safe.
    db_ptr: Arc<DbPtr>,
}

/// Wrapper to make the raw pointer cloneable.
struct DbPtr {
    ptr: *mut sqlite_wasm_rs::sqlite3,
}

// SAFETY: wasm32-unknown-unknown is single-threaded.
unsafe impl Send for DbPtr {}
unsafe impl Sync for DbPtr {}

impl LocalDb {
    /// Initialize the local database: install VFS, open in-memory DB, run DDL.
    pub fn init() -> Self {
        // Open an in-memory database (memory VFS is the default in sqlite-wasm-rs)
        let mut db: *mut sqlite_wasm_rs::sqlite3 = std::ptr::null_mut();
        let db_name = CString::new(":memory:").unwrap();
        let rc = unsafe { sqlite_wasm_rs::sqlite3_open(db_name.as_ptr(), &mut db) };
        assert_eq!(
            rc,
            sqlite_wasm_rs::SQLITE_OK,
            "Failed to open SQLite database: error code {rc}"
        );

        // Run DDL
        let ddl = CString::new(chat_shared::ddl::INIT_DDL).unwrap();
        let rc = unsafe {
            sqlite_wasm_rs::sqlite3_exec(
                db,
                ddl.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(
            rc,
            sqlite_wasm_rs::SQLITE_OK,
            "Failed to run DDL: error code {rc}"
        );

        Self {
            db_ptr: Arc::new(DbPtr { ptr: db }),
        }
    }

    /// Get the raw database pointer.
    fn raw(&self) -> *mut sqlite_wasm_rs::sqlite3 {
        self.db_ptr.ptr
    }

    /// Insert a user into the local DB, capturing the patchset via session extension.
    ///
    /// Returns the binary patchset bytes.
    pub fn insert_user_with_session(&self, id: &[u8], name: &str, created_at: &str) -> Vec<u8> {
        let sql = format!(
            "INSERT INTO users (id, name, created_at) VALUES (X'{}', '{}', '{}')",
            hex_encode(id),
            name.replace('\'', "''"),
            created_at.replace('\'', "''"),
        );

        session::capture_patchset(self.raw(), &sql)
    }

    /// Insert a message into the local DB, capturing the patchset via session extension.
    ///
    /// Returns the binary patchset bytes.
    pub fn insert_message_with_session(
        &self,
        id: &[u8],
        sender_id: &[u8],
        receiver_id: &[u8],
        body: &str,
        created_at: &str,
    ) -> Vec<u8> {
        let sql = format!(
            "INSERT INTO messages (id, sender_id, receiver_id, body, created_at) VALUES (X'{}', X'{}', X'{}', '{}', '{}')",
            hex_encode(id),
            hex_encode(sender_id),
            hex_encode(receiver_id),
            body.replace('\'', "''"),
            created_at.replace('\'', "''"),
        );

        session::capture_patchset(self.raw(), &sql)
    }

    /// Apply a binary patchset to the local database.
    pub fn apply_patchset(&self, patchset: &[u8]) {
        if patchset.is_empty() {
            return;
        }
        session::apply_patchset(self.raw(), patchset);
    }

    /// List all users from the local database.
    ///
    /// Returns `(id_bytes, name)` pairs.
    pub fn list_users(&self) -> Vec<(Vec<u8>, String)> {
        query_rows(
            self.raw(),
            "SELECT id, name FROM users ORDER BY created_at",
            |stmt| {
                let id = column_blob(stmt, 0);
                let name = column_text(stmt, 1);
                (id, name)
            },
        )
    }

    /// List messages between two users.
    ///
    /// Returns `(sender_name, body, created_at)` tuples.
    pub fn list_messages(&self, user_a: &[u8], user_b: &[u8]) -> Vec<(String, String, String)> {
        let sql = format!(
            "SELECT u.name, m.body, m.created_at \
             FROM messages m JOIN users u ON m.sender_id = u.id \
             WHERE (m.sender_id = X'{}' AND m.receiver_id = X'{}') \
                OR (m.sender_id = X'{}' AND m.receiver_id = X'{}') \
             ORDER BY m.created_at",
            hex_encode(user_a),
            hex_encode(user_b),
            hex_encode(user_b),
            hex_encode(user_a),
        );

        query_rows(self.raw(), &sql, |stmt| {
            let sender_name = column_text(stmt, 0);
            let body = column_text(stmt, 1);
            let created_at = column_text(stmt, 2);
            (sender_name, body, created_at)
        })
    }
}

/// Execute a query and collect rows using a mapper function.
fn query_rows<T>(
    db: *mut sqlite_wasm_rs::sqlite3,
    sql: &str,
    mapper: impl Fn(*mut sqlite_wasm_rs::sqlite3_stmt) -> T,
) -> Vec<T> {
    use sqlite_wasm_rs::*;

    let sql_c = CString::new(sql).unwrap();
    let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();

    let rc = unsafe { sqlite3_prepare_v2(db, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    if rc != SQLITE_OK {
        return Vec::new();
    }

    let mut rows = Vec::new();
    loop {
        let rc = unsafe { sqlite3_step(stmt) };
        if rc == SQLITE_ROW {
            rows.push(mapper(stmt));
        } else {
            break;
        }
    }

    unsafe { sqlite3_finalize(stmt) };
    rows
}

/// Read a BLOB column from a prepared statement.
fn column_blob(stmt: *mut sqlite_wasm_rs::sqlite3_stmt, col: i32) -> Vec<u8> {
    unsafe {
        let ptr = sqlite_wasm_rs::sqlite3_column_blob(stmt, col) as *const u8;
        let len = sqlite_wasm_rs::sqlite3_column_bytes(stmt, col) as usize;
        if ptr.is_null() || len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(ptr, len).to_vec()
        }
    }
}

/// Read a TEXT column from a prepared statement.
fn column_text(stmt: *mut sqlite_wasm_rs::sqlite3_stmt, col: i32) -> String {
    unsafe {
        let ptr = sqlite_wasm_rs::sqlite3_column_text(stmt, col);
        if ptr.is_null() {
            String::new()
        } else {
            let cstr = std::ffi::CStr::from_ptr(ptr as *const i8);
            cstr.to_string_lossy().into_owned()
        }
    }
}

/// Encode bytes as a hex string (for SQL literals).
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}
