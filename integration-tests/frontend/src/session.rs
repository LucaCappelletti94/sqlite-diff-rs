//! SQLite session extension FFI helpers.
//!
//! Provides two operations:
//! - `capture_patchset`: start a session, execute SQL, extract the patchset bytes.
//! - `apply_patchset`: apply a binary patchset to a database.
//!
//! These call the sqlite-wasm-rs FFI bindings for `sqlite3session_*` and
//! `sqlite3changeset_apply` directly.

use std::ffi::CString;
use std::os::raw::{c_int, c_void};

use sqlite_wasm_rs::*;

/// Capture a patchset by starting a session, executing SQL, and extracting
/// the binary patchset via `sqlite3session_patchset`.
///
/// # Panics
///
/// Panics if any FFI call returns an error.
pub fn capture_patchset(db: *mut sqlite3, sql: &str) -> Vec<u8> {
    unsafe {
        // Create session
        let mut session: *mut sqlite3_session = std::ptr::null_mut();
        let db_name = CString::new("main").unwrap();
        let rc = sqlite3session_create(db, db_name.as_ptr(), &mut session);
        assert_eq!(rc, SQLITE_OK, "sqlite3session_create failed: {rc}");

        // Attach all tables
        let rc = sqlite3session_attach(session, std::ptr::null());
        assert_eq!(rc, SQLITE_OK, "sqlite3session_attach failed: {rc}");

        // Execute the SQL
        let sql_c = CString::new(sql).unwrap();
        let rc = sqlite3_exec(
            db,
            sql_c.as_ptr(),
            None,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        assert_eq!(rc, SQLITE_OK, "sqlite3_exec failed: {rc}");

        // Extract patchset
        let mut n_patchset: c_int = 0;
        let mut p_patchset: *mut c_void = std::ptr::null_mut();
        let rc = sqlite3session_patchset(session, &mut n_patchset, &mut p_patchset);
        assert_eq!(rc, SQLITE_OK, "sqlite3session_patchset failed: {rc}");

        // Copy to Vec
        let bytes = if n_patchset > 0 && !p_patchset.is_null() {
            std::slice::from_raw_parts(p_patchset as *const u8, n_patchset as usize).to_vec()
        } else {
            Vec::new()
        };

        // Free patchset memory
        if !p_patchset.is_null() {
            sqlite3_free(p_patchset);
        }

        // Delete session
        sqlite3session_delete(session);

        bytes
    }
}

/// Apply a binary patchset to the database via `sqlite3changeset_apply`.
///
/// Uses a default conflict handler that returns `SQLITE_CHANGESET_OMIT`
/// for all conflicts.
///
/// # Panics
///
/// Panics if `sqlite3changeset_apply` returns an error.
pub fn apply_patchset(db: *mut sqlite3, patchset: &[u8]) {
    unsafe {
        let rc = sqlite3changeset_apply(
            db,
            patchset.len() as c_int,
            patchset.as_ptr() as *mut c_void,
            None, // xFilter: accept all tables
            Some(conflict_handler),
            std::ptr::null_mut(), // pCtx
        );
        assert_eq!(rc, SQLITE_OK, "sqlite3changeset_apply failed: {rc}");
    }
}

/// Default conflict handler: omit conflicting changes.
unsafe extern "C" fn conflict_handler(
    _p_ctx: *mut c_void,
    _e_conflict: c_int,
    _p: *mut sqlite3_changeset_iter,
) -> c_int {
    // SQLITE_CHANGESET_OMIT = 0
    0
}
