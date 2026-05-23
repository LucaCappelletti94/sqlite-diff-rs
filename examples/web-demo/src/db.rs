//! Local SQLite database wrapper around diesel + the session extension.
//!
//! Each instance owns an in-memory SQLite connection and a long-lived
//! session that captures every write. Calling [`Db::take_changeset`]
//! drains the current session's bytes and reattaches a fresh session, so
//! the next write starts a new batch.
//!
//! [`Db::apply_changeset`] temporarily disables session capture while
//! applying a peer's bytes, so peer-applied changes are not echoed back
//! through the local session.

use diesel::prelude::*;
use diesel::sql_query;
use diesel_sqlite_session::{
    ApplyError, ConflictAction, ConflictType, Session, SessionError, SqliteSessionExt,
};
use uuid::Uuid;

use crate::schema::{INIT_DDL, messages};

/// A row in the `messages` table.
#[derive(Queryable, Selectable, Clone, Debug, PartialEq, Eq)]
#[diesel(table_name = messages)]
pub struct Message {
    /// 16-byte UUIDv4 primary key.
    pub id: Vec<u8>,
    /// Author display name.
    pub author: String,
    /// Message body.
    pub body: String,
    /// Insert timestamp (milliseconds since the Unix epoch).
    pub created_at: i64,
    /// Last-edit timestamp, or `None` if the message has never been edited.
    pub edited_at: Option<i64>,
}

#[derive(Insertable)]
#[diesel(table_name = messages)]
struct NewMessage<'a> {
    id: &'a [u8],
    author: &'a str,
    body: &'a str,
    created_at: i64,
}

/// In-memory SQLite database with a session attached to the `messages` table.
///
/// Field order matters: `session` is declared before `conn` so that on drop
/// the session is freed before the connection it points into. Reversing the
/// order would dangle the session's raw pointer.
pub struct Db {
    session: Session,
    conn: SqliteConnection,
}

impl Db {
    /// Open a fresh in-memory database, run the DDL, attach a session.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] if establishing the connection, running the DDL,
    /// or attaching the session fails.
    pub fn open() -> Result<Self, DbError> {
        let mut conn = SqliteConnection::establish(":memory:").map_err(DbError::Establish)?;
        sql_query(INIT_DDL)
            .execute(&mut conn)
            .map_err(DbError::Diesel)?;
        let mut session = conn.create_session().map_err(DbError::Session)?;
        session
            .attach_by_name("messages")
            .map_err(DbError::Session)?;
        Ok(Self { conn, session })
    }

    /// Insert a new message authored by `author` with `body`. Returns the
    /// freshly generated 16-byte UUID primary key.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the underlying INSERT fails.
    pub fn insert_message(&mut self, author: &str, body: &str) -> Result<Vec<u8>, DbError> {
        let id = Uuid::new_v4().into_bytes().to_vec();
        let created_at = now_ms();
        diesel::insert_into(messages::table)
            .values(NewMessage {
                id: &id,
                author,
                body,
                created_at,
            })
            .execute(&mut self.conn)
            .map_err(DbError::Diesel)?;
        Ok(id)
    }

    /// Replace the body of an existing message and stamp `edited_at`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the UPDATE fails or matches no rows.
    pub fn edit_message(&mut self, id: &[u8], new_body: &str) -> Result<(), DbError> {
        let edited_at = now_ms();
        diesel::update(messages::table.find(id))
            .set((
                messages::body.eq(new_body),
                messages::edited_at.eq(edited_at),
            ))
            .execute(&mut self.conn)
            .map_err(DbError::Diesel)?;
        Ok(())
    }

    /// Hard-delete a message by primary key.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the DELETE fails.
    pub fn delete_message(&mut self, id: &[u8]) -> Result<(), DbError> {
        diesel::delete(messages::table.find(id))
            .execute(&mut self.conn)
            .map_err(DbError::Diesel)?;
        Ok(())
    }

    /// List all messages ordered by creation time ascending.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the SELECT fails.
    pub fn list_messages(&mut self) -> Result<Vec<Message>, DbError> {
        messages::table
            .order(messages::created_at.asc())
            .select(Message::as_select())
            .load(&mut self.conn)
            .map_err(DbError::Diesel)
    }

    /// Extract the current session's changeset bytes and replace the
    /// session with a fresh one. Subsequent writes start a new batch.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Session`] if changeset extraction or reattachment
    /// fails.
    pub fn take_changeset(&mut self) -> Result<Vec<u8>, DbError> {
        let bytes = self.session.changeset().map_err(DbError::Session)?;
        let mut fresh = self.conn.create_session().map_err(DbError::Session)?;
        fresh.attach_by_name("messages").map_err(DbError::Session)?;
        self.session = fresh;
        Ok(bytes)
    }

    /// Apply a peer's changeset to the local database. Session capture is
    /// disabled for the duration so the applied changes do not echo back
    /// through the next [`Self::take_changeset`].
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Apply`] if SQLite rejects the changeset (typically
    /// a schema mismatch or an aborted conflict).
    pub fn apply_changeset<F>(&mut self, bytes: &[u8], on_conflict: F) -> Result<(), DbError>
    where
        F: Fn(ConflictType) -> ConflictAction,
    {
        self.session.set_enabled(false);
        let result = self
            .conn
            .apply_changeset(bytes, on_conflict)
            .map_err(DbError::Apply);
        self.session.set_enabled(true);
        result
    }
}

/// Errors produced by [`Db`] operations.
#[derive(Debug)]
pub enum DbError {
    /// Establishing the diesel connection failed.
    Establish(diesel::ConnectionError),
    /// A diesel query returned an error.
    Diesel(diesel::result::Error),
    /// A session-extension operation failed.
    Session(SessionError),
    /// Applying a peer changeset failed.
    Apply(ApplyError),
}

impl core::fmt::Display for DbError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Establish(e) => write!(f, "establish: {e}"),
            Self::Diesel(e) => write!(f, "diesel: {e}"),
            Self::Session(e) => write!(f, "session: {e}"),
            Self::Apply(e) => write!(f, "apply: {e}"),
        }
    }
}

impl std::error::Error for DbError {}

fn now_ms() -> i64 {
    #[allow(clippy::cast_possible_truncation)]
    {
        js_sys::Date::now() as i64
    }
}
