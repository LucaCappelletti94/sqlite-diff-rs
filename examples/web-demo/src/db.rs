//! Local SQLite database with diesel, session-extension capture, and
//! update-hook view invalidation.
//!
//! Three tables are session-attached. Every write (whether by a local
//! diesel call or by `sqlite3changeset_apply` of a peer's bytes) is
//! captured by the session for outbound gossip AND notified through an
//! update-hook so the UI knows to re-query.
//!
//! The session extension gives us BYTES suitable for shipping over the
//! wire. The update hooks (diesel PR #4969) give us per-table
//! notifications without us having to thread "I just wrote table X"
//! calls through every code path.

use std::sync::{Arc, Mutex};

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::{ChangeHookId, SqliteChangeOps};
use diesel_sqlite_session::{
    ApplyError, ConflictAction, ConflictType, Session, SessionError, SqliteSessionExt,
};
use sqlite_diff_rs::DiffOps;
use uuid::Uuid;

use crate::schema::{INIT_DDL, messages, peers, typing};

/// Identifies which session-attached table a change notification is
/// about. Replaces stringly-typed `table_name` dispatch with an
/// exhaustive enum: the caller matches variants the compiler checks,
/// and the table-to-variant mapping lives in [`Db::open`] where it is
/// pinned to the diesel `table!` schema types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangedTable {
    /// The `messages` table changed.
    Messages,
    /// The `peers` table changed.
    Peers,
    /// The `typing` table changed.
    Typing,
}

/// A row in the `messages` table.
#[derive(Queryable, Selectable, Clone, Debug, PartialEq, Eq)]
#[diesel(table_name = messages)]
pub struct Message {
    /// 16-byte UUIDv4 primary key.
    pub id: Vec<u8>,
    /// Author display name (denormalized snapshot of `peers.display_name`
    /// at the moment the message was sent).
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

/// A row in the `peers` table. One per peer who has announced itself on
/// this session. `last_seen` is a presence heartbeat: the owning peer
/// refreshes it periodically (gossiped), and consumers treat a peer as
/// present only while its `last_seen` is recent. A peer that closes its
/// tab stops refreshing and ages out of every room list.
#[derive(Queryable, Selectable, Insertable, AsChangeset, Clone, Debug, PartialEq, Eq)]
#[diesel(table_name = peers)]
pub struct PeerRow {
    /// 16-byte UUIDv4 session identity of the peer.
    pub self_id: Vec<u8>,
    /// Display name the peer announced.
    pub display_name: String,
    /// Last presence-heartbeat timestamp (milliseconds since the Unix epoch).
    pub last_seen: i64,
}

/// A row in the `typing` table. One per peer who is currently typing.
#[derive(Queryable, Selectable, Insertable, AsChangeset, Clone, Debug, PartialEq, Eq)]
#[diesel(table_name = typing)]
pub struct TypingRow {
    /// 16-byte UUIDv4 session identity of the peer.
    pub self_id: Vec<u8>,
    /// Last refresh timestamp in milliseconds since the Unix epoch.
    pub updated_at: i64,
}

/// In-memory SQLite database with a session attached to every
/// session-aware table and a single update hook that dispatches
/// per-table notifications.
///
/// Field order matters: `session` is declared before `conn` so that on
/// drop the session is freed before the connection it points into.
pub struct Db {
    session: Session,
    _hook_ids: [ChangeHookId; 3],
    conn: SqliteConnection,
}

impl Db {
    /// Open a fresh in-memory database, run the DDL, register one typed
    /// change hook per session-attached table, and attach a session
    /// covering every table.
    ///
    /// `on_change` is invoked with the affected [`ChangedTable`] every
    /// time a row is inserted, updated, or deleted (whether by a local
    /// diesel call or by `sqlite3changeset_apply` of a peer's bytes). It
    /// runs inside the SQLite callback context and **must not use the
    /// database connection**; instead it should poke a Signal so a
    /// Dioxus `use_effect` re-queries on the next tick.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] if establishing the connection, running the
    /// DDL, or attaching the session fails.
    pub fn open(on_change: impl FnMut(ChangedTable) + Send + 'static) -> Result<Self, DbError> {
        let mut conn = SqliteConnection::establish(":memory:").map_err(DbError::Establish)?;
        // `batch_execute` runs every statement in the multi-statement
        // DDL. `sql_query().execute()` would only run the first, leaving
        // the `peers` and `typing` tables uncreated.
        conn.batch_execute(INIT_DDL).map_err(DbError::Diesel)?;

        // One logical callback fans out to three typed hooks. Sharing it
        // behind `Arc<Mutex>` keeps diesel's `Send` bound satisfied with
        // no `unsafe`, and the lock is never contended because every
        // hook fires on the single SQLite-stepping thread. Each table is
        // named by its `table!` type, so a schema rename is a compile
        // error rather than a silently dead hook.
        let on_change = Arc::new(Mutex::new(on_change));
        let messages_cb = Arc::clone(&on_change);
        let peers_cb = Arc::clone(&on_change);
        let typing_cb = on_change;
        let hook_ids = [
            conn.on_table_change::<messages::table, _>(SqliteChangeOps::ALL, move |_| {
                (messages_cb.lock().expect("change-hook mutex poisoned"))(ChangedTable::Messages);
            }),
            conn.on_table_change::<peers::table, _>(SqliteChangeOps::ALL, move |_| {
                (peers_cb.lock().expect("change-hook mutex poisoned"))(ChangedTable::Peers);
            }),
            conn.on_table_change::<typing::table, _>(SqliteChangeOps::ALL, move |_| {
                (typing_cb.lock().expect("change-hook mutex poisoned"))(ChangedTable::Typing);
            }),
        ];

        let mut session = conn.create_session().map_err(DbError::Session)?;
        session.attach_all().map_err(DbError::Session)?;

        Ok(Self {
            conn,
            _hook_ids: hook_ids,
            session,
        })
    }

    // -- messages --------------------------------------------------------

    /// Insert a new message authored by `author` with `body`. Returns
    /// the freshly generated 16-byte UUID primary key.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the INSERT fails.
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

    // -- peers -----------------------------------------------------------

    /// Insert or replace a peer's row, stamping `last_seen` with the
    /// current time. Called both for our own identity (on name change
    /// and on every presence heartbeat) and implicitly for remote peers
    /// when their changeset lands.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the UPSERT fails.
    pub fn upsert_peer(&mut self, self_id: &[u8], display_name: &str) -> Result<(), DbError> {
        let row = PeerRow {
            self_id: self_id.to_vec(),
            display_name: display_name.to_string(),
            last_seen: now_ms(),
        };
        diesel::replace_into(peers::table)
            .values(&row)
            .execute(&mut self.conn)
            .map_err(DbError::Diesel)?;
        Ok(())
    }

    /// List every peer the local DB knows about, ordered by display name.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the SELECT fails.
    pub fn list_peers(&mut self) -> Result<Vec<PeerRow>, DbError> {
        peers::table
            .order(peers::display_name.asc())
            .select(PeerRow::as_select())
            .load(&mut self.conn)
            .map_err(DbError::Diesel)
    }

    // -- typing ----------------------------------------------------------

    /// Refresh the local user's typing-heartbeat. Called periodically
    /// while the user is typing into the message input.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the UPSERT fails.
    pub fn touch_typing(&mut self, self_id: &[u8]) -> Result<(), DbError> {
        let row = TypingRow {
            self_id: self_id.to_vec(),
            updated_at: now_ms(),
        };
        diesel::replace_into(typing::table)
            .values(&row)
            .execute(&mut self.conn)
            .map_err(DbError::Diesel)?;
        Ok(())
    }

    /// Remove the local user's typing entry. Called on send or when
    /// the typing-idle timeout elapses.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the DELETE fails.
    pub fn clear_typing(&mut self, self_id: &[u8]) -> Result<(), DbError> {
        diesel::delete(typing::table.find(self_id))
            .execute(&mut self.conn)
            .map_err(DbError::Diesel)?;
        Ok(())
    }

    /// List every typing entry. The caller is expected to filter by
    /// `updated_at > now - TTL` so stale rows do not render.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if the SELECT fails.
    pub fn list_typing(&mut self) -> Result<Vec<TypingRow>, DbError> {
        typing::table
            .select(TypingRow::as_select())
            .load(&mut self.conn)
            .map_err(DbError::Diesel)
    }

    // -- session ---------------------------------------------------------

    /// Extract the current session's changeset bytes and replace the
    /// session with a fresh one. Subsequent writes start a new batch.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Session`] if changeset extraction or
    /// reattachment fails.
    pub fn take_changeset(&mut self) -> Result<Vec<u8>, DbError> {
        let bytes = self.session.changeset().map_err(DbError::Session)?;
        let mut fresh = self.conn.create_session().map_err(DbError::Session)?;
        fresh.attach_all().map_err(DbError::Session)?;
        self.session = fresh;
        Ok(bytes)
    }

    /// Build a single changeset that re-creates every current row of
    /// every session-attached table via INSERTs. Sent to freshly
    /// joined peers so they catch up to the local state. Idempotent
    /// on the receiving side because `apply_changeset` uses
    /// `Replace` on data conflicts.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Diesel`] if any of the SELECTs fails.
    pub fn snapshot_changeset(&mut self) -> Result<Vec<u8>, DbError> {
        let messages_rows = self.list_messages()?;
        let peers_rows = self.list_peers()?;
        let typing_rows = self.list_typing()?;

        let mut builder = sqlite_diff_rs::ChangeSet::<_, String, Vec<u8>>::new();
        let table = crate::schema::messages_table();
        for row in messages_rows {
            let insert = sqlite_diff_rs::Insert::from(table.clone())
                .set(0, row.id)
                .expect("column 0")
                .set(1, row.author)
                .expect("column 1")
                .set(2, row.body)
                .expect("column 2")
                .set(3, row.created_at)
                .expect("column 3")
                .set(4, row.edited_at)
                .expect("column 4");
            builder = builder.insert(insert);
        }
        let table = crate::schema::peers_table();
        for row in peers_rows {
            let insert = sqlite_diff_rs::Insert::from(table.clone())
                .set(0, row.self_id)
                .expect("column 0")
                .set(1, row.display_name)
                .expect("column 1")
                .set(2, row.last_seen)
                .expect("column 2");
            builder = builder.insert(insert);
        }
        let table = crate::schema::typing_table();
        for row in typing_rows {
            let insert = sqlite_diff_rs::Insert::from(table.clone())
                .set(0, row.self_id)
                .expect("column 0")
                .set(1, row.updated_at)
                .expect("column 1");
            builder = builder.insert(insert);
        }
        Ok(builder.into())
    }

    /// Apply a peer's changeset to the local database. Session
    /// capture is disabled for the duration so the applied changes do
    /// not echo back through the next [`Self::take_changeset`]. The
    /// update-hooks WILL still fire for each row written, which is
    /// the whole point: the local UI re-queries from the now-updated
    /// tables.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Apply`] if SQLite rejects the changeset
    /// (typically a schema mismatch or an aborted conflict).
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
