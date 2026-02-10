//! WebSocket handler: receives and sends raw binary patchset frames.
//!
//! Inbound patchsets are parsed with `ParsedDiffSet::parse` to extract the
//! table name and operation. Based on the table:
//!
//! - `users` INSERT → register the client, build catch-up patchset for all
//!   existing users and messages relevant to this user, send back.
//! - `messages` INSERT → store the message, build an outbound patchset,
//!   send to both sender and receiver.

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{info, warn};

use chat_shared::ddl::{messages_table_schema, users_table_schema};
use chat_shared::schema::{messages_columns, users_columns};
use sqlite_diff_rs::{DiffOps, Insert, ParsedDiffSet, PatchSet, TableSchema, Value};

// Type aliases to avoid verbosity
type Val = Value<String, Vec<u8>>;
type Schema = TableSchema<String>;

use crate::state::{AppState, Message as ChatMessage, User};

/// Axum handler for WebSocket upgrade.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Handle a single WebSocket connection.
async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    // Spawn a task that forwards patchsets from the channel to the WebSocket.
    let send_task = tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if ws_sender.send(Message::Binary(data.into())).await.is_err() {
                break;
            }
        }
    });

    // Track which user this connection belongs to (set on first `users` INSERT).
    let mut my_user_id: Option<Vec<u8>> = None;

    // Process inbound binary frames.
    while let Some(Ok(msg)) = ws_receiver.next().await {
        let data = match msg {
            Message::Binary(b) => b.to_vec(),
            Message::Close(_) => break,
            _ => continue,
        };

        if data.is_empty() {
            continue;
        }

        // Parse the patchset.
        let parsed = match ParsedDiffSet::parse(&data) {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to parse inbound patchset: {e}");
                continue;
            }
        };

        // Re-serialize to get the operations we can inspect.
        // We work with the patchset variant since that's what the frontend sends.
        match parsed {
            ParsedDiffSet::Patchset(ref _builder) => {
                // Re-serialize and re-parse to iterate over operations.
                // The builder exposes tables via the serialized binary format.
                // For now, we re-parse the inbound data directly.
                handle_patchset_operations(&data, &state, &tx, &mut my_user_id);
            }
            ParsedDiffSet::Changeset(_) => {
                warn!("Received changeset instead of patchset, ignoring");
            }
        }
    }

    // Clean up: remove client from connected map.
    if let Some(uid) = &my_user_id {
        state.clients.remove(uid);
        info!("Client disconnected: {:?}", uid);
    }

    send_task.abort();
}

/// Inspect a raw patchset's binary data to extract operations and act on them.
///
/// This manually walks the patchset binary format (which we know well since
/// sqlite-diff-rs builds it) to extract table names and column values for
/// INSERT operations.
fn handle_patchset_operations(
    data: &[u8],
    state: &AppState,
    tx: &mpsc::UnboundedSender<Vec<u8>>,
    my_user_id: &mut Option<Vec<u8>>,
) {
    // We re-parse using ParsedDiffSet to get structured data.
    // Since ParsedDiffSet gives us a DiffSetBuilder, we can re-serialize it.
    // But we actually need to extract individual operations from the raw bytes.
    //
    // The simplest approach: parse the binary format manually to extract
    // table name + operation + values. The format is well-defined:
    //
    // Table header: marker(1) | col_count(1) | pk_flags(col_count) | name\0
    // Operation:    opcode(1) | indirect(1)  | values...
    //
    // For patchset INSERTs, all column values follow.

    let mut pos = 0;

    while pos < data.len() {
        // Parse table header
        let marker = data[pos];
        if marker != 0x50 {
            // Not a patchset table marker
            return;
        }
        pos += 1;

        if pos >= data.len() {
            return;
        }
        let col_count = data[pos] as usize;
        pos += 1;

        // Skip PK flags
        if pos + col_count > data.len() {
            return;
        }
        pos += col_count;

        // Read table name (null-terminated)
        let name_start = pos;
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        if pos >= data.len() {
            return;
        }
        let table_name = String::from_utf8_lossy(&data[name_start..pos]).to_string();
        pos += 1; // skip null terminator

        // Read operations until we hit another table header or end of data
        while pos < data.len() {
            let byte = data[pos];
            // If it's a table marker, break to parse new table header
            if byte == 0x54 || byte == 0x50 {
                break;
            }

            let op_code = byte;
            pos += 1; // opcode
            if pos >= data.len() {
                return;
            }
            pos += 1; // indirect flag

            match (table_name.as_str(), op_code) {
                ("users", 0x12) => {
                    // INSERT into users: parse col_count values
                    let values = match parse_n_values(&data[pos..], col_count) {
                        Some((v, len)) => {
                            pos += len;
                            v
                        }
                        None => return,
                    };
                    handle_user_insert(state, tx, my_user_id, &values);
                }
                ("messages", 0x12) => {
                    // INSERT into messages: parse col_count values
                    let values = match parse_n_values(&data[pos..], col_count) {
                        Some((v, len)) => {
                            pos += len;
                            v
                        }
                        None => return,
                    };
                    handle_message_insert(state, &values);
                }
                _ => {
                    // Unknown table or non-INSERT operation: skip values.
                    // For simplicity, we skip by trying to parse values and discarding.
                    // INSERT: col_count values
                    // DELETE (patchset): pk_count values
                    // UPDATE (patchset): col_count values (old PKs) + col_count values (new)
                    // For now, only handle INSERTs.
                    match op_code {
                        0x12 => {
                            // INSERT
                            if let Some((_, len)) = parse_n_values(&data[pos..], col_count) {
                                pos += len;
                            } else {
                                return;
                            }
                        }
                        _ => {
                            // Skip unknown ops by trying to parse values
                            // This is a simplification; a full implementation would
                            // handle DELETE and UPDATE as well.
                            warn!(
                                "Unhandled operation 0x{op_code:02x} on table {table_name}, skipping rest"
                            );
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Handle an INSERT into the `users` table.
fn handle_user_insert(
    state: &AppState,
    tx: &mpsc::UnboundedSender<Vec<u8>>,
    my_user_id: &mut Option<Vec<u8>>,
    values: &[Val],
) {
    if values.len() < 3 {
        warn!("users INSERT with too few columns");
        return;
    }

    let id = match &values[users_columns::ID] {
        Value::Blob(b) => b.clone(),
        _ => {
            warn!("users.id is not a BLOB");
            return;
        }
    };
    let name = match &values[users_columns::NAME] {
        Value::Text(s) => s.clone(),
        _ => {
            warn!("users.name is not TEXT");
            return;
        }
    };
    let created_at = match &values[users_columns::CREATED_AT] {
        Value::Text(s) => s.clone(),
        _ => {
            warn!("users.created_at is not TEXT");
            return;
        }
    };

    let user = User {
        id: id.clone(),
        name: name.clone(),
        created_at: created_at.clone(),
    };

    // Store user
    {
        let mut users = state.users.lock().unwrap();
        // Avoid duplicates
        if !users.iter().any(|u| u.id == id) {
            users.push(user.clone());
        }
    }

    // Register this connection
    *my_user_id = Some(id.clone());
    state.clients.insert(id.clone(), tx.clone());
    info!("User registered: {name}");

    // Build catch-up patchset: all existing users + messages for this user
    let catchup = build_catchup_patchset(state, &id);
    if !catchup.is_empty() {
        let _ = tx.send(catchup);
    }

    // Broadcast the new user to all OTHER connected clients
    let new_user_patchset = build_user_patchset(&user);
    for entry in state.clients.iter() {
        if entry.key() != &id {
            let _ = entry.value().send(new_user_patchset.clone());
        }
    }
}

/// Handle an INSERT into the `messages` table.
fn handle_message_insert(state: &AppState, values: &[Val]) {
    if values.len() < 5 {
        warn!("messages INSERT with too few columns");
        return;
    }

    let id = match &values[messages_columns::ID] {
        Value::Blob(b) => b.clone(),
        _ => return,
    };
    let sender_id = match &values[messages_columns::SENDER_ID] {
        Value::Blob(b) => b.clone(),
        _ => return,
    };
    let receiver_id = match &values[messages_columns::RECEIVER_ID] {
        Value::Blob(b) => b.clone(),
        _ => return,
    };
    let body = match &values[messages_columns::BODY] {
        Value::Text(s) => s.clone(),
        _ => return,
    };
    let created_at = match &values[messages_columns::CREATED_AT] {
        Value::Text(s) => s.clone(),
        _ => return,
    };

    let msg = ChatMessage {
        id: id.clone(),
        sender_id: sender_id.clone(),
        receiver_id: receiver_id.clone(),
        body,
        created_at,
    };

    // Store message
    {
        let mut messages = state.messages.lock().unwrap();
        messages.push(msg.clone());
    }

    // Build patchset for this message
    let patchset_bytes = build_message_patchset(&msg);

    // Send to receiver (if connected)
    if let Some(client_tx) = state.clients.get(&receiver_id) {
        let _ = client_tx.send(patchset_bytes.clone());
    }

    // Send to sender too (if connected and different from receiver)
    if sender_id != receiver_id {
        if let Some(client_tx) = state.clients.get(&sender_id) {
            let _ = client_tx.send(patchset_bytes);
        }
    }
}

/// Build an `Insert` for a user row.
pub fn user_insert(user: &User) -> Insert<Schema, String, Vec<u8>> {
    let schema = users_table_schema();
    Insert::from(schema)
        .set(users_columns::ID, Value::Blob(user.id.clone()))
        .unwrap()
        .set(users_columns::NAME, Value::Text(user.name.clone()))
        .unwrap()
        .set(
            users_columns::CREATED_AT,
            Value::Text(user.created_at.clone()),
        )
        .unwrap()
}

/// Build a patchset containing a single user INSERT.
pub fn build_user_patchset(user: &User) -> Vec<u8> {
    Vec::from(PatchSet::<Schema, String, Vec<u8>>::new().insert(user_insert(user)))
}

/// Build an `Insert` for a message row.
pub fn message_insert(msg: &ChatMessage) -> Insert<Schema, String, Vec<u8>> {
    let schema = messages_table_schema();
    Insert::from(schema)
        .set(messages_columns::ID, Value::Blob(msg.id.clone()))
        .unwrap()
        .set(
            messages_columns::SENDER_ID,
            Value::Blob(msg.sender_id.clone()),
        )
        .unwrap()
        .set(
            messages_columns::RECEIVER_ID,
            Value::Blob(msg.receiver_id.clone()),
        )
        .unwrap()
        .set(messages_columns::BODY, Value::Text(msg.body.clone()))
        .unwrap()
        .set(
            messages_columns::CREATED_AT,
            Value::Text(msg.created_at.clone()),
        )
        .unwrap()
}

/// Build a patchset containing a single message INSERT.
pub fn build_message_patchset(msg: &ChatMessage) -> Vec<u8> {
    Vec::from(PatchSet::<Schema, String, Vec<u8>>::new().insert(message_insert(msg)))
}

/// Build a catch-up patchset with all existing users and messages for a given user.
fn build_catchup_patchset(state: &AppState, user_id: &[u8]) -> Vec<u8> {
    let mut builder: PatchSet<Schema, String, Vec<u8>> = PatchSet::new();

    // Add all users (except the one who just joined — they already have their own row).
    {
        let users = state.users.lock().unwrap();
        for user in users.iter() {
            if user.id == user_id {
                continue;
            }
            builder = builder.insert(user_insert(user));
        }
    }

    // Add all messages where user is sender or receiver.
    {
        let messages = state.messages.lock().unwrap();
        for msg in messages.iter() {
            if msg.sender_id == user_id || msg.receiver_id == user_id {
                builder = builder.insert(message_insert(msg));
            }
        }
    }

    if builder.is_empty() {
        return Vec::new();
    }

    builder.into()
}

/// Parse `n` values from a patchset binary stream, returning the values and
/// number of bytes consumed.
///
/// Value encoding (from sqlite-diff-rs):
/// - 0x00: Undefined (no data)
/// - 0x01: Integer (8 bytes big-endian i64)
/// - 0x02: Float (8 bytes big-endian f64)
/// - 0x03: Text (varint length + UTF-8 bytes)
/// - 0x04: Blob (varint length + raw bytes)
/// - 0x05: Null (no data)
fn parse_n_values(data: &[u8], n: usize) -> Option<(Vec<Val>, usize)> {
    let mut values = Vec::with_capacity(n);
    let mut pos = 0;

    for _ in 0..n {
        if pos >= data.len() {
            return None;
        }
        let type_code = data[pos];
        pos += 1;

        match type_code {
            0x00 => {
                // Undefined
                values.push(Value::Null);
            }
            0x01 => {
                // Integer: 8 bytes big-endian
                if pos + 8 > data.len() {
                    return None;
                }
                let v = i64::from_be_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                values.push(Value::Integer(v));
            }
            0x02 => {
                // Float: 8 bytes big-endian
                if pos + 8 > data.len() {
                    return None;
                }
                let v = f64::from_be_bytes(data[pos..pos + 8].try_into().ok()?);
                pos += 8;
                values.push(Value::Real(v));
            }
            0x03 => {
                // Text: varint length + UTF-8 bytes
                let (len, varint_size) = decode_varint(&data[pos..])?;
                pos += varint_size;
                if pos + len > data.len() {
                    return None;
                }
                let s = String::from_utf8(data[pos..pos + len].to_vec()).ok()?;
                pos += len;
                values.push(Value::Text(s));
            }
            0x04 => {
                // Blob: varint length + raw bytes
                let (len, varint_size) = decode_varint(&data[pos..])?;
                pos += varint_size;
                if pos + len > data.len() {
                    return None;
                }
                let b = data[pos..pos + len].to_vec();
                pos += len;
                values.push(Value::Blob(b));
            }
            0x05 => {
                // Null
                values.push(Value::Null);
            }
            _ => return None,
        }
    }

    Some((values, pos))
}

/// Decode a SQLite-style varint (1–9 bytes, big-endian, high-bit continuation).
///
/// Returns `(value, bytes_consumed)`.
fn decode_varint(data: &[u8]) -> Option<(usize, usize)> {
    if data.is_empty() {
        return None;
    }

    let mut result: u64 = 0;
    for i in 0..9 {
        if i >= data.len() {
            return None;
        }
        if i == 8 {
            // 9th byte: all 8 bits are data
            result = (result << 8) | u64::from(data[i]);
            return Some((result as usize, 9));
        }
        result = (result << 7) | u64::from(data[i] & 0x7F);
        if data[i] & 0x80 == 0 {
            return Some((result as usize, i + 1));
        }
    }
    None
}
