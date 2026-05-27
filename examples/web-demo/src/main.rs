//! Serverless peer-to-peer chat demo for sqlite-diff-rs.
//!
//! Each browser runs its own SQLite database via sqlite-wasm-rs. Every
//! event (chat messages, identity announcements, typing indicators)
//! flows through SQLite tables: a write hits a session-attached table,
//! the session extension captures it as raw changeset bytes, and the
//! bytes are gossipped over a WebRTC data channel directly between
//! peers. Incoming changesets are applied through `sqlite3changeset_apply`,
//! which fires the diesel update-hook (PR #4969) so the UI knows
//! exactly which tables need re-querying.
//!
//! The result: identity and presence are not bespoke wire envelopes,
//! they are just rows in `peers` and `typing` that ride the same
//! gossip path as message rows. The diff inspector at the bottom of
//! the page parses every outgoing and incoming byte buffer through
//! sqlite-diff-rs.

mod db;
mod inspector;
mod rtc;
mod schema;
mod signal;
mod wire;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use diesel_sqlite_session::{ConflictAction, ConflictType};
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::{
    FaArrowRight, FaCheck, FaPaperPlane, FaPenToSquare, FaPlug, FaReply, FaRotateRight, FaTrash,
    FaUserPlus, FaXmark,
};
use uuid::Uuid;

use crate::db::{ChangedTable, Db, Message, PeerRow, TypingRow};
use crate::inspector::{Direction, Entry, InspectorPane, parse_entry};
use crate::rtc::{Peer, PeerState};
use crate::signal::{
    Decoded, decode_answer_blob, encode_answer_blob, encode_offer_url, fragment_from_url,
};
use crate::wire::{DedupCache, Kind};

fn main() {
    launch(App);
}

/// Shared, single-threaded handle to the in-memory database.
type SharedDb = Rc<RefCell<Db>>;

/// Per-table dirty counters owned by the diesel update-hook closure.
///
/// The hook fires synchronously on every row write (local diesel call
/// or `sqlite3changeset_apply` of a peer's bytes) and bumps the
/// counter matching the affected table. A `use_effect` watching each
/// signal then re-queries the corresponding view signal on the next
/// tick. Bundling the three signals in one struct (rather than three
/// captured locals) sidesteps the Rust 2021 disjoint-capture rule and
/// lets us assert `Send` for the whole bundle in one place.
struct DirtyCounters {
    messages: Signal<u64>,
    peers: Signal<u64>,
    typing: Signal<u64>,
}

// SAFETY: wasm32 is single-threaded; values never actually cross
// threads, so the marker is decorative. The diesel update-hook API
// inherits its `Send` bound from non-wasm targets where SQLite
// connections can move between threads.
unsafe impl Send for DirtyCounters {}

impl DirtyCounters {
    fn bump(&mut self, table: ChangedTable) {
        let sig = match table {
            ChangedTable::Messages => &mut self.messages,
            ChangedTable::Peers => &mut self.peers,
            ChangedTable::Typing => &mut self.typing,
        };
        let next = *sig.peek() + 1;
        sig.set(next);
    }
}

/// A direct WebRTC neighbor in the gossip mesh. `id` is local-only and
/// identifies the entry to callbacks (e.g. to drop the entry when the
/// data channel closes). The remote peer's identity (display name,
/// `self_id`) is not stored here; it lives in the `peers` table and
/// is rendered from `peer_rows`.
#[derive(Clone)]
pub struct PeerEntry {
    pub id: Uuid,
    pub peer: Peer,
}

/// Typing rows older than this are treated as stale and hidden in
/// the UI. The local peer touches its typing row at most every
/// [`TYPING_THROTTLE_MS`], so this leaves room for ~2 missed refreshes
/// before a peer is considered to have stopped typing.
const TYPING_TTL_MS: i64 = 4000;

/// Minimum interval between consecutive local UPSERTs into the
/// `typing` table while the user keeps pressing keys. Lower values
/// produce more gossip traffic for no UI benefit.
const TYPING_THROTTLE_MS: f64 = 1500.0;

/// How often the local peer refreshes its own `peers` row to advertise
/// that it is still in the room. Each refresh is gossiped through the
/// mesh.
const PRESENCE_HEARTBEAT_MS: u32 = 3000;

/// A peer is rendered in the room list only while its `last_seen` is
/// newer than this. Set to several heartbeats so a couple of dropped or
/// delayed gossip messages do not flicker a peer out. A peer that closes
/// its tab stops heartbeating and ages out within roughly this window.
const PRESENCE_TTL_MS: i64 = 10_000;

#[component]
#[allow(clippy::too_many_lines)] // demo file, single-component-by-design
fn App() -> Element {
    // This peer's session-scoped identity. Stable for the lifetime of
    // the page (a refresh starts a new session with a fresh id).
    let local_self_id: Uuid = use_hook(Uuid::new_v4);
    let local_self_id_bytes: Vec<u8> = local_self_id.as_bytes().to_vec();

    // Per-table dirty counters. The diesel update-hook bumps the
    // matching counter every time a row in that table is written
    // (whether by a local diesel call or by `sqlite3changeset_apply`
    // of a peer's bytes). A `use_effect` watches each counter and
    // re-queries the corresponding view signal on the next tick.
    let messages_dirty: Signal<u64> = use_signal(|| 0);
    let peers_dirty: Signal<u64> = use_signal(|| 0);
    let typing_dirty: Signal<u64> = use_signal(|| 0);

    let db: Signal<SharedDb> = use_signal(|| {
        let mut counters = DirtyCounters {
            messages: messages_dirty,
            peers: peers_dirty,
            typing: typing_dirty,
        };
        let db = Db::open(move |table| counters.bump(table))
            .expect("opening in-memory SQLite must succeed");
        Rc::new(RefCell::new(db))
    });

    // Direct WebRTC neighbors.
    let mut peers: Signal<Vec<PeerEntry>> = use_signal(Vec::new);
    let dedup_cache: Signal<Rc<RefCell<DedupCache>>> =
        use_signal(|| Rc::new(RefCell::new(DedupCache::new())));

    // View signals re-populated from the database when the matching
    // dirty counter ticks.
    let mut messages: Signal<Vec<Message>> = use_signal(Vec::new);
    let mut peer_rows: Signal<Vec<PeerRow>> = use_signal(Vec::new);
    let mut typing_rows: Signal<Vec<TypingRow>> = use_signal(Vec::new);

    // Editor state.
    let mut input = use_signal(String::new);
    let mut editing: Signal<Option<Vec<u8>>> = use_signal(|| None);
    let mut edit_buffer = use_signal(String::new);
    let mut error = use_signal(String::new);

    // Connection state.
    let mut peer_state: Signal<Option<PeerState>> = use_signal(|| None);
    let mut offer_url: Signal<Option<String>> = use_signal(|| None);
    let mut answer_blob: Signal<Option<String>> = use_signal(|| None);
    let mut answer_input = use_signal(String::new);
    let mut incoming_offer: Signal<Option<String>> = use_signal(|| None);

    // Identity. Loaded from localStorage on first render; the user is
    // prompted for a name if the slot is empty.
    let mut display_name = use_signal(load_display_name);
    let name_input = use_signal(|| display_name.read().clone());

    // Diff inspector state. `page_start_ms` is captured at first render
    // so entry timestamps render as page-relative milliseconds.
    let inspector_entries: Signal<Vec<Entry>> = use_signal(Vec::new);
    let page_start_ms = use_hook(js_sys::Date::now);

    // Last time (page-relative ms) the local user touched their
    // typing row. Used to throttle UPSERTs to TYPING_THROTTLE_MS.
    let last_typing_touch: Rc<Cell<f64>> = use_hook(|| Rc::new(Cell::new(0.0)));

    // Re-query the messages list when the messages table changes.
    use_effect({
        let db = db.read().clone();
        move || {
            let _ = messages_dirty.read();
            match db.borrow_mut().list_messages() {
                Ok(rows) => messages.set(rows),
                Err(e) => error.set(format!("list messages: {e}")),
            }
        }
    });

    // Re-query the peers list when the peers table changes.
    use_effect({
        let db = db.read().clone();
        move || {
            let _ = peers_dirty.read();
            match db.borrow_mut().list_peers() {
                Ok(rows) => peer_rows.set(rows),
                Err(e) => error.set(format!("list peers: {e}")),
            }
        }
    });

    // Re-query the typing list when the typing table changes.
    use_effect({
        let db = db.read().clone();
        move || {
            let _ = typing_dirty.read();
            match db.borrow_mut().list_typing() {
                Ok(rows) => typing_rows.set(rows),
                Err(e) => error.set(format!("list typing: {e}")),
            }
        }
    });

    // On first render, see if we landed on a URL with an offer fragment.
    use_effect(move || match fragment_from_url() {
        Ok(Some(Decoded::Offer(sdp))) => incoming_offer.set(Some(sdp)),
        Ok(Some(Decoded::Answer(_))) | Ok(None) => {}
        Err(e) => error.set(format!("fragment: {e}")),
    });

    // Capture the session changeset after a successful local write,
    // log it in the diff inspector, then push it over the wire to
    // every direct neighbor.
    let capture_and_send = {
        let db = db.read().clone();
        let dedup = dedup_cache.read().clone();
        let mut inspector_entries = inspector_entries;
        move || {
            let bytes = match db.borrow_mut().take_changeset() {
                Ok(b) => b,
                Err(e) => {
                    error.set(format!("changeset: {e}"));
                    return;
                }
            };
            if bytes.is_empty() {
                return;
            }
            push_entry(
                &mut inspector_entries,
                &bytes,
                Direction::Out,
                page_start_ms,
            );
            let msg_id = Uuid::new_v4();
            let framed = wire::encode(Kind::Changeset, msg_id, &bytes);
            dedup.borrow_mut().insert(msg_id);
            let neighbors: Vec<Peer> = peers
                .read()
                .iter()
                .filter(|p| p.peer.is_open())
                .map(|p| p.peer.clone())
                .collect();
            for neighbor in &neighbors {
                // Tolerate send failure: the channel may have just
                // started closing in the gap between the is_open
                // filter and now.
                let _ = neighbor.send(&framed);
            }
        }
    };

    // Keep our own row in the `peers` table in sync with the display
    // name signal. Each change re-UPSERTs and emits a gossip changeset
    // so other peers learn (or relearn) our identity.
    use_effect({
        let db = db.read().clone();
        let mut capture = capture_and_send.clone();
        let self_id = local_self_id_bytes.clone();
        move || {
            let name = display_name.read().clone();
            if name.is_empty() {
                return;
            }
            let result = db.borrow_mut().upsert_peer(&self_id, &name);
            match result {
                Ok(()) => capture(),
                Err(e) => error.set(format!("upsert self peer: {e}")),
            }
        }
    });

    // Presence heartbeat. Every PRESENCE_HEARTBEAT_MS we re-UPSERT our
    // own `peers` row (refreshing `last_seen`) and gossip it, so other
    // peers keep seeing us. The same write bumps `peers_dirty` locally,
    // which re-renders the room list on a timer and ages out peers whose
    // own heartbeats have stopped (for example because they closed their
    // tab). A peer that leaves therefore disappears from every list
    // within PRESENCE_TTL_MS, with no explicit "leave" message.
    use_future({
        let db = db.read().clone();
        let capture = capture_and_send.clone();
        let self_id = local_self_id_bytes.clone();
        move || {
            let db = db.clone();
            let mut capture = capture.clone();
            let self_id = self_id.clone();
            async move {
                loop {
                    gloo_timers::future::TimeoutFuture::new(PRESENCE_HEARTBEAT_MS).await;
                    let name = display_name.read().clone();
                    if name.is_empty() {
                        continue;
                    }
                    let result = db.borrow_mut().upsert_peer(&self_id, &name);
                    match result {
                        Ok(()) => capture(),
                        Err(e) => error.set(format!("heartbeat: {e}")),
                    }
                }
            }
        }
    });

    let mut send_message = {
        let db = db.read().clone();
        let mut capture = capture_and_send.clone();
        let self_id = local_self_id_bytes.clone();
        move |_| {
            let body = input.read().trim().to_string();
            if body.is_empty() {
                return;
            }
            let author = display_name.read().clone();
            let result = db.borrow_mut().insert_message(&author, &body);
            match result {
                Ok(_) => {
                    input.set(String::new());
                    // Pressing send also stops the typing indicator.
                    let _ = db.borrow_mut().clear_typing(&self_id);
                    capture();
                }
                Err(e) => error.set(format!("insert: {e}")),
            }
        }
    };

    let mut start_edit = move |msg: Message| {
        edit_buffer.set(msg.body.clone());
        editing.set(Some(msg.id));
    };

    let cancel_edit = move |_| {
        editing.set(None);
        edit_buffer.set(String::new());
    };

    let save_edit = {
        let db = db.read().clone();
        let mut capture = capture_and_send.clone();
        move |_| {
            let Some(id) = editing.read().clone() else {
                return;
            };
            let new_body = edit_buffer.read().trim().to_string();
            if new_body.is_empty() {
                return;
            }
            let result = db.borrow_mut().edit_message(&id, &new_body);
            match result {
                Ok(()) => {
                    editing.set(None);
                    edit_buffer.set(String::new());
                    capture();
                }
                Err(e) => error.set(format!("edit: {e}")),
            }
        }
    };

    let delete_message = {
        let db = db.read().clone();
        let mut capture = capture_and_send.clone();
        move |id: Vec<u8>| {
            let result = db.borrow_mut().delete_message(&id);
            match result {
                Ok(()) => capture(),
                Err(e) => error.set(format!("delete: {e}")),
            }
        }
    };

    // Called on each keystroke in the message input. Throttled to
    // [`TYPING_THROTTLE_MS`] so we do not flood the wire with one
    // typing-update changeset per character.
    let on_input_typed = {
        let db = db.read().clone();
        let mut capture = capture_and_send.clone();
        let last_touch = last_typing_touch.clone();
        let self_id = local_self_id_bytes.clone();
        move || {
            let now = js_sys::Date::now();
            if now - last_touch.get() < TYPING_THROTTLE_MS {
                return;
            }
            last_touch.set(now);
            let result = db.borrow_mut().touch_typing(&self_id);
            match result {
                Ok(()) => capture(),
                Err(e) => error.set(format!("typing: {e}")),
            }
        }
    };

    // Shared `Rc<RefCell<Db>>` handle that the per-handler callback
    // builders can clone cheaply for each new peer.
    let db_for_callbacks = db.read().clone();

    let create_room = {
        let db = db_for_callbacks.clone();
        let dedup = dedup_cache.read().clone();
        move |_| {
            let peer_id = Uuid::new_v4();
            let (on_message, on_state) = build_peer_callbacks(
                db.clone(),
                peers,
                dedup.clone(),
                peer_id,
                inspector_entries,
                peer_state,
                error,
                offer_url,
                answer_blob,
                incoming_offer,
                page_start_ms,
            );
            spawn(async move {
                let p = match Peer::new(on_message, on_state) {
                    Ok(p) => p,
                    Err(e) => {
                        error.set(format!("peer: {e:?}"));
                        return;
                    }
                };
                let sdp = match p.create_offer().await {
                    Ok(s) => s,
                    Err(e) => {
                        error.set(format!("offer: {e:?}"));
                        return;
                    }
                };
                offer_url.set(Some(encode_offer_url(&sdp)));
                peers.with_mut(|v| {
                    v.push(PeerEntry {
                        id: peer_id,
                        peer: p,
                    });
                });
            });
        }
    };

    let join_room = {
        let db = db_for_callbacks.clone();
        let dedup = dedup_cache.read().clone();
        move |_| {
            let Some(offer_sdp) = incoming_offer.read().clone() else {
                error.set("no offer fragment on this URL".into());
                return;
            };
            let peer_id = Uuid::new_v4();
            let (on_message, on_state) = build_peer_callbacks(
                db.clone(),
                peers,
                dedup.clone(),
                peer_id,
                inspector_entries,
                peer_state,
                error,
                offer_url,
                answer_blob,
                incoming_offer,
                page_start_ms,
            );
            spawn(async move {
                let p = match Peer::new(on_message, on_state) {
                    Ok(p) => p,
                    Err(e) => {
                        error.set(format!("peer: {e:?}"));
                        return;
                    }
                };
                let sdp = match p.answer_offer(&offer_sdp).await {
                    Ok(s) => s,
                    Err(e) => {
                        error.set(format!("answer: {e:?}"));
                        return;
                    }
                };
                answer_blob.set(Some(encode_answer_blob(&sdp)));
                peers.with_mut(|v| {
                    v.push(PeerEntry {
                        id: peer_id,
                        peer: p,
                    });
                });
            });
        }
    };

    let connect_with_answer = move |_| {
        let text = answer_input.read().clone();
        spawn(async move {
            let sdp = match decode_answer_blob(&text) {
                Ok(s) => s,
                Err(e) => {
                    error.set(format!("decode: {e}"));
                    return;
                }
            };
            // Clone the most recently added Peer out so we don't
            // hold a Signal borrow across the await point.
            let peer_clone = peers.read().last().map(|e| e.peer.clone());
            let Some(p) = peer_clone else {
                error.set("no local peer yet".into());
                return;
            };
            if let Err(e) = p.accept_answer(&sdp).await {
                error.set(format!("accept: {e:?}"));
            } else {
                // Clear the pending invite slot so the user can start
                // another invite cleanly.
                offer_url.set(None);
                answer_input.set(String::new());
            }
        });
    };

    // Room membership is everyone whose presence heartbeat is recent,
    // plus ourselves unconditionally (we never need a heartbeat to know
    // we are here, and exempting self avoids a flicker if our own
    // heartbeat is briefly late). Peers that stopped heartbeating, e.g.
    // by closing their tab, fall outside the TTL and drop off the list.
    #[allow(clippy::cast_possible_truncation)]
    let now = js_sys::Date::now() as i64;
    let present_peers: Vec<PeerRow> = peer_rows
        .read()
        .iter()
        .filter(|p| p.self_id == local_self_id_bytes || p.last_seen > now - PRESENCE_TTL_MS)
        .cloned()
        .collect();
    let peer_count = present_peers.len();
    let has_offer = offer_url.read().is_some();
    let has_answer = answer_blob.read().is_some();
    let has_incoming_offer = incoming_offer.read().is_some();
    let name_set = !display_name.read().is_empty();

    // Persist the typed name to localStorage and unlock the chat.
    let save_name = move |_| {
        let typed = name_input.read().trim().to_string();
        if typed.is_empty() {
            return;
        }
        store_display_name(&typed);
        display_name.set(typed);
    };

    // Tear down every WebRTC neighbor. The local DB state (messages,
    // peers, typing) is left untouched: refreshing the page is the
    // explicit "start fresh" gesture (it starts a new session with a
    // new local_self_id and a fresh :memory: database).
    let reset_connection = move |_| {
        peers.with_mut(Vec::clear);
        offer_url.set(None);
        answer_blob.set(None);
        answer_input.set(String::new());
        incoming_offer.set(None);
        peer_state.set(None);
    };

    rsx! {
        style { {include_str!("../assets/styles.css")} }
        div { class: "page",
            header { class: "page-header",
                h1 { "sqlite-diff-rs P2P chat demo" }
                p { class: "subtitle",
                    "Two browsers each running their own SQLite, syncing changesets over WebRTC. No backend, no signaling server, no infrastructure. Open the diff inspector at the bottom to see the wire bytes parsed by "
                    a { href: "https://github.com/LucaCappelletti94/sqlite-diff-rs", "sqlite-diff-rs" }
                    "."
                }
            }

            if name_set {
                div { class: "identity-row",
                    span { "Logged in as " strong { "{display_name}" } }
                    span { class: "spacer" }
                    span { "Peers: " strong { "{peer_count}" } }
                    if !peers.read().is_empty() {
                        button {
                            class: "btn",
                            style: "margin-left: 0.5rem;",
                            "aria-label": "Disconnect from all peers",
                            onclick: reset_connection,
                            Icon { width: 14, height: 14, fill: "currentColor", icon: FaRotateRight }
                            "Leave room"
                        }
                    }
                }

                PeerList { peers: present_peers.clone(), local_self_id }

                ConnectionPanel {
                    has_offer,
                    has_answer,
                    has_incoming_offer,
                    offer_url: offer_url.read().clone(),
                    answer_blob: answer_blob.read().clone(),
                    answer_input,
                    on_create_room: create_room,
                    on_join_room: join_room,
                    on_connect_with_answer: connect_with_answer,
                }

                if !error.read().is_empty() {
                    div { class: "banner banner-error",
                        "{error}"
                        button {
                            class: "btn",
                            "aria-label": "Dismiss error",
                            onclick: move |_| error.set(String::new()),
                            Icon { width: 14, height: 14, fill: "currentColor", icon: FaXmark }
                        }
                    }
                }

                ul { class: "messages",
                    for msg in messages.read().iter().cloned() {
                        Row {
                            key: "{hex::encode(&msg.id)}",
                            msg: msg.clone(),
                            is_mine: msg.author == *display_name.read(),
                            is_editing: editing.read().as_deref() == Some(msg.id.as_slice()),
                            edit_buffer,
                            on_start_edit: {
                                let msg = msg.clone();
                                move |_| start_edit(msg.clone())
                            },
                            on_save_edit: save_edit.clone(),
                            on_cancel_edit: cancel_edit,
                            on_delete: {
                                let mut delete_message = delete_message.clone();
                                let id = msg.id.clone();
                                move |_| delete_message(id.clone())
                            },
                        }
                    }
                }

                TypingIndicator {
                    typing_rows,
                    peer_rows,
                    local_self_id_bytes: local_self_id_bytes.clone(),
                }

                form {
                    class: "input-row",
                    onsubmit: move |evt| {
                        evt.prevent_default();
                        send_message(());
                    },
                    input {
                        class: "input-text",
                        placeholder: "Type a message",
                        value: "{input}",
                        oninput: {
                            let mut on_typed = on_input_typed.clone();
                            move |evt: Event<FormData>| {
                                input.set(evt.value());
                                on_typed();
                            }
                        },
                    }
                    button { class: "btn-primary", r#type: "submit",
                        Icon { width: 14, height: 14, fill: "currentColor", icon: FaPaperPlane }
                        "Send"
                    }
                }

                InspectorPane { entries: inspector_entries }
            } else {
                NamePrompt { name_input, on_save: save_name }
            }
        }
    }
}

#[component]
fn PeerList(peers: Vec<PeerRow>, local_self_id: Uuid) -> Element {
    if peers.len() <= 1 {
        return rsx! {
            section { class: "peer-list peer-list-empty",
                "You are alone in this room. Invite a guest below."
            }
        };
    }

    let local_bytes = local_self_id.as_bytes().to_vec();
    let mut entries: Vec<PeerRow> = peers;
    entries.sort_by(|a, b| a.display_name.cmp(&b.display_name));

    rsx! {
        section { class: "peer-list",
            div { class: "peer-list-header", "Peers in this room" }
            ul { class: "peer-list-items",
                for row in entries {
                    li { key: "{hex::encode(&row.self_id)}", class: "peer-item",
                        span { class: "peer-name",
                            "{row.display_name}"
                            if row.self_id == local_bytes {
                                em { class: "peer-self-tag", " (you)" }
                            }
                        }
                        span { class: "peer-id-suffix", "#{short_self_id(&row.self_id)}" }
                    }
                }
            }
        }
    }
}

#[component]
fn TypingIndicator(
    typing_rows: Signal<Vec<TypingRow>>,
    peer_rows: Signal<Vec<PeerRow>>,
    local_self_id_bytes: Vec<u8>,
) -> Element {
    #[allow(clippy::cast_possible_truncation)]
    let now = js_sys::Date::now() as i64;
    let cutoff = now - TYPING_TTL_MS;

    let rows = typing_rows.read();
    let names_by_id: std::collections::HashMap<Vec<u8>, String> = peer_rows
        .read()
        .iter()
        .map(|p| (p.self_id.clone(), p.display_name.clone()))
        .collect();

    let mut active: Vec<String> = rows
        .iter()
        .filter(|t| t.updated_at >= cutoff)
        .filter(|t| t.self_id != local_self_id_bytes)
        .filter_map(|t| names_by_id.get(&t.self_id).cloned())
        .collect();
    active.sort();
    active.dedup();

    if active.is_empty() {
        return rsx! {
            div { class: "typing-indicator typing-indicator-empty" }
        };
    }

    let label = match active.len() {
        1 => format!("{} is typing...", active[0]),
        2 => format!("{} and {} are typing...", active[0], active[1]),
        _ => format!(
            "{} and {} others are typing...",
            active[0],
            active.len() - 1
        ),
    };

    rsx! {
        div { class: "typing-indicator", "{label}" }
    }
}

/// Render the first 8 hex characters of a peer's `self_id` for compact
/// identification next to their display name.
fn short_self_id(bytes: &[u8]) -> String {
    hex::encode(bytes).chars().take(8).collect()
}

#[component]
#[allow(clippy::too_many_arguments)] // event-driven props
fn ConnectionPanel(
    has_offer: bool,
    has_answer: bool,
    has_incoming_offer: bool,
    offer_url: Option<String>,
    answer_blob: Option<String>,
    answer_input: Signal<String>,
    on_create_room: EventHandler<()>,
    on_join_room: EventHandler<()>,
    on_connect_with_answer: EventHandler<()>,
) -> Element {
    rsx! {
        section { style: "border: 1px solid #ddd; padding: 0.75rem; margin-bottom: 1rem; background: #fafafa;",
            if has_incoming_offer && !has_answer {
                p { style: "margin: 0;",
                    "This URL contains an offer. Click below to generate a reply code to send back."
                }
                button {
                    class: "btn-primary",
                    style: "margin-top: 0.5rem;",
                    onclick: move |_| on_join_room.call(()),
                    Icon { width: 14, height: 14, fill: "currentColor", icon: FaReply }
                    "Generate reply code"
                }
            }

            if !has_offer && !has_incoming_offer {
                p { style: "margin: 0;",
                    "Invite a peer to this room: click below to generate an invite URL, then share it. Or open someone else's invite URL in this tab."
                }
                button {
                    class: "btn-primary",
                    style: "margin-top: 0.5rem;",
                    onclick: move |_| on_create_room.call(()),
                    Icon { width: 14, height: 14, fill: "currentColor", icon: FaUserPlus }
                    "Invite a guest"
                }
            }

            if has_offer {
                p { style: "margin-top: 0;", "Share this invite URL with the next peer:" }
                textarea {
                    style: "width: 100%; height: 4em; font-family: monospace; font-size: 0.75rem;",
                    readonly: true,
                    "{offer_url.clone().unwrap_or_default()}"
                }
                p { style: "margin-top: 0.5rem;", "Paste the reply code they send back here:" }
                textarea {
                    style: "width: 100%; height: 4em; font-family: monospace; font-size: 0.75rem;",
                    value: "{answer_input}",
                    oninput: move |evt| answer_input.set(evt.value()),
                }
                button {
                    class: "btn-primary",
                    style: "margin-top: 0.5rem;",
                    onclick: move |_| on_connect_with_answer.call(()),
                    Icon { width: 14, height: 14, fill: "currentColor", icon: FaPlug }
                    "Accept reply"
                }
            }

            if has_answer {
                p { style: "margin-top: 0;", "Send this reply code back to the peer who invited you:" }
                textarea {
                    style: "width: 100%; height: 4em; font-family: monospace; font-size: 0.75rem;",
                    readonly: true,
                    "{answer_blob.clone().unwrap_or_default()}"
                }
            }
        }
    }
}

#[component]
fn Row(
    msg: Message,
    is_mine: bool,
    is_editing: bool,
    edit_buffer: Signal<String>,
    on_start_edit: EventHandler<()>,
    on_save_edit: EventHandler<()>,
    on_cancel_edit: EventHandler<()>,
    on_delete: EventHandler<()>,
) -> Element {
    let row_class = if is_mine { "msg-row mine" } else { "msg-row" };
    rsx! {
        li { class: "{row_class}",
            if is_editing {
                input {
                    class: "input-text",
                    value: "{edit_buffer}",
                    oninput: move |evt| edit_buffer.set(evt.value()),
                }
                button {
                    class: "btn",
                    "aria-label": "Save edit",
                    onclick: move |_| on_save_edit.call(()),
                    Icon { width: 14, height: 14, fill: "currentColor", icon: FaCheck }
                    "Save"
                }
                button {
                    class: "btn",
                    "aria-label": "Cancel edit",
                    onclick: move |_| on_cancel_edit.call(()),
                    Icon { width: 14, height: 14, fill: "currentColor", icon: FaXmark }
                    "Cancel"
                }
            } else {
                div { class: "msg-bubble",
                    div { class: "msg-author", "{msg.author}" }
                    div { class: "msg-body", "{msg.body}" }
                    if msg.edited_at.is_some() {
                        em { class: "msg-edited", " (edited)" }
                    }
                }
                if is_mine {
                    div { class: "msg-actions",
                        button {
                            class: "btn icon-only",
                            "aria-label": "Edit message",
                            onclick: move |_| on_start_edit.call(()),
                            Icon { width: 14, height: 14, fill: "currentColor", icon: FaPenToSquare }
                        }
                        button {
                            class: "btn btn-danger icon-only",
                            "aria-label": "Delete message",
                            onclick: move |_| on_delete.call(()),
                            Icon { width: 14, height: 14, fill: "currentColor", icon: FaTrash }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn NamePrompt(name_input: Signal<String>, on_save: EventHandler<()>) -> Element {
    rsx! {
        section { class: "name-prompt",
            h2 { "What should we call you?" }
            p { "Your name is shown to the other peer next to every message you send. It is stored in localStorage and never sent to a server (there is no server)." }
            form {
                onsubmit: move |evt| {
                    evt.prevent_default();
                    on_save.call(());
                },
                input {
                    class: "input-text",
                    placeholder: "e.g. Alice",
                    value: "{name_input}",
                    oninput: move |evt| name_input.set(evt.value()),
                    autofocus: true,
                }
                button { class: "btn-primary", r#type: "submit",
                    "Continue"
                    Icon { width: 14, height: 14, fill: "currentColor", icon: FaArrowRight }
                }
            }
        }
    }
}

/// Build the pair of callbacks (`on_message`, `on_state`) that a freshly
/// constructed [`Peer`] needs. The message callback applies incoming
/// bytes through diesel-sqlite-session (session capture paused so the
/// peer's changes are not echoed back) and gossip-forwards the original
/// framed bytes verbatim. The state callback sends a one-shot snapshot
/// of every session-attached table to the new neighbor on connect and
/// removes this peer's `PeerEntry` from the shared registry on close.
#[allow(clippy::too_many_arguments)] // demo file, callback construction
fn build_peer_callbacks(
    db: SharedDb,
    mut peers: Signal<Vec<PeerEntry>>,
    dedup: Rc<RefCell<DedupCache>>,
    peer_id: Uuid,
    mut inspector_entries: Signal<Vec<Entry>>,
    mut peer_state: Signal<Option<PeerState>>,
    mut error: Signal<String>,
    mut offer_url: Signal<Option<String>>,
    mut answer_blob: Signal<Option<String>>,
    mut incoming_offer: Signal<Option<String>>,
    page_start_ms: f64,
) -> (
    impl FnMut(Vec<u8>) + 'static,
    impl FnMut(PeerState) + 'static,
) {
    let db_for_msg = db.clone();
    let dedup_for_msg = dedup.clone();
    let on_message = move |framed: Vec<u8>| {
        let frame = match wire::decode(&framed) {
            Ok(f) => f,
            Err(e) => {
                error.set(format!("wire decode: {e}"));
                return;
            }
        };

        // Gossip dedup. If we've seen this msg_id before, drop the
        // frame without applying or forwarding.
        if !dedup_for_msg.borrow_mut().insert(frame.msg_id) {
            return;
        }

        match frame.kind {
            Kind::Changeset => {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let ts = ((js_sys::Date::now() - page_start_ms).max(0.0)) as u32;
                let mut entry = inspector::parse_entry(frame.payload, Direction::In, ts);

                let apply_result = db_for_msg
                    .borrow_mut()
                    .apply_changeset(frame.payload, conflict_policy);
                if let Err(ref e) = apply_result {
                    entry.apply_error = Some(format!("{e}"));
                }
                inspector_entries.with_mut(|v| {
                    v.push(entry);
                    let overflow = v.len().saturating_sub(50);
                    if overflow > 0 {
                        v.drain(..overflow);
                    }
                });

                // Forward the original framed bytes (verbatim, same
                // msg_id) to every neighbor except the one we received
                // it from. Idempotency in the conflict handler covers
                // duplicates that arrive along another path.
                let to_forward: Vec<Peer> = peers
                    .read()
                    .iter()
                    .filter(|p| p.id != peer_id && p.peer.is_open())
                    .map(|p| p.peer.clone())
                    .collect();
                for neighbor in &to_forward {
                    // Tolerate send failure: the channel may have
                    // started closing between is_open and now.
                    let _ = neighbor.send(&framed);
                }
            }
        }
    };
    let on_state = move |s: PeerState| {
        peer_state.set(Some(s));
        match s {
            PeerState::Connected => {
                send_snapshot(
                    peers,
                    &dedup,
                    &db,
                    peer_id,
                    &mut inspector_entries,
                    &mut error,
                    page_start_ms,
                );
                // The pending-invite UI is now stale: whichever role
                // brought us here (offerer or answerer), we are a peer
                // in the mesh and can issue fresh invites symmetrically.
                offer_url.set(None);
                answer_blob.set(None);
                incoming_offer.set(None);
            }
            PeerState::Closed => {
                peers.with_mut(|v| v.retain(|p| p.id != peer_id));
            }
        }
    };
    (on_message, on_state)
}

/// Send a single bundled snapshot of every session-attached table to
/// a freshly-connected neighbor. The receiving side applies it through
/// `sqlite3changeset_apply` (idempotent via the `Replace` conflict
/// policy), which fires the local update hooks and pulls in messages,
/// peer identities, and typing rows in one shot.
#[allow(clippy::too_many_arguments)]
fn send_snapshot(
    peers: Signal<Vec<PeerEntry>>,
    dedup: &Rc<RefCell<DedupCache>>,
    db: &SharedDb,
    peer_id: Uuid,
    inspector_entries: &mut Signal<Vec<Entry>>,
    error: &mut Signal<String>,
    page_start_ms: f64,
) {
    let peer_clone = peers
        .read()
        .iter()
        .find(|p| p.id == peer_id)
        .map(|e| e.peer.clone());
    let Some(peer) = peer_clone else {
        return;
    };

    let snapshot = match db.borrow_mut().snapshot_changeset() {
        Ok(bytes) => bytes,
        Err(e) => {
            error.set(format!("snapshot: {e}"));
            return;
        }
    };
    if snapshot.is_empty() {
        return;
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let ts = ((js_sys::Date::now() - page_start_ms).max(0.0)) as u32;
    let snap_id = Uuid::new_v4();
    let snap_framed = wire::encode(Kind::Changeset, snap_id, &snapshot);
    dedup.borrow_mut().insert(snap_id);
    if let Err(e) = peer.send(&snap_framed) {
        error.set(format!("snapshot send: {e:?}"));
    }
    inspector_entries.with_mut(|v| {
        v.push(inspector::parse_entry(&snapshot, Direction::Out, ts));
        let overflow = v.len().saturating_sub(50);
        if overflow > 0 {
            v.drain(..overflow);
        }
    });
}

/// Parse a byte buffer and append an inspector entry, trimming the log
/// to the most recent 50 entries.
fn push_entry(
    entries: &mut Signal<Vec<Entry>>,
    bytes: &[u8],
    direction: Direction,
    page_start_ms: f64,
) {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let ts = ((js_sys::Date::now() - page_start_ms).max(0.0)) as u32;
    let entry = parse_entry(bytes, direction, ts);
    entries.with_mut(|v| {
        v.push(entry);
        let overflow = v.len().saturating_sub(50);
        if overflow > 0 {
            v.drain(..overflow);
        }
    });
}

/// Conflict resolution policy applied to incoming peer changesets.
///
/// The demo treats the peer's view as authoritative for the rows it
/// touches: data-level conflicts (the row exists locally with different
/// values) replace, foreign-key and constraint failures replace, and a
/// `NotFound` (the peer is updating or deleting a row we never saw) is
/// silently omitted. This is the simplest policy that keeps every peer
/// consistent for INSERT, UPDATE, and DELETE on `messages`, `peers`,
/// and `typing` alike.
fn conflict_policy(conflict: ConflictType) -> ConflictAction {
    match conflict {
        ConflictType::NotFound => ConflictAction::Omit,
        _ => ConflictAction::Replace,
    }
}

/// Key used to persist the display name across page loads.
const DISPLAY_NAME_KEY: &str = "sqlite-diff-rs.web-demo.display-name";

/// Load the persisted display name from `localStorage`. Returns an empty
/// string if no name has been set yet or if `localStorage` is unavailable.
fn load_display_name() -> String {
    web_sys::window()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|s| s.get_item(DISPLAY_NAME_KEY).ok().flatten())
        .unwrap_or_default()
}

/// Persist the display name to `localStorage`. Errors are silently
/// dropped because the demo still works fine without persistence (the
/// user just has to retype on reload).
fn store_display_name(name: &str) {
    if let Some(storage) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
        let _ = storage.set_item(DISPLAY_NAME_KEY, name);
    }
}
