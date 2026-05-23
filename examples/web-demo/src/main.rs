//! Serverless peer-to-peer chat demo for sqlite-diff-rs.
//!
//! Each browser runs its own SQLite database via sqlite-wasm-rs. Writes are
//! captured as session-extension changesets and sent over a WebRTC data
//! channel directly to the other peer, with no backend involved.
//!
//! Each browser holds its own in-memory SQLite, writes go through
//! diesel with the session extension attached, and the captured
//! changeset bytes flow directly to the other peer over a WebRTC data
//! channel. The diff inspector at the bottom of the page parses every
//! outgoing and incoming byte buffer through sqlite-diff-rs.

mod db;
mod inspector;
mod rtc;
mod schema;
mod signal;
mod wire;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use diesel_sqlite_session::{ConflictAction, ConflictType};
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::{
    FaArrowRight, FaCheck, FaPaperPlane, FaPenToSquare, FaPlug, FaReply, FaRotateRight, FaTrash,
    FaUserPlus, FaXmark,
};
use uuid::Uuid;

use crate::db::{Db, Message};
use crate::inspector::{Direction, Entry, InspectorPane, parse_entry};
use crate::rtc::{Peer, PeerState};
use crate::signal::{
    Decoded, decode_answer_blob, encode_answer_blob, encode_offer_url, fragment_from_url,
};
use crate::wire::{DedupCache, HelloPayload, Kind};

fn main() {
    launch(App);
}

/// Shared, single-threaded handle to the in-memory database.
type SharedDb = Rc<RefCell<Db>>;

/// One direct WebRTC neighbor in the gossip mesh. The `id` is local-only
/// and is used to identify the entry when callbacks fire (e.g. to remove
/// the entry on channel close). Identity (display name) of the remote
/// peer is stored in the App-scope `known_members` map keyed by the
/// remote's `self_id`, not on the per-edge `PeerEntry`.
#[derive(Clone)]
pub struct PeerEntry {
    pub id: Uuid,
    pub peer: Peer,
}

/// One member of the room learned via a `Kind::Hello` frame (directly
/// from this peer or gossip-forwarded from someone else). Stored in the
/// App-scope `known_members` map keyed by the originator's `self_id`.
///
/// `hello_bytes` is the verbatim framed envelope. Storing it lets us
/// replay the hello to a freshly-connected neighbor without re-encoding
/// or generating a new `msg_id`.
#[derive(Clone)]
pub struct KnownMember {
    pub name: String,
    pub hello_bytes: Vec<u8>,
}

#[component]
#[allow(clippy::too_many_lines)] // demo file, single-component-by-design
fn App() -> Element {
    // Persistent shared resources.
    let db: Signal<SharedDb> = use_signal(|| {
        Rc::new(RefCell::new(
            Db::open().expect("opening in-memory SQLite must succeed"),
        ))
    });
    let mut peers: Signal<Vec<PeerEntry>> = use_signal(Vec::new);
    let dedup_cache: Signal<Rc<RefCell<DedupCache>>> =
        use_signal(|| Rc::new(RefCell::new(DedupCache::new())));
    // This peer's session-scoped identity. Stable for the lifetime of
    // the page (a refresh starts a new session with a fresh id).
    let local_self_id: Uuid = use_hook(Uuid::new_v4);
    // Every member we know about in the room, keyed by their session
    // self_id. Includes our own entry. Populated by `Kind::Hello`
    // frames (gossiped) and rendered as the peer list.
    let mut known_members: Signal<HashMap<Uuid, KnownMember>> = use_signal(HashMap::new);

    // Chat state.
    let mut messages: Signal<Vec<Message>> = use_signal(Vec::new);
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
    // prompted for one if the slot is empty.
    let mut display_name = use_signal(load_display_name);
    let name_input = use_signal(|| display_name.read().clone());

    // Diff inspector state. `page_start_ms` is captured at first render
    // so entry timestamps render as page-relative milliseconds.
    let inspector_entries: Signal<Vec<Entry>> = use_signal(Vec::new);
    let page_start_ms = use_hook(js_sys::Date::now);

    // Refresh the message list from the local database.
    let refresh_messages = {
        let db = db.read().clone();
        move || match db.borrow_mut().list_messages() {
            Ok(rows) => messages.set(rows),
            Err(e) => error.set(format!("list: {e}")),
        }
    };

    use_effect({
        let mut refresh = refresh_messages.clone();
        move || refresh()
    });

    // On first render, see if we landed on a URL with an offer fragment.
    use_effect(move || match fragment_from_url() {
        Ok(Some(Decoded::Offer(sdp))) => incoming_offer.set(Some(sdp)),
        Ok(Some(Decoded::Answer(_))) | Ok(None) => {}
        Err(e) => error.set(format!("fragment: {e}")),
    });

    // Keep our own entry in the known-members map in sync with the
    // display name signal. The hello_bytes we store here are the
    // verbatim frame we will replay to every freshly-connected peer.
    use_effect({
        let dedup = dedup_cache.read().clone();
        move || {
            let name = display_name.read().clone();
            if name.is_empty() {
                return;
            }
            let payload = HelloPayload {
                self_id: local_self_id,
                name: name.clone(),
            };
            let msg_id = Uuid::new_v4();
            let hello_bytes = wire::encode(Kind::Hello, msg_id, &payload.encode_payload());
            dedup.borrow_mut().insert(msg_id);
            known_members.with_mut(|m| {
                m.insert(local_self_id, KnownMember { name, hello_bytes });
            });
        }
    });

    // Capture the session changeset after a successful local write, log
    // it in the diff inspector, then push it over the wire if a peer is
    // connected.
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
            push_entry(
                &mut inspector_entries,
                &bytes,
                Direction::Out,
                page_start_ms,
            );
            if bytes.is_empty() {
                return;
            }
            // Frame, mark our own message ID as seen so we drop the echo
            // if any peer gossips it back to us, then broadcast to every
            // direct neighbor.
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
                // filter above and now.
                let _ = neighbor.send(&framed);
            }
        }
    };

    let mut send_message = {
        let db = db.read().clone();
        let mut refresh = refresh_messages.clone();
        let mut capture = capture_and_send.clone();
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
                    refresh();
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
        let mut refresh = refresh_messages.clone();
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
                    refresh();
                    capture();
                }
                Err(e) => error.set(format!("edit: {e}")),
            }
        }
    };

    let delete_message = {
        let db = db.read().clone();
        let mut refresh = refresh_messages.clone();
        let mut capture = capture_and_send.clone();
        move |id: Vec<u8>| {
            let result = db.borrow_mut().delete_message(&id);
            match result {
                Ok(()) => {
                    refresh();
                    capture();
                }
                Err(e) => error.set(format!("delete: {e}")),
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
                known_members,
                dedup.clone(),
                peer_id,
                messages,
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
                known_members,
                dedup.clone(),
                peer_id,
                messages,
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

    let peer_count = known_members.read().len();
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

    // Tear down every neighbor connection and reset the invite UI.
    // Dropping each `PeerEntry` value drops its underlying data
    // channel. `known_members` is also cleared; the use_effect that
    // tracks `display_name` will re-seed our own entry on the next
    // render.
    let reset_connection = move |_| {
        peers.with_mut(Vec::clear);
        known_members.with_mut(HashMap::clear);
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
                    if peer_count > 0 {
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

                PeerList { known_members, local_self_id }

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
                        oninput: move |evt| input.set(evt.value()),
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
fn PeerList(known_members: Signal<HashMap<Uuid, KnownMember>>, local_self_id: Uuid) -> Element {
    // Sort by name for stable rendering. The local entry is always
    // labeled, regardless of where it falls in the alphabet.
    let mut entries: Vec<(Uuid, KnownMember)> = known_members
        .read()
        .iter()
        .map(|(id, m)| (*id, m.clone()))
        .collect();
    entries.sort_by(|a, b| a.1.name.cmp(&b.1.name));

    if entries.len() <= 1 {
        // Only ourselves (or no one): collapse to a hint.
        return rsx! {
            section { class: "peer-list peer-list-empty",
                "You are alone in this room. Invite a guest below."
            }
        };
    }

    rsx! {
        section { class: "peer-list",
            div { class: "peer-list-header", "Peers in this room" }
            ul { class: "peer-list-items",
                for (id, member) in entries {
                    li { key: "{id}", class: "peer-item",
                        span { class: "peer-name",
                            "{member.name}"
                            if id == local_self_id {
                                em { class: "peer-self-tag", " (you)" }
                            }
                        }
                        span { class: "peer-id-suffix", "#{short_id(id)}" }
                    }
                }
            }
        }
    }
}

/// Render the first 8 hex characters of a UUID for compact identification
/// in the peer list before the remote `Hello` arrives.
fn short_id(id: Uuid) -> String {
    let s = id.simple().to_string();
    s.chars().take(8).collect()
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
/// peer's changes are not echoed back), refreshes the message list, and
/// pushes a parsed entry into the diff inspector. The state callback
/// removes this peer's `PeerEntry` from the shared registry when the
/// data channel closes.
#[allow(clippy::too_many_arguments)] // demo file, callback construction
fn build_peer_callbacks(
    db: SharedDb,
    mut peers: Signal<Vec<PeerEntry>>,
    mut known_members: Signal<HashMap<Uuid, KnownMember>>,
    dedup: Rc<RefCell<DedupCache>>,
    peer_id: Uuid,
    mut messages: Signal<Vec<Message>>,
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
            Kind::Hello => {
                let payload = match HelloPayload::decode_payload(frame.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        error.set(format!("hello decode: {e}"));
                        return;
                    }
                };
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let ts = ((js_sys::Date::now() - page_start_ms).max(0.0)) as u32;
                let entry = inspector::hello_entry(&payload.name, framed.len(), Direction::In, ts);
                inspector_entries.with_mut(|v| {
                    v.push(entry);
                    let overflow = v.len().saturating_sub(50);
                    if overflow > 0 {
                        v.drain(..overflow);
                    }
                });

                // Content-level dedup keyed by the announcer's self_id.
                // If this identity is already known, we already forwarded
                // it the first time we saw it; drop now.
                let is_new = !known_members.read().contains_key(&payload.self_id);
                if !is_new {
                    return;
                }
                known_members.with_mut(|m| {
                    m.insert(
                        payload.self_id,
                        KnownMember {
                            name: payload.name,
                            hello_bytes: framed.clone(),
                        },
                    );
                });

                // Gossip-forward the original framed bytes verbatim to
                // every direct neighbor except the one that delivered
                // them. Existing neighbors will dedup-reject by wire
                // `msg_id`; only previously-unreached neighbors process.
                let to_forward: Vec<Peer> = peers
                    .read()
                    .iter()
                    .filter(|p| p.id != peer_id && p.peer.is_open())
                    .map(|p| p.peer.clone())
                    .collect();
                for neighbor in &to_forward {
                    // Tolerate send failure: the channel may have just
                    // started closing. The `onclose` callback will reap
                    // the dead entry on the next tick.
                    let _ = neighbor.send(&framed);
                }
            }
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

                if apply_result.is_ok() {
                    match db_for_msg.borrow_mut().list_messages() {
                        Ok(rows) => messages.set(rows),
                        Err(e) => error.set(format!("list after apply: {e}")),
                    }
                }

                // Forward the original framed bytes (verbatim, same
                // msg_id) to every neighbor except the one we received
                // it from. Idempotency in the conflict handler covers
                // any duplicate that arrives along another path.
                let to_forward: Vec<Peer> = peers
                    .read()
                    .iter()
                    .filter(|p| p.id != peer_id && p.peer.is_open())
                    .map(|p| p.peer.clone())
                    .collect();
                for neighbor in &to_forward {
                    // Tolerate send failure for the same race-window
                    // reason as the hello path above.
                    let _ = neighbor.send(&framed);
                }
            }
        }
    };
    let on_state = move |s: PeerState| {
        peer_state.set(Some(s));
        match s {
            PeerState::Connected => {
                send_membership_and_snapshot(
                    peers,
                    known_members,
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

/// Replay every known member's `Hello` envelope on a freshly-connected
/// neighbor's channel, then send the database snapshot. Called from the
/// `on_state` callback when the data channel first opens.
///
/// Replaying the stored `hello_bytes` rather than synthesizing fresh
/// frames means the new neighbor catches up on every identity we know
/// about (direct and indirect) without us having to re-encode anything.
/// Existing direct neighbors would dedup-reject these bytes by `msg_id`
/// if the new neighbor gossipped them onward, so traffic stays bounded.
#[allow(clippy::too_many_arguments)]
fn send_membership_and_snapshot(
    peers: Signal<Vec<PeerEntry>>,
    known_members: Signal<HashMap<Uuid, KnownMember>>,
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

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let ts = ((js_sys::Date::now() - page_start_ms).max(0.0)) as u32;

    // Replay every known member's hello to the new neighbor.
    let members: Vec<(String, Vec<u8>)> = known_members
        .read()
        .values()
        .map(|m| (m.name.clone(), m.hello_bytes.clone()))
        .collect();
    for (name, bytes) in &members {
        // Tolerate failure: this fires from the on-open callback,
        // and in principle the channel could have transitioned to
        // closing between the open event and these sends.
        let _ = peer.send(bytes);
        inspector_entries.with_mut(|v| {
            v.push(inspector::hello_entry(
                name,
                bytes.len(),
                Direction::Out,
                ts,
            ));
            let overflow = v.len().saturating_sub(50);
            if overflow > 0 {
                v.drain(..overflow);
            }
        });
    }

    // Snapshot of the current database state. Empty payload (no rows
    // yet) produces a zero-length changeset; skip sending in that case.
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
/// silently omitted. This is the simplest policy that keeps the two
/// tabs consistent for INSERT, UPDATE, and DELETE.
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

// Module-level type alias documented for downstream callers; reference
// from a no-op fn so dead-code analysis does not flag it.
#[allow(dead_code)]
fn _assert_shared_db_type(_: SharedDb) {}
