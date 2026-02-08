//! Chat frontend: Yew app with sqlite-wasm-rs local DB and WebSocket patchset protocol.
//!
//! All writes go through SQLite's session extension:
//!   `sqlite3session_create` → `sqlite3session_attach` → write → `sqlite3session_patchset` → send
//!
//! All incoming data is applied via `sqlite3changeset_apply`.

mod db;
mod session;
mod websocket;

use db::LocalDb;
use web_sys::HtmlInputElement;
use yew::prelude::*;

/// Top-level application component.
#[function_component(App)]
fn app() -> Html {
    // Current user (set after login).
    let current_user = use_state(|| None::<(Vec<u8>, String)>);
    // List of all users from local SQLite.
    let users = use_state(Vec::<(Vec<u8>, String)>::new);
    // Messages for the selected conversation.
    let messages = use_state(Vec::<(String, String, String)>::new);
    // Selected recipient user ID.
    let selected_recipient = use_state(|| None::<Vec<u8>>);
    // Mutable refs mirroring state for the WS callback.
    // `UseStateHandle` clones capture the value at clone-time; the WS
    // `on_message` closure therefore sees stale state.  These `RefCell`
    // refs are shared by reference and always hold the latest value.
    let selected_recipient_ref = use_mut_ref(|| None::<Vec<u8>>);
    let current_user_id_ref = use_mut_ref(|| None::<Vec<u8>>);
    // WebSocket sender (set after connection).
    let ws_sender = use_state(|| None::<websocket::WsSender>);
    // Local database handle.
    let local_db = use_state(|| None::<LocalDb>);

    // Login form
    let name_ref = use_node_ref();

    let on_login = {
        let current_user = current_user.clone();
        let name_ref = name_ref.clone();
        let ws_sender = ws_sender.clone();
        let local_db = local_db.clone();
        let users = users.clone();
        let messages = messages.clone();
        let selected_recipient = selected_recipient.clone();
        let selected_recipient_ref = selected_recipient_ref.clone();
        let current_user_id_ref = current_user_id_ref.clone();

        Callback::from(move |e: SubmitEvent| {
            e.prevent_default();

            let input: HtmlInputElement = name_ref.cast().unwrap();
            let name = input.value().trim().to_string();
            if name.is_empty() {
                return;
            }

            let current_user = current_user.clone();
            let ws_sender = ws_sender.clone();
            let local_db = local_db.clone();
            let users = users.clone();
            let messages = messages.clone();
            let selected_recipient_ref = selected_recipient_ref.clone();
            let current_user_id_ref = current_user_id_ref.clone();

            wasm_bindgen_futures::spawn_local(async move {
                // Initialize local SQLite DB
                let db = LocalDb::init();

                // Insert user into local DB, capturing the patchset via session extension
                let user_id = uuid::Uuid::new_v4();
                let now = js_sys::Date::new_0().to_iso_string().as_string().unwrap();

                let patchset = db.insert_user_with_session(user_id.as_bytes(), &name, &now);

                // Connect WebSocket
                let on_patchset = {
                    let db = db.clone();
                    let users = users.clone();
                    let messages = messages.clone();
                    let selected_ref = selected_recipient_ref.clone();
                    let my_id_ref = current_user_id_ref.clone();

                    move |data: Vec<u8>| {
                        // Apply incoming patchset to local SQLite
                        db.apply_patchset(&data);

                        // Refresh UI state from local DB
                        users.set(db.list_users());

                        // Refresh messages if a conversation is open.
                        // Read current values from the mutable refs —
                        // UseStateHandle clones captured at login are stale.
                        let my_id = my_id_ref.borrow().clone();
                        let rid = selected_ref.borrow().clone();
                        if let (Some(my_id), Some(rid)) = (my_id, rid) {
                            messages.set(db.list_messages(&my_id, &rid));
                        }
                    }
                };

                // Derive WebSocket URL from the current page origin so it
                // works with Docker port-mapping, VS Code port-forwarding,
                // and Trunk's built-in proxy (which relays /ws → backend:3000).
                let location = web_sys::window().unwrap().location();
                let host = location.host().unwrap(); // hostname:port
                let protocol = location.protocol().unwrap();
                let ws_scheme = if protocol == "https:" { "wss" } else { "ws" };
                let ws_url = format!("{}://{}/ws", ws_scheme, host);
                let sender = websocket::connect(&ws_url, on_patchset).await;

                // Send the login patchset
                if let Some(ref s) = sender {
                    s.send_binary(&patchset);
                }

                *current_user_id_ref.borrow_mut() = Some(user_id.as_bytes().to_vec());
                current_user.set(Some((user_id.as_bytes().to_vec(), name)));
                ws_sender.set(sender);
                local_db.set(Some(db));
            });
        })
    };

    // Send message handler
    let msg_ref = use_node_ref();
    let on_send_message = {
        let current_user = current_user.clone();
        let selected_recipient = selected_recipient.clone();
        let ws_sender = ws_sender.clone();
        let local_db = local_db.clone();
        let messages = messages.clone();
        let msg_ref = msg_ref.clone();

        Callback::from(move |e: SubmitEvent| {
            e.prevent_default();

            let input: HtmlInputElement = msg_ref.cast().unwrap();
            let body = input.value().trim().to_string();
            if body.is_empty() {
                return;
            }

            let (user_id, _) = match &*current_user {
                Some(u) => u.clone(),
                None => return,
            };
            let recipient_id = match &*selected_recipient {
                Some(r) => r.clone(),
                None => return,
            };

            if let (Some(db), Some(sender)) = ((*local_db).as_ref(), (*ws_sender).as_ref()) {
                let msg_id = uuid::Uuid::new_v4();
                let now = js_sys::Date::new_0().to_iso_string().as_string().unwrap();

                // Insert into local DB, capturing patchset via session extension
                let patchset = db.insert_message_with_session(
                    msg_id.as_bytes(),
                    &user_id,
                    &recipient_id,
                    &body,
                    &now,
                );

                // Send patchset to backend
                sender.send_binary(&patchset);

                // Refresh messages from local DB
                messages.set(db.list_messages(&user_id, &recipient_id));

                // Clear input
                input.set_value("");
            }
        })
    };

    // Select recipient handler
    let on_select_recipient = {
        let selected_recipient = selected_recipient.clone();
        let selected_recipient_ref = selected_recipient_ref.clone();
        let messages = messages.clone();
        let local_db = local_db.clone();
        let current_user = current_user.clone();

        Callback::from(move |user_id: Vec<u8>| {
            selected_recipient.set(Some(user_id.clone()));
            *selected_recipient_ref.borrow_mut() = Some(user_id.clone());
            if let (Some(db), Some((my_id, _))) = ((*local_db).as_ref(), &*current_user) {
                messages.set(db.list_messages(my_id, &user_id));
            }
        })
    };

    // Render
    if current_user.is_none() {
        // Login screen
        html! {
            <div class="flex items-center justify-center min-h-screen">
                <div class="bg-gray-800 rounded-lg shadow-xl p-8 max-w-md w-full">
                    <h1 class="text-3xl font-bold mb-6 text-center text-blue-400">
                        {"sqlite-diff-rs Chat"}
                    </h1>
                    <p class="text-gray-400 mb-6 text-center text-sm">
                        {"Real-time chat powered by SQLite patchsets"}
                    </p>
                    <form onsubmit={on_login}>
                        <input
                            ref={name_ref}
                            type="text"
                            placeholder="Enter your name..."
                            class="w-full px-4 py-3 rounded-lg bg-gray-700 text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 mb-4"
                            autofocus=true
                        />
                        <button
                            type="submit"
                            class="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 rounded-lg transition"
                        >
                            {"Join Chat"}
                        </button>
                    </form>
                </div>
            </div>
        }
    } else {
        let (my_id, my_name) = current_user.as_ref().unwrap().clone();

        // Chat screen
        html! {
            <div class="flex h-screen">
                // Sidebar: user list
                <div class="w-64 bg-gray-800 border-r border-gray-700 flex flex-col">
                    <div class="p-4 border-b border-gray-700">
                        <h2 class="text-lg font-semibold text-blue-400">{"Users"}</h2>
                        <p class="text-sm text-gray-400">{format!("Logged in as {my_name}")}</p>
                    </div>
                    <div class="flex-1 overflow-y-auto">
                        {for users.iter().filter(|(id, _)| *id != my_id).map(|(id, name)| {
                            let _id_clone = id.clone();
                            let is_selected = *selected_recipient == Some(id.clone());
                            let on_click = {
                                let on_select = on_select_recipient.clone();
                                let id = id.clone();
                                Callback::from(move |_: MouseEvent| on_select.emit(id.clone()))
                            };
                            html! {
                                <div
                                    onclick={on_click}
                                    class={classes!(
                                        "p-3", "cursor-pointer", "hover:bg-gray-700", "transition",
                                        is_selected.then(|| "bg-gray-700 border-l-4 border-blue-500")
                                    )}
                                >
                                    <span class="text-white">{name}</span>
                                </div>
                            }
                        })}
                    </div>
                </div>

                // Main chat area
                <div class="flex-1 flex flex-col">
                    // Header
                    <div class="p-4 bg-gray-800 border-b border-gray-700">
                        <h2 class="text-lg font-semibold">
                            {
                                if let Some(ref rid) = *selected_recipient {
                                    users.iter()
                                        .find(|(id, _)| id == rid)
                                        .map(|(_, n)| format!("Chat with {n}"))
                                        .unwrap_or_else(|| "Select a user".into())
                                } else {
                                    "Select a user to start chatting".into()
                                }
                            }
                        </h2>
                    </div>

                    // Messages
                    <div class="flex-1 overflow-y-auto p-4 space-y-3">
                        {for messages.iter().map(|(sender_name, body, time)| {
                            let is_mine = *sender_name == my_name;
                            html! {
                                <div class={classes!(
                                    "flex",
                                    is_mine.then(|| "justify-end").unwrap_or("justify-start")
                                )}>
                                    <div class={classes!(
                                        "max-w-xs", "px-4", "py-2", "rounded-lg",
                                        if is_mine { "bg-blue-600" } else { "bg-gray-700" }
                                    )}>
                                        <p class="text-sm font-semibold text-gray-300">{sender_name}</p>
                                        <p class="text-white">{body}</p>
                                        <p class="text-xs text-gray-400 mt-1">{time}</p>
                                    </div>
                                </div>
                            }
                        })}
                    </div>

                    // Input
                    {
                        if selected_recipient.is_some() {
                            html! {
                                <form
                                    onsubmit={on_send_message}
                                    class="p-4 bg-gray-800 border-t border-gray-700 flex gap-2"
                                >
                                    <input
                                        ref={msg_ref}
                                        type="text"
                                        placeholder="Type a message..."
                                        class="flex-1 px-4 py-2 rounded-lg bg-gray-700 text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                    <button
                                        type="submit"
                                        class="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition"
                                    >
                                        {"Send"}
                                    </button>
                                </form>
                            }
                        } else {
                            html! {}
                        }
                    }
                </div>
            </div>
        }
    }
}

fn main() {
    yew::Renderer::<App>::new().render();
}
