//! WebSocket connection to the backend.
//!
//! Uses the browser's native `WebSocket` API via `web-sys`.
//! All frames are binary (raw patchset bytes).

use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use web_sys::{BinaryType, MessageEvent, WebSocket};

/// Sender handle for the WebSocket connection.
#[derive(Clone)]
pub struct WsSender {
    ws: WebSocket,
}

impl WsSender {
    /// Send raw binary data over the WebSocket.
    pub fn send_binary(&self, data: &[u8]) {
        self.ws
            .send_with_u8_array(data)
            .expect("WebSocket send failed");
    }
}

/// Connect to the backend WebSocket and set up message handling.
///
/// `on_patchset` is called with the raw bytes of each incoming binary message.
///
/// Returns `Some(WsSender)` on success, `None` on failure.
pub async fn connect(url: &str, on_patchset: impl Fn(Vec<u8>) + 'static) -> Option<WsSender> {
    let ws = WebSocket::new(url).ok()?;
    ws.set_binary_type(BinaryType::Arraybuffer);

    // On message: extract binary data and call the handler.
    let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
        if let Ok(buf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let array = js_sys::Uint8Array::new(&buf);
            let data = array.to_vec();
            on_patchset(data);
        }
    });
    ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    on_message.forget();

    // On error: log to console.
    // Note: WebSocket `onerror` delivers a plain `Event`, not `ErrorEvent`.
    let on_error = Closure::<dyn FnMut(web_sys::Event)>::new(|e: web_sys::Event| {
        web_sys::console::error_1(&format!("WebSocket error: {:?}", e.type_()).into());
    });
    ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    on_error.forget();

    // Wait for the connection to open.
    let (tx, rx) = futures::channel::oneshot::channel::<()>();
    let mut tx = Some(tx);
    let on_open = Closure::<dyn FnMut()>::new(move || {
        if let Some(tx) = tx.take() {
            let _ = tx.send(());
        }
    });
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    on_open.forget();

    // Wait for connection
    let _ = rx.await;

    Some(WsSender { ws })
}
