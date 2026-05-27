//! WebRTC peer connection and data channel wrapper.
//!
//! Owns a single `RTCPeerConnection` configured with Google's public STUN
//! server, plus the data channel (created locally for the offerer or
//! received via `ondatachannel` for the answerer). All JS callbacks are
//! `Closure::forget`-ed since the demo only ever holds one peer at a
//! time for the lifetime of the page.
//!
//! The asynchronous offer/answer/ICE handshake follows the non-trickle
//! pattern: we wait for `iceGatheringState === 'complete'` before reading
//! `localDescription`, so the resulting SDP already contains every ICE
//! candidate and can be exchanged in a single fragment.

use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcDataChannelState,
    RtcDataChannelType, RtcIceGatheringState, RtcIceServer, RtcPeerConnection, RtcSdpType,
    RtcSessionDescriptionInit,
};

/// High-level connection state surfaced to the UI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerState {
    /// Data channel is open in both directions.
    Connected,
    /// Data channel closed or connection ended.
    Closed,
}

type MessageCb = dyn FnMut(Vec<u8>);
type StateCb = dyn FnMut(PeerState);

/// Wraps an `RTCPeerConnection` plus its single data channel. Holds the
/// user-provided message and state callbacks behind `Rc<RefCell<...>>`
/// so the same callbacks survive both the offerer path (channel created
/// locally) and the answerer path (channel received via `ondatachannel`).
///
/// `Clone` is cheap: every field is either a `JsValue` wrapper or an
/// `Rc`, so cloning just bumps reference counts. We expose `Clone` so
/// callers can move a peer handle into async tasks without holding a
/// `RefCell` borrow across an await point.
#[derive(Clone)]
pub struct Peer {
    pc: RtcPeerConnection,
    channel: Rc<RefCell<Option<RtcDataChannel>>>,
    on_message: Rc<RefCell<Box<MessageCb>>>,
    on_state: Rc<RefCell<Box<StateCb>>>,
}

impl Peer {
    /// Build a peer with the given binary-message and state-change callbacks.
    ///
    /// # Errors
    ///
    /// Returns the underlying `JsValue` if `RTCPeerConnection` construction
    /// fails (typically a browser without WebRTC support).
    pub fn new(
        on_message: impl FnMut(Vec<u8>) + 'static,
        on_state: impl FnMut(PeerState) + 'static,
    ) -> Result<Self, JsValue> {
        let cfg = RtcConfiguration::new();
        let servers = js_sys::Array::new();
        let server = RtcIceServer::new();
        server.set_urls(&JsValue::from_str("stun:stun.l.google.com:19302"));
        servers.push(&server);
        cfg.set_ice_servers(&servers);

        let pc = RtcPeerConnection::new_with_configuration(&cfg)?;

        Ok(Self {
            pc,
            channel: Rc::new(RefCell::new(None)),
            on_message: Rc::new(RefCell::new(Box::new(on_message))),
            on_state: Rc::new(RefCell::new(Box::new(on_state))),
        })
    }

    /// Create the offerer's data channel and produce an SDP offer.
    ///
    /// Waits for ICE gathering to complete and strips Chrome's mDNS
    /// host candidates (which are useless across networks). Returns the
    /// resulting SDP string ready for URL-fragment encoding.
    ///
    /// # Errors
    ///
    /// Returns any `JsValue` raised by the underlying WebRTC operations.
    pub async fn create_offer(&self) -> Result<String, JsValue> {
        let channel = self.pc.create_data_channel("chat");
        install_channel_handlers(&channel, self.on_message.clone(), self.on_state.clone());
        *self.channel.borrow_mut() = Some(channel);

        let offer = JsFuture::from(self.pc.create_offer()).await?;
        let offer_init: RtcSessionDescriptionInit = offer.unchecked_into();
        JsFuture::from(self.pc.set_local_description(&offer_init)).await?;

        self.wait_ice_complete().await;

        let local = self
            .pc
            .local_description()
            .ok_or_else(|| JsValue::from_str("no local description after createOffer"))?;
        Ok(strip_mdns(&local.sdp()))
    }

    /// Accept a remote offer SDP and produce the answer SDP.
    ///
    /// Registers `ondatachannel` first so the offerer's channel is captured
    /// the moment it arrives. Then sets the remote description, creates an
    /// answer, waits for ICE, and returns the answer SDP.
    ///
    /// # Errors
    ///
    /// Returns any `JsValue` raised by the underlying WebRTC operations.
    pub async fn answer_offer(&self, remote_offer_sdp: &str) -> Result<String, JsValue> {
        let channel_slot = self.channel.clone();
        let on_message = self.on_message.clone();
        let on_state = self.on_state.clone();
        let cb = Closure::<dyn FnMut(_)>::new(move |evt: RtcDataChannelEvent| {
            let ch = evt.channel();
            install_channel_handlers(&ch, on_message.clone(), on_state.clone());
            *channel_slot.borrow_mut() = Some(ch);
        });
        self.pc.set_ondatachannel(Some(cb.as_ref().unchecked_ref()));
        cb.forget();

        let remote = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
        remote.set_sdp(remote_offer_sdp);
        JsFuture::from(self.pc.set_remote_description(&remote)).await?;

        let answer = JsFuture::from(self.pc.create_answer()).await?;
        let answer_init: RtcSessionDescriptionInit = answer.unchecked_into();
        JsFuture::from(self.pc.set_local_description(&answer_init)).await?;

        self.wait_ice_complete().await;

        let local = self
            .pc
            .local_description()
            .ok_or_else(|| JsValue::from_str("no local description after createAnswer"))?;
        Ok(strip_mdns(&local.sdp()))
    }

    /// Apply the answer SDP that the offerer received back from the peer.
    /// This is the final step on the offerer's side before the data channel
    /// opens.
    ///
    /// # Errors
    ///
    /// Returns any `JsValue` raised by `setRemoteDescription`.
    pub async fn accept_answer(&self, remote_answer_sdp: &str) -> Result<(), JsValue> {
        let remote = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        remote.set_sdp(remote_answer_sdp);
        JsFuture::from(self.pc.set_remote_description(&remote)).await?;
        Ok(())
    }

    /// Send a binary payload to the peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the data channel is not yet open, or if the
    /// underlying `RTCDataChannel.send` throws.
    pub fn send(&self, bytes: &[u8]) -> Result<(), JsValue> {
        let channel = self.channel.borrow();
        let ch = channel
            .as_ref()
            .ok_or_else(|| JsValue::from_str("data channel not yet established"))?;
        ch.send_with_u8_array(bytes)
    }

    /// Returns `true` if the data channel is in the `open` state. Use
    /// this before `send` to skip neighbors whose channel has just
    /// transitioned to `closing` or `closed` but whose `onclose`
    /// callback has not yet had a chance to fire.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.channel
            .borrow()
            .as_ref()
            .is_some_and(|ch| ch.ready_state() == RtcDataChannelState::Open)
    }

    async fn wait_ice_complete(&self) {
        if self.pc.ice_gathering_state() == RtcIceGatheringState::Complete {
            return;
        }
        let pc = self.pc.clone();
        let promise = js_sys::Promise::new(&mut |resolve, _reject| {
            let pc_for_closure = pc.clone();
            let resolve = resolve.clone();
            let cb = Closure::<dyn FnMut()>::new(move || {
                if pc_for_closure.ice_gathering_state() == RtcIceGatheringState::Complete {
                    let _ = resolve.call0(&JsValue::null());
                }
            });
            pc.set_onicegatheringstatechange(Some(cb.as_ref().unchecked_ref()));
            cb.forget();
        });
        let _ = JsFuture::from(promise).await;
    }
}

fn install_channel_handlers(
    channel: &RtcDataChannel,
    on_message: Rc<RefCell<Box<MessageCb>>>,
    on_state: Rc<RefCell<Box<StateCb>>>,
) {
    channel.set_binary_type(RtcDataChannelType::Arraybuffer);

    let on_state_open = on_state.clone();
    let cb = Closure::<dyn FnMut()>::new(move || {
        (on_state_open.borrow_mut())(PeerState::Connected);
    });
    channel.set_onopen(Some(cb.as_ref().unchecked_ref()));
    cb.forget();

    let on_state_close = on_state;
    let cb = Closure::<dyn FnMut()>::new(move || {
        (on_state_close.borrow_mut())(PeerState::Closed);
    });
    channel.set_onclose(Some(cb.as_ref().unchecked_ref()));
    cb.forget();

    let cb = Closure::<dyn FnMut(_)>::new(move |evt: MessageEvent| {
        let data = evt.data();
        if let Ok(ab) = data.dyn_into::<js_sys::ArrayBuffer>() {
            let view = js_sys::Uint8Array::new(&ab);
            let mut bytes = vec![0u8; view.length() as usize];
            view.copy_to(&mut bytes);
            (on_message.borrow_mut())(bytes);
        }
    });
    channel.set_onmessage(Some(cb.as_ref().unchecked_ref()));
    cb.forget();
}

/// Drop SDP candidate lines that reference Chrome's mDNS hostnames
/// (`.local`). Those candidates are useless cross-network and only inflate
/// the SDP payload.
fn strip_mdns(sdp: &str) -> String {
    let mut out = String::with_capacity(sdp.len());
    for line in sdp.lines() {
        if line.starts_with("a=candidate") && line.contains(".local") {
            continue;
        }
        out.push_str(line);
        out.push_str("\r\n");
    }
    out
}
