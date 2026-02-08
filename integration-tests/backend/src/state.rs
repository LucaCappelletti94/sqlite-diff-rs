//! Application state: in-memory storage for users and messages.

use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// A user stored in the backend.
#[derive(Debug, Clone)]
pub struct User {
    /// 16-byte UUID v4.
    pub id: Vec<u8>,
    /// Display name.
    pub name: String,
    /// ISO 8601 timestamp.
    pub created_at: String,
}

/// A message stored in the backend.
#[derive(Debug, Clone)]
pub struct Message {
    /// 16-byte UUID v4.
    pub id: Vec<u8>,
    /// Sender user ID (16 bytes).
    pub sender_id: Vec<u8>,
    /// Receiver user ID (16 bytes).
    pub receiver_id: Vec<u8>,
    /// Message body.
    pub body: String,
    /// ISO 8601 timestamp.
    pub created_at: String,
}

/// Channel sender for pushing binary patchsets to a connected WebSocket client.
pub type ClientSender = mpsc::UnboundedSender<Vec<u8>>;

/// Shared application state.
#[derive(Debug, Clone)]
pub struct AppState {
    /// All registered users, keyed by 16-byte UUID.
    pub users: Arc<Mutex<Vec<User>>>,
    /// All messages.
    pub messages: Arc<Mutex<Vec<Message>>>,
    /// Connected WebSocket clients, keyed by user UUID (16 bytes).
    /// Value is a channel sender for pushing patchsets to the client.
    pub clients: Arc<DashMap<Vec<u8>, ClientSender>>,
}

impl AppState {
    /// Create a new empty state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            users: Arc::new(Mutex::new(Vec::new())),
            messages: Arc::new(Mutex::new(Vec::new())),
            clients: Arc::new(DashMap::new()),
        }
    }

    /// Find a user by their 16-byte UUID.
    pub fn find_user_by_id(&self, id: &[u8]) -> Option<User> {
        let users = self.users.lock().unwrap();
        users.iter().find(|u| u.id == id).cloned()
    }

    /// Find a user by name.
    pub fn find_user_by_name(&self, name: &str) -> Option<User> {
        let users = self.users.lock().unwrap();
        users.iter().find(|u| u.name == name).cloned()
    }
}
