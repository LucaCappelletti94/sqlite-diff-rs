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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_user(id: u8, name: &str) -> User {
        User {
            id: vec![id; 16],
            name: name.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    fn make_message(id: u8, sender: u8, receiver: u8, body: &str) -> Message {
        Message {
            id: vec![id; 16],
            sender_id: vec![sender; 16],
            receiver_id: vec![receiver; 16],
            body: body.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn test_app_state_new() {
        let state = AppState::new();
        assert!(state.users.lock().unwrap().is_empty());
        assert!(state.messages.lock().unwrap().is_empty());
        assert!(state.clients.is_empty());
    }

    #[test]
    fn test_find_user_by_id() {
        let state = AppState::new();
        let user = make_user(1, "Alice");

        state.users.lock().unwrap().push(user.clone());

        let found = state.find_user_by_id(&vec![1u8; 16]);
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Alice");

        let not_found = state.find_user_by_id(&vec![2u8; 16]);
        assert!(not_found.is_none());
    }

    #[test]
    fn test_find_user_by_name() {
        let state = AppState::new();
        let user = make_user(1, "Alice");

        state.users.lock().unwrap().push(user);

        let found = state.find_user_by_name("Alice");
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, vec![1u8; 16]);

        let not_found = state.find_user_by_name("Bob");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_multiple_users() {
        let state = AppState::new();

        {
            let mut users = state.users.lock().unwrap();
            users.push(make_user(1, "Alice"));
            users.push(make_user(2, "Bob"));
            users.push(make_user(3, "Charlie"));
        }

        assert!(state.find_user_by_name("Alice").is_some());
        assert!(state.find_user_by_name("Bob").is_some());
        assert!(state.find_user_by_name("Charlie").is_some());
        assert!(state.find_user_by_name("Diana").is_none());
    }

    #[test]
    fn test_messages_storage() {
        let state = AppState::new();

        {
            let mut messages = state.messages.lock().unwrap();
            messages.push(make_message(1, 1, 2, "Hello Bob"));
            messages.push(make_message(2, 2, 1, "Hello Alice"));
        }

        let messages = state.messages.lock().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].body, "Hello Bob");
        assert_eq!(messages[1].body, "Hello Alice");
    }

    #[test]
    fn test_clients_map() {
        let state = AppState::new();
        let (tx, _rx) = mpsc::unbounded_channel();

        state.clients.insert(vec![1u8; 16], tx);
        assert!(state.clients.contains_key(&vec![1u8; 16]));
        assert!(!state.clients.contains_key(&vec![2u8; 16]));

        state.clients.remove(&vec![1u8; 16]);
        assert!(!state.clients.contains_key(&vec![1u8; 16]));
    }

    #[test]
    fn test_user_clone() {
        let user = make_user(1, "Alice");
        let cloned = user.clone();
        assert_eq!(user.id, cloned.id);
        assert_eq!(user.name, cloned.name);
        assert_eq!(user.created_at, cloned.created_at);
    }

    #[test]
    fn test_message_clone() {
        let msg = make_message(1, 1, 2, "Hello");
        let cloned = msg.clone();
        assert_eq!(msg.id, cloned.id);
        assert_eq!(msg.sender_id, cloned.sender_id);
        assert_eq!(msg.receiver_id, cloned.receiver_id);
        assert_eq!(msg.body, cloned.body);
        assert_eq!(msg.created_at, cloned.created_at);
    }
}
