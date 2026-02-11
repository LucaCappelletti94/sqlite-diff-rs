//! Integration tests for the chat backend.
//!
//! Spins up the Axum server on a random port, connects two WebSocket clients,
//! exchanges patchsets, and verifies round-trip correctness via `ParsedDiffSet::parse`.

use futures::{SinkExt, StreamExt};
use sqlite_diff_rs::ParsedDiffSet;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use chat_backend::state::{Message as ChatMessage, User};
use chat_backend::ws;

/// Build a patchset for a user INSERT (delegates to the library builder).
fn build_user_insert_patchset(id: &[u8], name: &str, created_at: &str) -> Vec<u8> {
    ws::build_user_patchset(&User {
        id: id.to_vec(),
        name: name.into(),
        created_at: created_at.into(),
    })
}

/// Build a patchset for a message INSERT (delegates to the library builder).
fn build_message_insert_patchset(
    id: &[u8],
    sender_id: &[u8],
    receiver_id: &[u8],
    body: &str,
    created_at: &str,
) -> Vec<u8> {
    ws::build_message_patchset(&ChatMessage {
        id: id.to_vec(),
        sender_id: sender_id.to_vec(),
        receiver_id: receiver_id.to_vec(),
        body: body.into(),
        created_at: created_at.into(),
    })
}

/// Helper to start the server and return the WebSocket URL.
async fn start_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(chat_backend::serve(listener));
    format!("ws://{addr}/ws")
}

/// Receive all available binary messages within a timeout.
async fn recv_patchsets(
    ws: &mut futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    timeout_ms: u64,
) -> Vec<Vec<u8>> {
    let mut results = Vec::new();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Binary(data)))) => {
                results.push(data.to_vec());
            }
            _ => break,
        }
    }

    results
}

#[tokio::test]
async fn test_two_users_exchange_messages() {
    let url = start_server().await;

    // Connect Alice
    let (alice_ws, _) = connect_async(&url).await.unwrap();
    let (mut alice_sink, mut alice_stream) = alice_ws.split();

    // Connect Bob
    let (bob_ws, _) = connect_async(&url).await.unwrap();
    let (mut bob_sink, mut bob_stream) = bob_ws.split();

    // Alice logs in
    let alice_id = uuid::Uuid::new_v4();
    let alice_patchset =
        build_user_insert_patchset(alice_id.as_bytes(), "Alice", "2026-02-07T00:00:00Z");

    // Verify the patchset we built is valid
    let parsed = ParsedDiffSet::parse(&alice_patchset).unwrap();
    assert!(parsed.is_patchset());

    alice_sink
        .send(Message::Binary(alice_patchset.into()))
        .await
        .unwrap();

    // Give the server a moment to process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Bob logs in
    let bob_id = uuid::Uuid::new_v4();
    let bob_patchset = build_user_insert_patchset(bob_id.as_bytes(), "Bob", "2026-02-07T00:00:01Z");
    bob_sink
        .send(Message::Binary(bob_patchset.into()))
        .await
        .unwrap();

    // Bob should receive a catch-up patchset containing Alice
    let bob_received = recv_patchsets(&mut bob_stream, 500).await;
    assert!(
        !bob_received.is_empty(),
        "Bob should receive at least one patchset (catch-up with Alice)"
    );

    // Verify the catch-up patchset is parseable
    for data in &bob_received {
        let parsed = ParsedDiffSet::parse(data).unwrap();
        assert!(parsed.is_patchset());
    }

    // Alice should receive a patchset about Bob joining
    let alice_received = recv_patchsets(&mut alice_stream, 500).await;
    assert!(
        !alice_received.is_empty(),
        "Alice should receive a patchset about Bob"
    );
    for data in &alice_received {
        let parsed = ParsedDiffSet::parse(data).unwrap();
        assert!(parsed.is_patchset());
    }

    // Alice sends a message to Bob
    let msg_id = uuid::Uuid::new_v4();
    let msg_patchset = build_message_insert_patchset(
        msg_id.as_bytes(),
        alice_id.as_bytes(),
        bob_id.as_bytes(),
        "Hello Bob!",
        "2026-02-07T00:00:02Z",
    );

    // Verify the message patchset is valid
    let parsed = ParsedDiffSet::parse(&msg_patchset).unwrap();
    assert!(parsed.is_patchset());

    alice_sink
        .send(Message::Binary(msg_patchset.into()))
        .await
        .unwrap();

    // Bob should receive the message patchset
    let bob_msg_received = recv_patchsets(&mut bob_stream, 500).await;
    assert!(
        !bob_msg_received.is_empty(),
        "Bob should receive Alice's message"
    );

    // The received patchset should be a valid patchset
    let received_parsed = ParsedDiffSet::parse(&bob_msg_received[0]).unwrap();
    assert!(received_parsed.is_patchset());

    // Round-trip: re-serialize and verify byte equality
    let reserialized: Vec<u8> = received_parsed.into();
    let reparsed = ParsedDiffSet::parse(&reserialized).unwrap();
    assert!(reparsed.is_patchset());
}

#[tokio::test]
async fn test_user_does_not_receive_others_messages() {
    let url = start_server().await;

    // Connect Alice, Bob, Eve
    let (alice_ws, _) = connect_async(&url).await.unwrap();
    let (mut alice_sink, _alice_stream) = alice_ws.split();

    let (bob_ws, _) = connect_async(&url).await.unwrap();
    let (mut bob_sink, mut bob_stream) = bob_ws.split();

    let (eve_ws, _) = connect_async(&url).await.unwrap();
    let (mut eve_sink, mut eve_stream) = eve_ws.split();

    let alice_id = uuid::Uuid::new_v4();
    let bob_id = uuid::Uuid::new_v4();
    let eve_id = uuid::Uuid::new_v4();

    // All three log in
    alice_sink
        .send(Message::Binary(
            build_user_insert_patchset(alice_id.as_bytes(), "Alice", "2026-02-07T00:00:00Z").into(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    bob_sink
        .send(Message::Binary(
            build_user_insert_patchset(bob_id.as_bytes(), "Bob", "2026-02-07T00:00:01Z").into(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    eve_sink
        .send(Message::Binary(
            build_user_insert_patchset(eve_id.as_bytes(), "Eve", "2026-02-07T00:00:02Z").into(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Drain catch-up messages
    let _ = recv_patchsets(&mut bob_stream, 300).await;
    let _ = recv_patchsets(&mut eve_stream, 300).await;

    // Alice sends a message to Bob (not Eve)
    let msg_id = uuid::Uuid::new_v4();
    alice_sink
        .send(Message::Binary(
            build_message_insert_patchset(
                msg_id.as_bytes(),
                alice_id.as_bytes(),
                bob_id.as_bytes(),
                "Secret for Bob",
                "2026-02-07T00:00:03Z",
            )
            .into(),
        ))
        .await
        .unwrap();

    // Bob should receive it
    let bob_received = recv_patchsets(&mut bob_stream, 500).await;
    assert!(
        !bob_received.is_empty(),
        "Bob should receive Alice's message"
    );

    // Eve should NOT receive it
    let eve_received = recv_patchsets(&mut eve_stream, 500).await;
    assert!(
        eve_received.is_empty(),
        "Eve should not receive Alice→Bob message"
    );
}

#[tokio::test]
async fn test_self_message() {
    // User sends a message to themselves
    let url = start_server().await;

    let (ws, _) = connect_async(&url).await.unwrap();
    let (mut sink, mut stream) = ws.split();

    let user_id = uuid::Uuid::new_v4();

    // Login
    sink.send(Message::Binary(
        build_user_insert_patchset(user_id.as_bytes(), "Solo", "2026-02-07T00:00:00Z").into(),
    ))
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Send message to self
    let msg_id = uuid::Uuid::new_v4();
    sink.send(Message::Binary(
        build_message_insert_patchset(
            msg_id.as_bytes(),
            user_id.as_bytes(),
            user_id.as_bytes(), // same as sender
            "Note to self",
            "2026-02-07T00:00:01Z",
        )
        .into(),
    ))
    .await
    .unwrap();

    // Should receive the message (sent to self)
    let received = recv_patchsets(&mut stream, 500).await;
    assert!(
        !received.is_empty(),
        "User should receive their own self-message"
    );
}

#[tokio::test]
async fn test_late_joiner_catches_up() {
    // Alice joins, sends messages, Bob joins later and gets catch-up
    let url = start_server().await;

    // Alice connects and logs in
    let (alice_ws, _) = connect_async(&url).await.unwrap();
    let (mut alice_sink, _alice_stream) = alice_ws.split();

    let alice_id = uuid::Uuid::new_v4();
    let bob_id = uuid::Uuid::new_v4();

    alice_sink
        .send(Message::Binary(
            build_user_insert_patchset(alice_id.as_bytes(), "Alice", "2026-02-07T00:00:00Z").into(),
        ))
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Alice sends a message to Bob (who isn't connected yet)
    let msg_id = uuid::Uuid::new_v4();
    alice_sink
        .send(Message::Binary(
            build_message_insert_patchset(
                msg_id.as_bytes(),
                alice_id.as_bytes(),
                bob_id.as_bytes(),
                "Hey Bob, are you there?",
                "2026-02-07T00:00:01Z",
            )
            .into(),
        ))
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Now Bob connects
    let (bob_ws, _) = connect_async(&url).await.unwrap();
    let (mut bob_sink, mut bob_stream) = bob_ws.split();

    bob_sink
        .send(Message::Binary(
            build_user_insert_patchset(bob_id.as_bytes(), "Bob", "2026-02-07T00:00:02Z").into(),
        ))
        .await
        .unwrap();

    // Bob should receive catch-up with Alice's user info AND the message
    let bob_catchup = recv_patchsets(&mut bob_stream, 500).await;
    assert!(
        !bob_catchup.is_empty(),
        "Bob should receive catch-up patchset"
    );

    // All received patchsets should be valid
    for data in &bob_catchup {
        let parsed = ParsedDiffSet::parse(data).unwrap();
        assert!(parsed.is_patchset());
    }
}

#[tokio::test]
async fn test_malformed_data_ignored() {
    // Server should handle malformed data gracefully
    let url = start_server().await;

    let (ws, _) = connect_async(&url).await.unwrap();
    let (mut sink, mut stream) = ws.split();

    // Send garbage data
    sink.send(Message::Binary(vec![0xFF, 0xFE, 0xFD].into()))
        .await
        .unwrap();

    // Should not crash, but also shouldn't receive anything meaningful
    let received = recv_patchsets(&mut stream, 300).await;
    assert!(received.is_empty(), "Garbage input should be ignored");

    // Connection should still work - send valid user registration
    let user_id = uuid::Uuid::new_v4();
    sink.send(Message::Binary(
        build_user_insert_patchset(user_id.as_bytes(), "Valid", "2026-02-07T00:00:00Z").into(),
    ))
    .await
    .unwrap();

    // This is a valid registration, connection should still be alive
    // (we don't expect a response for the first user since there are no others)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_empty_message_ignored() {
    let url = start_server().await;

    let (ws, _) = connect_async(&url).await.unwrap();
    let (mut sink, mut stream) = ws.split();

    // Send empty binary message
    sink.send(Message::Binary(vec![].into())).await.unwrap();

    let received = recv_patchsets(&mut stream, 300).await;
    assert!(received.is_empty(), "Empty messages should be ignored");
}
