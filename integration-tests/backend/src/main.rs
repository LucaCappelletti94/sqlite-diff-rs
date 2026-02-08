//! Chat backend: Axum server with in-memory state, WebSocket binary patchset protocol.
//!
//! All WebSocket communication is raw binary frames containing sqlite-diff-rs patchsets.
//! The backend parses inbound patchsets with [`ParsedDiffSet::parse`] to inspect
//! table/operation, updates its in-memory state, and builds outbound patchsets
//! via [`PatchSetBuilder`].
//!
//! When the `STATIC_DIR` environment variable is set (e.g. in Docker), the
//! server also serves the pre-built Yew SPA from that directory.  This
//! eliminates the need for a separate Trunk runtime container.

use std::net::SocketAddr;
use std::path::PathBuf;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let static_dir: Option<PathBuf> = std::env::var("STATIC_DIR").ok().map(PathBuf::from);

    if let Some(ref dir) = static_dir {
        tracing::info!("Serving static files from {}", dir.display());
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("Listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    chat_backend::serve_with_static_dir(listener, static_dir.as_deref()).await;
}
