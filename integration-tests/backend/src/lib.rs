//! Chat backend library: exposes the server for integration tests.

pub mod state;
pub mod ws;

use std::path::Path;

use axum::Router;
use axum::routing::get;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use crate::state::AppState;

/// Start the server on a given listener (used by integration tests).
pub async fn serve(listener: tokio::net::TcpListener) {
    serve_with_static_dir(listener, None).await;
}

/// Start the server, optionally serving a static-files directory.
///
/// When `static_dir` is `Some`, the directory is served at `/` with a
/// fallback to `index.html` for SPA routing.  This is used in production
/// (Docker) where Trunk pre-builds the WASM bundle into a `dist/` folder.
pub async fn serve_with_static_dir(listener: tokio::net::TcpListener, static_dir: Option<&Path>) {
    let state = AppState::new();

    let app = Router::new()
        .route("/ws", get(ws::ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // If a static-files directory is provided, serve it as a fallback so
    // that the Yew SPA is available at `/` and any unknown path falls
    // back to `index.html`.
    let app = if let Some(dir) = static_dir {
        let index = dir.join("index.html");
        app.fallback_service(ServeDir::new(dir).fallback(ServeFile::new(index)))
    } else {
        app
    };

    // Compression must be applied AFTER the fallback so it wraps
    // everything — including the static file serving (1.6 MB WASM → ~400 KB gzipped).
    let app = app.layer(CompressionLayer::new());

    axum::serve(listener, app).await.unwrap();
}
