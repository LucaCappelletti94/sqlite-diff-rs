# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Base: Rust nightly + wasm32 target + clang + trunk
# ---------------------------------------------------------------------------
FROM rust:bookworm AS base

RUN rustup default nightly && rustup target add wasm32-unknown-unknown
RUN apt-get update && apt-get install -y clang lld && rm -rf /var/lib/apt/lists/*

# sqlite-wasm-rs headers use [[noreturn]] (C23 syntax); default C17 mode rejects it.
ENV CFLAGS_wasm32_unknown_unknown="-std=gnu2x"

# Install trunk before COPY so it survives source-code changes in the cache.
RUN cargo install trunk

WORKDIR /workspace
COPY . .

# ---------------------------------------------------------------------------
# test — backend integration tests (self-contained, no external deps)
# ---------------------------------------------------------------------------
FROM base AS test

# Pre-compile test binaries so the CMD only runs them.
RUN cargo build -p chat-backend --tests

CMD ["cargo", "test", "-p", "chat-backend"]

# ---------------------------------------------------------------------------
# frontend-build — Trunk compiles the Yew SPA into dist/
# ---------------------------------------------------------------------------
FROM base AS frontend-build

WORKDIR /workspace/integration-tests/frontend
# --release enables Rust release optimisations and runs wasm-opt.
# Trunk's bundled wasm-opt (binaryen 123) does not enable bulk-memory /
# nontrapping-fptoint features that Rust nightly emits, so the first build
# attempt fails but downloads wasm-opt.  We wrap the binary with the
# required feature flags and rebuild (cargo compilation is cached).
RUN trunk build --release 2>&1 || true && \
    WASM_OPT=$(find /root/.cache/trunk -name wasm-opt -type f | head -1) && \
    mv "$WASM_OPT" "${WASM_OPT}.real" && \
    printf '#!/bin/sh\nexec "${0}.real" --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext "$@"\n' > "$WASM_OPT" && \
    chmod +x "$WASM_OPT" && \
    trunk build --release

# Trunk embeds an inline <script type="module"> in index.html.  VS Code's
# Simple Browser (and other restricted webview environments) block inline
# scripts via Content Security Policy.  Extract the inline script into a
# separate file so the HTML only references it via src="…".
RUN cd dist && \
    perl -0777 -ne 'print $1 if m{<script type="module">\s*(.*?)\s*</script>}s' \
        index.html > init.js && \
    perl -0777 -pi -e 's{<script type="module">.*?</script>}{<script type="module" src="/init.js"></script>}s' \
        index.html

# ---------------------------------------------------------------------------
# app — single Axum server: WebSocket + static files from dist/
# ---------------------------------------------------------------------------
FROM base AS app

RUN cargo build -p chat-backend

# Copy the pre-built frontend assets into the image.
COPY --from=frontend-build /workspace/integration-tests/frontend/dist /app/dist

ENV RUST_LOG=info
ENV STATIC_DIR=/app/dist
EXPOSE 3000

CMD ["cargo", "run", "-p", "chat-backend"]
