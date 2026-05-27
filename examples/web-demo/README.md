# web-demo

Serverless peer-to-peer chat demo for [`sqlite-diff-rs`](../..). Hosted live at [https://lucacappelletti94.github.io/sqlite-diff-rs/](https://lucacappelletti94.github.io/sqlite-diff-rs/).

## What it does

Two browser tabs each hold their own in-memory SQLite database. Writes go through diesel and a session-extension session captures every change as a binary changeset. The bytes flow directly to the other peer over a WebRTC data channel, and the receiver applies them to its own SQLite. There is no backend.

A diff inspector at the bottom of the page parses every outgoing and incoming byte buffer through `sqlite-diff-rs` and renders the opcode, table, primary key, indirect flag, and per-column values for each operation. This is the most direct way to see what changesets look like on the wire.

## Stack

| Layer | Crate |
|-------|-------|
| UI | [`dioxus`](https://dioxuslabs.com/) 0.7 |
| In-browser SQLite | [`sqlite-wasm-rs`](https://crates.io/crates/sqlite-wasm-rs) |
| ORM + session capture | [`diesel`](https://diesel.rs/) + [`diesel-sqlite-session`](https://github.com/LucaCappelletti94/diesel-sqlite-session) |
| Changeset parsing | [`sqlite-diff-rs`](../..) |
| Transport | WebRTC data channel via [`web-sys`](https://docs.rs/web-sys/) |
| Signaling | URL fragment containing the full SDP (base64url, mDNS-stripped) |
| Hosting | GitHub Pages (static, no backend) |

## Local development

```bash
# From the repo root
dx serve --platform web -p web-demo
# Or from this crate:
# dx serve --platform web
```

Open two browser tabs at the printed URL. In one tab click **Create room** and copy the offer URL into the other tab's address bar. In the other tab click **Generate reply code** and paste the resulting base64 blob back into the first tab's "Paste their reply" box, then click **Connect**.

## Release build

```bash
dx build --release --platform web -p web-demo --base-path /sqlite-diff-rs/
```

The bundle lands in `target/dx/web-demo/release/web/public/`. The `--base-path` flag is required so asset URLs resolve correctly when served under the repo's GitHub Pages subpath.

## Known limitations

- **Symmetric NAT and corporate networks** may fail to establish a data channel with only public STUN (the demo uses `stun:stun.l.google.com:19302`). A TURN server would fix this but would violate the no-backend property of the demo.
- **The offer URL must be opened in a new tab on the answering peer's side, not the same tab as the offerer.** Each peer's WebRTC state lives in its own tab.
- **The display name** is persisted in `localStorage` and never leaves the browser, so the other peer only learns your name from the messages you send.

## Deployment

The [`.github/workflows/deploy-demo.yml`](../../.github/workflows/deploy-demo.yml) workflow builds and publishes the bundle to GitHub Pages on every push to `main`. To enable Pages: in the repository settings, set **Pages → Source → GitHub Actions**.
