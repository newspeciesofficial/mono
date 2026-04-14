# New pattern: cross-worker WebSocket/socket handoff via `process.send(msg, handle)` + `WebSocketServer.handleUpgrade`

## Category
B (Node stdlib) + C (Concurrency / architectural).

## Where used
- `packages/zero-cache/src/workers/syncer.ts:28` —
  `import {installWebSocketReceiver} from '../types/websocket-handoff.ts'`
- `workers/syncer.ts:122-127` — `installWebSocketReceiver(lc, this.#wss, this.#createConnection, this.#parent)` — receives a socket + pending upgrade headers from the dispatcher over the IPC parent channel.
- `server/worker-dispatcher.ts:11` — `import {installWebSocketHandoff}`
- `server/worker-dispatcher.ts:169-190` — installs the sender side: the dispatcher picks a syncer worker by hash/assignment and forwards the raw `net.Socket` + upgrade head buffer with a typed message envelope.

## TS form
```ts
// Receiver (syncer worker, workers/syncer.ts):
const wss = new WebSocketServer({noServer: true, maxPayload: …});
installWebSocketReceiver(lc, wss, this.#createConnection, this.#parent);

// Internally (types/websocket-handoff.ts):
receiver.onMessageType<Handoff<P>>('handoff', (msg, socket) => {
  if (socket.closed) return;
  server.handleUpgrade(message, socket as Socket, head, ws => {
    if (socket.closed) ws.close();
    else onConnection(ws, payload);
  });
});

// Sender (dispatcher):
process.send({type: 'handoff', message: serializableReq, payload}, socket);
```

The dispatcher process does the HTTP upgrade parse, routes by `clientGroupID`,
then hands the underlying TCP socket (as a file descriptor) to a specific
syncer worker process. The syncer-side `WebSocketServer.handleUpgrade` finishes
the WS handshake using that socket.

## Proposed Rust form
Two realistic options; neither is a one-line port.

**Option A — single process, tokio tasks (preferred).** Replace "workers" with
tokio tasks pinned via a `Vec<mpsc::Sender<UpgradedConnection>>`. The "dispatcher"
becomes a single axum handler that calls `WebSocketUpgrade::on_upgrade(|ws| async { … })`
and forwards the resulting `WebSocket` over an `mpsc::channel` to the chosen
worker task (hash-assigned). No file-descriptor passing.

```rust
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use tokio::sync::mpsc;

type WorkerTx = mpsc::Sender<WorkerJob>;

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(workers): State<Vec<WorkerTx>>,
    params: ConnectParams,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket: WebSocket| async move {
        let idx = xxh32(format!("{task_id}/{}", params.client_group_id).as_bytes(), 0) as usize
            % workers.len();
        let _ = workers[idx].send(WorkerJob { socket, params }).await;
    })
}
```

**Option B — multi-process parity via `SCM_RIGHTS`.** If the Rust port must keep
the TS process topology (N forked syncer processes), use `sendfd` +
`tokio-uds` (Unix domain sockets) to pass the raw TCP `RawFd` from the
dispatcher to the chosen worker. The worker then wraps that fd back into a
`tokio::net::TcpStream` and hands it to `tokio-tungstenite::accept_async`.
Windows has no direct equivalent (`WSADuplicateSocket` exists but differs);
prefer Option A on cross-platform builds.

```rust
// Sender:
use sendfd::SendWithFd;
unix_sock.send_with_fd(&encoded_headers, &[tcp_stream.as_raw_fd()])?;

// Receiver:
use sendfd::RecvWithFd;
let (buf, fds) = unix_sock.recv_with_fd(&mut scratch, &mut fd_buf)?;
let raw = fds[0];
let std_stream = unsafe { std::net::TcpStream::from_raw_fd(raw) };
std_stream.set_nonblocking(true)?;
let tcp = tokio::net::TcpStream::from_std(std_stream)?;
let ws = tokio_tungstenite::accept_hdr_async(tcp, |req, resp| { /* parse headers */ Ok(resp) }).await?;
```

## Classification
Redesign — there is no tokio/axum equivalent of
`WebSocketServer({noServer:true}).handleUpgrade(req, socket, head, cb)` paired
with `process.send(msg, handle)`. Rust ports must either collapse the worker
topology to tasks (Option A, recommended) or hand-code fd passing (Option B).

## Caveats
- The TS code intentionally parses the HTTP upgrade in the dispatcher and then
  calls `handleUpgrade` inside the worker — that lets the worker emit the
  101 Switching Protocols response on the socket it now owns. Option A
  preserves the semantics trivially; Option B has to ship the buffered upgrade
  request head along with the fd so the worker can compute `Sec-WebSocket-Accept`.
- `socket.closed` guard in `websocket-handoff.ts:144-155` is important: the
  client can drop between the sender committing to a worker and the worker
  finishing `handleUpgrade`. Mirror this check in both options.
- Option B is Unix-only without extra work; Option A is portable and aligns
  with the rest of the "single tokio process" direction documented in
  `ts-vs-rs-comparison.md` for the change-source/view-syncer/replicator chain.

## Citation
- axum WebSocket upgrade + `on_upgrade`:
  https://docs.rs/axum/latest/axum/extract/ws/struct.WebSocketUpgrade.html
- tokio-tungstenite `accept_hdr_async`:
  https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/fn.accept_hdr_async.html
- `sendfd` crate (SCM_RIGHTS fd passing over Unix sockets):
  https://docs.rs/sendfd/latest/sendfd/
- Node `subprocess.send(message, sendHandle)` reference, for the TS side:
  https://nodejs.org/api/child_process.html#subprocesssendmessage-sendhandle-options-callback
