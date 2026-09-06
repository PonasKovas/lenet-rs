# lenet-rs

Async Rust bindings for [Lenet](../lenet) — a sans-I/O implementation of the
[ENet](http://enet.bespin.org) protocol written in Lean 4, distributed as a
static C archive.

```
┌────────────────────────────────────────────────────────────┐
│  your app                                                  │
│    │ send()/disconnect()      ▲ events (CONNECT, RECEIVE…) │
│    ▼                          │                            │
│  lenet-tokio  one task owns: socket + host + pump loop      │
│    │  datagrams in, events + datagrams out, time supplied   │
│    ▼                                                       │
│  lenet        safe sans-I/O API over the C distribution     │
│    ▼                                                       │
│  lenet-sys     raw externs ── liblenet.a (Lean + C shim)    │
└────────────────────────────────────────────────────────────┘
```

## Crates

| crate         | purpose |
|---------------|---------|
| `lenet-sys`   | Raw 1:1 FFI to `lenet.h`. `build.rs` links `liblenet.a` from `LENET_LIB_DIR` (default: the sibling `lenet` checkout's `csrc/build`). |
| `lenet`       | Safe sans-I/O API: `HostBuilder` → `Host`. No sockets, no clock — datagrams and `now_ms` in, events + datagrams out. One host, one thread. |
| `lenet-tokio` | The async driver. One tokio task owns the `UdpSocket` + `Host`: received datagrams → `handle_datagram`, `service(now)` scheduled at the host's own `next_deadline` (plus a slow fallback tick), outgoing datagrams transmitted, events forwarded on an mpsc channel. Builder-style config, `Connection` handles for send/disconnect. |
| `enet-helper` | Test oracle only: a real C ENet endpoint (pinned upstream `lsalzman/enet @ 5a9c537`) built via the `enet` clone in the sibling directory. |

## Sans-I/O contract

The engine never touches the network or the clock; the tokio driver is the
only place that does. This mirrors the Lean side (`test/Replay.lean` drives
the core through the same primitive the driver uses: service ticks are
scheduled at `Host.nextDeadline` between datagram arrivals).

## Usage

```rust
use std::net::SocketAddr;
use lenet::SendFlags;
use lenet_tokio::{Endpoint, EventKind, LenetConfig};

#[tokio::main]
async fn main() {
    let (endpoint, mut events) =
        Endpoint::start(LenetConfig::new()).await.expect("endpoint");

    let server: SocketAddr = "127.0.0.1:32887".parse().unwrap();
    let conn = endpoint.connect(server, 2, 0).await.expect("connect");

    // Wait for the handshake, then send.
    match events.recv().await.unwrap().kind {
        EventKind::Connect => {}
        _ => panic!(),
    }
    conn.send(0, SendFlags::RELIABLE, b"hello".to_vec()).await.unwrap();

    while let Some(ev) = events.recv().await {
        if let EventKind::Receive = ev.kind {
            println!("got {} bytes: {:?}", ev.payload.len(), &ev.payload[..]);
        }
    }
}
```

Threading: the host is not thread-safe (C contract: one host, one thread).
The pump task is that thread; application handles only exchange messages
with it.

## Building & testing

Prerequisites: Rust (stable), the sibling `lenet` repo with its C
distribution built, and an ENet checkout (pinned revision) for the interop
test:

```sh
# 1. Build the Lenet C archive (done once, or after changes upstream):
make -C ../lenet/csrc

# 2. Build + test everything, including the live ENet interop:
cargo test --workspace
```

The interop test (`lenet-tokio/tests/interop.rs`) spawns the `enet-helper`
binary — a real ENet server/client — and drives a lenet-tokio endpoint
against it over live UDP:

- `connect_and_reliable_echo` — handshake, reliable roundtrip, graceful disconnect with data
- `fragmented_echo` — 40 KB reliable packet both directions (fragmentation on the wire)
- `server_role_roundtrip` — Rust as the *server*: ENet client connects, Rust echoes via the event stream, disconnect data verified

`ENET_LIB_DIR`/`ENET_HELPER` env vars override the library and oracle
locations. The helper builds ENet from `../../enet` (cmake); override by
editing `enet-helper/build.rs` or pointing `ENET_DIR` (see `build.rs`).

## Design notes

- **Payload delivery**: `lenet`'s `poll_event` copies payloads into a
  caller-supplied buffer (C API constraint); the tokio driver owns a
  reusable buffer (default 64 KiB, configurable) and attaches owned payload
  bytes to each delivered `DrivenEvent`. Events larger than the buffer are
  truncated (`PolledEvent::truncated`).
- **Scheduling**: between ticks the pump sleeps until the host's
  `next_deadline` (wrap-aware, same cyclic arithmetic as `Lenet.Time`), so
  retransmit/ping/throttle timing does not depend on busy-polling. A
  configurable fallback tick (default 100 ms) bounds missed-deadline drift.
- **Errors**: `connect`/`send` surface the C API's failure returns as typed
  errors; everything else is infallible by contract (the engine never
  panics — see the Lean side's no-panics gate).
- **Out of scope** (inherited from Lenet): compression (ENet's PPM range
  coder), shared libraries (Lean runtime is not -fPIC).
