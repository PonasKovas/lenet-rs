# lenet-rs

Async Rust bindings for [Lenet](../lenet) — a sans-I/O implementation of the
[ENet](http://enet.bespin.org) protocol written in Lean 4, distributed as a
static C archive.

```
┌────────────────────────────────────────────────────────────┐
│  your app                                                  │
│    │ Connecting / Connection / Incoming handles            │
│    ▼                                                       │
│  lenet-tokio  one pump task owns: socket + host + clocks   │
│    │  datagrams in, events + datagrams out, time supplied  │
│    ▼                                                       │
│  lenet        safe sans-I/O API over the C distribution    │
│    ▼                                                       │
│  lenet-sys     raw externs ── liblenet.a (Lean + C shim)   │
└────────────────────────────────────────────────────────────┘
```

## Crates

| crate         | purpose |
|---------------|---------|
| `lenet-sys`   | Raw 1:1 FFI to `lenet.h`. `build.rs` fetches Lenet at a pinned revision, provisions the Lean toolchain (elan) if needed, and builds `liblenet.a` automatically — the crate behaves like any normal sys crate. |
| `lenet`       | Safe sans-I/O API: `HostBuilder` → `Host`. No sockets, no clock — datagrams and `now_ms` in, events + datagrams out (borrowed views, zero-copy). One host, one thread. |
| `lenet-tokio` | The async driver: stall-free pump loop, `Connecting`/`Connection`/`Incoming` handles, per-connection event queues, real send backpressure. |
| `enet-helper` | Test oracle only: a real C ENet endpoint; fetches + builds upstream `lsalzman/enet` (pinned) automatically. |

## Usage

Client role — `connect` returns a `Connecting` future that resolves when the
handshake completes (no half-open connections):

```rust
use lenet::SendFlags;
use lenet_tokio::{ConnEvent, Endpoint, LenetConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::start(LenetConfig::new()).await?;

    let mut conn = endpoint
        .connect("127.0.0.1:32887".parse()?, 2, 0x1234)
        .await?   // queued on the driver
        .await?;  // handshake completes

    conn.send(0, SendFlags::RELIABLE, b"hello".to_vec()).await?;

    while let Some(event) = conn.recv().await {
        match event {
            ConnEvent::Receive { payload, .. } => {
                println!("got {} bytes: {:?}", payload.len(), &payload[..]);
            }
            ConnEvent::Disconnect { .. } => break,
        }
    }
    Ok(())
}
```

Server role — incoming connections arrive on an `AcceptQueue`:

```rust
use lenet::SendFlags;
use lenet_tokio::{Endpoint, LenetConfig};

async fn server() -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, mut accepts) =
        Endpoint::start(LenetConfig::new().bind_addr("0.0.0.0:9000".parse()?)).await?;
    while let Some(incoming) = accepts.accept().await {
        let connect_data = incoming.connect_data();
        let _ = connect_data;
        tokio::spawn(async move {
            let mut conn = incoming.accept();
            while let Some(ConnEvent::Receive { channel_id, payload }) = conn.recv().await {
                let _ = conn.send(channel_id, SendFlags::RELIABLE, payload).await;
            }
        });
    }
    Ok(())
}
```

## Design notes

**Stall-free pump.** The driver's pump task awaits only in its `select!`
(inbound datagram, host deadline, command channels). Everything else —
dispatching commands, running timers, flushing outgoing datagrams and
events — is non-blocking batch work:

- A slow application reader fills **only its own connection's** event queue;
  it can never stall the pump, miss the host's retransmit/keepalive
  deadlines, or block other connections (no head-of-line blocking).
- Outgoing datagrams go out with `try_send_to`; when the kernel socket
  buffer is full the datagram is dropped, which is equivalent to network
  loss (reliable traffic retransmits).

**Backpressure.** `Connection::send` queues into a bounded command channel
and genuinely awaits when it is full. Inbound behavior is configurable via
`EventQueuePolicy`: `Unbounded` (default; mirrors C ENet's receive queue —
events are never lost) or `BoundedThenDisconnect` (a full queue
force-disconnects that peer with a configurable payload instead of dropping
events or stalling). New *incoming* connections are rejected (graceful
disconnect) when the `accept_channel` queue is full.

**Handle safety.** The engine recycles peer ids, so user-facing handles are
keyed by internal generation tokens. A stale `Connection` (its peer
disconnected and the slot was reused) fails closed: `send` returns
`SendError::Closed` instead of silently messaging a stranger. Dropping a
`Connection` gracefully disconnects its peer. Dropping a `Connecting`
future without awaiting cancels the attempt.

**Sans-I/O contract.** `lenet` never touches the network or the clock; the
tokio driver is the only place that does. This mirrors the Lean side
(`test/Replay.lean` drives the core through the same primitive the driver
uses: service ticks are scheduled at `Host.nextDeadline` between datagram
arrivals). `Host::poll_event`/`poll_outgoing` return borrowed views into
host-internal buffers — the receive buffer is pre-sized to the engine's
hard reassembly maximum, so payloads are never truncated.

**Scheduling.** Between ticks the pump sleeps until the host's
`next_deadline` (wrap-aware, same cyclic arithmetic as `Lenet.Time`), so
retransmit/ping/throttle timing does not depend on busy-polling. A
configurable fallback tick (default 100 ms) bounds missed-deadline drift.

**Errors.** Everything fallible is a proper `std::error::Error`:
`lenet`'s engine-level `SendError`/`ConnectError`, and the driver-level
`lenet_tokio::{ConnectError, SendError}` (via `thiserror`).

**Shutdown.** `Endpoint::shutdown()` flushes pending outgoing traffic and
stops the driver. Dropping every handle also stops it (without a guaranteed
final flush).

**Out of scope** (inherited from Lenet): compression (ENet's PPM range
coder), IPv6 (datagrams from non-IPv4 sources are dropped), shared
libraries (the Lean runtime is not `-fPIC`).

## Building & testing

Prerequisites: Rust (stable), `git`, `curl`, and a C toolchain (`cc`/`make`).
That's it — the build scripts fetch and build everything else on first use:

```sh
cargo test --workspace
```

On the first build, `lenet-sys` clones the [Lenet](https://github.com/PonasKovas/lenet)
repo at a pinned revision (see `LENET_REV` in `lenet-sys/build.rs`), installs
the Lean toolchain it pins via elan if you don't have one, and compiles
`liblenet.a`; `enet-helper` clones upstream ENet at a pinned revision and
builds it with cmake. Subsequent builds are incremental no-ops.

**Developing against a local Lenet/ENet checkout** (e.g. iterating on the
Lean side): point the build scripts at your checkouts and they build from
there instead, incrementally:

```sh
export LENET_DIR=../lenet   # use + rebuild your Lenet checkout
export ENET_DIR=../enet     # use your ENet checkout for the interop oracle
cargo test --workspace
```

The interop tests (`lenet-tokio/tests/interop.rs`) spawn the `enet-helper`
binary — a real ENet server/client — and drive a lenet-tokio endpoint
against it over live UDP:

- `connect_and_reliable_echo` — handshake (awaited via `Connecting`), reliable roundtrip, graceful disconnect with data
- `fragmented_echo` — 40 KB reliable packet both directions (fragmentation on the wire)
- `server_role_roundtrip` — Rust as the *server*: ENet client connects, Rust echoes, disconnect data verified
- `stale_handle_fails_after_slot_reuse` — ABA safety: a handle from a disconnected peer fails closed after the slot is recycled
- `slow_consumer_does_not_stall_pump` — a blocked reader cannot stall the pump; other connections keep flowing; events are buffered
- `accept_overflow_rejects_newcomer` — with a full accept queue, new incoming connections are rejected instead of stalling
- `connect_timeout_resolves_timed_out` — attempts to an unresponsive address resolve `TimedOut` per the configured peer timeout
- `shutdown_closes_everything` — `Endpoint::shutdown` flushes and closes all handles

`ENET_HELPER` overrides the oracle binary location (the tests use a
prebuilt one from `target/debug` when present). `LENET_DIR`/`ENET_DIR`
override the source checkouts, as shown above.

Threading: the host is not thread-safe (C contract: one host, one thread).
The pump task is that thread; application handles only exchange messages
with it.
