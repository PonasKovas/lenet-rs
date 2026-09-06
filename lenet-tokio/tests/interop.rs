//! Async interop tests: `lenet-tokio` (Rust) vs real `ENet` (C, via the
//! `enet-helper` binary). Covers the driver API end to end over live UDP:
//! connect/echo, fragmentation, server role, stale-handle (ABA) safety,
//! slow-consumer isolation, accept backpressure, connect timeout, shutdown,
//! connection splitting, and remote-address visibility.
//!
//! Requires the `enet-helper` binary (`cargo build --workspace` produces
//! it); override its location with the `ENET_HELPER` env var.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::process::Command as AsyncCommand;

use bytes::Bytes;
use lenet::SendFlags;
use lenet_tokio::{ConnEvent, ConnectError, Endpoint, LenetConfig, SendError};

fn helper_path() -> PathBuf {
    if let Ok(p) = std::env::var("ENET_HELPER") {
        return PathBuf::from(p);
    }
    // The oracle binary is a plain build artifact: `cargo test` only builds
    // the test harness, so it must be produced by `cargo build --workspace`
    // (the CI and README workflows do exactly that).
    let mut dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("target")
        });
    dir.push("debug");
    dir.join("enet-helper")
}

/// An `ENet` echo server (the helper's `server` mode), killed on drop.
struct Helper {
    child: Child,
    port: u16,
}

impl Helper {
    /// Spawns the helper's echo server on a free port.
    fn start() -> Helper {
        let path = helper_path();
        let port = free_port();
        let mut child = Command::new(&path)
            .args(["server", &port.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn enet-helper — build it first with `cargo build --workspace`");
        let mut ready = String::new();
        if wait_ready(&mut child, &mut ready, 3000) {
            Helper { child, port }
        } else {
            let _ = child.kill();
            let _ = child.wait();
            panic!("enet-helper failed to start: {ready}");
        }
    }

    fn addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
    }
}

impl Drop for Helper {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A helper in `client` mode, killed on drop.
struct ClientProc {
    child: tokio::process::Child,
}

impl ClientProc {
    fn spawn(port: u16, msg: &str) -> ClientProc {
        let child = AsyncCommand::new(helper_path())
            .args(["client", "127.0.0.1", &port.to_string(), msg])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn helper client");
        ClientProc { child }
    }

    /// Waits for the process to exit.
    async fn wait(&mut self) -> std::process::ExitStatus {
        self.child.wait().await.expect("wait helper client")
    }
}

impl Drop for ClientProc {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn wait_ready(child: &mut Child, out: &mut String, timeout_ms: u64) -> bool {
    use std::io::Read;
    let start = std::time::Instant::now();
    let mut buf = [0u8; 256];
    if let Some(stdout) = child.stdout.as_mut() {
        while start.elapsed().as_millis() < timeout_ms as u128 {
            match stdout.read(&mut buf) {
                Ok(0) => return false, // EOF
                Ok(n) => {
                    out.push_str(&String::from_utf8_lossy(&buf[..n]));
                    if out.contains("READY") {
                        return true;
                    }
                }
                Err(_) => return false,
            }
        }
    }
    false
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind port 0")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Waits for the connection's handshake to complete.
async fn handshake(endpoint: &Endpoint, addr: SocketAddr) -> lenet_tokio::Connection {
    endpoint
        .connect(addr, 2, 0)
        .expect("connect queued")
        .await
        .expect("handshake")
}

/// Echoes everything received on `conn` until it closes. Returns when the
/// connection's event queue is exhausted.
async fn echo_until_closed(mut conn: lenet_tokio::Connection) {
    while let Some(ev) = conn.recv().await {
        if let ConnEvent::Receive {
            channel_id,
            payload,
        } = ev
        {
            conn.send(channel_id, SendFlags::RELIABLE, payload)
                .await
                .expect("echo send");
        }
    }
}

/// Client test flow: connect (handshake awaited by `Connecting`), send,
/// collect the echo.
#[tokio::test]
async fn connect_and_reliable_echo() {
    let mut helper = Helper::start();
    let endpoint = Endpoint::start(LenetConfig::new()).await.expect("endpoint");
    let mut conn = handshake(&endpoint, helper.addr()).await;

    // The client knows who it is talking to.
    assert_eq!(SocketAddr::V4(conn.remote_addr()), helper.addr());

    conn.send(0, SendFlags::RELIABLE, b"ping-from-rust".to_vec())
        .await
        .expect("send");

    // The helper echoes reliably, with the exact bytes.
    let ev = tokio::time::timeout(Duration::from_secs(5), conn.recv())
        .await
        .expect("recv within timeout")
        .expect("event");
    match ev {
        ConnEvent::Receive { payload, .. } => {
            assert_eq!(payload, Bytes::from_static(b"ping-from-rust"));
        }
        other => panic!("expected echo, got {other:?}"),
    }

    conn.disconnect(42).await.expect("disconnect");
    // The helper exits 0 after its DISCONNECT.
    let status = helper.child.wait().expect("wait helper");
    assert!(status.success(), "helper exited with {status}");
}

#[tokio::test]
async fn fragmented_echo() {
    let helper = Helper::start();
    let endpoint = Endpoint::start(LenetConfig::new()).await.expect("endpoint");
    let mut conn = handshake(&endpoint, helper.addr()).await;

    // 40 KB reliable: needs fragmentation on the wire (MTU ~1392).
    let big: Vec<u8> = (0..40_000u32).map(|i| (i * 7 + (i >> 8)) as u8).collect();
    conn.send(0, SendFlags::RELIABLE, big.clone())
        .await
        .expect("send");

    // The helper echoes the full payload reliably; compare bytes exactly.
    let ev = tokio::time::timeout(Duration::from_secs(10), conn.recv())
        .await
        .expect("recv within timeout")
        .expect("event");
    match ev {
        ConnEvent::Receive { payload, .. } => assert_eq!(payload, big),
        other => panic!("expected echo, got {other:?}"),
    }

    conn.disconnect(0).await.expect("disconnect");
}

#[tokio::test]
async fn server_role_roundtrip() {
    // lenet-tokio as the SERVER: fixed port, helper in client mode. The
    // helper drives connect -> send -> (expects echo) -> disconnect(42);
    // the Rust side accepts and echoes, and verifies the connect payload.
    let port = free_port();
    let bind = SocketAddr::from(([127, 0, 0, 1], port));
    let endpoint = Endpoint::start(LenetConfig::new().bind_addr(bind))
        .await
        .expect("endpoint");
    let mut accepts = endpoint.listen().await.expect("listen");

    let echo_task = tokio::spawn(async move {
        while let Some(incoming) = accepts.accept().await {
            assert_eq!(incoming.connect_data(), 0, "helper connects with data 0");
            // The server knows who is connecting: loopback, a real port.
            assert_eq!(incoming.remote_addr().ip().to_string(), "127.0.0.1");
            assert!(incoming.remote_addr().port() > 0);
            tokio::spawn(echo_until_closed(incoming.accept()));
        }
    });

    // Run the helper client without blocking the (single-threaded) test
    // runtime: the server pump task must keep running.
    let status = AsyncCommand::new(helper_path())
        .args(["client", "127.0.0.1", &port.to_string(), "hello-server"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .await
        .expect("spawn helper client");
    assert!(status.success(), "helper client failed: {status}");

    // The helper's own exit status already proves the roundtrip (connect,
    // echo bytes, disconnect); the echo task proves the Rust side accepted
    // and could reply.
    echo_task.abort();
}

/// Peer slots are recycled by the engine; a handle from a disconnected
/// connection must fail closed instead of silently reaching the *new*
/// occupant of the slot.
#[tokio::test]
async fn stale_handle_fails_after_slot_reuse() {
    let helper1 = Helper::start();
    let endpoint = Endpoint::start(LenetConfig::new()).await.expect("endpoint");

    let mut conn1 = handshake(&endpoint, helper1.addr()).await;
    let slot = conn1.peer_id();

    conn1.disconnect(0).await.expect("disconnect");
    // Drain until the pump tears the queue down (DISCONNECT dispatched).
    while conn1.recv().await.is_some() {}
    drop(helper1); // its echo server exits on disconnect

    // A second connection reuses the same peer slot.
    let helper2 = Helper::start();
    let mut conn2 = handshake(&endpoint, helper2.addr()).await;
    assert_eq!(
        conn2.peer_id(),
        slot,
        "peer slot must be recycled for the ABA case"
    );

    // The new connection works end to end.
    conn2
        .send(0, SendFlags::RELIABLE, b"second".to_vec())
        .await
        .expect("send");
    let ev = tokio::time::timeout(Duration::from_secs(5), conn2.recv())
        .await
        .expect("recv within timeout")
        .expect("event");
    match ev {
        ConnEvent::Receive { payload, .. } => assert_eq!(payload, Bytes::from_static(b"second")),
        other => panic!("expected echo, got {other:?}"),
    }

    // The stale handle must fail closed: no silent routing to the new
    // occupant of the recycled slot.
    assert!(
        matches!(
            conn1.send(0, SendFlags::RELIABLE, b"evil".to_vec()).await,
            Err(SendError::Closed)
        ),
        "stale handle must return SendError::Closed"
    );
    assert!(matches!(conn1.disconnect(0).await, Err(SendError::Closed)));
}

/// A slow reader fills only its own connection's queue: the pump keeps
/// servicing other connections and the host's timers, and the slow
/// connection's events are buffered (never dropped, default policy).
#[tokio::test]
async fn slow_consumer_does_not_stall_pump() {
    let port = free_port();
    let bind = SocketAddr::from(([127, 0, 0, 1], port));
    let endpoint = Endpoint::start(LenetConfig::new().bind_addr(bind))
        .await
        .expect("endpoint");
    let mut accepts = endpoint.listen().await.expect("listen");

    // Client A connects; we hold its connection and never read from it.
    let client_a = ClientProc::spawn(port, "slow-consumer-a");
    let mut conn_a = accepts.accept().await.expect("incoming a").accept();

    // Client B connects; echo it normally.
    let mut client_b = ClientProc::spawn(port, "slow-consumer-b");
    let conn_b = accepts.accept().await.expect("incoming b").accept();
    let echo_b = tokio::spawn(echo_until_closed(conn_b));

    // While A's events sit unread, B must complete its full roundtrip.
    let status_b = tokio::time::timeout(Duration::from_secs(10), client_b.wait())
        .await
        .expect("client b finish");
    assert!(
        status_b.success(),
        "client b (fast consumer) failed: {status_b}"
    );

    // The pump is alive and responsive.
    endpoint
        .with_host(|_| ())
        .await
        .expect("with_host while consumer blocked");

    // A's events were buffered, not dropped: drain them now.
    let ev = tokio::time::timeout(Duration::from_secs(5), conn_a.recv())
        .await
        .expect("drain conn a")
        .expect("buffered event");
    match ev {
        ConnEvent::Receive { payload, .. } => {
            assert_eq!(payload, Bytes::from_static(b"slow-consumer-a"));
        }
        other => panic!("expected buffered receive, got {other:?}"),
    }

    drop(conn_a);
    drop(client_a); // kill it; its echo was deliberately withheld
    echo_b.abort();
}

/// With `accept_channel(1)`: the queued incoming still completes, but the
/// newcomer that finds the queue full is rejected immediately (graceful
/// disconnect) instead of stalling the pump.
#[tokio::test]
async fn accept_overflow_rejects_newcomer() {
    let port = free_port();
    let bind = SocketAddr::from(([127, 0, 0, 1], port));
    let endpoint = Endpoint::start(LenetConfig::new().bind_addr(bind).accept_channel(1))
        .await
        .expect("endpoint");
    let mut accepts = endpoint.listen().await.expect("listen");

    // A connects and is accepted right away: queue empty again.
    let mut client_a = ClientProc::spawn(port, "overflow-a");
    let mut conn_a = accepts.accept().await.expect("incoming a").accept();

    // B and C connect concurrently. Whichever dispatches first takes the
    // single queue slot; the other must be rejected by the driver.
    let mut client_b = ClientProc::spawn(port, "overflow-b");
    let mut client_c = ClientProc::spawn(port, "overflow-c");

    // Echo A so it completes.
    match tokio::time::timeout(Duration::from_secs(5), conn_a.recv())
        .await
        .expect("conn a event")
        .expect("event")
    {
        ConnEvent::Receive {
            channel_id,
            payload,
        } => {
            conn_a
                .send(channel_id, SendFlags::RELIABLE, payload)
                .await
                .expect("echo a");
        }
        other => panic!("expected receive, got {other:?}"),
    }

    // Exactly one of B/C lands in the accept queue; echo it to completion.
    let incoming = tokio::time::timeout(Duration::from_secs(10), accepts.accept())
        .await
        .expect("one of b/c accepted")
        .expect("incoming");
    let conn_bc = incoming.accept();
    tokio::spawn(echo_until_closed(conn_bc));

    let status_b = tokio::time::timeout(Duration::from_secs(15), client_b.wait())
        .await
        .expect("client b finish");
    let status_c = tokio::time::timeout(Duration::from_secs(15), client_c.wait())
        .await
        .expect("client c finish");

    // A succeeded; exactly one of B/C was rejected by the driver.
    let status_a = tokio::time::timeout(Duration::from_secs(15), client_a.wait())
        .await
        .expect("client a finish");
    assert!(status_a.success(), "client a failed: {status_a}");
    assert_ne!(
        status_b.success(),
        status_c.success(),
        "exactly one of b/c must be rejected (b: {status_b}, c: {status_c})"
    );
    drop(endpoint);
}

/// A connect attempt to an unresponsive address resolves `TimedOut` within
/// the configured peer timeout instead of hanging forever.
#[tokio::test]
async fn connect_timeout_resolves_timed_out() {
    let endpoint = Endpoint::start(LenetConfig::new().peer_timeout(2, 200, 400))
        .await
        .expect("endpoint");

    // A loopback port nothing listens on: datagrams are silently dropped,
    // so the attempt can only end in a timeout.
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let connecting = endpoint.connect(dead, 1, 0).expect("connect queued");
    let result = tokio::time::timeout(Duration::from_secs(5), connecting)
        .await
        .expect("timeout resolution within 5s");
    assert!(
        matches!(result, Err(ConnectError::TimedOut)),
        "expected TimedOut, got {result:?}"
    );
}

/// `Endpoint::shutdown` flushes and stops the driver; handles then fail
/// closed and queues close.
#[tokio::test]
async fn shutdown_closes_everything() {
    let helper = Helper::start();
    let endpoint = Endpoint::start(LenetConfig::new()).await.expect("endpoint");
    let mut accepts = endpoint.listen().await.expect("listen");
    let mut conn = handshake(&endpoint, helper.addr()).await;

    endpoint.shutdown().await.expect("shutdown");

    assert!(
        conn.recv().await.is_none(),
        "queue must close after shutdown"
    );
    assert!(
        matches!(
            conn.send(0, SendFlags::RELIABLE, b"x".to_vec()).await,
            Err(SendError::Closed)
        ),
        "send must fail closed after shutdown"
    );
    assert!(matches!(
        endpoint.with_host(|_| ()).await,
        Err(SendError::Closed)
    ));
    assert!(accepts.accept().await.is_none(), "accept queue must close");

    drop(conn);
    drop(helper);
}

/// `Connection::split`: one task reads while two sender clones write
/// concurrently; the peer is disconnected only once every half is gone.
#[tokio::test]
async fn split_halves_full_duplex() {
    let mut helper = Helper::start();
    let endpoint = Endpoint::start(LenetConfig::new()).await.expect("endpoint");
    let conn = handshake(&endpoint, helper.addr()).await;
    let (tx, mut rx) = conn.split();

    // Two tasks send concurrently through cloned senders while this task
    // reads — no shared lock that a blocking recv could deadlock.
    let sender_a = tokio::spawn({
        let tx = tx.clone();
        async move {
            for i in 0..5 {
                tx.send(0, SendFlags::RELIABLE, format!("a{i}").into_bytes())
                    .await
                    .expect("sender a");
            }
        }
    });
    let sender_b = tokio::spawn({
        let tx = tx.clone();
        async move {
            for i in 0..5 {
                tx.send(0, SendFlags::RELIABLE, format!("b{i}").into_bytes())
                    .await
                    .expect("sender b");
            }
        }
    });

    let mut echoed = Vec::new();
    for _ in 0..10 {
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("echo within timeout")
            .expect("event");
        match ev {
            ConnEvent::Receive { payload, .. } => {
                echoed.push(String::from_utf8(payload.to_vec()).expect("utf8"));
            }
            other => panic!("expected echo, got {other:?}"),
        }
    }
    sender_a.await.expect("sender a task");
    sender_b.await.expect("sender b task");
    for i in 0..5 {
        assert!(echoed.contains(&format!("a{i}")), "missing echo a{i}");
        assert!(echoed.contains(&format!("b{i}")), "missing echo b{i}");
    }

    // Dropping both halves disconnects the peer: the helper's echo server
    // exits after its DISCONNECT.
    drop(tx);
    drop(rx);
    let status = helper.child.wait().expect("wait helper");
    assert!(status.success(), "helper exited with {status}");
}

/// The accept queue keeps the driver alive on its own: a server that only
/// holds the queue (endpoint dropped) still accepts and serves connections.
#[tokio::test]
async fn accept_queue_keeps_driver_alive_after_endpoint_drop() {
    let port = free_port();
    let bind = SocketAddr::from(([127, 0, 0, 1], port));
    let endpoint = Endpoint::start(LenetConfig::new().bind_addr(bind))
        .await
        .expect("endpoint");
    let mut accepts = endpoint.listen().await.expect("listen");
    drop(endpoint);

    // The helper client connects, is echoed, and disconnects with data 42;
    // a full roundtrip proves the driver is alive with no Endpoint handle.
    // The helper client exits 0 only after its echo arrives, so accept and
    // echo while it runs.
    let mut client = ClientProc::spawn(port, "still-listening");

    let incoming = accepts
        .accept()
        .await
        .expect("incoming after endpoint drop");
    let mut conn = incoming.accept();
    let ev = tokio::time::timeout(Duration::from_secs(5), conn.recv())
        .await
        .expect("event within timeout")
        .expect("buffered event");
    match ev {
        ConnEvent::Receive {
            channel_id,
            payload,
        } => {
            assert_eq!(payload, Bytes::from_static(b"still-listening"));
            conn.send(channel_id, SendFlags::RELIABLE, payload)
                .await
                .expect("echo send");
        }
        other => panic!("expected receive, got {other:?}"),
    }

    let status = tokio::time::timeout(Duration::from_secs(10), client.wait())
        .await
        .expect("helper client finish");
    assert!(status.success(), "helper client failed: {status}");
}
