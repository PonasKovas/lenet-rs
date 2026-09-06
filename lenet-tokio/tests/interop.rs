//! Async interop test: lenet-tokio (Rust) vs real ENet (C, via the
//! `enet-helper` binary). This is the Phase 4 exit criterion.
//!
//! Requires the `enet-helper` binary (built by `cargo test --workspace`);
//! override its location with the `ENET_HELPER` env var.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::process::Command as AsyncCommand;

use lenet::{EventKind, SendFlags};
use lenet_tokio::{Endpoint, LenetConfig};

fn helper_path() -> PathBuf {
    if let Ok(p) = std::env::var("ENET_HELPER") {
        return PathBuf::from(p);
    }
    // Fallback: workspace target dir, two levels up from this test file.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target")
        .join("debug")
        .join("enet-helper")
}

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
            .expect("spawn enet-helper (build with `cargo test --workspace`)");
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

/// Waits for the next event matching `pred`, returning it.
async fn wait_event(
    events: &mut lenet_tokio::EventReceiver,
    timeout: Duration,
    what: &str,
) -> lenet_tokio::DrivenEvent {
    match tokio::time::timeout(timeout, events.recv()).await {
        Ok(Some(ev)) => ev,
        Ok(None) => panic!("event stream closed while waiting for {what}"),
        Err(_) => panic!("timed out waiting for {what}"),
    }
}

/// Client test flow: connect, wait for the handshake (CONNECT event), then
/// send; collect CONNECT + RECEIVE(echo).
#[tokio::test]
async fn connect_and_reliable_echo() {
    let mut helper = Helper::start();
    let (_endpoint, conn, mut events) = {
        let (endpoint, events) = Endpoint::start(LenetConfig::new()).await.expect("endpoint");
        let conn = endpoint.connect(helper.addr(), 2, 0x1234).await.expect("connect");
        (endpoint, conn, events)
    };

    // Handshake must complete before the peer is sendable.
    let ev = wait_event(&mut events, Duration::from_secs(5), "CONNECT").await;
    assert_eq!(ev.kind, EventKind::Connect);
    assert_eq!(ev.data, 0x1234, "connect data mismatch");

    conn.send(0, SendFlags::RELIABLE, b"ping-from-rust".to_vec())
        .await
        .expect("send");

    // The helper echoes reliably, with the exact bytes.
    let ev = wait_event(&mut events, Duration::from_secs(5), "echo").await;
    assert_eq!(ev.kind, EventKind::Receive, "expected echo");
    assert_eq!(ev.payload, b"ping-from-rust", "echo bytes mismatch");

    conn.disconnect(42).await;
    // The helper exits 0 after its DISCONNECT; drain our side briefly.
    let _ = tokio::time::timeout(Duration::from_millis(500), events.recv()).await;

    let status = helper.child.wait().expect("wait helper");
    assert!(status.success(), "helper exited with {status}");
}

#[tokio::test]
async fn fragmented_echo() {
    let helper = Helper::start();
    let (_endpoint, conn, mut events) = {
        let (endpoint, events) = Endpoint::start(LenetConfig::new()).await.expect("endpoint");
        let conn = endpoint.connect(helper.addr(), 2, 0x1234).await.expect("connect");
        (endpoint, conn, events)
    };

    let ev = wait_event(&mut events, Duration::from_secs(5), "CONNECT").await;
    assert_eq!(ev.kind, EventKind::Connect);

    // 40 KB reliable: needs fragmentation on the wire (MTU ~1392).
    let big: Vec<u8> = (0..40_000u32).map(|i| (i * 7 + (i >> 8)) as u8).collect();
    conn.send(0, SendFlags::RELIABLE, big.clone()).await.expect("send");

    // The helper echoes the full payload reliably; compare bytes exactly.
    let ev = wait_event(&mut events, Duration::from_secs(10), "fragmented echo").await;
    assert_eq!(ev.kind, EventKind::Receive, "expected echo");
    assert_eq!(ev.payload, big, "fragmented echo bytes mismatch");

    conn.disconnect(0).await;
    let _ = tokio::time::timeout(Duration::from_millis(500), events.recv()).await;
}

#[tokio::test]
async fn server_role_roundtrip() {
    // lenet-tokio as the SERVER: fixed port, helper in client mode. The
    // helper drives connect -> send -> (expects echo) -> disconnect(42);
    // the Rust side echoes via its event stream and verifies the same
    // story from its end.
    let port = free_port();
    let bind = SocketAddr::from(([127, 0, 0, 1], port));
    let (endpoint, mut events) = Endpoint::start(LenetConfig::new().bind_addr(bind))
        .await
        .expect("endpoint");

    // Echo task: every RECEIVE is sent back on the same channel.
    let echo_ep = endpoint.clone();
    let echo_task = tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            match ev.kind {
                EventKind::Receive => {
                    echo_ep
                        .peer(ev.peer_id)
                        .send(ev.channel_id, SendFlags::RELIABLE, ev.payload)
                        .await
                        .expect("echo send");
                }
                EventKind::Disconnect => break,
                _ => {}
            }
        }
    });

    // Run the helper client without blocking the (single-threaded) test
    // runtime: the server pump task must keep running.
    let status = AsyncCommand::new(helper_path())
        .args(["client", "127.0.0.1", &port.to_string(), "hello-server"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .status()
        .await
        .expect("spawn helper client");
    assert!(status.success(), "helper client failed: {status}");

    // The helper's own exit status already proves the roundtrip
    // (connect, echo bytes, disconnect); the echo task proves the Rust
    // side saw RECEIVE and could reply.
    echo_task.abort();
}
