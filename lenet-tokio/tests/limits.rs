//! Driver-level send limits between two `lenet-tokio` endpoints over
//! loopback UDP: non-blocking sends never fail for lack of room, a peer
//! holding too much outgoing data is disconnected, and a rejected reliable
//! packet ends the connection.

use std::net::SocketAddr;
use std::time::Duration;

use lenet::SendFlags;
use lenet_tokio::{ConnEvent, Connection, Endpoint, LenetConfig};

/// Starts a server endpoint with `config`, connects a default client to
/// it, and returns (server endpoint, server-side connection, client
/// connection). The endpoints are returned so they outlive the test body.
async fn pair(config: LenetConfig) -> (Endpoint, Endpoint, Connection, Connection) {
    let server = Endpoint::start(config.bind_addr("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("server endpoint");
    let mut accepts = server.listen().await.expect("listen");
    let SocketAddr::V4(server_addr) = server.local_addr() else {
        panic!("IPv4 bind");
    };
    let client = Endpoint::start(LenetConfig::new())
        .await
        .expect("client endpoint");
    let connecting = client.connect(server_addr, 2, 0).expect("connect queued");
    let (client_conn, incoming) = tokio::join!(connecting, accepts.accept());
    let server_conn = incoming.expect("incoming").accept();
    (server, client, server_conn, client_conn.expect("handshake"))
}

/// Receives events on `conn` until it reports a disconnect; returns the
/// disconnect data and the received payloads.
async fn recv_until_disconnect(conn: &mut Connection) -> (u32, Vec<Vec<u8>>) {
    let mut received = Vec::new();
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(10), conn.recv())
            .await
            .expect("event within timeout")
            .expect("event");
        match ev {
            ConnEvent::Receive { payload, .. } => received.push(payload.to_vec()),
            ConnEvent::Disconnect { data } => return (data, received),
        }
    }
}

/// `try_send` bypasses the async backpressure: with room for a single
/// async command, thousands of non-blocking sends all queue, in order.
#[tokio::test]
async fn try_send_never_fails_for_lack_of_room() {
    let (_server, _client, server_conn, mut client_conn) =
        pair(LenetConfig::new().cmd_channel(1)).await;
    let (tx, _rx) = server_conn.split();

    const N: u32 = 2000;
    for i in 0..N {
        tx.try_send(0, SendFlags::RELIABLE, i.to_le_bytes().to_vec())
            .expect("try_send");
    }
    for i in 0..N {
        let ev = tokio::time::timeout(Duration::from_secs(10), client_conn.recv())
            .await
            .expect("packet within timeout")
            .expect("event");
        match ev {
            ConnEvent::Receive { payload, .. } => assert_eq!(&payload[..], &i.to_le_bytes()),
            other => panic!("expected packet {i}, got {other:?}"),
        }
    }
}

/// A peer holding more outgoing payload than the limit is disconnected
/// with the configured data.
#[tokio::test]
async fn outgoing_limit_disconnects_peer() {
    let (_server, _client, server_conn, mut client_conn) =
        pair(LenetConfig::new().peer_outgoing_limit(10_000, 42)).await;
    let (tx, _rx) = server_conn.split();

    // Queued in one go, before the pump runs: 50 KB unacknowledged.
    for _ in 0..50 {
        tx.try_send(0, SendFlags::RELIABLE, vec![0u8; 1000])
            .expect("try_send");
    }
    let (data, _) = recv_until_disconnect(&mut client_conn).await;
    assert_eq!(data, 42);
}

/// Below the limit nothing happens.
#[tokio::test]
async fn outgoing_limit_spares_peer_below_it() {
    let (_server, _client, server_conn, mut client_conn) =
        pair(LenetConfig::new().peer_outgoing_limit(100_000, 42)).await;
    for _ in 0..50 {
        server_conn
            .send(0, SendFlags::RELIABLE, vec![0u8; 1000])
            .await
            .expect("send");
    }
    for _ in 0..50 {
        let ev = tokio::time::timeout(Duration::from_secs(10), client_conn.recv())
            .await
            .expect("packet within timeout")
            .expect("event");
        assert!(matches!(ev, ConnEvent::Receive { .. }), "got {ev:?}");
    }
}

/// The engine rejecting a reliable packet (here: a channel beyond the
/// negotiated two) ends the connection rather than losing it silently.
#[tokio::test]
async fn rejected_reliable_send_disconnects() {
    let (_server, _client, server_conn, mut client_conn) = pair(LenetConfig::new()).await;
    server_conn
        .send(0, SendFlags::RELIABLE, b"before".to_vec())
        .await
        .expect("send");
    server_conn
        .send(5, SendFlags::RELIABLE, b"bad channel".to_vec())
        .await
        .expect("queued");
    let (data, received) = recv_until_disconnect(&mut client_conn).await;
    assert_eq!(data, 0);
    assert!(!received.contains(&b"bad channel".to_vec()));
}

/// A rejected unreliable packet is only dropped.
#[tokio::test]
async fn rejected_unreliable_send_is_dropped() {
    let (_server, _client, server_conn, mut client_conn) = pair(LenetConfig::new()).await;
    server_conn
        .send(5, SendFlags::UNRELIABLE, b"bad channel".to_vec())
        .await
        .expect("queued");
    server_conn
        .send(0, SendFlags::RELIABLE, b"after".to_vec())
        .await
        .expect("send");
    let ev = tokio::time::timeout(Duration::from_secs(10), client_conn.recv())
        .await
        .expect("packet within timeout")
        .expect("event");
    match ev {
        ConnEvent::Receive { payload, .. } => assert_eq!(&payload[..], b"after"),
        other => panic!("expected packet, got {other:?}"),
    }
}
