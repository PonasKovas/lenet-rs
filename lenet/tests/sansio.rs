//! Unit tests: two sans-I/O hosts connected to each other, driven manually
//! (datagrams routed by hand, time advanced by hand).

use std::net::Ipv4Addr;

use lenet::{EventKind, HostBuilder, SendFlags};

/// Routes every outgoing datagram of `from` into `to`.
fn route(from: &mut lenet::Host, to: &mut lenet::Host) -> usize {
    let mut sent = 0;
    while let Some(d) = from.poll_outgoing() {
        to.handle_datagram(1000, d.ip, d.port, &d.data);
        sent += 1;
    }
    sent
}

/// One pump round: service both hosts (outgoing datagrams materialize on
/// service, ACKs included), then relay both ways. Returns true if anything
/// moved.
fn pump(a: &mut lenet::Host, b: &mut lenet::Host) -> bool {
    a.service(1000);
    b.service(1000);
    let x = route(a, b);
    let y = route(b, a);
    (x + y) > 0
}

/// Pumps until quiescent.
fn settle(a: &mut lenet::Host, b: &mut lenet::Host) {
    for _ in 0..100 {
        if !pump(a, b) {
            return;
        }
    }
    panic!("hosts never quiesced");
}

/// Returns (host_a, host_b, peer id at a, peer id at b, connect data as
/// seen by a's CONNECT event).
#[allow(clippy::type_complexity)]
fn connect_pair() -> (lenet::Host, lenet::Host, u16, u16, u32) {
    let mut a = HostBuilder::new().peer_count(4).channel_limit(2).build().unwrap();
    let mut b = HostBuilder::new().peer_count(4).channel_limit(2).build().unwrap();
    let pa = a.connect(Ipv4Addr::LOCALHOST, 1, 2, 0x77).expect("connect");
    settle(&mut a, &mut b);
    // Drain the connect events on both sides.
    let mut pa_ev = None;
    let mut connect_data = None;
    let mut pb_ev = None;
    while let Some(ev) = a.poll_event(&mut []) {
        if ev.event.kind == EventKind::Connect {
            pa_ev = Some(ev.event.peer_id);
            connect_data = Some(ev.event.data);
        }
    }
    while let Some(ev) = b.poll_event(&mut []) {
        if ev.event.kind == EventKind::Connect {
            pb_ev = Some(ev.event.peer_id);
        }
    }
    assert_eq!(pa_ev, Some(pa));
    (a, b, pa, pb_ev.expect("b should have seen a connect"), connect_data.expect("connect data"))
}

#[test]
fn handshake_and_connect_data() {
    let (mut a, mut b, pa, pb, connect_data) = connect_pair();
    assert_eq!(pa, 0);
    assert_eq!(pb, 0);
    // The connect payload (0x77) surfaces on both sides' CONNECT events.
    assert_eq!(connect_data, 0x77);
    // Both hosts are idle-waiting: no more events.
    let mut buf = [0u8; 4];
    assert!(a.poll_event(&mut buf).is_none());
    assert!(b.poll_event(&mut buf).is_none());
}

#[test]
fn reliable_roundtrip() {
    let (mut a, mut b, pa, pb, _) = connect_pair();
    a.send(pa, 0, SendFlags::RELIABLE, b"hello").expect("send");
    settle(&mut a, &mut b);
    let mut buf = [0u8; 16];
    let ev = b.poll_event(&mut buf).expect("receive");
    assert_eq!(ev.event.kind, EventKind::Receive);
    assert_eq!(ev.event.channel_id, 0);
    assert_eq!(ev.payload_len, 5);
    assert_eq!(&buf[..5], b"hello");

    // And back the other way.
    b.send(pb, 1, SendFlags::RELIABLE, b"pong").expect("send");
    settle(&mut a, &mut b);
    let ev = a.poll_event(&mut buf).expect("receive");
    assert_eq!(ev.event.channel_id, 1);
    assert_eq!(&buf[..4], b"pong");
}

#[test]
fn large_packet_fragments_and_reassembles() {
    let (mut a, mut b, pa, _pb, _) = connect_pair();
    let big: Vec<u8> = (0..40_000u32).map(|i| (i * 7 + (i >> 8)) as u8).collect();
    a.send(pa, 0, SendFlags::RELIABLE, &big).expect("send");
    // Relay until b has the whole thing (fragmented, many datagrams).
    let mut buf = vec![0u8; big.len()];
    for _ in 0..100 {
        pump(&mut a, &mut b);
        if let Some(ev) = b.poll_event(&mut buf) {
            assert_eq!(ev.payload_len, big.len());
            assert_eq!(&buf[..], &big[..]);
            return;
        }
    }
    panic!("fragmented packet never delivered");
}

#[test]
fn graceful_disconnect() {
    let (mut a, mut b, pa, _pb, _) = connect_pair();
    a.disconnect(pa, 42);
    settle(&mut a, &mut b);
    let mut buf = [0u8; 4];
    let ev = b.poll_event(&mut buf).expect("disconnect event");
    assert_eq!(ev.event.kind, EventKind::Disconnect);
    assert_eq!(ev.event.data, 42);
    // a (the initiator) sees its own disconnect with data 0 (ENet parity).
    let ev = a.poll_event(&mut buf).expect("own disconnect event");
    assert_eq!(ev.event.kind, EventKind::Disconnect);
    assert_eq!(ev.event.data, 0);
}

#[test]
fn service_is_idle_stable() {
    let (mut a, mut b, _pa, _pb, _) = connect_pair();
    // Servicing a connected idle pair produces no events, no datagrams
    // (well before the keepalive ping boundary at ~500 ms after the
    // handshake at t=1000).
    a.service(1100);
    b.service(1100);
    assert!(a.poll_outgoing().is_none());
    assert!(b.poll_outgoing().is_none());
    let mut buf = [0u8; 4];
    assert!(a.poll_event(&mut buf).is_none());
    assert!(b.poll_event(&mut buf).is_none());
}

#[test]
fn keepalive_deadline_appears_when_connected() {
    let (a, _b, _pa, _pb, _) = connect_pair();
    // A connected, fully drained peer has a keepalive deadline scheduled.
    assert!(a.next_deadline().is_some());
}
