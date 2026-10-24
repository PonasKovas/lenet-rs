//! Unit tests: two sans-I/O hosts connected to each other, driven manually
//! (datagrams routed by hand, time advanced by hand).

use std::net::{Ipv4Addr, SocketAddrV4};

use lenet::{Event, HostBuilder, PeerState, SendFlags};

fn addr(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)
}

/// Routes every outgoing datagram of `from` into `to`.
fn route(from: &mut lenet::Host, to: &mut lenet::Host) -> usize {
    let mut sent = 0;
    while let Some(d) = from.poll_outgoing() {
        to.handle_datagram(1000, d.addr, d.data);
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

/// Returns (host_a, host_b, peer id at `a`, peer id at `b`, connect data as
/// seen by `b`'s (the accepting side's) CONNECT event).
#[allow(clippy::type_complexity)]
fn connect_pair() -> (lenet::Host, lenet::Host, lenet::PeerId, lenet::PeerId, u32) {
    let mut a = HostBuilder::new()
        .peer_count(4)
        .channel_limit(2)
        .build()
        .unwrap();
    let mut b = HostBuilder::new()
        .peer_count(4)
        .channel_limit(2)
        .build()
        .unwrap();
    let pa = a.connect(addr(1), 2, 0x77).expect("connect");
    settle(&mut a, &mut b);
    // Drain the connect events on both sides.
    let mut pa_ev = None;
    let mut connect_data = None;
    let mut pb_ev = None;
    while let Some(ev) = a.poll_event() {
        if let Event::Connect { peer, data } = ev {
            pa_ev = Some(peer);
            // The connecting side's event carries no payload (ENet parity).
            assert_eq!(data, 0);
        }
    }
    while let Some(ev) = b.poll_event() {
        if let Event::Connect { peer, data } = ev {
            pb_ev = Some(peer);
            connect_data = Some(data);
        }
    }
    assert_eq!(pa_ev, Some(pa));
    (
        a,
        b,
        pa,
        pb_ev.expect("b should have seen a connect"),
        connect_data.expect("connect data"),
    )
}

#[test]
fn handshake_and_connect_data() {
    let (mut a, mut b, pa, pb, connect_data) = connect_pair();
    assert_eq!(pa.raw(), 0);
    assert_eq!(pb.raw(), 0);
    // The connect payload (0x77) surfaces on the accepting side.
    assert_eq!(connect_data, 0x77);
    // Both hosts are idle-waiting: no more events.
    assert!(a.poll_event().is_none());
    assert!(b.poll_event().is_none());
}

#[test]
fn reliable_roundtrip() {
    let (mut a, mut b, pa, pb, _) = connect_pair();
    a.send(pa, 0, SendFlags::RELIABLE, b"hello").expect("send");
    settle(&mut a, &mut b);
    match b.poll_event().expect("receive") {
        Event::Receive {
            peer,
            channel_id,
            payload,
        } => {
            assert_eq!(peer, pb);
            assert_eq!(channel_id, 0);
            assert_eq!(payload, b"hello");
        }
        other => panic!("unexpected event: {other:?}"),
    }

    // And back the other way.
    b.send(pb, 1, SendFlags::RELIABLE, b"pong").expect("send");
    settle(&mut a, &mut b);
    match a.poll_event().expect("receive") {
        Event::Receive {
            channel_id,
            payload,
            ..
        } => {
            assert_eq!(channel_id, 1);
            assert_eq!(payload, b"pong");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn large_packet_fragments_and_reassembles() {
    let (mut a, mut b, pa, _pb, _) = connect_pair();
    let big: Vec<u8> = (0..40_000u32).map(|i| (i * 7 + (i >> 8)) as u8).collect();
    a.send(pa, 0, SendFlags::RELIABLE, &big).expect("send");
    // Relay until b has the whole thing (fragmented, many datagrams).
    for _ in 0..100 {
        pump(&mut a, &mut b);
        if let Some(Event::Receive { payload, .. }) = b.poll_event() {
            assert_eq!(payload, &big[..]);
            return;
        }
    }
    panic!("fragmented packet never delivered");
}

/// A payload larger than the old 64 KiB event-buffer default: proves the
/// internal receive buffer handles the full reassembled size without
/// truncation.
#[test]
fn reassembly_beyond_64k_is_never_truncated() {
    let (mut a, mut b, pa, _pb, _) = connect_pair();
    let big: Vec<u8> = (0..100_000u32).map(|i| (i * 13 + (i >> 5)) as u8).collect();
    a.send(pa, 0, SendFlags::RELIABLE, &big).expect("send");
    for _ in 0..300 {
        pump(&mut a, &mut b);
        if let Some(Event::Receive { payload, .. }) = b.poll_event() {
            assert_eq!(payload.len(), big.len(), "payload was truncated");
            assert_eq!(payload, &big[..]);
            return;
        }
    }
    panic!("100 KiB fragmented packet never delivered");
}

/// A payload larger than the receive buffer's starting size and larger
/// than the old fixed 4 MiB buffer: the buffer must grow to fit.
#[test]
fn payload_beyond_4mib_grows_the_buffer() {
    let (mut a, mut b, pa, _pb, _) = connect_pair();
    let big: Vec<u8> = (0..5_000_000u32)
        .map(|i| (i * 31 + (i >> 9)) as u8)
        .collect();
    a.send(pa, 0, SendFlags::RELIABLE, &big).expect("send");
    for _ in 0..20_000 {
        pump(&mut a, &mut b);
        if let Some(Event::Receive { payload, .. }) = b.poll_event() {
            assert_eq!(payload.len(), big.len(), "payload was truncated");
            assert_eq!(payload, &big[..]);
            return;
        }
    }
    panic!("5 MB fragmented packet never delivered");
}

#[test]
fn peer_info_reports_address_and_state() {
    let (a, b, pa, pb, _) = connect_pair();
    let info = a.peer_info(pa).expect("peer info");
    assert_eq!(info.state, PeerState::Connected);
    assert_eq!(info.addr, addr(1));
    assert_eq!(
        b.peer_info(pb).expect("peer info").state,
        PeerState::Connected
    );
    // Out-of-range ids are rejected.
    assert!(a.peer_info(lenet::PeerId::from_raw(999)).is_none());
}

/// ACKs and queued sends become datagrams on `flush`, without `service`.
#[test]
fn flush_emits_queued_datagrams() {
    let (mut a, _b, pa, _pb, _) = connect_pair();
    a.send(pa, 0, SendFlags::RELIABLE, b"x").expect("send");
    assert!(a.poll_outgoing().is_none(), "nothing before flush");
    a.flush(1000);
    assert!(a.poll_outgoing().is_some(), "flush packs the send");
}

#[test]
fn graceful_disconnect() {
    let (mut a, mut b, pa, _pb, _) = connect_pair();
    a.disconnect(pa, 42);
    settle(&mut a, &mut b);
    let ev = b.poll_event().expect("disconnect event");
    assert_eq!(
        ev,
        Event::Disconnect {
            peer: _pb,
            data: 42
        }
    );
    // a (the initiator) sees its own disconnect with data 0 (ENet parity).
    let ev = a.poll_event().expect("own disconnect event");
    assert_eq!(ev, Event::Disconnect { peer: pa, data: 0 });
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
    assert!(a.poll_event().is_none());
    assert!(b.poll_event().is_none());
}

#[test]
fn keepalive_deadline_appears_when_connected() {
    let (a, _b, _pa, _pb, _) = connect_pair();
    // A connected, fully drained peer has a keepalive deadline scheduled.
    assert!(a.next_deadline().is_some());
}

#[test]
fn send_flags_bit_ops() {
    // UNRELIABLE is the empty set: plain sends carry no flag bits.
    assert!(SendFlags::UNRELIABLE.is_empty());
    assert_eq!(SendFlags::UNRELIABLE.bits(), 0);

    let both = SendFlags::RELIABLE | SendFlags::UNSEQUENCED;
    assert!(both.contains(SendFlags::RELIABLE));
    assert!(both.contains(SendFlags::UNSEQUENCED));
    assert!(!both.contains(SendFlags::UNRELIABLE_FRAGMENT));
    // OR-ing with the empty set is a no-op.
    assert_eq!(
        SendFlags::RELIABLE | SendFlags::UNRELIABLE,
        SendFlags::RELIABLE
    );
    // Bits line up with the FFI constants.
    assert_eq!(
        both.bits(),
        lenet::ffi::LENET_RELIABLE | lenet::ffi::LENET_UNSEQUENCED
    );
    // BitAND intersects.
    assert_eq!(both & SendFlags::RELIABLE, SendFlags::RELIABLE);
    // Assign forms work too.
    let mut f = SendFlags::RELIABLE;
    f |= SendFlags::UNRELIABLE_FRAGMENT;
    assert_eq!(f, SendFlags::RELIABLE | SendFlags::UNRELIABLE_FRAGMENT);
}
