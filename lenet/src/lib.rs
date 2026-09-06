//! Safe sans-I/O Rust API over the Lenet C distribution.
//!
//! Lenet is a sans-I/O implementation of the ENet protocol: it never opens
//! sockets or reads clocks. This crate owns the raw host handle and exposes
//! a safe, panic-free API; every call is non-blocking and time is supplied
//! by the caller as milliseconds (`u32`, may wrap).
//!
//! Threading: a [`Host`] is NOT `Sync`. Drive it from a single thread (or
//! serialize access yourself) — see `lenet-tokio` for a ready-made driver
//! that does exactly that.
//!
//! # Example
//!
//! ```no_run
//! use lenet::{HostBuilder, SendFlags};
//!
//! let mut host = HostBuilder::new()
//!     .peer_count(16)
//!     .channel_limit(2)
//!     .build()
//!     .expect("host");
//! let peer = host.connect("127.0.0.1".parse().unwrap(), 40011, 2, 0x1234).expect("connect");
//! host.service(0);
//! host.send(peer, 0, SendFlags::RELIABLE, b"hello").expect("send");
//! while let Some(dgram) = host.poll_outgoing() { /* transmit via UDP */ }
//! let mut payload = [0u8; 64];
//! while let Some(event) = host.poll_event(&mut payload) { /* handle */ }
//! ```

#![deny(unsafe_op_in_unsafe_fn)]

use std::net::Ipv4Addr;

pub use lenet_sys as ffi;

/// Packet delivery modes (bitmask, ENet-compatible).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SendFlags(pub u32);

impl SendFlags {
    pub const UNRELIABLE: SendFlags = SendFlags(ffi::LENET_UNRELIABLE);
    pub const RELIABLE: SendFlags = SendFlags(ffi::LENET_RELIABLE);
    pub const UNSEQUENCED: SendFlags = SendFlags(ffi::LENET_UNSEQUENCED);
    pub const UNRELIABLE_FRAGMENT: SendFlags = SendFlags(ffi::LENET_UNRELIABLE_FRAGMENT);

    #[must_use]
    pub const fn union(self, other: SendFlags) -> SendFlags {
        SendFlags(self.0 | other.0)
    }
}

/// Application event. For [`EventKind::Receive`] the payload is delivered
/// through the buffer passed to [`Host::poll_event`].
#[derive(Clone, Debug)]
pub struct Event {
    pub kind: EventKind,
    pub peer_id: u16,
    pub channel_id: u8,
    pub data: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    Connect,
    Disconnect,
    Receive,
}

/// An outgoing datagram to transmit over the driver's UDP socket.
#[derive(Clone, Debug)]
pub struct Datagram {
    pub ip: u32,  // network byte order
    pub port: u16, // host byte order
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct SendError;

#[derive(Debug)]
pub struct ConnectError;

/// Builder for a [`Host`].
///
/// All fields are optional with Lenet defaults: unbound socket (informational
/// only — the driver owns the socket), 16 peer slots, 1 channel, unlimited
/// bandwidth, default MTU (1392, clamped to [576, 4096]).
#[derive(Clone, Debug)]
pub struct HostBuilder {
    bind_ip: u32,
    bind_port: u16,
    peer_count: usize,
    channel_limit: usize,
    incoming_bw: u32,
    outgoing_bw: u32,
    mtu: u32,
    checksum: bool,
}

impl Default for HostBuilder {
    fn default() -> Self {
        Self {
            bind_ip: 0,
            bind_port: 0,
            peer_count: 16,
            channel_limit: 1,
            incoming_bw: 0,
            outgoing_bw: 0,
            mtu: 0,
            checksum: false,
        }
    }
}

impl HostBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Informational bind address (the socket belongs to the driver).
    #[must_use]
    pub fn bind(mut self, ip: Ipv4Addr, port: u16) -> Self {
        self.bind_ip = u32::from(ip).to_be();
        self.bind_port = port;
        self
    }

    #[must_use]
    pub fn peer_count(mut self, n: usize) -> Self {
        self.peer_count = n.max(1);
        self
    }

    #[must_use]
    pub fn channel_limit(mut self, n: usize) -> Self {
        self.channel_limit = n.max(1);
        self
    }

    /// Bandwidth limits in bytes/second, 0 = unlimited.
    #[must_use]
    pub fn bandwidth(mut self, incoming: u32, outgoing: u32) -> Self {
        self.incoming_bw = incoming;
        self.outgoing_bw = outgoing;
        self
    }

    /// MTU in bytes; clamped by Lenet to [576, 4096]; 0 = default 1392.
    #[must_use]
    pub fn mtu(mut self, mtu: u32) -> Self {
        self.mtu = mtu;
        self
    }

    /// Enables CRC32 checksums (must be enabled on both ends, like ENet).
    #[must_use]
    pub fn checksum(mut self, enabled: bool) -> Self {
        self.checksum = enabled;
        self
    }

    #[must_use]
    pub fn build(self) -> Option<Host> {
        let raw = unsafe {
            ffi::lenet_host_create(
                self.bind_ip,
                self.bind_port,
                self.peer_count,
                self.channel_limit,
                self.incoming_bw,
                self.outgoing_bw,
                self.mtu,
            )
        };
        if raw.is_null() {
            return None;
        }
        let host = Host { raw };
        if self.checksum {
            unsafe { ffi::lenet_host_enable_checksum(host.raw) };
        }
        Some(host)
    }
}

/// A sans-I/O protocol host. Owns the raw handle; non-blocking; time is
/// supplied by the caller.
///
/// Not `Sync`: the threading contract is one host, one thread (or external
/// serialization).
pub struct Host {
    raw: *mut ffi::lenet_host,
}

// The C contract: one host, one thread. A Host may move between threads
// (Send) but never be shared concurrently (no Sync).
unsafe impl Send for Host {}

impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host").finish_non_exhaustive()
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        unsafe { ffi::lenet_host_destroy(self.raw) };
    }
}

/// Result of [`Host::poll_event`]: the event plus the number of payload
/// bytes written into the caller's buffer (the full payload length if it
/// was truncated).
pub struct PolledEvent {
    pub event: Event,
    pub payload_len: usize,
    pub truncated: bool,
}

impl Host {
    /// Starts a connection attempt to `ip:port` with `channel_count`
    /// channels (1..=255) and `data` as the connect payload.
    /// Returns the local peer id, or [`ConnectError`] if no slot is free.
    pub fn connect(
        &mut self,
        ip: Ipv4Addr,
        port: u16,
        channel_count: usize,
        data: u32,
    ) -> Result<u16, ConnectError> {
        let r = unsafe {
            ffi::lenet_host_connect(self.raw, u32::from(ip).to_be(), port, channel_count, data)
        };
        if r < 0 {
            Err(ConnectError)
        } else {
            Ok(r as u16)
        }
    }

    /// Queues a packet for transmission on `channel`.
    pub fn send(
        &mut self,
        peer_id: u16,
        channel: u8,
        flags: SendFlags,
        data: &[u8],
    ) -> Result<(), SendError> {
        let r = unsafe {
            ffi::lenet_host_send(self.raw, peer_id, channel, flags.0, data.as_ptr().cast(), data.len())
        };
        if r < 0 {
            Err(SendError)
        } else {
            Ok(())
        }
    }

    /// Queues a packet on `channel` of every connected peer.
    pub fn broadcast(&mut self, channel: u8, flags: SendFlags, data: &[u8]) {
        unsafe {
            ffi::lenet_host_broadcast(self.raw, channel, flags.0, data.as_ptr().cast(), data.len())
        };
    }

    /// Begins a graceful disconnect of `peer_id`.
    pub fn disconnect(&mut self, peer_id: u16, data: u32) {
        unsafe { ffi::lenet_host_disconnect(self.raw, peer_id, data) };
    }

    /// ENet's `enet_peer_disconnect_later`: flushes queued packets, then
    /// disconnects. Degrades to [`Host::disconnect`] when nothing is pending.
    pub fn disconnect_later(&mut self, peer_id: u16, data: u32) {
        unsafe { ffi::lenet_host_disconnect_later(self.raw, peer_id, data) };
    }

    /// ENet's `enet_peer_throttle_configure`: sets the local throttle
    /// parameters and informs the remote peer.
    pub fn throttle_configure(
        &mut self,
        peer_id: u16,
        interval: u32,
        acceleration: u32,
        deceleration: u32,
    ) {
        unsafe {
            ffi::lenet_peer_throttle_configure(self.raw, peer_id, interval, acceleration, deceleration)
        };
    }

    /// Configures timeout behavior for `peer_id` (see `enet_peer_timeout`).
    pub fn set_timeout(&mut self, peer_id: u16, limit: u32, minimum: u32, maximum: u32) {
        unsafe { ffi::lenet_peer_set_timeout(self.raw, peer_id, limit, minimum, maximum) };
    }

    /// Feeds one received UDP datagram into the protocol engine.
    /// `ip` in network byte order, `port` in host byte order.
    pub fn handle_datagram(&mut self, now_ms: u32, ip: u32, port: u16, data: &[u8]) {
        unsafe {
            ffi::lenet_host_handle_datagram(self.raw, now_ms, ip, port, data.as_ptr().cast(), data.len())
        };
    }

    /// Runs the connection's timers: retransmissions, timeouts, keepalive
    /// pings, and the periodic bandwidth recalculation. Call regularly
    /// (the tokio driver does this automatically at `next_deadline`).
    pub fn service(&mut self, now_ms: u32) {
        unsafe { ffi::lenet_host_service(self.raw, now_ms) };
    }

    /// The next wall-clock deadline (ms, same clock as [`Host::service`])
    /// at which the host's state can change, or `None` if nothing is
    /// scheduled. Timestamps may wrap; compare wrap-aware.
    pub fn next_deadline(&self) -> Option<u32> {
        let mut d: u32 = 0;
        let r = unsafe { ffi::lenet_host_next_deadline(self.raw, &mut d) };
        if r == 1 {
            Some(d)
        } else {
            None
        }
    }

    /// Pops one outgoing datagram to transmit over the driver's socket.
    pub fn poll_outgoing(&mut self) -> Option<Datagram> {
        let mut out: ffi::lenet_datagram = ffi::lenet_datagram {
            ip: 0,
            port: 0,
            data: std::ptr::null(),
            len: 0,
        };
        let r = unsafe { ffi::lenet_host_poll_outgoing(self.raw, &mut out) };
        if r != 1 {
            return None;
        }
        let bytes =
            unsafe { std::slice::from_raw_parts(out.data.cast::<u8>(), out.len) }.to_vec();
        Some(Datagram {
            ip: out.ip,
            port: out.port,
            data: bytes,
        })
    }

    /// Pops one application event into `payload_buf`.
    /// Returns `None` if no event is pending. If the payload exceeds
    /// `payload_buf.len()`, `PolledEvent::payload_len` holds the full length
    /// and `truncated` is set.
    pub fn poll_event(&mut self, payload_buf: &mut [u8]) -> Option<PolledEvent> {
        let mut raw: ffi::lenet_event = ffi::lenet_event {
            type_: 0,
            peer_id: 0,
            channel_id: 0,
            data: 0,
        };
        let mut payload_len: usize = 0;
        let r = unsafe {
            ffi::lenet_host_poll_event(
                self.raw,
                &mut raw,
                payload_buf.as_mut_ptr().cast(),
                payload_buf.len(),
                &mut payload_len,
            )
        };
        if r != 1 {
            return None;
        }
        let kind = match raw.type_ {
            ffi::LENET_EVENT_CONNECT => EventKind::Connect,
            ffi::LENET_EVENT_DISCONNECT => EventKind::Disconnect,
            ffi::LENET_EVENT_RECEIVE => EventKind::Receive,
            _ => return None,
        };
        Some(PolledEvent {
            event: Event {
                kind,
                peer_id: raw.peer_id,
                channel_id: raw.channel_id,
                data: raw.data,
            },
            payload_len,
            truncated: payload_len > payload_buf.len(),
        })
    }
}
