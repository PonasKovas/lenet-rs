//! Safe sans-I/O Rust API over the `Lenet` C distribution.
//!
//! `Lenet` is a sans-I/O implementation of the ENet protocol: it never opens
//! sockets or reads clocks. This crate owns the raw host handle and exposes
//! a safe, panic-free API; every call is non-blocking and time is supplied
//! by the caller as milliseconds (`u32`, may wrap).
//!
//! Threading: a [`Host`] is NOT `Sync`. Drive it from a single thread (or
//! serialize access yourself) — see `lenet-tokio` for a ready-made driver
//! that does exactly that.
//!
//! # Borrowing
//!
//! [`Host::poll_event`] and [`Host::poll_outgoing`] return views that borrow
//! the host: handle (or copy) each item before the next call on the same
//! host. This is the standard sans-I/O shape and keeps the core
//! allocation-free on the hot path.
//!
//! # Example
//!
//! ```no_run
//! use lenet::{HostBuilder, PeerId, SendFlags};
//! use std::net::SocketAddrV4;
//!
//! let mut host = HostBuilder::new()
//!     .peer_count(16)
//!     .channel_limit(2)
//!     .build()
//!     .expect("host");
//! let addr: SocketAddrV4 = "127.0.0.1:40011".parse().unwrap();
//! let peer = host.connect(addr, 2, 0x1234).expect("connect");
//! host.service(0);
//! host.send(peer, 0, SendFlags::RELIABLE, b"hello").expect("send");
//! while let Some(out) = host.poll_outgoing() { /* transmit out.data via UDP */ }
//! while let Some(event) = host.poll_event() { /* handle */ }
//! ```

#![deny(unsafe_op_in_unsafe_fn)]

use std::net::{Ipv4Addr, SocketAddrV4};

use bitflags::bitflags;

pub use lenet_sys as ffi;

/// Maximum payload length a receive event can carry: the engine rejects
/// reassemblies above `maximumMtu * 1024` (see `FragmentAssembler.init`),
/// so a buffer of this size makes truncation structurally impossible.
const MAX_RECV_PAYLOAD: usize = 4096 * 1024;

/// Identifies a peer within a host. Opaque newtype; ids are recycled by the
/// protocol engine after a peer disconnects — upper layers (e.g.
/// `lenet-tokio`) are responsible for stale-handle detection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(u16);

impl PeerId {
    #[must_use]
    pub const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn raw(self) -> u16 {
        self.0
    }
}

bitflags! {
    /// Packet delivery modes (bitmask, ENet-compatible).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct SendFlags: u32 {
        const UNRELIABLE = ffi::LENET_UNRELIABLE;
        const RELIABLE = ffi::LENET_RELIABLE;
        const UNSEQUENCED = ffi::LENET_UNSEQUENCED;
        const UNRELIABLE_FRAGMENT = ffi::LENET_UNRELIABLE_FRAGMENT;
    }
}

/// An application event. [`Event::Receive`] borrows the payload from the
/// host's internal buffer; the borrow ends at the next [`Host`] method call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event<'a> {
    Connect {
        peer: PeerId,
        data: u32,
    },
    Disconnect {
        peer: PeerId,
        data: u32,
    },
    Receive {
        peer: PeerId,
        channel_id: u8,
        payload: &'a [u8],
    },
}

/// An outgoing datagram to transmit over the driver's UDP socket. `data`
/// borrows the host's internal buffer until the next [`Host::poll_outgoing`]
/// (or any other `&mut self` call).
#[derive(Clone, Copy, Debug)]
pub struct Outgoing<'a> {
    pub addr: SocketAddrV4,
    pub data: &'a [u8],
}

#[derive(Debug)]
#[non_exhaustive]
pub enum SendError {
    /// Unknown peer, peer not connected, channel out of range, packet too
    /// large, or peer id out of range.
    Rejected,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Rejected => write!(
                f,
                "send rejected (unknown/disconnected peer, bad channel, or packet too large)"
            ),
        }
    }
}

impl std::error::Error for SendError {}

#[derive(Debug)]
#[non_exhaustive]
pub enum ConnectError {
    /// The protocol engine refused (no free peer slot, invalid channel
    /// count, or peer id out of range).
    Rejected,
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Rejected => write!(f, "connection rejected (no free peer slot)"),
        }
    }
}

impl std::error::Error for ConnectError {}

/// Calls `lenet_initialize` exactly once per process (idempotent in the C
/// layer; this makes the up-front cost explicit and avoids lazy-init jitter
/// on the first packet).
fn initialize() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // SAFETY: `lenet_initialize` is a plain void() call; safe to invoke
        // from any thread once (and internally guarded there as well).
        unsafe { ffi::lenet_initialize() };
    });
}

/// Builder for a [`Host`].
///
/// All fields are optional with Lenet defaults: unbound socket (informational
/// only — the driver owns the socket), 16 peer slots, 1 channel, unlimited
/// bandwidth, default MTU (1392, clamped to [576, 4096]).
#[derive(Clone, Debug)]
pub struct HostBuilder {
    bind: Option<SocketAddrV4>,
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
            bind: None,
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
    pub fn bind(mut self, addr: SocketAddrV4) -> Self {
        self.bind = Some(addr);
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

    /// Enables CRC32 checksums (must be enabled on both ends, like `ENet`).
    #[must_use]
    pub fn checksum(mut self, enabled: bool) -> Self {
        self.checksum = enabled;
        self
    }

    #[must_use]
    pub fn build(self) -> Option<Host> {
        initialize();
        let (bind_ip, bind_port) = match self.bind {
            Some(addr) => (u32::from(*addr.ip()).to_be(), addr.port()),
            None => (0, 0),
        };
        let raw = unsafe {
            ffi::lenet_host_create(
                bind_ip,
                bind_port,
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
        let host = Host {
            raw,
            payload_buf: Vec::with_capacity(MAX_RECV_PAYLOAD),
        };
        if self.checksum {
            // SAFETY: `host.raw` is a valid host pointer as of the line above.
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
    /// Reusable receive buffer, pre-sized to the engine's hard maximum
    /// payload so `poll_event` can never truncate. The allocation is
    /// virtual; pages are only touched as payloads actually arrive.
    payload_buf: Vec<u8>,
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
        // SAFETY: `self.raw` is a valid host pointer created by
        // `lenet_host_create` and not otherwise freed.
        unsafe { ffi::lenet_host_destroy(self.raw) };
    }
}

impl Host {
    /// Starts a connection attempt to `addr` with `channel_count` channels
    /// (1..=255) and `data` as the connect payload.
    /// Returns the local peer id.
    ///
    /// # Errors
    ///
    /// [`ConnectError::Rejected`] if no peer slot is free (or the engine
    /// rejects the parameters).
    pub fn connect(
        &mut self,
        addr: SocketAddrV4,
        channel_count: usize,
        data: u32,
    ) -> Result<PeerId, ConnectError> {
        let r = unsafe {
            // SAFETY: `self.raw` is valid; the scalars are passed by value.
            ffi::lenet_host_connect(
                self.raw,
                u32::from(*addr.ip()).to_be(),
                addr.port(),
                channel_count,
                data,
            )
        };
        if r < 0 {
            Err(ConnectError::Rejected)
        } else {
            Ok(PeerId(r as u16))
        }
    }

    /// Queues a packet for transmission on `channel`.
    ///
    /// # Errors
    ///
    /// [`SendError::Rejected`] on unknown/disconnected peer, out-of-range
    /// channel, or packet too large.
    pub fn send(
        &mut self,
        peer: PeerId,
        channel: u8,
        flags: SendFlags,
        data: &[u8],
    ) -> Result<(), SendError> {
        let r = unsafe {
            // SAFETY: `self.raw` is valid; `data` is a valid read-only
            // buffer of `data.len()` bytes for the call's duration.
            ffi::lenet_host_send(
                self.raw,
                peer.raw(),
                channel,
                flags.bits(),
                data.as_ptr().cast(),
                data.len(),
            )
        };
        if r < 0 {
            Err(SendError::Rejected)
        } else {
            Ok(())
        }
    }

    /// Queues a packet on `channel` of every connected peer.
    pub fn broadcast(&mut self, channel: u8, flags: SendFlags, data: &[u8]) {
        unsafe {
            // SAFETY: `self.raw` is valid; `data` is a valid read-only
            // buffer of `data.len()` bytes for the call's duration.
            ffi::lenet_host_broadcast(
                self.raw,
                channel,
                flags.bits(),
                data.as_ptr().cast(),
                data.len(),
            )
        };
    }

    /// Begins a graceful disconnect of `peer`.
    pub fn disconnect(&mut self, peer: PeerId, data: u32) {
        // SAFETY: `self.raw` is valid.
        unsafe { ffi::lenet_host_disconnect(self.raw, peer.raw(), data) };
    }

    /// `ENet`'s `enet_peer_disconnect_later`: flushes queued packets, then
    /// disconnects. Degrades to [`Host::disconnect`] when nothing is pending.
    pub fn disconnect_later(&mut self, peer: PeerId, data: u32) {
        // SAFETY: `self.raw` is valid.
        unsafe { ffi::lenet_host_disconnect_later(self.raw, peer.raw(), data) };
    }

    /// `ENet`'s `enet_peer_throttle_configure`: sets the local throttle
    /// parameters and informs the remote peer.
    pub fn throttle_configure(
        &mut self,
        peer: PeerId,
        interval: u32,
        acceleration: u32,
        deceleration: u32,
    ) {
        // SAFETY: `self.raw` is valid.
        unsafe {
            ffi::lenet_peer_throttle_configure(
                self.raw,
                peer.raw(),
                interval,
                acceleration,
                deceleration,
            )
        };
    }

    /// Configures timeout behavior for `peer` (see `enet_peer_timeout`).
    pub fn set_timeout(&mut self, peer: PeerId, limit: u32, minimum: u32, maximum: u32) {
        // SAFETY: `self.raw` is valid.
        unsafe { ffi::lenet_peer_set_timeout(self.raw, peer.raw(), limit, minimum, maximum) };
    }

    /// Feeds one received UDP datagram into the protocol engine.
    pub fn handle_datagram(&mut self, now_ms: u32, from: SocketAddrV4, data: &[u8]) {
        // SAFETY: `self.raw` is valid; `data` is a valid read-only buffer of
        // `data.len()` bytes for the call's duration.
        unsafe {
            ffi::lenet_host_handle_datagram(
                self.raw,
                now_ms,
                u32::from(*from.ip()).to_be(),
                from.port(),
                data.as_ptr().cast(),
                data.len(),
            )
        };
    }

    /// Runs the connection's timers: retransmissions, timeouts, keepalive
    /// pings, and the periodic bandwidth recalculation. Call regularly
    /// (the tokio driver does this automatically at `next_deadline`).
    pub fn service(&mut self, now_ms: u32) {
        // SAFETY: `self.raw` is valid.
        unsafe { ffi::lenet_host_service(self.raw, now_ms) };
    }

    /// The next wall-clock deadline (ms, same clock as [`Host::service`])
    /// at which the host's state can change, or `None` if nothing is
    /// scheduled. Timestamps may wrap; compare wrap-aware.
    #[must_use]
    pub fn next_deadline(&self) -> Option<u32> {
        let mut d: u32 = 0;
        let r = unsafe {
            // SAFETY: `self.raw` is valid; `d` is a valid out-pointer.
            ffi::lenet_host_next_deadline(self.raw, &mut d)
        };
        if r == 1 {
            Some(d)
        } else {
            None
        }
    }

    /// Pops one outgoing datagram to transmit over the driver's socket.
    /// The returned view borrows the host's internal buffer and is valid
    /// until the next `&mut self` call on this host.
    pub fn poll_outgoing(&mut self) -> Option<Outgoing<'_>> {
        let mut out: ffi::lenet_datagram = ffi::lenet_datagram {
            ip: 0,
            port: 0,
            data: std::ptr::null(),
            len: 0,
        };
        let r = unsafe {
            // SAFETY: `self.raw` is valid; `out` is a valid out-struct.
            // The returned `data` pointer stays valid until the next
            // poll_outgoing call on this host (C contract), and the
            // `&mut self` borrow of the returned view enforces exactly
            // that lifetime.
            ffi::lenet_host_poll_outgoing(self.raw, &mut out)
        };
        if r != 1 {
            return None;
        }
        // SAFETY: per the C contract, `out.data` points to `out.len`
        // readable bytes valid until the next poll_outgoing call.
        let data = unsafe { std::slice::from_raw_parts(out.data.cast::<u8>(), out.len) };
        let addr = SocketAddrV4::new(Ipv4Addr::from(u32::from_be(out.ip)), out.port);
        Some(Outgoing { addr, data })
    }

    /// Pops one application event. The payload of [`Event::Receive`] borrows
    /// the host's internal buffer and is valid until the next `&mut self`
    /// call on this host. Never truncates: the internal buffer is sized to
    /// the engine's maximum payload.
    pub fn poll_event(&mut self) -> Option<Event<'_>> {
        let mut raw: ffi::lenet_event = ffi::lenet_event {
            type_: 0,
            peer_id: 0,
            channel_id: 0,
            data: 0,
        };
        let mut payload_len: usize = 0;
        // The buffer is never written through the safe Vec API, so its
        // length stays 0 and its full reserved capacity is usable scratch
        // space for the C layer.
        let cap = self.payload_buf.capacity();
        let r = unsafe {
            // SAFETY: `self.raw` is valid; `payload_buf` is a valid writable
            // buffer of `cap` bytes, so `payload_len <= cap` always fits the
            // slice below.
            ffi::lenet_host_poll_event(
                self.raw,
                &mut raw,
                self.payload_buf.as_mut_ptr().cast(),
                cap,
                &mut payload_len,
            )
        };
        debug_assert!(r != -1);
        if r != 1 {
            return None;
        }
        debug_assert!(payload_len <= cap);
        let peer = PeerId(raw.peer_id);
        let event = match raw.type_ {
            ffi::LENET_EVENT_CONNECT => Event::Connect {
                peer,
                data: raw.data,
            },
            ffi::LENET_EVENT_DISCONNECT => Event::Disconnect {
                peer,
                data: raw.data,
            },
            ffi::LENET_EVENT_RECEIVE => Event::Receive {
                peer,
                channel_id: raw.channel_id,
                // SAFETY: the C layer wrote `payload_len` (<= buffer len,
                // see the assertion above) initialized bytes into
                // `payload_buf` during the call above.
                payload: unsafe {
                    std::slice::from_raw_parts(self.payload_buf.as_ptr(), payload_len)
                },
            },
            _ => return None,
        };
        Some(event)
    }
}
