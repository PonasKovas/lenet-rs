//! Raw FFI bindings to the Lenet C API.
//!
//! Declared 1:1 with `lenet.h` (the Lenet C distribution) at the revision
//! pinned in `build.rs`. See the `lenet` crate for the safe sans-I/O
//! wrapper. All functions are non-blocking and time is supplied by the
//! caller; a host is NOT thread-safe.

#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

use std::marker::{PhantomData, PhantomPinned};
use std::os::raw::{c_uchar, c_uint, c_ushort, c_void};

/// Packet delivery modes (bitmask, ENet-compatible).
pub const LENET_UNRELIABLE: c_uint = 0;
pub const LENET_RELIABLE: c_uint = 1 << 0;
pub const LENET_UNSEQUENCED: c_uint = 1 << 1;
pub const LENET_UNRELIABLE_FRAGMENT: c_uint = 1 << 3;

/// Event types.
pub const LENET_EVENT_NONE: c_uint = 0;
pub const LENET_EVENT_CONNECT: c_uint = 1;
pub const LENET_EVENT_DISCONNECT: c_uint = 2;
pub const LENET_EVENT_RECEIVE: c_uint = 3;

/// Peer states, numbered as ENet's `ENetPeerState` (Lenet has no
/// `CONNECTION_PENDING = 3` or `CONNECTION_SUCCEEDED = 4`).
pub const LENET_PEER_STATE_DISCONNECTED: c_uint = 0;
pub const LENET_PEER_STATE_CONNECTING: c_uint = 1;
pub const LENET_PEER_STATE_ACKNOWLEDGING_CONNECT: c_uint = 2;
pub const LENET_PEER_STATE_CONNECTED: c_uint = 5;
pub const LENET_PEER_STATE_DISCONNECT_LATER: c_uint = 6;
pub const LENET_PEER_STATE_DISCONNECTING: c_uint = 7;
pub const LENET_PEER_STATE_ACKNOWLEDGING_DISCONNECT: c_uint = 8;
pub const LENET_PEER_STATE_ZOMBIE: c_uint = 9;

pub const LENET_MAX_MTU: c_uint = 4096;
pub const LENET_MIN_MTU: c_uint = 576;

/// The largest packet `lenet_host_send` takes (ENet's default maximum
/// packet size). Also the largest payload a receive event can carry.
pub const LENET_MAX_PACKET_SIZE: usize = 32 * 1024 * 1024;

/// Application event.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct lenet_event {
    pub type_: c_uint,
    pub peer_id: c_ushort,
    pub channel_id: c_uchar,
    pub data: c_uint,
}

/// A peer as the application may inspect it. `ip` is in network byte
/// order, `port` in host byte order; times in ms; the packet throttle is
/// out of 32.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct lenet_peer_info {
    pub state: c_uint,
    pub ip: c_uint,
    pub port: c_ushort,
    pub round_trip_time: c_uint,
    pub round_trip_time_variance: c_uint,
    pub packet_throttle: c_uint,
}

/// Outgoing datagram. `data` points to an internal buffer valid until the
/// next `lenet_host_poll_outgoing` call on the same host.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct lenet_datagram {
    pub ip: c_uint,
    pub port: c_ushort,
    pub data: *const c_void,
    pub len: usize,
}

extern "C" {
    pub fn lenet_initialize();
    pub fn lenet_host_create(
        bind_ip: c_uint,
        bind_port: c_ushort,
        peer_count: usize,
        channel_limit: usize,
        incoming_bw: c_uint,
        outgoing_bw: c_uint,
        mtu: c_uint,
    ) -> *mut lenet_host;
    pub fn lenet_host_destroy(host: *mut lenet_host);
    pub fn lenet_host_connect(
        host: *mut lenet_host,
        ip: c_uint,
        port: c_ushort,
        channel_count: usize,
        user_data: c_uint,
    ) -> i32;
    pub fn lenet_host_send(
        host: *mut lenet_host,
        peer_id: c_ushort,
        channel: c_uchar,
        flags: c_uint,
        data: *const c_void,
        len: usize,
    ) -> i32;
    pub fn lenet_host_broadcast(
        host: *mut lenet_host,
        channel: c_uchar,
        flags: c_uint,
        data: *const c_void,
        len: usize,
    );
    pub fn lenet_host_disconnect(host: *mut lenet_host, peer_id: c_ushort, data: c_uint);
    pub fn lenet_host_disconnect_later(host: *mut lenet_host, peer_id: c_ushort, data: c_uint);
    pub fn lenet_peer_disconnect_now(host: *mut lenet_host, peer_id: c_ushort, data: c_uint);
    pub fn lenet_peer_reset(host: *mut lenet_host, peer_id: c_ushort);
    pub fn lenet_peer_ping(host: *mut lenet_host, peer_id: c_ushort);
    pub fn lenet_peer_ping_interval(host: *mut lenet_host, peer_id: c_ushort, interval_ms: c_uint);
    pub fn lenet_host_bandwidth_limit(
        host: *mut lenet_host,
        incoming_bw: c_uint,
        outgoing_bw: c_uint,
    );
    pub fn lenet_host_channel_limit(host: *mut lenet_host, channel_limit: usize);
    pub fn lenet_peer_get_info(
        host: *mut lenet_host,
        peer_id: c_ushort,
        out: *mut lenet_peer_info,
    ) -> i32;
    pub fn lenet_peer_outgoing_data(
        host: *mut lenet_host,
        peer_id: c_ushort,
        out: *mut usize,
    ) -> i32;
    pub fn lenet_peer_throttle_configure(
        host: *mut lenet_host,
        peer_id: c_ushort,
        interval: c_uint,
        acceleration: c_uint,
        deceleration: c_uint,
    );
    pub fn lenet_host_enable_checksum(host: *mut lenet_host);
    pub fn lenet_peer_set_timeout(
        host: *mut lenet_host,
        peer_id: c_ushort,
        limit: c_uint,
        minimum: c_uint,
        maximum: c_uint,
    );
    pub fn lenet_host_handle_datagram(
        host: *mut lenet_host,
        now_ms: c_uint,
        ip: c_uint,
        port: c_ushort,
        data: *const c_void,
        len: usize,
    ) -> i32;
    pub fn lenet_host_flush(host: *mut lenet_host, now_ms: c_uint) -> i32;
    pub fn lenet_host_service(host: *mut lenet_host, now_ms: c_uint) -> i32;
    pub fn lenet_host_next_deadline(host: *mut lenet_host, deadline: *mut c_uint) -> i32;
    pub fn lenet_host_poll_event(
        host: *mut lenet_host,
        out: *mut lenet_event,
        payload_buf: *mut c_void,
        payload_cap: usize,
        payload_len: *mut usize,
    ) -> i32;
    pub fn lenet_host_poll_outgoing(host: *mut lenet_host, out: *mut lenet_datagram) -> i32;
}

/// Opaque host type (defined in C). `PhantomData<(*mut u8, PhantomPinned)>`
/// marks it !Send/!Sync and non-Unpin so it can never be safely moved or
/// shared by value — only ever handled through the raw pointers the FFI
/// returns.
#[repr(C)]
pub struct lenet_host {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}
