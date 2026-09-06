//! Raw FFI bindings to the Lenet C API.
//!
//! Declared 1:1 with `lenet.h` (the Lenet C distribution). See the `lenet`
//! crate for the safe sans-I/O wrapper. All functions are non-blocking and
//! time is supplied by the caller; a host is NOT thread-safe.

#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

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

pub const LENET_MAX_MTU: c_uint = 4096;
pub const LENET_MIN_MTU: c_uint = 576;

/// Application event.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct lenet_event {
    pub type_: c_uint,
    pub peer_id: c_ushort,
    pub channel_id: c_uchar,
    pub data: c_uint,
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

#[repr(C)]
pub struct lenet_host {
    _unused: [u8; 0],
}
