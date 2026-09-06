//! Async (tokio) driver for Lenet.
//!
//! Lenet itself is sans-I/O: datagrams in, events + outgoing datagrams out,
//! time supplied by the caller. This crate supplies the I/O and the clock:
//! one tokio task owns the host, its UDP socket, and the pump loop —
//!
//!   - received UDP datagrams are fed in via [`Host::handle_datagram`],
//!   - `Host::service(now)` runs at the host's own timer deadlines
//!     ([`Host::next_deadline`], the same primitive the Lean replay uses),
//!     with a slow fallback tick for safety,
//!   - outgoing datagrams are transmitted on the socket,
//!   - application events are forwarded on an mpsc channel.
//!
//! Threading contract: one host, one thread — the pump task is that thread;
//! application handles only exchange messages with it.

#![deny(unsafe_op_in_unsafe_fn)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use lenet::{Event, Host, HostBuilder, SendFlags};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{sleep_until, Instant as TokioInstant};

/// Configuration for the driver, builder-style.
#[derive(Clone, Debug)]
pub struct LenetConfig {
    host: HostBuilder,
    /// Socket to bind for the driver's UDP endpoint.
    bind_addr: SocketAddr,
    /// Slow fallback service tick. The host's own deadlines schedule the
    /// fine-grained wakeups; this bounds how late a missed deadline can be
    /// observed. Default 100 ms.
    fallback_tick: Duration,
    /// Payload buffer size for polling events. Default 64 KiB.
    event_buffer: usize,
    /// Capacity of the event channel. Default 1024.
    event_channel: usize,
}

impl Default for LenetConfig {
    fn default() -> Self {
        Self {
            host: HostBuilder::new(),
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            fallback_tick: Duration::from_millis(100),
            event_buffer: 64 * 1024,
            event_channel: 1024,
        }
    }
}

impl LenetConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    #[must_use]
    pub fn host(mut self, builder: HostBuilder) -> Self {
        self.host = builder;
        self
    }

    #[must_use]
    pub fn fallback_tick(mut self, tick: Duration) -> Self {
        self.fallback_tick = tick;
        self
    }

    #[must_use]
    pub fn event_buffer(mut self, size: usize) -> Self {
        self.event_buffer = size.max(1);
        self
    }

    #[must_use]
    pub fn event_channel(mut self, cap: usize) -> Self {
        self.event_channel = cap.max(1);
        self
    }
}

/// Errors from [`Endpoint::connect`].
#[derive(Debug)]
pub enum ConnectError {
    /// The protocol engine refused (no free peer slot).
    Rejected,
    /// The driver task is gone.
    Closed,
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Rejected => write!(f, "connection rejected (no free peer slot)"),
            ConnectError::Closed => write!(f, "driver task is gone"),
        }
    }
}

impl std::error::Error for ConnectError {}

#[derive(Debug)]
pub struct SendError;

enum Command {
    Connect {
        addr: SocketAddr,
        channel_count: usize,
        data: u32,
        done: tokio::sync::oneshot::Sender<Result<u16, ConnectError>>,
    },
    Send {
        peer_id: u16,
        channel: u8,
        flags: SendFlags,
        data: Vec<u8>,
    },
    Broadcast {
        channel: u8,
        flags: SendFlags,
        data: Vec<u8>,
    },
    Disconnect {
        peer_id: u16,
        data: u32,
    },
    /// Applies `f` to the host on the owner thread. Escape hatch for
    /// anything the typed API doesn't cover.
    WithHost(Box<dyn FnOnce(&mut Host) + Send>),
}

/// A running Lenet endpoint: owns the driver task.
#[derive(Clone)]
pub struct Endpoint {
    cmd_tx: mpsc::UnboundedSender<Command>,
    socket_addr: SocketAddr,
}

/// An application event with its payload (for [`EventKind::Receive`];
/// empty otherwise).
#[derive(Clone, Debug)]
pub struct DrivenEvent {
    pub event: Event,
    pub payload: Vec<u8>,
}

impl std::ops::Deref for DrivenEvent {
    type Target = Event;
    fn deref(&self) -> &Event {
        &self.event
    }
}

/// Receiver for application events produced by the driver.
#[derive(Debug)]
pub struct EventReceiver {
    rx: mpsc::Receiver<DrivenEvent>,
}

/// A remote peer handle.
#[derive(Clone, Debug)]
pub struct Connection {
    peer_id: u16,
    cmd_tx: mpsc::UnboundedSender<Command>,
}

impl Connection {
    #[must_use]
    pub fn peer_id(&self) -> u16 {
        self.peer_id
    }

    /// Queues a packet for transmission. `Err` if the driver task is gone.
    pub async fn send(
        &self,
        channel: u8,
        flags: SendFlags,
        data: Vec<u8>,
    ) -> Result<(), SendError> {
        self.cmd_tx
            .send(Command::Send {
                peer_id: self.peer_id,
                channel,
                flags,
                data,
            })
            .map_err(|_| SendError)
    }

    /// Begins a graceful disconnect.
    pub async fn disconnect(&self, data: u32) {
        let _ = self
            .cmd_tx
            .send(Command::Disconnect {
                peer_id: self.peer_id,
                data,
            });
    }
}

impl Endpoint {
    /// Starts the driver task: binds the socket and spawns the pump loop.
    pub async fn start(config: LenetConfig) -> std::io::Result<(Endpoint, EventReceiver)> {
        let socket = UdpSocket::bind(config.bind_addr).await?;
        let socket_addr = socket.local_addr()?;
        let (event_tx, event_rx) = mpsc::channel::<DrivenEvent>(config.event_channel);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let host = config
            .host
            .clone()
            .build()
            .ok_or_else(|| std::io::Error::other("lenet_host_create failed"))?;

        tokio::spawn(pump(host, socket, config, cmd_rx, event_tx));

        Ok((Endpoint { cmd_tx, socket_addr }, EventReceiver { rx: event_rx }))
    }

    /// The local UDP address the driver bound.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    /// Starts a connection attempt. Returns the peer id once the request
    /// was applied on the driver thread (not once the handshake completes —
    /// watch the event stream for the CONNECT event).
    pub async fn connect(
        &self,
        addr: SocketAddr,
        channel_count: usize,
        data: u32,
    ) -> Result<Connection, ConnectError> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::Connect {
                addr,
                channel_count,
                data,
                done: done_tx,
            })
            .map_err(|_| ConnectError::Closed)?;
        let peer_id = done_rx.await.map_err(|_| ConnectError::Closed)??;
        Ok(Connection {
            peer_id,
            cmd_tx: self.cmd_tx.clone(),
        })
    }

    /// A handle for sending to an existing peer by id (server role: peers
    /// appear via CONNECT events, not via [`Endpoint::connect`]).
    #[must_use]
    pub fn peer(&self, peer_id: u16) -> Connection {
        Connection {
            peer_id,
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    /// Broadcasts a packet to every connected peer.
    pub async fn broadcast(&self, channel: u8, flags: SendFlags, data: Vec<u8>) {
        let _ = self.cmd_tx.send(Command::Broadcast { channel, flags, data });
    }

    /// Runs `f` with exclusive access to the host on the driver thread.
    /// Returns `false` if the driver task is gone.
    pub async fn with_host<F>(&self, f: F) -> bool
    where
        F: FnOnce(&mut Host) + Send + 'static,
    {
        self.cmd_tx.send(Command::WithHost(Box::new(f))).is_ok()
    }
}

impl EventReceiver {
    /// Receives the next application event, or `None` when the driver is
    /// shut down.
    pub async fn recv(&mut self) -> Option<DrivenEvent> {
        self.rx.recv().await
    }
}

/// The driver loop. One task, one host, one socket.
async fn pump(
    mut host: Host,
    socket: UdpSocket,
    config: LenetConfig,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    event_tx: mpsc::Sender<DrivenEvent>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    let mut payload = vec![0u8; config.event_buffer];
    let mut next_fallback = now_tokio() + config.fallback_tick;

    loop {
        // Deadline-driven tick: wake at the host's next timer deadline if
        // there is one, otherwise at the fallback tick. Commands wake us
        // immediately.
        let wake = host
            .next_deadline()
            .map(deadline_instant)
            .unwrap_or(next_fallback)
            .min(next_fallback);

        tokio::select! {
            // Timer branch: a deadline (host or fallback) came due.
            _ = sleep_until(wake) => {
                host.service(now_ms());
                next_fallback = now_tokio() + config.fallback_tick;
            }
            // Inbound datagram.
            recv = socket.recv_from(&mut buf) => {
                match recv {
                    Ok((n, from)) => {
                        host.handle_datagram(now_ms(), ipv4_net(from), from.port(), &buf[..n]);
                    }
                    Err(e) => {
                        eprintln!("lenet-tokio: recv error: {e}");
                        sleep_until(now_tokio() + Duration::from_millis(10)).await;
                        continue;
                    }
                }
            }
            // Command from application handles.
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::Connect { addr, channel_count, data, done }) => {
                        let ip = match addr.ip() {
                            IpAddr::V4(v4) => v4,
                            IpAddr::V6(_) => {
                                let _ = done.send(Err(ConnectError::Rejected));
                                continue;
                            }
                        };
                        let _ = done.send(
                            host.connect(ip, addr.port(), channel_count, data)
                                .map_err(|_| ConnectError::Rejected),
                        );
                    }
                    Some(Command::Send { peer_id, channel, flags, data }) => {
                        let _ = host.send(peer_id, channel, flags, &data);
                    }
                    Some(Command::Broadcast { channel, flags, data }) => {
                        host.broadcast(channel, flags, &data);
                    }
                    Some(Command::Disconnect { peer_id, data }) => {
                        host.disconnect(peer_id, data);
                    }
                    Some(Command::WithHost(f)) => f(&mut host),
                    None => return, // all handles dropped: shut down
                }
            }
        }

        // After any progress: flush outgoing datagrams and events, then
        // re-evaluate deadlines on the next iteration.
        flush_outgoing(&mut host, &socket).await;
        flush_events(&mut host, &mut payload, &event_tx).await;
    }
}

async fn flush_outgoing(host: &mut Host, socket: &UdpSocket) {
    while let Some(dgram) = host.poll_outgoing() {
        let addr = socket_addr(dgram.ip, dgram.port);
        if let Err(e) = socket.send_to(&dgram.data, addr).await {
            eprintln!("lenet-tokio: send error to {addr}: {e}");
            break;
        }
    }
}

async fn flush_events(host: &mut Host, payload: &mut [u8], event_tx: &mpsc::Sender<DrivenEvent>) {
    while let Some(polled) = host.poll_event(payload) {
        let driven = DrivenEvent {
            event: polled.event,
            payload: payload[..polled.payload_len.min(payload.len())].to_vec(),
        };
        if event_tx.send(driven).await.is_err() {
            break;
        }
    }
}

/// Converts a Lenet ms deadline (may wrap) into a tokio Instant. Wrap-aware:
/// the deadline is interpreted as the first occurrence within ±2^31 ms of
/// now (same cyclic arithmetic as `Lenet.Time`).
fn deadline_instant(deadline_ms: u32) -> TokioInstant {
    let now = Instant::now();
    let diff = deadline_ms.wrapping_sub(now_ms()) as i32;
    if diff <= 0 {
        TokioInstant::from_std(now)
    } else {
        TokioInstant::from_std(now + Duration::from_millis(diff as u64))
    }
}

fn now_tokio() -> TokioInstant {
    TokioInstant::from_std(Instant::now())
}

/// Monotonic clock in milliseconds anchored at first use, truncated to u32
/// (wraps like Lenet's timestamps; all Lenet time math is wrap-aware).
fn now_ms() -> u32 {
    use std::sync::OnceLock;
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    let anchor = ANCHOR.get_or_init(Instant::now);
    anchor.elapsed().as_millis() as u32
}

fn ipv4_net(addr: SocketAddr) -> u32 {
    match addr.ip() {
        IpAddr::V4(v4) => u32::from(v4).to_be(),
        IpAddr::V6(_) => 0,
    }
}

fn socket_addr(ip_net: u32, port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::from(u32::from_be(ip_net)), port))
}
