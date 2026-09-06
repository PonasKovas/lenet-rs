//! Async (tokio) driver for Lenet.
//!
//! Lenet itself is sans-I/O: datagrams in, events + outgoing datagrams out,
//! time supplied by the caller. This crate supplies the I/O and the clock.
//! One tokio task (the pump) owns the host and its UDP socket:
//!
//!   - received UDP datagrams are fed in via [`Host::handle_datagram`],
//!   - `Host::service(now)` runs at the host's own timer deadlines
//!     ([`Host::next_deadline`]) with a slow fallback tick for safety,
//!   - outgoing datagrams are transmitted on the socket,
//!   - engine events are demultiplexed into per-connection queues.
//!
//! # Stall-freedom
//!
//! The pump only ever awaits inside its `select!` (inbound datagram, timer,
//! command channels). All other work — dispatching commands, running
//! timers, flushing outgoing datagrams and events — is non-blocking: a slow
//! application reader can fill its own connection's queue, but it can never
//! stall the pump, miss the host's deadlines, or block other connections.
//! Outgoing datagrams are sent with `try_send_to`; when the kernel socket
//! buffer is full the datagram is dropped, which is equivalent to network
//! loss (reliable traffic retransmits).
//!
//! # Backpressure
//!
//! [`Connection::send`] queues into a bounded command channel and truly
//! awaits when it is full. The connection-level event queue behavior is
//! configurable via [`EventQueuePolicy`].
//!
//! # Handle safety
//!
//! Protocol peer ids are recycled by the engine, so user-facing handles are
//! keyed by an internal generation token instead. A stale
//! [`Connection`]/[`Incoming`] (its peer disconnected and the slot was
//! reused) fails closed: [`Connection::send`] returns [`SendError::Closed`]
//! and the pump ignores commands from stale tokens. Dropping a
//! [`Connection`] gracefully disconnects its peer.
//!
//! Threading contract: one host, one thread — the pump task is that thread;
//! application handles only exchange messages with it.
//!
//! # Example
//!
//! Client role — the handshake is awaited via [`Connecting`]:
//!
//! ```no_run
//! use lenet::SendFlags;
//! use lenet_tokio::{ConnEvent, Endpoint, LenetConfig};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let (endpoint, _accepts) = Endpoint::start(LenetConfig::new()).await?;
//! let server = "127.0.0.1:9000".parse()?;
//!
//! let mut conn = endpoint.connect(server, 2, 0x1234).await?.await?;
//! conn.send(0, SendFlags::RELIABLE, b"hello".to_vec()).await?;
//!
//! while let Some(event) = conn.recv().await {
//!     match event {
//!         ConnEvent::Receive { payload, .. } => println!("got {} bytes", payload.len()),
//!         ConnEvent::Disconnect { data } => break,
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Server role — incoming connections arrive on an [`AcceptQueue`]:
//!
//! ```no_run
//! use lenet::SendFlags;
//! use lenet_tokio::{ConnEvent, Endpoint, LenetConfig};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let (endpoint, mut accepts) =
//!     Endpoint::start(LenetConfig::new().bind_addr("0.0.0.0:9000".parse()?)).await?;
//! while let Some(incoming) = accepts.accept().await {
//!     let _payload = incoming.connect_data(); // connect payload from the remote
//!     tokio::spawn(async move {
//!         let mut conn = incoming.accept();
//!         while let Some(ConnEvent::Receive { channel_id, payload }) = conn.recv().await {
//!             let _ = conn.send(channel_id, SendFlags::RELIABLE, payload).await;
//!         }
//!     });
//! }
//! # Ok(())
//! # }
//! ```

#![deny(unsafe_op_in_unsafe_fn)]

use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use lenet::{Event as CoreEvent, Host, HostBuilder, PeerId, SendFlags};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
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
    /// Capacity of the (bounded) command channel carrying `send`/`broadcast`
    /// traffic. `Connection::send` awaits when full — this is the
    /// driver's outbound backpressure. Default 1024.
    cmd_channel: usize,
    /// Capacity of the incoming-connection queue. When full, new incoming
    /// connections are rejected immediately (graceful disconnect) instead
    /// of stalling the pump. Default 64.
    accept_channel: usize,
    /// Per-connection event queue behavior. Default [`EventQueuePolicy::Unbounded`].
    event_queue: EventQueuePolicy,
    /// Applied to every new peer (`Host::set_timeout`): `(limit, minimum,
    /// maximum)` in the units of `enet_peer_timeout`. `None` = engine
    /// defaults (32, 5000, 30000).
    peer_timeout: Option<(u32, u32, u32)>,
}

/// Queue behavior for a connection's event queue ([`Connection::recv`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EventQueuePolicy {
    /// The queue grows without bound (default). Mirrors C `ENet`, whose
    /// receive queue is also unbounded: events are never lost and the pump
    /// never stalls; memory is bounded by actual traffic.
    #[default]
    Unbounded,
    /// The queue holds at most `cap` events. When full, the driver
    /// force-disconnects the peer with a graceful disconnect carrying
    /// `disconnect_data` and discards that connection's pending events —
    /// rather than dropping individual events (which would silently
    /// violate reliable delivery) or stalling the pump.
    BoundedThenDisconnect { cap: usize, disconnect_data: u32 },
}

impl Default for LenetConfig {
    fn default() -> Self {
        Self {
            host: HostBuilder::new(),
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            fallback_tick: Duration::from_millis(100),
            cmd_channel: 1024,
            accept_channel: 64,
            event_queue: EventQueuePolicy::Unbounded,
            peer_timeout: None,
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
    pub fn cmd_channel(mut self, cap: usize) -> Self {
        self.cmd_channel = cap.max(1);
        self
    }

    #[must_use]
    pub fn accept_channel(mut self, cap: usize) -> Self {
        self.accept_channel = cap.max(1);
        self
    }

    #[must_use]
    pub fn event_queue(mut self, policy: EventQueuePolicy) -> Self {
        self.event_queue = policy;
        self
    }

    /// Applies [`Host::set_timeout`] to every new peer. `(limit, minimum,
    /// maximum)` as in `enet_peer_timeout`.
    #[must_use]
    pub fn peer_timeout(mut self, limit: u32, minimum: u32, maximum: u32) -> Self {
        self.peer_timeout = Some((limit, minimum, maximum));
        self
    }
}

/// Errors from [`Endpoint::connect`] and awaiting [`Connecting`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ConnectError {
    /// The engine refused the attempt (no free peer slot), or the remote
    /// address is IPv6 (Lenet is IPv4-only).
    #[error("connection rejected (no free peer slot, or IPv6 address)")]
    Rejected,
    /// The handshake never completed: the remote refused, or the attempt
    /// timed out per the peer timeout configuration.
    #[error("handshake failed or timed out before connecting")]
    TimedOut,
    /// The endpoint's driver task is gone.
    #[error("endpoint is closed")]
    Closed,
}

/// Errors from [`Connection::send`], [`Connection::disconnect`],
/// [`Endpoint::broadcast`], and [`Endpoint::with_host`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SendError {
    /// The connection is closed or stale (peer disconnected, slot
    /// recycled), or the endpoint's driver task is gone.
    #[error("connection is closed or stale")]
    Closed,
}

/// An event on a specific [`Connection`], delivered via [`Connection::recv`].
#[derive(Clone, Debug)]
pub enum ConnEvent {
    /// A packet arrived on `channel_id`.
    Receive { channel_id: u8, payload: Bytes },
    /// The peer disconnected (gracefully, or by timeout). `data` is the
    /// disconnect payload; `0` for timeouts and locally initiated
    /// disconnects. After this event the connection is closed.
    Disconnect { data: u32 },
}

/// Generation token identifying one connection for its whole lifetime.
/// Unlike raw peer ids, tokens are never recycled, so stale handles are
/// always detectable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Token(u64);

/// Channels shared by all user-facing handles. The `handles` Arc's strong
/// count tracks live external handles; the pump shuts down when only its
/// own copy remains.
#[derive(Clone, Debug)]
struct Shared {
    /// Control commands: never dropped or backpressured (low rate, tiny).
    ctrl_tx: mpsc::UnboundedSender<Command>,
    /// Bulk data commands (`send`/`broadcast`): bounded, provides real
    /// backpressure to `Connection::send`.
    data_tx: mpsc::Sender<Command>,
    /// Only cloned, never read: its Arc strong count is the live-handle
    /// registry the pump polls (see `Pump::run`).
    #[allow(dead_code)]
    handles: Arc<()>,
}

impl Shared {
    fn send_ctrl(&self, cmd: Command) -> Result<(), SendError> {
        self.ctrl_tx.send(cmd).map_err(|_| SendError::Closed)
    }

    fn try_send_ctrl(&self, cmd: Command) -> Result<(), SendError> {
        self.ctrl_tx.send(cmd).map_err(|_| SendError::Closed)
    }

    async fn send_data(&self, cmd: Command) -> Result<(), SendError> {
        self.data_tx.send(cmd).await.map_err(|_| SendError::Closed)
    }
}

/// Type-erased result of a [`Endpoint::with_host`] closure.
type WithHostResult = Box<dyn std::any::Any + Send>;
/// Type-erased closure applied to the host on the pump thread.
type WithHostFn = Box<dyn FnOnce(&mut Host) -> WithHostResult + Send>;

enum Command {
    Connect {
        addr: SocketAddrV4,
        channel_count: usize,
        data: u32,
        done: oneshot::Sender<Result<(Token, PeerId, EventRx), ConnectError>>,
    },
    Send {
        token: Token,
        channel: u8,
        flags: SendFlags,
        data: Bytes,
    },
    Broadcast {
        channel: u8,
        flags: SendFlags,
        data: Bytes,
    },
    Disconnect {
        token: Token,
        data: u32,
    },
    Shutdown,
    /// Applies `f` to the host on the pump thread and returns its result.
    /// Escape hatch for anything the typed API doesn't cover.
    WithHost {
        f: WithHostFn,
        done: oneshot::Sender<WithHostResult>,
    },
}

/// Sender side of a per-connection event queue.
#[derive(Debug)]
enum EventTx {
    Bounded(mpsc::Sender<ConnEvent>),
    Unbounded(mpsc::UnboundedSender<ConnEvent>),
}

enum QueueError {
    Full,
    Closed,
}

impl EventTx {
    fn try_send(&self, ev: ConnEvent) -> Result<(), QueueError> {
        match self {
            EventTx::Bounded(tx) => tx.try_send(ev).map_err(|e| match e {
                // A rejected event is discarded here by design: the
                // force-close path in `dispatch` follows immediately.
                mpsc::error::TrySendError::Full(_) => QueueError::Full,
                mpsc::error::TrySendError::Closed(_) => QueueError::Closed,
            }),
            EventTx::Unbounded(tx) => tx.send(ev).map_err(|_| QueueError::Closed),
        }
    }
}

/// Receiver side of a per-connection event queue (owned by the user's
/// [`Connection`]).
#[derive(Debug)]
enum EventRx {
    Bounded(mpsc::Receiver<ConnEvent>),
    Unbounded(mpsc::UnboundedReceiver<ConnEvent>),
}

impl EventRx {
    async fn recv(&mut self) -> Option<ConnEvent> {
        match self {
            EventRx::Bounded(rx) => rx.recv().await,
            EventRx::Unbounded(rx) => rx.recv().await,
        }
    }

    /// True once the pump has torn the queue down (peer disconnected or the
    /// endpoint shut down): all pump-side senders are gone.
    fn is_dead(&self) -> bool {
        match self {
            EventRx::Bounded(rx) => rx.sender_strong_count() == 0,
            EventRx::Unbounded(rx) => rx.sender_strong_count() == 0,
        }
    }
}

fn make_queue(policy: &EventQueuePolicy) -> (EventTx, EventRx) {
    match policy {
        EventQueuePolicy::Unbounded => {
            let (tx, rx) = mpsc::unbounded_channel();
            (EventTx::Unbounded(tx), EventRx::Unbounded(rx))
        }
        EventQueuePolicy::BoundedThenDisconnect { cap, .. } => {
            let (tx, rx) = mpsc::channel((*cap).max(1));
            (EventTx::Bounded(tx), EventRx::Bounded(rx))
        }
    }
}

/// A running Lenet endpoint: owns the driver task (pump loop, UDP socket,
/// host). Clones share the same driver; the driver runs until
/// [`Endpoint::shutdown`] is called or every handle is dropped.
#[derive(Clone, Debug)]
pub struct Endpoint {
    shared: Shared,
    local_addr: SocketAddr,
}

impl Endpoint {
    /// Starts the driver task: binds the socket and spawns the pump loop.
    pub async fn start(config: LenetConfig) -> std::io::Result<(Endpoint, AcceptQueue)> {
        let socket = UdpSocket::bind(config.bind_addr).await?;
        let local_addr = socket.local_addr()?;
        let host = config
            .host
            .clone()
            .build()
            .ok_or_else(|| std::io::Error::other("lenet_host_create failed"))?;

        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
        let (data_tx, data_rx) = mpsc::channel(config.cmd_channel.max(1));
        let (accept_tx, accept_rx) = mpsc::channel(config.accept_channel.max(1));
        let handles = Arc::new(());

        let shared = Shared {
            ctrl_tx: ctrl_tx.clone(),
            data_tx: data_tx.clone(),
            handles: Arc::clone(&handles),
        };

        tokio::spawn(
            Pump {
                host,
                socket,
                config,
                udp_buf: vec![0u8; 64 * 1024],
                ctrl_tx,
                ctrl_rx,
                data_tx,
                data_rx,
                accept_tx,
                handles,
                next_token: 0,
                peers: HashMap::new(),
                tokens: HashMap::new(),
                pending: HashMap::new(),
                pooled: BytesMut::new(),
                shutdown: false,
            }
            .run(),
        );

        Ok((
            Endpoint { shared, local_addr },
            AcceptQueue { rx: accept_rx },
        ))
    }

    /// The local UDP address the driver bound.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Starts a connection attempt to `addr`. The returned [`Connecting`]
    /// future resolves once the handshake completes (yielding the
    /// [`Connection`]) or fails/times out (yielding
    /// [`ConnectError::TimedOut`]).
    ///
    /// Dropping the [`Connecting`] future without awaiting it cancels the
    /// attempt (the peer is gracefully disconnected).
    ///
    /// # Errors
    ///
    /// [`ConnectError::Rejected`] for IPv6 addresses (Lenet is IPv4-only)
    /// or when the engine has no free peer slot; [`ConnectError::Closed`]
    /// when the driver task is gone.
    pub async fn connect(
        &self,
        addr: SocketAddr,
        channel_count: usize,
        data: u32,
    ) -> Result<Connecting, ConnectError> {
        let addr = match addr.ip() {
            IpAddr::V4(v4) => SocketAddrV4::new(v4, addr.port()),
            IpAddr::V6(_) => return Err(ConnectError::Rejected),
        };
        let (done_tx, done_rx) = oneshot::channel();
        self.shared
            .send_ctrl(Command::Connect {
                addr,
                channel_count,
                data,
                done: done_tx,
            })
            .map_err(|_| ConnectError::Closed)?;
        Ok(Connecting {
            shared: self.shared.clone(),
            done: done_rx,
        })
    }

    /// Broadcasts a packet to every connected peer.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] when the driver task is gone.
    pub async fn broadcast(
        &self,
        channel: u8,
        flags: SendFlags,
        data: impl Into<Bytes>,
    ) -> Result<(), SendError> {
        self.shared
            .send_data(Command::Broadcast {
                channel,
                flags,
                data: data.into(),
            })
            .await
    }

    /// Runs `f` with exclusive access to the host on the pump thread and
    /// returns its result. Escape hatch for anything the typed API doesn't
    /// cover (stats, throttle configuration, ...).
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] when the driver task is gone.
    pub async fn with_host<T, F>(&self, f: F) -> Result<T, SendError>
    where
        F: FnOnce(&mut Host) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (done_tx, done_rx) = oneshot::channel::<WithHostResult>();
        let f = move |host: &mut Host| -> WithHostResult { Box::new(f(host)) };
        self.shared.send_ctrl(Command::WithHost {
            f: Box::new(f),
            done: done_tx,
        })?;
        let boxed = done_rx.await.map_err(|_| SendError::Closed)?;
        match boxed.downcast::<T>() {
            Ok(t) => Ok(*t),
            Err(_) => unreachable!("with_host result type mismatch"),
        }
    }

    /// Flushes pending outgoing traffic and stops the driver task. All
    /// [`Connection`]s see their queues drain and close. Dropping the
    /// endpoint (and all other handles) also stops the driver, but without
    /// a guaranteed final flush.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] when the driver task is already gone.
    pub async fn shutdown(&self) -> Result<(), SendError> {
        self.shared.send_ctrl(Command::Shutdown)
    }
}

/// Queue of incoming connections (server role). Drained with
/// [`AcceptQueue::accept`].
///
/// When this queue is full, the driver rejects new incoming connections
/// immediately instead of stalling — server-side backpressure.
#[derive(Debug)]
pub struct AcceptQueue {
    rx: mpsc::Receiver<Incoming>,
}

impl AcceptQueue {
    /// Waits for the next incoming connection, or `None` once the endpoint
    /// is shut down.
    pub async fn accept(&mut self) -> Option<Incoming> {
        self.rx.recv().await
    }
}

/// An incoming connection, pending accept or reject.
///
/// Dropping it without calling [`Incoming::accept`] or
/// [`Incoming::reject`] rejects the connection with a graceful disconnect.
#[derive(Debug)]
pub struct Incoming {
    token: Token,
    peer_id: PeerId,
    connect_data: u32,
    shared: Shared,
    event_rx: Option<EventRx>,
}

impl Incoming {
    /// The peer's slot index on the host (informational).
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The connect payload sent by the remote.
    #[must_use]
    pub fn connect_data(&self) -> u32 {
        self.connect_data
    }

    /// Accepts the connection, returning its handle.
    ///
    /// # Panics
    ///
    /// Never: `event_rx` is always present until this method takes it
    /// (or a preceding `reject`/drop consumed it, making this
    /// unreachable).
    #[must_use]
    pub fn accept(mut self) -> Connection {
        Connection {
            token: self.token,
            peer_id: self.peer_id,
            shared: self.shared.clone(),
            event_rx: self
                .event_rx
                .take()
                .expect("event_rx present until consumed"),
        }
    }

    /// Rejects the connection with a graceful disconnect carrying `data`.
    pub fn reject(mut self, data: u32) {
        self.disconnect(data);
    }

    fn disconnect(&mut self, data: u32) {
        // `event_rx` doubles as the "still undecided" marker, so a reject
        // after an accept (or a reject after a reject) is a no-op.
        if self.event_rx.take().is_some() {
            let _ = self.shared.try_send_ctrl(Command::Disconnect {
                token: self.token,
                data,
            });
        }
    }
}

impl Drop for Incoming {
    fn drop(&mut self) {
        self.disconnect(0);
    }
}

/// A connection attempt in progress. Resolves to a [`Connection`] once the
/// handshake completes, or to [`ConnectError`] if it fails or times out.
///
/// Dropping it without awaiting cancels the attempt.
#[derive(Debug)]
pub struct Connecting {
    shared: Shared,
    done: oneshot::Receiver<Result<(Token, PeerId, EventRx), ConnectError>>,
}

impl Future for Connecting {
    type Output = Result<Connection, ConnectError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.done).poll(cx).map(|res| {
            let parts = match res {
                Ok(Ok(parts)) => Ok(parts),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(ConnectError::Closed),
            };
            parts.map(|(token, peer_id, event_rx)| Connection {
                token,
                peer_id,
                shared: self.shared.clone(),
                event_rx,
            })
        })
    }
}

/// A handle to a connected remote peer. Owns the connection's event queue.
///
/// Dropping it gracefully disconnects the peer (there is no other way to
/// reach that peer afterwards). A stale connection — the peer disconnected
/// and its slot was recycled — fails closed: `send`/`disconnect` return
/// [`SendError::Closed`] instead of reaching a stranger.
#[derive(Debug)]
pub struct Connection {
    token: Token,
    peer_id: PeerId,
    shared: Shared,
    event_rx: EventRx,
}

impl Connection {
    /// The peer's slot index on the host (informational).
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Queues a packet for transmission. Awaits when the endpoint's command
    /// channel is full (real backpressure).
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] if the connection is closed or stale, or the
    /// driver task is gone.
    pub async fn send(
        &self,
        channel: u8,
        flags: SendFlags,
        data: impl Into<Bytes>,
    ) -> Result<(), SendError> {
        if self.event_rx.is_dead() {
            return Err(SendError::Closed);
        }
        self.shared
            .send_data(Command::Send {
                token: self.token,
                channel,
                flags,
                data: data.into(),
            })
            .await
    }

    /// Begins a graceful disconnect carrying `data`.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] if the connection is already closed or stale,
    /// or the driver task is gone.
    pub async fn disconnect(&self, data: u32) -> Result<(), SendError> {
        if self.event_rx.is_dead() {
            return Err(SendError::Closed);
        }
        self.shared.send_ctrl(Command::Disconnect {
            token: self.token,
            data,
        })
    }

    /// Waits for the next event on this connection: `None` once the
    /// connection is closed (remote disconnect, timeout, force-close by the
    /// [`EventQueuePolicy`], or endpoint shutdown).
    pub async fn recv(&mut self) -> Option<ConnEvent> {
        self.event_rx.recv().await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Dropping the last handle to a peer disconnects it gracefully
        // instead of leaking a zombie connection until timeout.
        let _ = self.shared.try_send_ctrl(Command::Disconnect {
            token: self.token,
            data: 0,
        });
    }
}

/// The driver loop. One task, one host, one socket.
struct Pump {
    host: Host,
    socket: UdpSocket,
    config: LenetConfig,
    udp_buf: Vec<u8>,
    ctrl_tx: mpsc::UnboundedSender<Command>,
    ctrl_rx: mpsc::UnboundedReceiver<Command>,
    data_tx: mpsc::Sender<Command>,
    data_rx: mpsc::Receiver<Command>,
    accept_tx: mpsc::Sender<Incoming>,
    /// Strong-count handle registry: the pump's own copy plus one per live
    /// external handle. When only the pump's copy remains, shut down.
    handles: Arc<()>,
    next_token: u64,
    peers: HashMap<PeerId, PeerEntry>,
    tokens: HashMap<Token, PeerId>,
    pending: HashMap<PeerId, PendingConnect>,
    pooled: BytesMut,
    shutdown: bool,
}

struct PeerEntry {
    token: Token,
    event_tx: EventTx,
}

struct PendingConnect {
    token: Token,
    event_tx: EventTx,
    event_rx: EventRx,
    done: oneshot::Sender<Result<(Token, PeerId, EventRx), ConnectError>>,
}

/// Engine event with an owned payload, safe to dispatch after the host
/// borrow ends.
enum OwnedEvent {
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
        payload: Bytes,
    },
}

/// Copies `src` into the pooled buffer and freezes it as an owned `Bytes`.
/// The pool grows to the largest payload seen and is reused, so allocation
/// churn is amortized to ~zero on the steady state.
fn copy_bytes(pooled: &mut BytesMut, src: &[u8]) -> Bytes {
    pooled.reserve(src.len());
    pooled.extend_from_slice(src);
    pooled.split_to(src.len()).freeze()
}

impl Pump {
    fn alloc_token(&mut self) -> Token {
        self.next_token += 1;
        Token(self.next_token)
    }

    fn register_peer(&mut self, peer: PeerId, token: Token, event_tx: EventTx) {
        self.peers.insert(peer, PeerEntry { token, event_tx });
        self.tokens.insert(token, peer);
    }

    fn remove_peer(&mut self, peer: PeerId) {
        if let Some(entry) = self.peers.remove(&peer) {
            self.tokens.remove(&entry.token);
        }
    }

    fn resolve(&self, token: Token) -> Option<PeerId> {
        self.tokens.get(&token).copied()
    }

    fn apply_peer_timeout(&mut self, peer: PeerId) {
        if let Some((limit, minimum, maximum)) = self.config.peer_timeout {
            self.host.set_timeout(peer, limit, minimum, maximum);
        }
    }

    async fn run(mut self) {
        let mut fallback_due = TokioInstant::from_std(Instant::now()) + self.config.fallback_tick;

        while !self.shutdown {
            // All external handles dropped? Shut down.
            if Arc::strong_count(&self.handles) == 1 {
                break;
            }

            // Deadline-driven tick: wake at the host's next timer deadline
            // if there is one, otherwise at the fallback tick.
            let wake = self
                .host
                .next_deadline()
                .map(deadline_instant)
                .unwrap_or(fallback_due)
                .min(fallback_due);

            tokio::select! {
                // Inbound datagram.
                recv = self.socket.recv_from(&mut self.udp_buf) => {
                    match recv {
                        Ok((n, from)) => self.handle_inbound(n, from),
                        Err(e) => {
                            log::warn!("lenet-tokio: recv error: {e}");
                            sleep_until(
                                TokioInstant::from_std(Instant::now())
                                    + Duration::from_millis(10),
                            )
                            .await;
                            continue;
                        }
                    }
                }
                // Control command from application handles.
                ctrl = self.ctrl_rx.recv() => {
                    match ctrl {
                        Some(Command::Shutdown) => self.shutdown = true,
                        Some(cmd) => self.apply_command(cmd),
                        // The pump itself holds a sender; unreachable, but
                        // treat defensively as shutdown.
                        None => self.shutdown = true,
                    }
                }
                // Bulk data command.
                cmd = self.data_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.apply_command(cmd),
                        None => self.shutdown = true,
                    }
                }
                // Timer branch: a deadline (host or fallback) came due.
                _ = sleep_until(wake) => {
                    self.host.service(now_ms());
                    fallback_due =
                        TokioInstant::from_std(Instant::now()) + self.config.fallback_tick;
                }
            }

            self.drain();
            self.flush_outgoing();
            self.flush_events();
        }

        // Final drain + flush: deliver everything queued (e.g. reliable
        // sends flushed by a disconnect) before the host is destroyed.
        self.drain();
        self.flush_outgoing();
        self.flush_events();
    }

    fn handle_inbound(&mut self, n: usize, from: SocketAddr) {
        // Lenet is IPv4-only; silently ignore other families rather than
        // mangling them into 0.0.0.0.
        let SocketAddr::V4(v4) = from else {
            log::debug!("lenet-tokio: dropping non-IPv4 datagram from {from}");
            return;
        };
        self.host.handle_datagram(now_ms(), v4, &self.udp_buf[..n]);
    }

    fn apply_command(&mut self, cmd: Command) {
        match cmd {
            Command::Connect {
                addr,
                channel_count,
                data,
                done,
            } => {
                let Ok(peer) = self.host.connect(addr, channel_count, data) else {
                    let _ = done.send(Err(ConnectError::Rejected));
                    return;
                };
                let token = self.alloc_token();
                let (event_tx, event_rx) = make_queue(&self.config.event_queue);
                self.apply_peer_timeout(peer);
                self.pending.insert(
                    peer,
                    PendingConnect {
                        token,
                        event_tx,
                        event_rx,
                        done,
                    },
                );
            }
            Command::Send {
                token,
                channel,
                flags,
                data,
            } => {
                if let Some(peer) = self.resolve(token) {
                    if let Err(e) = self.host.send(peer, channel, flags, &data) {
                        log::warn!("lenet-tokio: send to peer {} failed: {e}", peer.raw());
                    }
                }
                // Stale token: silently dropped by design — the handle
                // outlived its connection and must not reach the new
                // occupant of the peer slot.
            }
            Command::Broadcast {
                channel,
                flags,
                data,
            } => {
                self.host.broadcast(channel, flags, &data);
            }
            Command::Disconnect { token, data } => {
                if let Some(peer) = self.resolve(token) {
                    self.host.disconnect(peer, data);
                    // The engine will emit the local DISCONNECT event; the
                    // queue closes when it is dispatched.
                }
            }
            Command::Shutdown => self.shutdown = true,
            Command::WithHost { f, done } => {
                let _ = done.send(f(&mut self.host));
            }
        }
    }

    /// Drains every queued command and reject without awaiting, so a burst
    /// of application traffic can never starve socket I/O or timers.
    fn drain(&mut self) {
        while let Ok(cmd) = self.ctrl_rx.try_recv() {
            if let Command::Shutdown = cmd {
                self.shutdown = true;
            } else {
                self.apply_command(cmd);
            }
        }
        while let Ok(cmd) = self.data_rx.try_recv() {
            self.apply_command(cmd);
        }
    }

    /// Transmits all queued outgoing datagrams without awaiting. On a full
    /// kernel socket buffer the datagram is dropped: equivalent to network
    /// loss, which the protocol tolerates (reliable traffic retransmits).
    fn flush_outgoing(&mut self) {
        while let Some(out) = self.host.poll_outgoing() {
            match self.socket.try_send_to(out.data, SocketAddr::V4(out.addr)) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    log::debug!("lenet-tokio: socket buffer full; dropping datagram");
                }
                Err(e) => log::warn!("lenet-tokio: send error: {e}"),
            }
        }
    }

    /// Demultiplexes all pending engine events into per-connection queues
    /// without awaiting. A slow reader fills only its own queue; other
    /// connections and the host's timers are unaffected.
    fn flush_events(&mut self) {
        loop {
            // Convert to an owned event first so the host borrow ends, then
            // dispatch (dispatch may call back into the host).
            let ev = match self.host.poll_event() {
                None => break,
                Some(CoreEvent::Connect { peer, data }) => OwnedEvent::Connect { peer, data },
                Some(CoreEvent::Disconnect { peer, data }) => OwnedEvent::Disconnect { peer, data },
                Some(CoreEvent::Receive {
                    peer,
                    channel_id,
                    payload,
                }) => OwnedEvent::Receive {
                    peer,
                    channel_id,
                    payload: copy_bytes(&mut self.pooled, payload),
                },
            };
            self.dispatch(ev);
        }
    }

    fn dispatch(&mut self, ev: OwnedEvent) {
        match ev {
            OwnedEvent::Connect { peer, data } => {
                if let Some(pending) = self.pending.remove(&peer) {
                    let PendingConnect {
                        token,
                        event_tx,
                        event_rx,
                        done,
                    } = pending;
                    if done.send(Ok((token, peer, event_rx))).is_ok() {
                        self.register_peer(peer, token, event_tx);
                    } else {
                        // The Connecting future was dropped: cancel the
                        // attempt instead of leaking an uncontrollable peer.
                        self.host.disconnect(peer, 0);
                    }
                } else {
                    // Incoming connection (server role).
                    let token = self.alloc_token();
                    let (event_tx, event_rx) = make_queue(&self.config.event_queue);
                    self.apply_peer_timeout(peer);
                    self.register_peer(peer, token, event_tx);
                    let incoming = Incoming {
                        token,
                        peer_id: peer,
                        connect_data: data,
                        shared: Shared {
                            ctrl_tx: self.ctrl_tx.clone(),
                            data_tx: self.data_tx.clone(),
                            handles: Arc::clone(&self.handles),
                        },
                        event_rx: Some(event_rx),
                    };
                    if self.accept_tx.try_send(incoming).is_err() {
                        // Accept queue full: reject instead of stalling.
                        self.remove_peer(peer);
                        self.host.disconnect(peer, 0);
                    }
                }
            }
            OwnedEvent::Disconnect { peer, data } => {
                if let Some(pending) = self.pending.remove(&peer) {
                    // Handshake never completed (timeout or refusal).
                    // Dropping the pending tears down its queue.
                    let _ = pending.done.send(Err(ConnectError::TimedOut));
                } else if let Some(entry) = self.peers.remove(&peer) {
                    self.tokens.remove(&entry.token);
                    // Deliver the disconnect, then close the queue: recv()
                    // yields this event and then None.
                    let _ = entry.event_tx.try_send(ConnEvent::Disconnect { data });
                }
                // else: already torn down (e.g. force-closed) — ignore.
            }
            OwnedEvent::Receive {
                peer,
                channel_id,
                payload,
            } => {
                let outcome = self.peers.get(&peer).map(|entry| {
                    entry.event_tx.try_send(ConnEvent::Receive {
                        channel_id,
                        payload,
                    })
                });
                match outcome {
                    Some(Ok(())) => {}
                    // Handle dropped (Connection::Drop already sent the
                    // disconnect) or unknown peer: just forget the registry.
                    Some(Err(QueueError::Closed)) | None => {
                        self.remove_peer(peer);
                    }
                    // Event queue full per policy: force-close the peer
                    // instead of dropping the event or stalling.
                    Some(Err(QueueError::Full)) => {
                        let data = match self.config.event_queue {
                            EventQueuePolicy::BoundedThenDisconnect {
                                disconnect_data, ..
                            } => disconnect_data,
                            EventQueuePolicy::Unbounded => 0,
                        };
                        self.remove_peer(peer);
                        self.host.disconnect(peer, data);
                    }
                }
            }
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

/// Monotonic clock in milliseconds anchored at first use, truncated to u32
/// (wraps like Lenet's timestamps; all Lenet time math is wrap-aware).
fn now_ms() -> u32 {
    use std::sync::OnceLock;
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    let anchor = ANCHOR.get_or_init(Instant::now);
    anchor.elapsed().as_millis() as u32
}
