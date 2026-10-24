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
//! command channels, socket writability). All other work — dispatching
//! commands, running timers, flushing outgoing datagrams and events — is
//! non-blocking: a slow application reader can fill its own connection's
//! queue, but it can never stall the pump, miss the host's deadlines, or
//! block other connections. Outgoing datagrams are staged one at a time;
//! when the kernel socket buffer is full the staged datagram is retained
//! and retried once the socket becomes writable, and the host's outgoing
//! queue applies backpressure to the protocol engine in the meantime.
//!
//! # Backpressure and ordering
//!
//! [`Connection::send`] queues into a bounded command channel and truly
//! awaits when it is full. Per-connection operations preserve FIFO order:
//! a `send` followed by `disconnect` (or `disconnect_later`) is applied in
//! exactly that order by the driver. The connection-level event queue
//! behavior is configurable via [`EventQueuePolicy`].
//!
//! # Handle safety
//!
//! Protocol peer ids are recycled by the engine, so user-facing handles are
//! keyed by an internal generation token instead. A stale
//! [`Connection`]/[`Incoming`] (its peer disconnected and the slot was
//! reused) fails closed: [`Connection::send`] returns [`SendError::Closed`]
//! and the pump ignores commands from stale tokens. Dropping a
//! [`Connection`] (or all halves of a [`Connection::split`]) gracefully
//! disconnects its peer. Dropping a [`Connecting`] future cancels the
//! pending handshake immediately, freeing the peer slot.
//!
//! Threading contract: one host, one thread — the pump task is that thread;
//! application handles only exchange messages with it.
//!
//! # Shutdown
//!
//! [`Endpoint::shutdown`] gracefully disconnects every peer, waits (up to
//! [`LenetConfig::shutdown_timeout`]) for the disconnects to be
//! acknowledged, flushes, and resolves once the driver has stopped. The
//! driver also closes the same way when the last handle (endpoint, accept
//! queue, or connection) is dropped, which it observes via command-channel
//! closure.
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
//! let endpoint = Endpoint::start(LenetConfig::new()).await?;
//! let server: std::net::SocketAddrV4 = "127.0.0.1:9000".parse()?;
//!
//! let mut conn = endpoint.connect(server, 2, 0x1234)?.await?;
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
//! Server role — incoming connections arrive on an [`AcceptQueue`] created
//! by [`Endpoint::listen`]:
//!
//! ```no_run
//! use lenet::SendFlags;
//! use lenet_tokio::{ConnEvent, Endpoint, LenetConfig};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let endpoint = Endpoint::start(LenetConfig::new().bind_addr("0.0.0.0:9000".parse()?)).await?;
//! let mut accepts = endpoint.listen().await?;
//! while let Some(incoming) = accepts.accept().await {
//!     let peer = incoming.remote_addr(); // e.g. for logging or an IP banlist
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
//!
//! A [`Connection`] can be split into a cloneable sender half and a
//! receiver half, so one task can read while another writes (no shared
//! mutex that a blocking `recv` would deadlock):
//!
//! ```no_run
//! # use lenet_tokio::Connection;
//! # async fn demo(conn: Connection) {
//! let (tx, mut rx) = conn.split();
//! tokio::spawn(async move {
//!     while let Some(event) = rx.recv().await { /* handle */ }
//! });
//! // `tx` is Clone: many tasks can send concurrently.
//! tx.send(0, lenet::SendFlags::RELIABLE, b"hi".to_vec()).await;
//! # }
//! ```

#![deny(unsafe_op_in_unsafe_fn)]

use std::collections::HashMap;
use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
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
    /// How long a shutdown waits for peers to acknowledge their
    /// disconnects before ending the remaining connections at once.
    /// Default 3 s.
    shutdown_timeout: Duration,
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
            shutdown_timeout: Duration::from_secs(3),
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

    /// How long [`Endpoint::shutdown`] (or dropping every handle) waits for
    /// peers to acknowledge their disconnects. Peers still connected after
    /// it are sent one last unacknowledged disconnect and dropped.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
}

/// Errors from [`Endpoint::connect`] and awaiting [`Connecting`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ConnectError {
    /// The engine refused the attempt (no free peer slot, or an invalid
    /// channel count).
    #[error("connection rejected (no free peer slot)")]
    Rejected,
    /// The handshake never completed: the remote refused, or the attempt
    /// timed out per the peer timeout configuration.
    #[error("handshake failed or timed out before connecting")]
    TimedOut,
    /// The endpoint's driver task is gone.
    #[error("endpoint is closed")]
    Closed,
}

/// Errors from [`Connection::send`], [`ConnectionSender::send`],
/// [`Connection::disconnect`], [`ConnectionSender::disconnect`],
/// [`Endpoint::broadcast`], and [`Endpoint::with_host`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SendError {
    /// The connection is closed or stale (peer disconnected, slot
    /// recycled), or the endpoint's driver task is gone.
    #[error("connection is closed or stale")]
    Closed,
}

/// Errors from the non-blocking [`ConnectionSender::try_send`],
/// [`ConnectionSender::try_disconnect`], and
/// [`ConnectionSender::try_disconnect_later`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TrySendError {
    /// The endpoint's command channel is full; nothing was queued.
    #[error("command channel is full")]
    Full,
    /// The connection is closed or stale, or the driver task is gone.
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

/// Allocates a fresh generation token. Process-global so that
/// [`Endpoint::connect`] can mint the token user-side: the [`Connecting`]
/// future needs it to cancel the attempt if dropped before resolving.
fn alloc_token() -> Token {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    Token(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Channels shared by all user-facing handles. Held via `Arc` by every
/// handle (endpoints, accept queues, connections); the pump holds only a
/// `Weak`, so both command channels close — and the pump shuts down —
/// exactly when the last handle is dropped.
#[derive(Debug)]
struct Shared {
    /// Control commands: never dropped or backpressured (low rate, tiny).
    /// Carries nothing that must stay ordered with respect to a
    /// connection's queued `send`s.
    ctrl_tx: mpsc::UnboundedSender<Command>,
    /// Bulk data commands (`send`/`broadcast`/`disconnect`): bounded,
    /// provides real backpressure to `Connection::send`, and preserves
    /// per-connection FIFO order between those operations.
    data_tx: mpsc::Sender<Command>,
}

impl Shared {
    fn send_ctrl(&self, cmd: Command) -> Result<(), SendError> {
        self.ctrl_tx.send(cmd).map_err(|_| SendError::Closed)
    }

    async fn send_data(&self, cmd: Command) -> Result<(), SendError> {
        self.data_tx.send(cmd).await.map_err(|_| SendError::Closed)
    }

    fn try_send_data(&self, cmd: Command) -> Result<(), TrySendError> {
        self.data_tx.try_send(cmd).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => TrySendError::Full,
            mpsc::error::TrySendError::Closed(_) => TrySendError::Closed,
        })
    }

    /// Enqueues `cmd` from a non-async context (`Drop`). Prefers the data
    /// channel so per-connection FIFO order with queued `send`s is kept;
    /// when it is full, falls back to the control channel — the command
    /// may then overtake queued packets, which beats leaking a connected
    /// peer until timeout.
    fn send_ordered_lossy(&self, cmd: Command) {
        match self.data_tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(cmd)) => {
                let _ = self.ctrl_tx.send(cmd);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// Type-erased result of a [`Endpoint::with_host`] closure.
type WithHostResult = Box<dyn std::any::Any + Send>;
/// Type-erased closure applied to the host on the pump thread.
type WithHostFn = Box<dyn FnOnce(&mut Host) -> WithHostResult + Send>;

/// Everything a resolved handshake (or accept) needs to build a
/// [`Connection`].
type ConnParts = (Token, PeerId, EventRx, SocketAddrV4, Arc<AtomicBool>);

enum Command {
    Connect {
        token: Token,
        addr: SocketAddrV4,
        channel_count: usize,
        data: u32,
        done: oneshot::Sender<Result<ConnParts, ConnectError>>,
    },
    /// Cancels a connect attempt whose [`Connecting`] future was dropped
    /// before resolving: frees the pending peer slot (or tears down a peer
    /// whose handshake completed between resolution and the future being
    /// dropped) instead of retransmitting until the peer timeout.
    CancelConnect { token: Token },
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
    /// Sent on the data channel so it stays FIFO-ordered after any `send`s
    /// the application queued on the same connection.
    Disconnect { token: Token, data: u32 },
    /// `Host::disconnect_later` (ENet parity): flush queued packets, then
    /// disconnect once they are delivered. Ordered like [`Command::Disconnect`].
    DisconnectLater { token: Token, data: u32 },
    /// Starts the graceful close (see [`Endpoint::shutdown`]); `done`
    /// fires once the pump has stopped.
    Shutdown { done: oneshot::Sender<()> },
    /// Registers a new accept queue ([`Endpoint::listen`]). The pump acks
    /// once the queue is live, so incoming connections can no longer be
    /// missed between `listen()` being called and returning.
    Listen {
        tx: mpsc::Sender<Incoming>,
        ack: oneshot::Sender<()>,
    },
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
/// [`Endpoint::shutdown`] is called or every handle (endpoints, accept
/// queues, connections) is dropped.
///
/// Client role: just [`Endpoint::connect`]. Server role: call
/// [`Endpoint::listen`] to obtain an [`AcceptQueue`].
#[derive(Clone, Debug)]
pub struct Endpoint {
    shared: Arc<Shared>,
    local_addr: SocketAddr,
    /// Capacity for accept queues created by [`Endpoint::listen`].
    accept_channel: usize,
}

impl Endpoint {
    /// Starts the driver task: binds the socket and spawns the pump loop.
    /// The driver runs until [`Endpoint::shutdown`] is called or every
    /// handle to it is dropped (the pump notices via command-channel
    /// closure). For the server role, call [`Endpoint::listen`]
    /// (promptly — until an accept queue is registered, incoming
    /// connections are rejected) to start receiving incoming connections.
    pub async fn start(config: LenetConfig) -> std::io::Result<Endpoint> {
        let socket = UdpSocket::bind(config.bind_addr).await?;
        let local_addr = socket.local_addr()?;
        let host = config
            .host
            .clone()
            .build()
            .ok_or_else(|| std::io::Error::other("lenet_host_create failed"))?;

        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
        let (data_tx, data_rx) = mpsc::channel(config.cmd_channel.max(1));
        let accept_channel = config.accept_channel.max(1);

        let shared = Arc::new(Shared { ctrl_tx, data_tx });

        tokio::spawn(
            Pump {
                host,
                socket,
                config,
                udp_buf: vec![0u8; 64 * 1024],
                // No strong reference: when the last handle drops, both
                // command channels close and the pump's `select!` sees
                // `None` on both receivers.
                shared: Arc::downgrade(&shared),
                ctrl_rx,
                data_rx,
                accept_txs: Vec::new(),
                peers: HashMap::new(),
                tokens: HashMap::new(),
                pending: HashMap::new(),
                out_buf: BytesMut::new(),
                out_pending: None,
                recv_paused_until: None,
                ctrl_open: true,
                data_open: true,
                closing: None,
                shutdown_waiters: Vec::new(),
            }
            .run(),
        );

        Ok(Endpoint {
            shared,
            local_addr,
            accept_channel,
        })
    }

    /// The local UDP address the driver bound.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Creates the queue incoming connections are delivered on (server
    /// role). The queue keeps the driver alive on its own: the endpoint
    /// keeps accepting and servicing connections until the queue is
    /// dropped, even if every [`Endpoint`] clone has been dropped.
    ///
    /// May be called more than once to fan out across queues; an incoming
    /// connection goes to the first queue with capacity. Until the first
    /// queue is registered (this method returns), incoming connections are
    /// rejected immediately (graceful disconnect).
    ///
    /// When a queue is full, new incoming connections are rejected instead
    /// of stalling the pump — server-side backpressure
    /// ([`LenetConfig::accept_channel`]).
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] when the driver task is gone.
    pub async fn listen(&self) -> Result<AcceptQueue, SendError> {
        let (tx, rx) = mpsc::channel(self.accept_channel);
        let (ack_tx, ack_rx) = oneshot::channel();
        self.shared.send_ctrl(Command::Listen { tx, ack: ack_tx })?;
        ack_rx.await.map_err(|_| SendError::Closed)?;
        Ok(AcceptQueue {
            rx,
            _shared: Arc::clone(&self.shared),
        })
    }

    /// Starts a connection attempt to `addr`. This only queues the attempt
    /// on the driver (non-blocking); the returned [`Connecting`] future
    /// resolves once the handshake completes (yielding the [`Connection`])
    /// or fails/times out (yielding [`ConnectError::TimedOut`]).
    ///
    /// Dropping the [`Connecting`] future without awaiting it cancels the
    /// attempt immediately: the pump frees the peer slot (or disconnects a
    /// peer whose handshake completed in the meantime) instead of
    /// retransmitting until the peer timeout.
    ///
    /// # Errors
    ///
    /// [`ConnectError::Rejected`] when the engine has no free peer slot;
    /// [`ConnectError::Closed`] when the driver task is gone.
    pub fn connect(
        &self,
        addr: SocketAddrV4,
        channel_count: usize,
        data: u32,
    ) -> Result<Connecting, ConnectError> {
        let token = alloc_token();
        let (done_tx, done_rx) = oneshot::channel();
        self.shared
            .send_ctrl(Command::Connect {
                token,
                addr,
                channel_count,
                data,
                done: done_tx,
            })
            .map_err(|_| ConnectError::Closed)?;
        Ok(Connecting {
            shared: Arc::clone(&self.shared),
            token,
            done: Some(done_rx),
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

    /// Gracefully closes the endpoint and waits for the driver to stop.
    ///
    /// Every peer is sent a graceful disconnect (data 0); the driver keeps
    /// running until all of them have acknowledged it or
    /// [`LenetConfig::shutdown_timeout`] passes, then ends any remaining
    /// connections at once, flushes, and stops. New connections are
    /// refused meanwhile. Every [`Connection`] sees its
    /// [`ConnEvent::Disconnect`] (when acknowledged in time) and then the
    /// end of its queue; later sends return [`SendError::Closed`].
    ///
    /// Dropping every handle closes the driver the same way, just without
    /// anyone waiting for it.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] when the driver task is already gone.
    pub async fn shutdown(&self) -> Result<(), SendError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.shared.send_ctrl(Command::Shutdown { done: done_tx })?;
        // The pump drops `done` (instead of firing it) only if it panicked;
        // either way it is gone, which is what the caller waits for.
        let _ = done_rx.await;
        Ok(())
    }
}

/// Queue of incoming connections (server role), created by
/// [`Endpoint::listen`]. Drained with [`AcceptQueue::accept`].
///
/// Holds a driver handle: the pump keeps running (and accepting) while this
/// queue lives, even if every [`Endpoint`] clone was dropped — the server
/// is exactly as alive as its listener.
///
/// When this queue is full, the driver rejects new incoming connections
/// immediately instead of stalling — server-side backpressure.
#[derive(Debug)]
pub struct AcceptQueue {
    rx: mpsc::Receiver<Incoming>,
    /// Held (never read) so the command channels stay open — and the pump
    /// keeps running — while the queue lives.
    _shared: Arc<Shared>,
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
    remote_addr: SocketAddrV4,
    connect_data: u32,
    shared: Arc<Shared>,
    closed: Arc<AtomicBool>,
    event_rx: Option<EventRx>,
}

impl Incoming {
    /// The peer's slot index on the host (informational).
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The address the connection was received from.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddrV4 {
        self.remote_addr
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
        let guard = Arc::new(DisconnectGuard {
            token: self.token,
            shared: Arc::clone(&self.shared),
        });
        Connection {
            token: self.token,
            peer_id: self.peer_id,
            remote_addr: self.remote_addr,
            shared: Arc::clone(&self.shared),
            closed: Arc::clone(&self.closed),
            guard,
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
            let _ = self.shared.send_ctrl(Command::Disconnect {
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
/// Dropping it without awaiting cancels the attempt: the pump frees the
/// peer slot (or disconnects a peer whose handshake completed between
/// resolution and the drop) instead of retransmitting until the timeout.
#[derive(Debug)]
pub struct Connecting {
    shared: Arc<Shared>,
    token: Token,
    /// `Some` until the handshake resolves (a successful poll takes it).
    done: Option<oneshot::Receiver<Result<ConnParts, ConnectError>>>,
}

impl Future for Connecting {
    type Output = Result<Connection, ConnectError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let done = self
            .done
            .as_mut()
            .expect("Connecting polled after completion");
        let res = match Pin::new(done).poll(cx) {
            Poll::Ready(res) => res,
            Poll::Pending => return Poll::Pending,
        };
        self.done = None;
        let parts = match res {
            Ok(Ok(parts)) => Ok(parts),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ConnectError::Closed),
        };
        Poll::Ready(
            parts.map(|(token, peer_id, event_rx, remote_addr, closed)| {
                let guard = Arc::new(DisconnectGuard {
                    token,
                    shared: Arc::clone(&self.shared),
                });
                Connection {
                    token,
                    peer_id,
                    remote_addr,
                    shared: Arc::clone(&self.shared),
                    closed,
                    guard,
                    event_rx,
                }
            }),
        )
    }
}

impl Drop for Connecting {
    fn drop(&mut self) {
        // Still unresolved (the handshake may still be negotiating): tell
        // the pump to cancel the attempt. A no-op once the future resolved
        // (a poll took `done`).
        if self.done.take().is_some() {
            let _ = self
                .shared
                .send_ctrl(Command::CancelConnect { token: self.token });
        }
    }
}

/// Gracefully disconnects the peer when the last handle to it is dropped:
/// an unsplit [`Connection`], or all halves of a split one.
#[derive(Debug)]
struct DisconnectGuard {
    token: Token,
    shared: Arc<Shared>,
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        // Dropping the last handle to a peer disconnects it gracefully
        // instead of leaking a zombie connection until timeout. Ordered
        // with queued `send`s when the data channel has room.
        self.shared.send_ordered_lossy(Command::Disconnect {
            token: self.token,
            data: 0,
        });
    }
}

/// A handle to a connected remote peer. Owns the connection's event queue.
///
/// Dropping it gracefully disconnects the peer (there is no other way to
/// reach that peer afterwards). A stale connection — the peer disconnected
/// and its slot was recycled — fails closed: `send`/`disconnect` return
/// [`SendError::Closed`] instead of reaching a stranger.
///
/// To read and write from separate tasks, use [`Connection::split`].
#[derive(Debug)]
pub struct Connection {
    token: Token,
    peer_id: PeerId,
    remote_addr: SocketAddrV4,
    shared: Arc<Shared>,
    closed: Arc<AtomicBool>,
    guard: Arc<DisconnectGuard>,
    event_rx: EventRx,
}

impl Connection {
    /// The peer's slot index on the host (informational).
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The remote address of the connection.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddrV4 {
        self.remote_addr
    }

    /// Queues a packet for transmission. Awaits when the endpoint's command
    /// channel is full (real backpressure).
    ///
    /// This is fire-and-forget once queued: `Ok(())` means the packet was
    /// handed to the driver, not that the protocol engine accepted it.
    /// Engine-level rejections (channel out of range for the negotiated
    /// channel count, packet too large, or the connection dying in the
    /// meantime) are logged by the driver at `warn` level and the packet is
    /// dropped.
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
        if self.event_rx.is_dead() || self.closed.load(Ordering::Acquire) {
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

    /// Begins a graceful disconnect carrying `data`. Ordered after any
    /// packets queued by earlier `send` calls on this connection.
    ///
    /// Note the engine does not wait for queued packets to be delivered —
    /// use [`Connection::disconnect_later`] for a flush-then-disconnect
    /// close.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] if the connection is already closed or stale,
    /// or the driver task is gone.
    pub async fn disconnect(&self, data: u32) -> Result<(), SendError> {
        if self.event_rx.is_dead() || self.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed);
        }
        self.shared
            .send_data(Command::Disconnect {
                token: self.token,
                data,
            })
            .await
    }

    /// Begins a graceful disconnect carrying `data` after flushing: queued
    /// packets are delivered (and acknowledged) before the peer is torn
    /// down — `enet_peer_disconnect_later` semantics. Ordered after any
    /// packets queued by earlier `send` calls on this connection.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] if the connection is already closed or stale,
    /// or the driver task is gone.
    pub async fn disconnect_later(&self, data: u32) -> Result<(), SendError> {
        if self.event_rx.is_dead() || self.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed);
        }
        self.shared
            .send_data(Command::DisconnectLater {
                token: self.token,
                data,
            })
            .await
    }

    /// Waits for the next event on this connection: `None` once the
    /// connection is closed (remote disconnect, timeout, force-close by the
    /// [`EventQueuePolicy`], or endpoint shutdown).
    pub async fn recv(&mut self) -> Option<ConnEvent> {
        self.event_rx.recv().await
    }

    /// Splits the connection into a sender half and a receiver half, so one
    /// task can await [`ConnectionReceiver::recv`] while others send
    /// concurrently — without a shared mutex (a blocking `recv` under a
    /// lock would deadlock concurrent senders).
    ///
    /// The sender half is [`Clone`]; every clone queues on the same
    /// connection. The peer is gracefully disconnected when **both** halves
    /// (and all sender clones) have been dropped.
    #[must_use]
    pub fn split(self) -> (ConnectionSender, ConnectionReceiver) {
        let sender = ConnectionSender {
            token: self.token,
            peer_id: self.peer_id,
            remote_addr: self.remote_addr,
            shared: Arc::clone(&self.shared),
            closed: Arc::clone(&self.closed),
            guard: Arc::clone(&self.guard),
        };
        let receiver = ConnectionReceiver {
            peer_id: self.peer_id,
            remote_addr: self.remote_addr,
            guard: Arc::clone(&self.guard),
            event_rx: self.event_rx,
        };
        (sender, receiver)
    }
}

/// The sending half of a [`Connection::split`]. Cheap to [`Clone`]: every
/// clone queues packets on the same connection.
#[derive(Clone, Debug)]
pub struct ConnectionSender {
    token: Token,
    peer_id: PeerId,
    remote_addr: SocketAddrV4,
    shared: Arc<Shared>,
    closed: Arc<AtomicBool>,
    /// Held (never read) so the peer is disconnected when the last half is
    /// dropped — see [`DisconnectGuard`].
    #[allow(dead_code)]
    guard: Arc<DisconnectGuard>,
}

impl ConnectionSender {
    /// The peer's slot index on the host (informational).
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The remote address of the connection.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddrV4 {
        self.remote_addr
    }

    /// Queues a packet for transmission — see [`Connection::send`] for the
    /// fire-and-forget semantics.
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
        if self.closed.load(Ordering::Acquire) {
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

    /// Begins a graceful disconnect carrying `data`. Ordered after any
    /// packets queued by earlier `send` calls on this connection.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] if the connection is already closed or stale,
    /// or the driver task is gone.
    pub async fn disconnect(&self, data: u32) -> Result<(), SendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed);
        }
        self.shared
            .send_data(Command::Disconnect {
                token: self.token,
                data,
            })
            .await
    }

    /// Begins a graceful disconnect carrying `data` after flushing queued
    /// packets (`enet_peer_disconnect_later` semantics). Ordered after any
    /// packets queued by earlier `send` calls on this connection.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] if the connection is already closed or stale,
    /// or the driver task is gone.
    pub async fn disconnect_later(&self, data: u32) -> Result<(), SendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed);
        }
        self.shared
            .send_data(Command::DisconnectLater {
                token: self.token,
                data,
            })
            .await
    }

    /// Non-blocking [`ConnectionSender::send`]: queues the packet if the
    /// endpoint's command channel has room, otherwise fails with
    /// [`TrySendError::Full`] without queuing anything.
    ///
    /// # Errors
    ///
    /// [`TrySendError::Full`] when the command channel is full;
    /// [`TrySendError::Closed`] if the connection is closed or stale, or
    /// the driver task is gone.
    pub fn try_send(
        &self,
        channel: u8,
        flags: SendFlags,
        data: impl Into<Bytes>,
    ) -> Result<(), TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed);
        }
        self.shared.try_send_data(Command::Send {
            token: self.token,
            channel,
            flags,
            data: data.into(),
        })
    }

    /// Non-blocking [`ConnectionSender::disconnect`].
    ///
    /// # Errors
    ///
    /// See [`ConnectionSender::try_send`].
    pub fn try_disconnect(&self, data: u32) -> Result<(), TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed);
        }
        self.shared.try_send_data(Command::Disconnect {
            token: self.token,
            data,
        })
    }

    /// Non-blocking [`ConnectionSender::disconnect_later`].
    ///
    /// # Errors
    ///
    /// See [`ConnectionSender::try_send`].
    pub fn try_disconnect_later(&self, data: u32) -> Result<(), TrySendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed);
        }
        self.shared.try_send_data(Command::DisconnectLater {
            token: self.token,
            data,
        })
    }
}

/// The receiving half of a [`Connection::split`]. Not `Clone`: exactly one
/// task owns the event stream.
#[derive(Debug)]
pub struct ConnectionReceiver {
    peer_id: PeerId,
    remote_addr: SocketAddrV4,
    /// Held (never read) so the peer is disconnected when the last half is
    /// dropped — see [`DisconnectGuard`].
    #[allow(dead_code)]
    guard: Arc<DisconnectGuard>,
    event_rx: EventRx,
}

impl ConnectionReceiver {
    /// The peer's slot index on the host (informational).
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The remote address of the connection.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddrV4 {
        self.remote_addr
    }

    /// Waits for the next event on this connection — see
    /// [`Connection::recv`].
    pub async fn recv(&mut self) -> Option<ConnEvent> {
        self.event_rx.recv().await
    }
}

/// The driver loop. One task, one host, one socket.
struct Pump {
    host: Host,
    socket: UdpSocket,
    config: LenetConfig,
    udp_buf: Vec<u8>,
    /// Senders, upgraded only to hand them to a new [`Incoming`]. No strong
    /// reference is held: when the last handle drops, both command channels
    /// close and the pump's `select!` sees `None` on both receivers.
    shared: Weak<Shared>,
    ctrl_rx: mpsc::UnboundedReceiver<Command>,
    data_rx: mpsc::Receiver<Command>,
    /// Accept queues registered via [`Endpoint::listen`]. Incoming
    /// connections go to the first queue with capacity; an empty list (no
    /// listener yet) rejects every incoming connection.
    accept_txs: Vec<mpsc::Sender<Incoming>>,
    peers: HashMap<PeerId, PeerEntry>,
    tokens: HashMap<Token, PeerId>,
    pending: HashMap<PeerId, PendingConnect>,
    /// Staging buffer for the next outgoing datagram (see
    /// [`Pump::flush_outgoing`]).
    out_buf: BytesMut,
    /// `Some((len, dest))` while a datagram staged in `out_buf` is waiting
    /// for the socket to become writable.
    out_pending: Option<(usize, SocketAddrV4)>,
    /// Set after a socket receive error: the receive branch stays off
    /// until then, so a persistent error cannot spin the pump (and the
    /// pump never sleeps outside its `select!`).
    recv_paused_until: Option<TokioInstant>,
    /// False once the control channel reported closure (all handles gone),
    /// so its `select!` branch stops firing.
    ctrl_open: bool,
    /// Same for the data channel.
    data_open: bool,
    /// `Some(deadline)` once the graceful close has started (shutdown
    /// requested or every handle dropped): the pump runs until every peer
    /// is gone or `deadline` passes.
    closing: Option<TokioInstant>,
    /// Callers of [`Endpoint::shutdown`], told once the pump has stopped.
    shutdown_waiters: Vec<oneshot::Sender<()>>,
}

struct PeerEntry {
    token: Token,
    event_tx: EventTx,
    /// Set by the pump once the connection is torn down, so split sender
    /// halves (which cannot observe the event queue) still fail closed.
    closed: Arc<AtomicBool>,
}

struct PendingConnect {
    token: Token,
    event_tx: EventTx,
    event_rx: EventRx,
    closed: Arc<AtomicBool>,
    addr: SocketAddrV4,
    done: oneshot::Sender<Result<ConnParts, ConnectError>>,
}

/// Engine event with an owned payload, safe to dispatch after the host
/// borrow ends. Receive payloads are copied into a fresh `Bytes`
/// (`Bytes::copy_from_slice`) — the payload view borrows the host's
/// internal buffer, so it cannot outlive the call, and any sharing between
/// queued events must be a genuinely owned buffer.
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

impl Pump {
    fn register_peer(
        &mut self,
        peer: PeerId,
        token: Token,
        event_tx: EventTx,
        closed: Arc<AtomicBool>,
    ) {
        self.peers.insert(
            peer,
            PeerEntry {
                token,
                event_tx,
                closed,
            },
        );
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
        let mut fallback_due = TokioInstant::now() + self.config.fallback_tick;

        loop {
            if let Some(deadline) = self.closing {
                if self.peers.is_empty() || TokioInstant::now() >= deadline {
                    break;
                }
            }

            // Deadline-driven tick: wake at the host's next timer deadline
            // if there is one, otherwise at the fallback tick (and never
            // later than the end of a graceful close).
            let mut wake = self
                .host
                .next_deadline()
                .map(deadline_instant)
                .unwrap_or(fallback_due)
                .min(fallback_due);
            if let Some(deadline) = self.closing {
                wake = wake.min(deadline);
            }
            let recv_paused_until = self.recv_paused_until;

            tokio::select! {
                // Inbound datagram (off for a moment after a socket error).
                recv = self.socket.recv_from(&mut self.udp_buf), if recv_paused_until.is_none() => {
                    match recv {
                        Ok((n, from)) => self.handle_inbound(n, from),
                        Err(e) => {
                            log::warn!("lenet-tokio: recv error: {e}");
                            self.recv_paused_until =
                                Some(TokioInstant::now() + Duration::from_millis(10));
                        }
                    }
                }
                _ = sleep_until(recv_paused_until.unwrap_or_else(TokioInstant::now)), if recv_paused_until.is_some() => {
                    self.recv_paused_until = None;
                }
                // Control command from application handles.
                ctrl = self.ctrl_rx.recv(), if self.ctrl_open => {
                    match ctrl {
                        Some(cmd) => self.apply_command(cmd),
                        // All handles dropped: the command channels are
                        // closed (the pump holds no senders).
                        None => {
                            self.ctrl_open = false;
                            self.start_closing();
                        }
                    }
                }
                // Bulk data command.
                cmd = self.data_rx.recv(), if self.data_open => {
                    match cmd {
                        Some(cmd) => self.apply_command(cmd),
                        None => {
                            self.data_open = false;
                            self.start_closing();
                        }
                    }
                }
                // Timer branch: a deadline (host, fallback, or end of the
                // graceful close) came due.
                _ = sleep_until(wake) => {
                    self.host.service(now_ms());
                    fallback_due = TokioInstant::now() + self.config.fallback_tick;
                }
                // Socket send-buffer drained: retry the staged datagram.
                // (Only armed while `flush_outgoing` left one pending; the
                // retry itself happens in `flush_outgoing` below.)
                res = self.socket.writable(), if self.out_pending.is_some() => {
                    if let Err(e) = res {
                        // e.g. the socket entered an error state; the next
                        // `flush_outgoing` will surface it per-datagram.
                        log::debug!("lenet-tokio: socket writability error: {e}");
                    }
                }
            }

            self.drain();
            // Pack ACKs for any datagram just handled and anything the
            // commands queued; the engine only emits datagrams on flush or
            // service.
            self.host.flush(now_ms());
            self.flush_outgoing();
            self.flush_events();
        }

        self.finish().await;
    }

    /// Begins the graceful close: disconnects every peer, fails pending
    /// connect attempts, and sets the deadline the run loop waits for.
    /// Idempotent.
    fn start_closing(&mut self) {
        if self.closing.is_some() {
            return;
        }
        self.closing = Some(TokioInstant::now() + self.config.shutdown_timeout);
        for (peer, pending) in self.pending.drain() {
            let _ = pending.done.send(Err(ConnectError::Closed));
            self.host.disconnect(peer, 0);
        }
        let peers: Vec<PeerId> = self.peers.keys().copied().collect();
        for peer in peers {
            self.host.disconnect(peer, 0);
        }
    }

    /// Ends whatever is left after the graceful close, sends the last
    /// datagrams, and tells shutdown callers the pump has stopped.
    async fn finish(mut self) {
        // Peers that did not acknowledge in time get one last
        // unacknowledged DISCONNECT (`enet_peer_disconnect_now`).
        for (peer, entry) in self.peers.drain() {
            entry.closed.store(true, Ordering::Release);
            self.host.disconnect_now(peer, 0);
        }
        self.tokens.clear();
        self.drain();
        self.host.flush(now_ms());
        self.flush_outgoing();
        // Give a full socket buffer a short grace period instead of
        // dropping the final datagrams.
        let grace = TokioInstant::now() + Duration::from_millis(100);
        while self.out_pending.is_some() && TokioInstant::now() < grace {
            tokio::select! {
                _ = self.socket.writable() => {}
                _ = sleep_until(grace) => {}
            }
            self.flush_outgoing();
        }
        // Drop the host and every queue before reporting completion, so a
        // caller of `shutdown` sees all handles closed when it resumes.
        let waiters = std::mem::take(&mut self.shutdown_waiters);
        drop(self);
        for done in waiters {
            let _ = done.send(());
        }
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
                token,
                addr,
                channel_count,
                data,
                done,
            } => {
                if self.closing.is_some() {
                    let _ = done.send(Err(ConnectError::Closed));
                    return;
                }
                let Ok(peer) = self.host.connect(addr, channel_count, data) else {
                    let _ = done.send(Err(ConnectError::Rejected));
                    return;
                };
                let (event_tx, event_rx) = make_queue(&self.config.event_queue);
                self.apply_peer_timeout(peer);
                self.pending.insert(
                    peer,
                    PendingConnect {
                        token,
                        event_tx,
                        event_rx,
                        closed: Arc::new(AtomicBool::new(false)),
                        addr,
                        done,
                    },
                );
            }
            Command::CancelConnect { token } => {
                let pending_peer = self
                    .pending
                    .iter()
                    .find_map(|(peer, p)| (p.token == token).then_some(*peer));
                if let Some(peer) = pending_peer {
                    // The handshake is still negotiating: free the peer
                    // slot now instead of retransmitting until timeout.
                    self.pending.remove(&peer);
                    self.host.disconnect(peer, 0);
                } else if let Some(peer) = self.resolve(token) {
                    // The handshake completed while the dropped future was
                    // between resolution and poll: no Connection was ever
                    // handed out, so tear the peer down.
                    self.remove_peer(peer);
                    self.host.disconnect(peer, 0);
                }
                // else: already gone (failed, timed out) — nothing to do.
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
            Command::DisconnectLater { token, data } => {
                if let Some(peer) = self.resolve(token) {
                    self.host.disconnect_later(peer, data);
                }
            }
            Command::Shutdown { done } => {
                self.shutdown_waiters.push(done);
                self.start_closing();
            }
            Command::Listen { tx, ack } => {
                self.accept_txs.push(tx);
                let _ = ack.send(());
            }
            Command::WithHost { f, done } => {
                let _ = done.send(f(&mut self.host));
            }
        }
    }

    /// Drains every queued command without awaiting, so a burst of
    /// application traffic can never starve socket I/O or timers.
    ///
    /// The two channels carry independent concerns (control vs bulk data),
    /// so relative order between them does not matter: per-connection
    /// causality (send vs disconnect) is preserved by routing both through
    /// the data channel.
    fn drain(&mut self) {
        while let Ok(cmd) = self.ctrl_rx.try_recv() {
            self.apply_command(cmd);
        }
        while let Ok(cmd) = self.data_rx.try_recv() {
            self.apply_command(cmd);
        }
    }

    /// Transmits all queued outgoing datagrams without awaiting. On a full
    /// kernel socket buffer the unsent datagram is staged in `out_pending`
    /// and retried once the socket becomes writable (the pump's `select!`
    /// arms a `socket.writable()` branch while a datagram is staged) —
    /// nothing is dropped locally. While the stage is occupied the host's
    /// outgoing queue backs up, which throttles the protocol engine the
    /// same way a slow link would.
    fn flush_outgoing(&mut self) {
        loop {
            if self.out_pending.is_none() {
                let Some(out) = self.host.poll_outgoing() else {
                    break;
                };
                // The view borrows the host buffer; copy it out so it
                // survives awaiting writability.
                self.out_buf.clear();
                self.out_buf.extend_from_slice(out.data);
                let len = self.out_buf.len();
                self.out_pending = Some((len, out.addr));
            }
            let (len, addr) = self.out_pending.expect("staged above");
            match self
                .socket
                .try_send_to(&self.out_buf[..len], SocketAddr::V4(addr))
            {
                Ok(_) => self.out_pending = None,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    // Permanent error (e.g. invalid destination): drop this
                    // datagram and try the next one.
                    log::warn!("lenet-tokio: send error: {e}");
                    self.out_pending = None;
                }
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
                    payload: Bytes::copy_from_slice(payload),
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
                        closed,
                        addr,
                        done,
                    } = pending;
                    if done
                        .send(Ok((token, peer, event_rx, addr, Arc::clone(&closed))))
                        .is_ok()
                    {
                        self.register_peer(peer, token, event_tx, closed);
                    } else {
                        // The Connecting future was dropped: cancel the
                        // attempt instead of leaking an uncontrollable peer.
                        self.host.disconnect(peer, 0);
                    }
                } else {
                    // Incoming connection (server role).
                    if self.closing.is_some() {
                        self.host.disconnect(peer, 0);
                        return;
                    }
                    let Some(remote_addr) = self.host.peer_info(peer).map(|info| info.addr) else {
                        // Cannot happen for an id the engine just reported.
                        log::warn!("lenet-tokio: no info for new peer {}", peer.raw());
                        self.host.disconnect(peer, 0);
                        return;
                    };
                    let token = alloc_token();
                    let (event_tx, event_rx) = make_queue(&self.config.event_queue);
                    let closed = Arc::new(AtomicBool::new(false));
                    self.apply_peer_timeout(peer);
                    self.register_peer(peer, token, event_tx, Arc::clone(&closed));
                    // All handles dropped mid-flight: the driver is going
                    // away; reject instead of handing out a new handle.
                    let Some(shared) = self.shared.upgrade() else {
                        self.remove_peer(peer);
                        self.host.disconnect(peer, 0);
                        return;
                    };
                    self.accept_txs.retain(|tx| !tx.is_closed());
                    let mut delivered = false;
                    let mut incoming = Some(Incoming {
                        token,
                        peer_id: peer,
                        remote_addr,
                        connect_data: data,
                        shared,
                        closed,
                        event_rx: Some(event_rx),
                    });
                    for tx in &self.accept_txs {
                        let Some(inc) = incoming.take() else { break };
                        match tx.try_send(inc) {
                            Ok(()) => {
                                delivered = true;
                                break;
                            }
                            Err(mpsc::error::TrySendError::Full(incoming_back)) => {
                                incoming = Some(incoming_back); // try the next queue
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                // This queue's receiver is gone; try the
                                // next one.
                            }
                        }
                    }
                    if !delivered {
                        // No listener yet, or every accept queue full:
                        // reject instead of stalling.
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
                    entry.closed.store(true, Ordering::Release);
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
                    (
                        Arc::clone(&entry.closed),
                        entry.event_tx.try_send(ConnEvent::Receive {
                            channel_id,
                            payload,
                        }),
                    )
                });
                match outcome {
                    // Delivered, or the peer was already torn down (e.g.
                    // force-closed) and its late events are dropped.
                    Some((_, Ok(()))) | None => {}
                    // Event queue full per policy: force-close the peer
                    // instead of dropping the event or stalling.
                    Some((closed, Err(QueueError::Full))) => {
                        let data = match self.config.event_queue {
                            EventQueuePolicy::BoundedThenDisconnect {
                                disconnect_data, ..
                            } => disconnect_data,
                            EventQueuePolicy::Unbounded => 0,
                        };
                        closed.store(true, Ordering::Release);
                        self.remove_peer(peer);
                        self.host.disconnect(peer, data);
                    }
                    // Receiver dropped while a sender half is still alive:
                    // its events can never be read again, so tear the peer
                    // down rather than leaking a zombie connection.
                    Some((closed, Err(QueueError::Closed))) => {
                        closed.store(true, Ordering::Release);
                        self.remove_peer(peer);
                        self.host.disconnect(peer, 0);
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
