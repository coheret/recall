//! The initial memory-only Recall server. No persistence or public-network mode.

pub mod config;
pub mod engine;
mod session;

pub use config::Config;

use bytes::BytesMut;
use engine::{Engine, EngineHandle};
use recall_core::parse;
use recall_protocol::{Decoder, Reply, Request};
use session::Session;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, timeout_at, Instant};

/// Best-effort notice sent before closing a connection rejected at the
/// configured connection limit. Encoded as a RESP2 error reply.
const CONNECTION_LIMIT_REPLY: &[u8] = b"-ERR max number of clients reached\r\n";

/// Queue bound and per-notice write budget for rejected-connection replies.
const REJECTION_QUEUE_CAPACITY: usize = 32;
const REJECTION_WRITE_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Default)]
pub(crate) struct Metrics {
    active_connections: AtomicUsize,
    total_connections: AtomicU64,
    rejected_connections: AtomicU64,
    total_commands: AtomicU64,
    protocol_errors: AtomicU64,
    io_timeouts: AtomicU64,
}

pub struct Server {
    listener: TcpListener,
    config: Arc<Config>,
    engine: Engine,
    metrics: Arc<Metrics>,
}

impl Server {
    pub async fn bind(config: Config) -> io::Result<Self> {
        config.validate()?;
        let listener = TcpListener::bind(config.bind).await?;
        let engine = Engine::start(&config)?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            engine,
            metrics: Arc::new(Metrics::default()),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Shutdown interrupts socket reads, not admitted mutations. Existing
    /// operations finish and replies get their bounded write window before join.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) -> io::Result<()> {
        let Self {
            listener,
            config,
            engine,
            metrics,
        } = self;
        let permits = Arc::new(Semaphore::new(config.max_connections));
        let mut connections = JoinSet::new();
        let (stopping, shutdown_rx) = watch::channel(false);
        // One bounded writer delivers rejection notices: the accept loop never
        // awaits a rejected client's socket, and excess rejections close
        // silently when the queue is full.
        let (rejection_tx, mut rejection_rx) =
            mpsc::channel::<TcpStream>(REJECTION_QUEUE_CAPACITY);
        let rejections = tokio::spawn(async move {
            while let Some(mut stream) = rejection_rx.recv().await {
                let _ =
                    timeout(REJECTION_WRITE_TIMEOUT, stream.write_all(CONNECTION_LIMIT_REPLY)).await;
            }
        });
        tokio::pin!(shutdown);
        let outcome = loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break Ok(()),
                completed = connections.join_next(), if !connections.is_empty() => {
                    if completed.is_some_and(|result| result.is_err()) {
                        engine::fatal("a connection task panicked");
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => break Err(error),
                    };
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        let rejected = metrics.rejected_connections.fetch_add(1, Ordering::Relaxed) + 1;
                        // The sampled log notice has no client side effects;
                        // stderr backpressure under a flood must not stall
                        // admission either.
                        if rejected == 1 || rejected % 1024 == 0 {
                            eprintln!(
                                "Recall connection limit reached; {rejected} connections rejected so far"
                            );
                        }
                        let _ = rejection_tx.try_send(stream);
                        continue;
                    };
                    let id = metrics.total_connections.fetch_add(1, Ordering::Relaxed) + 1;
                    if id > i64::MAX as u64 { engine::fatal("connection identity exhausted"); }
                    let guard = ConnectionGuard::new(Arc::clone(&metrics), permit);
                    connections.spawn(connection(
                        stream, engine.handle(), Arc::clone(&config), shutdown_rx.clone(),
                        Arc::clone(&metrics), id, guard,
                    ));
                }
            }
        };
        drop(listener);
        drop(rejection_tx);
        let _ = stopping.send(true);
        while let Some(completed) = connections.join_next().await {
            if completed.is_err() {
                engine::fatal("a connection task failed during shutdown");
            }
        }
        engine.shutdown().await?;
        rejections.await.map_err(io::Error::other)?;
        outcome
    }
}

struct ConnectionGuard {
    metrics: Arc<Metrics>,
    _permit: OwnedSemaphorePermit,
}

impl ConnectionGuard {
    fn new(metrics: Arc<Metrics>, permit: OwnedSemaphorePermit) -> Self {
        metrics.active_connections.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            _permit: permit,
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

async fn connection(
    mut stream: TcpStream,
    engine: EngineHandle,
    config: Arc<Config>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
    id: u64,
    _guard: ConnectionGuard,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut decoder = Decoder::new(config.protocol.clone()).map_err(io::Error::other)?;
    let mut input = BytesMut::with_capacity(8192.min(config.protocol.max_frame_bytes));
    let mut session = Session::new(id, &config);
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let request = match next_request(
            &mut stream,
            &mut decoder,
            &mut input,
            &config,
            &mut shutdown,
        )
        .await
        {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(ReadFailure::Protocol(message)) => {
                metrics.protocol_errors.fetch_add(1, Ordering::Relaxed);
                let reply = Reply::error(format!("ERR Protocol error: {message}"));
                let _ = write_reply(&mut stream, reply, &config, &metrics).await;
                return Ok(());
            }
            Err(ReadFailure::Io(error)) => {
                if error.kind() == io::ErrorKind::TimedOut {
                    metrics.io_timeouts.fetch_add(1, Ordering::Relaxed);
                }
                return Err(error);
            }
        };
        let parsed = parse(&request.arguments, &config.commands);
        drop(request); // Commands retain only their necessary, independently owned arguments.
        metrics.total_commands.fetch_add(1, Ordering::Relaxed);
        let (reply, close) = match parsed {
            Ok(command) => session.execute(command, &engine, &config, &metrics).await,
            Err(error) => (error.reply(), false),
        };
        write_reply(&mut stream, reply, &config, &metrics).await?;
        if close {
            return Ok(());
        }
    }
}

enum ReadFailure {
    Protocol(&'static str),
    Io(io::Error),
}

async fn next_request(
    stream: &mut TcpStream,
    decoder: &mut Decoder,
    input: &mut BytesMut,
    config: &Config,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<Request>, ReadFailure> {
    // One absolute deadline per frame prevents a byte-at-a-time client from
    // extending its read budget indefinitely.
    let deadline = Instant::now() + config.read_timeout;
    let mut chunk = [0_u8; 8192];
    loop {
        if let Some(request) = decoder
            .decode(input)
            .map_err(|error| ReadFailure::Protocol(error.0))?
        {
            return Ok(Some(request));
        }
        let remaining = config
            .protocol
            .max_frame_bytes
            .saturating_sub(input.len())
            .min(chunk.len());
        if remaining == 0 {
            return Err(ReadFailure::Protocol("request exceeds configured limit"));
        }
        let received = tokio::select! {
            biased;
            _ = shutdown.changed() => return Ok(None),
            result = timeout_at(deadline, stream.read(&mut chunk[..remaining])) => {
                result.map_err(|_| ReadFailure::Io(io::Error::new(io::ErrorKind::TimedOut, "request read deadline exceeded")))?
                    .map_err(ReadFailure::Io)?
            }
        };
        if received == 0 {
            return if input.is_empty() {
                Ok(None)
            } else {
                Err(ReadFailure::Protocol("truncated request"))
            };
        }
        input.extend_from_slice(&chunk[..received]);
    }
}
async fn write_reply(
    stream: &mut TcpStream,
    mut reply: Reply,
    config: &Config,
    metrics: &Metrics,
) -> io::Result<()> {
    if reply
        .encoded_len()
        .is_none_or(|length| length > config.max_reply_bytes)
    {
        reply = Reply::error("ERR response exceeds configured limit");
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(reply.encoded_len().expect("bounded response length"))
        .map_err(io::Error::other)?;
    reply.encode(&mut output);
    drop(reply);
    match timeout(config.write_timeout, stream.write_all(&output)).await {
        Ok(result) => result,
        Err(_) => {
            metrics.io_timeouts.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "response write deadline exceeded",
            ))
        }
    }
}
