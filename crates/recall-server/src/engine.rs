//! Mutable state lives only on dedicated owner threads. The baseline multi-key
//! coordinator owns accepted work independently of frontend future lifetimes.

use crate::config::Config;
use bytes::Bytes;
use recall_core::{EngineError, Operation, Prepared, Shard, ShardStats};
use recall_protocol::Reply;
use std::collections::{hash_map::RandomState, BTreeMap};
use std::hash::BuildHasher;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout, MissedTickBehavior};

pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
    }
}

struct DataRequest {
    operation: Operation,
    response: oneshot::Sender<Reply>,
    _credit: OwnedSemaphorePermit,
}

enum Control {
    Reserve {
        id: u64,
        response: oneshot::Sender<()>,
    },
    Prepare {
        id: u64,
        operation: Operation,
        now_ms: i64,
        response: oneshot::Sender<Result<Reply, EngineError>>,
    },
    Apply {
        id: u64,
        response: oneshot::Sender<()>,
    },
    Release {
        id: u64,
        response: oneshot::Sender<()>,
    },
    Stats(oneshot::Sender<ShardStats>),
    Stop,
}

#[derive(Clone)]
struct Owner {
    data: mpsc::Sender<DataRequest>,
    control: mpsc::Sender<Control>,
    credits: Arc<Semaphore>,
}

enum CrossRequest {
    Execute(DataRequest),
    Stop,
}

struct Routing {
    owners: Vec<Owner>,
    hash: RandomState,
    cross: mpsc::Sender<CrossRequest>,
    cross_credits: Arc<Semaphore>,
    queue_bytes: usize,
    admission_timeout: Duration,
    closing: AtomicBool,
}

#[derive(Clone)]
pub struct EngineHandle {
    routing: Arc<Routing>,
}

pub struct Engine {
    handle: EngineHandle,
    coordinator: JoinHandle<()>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Engine {
    pub fn start(config: &Config) -> io::Result<Self> {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    pub fn with_clock(config: &Config, clock: Arc<dyn Clock>) -> io::Result<Self> {
        config.validate()?;
        // Reserve all storage/runtime resources before publishing a usable handle.
        let mut startup = Vec::with_capacity(config.workers);
        let mut owners = Vec::with_capacity(config.workers);
        for index in 0..config.workers {
            let shard = Shard::new(config.shard_limits(index)).map_err(io::Error::other)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()?;
            let (data, data_rx) = mpsc::channel(config.queue_capacity);
            // Reserved control capacity is independent of client-data saturation.
            let (control, control_rx) = mpsc::channel(8);
            owners.push(Owner {
                data,
                control,
                credits: Arc::new(Semaphore::new(config.queue_bytes)),
            });
            startup.push((shard, runtime, data_rx, control_rx));
        }
        let mut threads = Vec::with_capacity(config.workers);
        for (index, (shard, runtime, data_rx, control_rx)) in startup.into_iter().enumerate() {
            let clock = Arc::clone(&clock);
            let expiry_interval = config.expiry_interval;
            let expiry_batch = config.expiry_batch;
            let thread = thread::Builder::new()
                .name(format!("recall-owner-{index}"))
                .spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime.block_on(owner_loop(
                            shard,
                            data_rx,
                            control_rx,
                            clock,
                            expiry_interval,
                            expiry_batch,
                        ));
                    }));
                    if result.is_err() {
                        fatal("an owner panicked");
                    }
                });
            match thread {
                Ok(thread) => threads.push(thread),
                Err(error) => {
                    // No handle was published; channel closure stops already-created owners.
                    drop(owners);
                    for thread in threads {
                        let _ = thread.join();
                    }
                    return Err(error);
                }
            }
        }
        let hash = RandomState::new();
        let (cross, cross_rx) = mpsc::channel(config.queue_capacity);
        let coordinator = tokio::spawn(coordinator_loop(
            owners.clone(),
            hash.clone(),
            cross_rx,
            clock,
            config.max_reply_bytes,
        ));
        let handle = EngineHandle {
            routing: Arc::new(Routing {
                owners,
                hash,
                cross,
                cross_credits: Arc::new(Semaphore::new(config.queue_bytes)),
                queue_bytes: config.queue_bytes,
                admission_timeout: config.admission_timeout,
                closing: AtomicBool::new(false),
            }),
        };
        Ok(Self {
            handle,
            coordinator,
            threads,
        })
    }

    pub fn handle(&self) -> EngineHandle {
        self.handle.clone()
    }

    /// Closes admission, finishes every accepted cross-owner decision, then
    /// drains accepted single-owner work before joining the owner threads.
    pub async fn shutdown(self) -> io::Result<()> {
        self.handle.routing.closing.store(true, Ordering::Release);
        self.handle
            .routing
            .cross
            .send(CrossRequest::Stop)
            .await
            .unwrap_or_else(|_| fatal("coordinator stopped before shutdown"));
        self.coordinator
            .await
            .unwrap_or_else(|_| fatal("coordinator task failed"));
        for owner in &self.handle.routing.owners {
            owner
                .control
                .send(Control::Stop)
                .await
                .unwrap_or_else(|_| fatal("owner stopped before shutdown"));
        }
        tokio::task::spawn_blocking(move || {
            for thread in self.threads {
                thread
                    .join()
                    .unwrap_or_else(|_| fatal("owner thread failed during shutdown"));
            }
        })
        .await
        .map_err(io::Error::other)?;
        Ok(())
    }
}

impl EngineHandle {
    pub fn owner_for(&self, key: &[u8]) -> usize {
        route(&self.routing.hash, self.routing.owners.len(), key)
    }

    pub fn worker_count(&self) -> usize {
        self.routing.owners.len()
    }

    pub async fn execute(&self, operation: Operation) -> Reply {
        if self.routing.closing.load(Ordering::Acquire) {
            return Reply::error("BUSY Recall is shutting down");
        }
        let keys = operation.keys();
        let Some(first_key) = keys.first() else {
            return Reply::error("ERR empty key set");
        };
        let first_owner = self.owner_for(first_key);
        let single_owner = keys.iter().all(|key| self.owner_for(key) == first_owner);
        // Payload credits plus a conservative per-argument/request metadata charge.
        let charge = operation
            .payload_bytes()
            .saturating_add(keys.len().saturating_mul(128))
            .saturating_add(256);
        if charge > self.routing.queue_bytes || charge > u32::MAX as usize {
            return Reply::error("BUSY command exceeds queue byte capacity");
        }
        drop(keys);
        let (response, result) = oneshot::channel();
        let routing = &self.routing;
        let admission = async {
            let credits = if single_owner {
                Arc::clone(&routing.owners[first_owner].credits)
            } else {
                Arc::clone(&routing.cross_credits)
            };
            let credit = credits
                .acquire_many_owned(charge as u32)
                .await
                .map_err(|_| ())?;
            let request = DataRequest {
                operation,
                response,
                _credit: credit,
            };
            if single_owner {
                routing.owners[first_owner]
                    .data
                    .send(request)
                    .await
                    .map_err(|_| ())
            } else {
                routing
                    .cross
                    .send(CrossRequest::Execute(request))
                    .await
                    .map_err(|_| ())
            }
        };
        match timeout(routing.admission_timeout, admission).await {
            Err(_) => Reply::error("BUSY admission queue deadline exceeded"),
            Ok(Err(())) => Reply::error("BUSY Recall is shutting down"),
            Ok(Ok(())) => result
                .await
                .unwrap_or_else(|_| fatal("accepted command lost its completion")),
        }
    }

    /// Operational samples, not a linearizable global keyspace snapshot.
    pub async fn stats(&self) -> Vec<ShardStats> {
        let mut stats = Vec::with_capacity(self.routing.owners.len());
        for owner in &self.routing.owners {
            let (response, result) = oneshot::channel();
            if owner.control.send(Control::Stats(response)).await.is_err() {
                break;
            }
            if let Ok(value) = result.await {
                stats.push(value);
            }
        }
        stats
    }
}

struct Reservation {
    id: u64,
    prepared: Option<Prepared>,
}

async fn owner_loop(
    mut shard: Shard,
    mut data: mpsc::Receiver<DataRequest>,
    mut controls: mpsc::Receiver<Control>,
    clock: Arc<dyn Clock>,
    expiry_interval: Duration,
    expiry_batch: usize,
) {
    let mut reservation: Option<Reservation> = None;
    let mut tick = interval(expiry_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut stopping = false;
    let mut data_turn = false;
    loop {
        // An ongoing stream of cross-owner reservations cannot starve queued local work.
        if data_turn && reservation.is_none() {
            data_turn = false;
            if let Ok(request) = data.try_recv() {
                execute_local(&mut shard, request, clock.now_ms());
            }
        }
        tokio::select! {
            biased;
            control = controls.recv(), if !stopping => {
                let Some(control) = control else {
                    if reservation.is_some() { fatal("control channel closed while reserved"); }
                    data.close();
                    stopping = true;
                    continue;
                };
                match control {
                    Control::Reserve { id, response } => {
                        assert!(reservation.is_none(), "duplicate owner reservation");
                        reservation = Some(Reservation { id, prepared: None });
                        let _ = response.send(());
                    }
                    Control::Prepare { id, operation, now_ms, response } => {
                        let active = reservation.as_mut().expect("prepare without reservation");
                        assert_eq!(active.id, id);
                        assert!(active.prepared.is_none());
                        let result = shard.prepare(&operation, now_ms).map(|prepared| {
                            let reply = prepared.response().clone();
                            active.prepared = Some(prepared);
                            reply
                        });
                        let _ = response.send(result);
                    }
                    Control::Apply { id, response } => {
                        let active = reservation.as_mut().expect("apply without reservation");
                        assert_eq!(active.id, id);
                        shard.apply(active.prepared.take().expect("apply without preparation"));
                        let _ = response.send(());
                    }
                    Control::Release { id, response } => {
                        let active = reservation.take().expect("release without reservation");
                        assert_eq!(active.id, id);
                        drop(active); // Unapplied preparations are aborted here.
                        data_turn = true;
                        let _ = response.send(());
                    }
                    Control::Stats(response) => {
                        let _ = response.send(shard.stats());
                        data_turn = true;
                    }
                    Control::Stop => {
                        assert!(reservation.is_none(), "shutdown during a coordinated operation");
                        data.close();
                        stopping = true;
                    }
                }
            }
            _ = tick.tick(), if reservation.is_none() && !stopping => {
                shard.expire_due(clock.now_ms(), expiry_batch);
                data_turn = true;
            }
            request = data.recv(), if reservation.is_none() => {
                match request {
                    Some(request) => execute_local(&mut shard, request, clock.now_ms()),
                    None => break,
                }
            }
        }
    }
}

fn execute_local(shard: &mut Shard, request: DataRequest, now_ms: i64) {
    let reply = shard
        .execute(&request.operation, now_ms)
        .unwrap_or_else(EngineError::reply);
    // Dropping a disconnected receiver does not undo the operation or retain credits.
    let _ = request.response.send(reply);
}

struct Part {
    owner: usize,
    operation: Operation,
    positions: Vec<usize>,
}

fn split(operation: &Operation, hash: &RandomState, owners: usize) -> Vec<Part> {
    match operation {
        Operation::MultiSet(pairs) => {
            let mut groups: BTreeMap<usize, Vec<(Bytes, Bytes)>> = BTreeMap::new();
            for (key, value) in pairs {
                groups
                    .entry(route(hash, owners, key))
                    .or_default()
                    .push((key.clone(), value.clone()));
            }
            groups
                .into_iter()
                .map(|(owner, pairs)| Part {
                    owner,
                    operation: Operation::MultiSet(pairs),
                    positions: vec![],
                })
                .collect()
        }
        Operation::Delete(keys) | Operation::Exists(keys) | Operation::MultiGet(keys) => {
            let mut groups: BTreeMap<usize, (Vec<Bytes>, Vec<usize>)> = BTreeMap::new();
            for (index, key) in keys.iter().enumerate() {
                let group = groups.entry(route(hash, owners, key)).or_default();
                group.0.push(key.clone());
                group.1.push(index);
            }
            groups
                .into_iter()
                .map(|(owner, (keys, positions))| Part {
                    owner,
                    operation: match operation {
                        Operation::Delete(_) => Operation::Delete(keys),
                        Operation::Exists(_) => Operation::Exists(keys),
                        _ => Operation::MultiGet(keys),
                    },
                    positions,
                })
                .collect()
        }
        _ => fatal("single-key operation reached the cross-owner coordinator"),
    }
}

async fn coordinator_loop(
    owners: Vec<Owner>,
    hash: RandomState,
    mut requests: mpsc::Receiver<CrossRequest>,
    clock: Arc<dyn Clock>,
    max_reply_bytes: usize,
) {
    // Tokio isolates task panics by default. That is unsafe for a coordinator
    // that may own shard reservations: unexpected cancellation/panic is fatal.
    let mut completion_guard = CoordinatorCompletionGuard { finished: false };
    let mut id = 0_u64;
    while let Some(request) = requests.recv().await {
        match request {
            CrossRequest::Stop => requests.close(),
            CrossRequest::Execute(request) => {
                id = id
                    .checked_add(1)
                    .unwrap_or_else(|| fatal("coordinator identity exhausted"));
                let reply = coordinate(
                    &owners,
                    &hash,
                    &request.operation,
                    clock.as_ref(),
                    id,
                    max_reply_bytes,
                )
                .await;
                let _ = request.response.send(reply);
            }
        }
    }
    completion_guard.finished = true;
}

struct CoordinatorCompletionGuard {
    finished: bool,
}

impl Drop for CoordinatorCompletionGuard {
    fn drop(&mut self) {
        if !self.finished {
            fatal("coordinator execution was interrupted");
        }
    }
}

async fn coordinate(
    owners: &[Owner],
    hash: &RandomState,
    operation: &Operation,
    clock: &dyn Clock,
    id: u64,
    max_reply_bytes: usize,
) -> Reply {
    let parts = split(operation, hash, owners.len());
    // Sorted ownership order; the baseline has only one cross-owner context.
    for part in &parts {
        let (response, received) = oneshot::channel();
        control(&owners[part.owner], Control::Reserve { id, response }).await;
        completed(received).await;
    }
    let now_ms = clock.now_ms();
    let mut results = Vec::with_capacity(parts.len());
    let mut error = None;
    for part in &parts {
        let (response, received) = oneshot::channel();
        control(
            &owners[part.owner],
            Control::Prepare {
                id,
                operation: part.operation.clone(),
                now_ms,
                response,
            },
        )
        .await;
        match received
            .await
            .unwrap_or_else(|_| fatal("lost prepare completion"))
        {
            Ok(reply) => results.push(reply),
            Err(failure) => {
                error = Some(failure.reply());
                break;
            }
        }
    }
    let response = if let Some(error) = error {
        error
    } else {
        let reply = combine(operation, &parts, results);
        if reply
            .encoded_len()
            .is_none_or(|bytes| bytes > max_reply_bytes)
        {
            Reply::error("ERR response exceeds configured limit")
        } else {
            // Every owner remains reserved until ALL installations are complete.
            for part in &parts {
                let (response, received) = oneshot::channel();
                control(&owners[part.owner], Control::Apply { id, response }).await;
                completed(received).await;
            }
            reply
        }
    };
    for part in &parts {
        let (response, received) = oneshot::channel();
        control(&owners[part.owner], Control::Release { id, response }).await;
        completed(received).await;
    }
    response
}

fn combine(operation: &Operation, parts: &[Part], results: Vec<Reply>) -> Reply {
    match operation {
        Operation::MultiSet(_) => Reply::ok(),
        Operation::MultiGet(keys) => {
            let mut response = vec![Reply::Bulk(None); keys.len()];
            for (part, result) in parts.iter().zip(results) {
                let Reply::Array(values) = result else {
                    fatal("invalid multi-get participant response")
                };
                assert_eq!(part.positions.len(), values.len());
                for (&position, value) in part.positions.iter().zip(values) {
                    response[position] = value;
                }
            }
            Reply::Array(response)
        }
        Operation::Delete(_) | Operation::Exists(_) => Reply::Integer(
            results
                .into_iter()
                .map(|result| {
                    let Reply::Integer(value) = result else {
                        fatal("invalid integer participant response")
                    };
                    value
                })
                .sum(),
        ),
        _ => fatal("invalid coordinated operation"),
    }
}

async fn control(owner: &Owner, message: Control) {
    owner
        .control
        .send(message)
        .await
        .unwrap_or_else(|_| fatal("lost owner control channel"));
}

async fn completed(received: oneshot::Receiver<()>) {
    received
        .await
        .unwrap_or_else(|_| fatal("lost owner control completion"));
}

fn route(hash: &RandomState, owners: usize, key: &[u8]) -> usize {
    (hash.hash_one(key) % owners as u64) as usize
}

pub(crate) fn fatal(message: &str) -> ! {
    // Write straight to the stderr descriptor: the test harness captures
    // eprintln! per test, which would hide the reason for a process abort.
    use std::io::Write as _;
    let line = format!("Recall invariant failure: {message}; stopping rather than serving uncertain state\n");
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
    std::process::abort()
}

#[cfg(test)]
mod tests {
    use super::*;
    use recall_core::command::{Condition, Expiry};
    use std::sync::atomic::AtomicI64;

    struct TestClock(AtomicI64);

    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn config() -> Config {
        Config {
            workers: 2,
            keys_per_worker: 128,
            max_payload_bytes: 1024 * 1024,
            ..Config::default()
        }
    }

    fn different_keys(handle: &EngineHandle) -> (Bytes, Bytes) {
        let first = Bytes::from_static(b"first");
        for index in 0..1000 {
            let second = Bytes::from(format!("second-{index}"));
            if handle.owner_for(&first) != handle.owner_for(&second) {
                return (first, second);
            }
        }
        panic!("could not locate keys on different owners")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_clients_do_not_lose_counter_updates() {
        let engine = Engine::start(&config()).unwrap();
        let mut clients = Vec::new();
        for _ in 0..8 {
            let handle = engine.handle();
            clients.push(tokio::spawn(async move {
                for _ in 0..100 {
                    assert!(matches!(
                        handle
                            .execute(Operation::Increment {
                                key: Bytes::from_static(b"n"),
                                amount: 1,
                                subtract: false,
                            })
                            .await,
                        Reply::Integer(_)
                    ));
                }
            }));
        }
        for client in clients {
            client.await.unwrap();
        }
        assert_eq!(
            engine
                .handle()
                .execute(Operation::Get(Bytes::from_static(b"n")))
                .await,
            Reply::bulk("800")
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overlapping_multi_key_reads_never_observe_partial_writes() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        let (a, b) = different_keys(&handle);
        handle
            .execute(Operation::MultiSet(vec![
                (a.clone(), Bytes::from_static(b"0")),
                (b.clone(), Bytes::from_static(b"0")),
            ]))
            .await;
        let writer = handle.clone();
        let (wa, wb) = (a.clone(), b.clone());
        let task = tokio::spawn(async move {
            for i in 0..200 {
                let value = Bytes::from(i.to_string());
                assert_eq!(
                    writer
                        .execute(Operation::MultiSet(vec![
                            (wa.clone(), value.clone()),
                            (wb.clone(), value)
                        ]))
                        .await,
                    Reply::ok()
                );
            }
        });
        for _ in 0..200 {
            let reply = handle
                .execute(Operation::MultiGet(vec![a.clone(), b.clone(), a.clone()]))
                .await;
            let Reply::Array(values) = reply else {
                panic!("expected an array")
            };
            assert_eq!(values[0], values[1]);
            assert_eq!(values[1], values[2]);
        }
        task.await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn participant_capacity_failure_aborts_all_requested_writes() {
        let mut config = config();
        config.max_payload_bytes = 128;
        let engine = Engine::start(&config).unwrap();
        let handle = engine.handle();
        let (a, b) = different_keys(&handle);
        assert_eq!(
            handle
                .execute(Operation::Set {
                    key: a.clone(),
                    value: Bytes::from_static(b"old"),
                    condition: Condition::Always,
                    expiry: Expiry::Clear,
                })
                .await,
            Reply::ok()
        );
        let reply = handle
            .execute(Operation::MultiSet(vec![
                (a.clone(), Bytes::from_static(b"new")),
                (b.clone(), Bytes::from(vec![0; 100])),
            ]))
            .await;
        assert!(matches!(reply, Reply::Error(_)));
        assert_eq!(handle.execute(Operation::Get(a)).await, Reply::bulk("old"));
        assert_eq!(handle.execute(Operation::Get(b)).await, Reply::Bulk(None));
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unrelated_owner_progresses_during_a_reservation() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        let (a, b) = different_keys(&handle);
        let owner = &handle.routing.owners[handle.owner_for(&a)];
        let (response, received) = oneshot::channel();
        control(owner, Control::Reserve { id: 999, response }).await;
        completed(received).await;
        let result = timeout(
            Duration::from_secs(2),
            handle.execute(Operation::Increment {
                key: b,
                amount: 1,
                subtract: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result, Reply::Integer(1));
        let (response, received) = oneshot::channel();
        control(owner, Control::Release { id: 999, response }).await;
        completed(received).await;
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dropped_cross_command_receiver_does_not_strand_reservations() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        let (a, b) = different_keys(&handle);
        let credit = Arc::clone(&handle.routing.cross_credits)
            .acquire_many_owned(512)
            .await
            .unwrap();
        let (response, receiver) = oneshot::channel();
        handle
            .routing
            .cross
            .send(CrossRequest::Execute(DataRequest {
                operation: Operation::MultiSet(vec![
                    (a.clone(), Bytes::from_static(b"1")),
                    (b.clone(), Bytes::from_static(b"1")),
                ]),
                response,
                _credit: credit,
            }))
            .await
            .unwrap_or_else(|_| panic!("coordinator closed"));
        drop(receiver);
        // Same coordinator queue supplies an ordering barrier after the abandoned request.
        assert_eq!(
            timeout(
                Duration::from_secs(2),
                handle.execute(Operation::MultiGet(vec![a, b]))
            )
            .await
            .unwrap(),
            Reply::Array(vec![Reply::bulk("1"), Reply::bulk("1")])
        );
        // The abandoned request's cross-owner byte credits were released as well.
        assert_eq!(
            handle.routing.cross_credits.available_permits(),
            config().queue_bytes
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn full_data_queue_does_not_block_control_release_or_leak_byte_credits() {
        let config = Config {
            queue_capacity: 1,
            admission_timeout: Duration::from_millis(20),
            ..config()
        };
        let engine = Engine::start(&config).unwrap();
        let handle = engine.handle();
        let key = Bytes::from_static(b"blocked");
        let owner = &handle.routing.owners[handle.owner_for(&key)];
        let (response, received) = oneshot::channel();
        control(owner, Control::Reserve { id: 999, response }).await;
        completed(received).await;

        let credit = Arc::clone(&owner.credits)
            .acquire_many_owned(512)
            .await
            .unwrap();
        let (response, result) = oneshot::channel();
        owner
            .data
            .send(DataRequest {
                operation: Operation::Increment {
                    key: key.clone(),
                    amount: 1,
                    subtract: false,
                },
                response,
                _credit: credit,
            })
            .await
            .unwrap_or_else(|_| panic!("owner closed"));
        let denied = handle
            .execute(Operation::Increment {
                key: key.clone(),
                amount: 100,
                subtract: false,
            })
            .await;
        assert!(matches!(denied, Reply::Error(_)));

        let (response, released) = oneshot::channel();
        control(owner, Control::Release { id: 999, response }).await;
        timeout(Duration::from_secs(2), completed(released))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), result)
                .await
                .unwrap()
                .unwrap(),
            Reply::Integer(1)
        );
        assert_eq!(handle.execute(Operation::Get(key)).await, Reply::bulk("1"));
        // A control round trip confirms the preceding data turn and credit drop finished.
        let (response, stats) = oneshot::channel();
        control(owner, Control::Stats(response)).await;
        stats.await.unwrap();
        assert_eq!(owner.credits.available_permits(), config.queue_bytes);
        engine.shutdown().await.unwrap();
    }

    /// Two racing cross-owner writers must never interleave: every read
    /// observes one writer's complete pair, and the final state is one whole
    /// history, not a mixture.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conflicting_cross_owner_writes_resolve_to_one_complete_history() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        let (a, b) = different_keys(&handle);
        let mut writers = Vec::new();
        for prefix in ["A", "B"] {
            let writer = handle.clone();
            let (wa, wb) = (a.clone(), b.clone());
            writers.push(tokio::spawn(async move {
                for index in 0..100 {
                    // Both slots carry the same value, so any mixture of the
                    // two writers is observable as an inequality.
                    let value = Bytes::from(format!("{prefix}{index}"));
                    assert_eq!(
                        writer
                            .execute(Operation::MultiSet(vec![
                                (wa.clone(), value.clone()),
                                (wb.clone(), value)
                            ]))
                            .await,
                        Reply::ok()
                    );
                }
            }));
        }
        for _ in 0..200 {
            let reply = handle
                .execute(Operation::MultiGet(vec![a.clone(), b.clone()]))
                .await;
            let Reply::Array(values) = reply else {
                panic!("expected an array")
            };
            match (&values[0], &values[1]) {
                (Reply::Bulk(Some(left)), Reply::Bulk(Some(right))) => {
                    assert_eq!(left, right)
                }
                (Reply::Bulk(None), Reply::Bulk(None)) => {}
                _ => panic!("observed a partial cross-owner write"),
            }
        }
        for writer in writers {
            writer.await.unwrap();
        }
        let reply = handle.execute(Operation::MultiGet(vec![a, b])).await;
        let Reply::Array(values) = reply else {
            panic!("expected an array")
        };
        assert_eq!(values[0], values[1]);
        assert!(
            matches!(&values[0], Reply::Bulk(Some(value)) if value.starts_with(b"A") || value.starts_with(b"B"))
        );
        engine.shutdown().await.unwrap();
    }

    /// A cross-owner delete must never be observed halfway: EXISTS over the
    /// pair only ever counts both keys or neither.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_owner_deletes_and_reads_are_atomic() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        let (a, b) = different_keys(&handle);
        let writer = handle.clone();
        let (wa, wb) = (a.clone(), b.clone());
        let task = tokio::spawn(async move {
            for _ in 0..100 {
                assert_eq!(
                    writer
                        .execute(Operation::MultiSet(vec![
                            (wa.clone(), Bytes::from_static(b"1")),
                            (wb.clone(), Bytes::from_static(b"1")),
                        ]))
                        .await,
                    Reply::ok()
                );
                assert_eq!(
                    writer
                        .execute(Operation::Delete(vec![wa.clone(), wb.clone()]))
                        .await,
                    Reply::Integer(2)
                );
            }
        });
        for _ in 0..200 {
            let reply = handle
                .execute(Operation::Exists(vec![a.clone(), b.clone()]))
                .await;
            assert!(
                matches!(reply, Reply::Integer(0) | Reply::Integer(2)),
                "observed a partial cross-owner delete: {reply:?}"
            );
        }
        task.await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn execute_after_shutdown_returns_busy() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        engine.shutdown().await.unwrap();
        assert_eq!(
            handle
                .execute(Operation::Get(Bytes::from_static(b"k")))
                .await,
            Reply::error("BUSY Recall is shutting down")
        );
    }

    /// Shutdown drains already-accepted work and rejects new admissions with a
    /// bounded error instead of hanging the caller.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_completes_or_rejects_in_flight_work_without_hanging() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        let task = tokio::spawn(async move {
            for _ in 0..100 {
                let reply = handle
                    .execute(Operation::Increment {
                        key: Bytes::from_static(b"n"),
                        amount: 1,
                        subtract: false,
                    })
                    .await;
                match reply {
                    Reply::Integer(_) => {}
                    other => assert_eq!(other, Reply::error("BUSY Recall is shutting down")),
                }
            }
        });
        engine.shutdown().await.unwrap();
        timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    /// When an owner's byte credits are exhausted, admission waits for the
    /// configured deadline and then rejects; releasing credits lets the same
    /// command through.
    #[tokio::test]
    async fn byte_saturated_admission_times_out_and_recovers() {
        let config = Config {
            queue_bytes: 1024,
            admission_timeout: Duration::from_millis(20),
            ..config()
        };
        let engine = Engine::start(&config).unwrap();
        let handle = engine.handle();
        let key = Bytes::from_static(b"hot");
        let owner = &handle.routing.owners[handle.owner_for(&key)];
        // Charge for the write below is 687 bytes; hold all but 512.
        let held = Arc::clone(&owner.credits)
            .acquire_many_owned(512)
            .await
            .unwrap();
        let denied = handle
            .execute(Operation::Set {
                key: key.clone(),
                value: Bytes::from(vec![0; 300]),
                condition: Condition::Always,
                expiry: Expiry::Clear,
            })
            .await;
        assert_eq!(
            denied,
            Reply::error("BUSY admission queue deadline exceeded")
        );
        drop(held);
        assert_eq!(
            handle
                .execute(Operation::Set {
                    key: key.clone(),
                    value: Bytes::from(vec![0; 300]),
                    condition: Condition::Always,
                    expiry: Expiry::Clear,
                })
                .await,
            Reply::ok()
        );
        engine.shutdown().await.unwrap();
    }

    /// A command whose charge exceeds the entire queue byte budget is rejected
    /// without waiting, and admission stays healthy for ordinary commands.
    #[tokio::test]
    async fn commands_exceeding_queue_byte_capacity_are_rejected_immediately() {
        let config = Config {
            queue_bytes: 1024,
            ..config()
        };
        let engine = Engine::start(&config).unwrap();
        let handle = engine.handle();
        // Charge is 1 + 1024 payload bytes plus per-key and request overhead.
        let reply = handle
            .execute(Operation::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from(vec![0; 1024]),
                condition: Condition::Always,
                expiry: Expiry::Clear,
            })
            .await;
        assert_eq!(
            reply,
            Reply::error("BUSY command exceeds queue byte capacity")
        );
        assert_eq!(
            handle
                .execute(Operation::Get(Bytes::from_static(b"k")))
                .await,
            Reply::Bulk(None)
        );
        engine.shutdown().await.unwrap();
    }

    /// A single-owner request whose response receiver is gone still applies,
    /// and its byte credits return to the owner.
    #[tokio::test]
    async fn dropped_single_owner_receiver_still_applies_and_releases_credits() {
        let config = config();
        let engine = Engine::start(&config).unwrap();
        let handle = engine.handle();
        let key = Bytes::from_static(b"abandoned");
        let owner = &handle.routing.owners[handle.owner_for(&key)];
        let credit = Arc::clone(&owner.credits)
            .acquire_many_owned(512)
            .await
            .unwrap();
        let (response, receiver) = oneshot::channel();
        owner
            .data
            .send(DataRequest {
                operation: Operation::Set {
                    key: key.clone(),
                    value: Bytes::from_static(b"kept"),
                    condition: Condition::Always,
                    expiry: Expiry::Clear,
                },
                response,
                _credit: credit,
            })
            .await
            .unwrap_or_else(|_| panic!("owner closed"));
        drop(receiver);
        // The same owner's data channel orders the read behind the abandoned write.
        assert_eq!(
            handle.execute(Operation::Get(key)).await,
            Reply::bulk("kept")
        );
        // A control round trip confirms the data turn and credit drop finished.
        let (response, stats) = oneshot::channel();
        control(owner, Control::Stats(response)).await;
        stats.await.unwrap();
        assert_eq!(owner.credits.available_permits(), config.queue_bytes);
        engine.shutdown().await.unwrap();
    }

    /// Engine time comes from the injected clock: expiration follows the
    /// deterministic now_ms given to each command, not wall-clock time.
    #[tokio::test]
    async fn injected_clock_drives_expiration_without_wall_time() {
        let clock = Arc::new(TestClock(AtomicI64::new(1000)));
        let engine = Engine::with_clock(&config(), clock.clone()).unwrap();
        let handle = engine.handle();
        assert_eq!(
            handle
                .execute(Operation::Set {
                    key: Bytes::from_static(b"ttl"),
                    value: Bytes::from_static(b"v"),
                    condition: Condition::Always,
                    expiry: Expiry::AfterMilliseconds(1000),
                })
                .await,
            Reply::ok()
        );
        assert_eq!(
            handle
                .execute(Operation::Ttl {
                    key: Bytes::from_static(b"ttl"),
                    milliseconds: true,
                })
                .await,
            Reply::Integer(1000)
        );
        clock.0.store(1999, Ordering::Relaxed);
        assert_eq!(
            handle
                .execute(Operation::Get(Bytes::from_static(b"ttl")))
                .await,
            Reply::bulk("v")
        );
        // The deadline is inclusive: at exactly now + duration the key is gone.
        clock.0.store(2000, Ordering::Relaxed);
        assert_eq!(
            handle
                .execute(Operation::Get(Bytes::from_static(b"ttl")))
                .await,
            Reply::Bulk(None)
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stats_cover_every_owner_and_count_applied_commands() {
        let engine = Engine::start(&config()).unwrap();
        let handle = engine.handle();
        let (a, b) = different_keys(&handle);
        assert_eq!(
            handle
                .execute(Operation::Set {
                    key: a.clone(),
                    value: Bytes::from_static(b"1"),
                    condition: Condition::Always,
                    expiry: Expiry::Clear,
                })
                .await,
            Reply::ok()
        );
        assert_eq!(
            handle
                .execute(Operation::Increment {
                    key: b.clone(),
                    amount: 2,
                    subtract: false,
                })
                .await,
            Reply::Integer(2)
        );
        let stats = handle.stats().await;
        assert_eq!(stats.len(), handle.worker_count());
        assert_eq!(stats.iter().map(|owner| owner.keys).sum::<usize>(), 2);
        assert_eq!(
            stats
                .iter()
                .map(|owner| owner.applied_commands)
                .sum::<u64>(),
            2
        );
        assert_eq!(
            stats.iter().map(|owner| owner.payload_bytes).sum::<usize>(),
            a.len() + 1 + b.len() + 1
        );
        engine.shutdown().await.unwrap();
    }

    /// Multi-get replies follow argument order even when keys are scattered
    /// across every owner and requested in reverse.
    #[tokio::test]
    async fn multi_get_reassembles_scattered_keys_in_argument_order() {
        let config = Config {
            workers: 4,
            ..config()
        };
        let engine = Engine::start(&config).unwrap();
        let handle = engine.handle();
        // Find one key per owner.
        let mut keys: Vec<Bytes> = Vec::new();
        for index in 0..10_000 {
            let candidate = Bytes::from(format!("scatter-{index}"));
            if !keys
                .iter()
                .any(|key| handle.owner_for(key) == handle.owner_for(&candidate))
            {
                keys.push(candidate);
            }
            if keys.len() == 4 {
                break;
            }
        }
        assert_eq!(keys.len(), 4);
        let pairs: Vec<(Bytes, Bytes)> = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (key.clone(), Bytes::from(index.to_string())))
            .collect();
        assert_eq!(
            handle.execute(Operation::MultiSet(pairs)).await,
            Reply::ok()
        );
        let reversed: Vec<Bytes> = keys.iter().rev().cloned().collect();
        assert_eq!(
            handle.execute(Operation::MultiGet(reversed)).await,
            Reply::Array(vec![
                Reply::bulk("3"),
                Reply::bulk("2"),
                Reply::bulk("1"),
                Reply::bulk("0"),
            ])
        );
        engine.shutdown().await.unwrap();
    }
}
