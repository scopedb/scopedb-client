// Copyright 2024 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error as StdError;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

use crate::AppendErrorDetails;
use crate::AppendRowsResult;
use crate::AppendState;
use crate::Client;
use crate::Error;
use crate::ErrorKind;
use crate::client::MAX_APPEND_BODY_BYTES;
use crate::client::MAX_APPEND_ROWS;

const DEFAULT_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_BATCH_ROWS: usize = MAX_APPEND_ROWS;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 4;
const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_RETRIES: usize = 8;
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_CIRCUIT_FAILURE_THRESHOLD: usize = 5;
const DEFAULT_CIRCUIT_COOLDOWN: Duration = Duration::from_secs(30);

/// Determines what an append stream does after one HTTP batch fails.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendFailurePolicy {
    /// Stop accepting rows and fail the stream's delivery barrier.
    #[default]
    Stop,
    /// Record the loss in the delivery report and continue with later batches.
    Continue,
}

/// The lifecycle state of an append stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendStreamState {
    Open,
    Closing,
    Closed,
    Failed,
}

/// The state of the continue-mode availability circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendCircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// The aggregate result of a delivery barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendDeliveryOutcome {
    /// Every covered row was committed and no local rows were dropped.
    Ok,
    /// Some covered rows committed and some were lost or have an unknown outcome.
    Partial,
    /// No covered row committed and all losses are known not to have committed.
    Failed,
    /// No covered row is known to have committed and at least one may have committed.
    Unknown,
}

/// Rows accepted by a bulk local-admission operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendAdmissionResult {
    pub accepted_rows: u64,
}

/// Aggregate settlement information since the previous delivery barrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendDeliveryReport {
    pub outcome: AppendDeliveryOutcome,
    pub accepted_rows: u64,
    /// Logical input rows acknowledged as committed at least once. Retrying an
    /// unknown outcome may insert additional copies that this count cannot measure.
    pub committed_rows: u64,
    pub failed_rows: u64,
    pub unknown_rows: u64,
    pub dropped_rows: u64,
    pub committed_batches: u64,
    pub failed_batches: u64,
    pub unknown_batches: u64,
    pub retries: u64,
    /// Wall time spent waiting for this barrier.
    pub duration: Duration,
}

/// Local drop counters grouped by admission failure reason.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AppendDroppedRows {
    pub buffer_full: u64,
    pub invalid_record: u64,
    pub record_too_large: u64,
    pub circuit_open: u64,
    pub closed: u64,
}

/// The most recent remote append failure observed by the stream.
#[derive(Debug, Clone)]
pub struct AppendLastFailure {
    pub at: SystemTime,
    pub error: Error,
}

/// A point-in-time snapshot of append stream counters and state.
#[derive(Debug, Clone)]
pub struct AppendStreamStats {
    pub state: AppendStreamState,
    pub circuit_state: AppendCircuitState,
    pub accepted_rows: u64,
    pub committed_rows: u64,
    pub failed_rows: u64,
    pub unknown_rows: u64,
    pub dropped_rows: u64,
    pub dropped_by_reason: AppendDroppedRows,
    pub retries: u64,
    pub pending_rows: u64,
    pub pending_bytes: usize,
    pub in_flight_batches: usize,
    pub last_failure: Option<AppendLastFailure>,
    pub last_report: Option<AppendDeliveryReport>,
}

/// Action taken after a batch failure listener is invoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendBatchFailureAction {
    Continuing,
    CircuitOpened,
    Stopped,
}

/// Diagnostic event emitted after a rejected or ambiguous append batch.
pub struct AppendBatchFailureEvent {
    pub error: Error,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub outcome: AppendState,
    pub action: AppendBatchFailureAction,
}

impl fmt::Debug for AppendBatchFailureEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendBatchFailureEvent")
            .field("error", &self.error)
            .field("batch_rows", &self.batch_rows)
            .field("batch_bytes", &self.batch_bytes)
            .field("outcome", &self.outcome)
            .field("action", &self.action)
            .finish()
    }
}

/// Continue-mode circuit breaker settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct AppendCircuitBreakerOptions {
    pub failure_threshold: usize,
    pub cooldown: Duration,
}

impl AppendCircuitBreakerOptions {
    pub fn new(failure_threshold: usize, cooldown: Duration) -> Self {
        Self {
            failure_threshold,
            cooldown,
        }
    }
}

impl Default for AppendCircuitBreakerOptions {
    fn default() -> Self {
        Self::new(DEFAULT_CIRCUIT_FAILURE_THRESHOLD, DEFAULT_CIRCUIT_COOLDOWN)
    }
}

/// A synchronous local-admission failure from [`AppendStream::try_send`].
#[derive(Debug)]
#[non_exhaustive]
pub enum AppendTrySendError {
    Serialization(serde_json::Error),
    InvalidRecord,
    RecordTooLarge { bytes: usize, limit: usize },
    BufferFull,
    CircuitOpen,
    Closed,
}

impl fmt::Display for AppendTrySendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(err) => write!(f, "failed to serialize append row: {err}"),
            Self::InvalidRecord => write!(f, "append row must serialize to a JSON object"),
            Self::RecordTooLarge { bytes, limit } => {
                write!(
                    f,
                    "append row is {bytes} bytes, exceeding the {limit}-byte limit"
                )
            }
            Self::BufferFull => write!(f, "append stream local buffer is full"),
            Self::CircuitOpen => write!(f, "append stream circuit breaker is open"),
            Self::Closed => write!(f, "append stream is closed"),
        }
    }
}

impl StdError for AppendTrySendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Serialization(err) => Some(err),
            _ => None,
        }
    }
}

type BatchFailureListener = Arc<dyn Fn(&AppendBatchFailureEvent) + Send + Sync>;

#[derive(Debug, Clone, Copy)]
struct RetryConfig {
    max_retries: usize,
    retry_unknown: bool,
    initial_backoff: Duration,
    max_backoff: Duration,
}

/// Configures a bounded, concurrent table append stream.
///
/// Retries temporary rejections by default. Replaying unknown outcomes requires
/// [`Self::retry_unknown`] because an unacknowledged attempt may have committed.
pub struct AppendStreamBuilder {
    client: Client,
    database: String,
    schema: String,
    table: String,
    batch_bytes: usize,
    max_batch_rows: usize,
    flush_interval: Duration,
    channel_capacity: usize,
    max_pending_bytes: usize,
    max_in_flight_requests: usize,
    retry: RetryConfig,
    failure_policy: AppendFailurePolicy,
    attempt_timeout: Option<Duration>,
    circuit_breaker: Option<AppendCircuitBreakerOptions>,
    batch_failure_listeners: Vec<BatchFailureListener>,
}

impl AppendStreamBuilder {
    pub(crate) fn new(client: Client, database: String, schema: String, table: String) -> Self {
        Self {
            client,
            database,
            schema,
            table,
            batch_bytes: DEFAULT_BATCH_BYTES,
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
            retry: RetryConfig {
                max_retries: DEFAULT_MAX_RETRIES,
                retry_unknown: false,
                initial_backoff: DEFAULT_INITIAL_BACKOFF,
                max_backoff: DEFAULT_MAX_BACKOFF,
            },
            failure_policy: AppendFailurePolicy::Stop,
            attempt_timeout: None,
            circuit_breaker: Some(AppendCircuitBreakerOptions::default()),
            batch_failure_listeners: Vec::new(),
        }
    }

    pub fn failure_policy(mut self, failure_policy: AppendFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    /// Sets the target NDJSON payload size (default 4 MiB, maximum 8 MiB).
    /// One row may exceed this target up to 8 MiB.
    pub fn target_batch_bytes(mut self, target_batch_bytes: usize) -> Self {
        self.batch_bytes = target_batch_bytes;
        self
    }

    /// Sets the maximum rows in one HTTP batch, up to the 200,000-row protocol limit.
    pub fn max_batch_rows(mut self, max_batch_rows: usize) -> Self {
        self.max_batch_rows = max_batch_rows;
        self
    }

    /// Sets the maximum delay between buffering the first row and dispatching its batch.
    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    pub fn channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity;
        self
    }

    /// Sets the maximum bytes admitted locally but not yet settled remotely.
    pub fn max_buffered_bytes(mut self, max_buffered_bytes: usize) -> Self {
        self.max_pending_bytes = max_buffered_bytes;
        self
    }

    /// Sets the maximum number of append batches sent concurrently.
    pub fn max_concurrent_batches(mut self, max_concurrent_batches: usize) -> Self {
        self.max_in_flight_requests = max_concurrent_batches;
        self
    }

    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.retry.max_retries = max_retries;
        self
    }

    /// Opts into replaying transient unknown outcomes. This can duplicate rows.
    /// Client timeouts are never replayed automatically, even when enabled.
    pub fn retry_unknown(mut self, enabled: bool) -> Self {
        self.retry.retry_unknown = enabled;
        self
    }

    pub fn initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.retry.initial_backoff = initial_backoff;
        self
    }

    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.retry.max_backoff = max_backoff;
        self
    }

    /// Sets a timeout for each HTTP attempt, overriding the client's write timeout.
    /// A timeout has an unknown commit outcome. By default the client uses 30 seconds.
    pub fn attempt_timeout(mut self, attempt_timeout: Duration) -> Self {
        self.attempt_timeout = Some(attempt_timeout);
        self
    }

    /// Configures the continue-mode availability circuit, or disables it with `None`.
    pub fn circuit_breaker(mut self, circuit_breaker: Option<AppendCircuitBreakerOptions>) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }

    /// Observes remote batch failures. Listener panics are isolated from the worker.
    pub fn on_batch_failure<F>(mut self, listener: F) -> Self
    where
        F: Fn(&AppendBatchFailureEvent) + Send + Sync + 'static,
    {
        self.batch_failure_listeners.push(Arc::new(listener));
        self
    }

    pub fn build(self) -> Result<AppendStream, Error> {
        validate_builder(&self)?;
        tokio::runtime::Handle::try_current().map_err(|err| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "append stream must be built inside a Tokio runtime",
            )
            .set_source(err)
        })?;

        Ok(AppendStream::new(AppendStreamConfig {
            client: self.client,
            database: self.database,
            schema: self.schema,
            table: self.table,
            batch_bytes: self.batch_bytes,
            max_batch_rows: self.max_batch_rows,
            flush_interval: self.flush_interval,
            channel_capacity: self.channel_capacity,
            max_pending_bytes: self.max_pending_bytes,
            max_in_flight_requests: self.max_in_flight_requests,
            retry: self.retry,
            failure_policy: self.failure_policy,
            attempt_timeout: self.attempt_timeout,
            circuit_breaker: self.circuit_breaker,
            batch_failure_listeners: self.batch_failure_listeners,
        }))
    }
}

fn validate_builder(builder: &AppendStreamBuilder) -> Result<(), Error> {
    builder.client.make_resource_url(&[
        "catalog",
        "databases",
        &builder.database,
        "schemas",
        &builder.schema,
        "tables",
        &builder.table,
        "ingest",
    ])?;
    validate_deadline("initial_backoff", builder.retry.initial_backoff)?;
    validate_deadline("max_backoff", builder.retry.max_backoff)?;
    if builder.channel_capacity > Semaphore::MAX_PERMITS {
        return Err(config_error("channel_capacity is too large"));
    }
    validate_positive("target_batch_bytes", builder.batch_bytes)?;
    if builder.batch_bytes > MAX_APPEND_BODY_BYTES {
        return Err(config_error(format!(
            "target_batch_bytes must not exceed {MAX_APPEND_BODY_BYTES}"
        )));
    }
    validate_positive("max_batch_rows", builder.max_batch_rows)?;
    if builder.max_batch_rows > MAX_APPEND_ROWS {
        return Err(config_error(format!(
            "max_batch_rows must not exceed {MAX_APPEND_ROWS}"
        )));
    }
    if builder.flush_interval.is_zero() {
        return Err(config_error("flush_interval must be greater than zero"));
    }
    validate_deadline("flush_interval", builder.flush_interval)?;
    validate_positive("channel_capacity", builder.channel_capacity)?;
    validate_positive("max_buffered_bytes", builder.max_pending_bytes)?;
    if builder.max_pending_bytes > (u32::MAX as usize).min(Semaphore::MAX_PERMITS) {
        return Err(config_error(format!(
            "max_buffered_bytes must not exceed {}",
            u32::MAX
        )));
    }
    validate_positive("max_concurrent_batches", builder.max_in_flight_requests)?;
    if builder
        .attempt_timeout
        .is_some_and(|attempt_timeout| attempt_timeout.is_zero())
    {
        return Err(config_error("attempt_timeout must be greater than zero"));
    }
    if let Some(attempt_timeout) = builder.attempt_timeout {
        validate_deadline("attempt_timeout", attempt_timeout)?;
    }
    if let Some(circuit) = builder.circuit_breaker {
        validate_positive(
            "circuit_breaker.failure_threshold",
            circuit.failure_threshold,
        )?;
        if circuit.cooldown.is_zero() {
            return Err(config_error(
                "circuit_breaker.cooldown must be greater than zero",
            ));
        }
        validate_deadline("circuit_breaker.cooldown", circuit.cooldown)?;
    }
    Ok(())
}

fn validate_deadline(name: &str, duration: Duration) -> Result<(), Error> {
    if Instant::now().checked_add(duration).is_none() {
        Err(config_error(format!("{name} is too large")))
    } else {
        Ok(())
    }
}

fn validate_positive(name: &str, value: usize) -> Result<(), Error> {
    if value == 0 {
        Err(config_error(format!("{name} must be greater than zero")))
    } else {
        Ok(())
    }
}

fn config_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::ConfigInvalid, message)
}

struct AppendStreamConfig {
    client: Client,
    database: String,
    schema: String,
    table: String,
    batch_bytes: usize,
    max_batch_rows: usize,
    flush_interval: Duration,
    channel_capacity: usize,
    max_pending_bytes: usize,
    max_in_flight_requests: usize,
    retry: RetryConfig,
    failure_policy: AppendFailurePolicy,
    attempt_timeout: Option<Duration>,
    circuit_breaker: Option<AppendCircuitBreakerOptions>,
    batch_failure_listeners: Vec<BatchFailureListener>,
}

enum AppendCommand {
    Record(BufferedRecord),
    Flush(BarrierCommand),
    Shutdown(BarrierCommand),
}

struct BarrierCommand {
    dropped_rows_at_barrier: u64,
    started_at: Instant,
    ack: oneshot::Sender<Result<AppendDeliveryReport, Error>>,
}

struct BufferedRecord {
    payload: String,
    reserved_bytes: usize,
    _reservation: OwnedSemaphorePermit,
}

struct PendingBytesBudget {
    capacity: usize,
    semaphore: Arc<Semaphore>,
    closed: AtomicBool,
}

impl PendingBytesBudget {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
            closed: AtomicBool::new(false),
        }
    }

    async fn acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit, PendingAcquireError> {
        if bytes > self.capacity {
            return Err(PendingAcquireError::ExceedsCapacity);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PendingAcquireError::Closed);
        }
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| PendingAcquireError::Closed)?;
        if self.closed.load(Ordering::Acquire) {
            drop(permit);
            return Err(PendingAcquireError::Closed);
        }
        Ok(permit)
    }

    fn try_acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit, PendingAcquireError> {
        if bytes > self.capacity {
            return Err(PendingAcquireError::ExceedsCapacity);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PendingAcquireError::Closed);
        }
        self.semaphore
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|err| match err {
                tokio::sync::TryAcquireError::Closed => PendingAcquireError::Closed,
                tokio::sync::TryAcquireError::NoPermits => PendingAcquireError::Full,
            })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.semaphore.close();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingAcquireError {
    Closed,
    Full,
    ExceedsCapacity,
}

#[derive(Debug, Clone, Copy, Default)]
struct DeliveryCounters {
    accepted_rows: u64,
    committed_rows: u64,
    failed_rows: u64,
    unknown_rows: u64,
    committed_batches: u64,
    failed_batches: u64,
    unknown_batches: u64,
    retries: u64,
}

struct CircuitRuntime {
    state: AppendCircuitState,
    opened_until: Option<Instant>,
    consecutive_failures: usize,
    probe_admission_claimed: bool,
}

impl Default for CircuitRuntime {
    fn default() -> Self {
        Self {
            state: AppendCircuitState::Closed,
            opened_until: None,
            consecutive_failures: 0,
            probe_admission_claimed: false,
        }
    }
}

struct SharedState {
    lifecycle: AppendStreamState,
    fatal: Option<Error>,
    final_result: Option<Result<AppendDeliveryReport, Error>>,
    accepted_rows: u64,
    committed_rows: u64,
    failed_rows: u64,
    unknown_rows: u64,
    dropped_rows: u64,
    dropped_by_reason: AppendDroppedRows,
    retries: u64,
    pending_rows: u64,
    pending_bytes: usize,
    in_flight_batches: usize,
    last_failure: Option<AppendLastFailure>,
    last_report: Option<AppendDeliveryReport>,
    circuit: CircuitRuntime,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            lifecycle: AppendStreamState::Open,
            fatal: None,
            final_result: None,
            accepted_rows: 0,
            committed_rows: 0,
            failed_rows: 0,
            unknown_rows: 0,
            dropped_rows: 0,
            dropped_by_reason: AppendDroppedRows::default(),
            retries: 0,
            pending_rows: 0,
            pending_bytes: 0,
            in_flight_batches: 0,
            last_failure: None,
            last_report: None,
            circuit: CircuitRuntime::default(),
        }
    }
}

struct AppendStreamInner {
    tx: mpsc::Sender<AppendCommand>,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
}

/// A cloneable, bounded producer handle for asynchronous table appends.
/// Dropping every handle drains admitted rows in the background while the Tokio
/// runtime remains alive. Use shutdown() to observe settlement before exiting.
#[derive(Clone)]
pub struct AppendStream {
    inner: Arc<AppendStreamInner>,
}

impl AppendStream {
    fn new(config: AppendStreamConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let shared = Arc::new(Mutex::new(SharedState::default()));
        let pending_bytes = Arc::new(PendingBytesBudget::new(config.max_pending_bytes));
        let shutdown_notify = Arc::new(Notify::new());

        let worker = tokio::spawn(run_append_worker(
            config,
            rx,
            shared.clone(),
            pending_bytes.clone(),
            shutdown_notify.clone(),
        ));
        tokio::spawn(monitor_append_worker(
            worker,
            shared.clone(),
            pending_bytes.clone(),
            shutdown_notify.clone(),
        ));

        Self {
            inner: Arc::new(AppendStreamInner {
                tx,
                shared,
                pending_bytes,
                shutdown_notify,
            }),
        }
    }

    /// Serializes and admits one row, waiting for bounded local capacity.
    ///
    /// Success means local admission only. Use [`Self::flush`] or
    /// [`Self::shutdown`] to wait for remote settlement.
    pub async fn send<T>(&self, row: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.check_open()?;
        let payload = prepare_record(row)?;
        let reserved_bytes = payload.len().saturating_add(1);
        let reservation = self
            .inner
            .pending_bytes
            .acquire(reserved_bytes)
            .await
            .map_err(|err| self.map_pending_error(err, reserved_bytes))?;
        let permit = match self.inner.tx.reserve().await {
            Ok(permit) => permit,
            Err(_) => {
                drop(reservation);
                return Err(self.closed_or_fatal_error());
            }
        };

        let mut shared = lock(&self.inner.shared);
        if shared.lifecycle != AppendStreamState::Open {
            drop(shared);
            drop(permit);
            drop(reservation);
            return Err(self.closed_or_fatal_error());
        }
        shared.accepted_rows = shared.accepted_rows.saturating_add(1);
        shared.pending_rows = shared.pending_rows.saturating_add(1);
        shared.pending_bytes = shared.pending_bytes.saturating_add(reserved_bytes);
        permit.send(AppendCommand::Record(BufferedRecord {
            payload,
            reserved_bytes,
            _reservation: reservation,
        }));
        Ok(())
    }

    /// Attempts synchronous local admission without waiting for capacity.
    pub fn try_send<T>(&self, row: &T) -> Result<(), AppendTrySendError>
    where
        T: Serialize + ?Sized,
    {
        {
            let shared = lock(&self.inner.shared);
            if shared.lifecycle != AppendStreamState::Open {
                drop(shared);
                note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
                return Err(AppendTrySendError::Closed);
            }
            if !circuit_admission_available(&shared) {
                drop(shared);
                note_drop(&mut lock(&self.inner.shared), DropReason::CircuitOpen);
                return Err(AppendTrySendError::CircuitOpen);
            }
        }

        let payload = match prepare_record_for_try_send(row) {
            Ok(payload) => payload,
            Err(err) => {
                let reason = match err {
                    AppendTrySendError::RecordTooLarge { .. } => DropReason::RecordTooLarge,
                    _ => DropReason::InvalidRecord,
                };
                note_drop(&mut lock(&self.inner.shared), reason);
                return Err(err);
            }
        };
        let reserved_bytes = payload.len().saturating_add(1);
        let reservation = match self.inner.pending_bytes.try_acquire(reserved_bytes) {
            Ok(reservation) => reservation,
            Err(PendingAcquireError::Closed) => {
                note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
                return Err(AppendTrySendError::Closed);
            }
            Err(PendingAcquireError::Full | PendingAcquireError::ExceedsCapacity) => {
                note_drop(&mut lock(&self.inner.shared), DropReason::BufferFull);
                return Err(AppendTrySendError::BufferFull);
            }
        };
        let permit = match self.inner.tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                drop(reservation);
                note_drop(&mut lock(&self.inner.shared), DropReason::BufferFull);
                return Err(AppendTrySendError::BufferFull);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                drop(reservation);
                note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
                return Err(AppendTrySendError::Closed);
            }
        };

        let mut shared = lock(&self.inner.shared);
        if shared.lifecycle != AppendStreamState::Open {
            drop(shared);
            drop(permit);
            drop(reservation);
            note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
            return Err(AppendTrySendError::Closed);
        }
        if !try_claim_circuit_admission(&mut shared) {
            drop(shared);
            drop(permit);
            drop(reservation);
            note_drop(&mut lock(&self.inner.shared), DropReason::CircuitOpen);
            return Err(AppendTrySendError::CircuitOpen);
        }
        shared.accepted_rows = shared.accepted_rows.saturating_add(1);
        shared.pending_rows = shared.pending_rows.saturating_add(1);
        shared.pending_bytes = shared.pending_bytes.saturating_add(reserved_bytes);
        permit.send(AppendCommand::Record(BufferedRecord {
            payload,
            reserved_bytes,
            _reservation: reservation,
        }));
        Ok(())
    }

    /// Admits an iterator of rows sequentially with bounded memory.
    pub async fn send_all<I>(&self, rows: I) -> Result<AppendAdmissionResult, Error>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        let mut accepted_rows = 0u64;
        for row in rows {
            self.send(&row)
                .await
                .map_err(|error| error.with_context("accepted_rows", accepted_rows))?;
            accepted_rows = accepted_rows.saturating_add(1);
        }
        Ok(AppendAdmissionResult { accepted_rows })
    }

    /// Dispatches rows admitted before this barrier and waits for their outcomes.
    ///
    /// Committed rows have at least one successful acknowledgement; earlier
    /// unknown attempts may also have committed. A failed barrier may include
    /// successful batches, so replaying its entire input can duplicate rows.
    ///
    /// Once enqueued, dropping this future does not cancel remote settlement.
    /// Keep the future alive to receive its interval report; the latest completed
    /// report also remains observable through [`Self::stats`].
    pub async fn flush(&self) -> Result<AppendDeliveryReport, Error> {
        loop {
            let notified = self.inner.shutdown_notify.notified();
            match self.current_terminal_result() {
                TerminalResult::Open => {}
                TerminalResult::Waiting => {
                    notified.await;
                    continue;
                }
                TerminalResult::Complete(result) => return result,
            }

            let permit = match self.inner.tx.reserve().await {
                Ok(permit) => permit,
                Err(_) => return self.wait_for_terminal().await,
            };
            let (ack, response) = oneshot::channel();
            {
                let shared = lock(&self.inner.shared);
                if shared.lifecycle != AppendStreamState::Open {
                    drop(shared);
                    drop(permit);
                    continue;
                }
                permit.send(AppendCommand::Flush(BarrierCommand {
                    dropped_rows_at_barrier: shared.dropped_rows,
                    started_at: Instant::now(),
                    ack,
                }));
            }
            return match response.await {
                Ok(Ok(report)) => Ok(report),
                Ok(Err(_)) | Err(_) => self.wait_for_terminal().await,
            };
        }
    }

    /// Permanently closes the stream after settling every admitted row.
    ///
    /// This method is idempotent across cloned producer handles. Once polled,
    /// dropping its future stops waiting but settlement continues in the worker.
    /// Call shutdown again to observe completion. Failure reports remain in stats().
    pub async fn shutdown(&self) -> Result<AppendDeliveryReport, Error> {
        let should_start = {
            let mut shared = lock(&self.inner.shared);
            match shared.lifecycle {
                AppendStreamState::Open => {
                    shared.lifecycle = AppendStreamState::Closing;
                    true
                }
                AppendStreamState::Closing | AppendStreamState::Failed => false,
                AppendStreamState::Closed if shared.final_result.is_some() => {
                    return terminal_result(&shared);
                }
                AppendStreamState::Closed => false,
            }
        };

        if should_start {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _ = enqueue_shutdown(inner).await;
            });
        }

        self.wait_for_terminal().await
    }

    /// Returns a synchronous snapshot suitable for metrics and shutdown diagnostics.
    pub fn stats(&self) -> AppendStreamStats {
        let shared = lock(&self.inner.shared);
        AppendStreamStats {
            state: shared.lifecycle,
            circuit_state: shared.circuit.state,
            accepted_rows: shared.accepted_rows,
            committed_rows: shared.committed_rows,
            failed_rows: shared.failed_rows,
            unknown_rows: shared.unknown_rows,
            dropped_rows: shared.dropped_rows,
            dropped_by_reason: shared.dropped_by_reason,
            retries: shared.retries,
            pending_rows: shared.pending_rows,
            pending_bytes: shared.pending_bytes,
            in_flight_batches: shared.in_flight_batches,
            last_failure: shared.last_failure.clone(),
            last_report: shared.last_report.clone(),
        }
    }

    fn check_open(&self) -> Result<(), Error> {
        let shared = lock(&self.inner.shared);
        if shared.lifecycle == AppendStreamState::Open {
            Ok(())
        } else {
            Err(error_from_shared(&shared))
        }
    }

    fn closed_or_fatal_error(&self) -> Error {
        error_from_shared(&lock(&self.inner.shared))
    }

    fn map_pending_error(&self, error: PendingAcquireError, requested: usize) -> Error {
        match error {
            PendingAcquireError::Closed => self.closed_or_fatal_error(),
            PendingAcquireError::Full => Error::new(
                ErrorKind::Unexpected,
                "append stream local byte budget is full",
            )
            .set_temporary(),
            PendingAcquireError::ExceedsCapacity => Error::new(
                ErrorKind::Unexpected,
                format!(
                    "append row requires {requested} buffered bytes, exceeding max_pending_bytes={}",
                    self.inner.pending_bytes.capacity
                ),
            )
            .set_permanent(),
        }
    }

    fn current_terminal_result(&self) -> TerminalResult {
        let shared = lock(&self.inner.shared);
        match shared.lifecycle {
            AppendStreamState::Open => TerminalResult::Open,
            AppendStreamState::Closing => TerminalResult::Waiting,
            AppendStreamState::Closed | AppendStreamState::Failed
                if shared.final_result.is_none() =>
            {
                TerminalResult::Waiting
            }
            AppendStreamState::Closed | AppendStreamState::Failed => {
                TerminalResult::Complete(terminal_result(&shared))
            }
        }
    }

    async fn wait_for_terminal(&self) -> Result<AppendDeliveryReport, Error> {
        loop {
            let notified = self.inner.shutdown_notify.notified();
            {
                let shared = lock(&self.inner.shared);
                if matches!(
                    shared.lifecycle,
                    AppendStreamState::Closed | AppendStreamState::Failed
                ) && shared.final_result.is_some()
                {
                    return terminal_result(&shared);
                }
            }
            notified.await;
        }
    }
}

enum TerminalResult {
    Open,
    Waiting,
    Complete(Result<AppendDeliveryReport, Error>),
}

async fn enqueue_shutdown(inner: Arc<AppendStreamInner>) -> Result<AppendDeliveryReport, Error> {
    let (ack, response) = oneshot::channel();
    let dropped_rows_at_barrier = lock(&inner.shared).dropped_rows;
    inner
        .tx
        .send(AppendCommand::Shutdown(BarrierCommand {
            dropped_rows_at_barrier,
            started_at: Instant::now(),
            ack,
        }))
        .await
        .map_err(|_| error_from_shared(&lock(&inner.shared)))?;
    response
        .await
        .unwrap_or_else(|_| Err(error_from_shared(&lock(&inner.shared))))
}

fn terminal_result(shared: &SharedState) -> Result<AppendDeliveryReport, Error> {
    match &shared.final_result {
        Some(Ok(report)) => Ok(report.clone()),
        Some(Err(error)) => Err(error.clone()),
        None => Err(error_from_shared(shared)),
    }
}

fn error_from_shared(shared: &SharedState) -> Error {
    shared.fatal.as_ref().cloned().unwrap_or_else(|| {
        Error::new(ErrorKind::Unexpected, "append stream is closed").set_persistent()
    })
}

fn prepare_record<T>(row: &T) -> Result<String, Error>
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_string(row).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "failed to serialize append row").set_source(err)
    })?;
    validate_record_payload(&payload).map_err(|err| match err {
        AppendTrySendError::RecordTooLarge { bytes, limit } => Error::new(
            ErrorKind::Unexpected,
            format!("append row is {bytes} bytes, exceeding the {limit}-byte limit"),
        ),
        _ => Error::new(
            ErrorKind::Unexpected,
            "append row must serialize to a JSON object",
        ),
    })?;
    Ok(payload)
}

fn prepare_record_for_try_send<T>(row: &T) -> Result<String, AppendTrySendError>
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_string(row).map_err(AppendTrySendError::Serialization)?;
    validate_record_payload(&payload)?;
    Ok(payload)
}

fn validate_record_payload(payload: &str) -> Result<(), AppendTrySendError> {
    let bytes = payload.len();
    if bytes > MAX_APPEND_BODY_BYTES {
        return Err(AppendTrySendError::RecordTooLarge {
            bytes,
            limit: MAX_APPEND_BODY_BYTES,
        });
    }
    if !payload.starts_with('{') {
        return Err(AppendTrySendError::InvalidRecord);
    }
    Ok(())
}

struct BatchOutcome {
    records: Vec<BufferedRecord>,
    batch_bytes: usize,
    result: Result<AppendRowsResult, Error>,
    retries: usize,
    circuit_probe: bool,
}

struct AppendBatchRequest {
    client: Client,
    database: String,
    schema: String,
    table: String,
    retry: RetryConfig,
    attempt_timeout: Option<Duration>,
}

struct AppendWorker {
    config: AppendStreamConfig,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
    rows: Vec<BufferedRecord>,
    current_bytes: usize,
    batch_deadline: Option<Instant>,
    in_flight: JoinSet<BatchOutcome>,
    interval: DeliveryCounters,
    reported_dropped_rows: u64,
    fatal: Option<Error>,
}

async fn run_append_worker(
    config: AppendStreamConfig,
    mut rx: mpsc::Receiver<AppendCommand>,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
) {
    let mut worker = AppendWorker {
        config,
        shared,
        pending_bytes,
        shutdown_notify,
        rows: Vec::new(),
        current_bytes: 0,
        batch_deadline: None,
        in_flight: JoinSet::new(),
        interval: DeliveryCounters::default(),
        reported_dropped_rows: 0,
        fatal: None,
    };

    loop {
        if worker.fatal.is_some() {
            worker.finish_fatal(&mut rx).await;
            return;
        }

        let deadline = worker
            .batch_deadline
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86400 * 365));
        tokio::select! {
            biased;
            outcome = worker.in_flight.join_next(), if !worker.in_flight.is_empty() => {
                worker.handle_joined(outcome).await;
            }
            _ = tokio::time::sleep_until(deadline.into()), if worker.batch_deadline.is_some() => {
                worker.dispatch_buffered().await;
            }
            command = rx.recv() => {
                match command {
                    Some(AppendCommand::Record(record)) => worker.buffer_record(record).await,
                    Some(AppendCommand::Flush(barrier)) => {
                        worker.complete_barrier(barrier).await;
                    }
                    Some(AppendCommand::Shutdown(barrier)) => {
                        worker.complete_barrier(barrier).await;
                        if worker.fatal.is_some() {
                            worker.finish_fatal(&mut rx).await;
                        } else {
                            worker.finish_closed();
                        }
                        return;
                    }
                    None => {
                        worker.dispatch_buffered().await;
                        worker.drain_in_flight().await;
                        if worker.fatal.is_some() {
                            worker.finish_fatal(&mut rx).await;
                        } else {
                            let dropped = lock(&worker.shared).dropped_rows;
                            let report = worker.take_report(dropped, Instant::now());
                            worker.finish_closed_with_report(report);
                        }
                        return;
                    }
                }
            }
        }
    }
}

async fn monitor_append_worker(
    worker: JoinHandle<()>,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
) {
    let worker_result = worker.await;
    let mut shared = lock(&shared);
    if shared.final_result.is_some() {
        return;
    }

    let error = match worker_result {
        Ok(()) => Error::new(
            ErrorKind::Unexpected,
            "append stream worker stopped before publishing a final result",
        )
        .set_persistent(),
        Err(error) => {
            unknown_append_error("append stream worker terminated unexpectedly", Some(error))
        }
    };
    let snapshot = error.clone();
    let resolved = shared
        .committed_rows
        .saturating_add(shared.failed_rows)
        .saturating_add(shared.unknown_rows);
    let unresolved = shared.accepted_rows.saturating_sub(resolved);
    shared.unknown_rows = shared.unknown_rows.saturating_add(unresolved);
    shared.pending_rows = 0;
    shared.pending_bytes = 0;
    shared.in_flight_batches = 0;
    shared.lifecycle = AppendStreamState::Failed;
    shared.fatal = Some(snapshot.clone());
    shared.final_result = Some(Err(snapshot));
    drop(shared);
    pending_bytes.close();
    shutdown_notify.notify_waiters();
}

impl AppendWorker {
    async fn buffer_record(&mut self, record: BufferedRecord) {
        self.interval.accepted_rows = self.interval.accepted_rows.saturating_add(1);
        let separator = usize::from(!self.rows.is_empty());
        if !self.rows.is_empty()
            && self
                .current_bytes
                .saturating_add(separator)
                .saturating_add(record.payload.len())
                > self.config.batch_bytes
        {
            self.dispatch_buffered().await;
        }
        if self.fatal.is_some() {
            self.mark_records_failed(vec![record]);
            return;
        }

        if self.rows.is_empty() {
            self.batch_deadline = Some(deadline_after(self.config.flush_interval));
        } else {
            self.current_bytes = self.current_bytes.saturating_add(1);
        }
        self.current_bytes = self.current_bytes.saturating_add(record.payload.len());
        self.rows.push(record);
        if self.current_bytes >= self.config.batch_bytes
            || self.rows.len() >= self.config.max_batch_rows
        {
            self.dispatch_buffered().await;
        }
    }

    async fn dispatch_buffered(&mut self) {
        if self.rows.is_empty() || self.fatal.is_some() {
            return;
        }
        while self.in_flight.len() >= self.config.max_in_flight_requests {
            let outcome = self.in_flight.join_next().await;
            self.handle_joined(outcome).await;
            if self.fatal.is_some() {
                return;
            }
        }

        let circuit_probe = self.wait_for_circuit().await;
        if self.fatal.is_some() {
            return;
        }
        let records = std::mem::take(&mut self.rows);
        let batch_bytes = std::mem::take(&mut self.current_bytes);
        self.batch_deadline = None;
        let payload = records
            .iter()
            .map(|record| record.payload.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let request = AppendBatchRequest {
            client: self.config.client.clone(),
            database: self.config.database.clone(),
            schema: self.config.schema.clone(),
            table: self.config.table.clone(),
            retry: self.config.retry,
            attempt_timeout: self.config.attempt_timeout,
        };
        lock(&self.shared).in_flight_batches += 1;
        self.in_flight.spawn(async move {
            let (result, retries) = append_batch(request, payload).await;
            BatchOutcome {
                records,
                batch_bytes,
                result,
                retries,
                circuit_probe,
            }
        });
    }

    async fn wait_for_circuit(&mut self) -> bool {
        if self.config.failure_policy != AppendFailurePolicy::Continue
            || self.config.circuit_breaker.is_none()
        {
            return false;
        }

        loop {
            let (state, remaining) = {
                let shared = lock(&self.shared);
                let remaining = shared
                    .circuit
                    .opened_until
                    .and_then(|until| until.checked_duration_since(Instant::now()));
                (shared.circuit.state, remaining)
            };
            match state {
                AppendCircuitState::Closed => return false,
                AppendCircuitState::Open => {
                    if let Some(remaining) = remaining {
                        if self.in_flight.is_empty() {
                            tokio::time::sleep(remaining).await;
                        } else {
                            tokio::select! {
                                _ = tokio::time::sleep(remaining) => {}
                                outcome = self.in_flight.join_next() => {
                                    self.handle_joined(outcome).await;
                                    if self.fatal.is_some() {
                                        return false;
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    if !self.in_flight.is_empty() {
                        self.drain_in_flight().await;
                        if self.fatal.is_some() {
                            return false;
                        }
                        continue;
                    }
                    let mut shared = lock(&self.shared);
                    if shared.circuit.state == AppendCircuitState::Open {
                        shared.circuit.state = AppendCircuitState::HalfOpen;
                        return true;
                    }
                }
                AppendCircuitState::HalfOpen => {
                    if self.in_flight.is_empty() {
                        return true;
                    }
                    let outcome = self.in_flight.join_next().await;
                    self.handle_joined(outcome).await;
                    if self.fatal.is_some() {
                        return false;
                    }
                }
            }
        }
    }

    async fn complete_barrier(&mut self, barrier: BarrierCommand) {
        self.dispatch_buffered().await;
        self.drain_in_flight().await;
        if let Some(fatal) = &self.fatal {
            let _ = barrier.ack.send(Err(fatal.clone()));
            return;
        }
        let report = self.take_report(barrier.dropped_rows_at_barrier, barrier.started_at);
        let _ = barrier.ack.send(Ok(report));
    }

    async fn drain_in_flight(&mut self) {
        while let Some(outcome) = self.in_flight.join_next().await {
            self.handle_joined(Some(outcome)).await;
        }
    }

    async fn handle_joined(
        &mut self,
        outcome: Option<Result<BatchOutcome, tokio::task::JoinError>>,
    ) {
        let Some(outcome) = outcome else {
            return;
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let fatal = unknown_append_error(
                    "append stream background request task failed",
                    Some(error),
                );
                self.set_fatal(fatal.clone());
                return;
            }
        };

        let rows = outcome.records.len() as u64;
        let reserved_bytes = outcome
            .records
            .iter()
            .map(|record| record.reserved_bytes)
            .sum::<usize>();
        let mut failure_event = None;
        {
            let mut shared = lock(&self.shared);
            shared.in_flight_batches = shared.in_flight_batches.saturating_sub(1);
            shared.pending_rows = shared.pending_rows.saturating_sub(rows);
            shared.pending_bytes = shared.pending_bytes.saturating_sub(reserved_bytes);
            shared.retries = shared.retries.saturating_add(outcome.retries as u64);
            self.interval.retries = self.interval.retries.saturating_add(outcome.retries as u64);

            match &outcome.result {
                Ok(result) => {
                    let committed = result.num_rows_inserted.max(0) as u64;
                    shared.committed_rows = shared.committed_rows.saturating_add(committed);
                    self.interval.committed_rows =
                        self.interval.committed_rows.saturating_add(committed);
                    self.interval.committed_batches =
                        self.interval.committed_batches.saturating_add(1);
                    record_circuit_success(&mut shared, outcome.circuit_probe);
                }
                Err(error) => {
                    let snapshot = error.clone();
                    let status = snapshot
                        .append_details()
                        .map(|d| d.status)
                        .unwrap_or(AppendState::Unknown);
                    if status == AppendState::Unknown {
                        shared.unknown_rows = shared.unknown_rows.saturating_add(rows);
                        self.interval.unknown_rows =
                            self.interval.unknown_rows.saturating_add(rows);
                        self.interval.unknown_batches =
                            self.interval.unknown_batches.saturating_add(1);
                    } else {
                        shared.failed_rows = shared.failed_rows.saturating_add(rows);
                        self.interval.failed_rows = self.interval.failed_rows.saturating_add(rows);
                        self.interval.failed_batches =
                            self.interval.failed_batches.saturating_add(1);
                    }
                    shared.last_failure = Some(AppendLastFailure {
                        at: SystemTime::now(),
                        error: error.clone(),
                    });
                    let circuit_opened = self.config.failure_policy
                        == AppendFailurePolicy::Continue
                        && record_circuit_failure(
                            &mut shared,
                            self.config.circuit_breaker,
                            error,
                            outcome.circuit_probe,
                        );
                    let action = if self.config.failure_policy == AppendFailurePolicy::Stop {
                        AppendBatchFailureAction::Stopped
                    } else if circuit_opened {
                        AppendBatchFailureAction::CircuitOpened
                    } else {
                        AppendBatchFailureAction::Continuing
                    };
                    failure_event = Some(AppendBatchFailureEvent {
                        error: snapshot.clone(),
                        batch_rows: rows as usize,
                        batch_bytes: outcome.batch_bytes,
                        outcome: status,
                        action,
                    });
                    if self.config.failure_policy == AppendFailurePolicy::Stop {
                        drop(shared);
                        self.set_fatal(snapshot);
                    }
                }
            }
        }
        drop(outcome.records);

        if let Some(event) = &failure_event {
            for listener in &self.config.batch_failure_listeners {
                let _ = std::panic::catch_unwind(AssertUnwindSafe(|| listener(event)));
            }
        }
    }

    fn set_fatal(&mut self, snapshot: Error) {
        let replace = match &self.fatal {
            None => true,
            Some(current) => {
                current.append_details().map(|d| d.status) != Some(AppendState::Unknown)
                    && snapshot.append_details().map(|d| d.status) == Some(AppendState::Unknown)
            }
        };
        if replace {
            self.fatal = Some(snapshot.clone());
        }
        let fatal = self.fatal.clone().expect("fatal state must be set");
        let mut shared = lock(&self.shared);
        shared.lifecycle = AppendStreamState::Failed;
        shared.fatal = Some(fatal);
        drop(shared);
        self.pending_bytes.close();
    }

    fn mark_records_failed(&mut self, records: Vec<BufferedRecord>) {
        if records.is_empty() {
            return;
        }
        let rows = records.len() as u64;
        let bytes = records
            .iter()
            .map(|record| record.reserved_bytes)
            .sum::<usize>();
        let mut shared = lock(&self.shared);
        shared.failed_rows = shared.failed_rows.saturating_add(rows);
        shared.pending_rows = shared.pending_rows.saturating_sub(rows);
        shared.pending_bytes = shared.pending_bytes.saturating_sub(bytes);
        self.interval.failed_rows = self.interval.failed_rows.saturating_add(rows);
        drop(shared);
        drop(records);
    }

    async fn finish_fatal(&mut self, rx: &mut mpsc::Receiver<AppendCommand>) {
        rx.close();
        let buffered = std::mem::take(&mut self.rows);
        self.mark_records_failed(buffered);
        self.current_bytes = 0;
        self.batch_deadline = None;

        let mut barriers = Vec::new();
        while let Some(command) = rx.recv().await {
            match command {
                AppendCommand::Record(record) => {
                    self.interval.accepted_rows = self.interval.accepted_rows.saturating_add(1);
                    self.mark_records_failed(vec![record]);
                }
                AppendCommand::Flush(barrier) | AppendCommand::Shutdown(barrier) => {
                    barriers.push(barrier.ack);
                }
            }
        }
        self.drain_in_flight().await;

        let fatal = self
            .fatal
            .clone()
            .unwrap_or_else(|| Error::new(ErrorKind::Unexpected, "append stream failed"));
        {
            let mut shared = lock(&self.shared);
            let resolved = shared
                .committed_rows
                .saturating_add(shared.failed_rows)
                .saturating_add(shared.unknown_rows);
            let unresolved = shared.accepted_rows.saturating_sub(resolved);
            if unresolved > 0 {
                shared.unknown_rows = shared.unknown_rows.saturating_add(unresolved);
                self.interval.unknown_rows = self.interval.unknown_rows.saturating_add(unresolved);
                self.interval.unknown_batches = self.interval.unknown_batches.saturating_add(1);
            }
            shared.pending_rows = 0;
            shared.pending_bytes = 0;
            shared.in_flight_batches = 0;
        }
        let dropped = lock(&self.shared).dropped_rows;
        self.take_report(dropped, Instant::now());
        {
            let mut shared = lock(&self.shared);
            shared.lifecycle = AppendStreamState::Failed;
            shared.fatal = Some(fatal.clone());
            shared.final_result = Some(Err(fatal.clone()));
        }
        for ack in barriers {
            let _ = ack.send(Err(fatal.clone()));
        }
        self.pending_bytes.close();
        self.shutdown_notify.notify_waiters();
    }

    fn take_report(
        &mut self,
        dropped_rows_at_barrier: u64,
        started_at: Instant,
    ) -> AppendDeliveryReport {
        let dropped_rows = dropped_rows_at_barrier.saturating_sub(self.reported_dropped_rows);
        self.reported_dropped_rows = self.reported_dropped_rows.max(dropped_rows_at_barrier);
        let lost_rows = self
            .interval
            .failed_rows
            .saturating_add(self.interval.unknown_rows)
            .saturating_add(dropped_rows);
        let outcome = if lost_rows == 0 {
            AppendDeliveryOutcome::Ok
        } else if self.interval.committed_rows > 0 {
            AppendDeliveryOutcome::Partial
        } else if self.interval.unknown_rows > 0 {
            AppendDeliveryOutcome::Unknown
        } else {
            AppendDeliveryOutcome::Failed
        };
        let report = AppendDeliveryReport {
            outcome,
            accepted_rows: self.interval.accepted_rows,
            committed_rows: self.interval.committed_rows,
            failed_rows: self.interval.failed_rows,
            unknown_rows: self.interval.unknown_rows,
            dropped_rows,
            committed_batches: self.interval.committed_batches,
            failed_batches: self.interval.failed_batches,
            unknown_batches: self.interval.unknown_batches,
            retries: self.interval.retries,
            duration: started_at.elapsed(),
        };
        self.interval = DeliveryCounters::default();
        let mut shared = lock(&self.shared);
        shared.last_report = Some(report.clone());
        report
    }

    fn finish_closed(&mut self) {
        let report = lock(&self.shared)
            .last_report
            .clone()
            .unwrap_or_else(empty_report);
        self.finish_closed_with_report(report);
    }

    fn finish_closed_with_report(&mut self, report: AppendDeliveryReport) {
        let mut shared = lock(&self.shared);
        shared.lifecycle = AppendStreamState::Closed;
        shared.last_report = Some(report.clone());
        shared.final_result = Some(Ok(report));
        drop(shared);
        self.pending_bytes.close();
        self.shutdown_notify.notify_waiters();
    }
}

async fn append_batch(
    request: AppendBatchRequest,
    payload: String,
) -> (Result<AppendRowsResult, Error>, usize) {
    let mut retries = 0usize;
    let mut backoff = request.retry.initial_backoff.min(request.retry.max_backoff);
    let mut saw_unknown = false;
    loop {
        let result = request
            .client
            .append_rows_with_timeout(
                &request.database,
                &request.schema,
                &request.table,
                payload.clone(),
                request.attempt_timeout,
            )
            .await;

        let error = match result {
            Ok(result) => return (Ok(result), retries),
            Err(error) => error,
        };
        saw_unknown |= error
            .append_details()
            .is_some_and(|details| details.status == AppendState::Unknown);
        let retryable = append_retryable(&error, request.retry.retry_unknown);
        if !retryable || retries >= request.retry.max_retries {
            let error = if retryable {
                error.with_context("retries", retries).set_persistent()
            } else {
                error
            };
            return (Err(preserve_unknown_outcome(error, saw_unknown)), retries);
        }
        let retry_delay = retry_delay(backoff, &error, request.retry.max_backoff);
        if !retry_delay.is_zero() {
            tokio::time::sleep(retry_delay).await;
        }
        retries += 1;
        backoff = next_backoff(backoff, request.retry.max_backoff);
    }
}

fn append_retryable(error: &Error, retry_unknown: bool) -> bool {
    if error.write_details().is_some() || error.server_retryable() == Some(false) {
        return false;
    }
    match error.append_details().map(|details| details.status) {
        Some(AppendState::Rejected) => error.is_temporary(),
        Some(AppendState::Unknown) if retry_unknown => error.http_status().is_none_or(|status| {
            status.is_success()
                || status == reqwest::StatusCode::REQUEST_TIMEOUT
                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error()
        }),
        _ => false,
    }
}

fn preserve_unknown_outcome(error: Error, saw_unknown: bool) -> Error {
    if !saw_unknown
        || error
            .append_details()
            .is_some_and(|details| details.status == AppendState::Unknown)
    {
        return error;
    }
    // A later rejection cannot establish whether an earlier attempt committed.
    let mut details = error
        .append_details()
        .cloned()
        .unwrap_or(AppendErrorDetails {
            status: AppendState::Unknown,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        });
    details.status = AppendState::Unknown;
    error
        .with_context("delivery", "an earlier append attempt may have committed")
        .set_append_details(details)
        .set_persistent()
}

fn unknown_append_error<E>(message: impl Into<String>, source: Option<E>) -> Error
where
    E: Into<anyhow::Error>,
{
    let mut error =
        Error::new(ErrorKind::AppendRowsFailed, message).set_append_details(AppendErrorDetails {
            status: AppendState::Unknown,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        });
    if let Some(source) = source {
        error = error.set_source(source);
    }
    error.set_persistent()
}

fn next_backoff(current: Duration, max_backoff: Duration) -> Duration {
    if current.is_zero() {
        return Duration::ZERO;
    }
    current
        .checked_mul(2)
        .unwrap_or(max_backoff)
        .min(max_backoff)
}

fn retry_delay(backoff: Duration, error: &Error, max_backoff: Duration) -> Duration {
    backoff
        .max(error.retry_after().unwrap_or(Duration::ZERO))
        .min(max_backoff)
}

fn try_claim_circuit_admission(shared: &mut SharedState) -> bool {
    match shared.circuit.state {
        AppendCircuitState::Closed => true,
        AppendCircuitState::HalfOpen => false,
        AppendCircuitState::Open => {
            let cooled_down = shared
                .circuit
                .opened_until
                .is_none_or(|until| Instant::now() >= until);
            if cooled_down && !shared.circuit.probe_admission_claimed {
                shared.circuit.probe_admission_claimed = true;
                true
            } else {
                false
            }
        }
    }
}

fn circuit_admission_available(shared: &SharedState) -> bool {
    match shared.circuit.state {
        AppendCircuitState::Closed => true,
        AppendCircuitState::HalfOpen => false,
        AppendCircuitState::Open => {
            shared
                .circuit
                .opened_until
                .is_none_or(|until| Instant::now() >= until)
                && !shared.circuit.probe_admission_claimed
        }
    }
}

fn record_circuit_success(shared: &mut SharedState, probe: bool) {
    shared.circuit.consecutive_failures = 0;
    if probe {
        shared.circuit.state = AppendCircuitState::Closed;
        shared.circuit.opened_until = None;
        shared.circuit.probe_admission_claimed = false;
    }
}

fn record_circuit_failure(
    shared: &mut SharedState,
    options: Option<AppendCircuitBreakerOptions>,
    error: &Error,
    probe: bool,
) -> bool {
    let Some(options) = options else {
        return false;
    };
    let availability_failure = error.is_temporary()
        || error.is_persistent()
        || error
            .append_details()
            .is_some_and(|details| details.status == AppendState::Unknown);
    if !availability_failure {
        if probe {
            record_circuit_success(shared, true);
        }
        return false;
    }

    shared.circuit.consecutive_failures = shared.circuit.consecutive_failures.saturating_add(1);
    if probe || shared.circuit.consecutive_failures >= options.failure_threshold {
        shared.circuit.state = AppendCircuitState::Open;
        shared.circuit.opened_until = Some(deadline_after(options.cooldown));
        shared.circuit.probe_admission_claimed = false;
        true
    } else {
        false
    }
}

#[derive(Debug, Clone, Copy)]
enum DropReason {
    BufferFull,
    InvalidRecord,
    RecordTooLarge,
    CircuitOpen,
    Closed,
}

fn note_drop(shared: &mut SharedState, reason: DropReason) {
    shared.dropped_rows = shared.dropped_rows.saturating_add(1);
    let counter = match reason {
        DropReason::BufferFull => &mut shared.dropped_by_reason.buffer_full,
        DropReason::InvalidRecord => &mut shared.dropped_by_reason.invalid_record,
        DropReason::RecordTooLarge => &mut shared.dropped_by_reason.record_too_large,
        DropReason::CircuitOpen => &mut shared.dropped_by_reason.circuit_open,
        DropReason::Closed => &mut shared.dropped_by_reason.closed,
    };
    *counter = counter.saturating_add(1);
}

fn empty_report() -> AppendDeliveryReport {
    AppendDeliveryReport {
        outcome: AppendDeliveryOutcome::Ok,
        accepted_rows: 0,
        committed_rows: 0,
        failed_rows: 0,
        unknown_rows: 0,
        dropped_rows: 0,
        committed_batches: 0,
        failed_batches: 0,
        unknown_batches: 0,
        retries: 0,
        duration: Duration::ZERO,
    }
}

fn deadline_after(duration: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(duration).unwrap_or(now)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Read;
    use std::io::Write;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    use super::*;

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        target: String,
        headers: HashMap<String, String>,
        body: String,
    }

    #[derive(Debug, Clone)]
    struct MockResponse {
        status: u16,
        body: String,
        delay: Duration,
        headers: Vec<(&'static str, String)>,
        body_delay: Duration,
        disconnect: Option<MockDisconnect>,
    }

    #[derive(Debug, Clone, Copy)]
    enum MockDisconnect {
        BeforeHeaders,
        DuringBody,
    }

    impl MockResponse {
        fn json(status: u16, body: impl Into<String>) -> Self {
            Self {
                status,
                body: body.into(),
                delay: Duration::ZERO,
                headers: Vec::new(),
                body_delay: Duration::ZERO,
                disconnect: None,
            }
        }

        fn committed(request: &RecordedRequest) -> Self {
            let rows = request
                .body
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count();
            Self::json(
                200,
                format!(r#"{{"status":"committed","num_rows_inserted":{rows}}}"#),
            )
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }

        fn with_header(mut self, name: &'static str, value: &str) -> Self {
            self.headers.push((name, value.to_string()));
            self
        }

        fn without_ack(mut self) -> Self {
            self.disconnect = Some(MockDisconnect::BeforeHeaders);
            self
        }

        fn with_body_delay(mut self, delay: Duration) -> Self {
            self.body_delay = delay;
            self
        }

        fn with_disconnect(mut self, disconnect: MockDisconnect) -> Self {
            self.disconnect = Some(disconnect);
            self
        }
    }

    type Responder = dyn Fn(usize, &RecordedRequest) -> MockResponse + Send + Sync;

    struct MockServer {
        endpoint: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        max_active: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
        accept_task: Option<thread::JoinHandle<()>>,
    }

    impl MockServer {
        fn start<F>(responder: F) -> Self
        where
            F: Fn(usize, &RecordedRequest) -> MockResponse + Send + Sync + 'static,
        {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let endpoint = format!("http://{address}");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let active = Arc::new(AtomicUsize::new(0));
            let max_active = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let responder: Arc<Responder> = Arc::new(responder);

            let accept_task = thread::spawn({
                let requests = requests.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                let stop = stop.clone();
                move || {
                    while !stop.load(Ordering::Acquire) {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                if stop.load(Ordering::Acquire) {
                                    break;
                                }
                                let requests = requests.clone();
                                let active = active.clone();
                                let max_active = max_active.clone();
                                let responder = responder.clone();
                                thread::spawn(move || {
                                    handle_connection(
                                        stream, requests, active, max_active, responder,
                                    );
                                });
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(1));
                            }
                            Err(_) => break,
                        }
                    }
                }
            });

            Self {
                endpoint,
                requests,
                max_active,
                stop,
                accept_task: Some(accept_task),
            }
        }

        fn client(&self) -> Client {
            let http_client = reqwest::Client::builder().no_proxy().build().unwrap();
            Client::new(&self.endpoint, http_client).unwrap()
        }

        fn client_with_write_timeout(&self, timeout: Duration) -> Client {
            Client::builder(&self.endpoint)
                .http_client(reqwest::Client::builder().no_proxy().build().unwrap())
                .write_timeout(timeout)
                .build()
                .unwrap()
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            lock(&self.requests).clone()
        }

        fn max_active(&self) -> usize {
            self.max_active.load(Ordering::Acquire)
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.endpoint.trim_start_matches("http://"));
            if let Some(task) = self.accept_task.take() {
                let _ = task.join();
            }
        }
    }

    fn handle_connection(
        mut stream: TcpStream,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        responder: Arc<Responder>,
    ) {
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let Some(request) = read_request(&mut stream) else {
            return;
        };
        let index = {
            let mut recorded = lock(&requests);
            let index = recorded.len();
            recorded.push(request.clone());
            index
        };
        let now_active = active.fetch_add(1, Ordering::AcqRel) + 1;
        max_active.fetch_max(now_active, Ordering::AcqRel);
        let response = responder(index, &request);
        if !response.delay.is_zero() {
            thread::sleep(response.delay);
        }
        if matches!(response.disconnect, Some(MockDisconnect::BeforeHeaders)) {
            active.fetch_sub(1, Ordering::AcqRel);
            return;
        }
        let reason = match response.status {
            200 => "OK",
            400 => "Bad Request",
            422 => "Unprocessable Entity",
            503 => "Service Unavailable",
            _ => "Test Response",
        };
        let headers = response
            .headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        let wire = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
            response.status,
            reason,
            response.body.len(),
            headers,
        );
        let _ = stream.write_all(wire.as_bytes());
        let _ = stream.flush();
        if !response.body_delay.is_zero() {
            thread::sleep(response.body_delay);
        }
        let body = response.body.as_bytes();
        let body = if matches!(response.disconnect, Some(MockDisconnect::DuringBody)) {
            &body[..body.len().min(1)]
        } else {
            body
        };
        let _ = stream.write_all(body);
        let _ = stream.flush();
        active.fetch_sub(1, Ordering::AcqRel);
    }

    fn read_request(stream: &mut TcpStream) -> Option<RecordedRequest> {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).ok()?;
            if read == 0 {
                return None;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers_text = String::from_utf8(bytes[..header_end].to_vec()).ok()?;
        let mut lines = headers_text.split("\r\n");
        let target = lines.next()?.split_whitespace().nth(1)?.to_string();
        let mut headers = HashMap::new();
        for line in lines.filter(|line| !line.is_empty()) {
            let (name, value) = line.split_once(':')?;
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len().saturating_sub(header_end) < content_length {
            let read = stream.read(&mut buffer).ok()?;
            if read == 0 {
                return None;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        let wire_body = &bytes[header_end..header_end.saturating_add(content_length)];
        let decoded_body = match headers.get("content-encoding").map(String::as_str) {
            Some("zstd") => zstd::stream::decode_all(wire_body).ok()?,
            None | Some("identity") => wire_body.to_vec(),
            Some(_) => return None,
        };
        let body = String::from_utf8(decoded_body).ok()?;
        Some(RecordedRequest {
            target,
            headers,
            body,
        })
    }

    #[test]
    fn mock_server_waits_for_delayed_request_bytes() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let mut stream = TcpStream::connect(server.endpoint.trim_start_matches("http://")).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        thread::sleep(Duration::from_millis(20));
        let body = "{\"id\":1}\n";
        let request = format!(
            "POST /rows HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert_eq!(server.requests().len(), 1);
    }

    #[test]
    fn rejects_non_object_rows() {
        assert!(matches!(
            prepare_record_for_try_send(&vec![1, 2, 3]),
            Err(AppendTrySendError::InvalidRecord)
        ));
    }

    #[test]
    fn report_outcomes_include_drops_and_unknown_rows() {
        let config = AppendStreamConfig {
            client: Client::new("http://127.0.0.1:1", reqwest::Client::new()).unwrap(),
            database: "scopedb".to_string(),
            schema: "public".to_string(),
            table: "events".to_string(),
            batch_bytes: 1024,
            max_batch_rows: 100,
            flush_interval: Duration::from_secs(1),
            channel_capacity: 1,
            max_pending_bytes: 1024,
            max_in_flight_requests: 1,
            retry: RetryConfig {
                max_retries: 0,
                retry_unknown: false,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            },
            failure_policy: AppendFailurePolicy::Continue,
            attempt_timeout: Some(Duration::from_secs(1)),
            circuit_breaker: None,
            batch_failure_listeners: Vec::new(),
        };
        let shared = Arc::new(Mutex::new(SharedState::default()));
        let budget = Arc::new(PendingBytesBudget::new(1024));
        let mut worker = AppendWorker {
            config,
            shared,
            pending_bytes: budget,
            shutdown_notify: Arc::new(Notify::new()),
            rows: Vec::new(),
            current_bytes: 0,
            batch_deadline: None,
            in_flight: JoinSet::new(),
            interval: DeliveryCounters {
                accepted_rows: 2,
                committed_rows: 1,
                unknown_rows: 1,
                ..DeliveryCounters::default()
            },
            reported_dropped_rows: 0,
            fatal: None,
        };
        let report = worker.take_report(1, Instant::now());
        assert_eq!(report.outcome, AppendDeliveryOutcome::Partial);
        assert_eq!(report.dropped_rows, 1);
    }

    #[test]
    fn backoff_is_capped() {
        assert_eq!(
            next_backoff(Duration::from_secs(4), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn retry_after_is_honored_and_capped() {
        let error = Error::new(ErrorKind::AppendRowsFailed, "busy")
            .set_retry_after(Duration::from_secs(30));
        assert_eq!(
            retry_delay(Duration::from_millis(100), &error, Duration::from_secs(5)),
            Duration::from_secs(5)
        );

        let error = Error::new(ErrorKind::AppendRowsFailed, "busy")
            .set_retry_after(Duration::from_millis(200));
        assert_eq!(
            retry_delay(Duration::from_secs(1), &error, Duration::from_secs(5)),
            Duration::from_secs(1)
        );
    }

    #[tokio::test]
    async fn invalid_stream_limits_fail_before_spawning_a_worker() {
        let client = Client::builder("https://example.invalid").build().unwrap();
        for builder in [
            client
                .table("events")
                .append_stream()
                .channel_capacity(usize::MAX),
            client
                .table("events")
                .append_stream()
                .initial_backoff(Duration::MAX),
            client
                .table("events")
                .append_stream()
                .max_backoff(Duration::MAX),
            client.table("..").append_stream(),
        ] {
            assert_eq!(
                builder
                    .build()
                    .err()
                    .expect("expected invalid configuration")
                    .kind(),
                ErrorKind::ConfigInvalid
            );
        }
    }

    #[test]
    fn max_batch_rows_cannot_exceed_protocol_limit() {
        let result = Client::new("https://example.com", reqwest::Client::new())
            .unwrap()
            .table("events")
            .append_stream()
            .max_batch_rows(MAX_APPEND_ROWS + 1)
            .build();
        let error = match result {
            Ok(_) => panic!("invalid max_batch_rows was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn append_stream_enforces_eight_mibibyte_request_limit() {
        let result = Client::new("https://example.com", reqwest::Client::new())
            .unwrap()
            .table("events")
            .append_stream()
            .target_batch_bytes(MAX_APPEND_BODY_BYTES + 1)
            .build();
        let error = match result {
            Ok(_) => panic!("oversized target_batch_bytes was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);

        let row = serde_json::json!({"payload": "x".repeat(MAX_APPEND_BODY_BYTES)});
        assert!(matches!(
            prepare_record_for_try_send(&row),
            Err(AppendTrySendError::RecordTooLarge {
                limit: MAX_APPEND_BODY_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn error_snapshot_preserves_http_metadata() {
        let error = Error::new(ErrorKind::AppendRowsFailed, "busy")
            .set_http_status(reqwest::StatusCode::SERVICE_UNAVAILABLE)
            .set_request_id("request-123".to_string())
            .set_retry_after(Duration::from_secs(2))
            .with_context("retries", 3)
            .set_source(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "lost response",
            ))
            .set_temporary();
        let restored = error.clone();
        assert_eq!(
            restored.http_status(),
            Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(restored.request_id(), Some("request-123"));
        assert_eq!(restored.retry_after(), Some(Duration::from_secs(2)));
        assert!(restored.is_temporary());
        let cause = StdError::source(&restored)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
        assert!(format!("{restored:?}").contains("retries: 3"));
    }

    #[tokio::test]
    async fn owned_http_client_does_not_follow_redirected_writes() {
        for status in [302, 307, 308] {
            let server = MockServer::start(move |index, request| {
                if index == 0 {
                    MockResponse::json(status, "redirect").with_header("Location", "/replayed")
                } else {
                    MockResponse::committed(request)
                }
            });
            let client = Client::builder(&server.endpoint).build().unwrap();
            let error = client
                .table("events")
                .append(r#"{"id":1}"#)
                .await
                .unwrap_err();
            assert_eq!(error.http_status().unwrap().as_u16(), status);
            assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
            assert_eq!(server.requests().len(), 1);
        }
    }

    #[tokio::test]
    async fn cancelled_admission_and_shutdown_wait_release_capacity_and_settle_rows() {
        use std::future::Future;
        use std::task::Poll;
        let (arrived_tx, arrived_rx) = oneshot::channel();
        let arrived_tx = Mutex::new(Some(arrived_tx));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let server = MockServer::start(move |_, request| {
            if let Some(tx) = lock(&arrived_tx).take() {
                let _ = tx.send(());
            }
            lock(&release_rx)
                .recv_timeout(Duration::from_secs(3))
                .unwrap();
            MockResponse::committed(request)
        });
        let row = serde_json::json!({"id":1});
        let bytes = row.to_string().len() + 1;
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .max_batch_rows(1)
            .max_buffered_bytes(bytes)
            .build()
            .unwrap();
        stream.send(&row).await.unwrap();
        arrived_rx.await.unwrap();
        {
            let mut blocked = Box::pin(stream.send(&row));
            std::future::poll_fn(|cx| {
                assert!(blocked.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(stream.stats().accepted_rows, 1);
        assert_eq!(stream.stats().pending_bytes, bytes);
        {
            let mut shutdown = Box::pin(stream.shutdown());
            std::future::poll_fn(|cx| {
                assert!(shutdown.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(stream.stats().state, AppendStreamState::Closing);
        assert!(stream.send(&row).await.is_err());
        release_tx.send(()).unwrap();
        let report = stream.shutdown().await.unwrap();
        assert_eq!(report.accepted_rows, 1);
        assert_eq!(report.committed_rows, 1);
        assert_eq!(stream.stats().pending_rows, 0);
        assert_eq!(stream.stats().pending_bytes, 0);
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn direct_append_rejects_excess_rows_before_network_io() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let error = server
            .client()
            .table("events")
            .append("{}\n".repeat(200_001))
            .await
            .unwrap_err();
        assert_eq!(
            error.append_details().unwrap().status,
            AppendState::Rejected
        );
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn direct_append_uses_default_request_compression() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let ndjson = "{\"id\":1}\n{\"id\":2}\n";

        let result = server
            .client()
            .table("events")
            .append(ndjson)
            .await
            .unwrap();

        assert_eq!(result.num_rows_inserted, 2);
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0]
                .headers
                .get("content-encoding")
                .map(String::as_str),
            Some("zstd")
        );
        assert_eq!(requests[0].body, ndjson);
    }

    #[tokio::test]
    async fn direct_append_rejects_a_body_over_eight_mibibytes_before_sending() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));

        let error = server
            .client()
            .table("events")
            .append("x".repeat(MAX_APPEND_BODY_BYTES + 1))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::AppendRowsFailed);
        assert_eq!(
            error.append_details().map(|details| details.status),
            Some(AppendState::Rejected)
        );
        assert!(error.message().contains("8388608-byte append limit"));
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn batches_ndjson_and_runs_requests_concurrently() {
        let server = MockServer::start(|_, request| {
            MockResponse::committed(request).with_delay(Duration::from_millis(30))
        });
        let stream = server
            .client()
            .table("events/archive")
            .with_database("analytics 2026")
            .with_schema("public")
            .append_stream()
            .target_batch_bytes(1)
            .max_concurrent_batches(3)
            .build()
            .unwrap();

        let admitted = stream
            .send_all((0..6).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();
        let report = stream.shutdown().await.unwrap();

        assert_eq!(admitted.accepted_rows, 6);
        assert_eq!(report.outcome, AppendDeliveryOutcome::Ok);
        assert_eq!(report.accepted_rows, 6);
        assert_eq!(report.committed_rows, 6);
        assert_eq!(report.committed_batches, 6);
        assert!(server.max_active() >= 2);
        let requests = server.requests();
        assert_eq!(requests.len(), 6);
        for request in requests {
            assert_eq!(
                request.target,
                "/v1/catalog/databases/analytics%202026/schemas/public/tables/events%2Farchive/ingest"
            );
            assert_eq!(
                request.headers.get("content-type").map(String::as_str),
                Some("application/x-ndjson")
            );
            assert_eq!(
                request.headers.get("content-encoding").map(String::as_str),
                Some("zstd")
            );
            assert_eq!(request.body.lines().count(), 1);
            assert!(request.body.starts_with('{'));
        }
    }

    #[tokio::test]
    async fn default_and_maximum_batch_targets() {
        for target in [None, Some(8 * 1024 * 1024)] {
            let server = MockServer::start(|_, request| MockResponse::committed(request));
            let mut builder = server
                .client()
                .table("events")
                .append_stream()
                .flush_interval(Duration::from_secs(3600))
                .max_concurrent_batches(1);
            if let Some(target) = target {
                builder = builder.target_batch_bytes(target);
            }
            let stream = builder.build().unwrap();
            let row = serde_json::json!({"payload": "x".repeat(2 * 1024 * 1024)});
            stream.send(&row).await.unwrap();
            stream.send(&row).await.unwrap();
            let report = stream.shutdown().await.unwrap();
            assert_eq!(report.committed_rows, 2);
            let row_counts = server
                .requests()
                .iter()
                .map(|request| request.body.lines().count())
                .collect::<Vec<_>>();
            assert_eq!(
                row_counts,
                if target.is_none() {
                    vec![1, 1]
                } else {
                    vec![2]
                }
            );
        }
    }

    #[tokio::test]
    async fn semantic_builder_names_split_batches_by_row_count() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .target_batch_bytes(1024 * 1024)
            .max_batch_rows(2)
            .max_buffered_bytes(1024 * 1024)
            .max_concurrent_batches(1)
            .build()
            .unwrap();

        stream
            .send_all((0..5).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();
        let report = stream.shutdown().await.unwrap();

        assert_eq!(report.committed_rows, 5);
        let row_counts = server
            .requests()
            .iter()
            .map(|request| request.body.lines().count())
            .collect::<Vec<_>>();
        assert_eq!(row_counts, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn client_builder_attaches_api_key_to_requests() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let http_client = reqwest::Client::builder().no_proxy().build().unwrap();
        let client = Client::builder(&server.endpoint)
            .api_key("secret-api-key")
            .http_client(http_client)
            .build()
            .unwrap();

        client.table("events").append(r#"{"id":1}"#).await.unwrap();

        let requests = server.requests();
        assert_eq!(
            requests[0].headers.get("authorization").map(String::as_str),
            Some("Bearer secret-api-key")
        );
        assert_eq!(
            requests[0]
                .headers
                .get("content-encoding")
                .map(String::as_str),
            Some("zstd")
        );
    }

    #[tokio::test]
    async fn lost_ack_requires_explicit_replay_and_counts_logical_rows() {
        for replay in [false, true] {
            for failure_policy in [AppendFailurePolicy::Stop, AppendFailurePolicy::Continue] {
                let commits = Arc::new(AtomicUsize::new(0));
                let server = MockServer::start({
                    let commits = commits.clone();
                    move |index, request| {
                        commits.fetch_add(1, Ordering::AcqRel);
                        let response = MockResponse::committed(request);
                        if index == 0 {
                            response.without_ack()
                        } else {
                            response
                        }
                    }
                });
                let mut builder = server
                    .client()
                    .table("events")
                    .append_stream()
                    .failure_policy(failure_policy)
                    .initial_backoff(Duration::ZERO);
                if replay {
                    builder = builder.retry_unknown(true);
                }
                let stream = builder.build().unwrap();
                stream.send(&serde_json::json!({"id": 1})).await.unwrap();

                let result = stream.shutdown().await;
                if !replay {
                    if failure_policy == AppendFailurePolicy::Stop {
                        assert_eq!(
                            result.unwrap_err().append_details().unwrap().status,
                            AppendState::Unknown
                        );
                    } else {
                        assert_eq!(result.unwrap().outcome, AppendDeliveryOutcome::Unknown);
                    }
                    let stats = stream.stats();
                    assert_eq!(commits.load(Ordering::Acquire), 1);
                    assert_eq!(stats.unknown_rows, 1);
                    assert_eq!(stats.retries, 0);
                    assert_eq!(stats.last_report.unwrap().unknown_rows, 1);
                    continue;
                }
                let report = result.unwrap();

                assert_eq!(commits.load(Ordering::Acquire), 2);
                let requests = server.requests();
                assert_eq!(requests.len(), 2);
                assert_eq!(requests[0].body, requests[1].body);
                assert_eq!(report.outcome, AppendDeliveryOutcome::Ok);
                assert_eq!(report.accepted_rows, 1);
                assert_eq!(report.committed_rows, 1);
                assert_eq!(report.committed_batches, 1);
                assert_eq!(report.unknown_rows, 0);
                assert_eq!(report.retries, 1);
            }
        }
    }

    #[tokio::test]
    async fn unknown_replay_respects_explicit_server_retry_refusal() {
        let server = MockServer::start(|_, _| {
            MockResponse::json(
                503,
                r#"{"code":"future_failure","message":"do not replay","retryable":false,"details":{"status":"unknown"}}"#,
            )
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .retry_unknown(true)
            .initial_backoff(Duration::ZERO)
            .build()
            .unwrap();
        stream.send(&serde_json::json!({"id":1})).await.unwrap();
        let error = stream.shutdown().await.unwrap_err();
        assert_eq!(error.code(), Some("future_failure"));
        assert_eq!(stream.stats().unknown_rows, 1);
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn direct_append_does_not_retry_lost_ack() {
        let server = MockServer::start(|_, request| MockResponse::committed(request).without_ack());

        let error = server
            .client()
            .table("events")
            .append(r#"{"id":1}"#)
            .await
            .unwrap_err();

        assert_eq!(server.requests().len(), 1);
        assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
        assert!(!error.is_retryable());
        assert!(StdError::source(&error).unwrap().is::<reqwest::Error>());
    }

    #[tokio::test]
    async fn zero_retry_budget_preserves_transport_cause_at_barrier() {
        let server = MockServer::start(|_, request| MockResponse::committed(request).without_ack());
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .max_retries(0)
            .build()
            .unwrap();
        stream.send(&serde_json::json!({"id": 1})).await.unwrap();

        let error = stream.shutdown().await.unwrap_err();

        assert_eq!(server.requests().len(), 1);
        assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
        assert!(StdError::source(&error).unwrap().is::<reqwest::Error>());
        assert_eq!(stream.stats().retries, 0);
    }

    #[tokio::test]
    async fn unknown_remains_unknown_after_terminal_rejection() {
        for final_status in [422, 503] {
            let server = MockServer::start(move |index, _| {
                match index {
                0 => MockResponse::json(503, r#"{"message":"ambiguous","code":"ingest_failure","details":{"status":"unknown"}}"#),
                1 => MockResponse::json(
                    final_status,
                    r#"{"message":"last rejection","request_id":"last-attempt","code":"ingest_failure","details":{"status":"rejected","row_errors":[{"row_index":0,"column":"id","message":"invalid"}],"row_errors_truncated":true}}"#,
                )
                .with_header("Retry-After", "2"),
                _ => panic!("unexpected retry request {index}"),
            }
            });
            let stream = server
                .client()
                .table("events")
                .append_stream()
                .retry_unknown(true)
                .max_retries(1)
                .initial_backoff(Duration::ZERO)
                .build()
                .unwrap();
            stream.send(&serde_json::json!({"id": 1})).await.unwrap();

            let error = stream.shutdown().await.unwrap_err();

            assert_eq!(server.requests().len(), 2);
            assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
            assert!(format!("{error:?}").contains("earlier append attempt may have committed"));
            assert_eq!(error.message(), "last rejection");
            assert_eq!(error.code(), Some("ingest_failure"));
            assert_eq!(error.details().unwrap()["status"], "rejected");
            assert_eq!(error.append_details().unwrap().row_errors[0].column, "id");
            assert!(error.append_details().unwrap().row_errors_truncated);
            assert_eq!(error.http_status().unwrap().as_u16(), final_status);
            assert_eq!(error.request_id(), Some("last-attempt"));
            assert_eq!(error.retry_after(), Some(Duration::from_secs(2)));
            if final_status == 503 {
                assert!(format!("{error:?}").contains("retries: 1"));
            }
            let stats = stream.stats();
            assert_eq!(stats.unknown_rows, 1);
            assert_eq!(stats.failed_rows, 0);
            assert_eq!(stats.retries, 1);
        }
    }

    #[tokio::test]
    async fn unknown_retry_budget_preserves_one_unresolved_logical_batch() {
        for max_retries in [0, 1] {
            for disconnect in [false, true] {
                let server = MockServer::start(move |index, request| {
                    if disconnect {
                        MockResponse::committed(request).without_ack()
                    } else {
                        MockResponse::json(
                            503,
                            format!(
                                r#"{{
                            "message":"still ambiguous",
                            "status":"unknown",
                            "request_id":"attempt-{index}"
                        }}"#
                            ),
                        )
                    }
                });
                let stream = server
                    .client()
                    .table("events")
                    .append_stream()
                    .retry_unknown(true)
                    .max_retries(max_retries)
                    .initial_backoff(Duration::ZERO)
                    .build()
                    .unwrap();
                stream.send(&serde_json::json!({"id": 1})).await.unwrap();

                let error = stream.shutdown().await.unwrap_err();

                assert_eq!(server.requests().len(), max_retries + 1);
                assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
                assert!(format!("{error:?}").contains(&format!("retries: {max_retries}")));
                if disconnect {
                    assert!(StdError::source(&error).unwrap().is::<reqwest::Error>());
                } else {
                    assert_eq!(
                        error.http_status(),
                        Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
                    );
                    assert_eq!(
                        error.request_id(),
                        Some(format!("attempt-{max_retries}").as_str())
                    );
                }
                let stats = stream.stats();
                assert_eq!(stats.accepted_rows, 1);
                assert_eq!(stats.unknown_rows, 1);
                assert_eq!(stats.failed_rows, 0);
                assert_eq!(stats.retries, max_retries as u64);
                assert_eq!(stats.pending_rows, 0);
            }
        }
    }

    #[tokio::test]
    async fn explicit_replay_retries_transient_unknown_responses() {
        for status in [200, 204, 408, 429, 500, 503] {
            let server = MockServer::start(move |index, request| match index {
                0 => MockResponse::json(status, "invalid response"),
                1 => MockResponse::committed(request),
                _ => panic!("unexpected retry request {index}"),
            });
            let stream = server
                .client()
                .table("events")
                .append_stream()
                .retry_unknown(true)
                .initial_backoff(Duration::ZERO)
                .build()
                .unwrap();
            stream.send(&serde_json::json!({"id": 1})).await.unwrap();

            let report = stream.shutdown().await.unwrap();

            assert_eq!(server.requests().len(), 2, "HTTP {status}");
            assert_eq!(report.committed_rows, 1);
            assert_eq!(report.retries, 1);
        }
    }

    #[tokio::test]
    async fn default_delivery_does_not_retry_permanent_unknown_responses() {
        for status in [400, 401, 403, 404, 413, 422] {
            let server = MockServer::start(move |_, _| {
                MockResponse::json(status, r#"{"message":"permanent failure"}"#)
            });
            let stream = server
                .client()
                .table("events")
                .append_stream()
                .build()
                .unwrap();
            stream.send(&serde_json::json!({"id": 1})).await.unwrap();

            let error = stream.shutdown().await.unwrap_err();

            assert_eq!(server.requests().len(), 1, "HTTP {status}");
            assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
            assert_eq!(error.http_status().unwrap().as_u16(), status);
            assert_eq!(stream.stats().retries, 0);
        }
    }

    #[tokio::test]
    async fn continue_barrier_settles_mixed_results_after_unknown_retry() {
        let ambiguous_attempts = AtomicUsize::new(0);
        let server = MockServer::start(move |_, request| {
            let row: serde_json::Value = serde_json::from_str(&request.body).unwrap();
            match row["id"].as_u64().unwrap() {
                0 => MockResponse::committed(request),
                1 if ambiguous_attempts.fetch_add(1, Ordering::AcqRel) == 0 => MockResponse::json(
                    503,
                    r#"{"message":"ambiguous","code":"ingest_failure","details":{"status":"unknown"}}"#,
                ),
                _ => MockResponse::json(
                    422,
                    r#"{"message":"invalid","code":"ingest_failure","details":{"status":"rejected"}}"#,
                ),
            }
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .retry_unknown(true)
            .failure_policy(AppendFailurePolicy::Continue)
            .circuit_breaker(None)
            .max_batch_rows(1)
            .initial_backoff(Duration::ZERO)
            .build()
            .unwrap();
        stream
            .send_all((0..3).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();

        let report = stream.flush().await.unwrap();

        assert_eq!(server.requests().len(), 4);
        assert_eq!(report.outcome, AppendDeliveryOutcome::Partial);
        assert_eq!(report.accepted_rows, 3);
        assert_eq!(report.committed_rows, 1);
        assert_eq!(report.failed_rows, 1);
        assert_eq!(report.unknown_rows, 1);
        assert_eq!(report.retries, 1);
        assert_eq!(
            report.accepted_rows,
            report.committed_rows + report.failed_rows + report.unknown_rows
        );
        assert_eq!(stream.stats().pending_rows, 0);
        assert_eq!(stream.shutdown().await.unwrap().accepted_rows, 0);
        assert_eq!(stream.stats().committed_rows, 1);
    }

    #[tokio::test]
    async fn continue_mode_retries_temporary_rejections_and_unknown_outcomes() {
        let server = MockServer::start(|index, request| match index {
            0 => MockResponse::json(
                503,
                r#"{"message":"busy","code":"ingest_failure","details":{"status":"rejected"}}"#,
            ),
            1 => MockResponse::committed(request),
            2 => MockResponse::json(
                503,
                r#"{"message":"ambiguous","code":"ingest_failure","details":{"status":"unknown"}}"#,
            ),
            3 | 4 => MockResponse::json(
                422,
                r#"{"message":"bad row","code":"ingest_failure","details":{"status":"rejected"}}"#,
            ),
            _ => panic!("unexpected retry request {index}"),
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .retry_unknown(true)
            .failure_policy(AppendFailurePolicy::Continue)
            .circuit_breaker(None)
            .target_batch_bytes(1)
            .max_concurrent_batches(1)
            .max_retries(4)
            .initial_backoff(Duration::ZERO)
            .max_backoff(Duration::ZERO)
            .build()
            .unwrap();

        stream
            .send_all((0..3).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();
        let report = stream.shutdown().await.unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0].body, requests[1].body);
        assert_eq!(requests[2].body, requests[3].body);
        assert_ne!(requests[3].body, requests[4].body);
        assert_eq!(report.outcome, AppendDeliveryOutcome::Partial);
        assert_eq!(report.accepted_rows, 3);
        assert_eq!(report.committed_rows, 1);
        assert_eq!(report.failed_rows, 1);
        assert_eq!(report.unknown_rows, 1);
        assert_eq!(report.committed_batches, 1);
        assert_eq!(report.failed_batches, 1);
        assert_eq!(report.unknown_batches, 1);
        assert_eq!(report.retries, 2);
        assert_eq!(
            report.accepted_rows,
            report.committed_rows + report.failed_rows + report.unknown_rows
        );
    }

    #[tokio::test]
    async fn zero_retry_budget_reports_unknown_timeout() {
        let server = MockServer::start(|_, request| {
            MockResponse::committed(request).with_delay(Duration::from_millis(100))
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .failure_policy(AppendFailurePolicy::Continue)
            .circuit_breaker(None)
            .target_batch_bytes(1)
            .max_retries(0)
            .attempt_timeout(Duration::from_millis(10))
            .build()
            .unwrap();

        stream.send(&serde_json::json!({"id": 1})).await.unwrap();
        let report = stream.shutdown().await.unwrap();

        assert_eq!(server.requests().len(), 1);
        assert_eq!(report.outcome, AppendDeliveryOutcome::Unknown);
        assert_eq!(report.unknown_rows, 1);
        assert_eq!(report.retries, 0);
    }

    #[tokio::test]
    async fn write_timeout_bounds_direct_append_including_response_body() {
        for body_delay in [false, true] {
            let server = MockServer::start(move |_, _| {
                let response =
                    MockResponse::json(200, r#"{"status":"committed","num_rows_inserted":1}"#);
                if body_delay {
                    response.with_body_delay(Duration::from_millis(150))
                } else {
                    response.with_delay(Duration::from_millis(150))
                }
            });
            let client = server.client_with_write_timeout(Duration::from_millis(50));
            let error = client
                .table("events")
                .append("{\"id\":1}".to_string())
                .await
                .unwrap_err();
            assert!(!error.is_retryable());
            assert_write_timeout_contract(&error);
            let source = std::error::Error::source(&error).unwrap();
            assert!(source.is::<tokio::time::error::Elapsed>());
            assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
            if body_delay {
                assert_eq!(error.http_status(), Some(reqwest::StatusCode::OK));
            }
            assert_eq!(server.requests().len(), 1);
        }
    }

    #[tokio::test]
    async fn append_stream_policies_inherit_the_client_write_timeout() {
        for policy in [AppendFailurePolicy::Stop, AppendFailurePolicy::Continue] {
            let server = MockServer::start(|_, request| {
                MockResponse::committed(request).with_delay(Duration::from_millis(150))
            });
            let stream = server
                .client_with_write_timeout(Duration::from_millis(50))
                .table("events")
                .append_stream()
                .failure_policy(policy)
                .build()
                .unwrap();
            stream.send(&serde_json::json!({"id":1})).await.unwrap();
            let result = stream.shutdown().await;
            if policy == AppendFailurePolicy::Continue {
                let report = result.unwrap();
                assert_eq!(report.outcome, AppendDeliveryOutcome::Unknown);
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
                assert_write_timeout_contract(&error);
                assert_write_timeout_contract(&stream.flush().await.unwrap_err());
            }
            assert_eq!(stream.stats().unknown_rows, 1);
            assert_write_timeout_contract(&stream.stats().last_failure.unwrap().error);
            assert_eq!(stream.stats().retries, 0);
            assert_eq!(server.requests().len(), 1);
        }
    }

    #[tokio::test]
    async fn attempt_timeout_can_exceed_the_client_write_timeout() {
        let server = MockServer::start(|_, request| {
            MockResponse::committed(request).with_delay(Duration::from_millis(50))
        });
        let stream = server
            .client_with_write_timeout(Duration::from_millis(10))
            .table("events")
            .append_stream()
            .attempt_timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        stream.send(&serde_json::json!({"id":1})).await.unwrap();
        assert_eq!(stream.shutdown().await.unwrap().committed_rows, 1);
    }

    #[tokio::test]
    async fn write_timeout_preserves_a_shorter_custom_http_timeout() {
        for body_delay in [false, true] {
            let server = MockServer::start(move |_, _| {
                let response =
                    MockResponse::json(200, r#"{"status":"committed","num_rows_inserted":1}"#);
                if body_delay {
                    response.with_body_delay(Duration::from_millis(150))
                } else {
                    response.with_delay(Duration::from_millis(150))
                }
            });
            let http = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_millis(50))
                .build()
                .unwrap();
            let client = Client::builder(&server.endpoint)
                .http_client(http)
                .write_timeout(Duration::from_secs(1))
                .build()
                .unwrap();
            let error = client
                .table("events")
                .append("{\"id\":1}".to_string())
                .await
                .unwrap_err();
            let mut source = std::error::Error::source(&error);
            let mut transport_timeout = false;
            while let Some(error) = source {
                transport_timeout |= error
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(reqwest::Error::is_timeout);
                source = error.source();
            }
            assert!(
                transport_timeout,
                "the native transport cause must remain available"
            );
            assert_write_timeout_contract(&error);
            if body_delay {
                assert_eq!(error.http_status(), Some(reqwest::StatusCode::OK));
            }
            assert_eq!(server.requests().len(), 1);
        }
    }

    fn assert_write_timeout_contract(error: &crate::Error) {
        let details = error.write_details().unwrap();
        assert_eq!(details.reason, crate::WriteErrorReason::Timeout);
        assert_eq!(details.outcome, AppendState::Unknown);
        assert!(!error.is_retryable());
        assert_eq!(
            serde_json::to_value(details).unwrap(),
            serde_json::json!({"reason":"timeout","outcome":"unknown"})
        );
    }

    #[tokio::test]
    async fn write_timeout_starts_after_materializing_the_request() {
        struct SlowPayload;
        impl From<SlowPayload> for String {
            fn from(_: SlowPayload) -> Self {
                thread::sleep(Duration::from_millis(100));
                "{\"id\":1}".to_string()
            }
        }
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let result = server
            .client_with_write_timeout(Duration::from_millis(50))
            .append_rows("scopedb", "public", "events", SlowPayload)
            .await
            .unwrap();
        assert_eq!(result.num_rows_inserted, 1);
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn server_timeout_rejection_remains_safely_retryable() {
        let server = MockServer::start(|_, _| {
            MockResponse::json(
                408,
                r#"{"message":"admission timed out","retryable":true,"code":"ingest_failure","details":{"status":"rejected"}}"#,
            )
        });
        let error = server
            .client()
            .table("events")
            .append("{\"id\":1}".to_string())
            .await
            .unwrap_err();
        assert!(error.write_details().is_none());
        assert_eq!(
            error.append_details().unwrap().status,
            AppendState::Rejected
        );
        assert!(error.is_retryable());
    }

    #[tokio::test]
    async fn connection_loss_is_not_replayed_when_append_retries_are_disabled() {
        for disconnect in [MockDisconnect::BeforeHeaders, MockDisconnect::DuringBody] {
            let server = MockServer::start(move |_, request| {
                MockResponse::committed(request).with_disconnect(disconnect)
            });
            let client = server.client();
            let direct = client.table("events").append(r#"{"id":1}"#).await;
            let stream = client
                .table("events")
                .append_stream()
                .max_retries(0)
                .build()
                .unwrap();
            stream.send(&serde_json::json!({"id":1})).await.unwrap();
            for error in [direct.unwrap_err(), stream.shutdown().await.unwrap_err()] {
                assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
                assert!(!error.is_retryable());
                if matches!(disconnect, MockDisconnect::DuringBody) {
                    assert_eq!(error.http_status(), Some(reqwest::StatusCode::OK));
                }
            }
            assert_eq!(server.requests().len(), 2);
        }
    }

    #[tokio::test]
    async fn stop_policy_closes_after_a_rejected_batch() {
        let server = MockServer::start(|_, _| {
            MockResponse::json(
                422,
                r#"{"message":"invalid","code":"ingest_failure","details":{"status":"rejected"}}"#,
            )
            .with_delay(Duration::from_millis(20))
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .target_batch_bytes(1)
            .max_concurrent_batches(1)
            .build()
            .unwrap();
        stream
            .send_all((0..2).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();

        let error = stream.shutdown().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AppendRowsFailed);
        assert_eq!(
            error.append_details().map(|details| details.status),
            Some(AppendState::Rejected)
        );
        let stats = stream.stats();
        assert_eq!(stats.state, AppendStreamState::Failed);
        assert_eq!(stats.accepted_rows, 2);
        assert_eq!(stats.failed_rows, 2);
        assert_eq!(stats.pending_rows, 0);
        let report = stats.last_report.unwrap();
        assert_eq!(report.accepted_rows, 2);
        assert_eq!(report.failed_rows, 2);
        assert!(matches!(
            stream.try_send(&serde_json::json!({"id": 3})),
            Err(AppendTrySendError::Closed)
        ));
    }

    #[tokio::test]
    async fn strict_terminal_waits_for_every_in_flight_outcome() {
        use std::future::Future;
        use std::task::Poll;
        let both_started = std::sync::Barrier::new(2);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let server = MockServer::start(move |index, _| {
            both_started.wait();
            if index == 0 {
                MockResponse::json(
                    422,
                    r#"{"message":"invalid","code":"invalid_schema","details":{"status":"rejected"}}"#,
                )
            } else {
                lock(&release_rx)
                    .recv_timeout(Duration::from_secs(3))
                    .unwrap();
                MockResponse::json(
                    503,
                    r#"{"message":"ambiguous","code":"ingest_failure","details":{"status":"unknown"}}"#,
                )
            }
        });
        let (failed_tx, failed_rx) = oneshot::channel();
        let failed_tx = Mutex::new(Some(failed_tx));
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .max_batch_rows(1)
            .max_concurrent_batches(2)
            .max_retries(0)
            .on_batch_failure(move |_| {
                if let Some(tx) = lock(&failed_tx).take() {
                    let _ = tx.send(());
                }
            })
            .build()
            .unwrap();
        stream
            .send_all((0..2).map(|id| serde_json::json!({"id":id})))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), failed_rx)
            .await
            .unwrap()
            .unwrap();
        let mut shutdown = Box::pin(stream.shutdown());
        std::future::poll_fn(|cx| {
            assert!(shutdown.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        release_tx.send(()).unwrap();
        let error = shutdown.await.unwrap_err();
        assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
        let stats = stream.stats();
        assert_eq!(stats.failed_rows, 1);
        assert_eq!(stats.unknown_rows, 1);
        assert_eq!(stats.pending_rows, 0);
        let report = stats.last_report.unwrap();
        assert_eq!(report.accepted_rows, 2);
        assert_eq!(report.failed_rows, 1);
        assert_eq!(report.unknown_rows, 1);
    }

    #[tokio::test]
    async fn concurrent_flush_and_shutdown_both_settle() {
        let server = MockServer::start(|_, request| {
            MockResponse::committed(request).with_delay(Duration::from_millis(5))
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .build()
            .unwrap();
        stream.send(&serde_json::json!({"id": 1})).await.unwrap();

        let (flush, shutdown) = tokio::join!(stream.flush(), stream.shutdown());
        assert!(flush.is_ok(), "flush failed: {flush:?}");
        assert!(shutdown.is_ok(), "shutdown failed: {shutdown:?}");
        assert_eq!(stream.stats().state, AppendStreamState::Closed);
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_across_clones() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .build()
            .unwrap();
        let clone = stream.clone();
        stream.send(&serde_json::json!({"id": 1})).await.unwrap();

        let (first, second) = tokio::join!(stream.shutdown(), clone.shutdown());
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(server.requests().len(), 1);
        assert!(matches!(
            stream.try_send(&serde_json::json!({"id": 2})),
            Err(AppendTrySendError::Closed)
        ));
    }

    #[tokio::test]
    async fn try_send_accounts_for_local_drops() {
        let client = Client::new("http://127.0.0.1:1", reqwest::Client::new()).unwrap();
        let stream = client
            .table("events")
            .append_stream()
            .failure_policy(AppendFailurePolicy::Continue)
            .max_buffered_bytes(1)
            .build()
            .unwrap();

        assert!(matches!(
            stream.try_send(&serde_json::json!({"id": 1})),
            Err(AppendTrySendError::BufferFull)
        ));
        let report = stream.shutdown().await.unwrap();
        assert_eq!(report.outcome, AppendDeliveryOutcome::Failed);
        assert_eq!(report.accepted_rows, 0);
        assert_eq!(report.dropped_rows, 1);
        assert_eq!(stream.stats().dropped_by_reason.buffer_full, 1);
    }
}
