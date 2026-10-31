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

use std::fmt;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
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

use crate::AppendDeliveryOutcome;
use crate::AppendErrorDetails;
use crate::AppendFailurePolicy;
use crate::AppendRowsResult;
use crate::AppendState;
use crate::Error;
use crate::ErrorKind;
use crate::ErrorOperation;
use crate::Table;
use crate::append::AppendOptions;
use crate::append::RecordError;
use crate::append::append_batch;
use crate::append::config_error;
use crate::append::encode_record;
use crate::append::validate_deadline;
use crate::append::validate_positive;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CIRCUIT_FAILURE_THRESHOLD: usize = 5;
const DEFAULT_CIRCUIT_COOLDOWN: Duration = Duration::from_secs(30);

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

/// Rows accepted by a bulk local-admission operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendAdmissionResult {
    pub accepted_rows: u64,
}

/// Aggregate settlement information since the previous delivery barrier.
/// Counts do not identify individual input records. Use application-owned
/// business keys to reconcile rows before replaying them.
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
    /// A shared terminal failure report starts at the earliest outstanding barrier.
    /// Zero when the worker terminates unexpectedly and its wait time is unavailable.
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

impl From<RecordError> for AppendTrySendError {
    fn from(error: RecordError) -> Self {
        match error {
            RecordError::Serialization(error) => Self::Serialization(error),
            RecordError::InvalidRecord => Self::InvalidRecord,
            RecordError::RecordTooLarge { bytes, limit } => Self::RecordTooLarge { bytes, limit },
        }
    }
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

impl std::error::Error for AppendTrySendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialization(err) => Some(err),
            _ => None,
        }
    }
}

type BatchFailureListener = Arc<dyn Fn(&AppendBatchFailureEvent) + Send + Sync>;

/// Configures a bounded, concurrent table append stream.
///
/// Retries temporary rejections by default. Replaying unknown outcomes requires
/// [`Self::retry_unknown`] because an unacknowledged attempt may have committed.
///
/// # Telemetry
///
/// For a log or telemetry sink that should keep running after batch failures,
/// use [`AppendFailurePolicy::Continue`] and keep the circuit breaker enabled.
/// Start with one concurrent batch per stream when producers run on multiple
/// application replicas; tune the total concurrency across all producers.
///
/// ```no_run
/// use scopedb_client::AppendFailurePolicy;
///
/// # async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
/// let stream = client.table("events").append_stream()
///     .failure_policy(AppendFailurePolicy::Continue)
///     .max_concurrent_batches(1)
///     .build()?;
///
/// if let Err(error) = stream.try_send(&serde_json::json!({"name": "request.completed"})) {
///     eprintln!("telemetry row not admitted: {error}");
/// }
/// let report = stream.shutdown().await?;
/// println!("Write results: {report:?}");
/// # Ok(())
/// # }
/// ```
///
/// Continue mode is best effort. Monitor reports and [`AppendStream::stats`]
/// for failed, unknown, and locally dropped rows. Send failure diagnostics to
/// a separate sink. [`AppendStream::try_send`] never waits for capacity;
/// [`AppendStream::send`] provides backpressure instead.
pub struct AppendStreamBuilder {
    table: Table,
    flush_interval: Duration,
    channel_capacity: usize,
    max_pending_bytes: usize,
    failure_policy: AppendFailurePolicy,
    circuit_breaker: Option<AppendCircuitBreakerOptions>,
    batch_failure_listeners: Vec<BatchFailureListener>,
    options: AppendOptions,
}

impl AppendStreamBuilder {
    /// Chooses whether to stop or continue after a batch fails. Defaults to Stop.
    pub fn failure_policy(mut self, failure_policy: AppendFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    /// Sets the target NDJSON payload size (default 4 MiB, maximum 8 MiB).
    /// One row may exceed this target up to 8 MiB.
    pub fn target_batch_bytes(mut self, target_batch_bytes: usize) -> Self {
        self.options.batch_bytes = target_batch_bytes;
        self
    }

    /// Sets the maximum rows in one HTTP batch, up to the 200,000-row protocol limit.
    pub fn max_batch_rows(mut self, max_batch_rows: usize) -> Self {
        self.options.max_batch_rows = max_batch_rows;
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

    /// Sets the maximum number of append batches sent concurrently per stream (default 4).
    pub fn max_concurrent_batches(mut self, max_concurrent_batches: usize) -> Self {
        self.options.max_in_flight_requests = max_concurrent_batches;
        self
    }

    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.options.retry.max_retries = max_retries;
        self
    }

    /// Opts into replaying transient unknown outcomes. This can duplicate rows.
    /// Client timeouts are never replayed automatically, even when enabled.
    pub fn retry_unknown(mut self, enabled: bool) -> Self {
        self.options.retry.retry_unknown = enabled;
        self
    }

    pub fn initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.options.retry.initial_backoff = initial_backoff;
        self
    }

    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.options.retry.max_backoff = max_backoff;
        self
    }

    /// Sets a timeout for each HTTP attempt, overriding the client's write timeout.
    /// A timeout has an unknown commit outcome. By default the client uses 30 seconds.
    /// The duration must be finite and nonzero; this deadline cannot be disabled.
    pub fn attempt_timeout(mut self, attempt_timeout: Duration) -> Self {
        self.options.attempt_timeout = Some(attempt_timeout);
        self
    }

    /// Configures the continue-mode availability circuit, or disables it with `None`.
    /// Enabled by default: five consecutive availability failures pause dispatch
    /// for 30 seconds before probing recovery.
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
        validate_builder(&self)
            .map_err(|error| error.with_operation(ErrorOperation::BuildAppendStream))?;
        tokio::runtime::Handle::try_current().map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::BuildAppendStream,
                "append stream must be built inside a Tokio runtime",
            )
            .set_source(err)
        })?;

        Ok(AppendStream::new(AppendStreamConfig {
            table: self.table,
            options: self.options,
            flush_interval: self.flush_interval,
            channel_capacity: self.channel_capacity,
            max_pending_bytes: self.max_pending_bytes,
            failure_policy: self.failure_policy,
            circuit_breaker: self.circuit_breaker,
            batch_failure_listeners: self.batch_failure_listeners,
        }))
    }

    pub(crate) fn new(table: Table) -> Self {
        Self {
            table,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
            options: AppendOptions::default(),
            failure_policy: AppendFailurePolicy::Stop,
            circuit_breaker: Some(AppendCircuitBreakerOptions::default()),
            batch_failure_listeners: Vec::new(),
        }
    }
}

fn validate_builder(builder: &AppendStreamBuilder) -> Result<(), Error> {
    builder.table.validate_append()?;
    builder.options.validate()?;
    if builder.channel_capacity > Semaphore::MAX_PERMITS {
        return Err(config_error("channel_capacity is too large"));
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

struct AppendStreamConfig {
    table: Table,
    flush_interval: Duration,
    channel_capacity: usize,
    max_pending_bytes: usize,
    failure_policy: AppendFailurePolicy,
    circuit_breaker: Option<AppendCircuitBreakerOptions>,
    batch_failure_listeners: Vec<BatchFailureListener>,
    options: AppendOptions,
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
}

impl PendingBytesBudget {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
        }
    }

    async fn acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit, PendingAcquireError> {
        if bytes > self.capacity {
            return Err(PendingAcquireError::ExceedsCapacity);
        }
        self.semaphore
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| PendingAcquireError::Closed)
    }

    fn try_acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit, PendingAcquireError> {
        if bytes > self.capacity {
            return Err(PendingAcquireError::ExceedsCapacity);
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
    // Retain unreported settlement even if the worker is aborted.
    interval: DeliveryCounters,
    reported_dropped_rows: u64,
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
            interval: DeliveryCounters::default(),
            reported_dropped_rows: 0,
            circuit: CircuitRuntime::default(),
        }
    }
}

impl SharedState {
    fn take_report(
        &mut self,
        dropped_rows_at_barrier: u64,
        duration: Duration,
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
            duration,
        };
        self.interval = DeliveryCounters::default();
        self.last_report = Some(report.clone());
        report
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
    /// Serializes and admits one row, waiting for bounded local capacity.
    ///
    /// Success means local admission only. Use [`Self::flush`] or
    /// [`Self::shutdown`] to wait for remote settlement.
    pub async fn send<T>(&self, row: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.check_open()?;
        let payload = encode_record(row)?;
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
    /// Returns an error if the circuit is open or local capacity is full.
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

        let payload = match encode_record(row).map_err(AppendTrySendError::from) {
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
            Err(PendingAcquireError::Full) => {
                note_drop(&mut lock(&self.inner.shared), DropReason::BufferFull);
                return Err(AppendTrySendError::BufferFull);
            }
            Err(PendingAcquireError::ExceedsCapacity) => {
                note_drop(&mut lock(&self.inner.shared), DropReason::RecordTooLarge);
                return Err(AppendTrySendError::RecordTooLarge {
                    bytes: reserved_bytes,
                    limit: self.inner.pending_bytes.capacity,
                });
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
    ///
    /// If admission fails partway, [`Error::append_admission`] identifies
    /// the prefix admitted by this call, independently of other producers.
    /// Call [`Self::shutdown`] to settle those rows; [`Self::stats`] retains
    /// cumulative counts. Dropping this future can leave an admitted prefix
    /// without returning a count; it does not cancel those rows.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] with admission progress when encoding, capacity, or
    /// stream state prevents the next admission.
    pub async fn send_all<I>(&self, rows: I) -> Result<AppendAdmissionResult, Error>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        let mut accepted_rows = 0u64;
        for row in rows {
            self.send(&row)
                .await
                .map_err(|error| error.with_append_admission(accepted_rows))?;
            accepted_rows = accepted_rows.saturating_add(1);
        }
        Ok(AppendAdmissionResult { accepted_rows })
    }

    /// Dispatches rows admitted before this barrier and waits for their outcomes.
    ///
    /// The report covers activity since the previous barrier. On a Stop-mode
    /// failure, [`Error::append_delivery_report`] carries the terminal settlement,
    /// also retained in [`Self::stats`].
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
                TerminalResult::Complete(result) => {
                    return result.map_err(|error| error.with_operation(ErrorOperation::Flush));
                }
            }

            let permit = match self.inner.tx.reserve().await {
                Ok(permit) => permit,
                Err(_) => return self.wait_for_terminal(ErrorOperation::Flush).await,
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
                Ok(Err(_)) | Err(_) => self.wait_for_terminal(ErrorOperation::Flush).await,
            };
        }
    }

    /// Permanently closes the stream after settling every admitted row.
    ///
    /// This method is idempotent across cloned producer handles. Once polled,
    /// dropping its future stops waiting but settlement continues in the worker.
    /// Call shutdown again to observe completion. Failure reports are returned
    /// through [`Error::append_delivery_report`] and remain available in stats().
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
                    return terminal_result(&shared)
                        .map_err(|error| error.with_operation(ErrorOperation::Shutdown));
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

        self.wait_for_terminal(ErrorOperation::Shutdown).await
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
                ErrorKind::RateLimited,
                ErrorOperation::AdmitRows,
                "append stream local byte budget is full",
            )
            .with_transient_failure(true),
            PendingAcquireError::ExceedsCapacity => Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::AdmitRows,
                format!(
                    "append row requires {requested} buffered bytes, exceeding max_pending_bytes={}",
                    self.inner.pending_bytes.capacity
                ),
            ),
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

    async fn wait_for_terminal(
        &self,
        operation: ErrorOperation,
    ) -> Result<AppendDeliveryReport, Error> {
        loop {
            let notified = self.inner.shutdown_notify.notified();
            {
                let shared = lock(&self.inner.shared);
                if matches!(
                    shared.lifecycle,
                    AppendStreamState::Closed | AppendStreamState::Failed
                ) && shared.final_result.is_some()
                {
                    return terminal_result(&shared)
                        .map_err(|error| error.with_operation(operation));
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
    match shared
        .final_result
        .as_ref()
        .expect("terminal result is present")
    {
        Ok(report) => Ok(report.clone()),
        Err(error) => Err(error.clone().with_append_delivery(
            shared
                .last_report
                .clone()
                .expect("terminal settlement is present"),
        )),
    }
}

fn error_from_shared(shared: &SharedState) -> Error {
    shared
        .fatal
        .as_ref()
        .cloned()
        .map(Error::for_closed_stream)
        .unwrap_or_else(|| {
            Error::new(
                ErrorKind::Closed,
                ErrorOperation::AdmitRows,
                "append stream is closed",
            )
        })
}

struct BatchOutcome {
    records: Vec<BufferedRecord>,
    batch_bytes: usize,
    result: Result<AppendRowsResult, Error>,
    retries: usize,
    circuit_probe: bool,
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
        fatal: None,
    };

    loop {
        if worker.fatal.is_some() {
            worker.finish_fatal(&mut rx, None).await;
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
                        if let Some(barrier) = worker.complete_barrier(barrier).await {
                            worker.finish_fatal(&mut rx, Some(barrier)).await;
                            return;
                        }
                    }
                    Some(AppendCommand::Shutdown(barrier)) => {
                        if let Some(barrier) = worker.complete_barrier(barrier).await {
                            worker.finish_fatal(&mut rx, Some(barrier)).await;
                        } else {
                            worker.finish_closed();
                        }
                        return;
                    }
                    None => {
                        worker.dispatch_buffered().await;
                        worker.drain_in_flight().await;
                        if worker.fatal.is_some() {
                            worker.finish_fatal(&mut rx, None).await;
                        } else {
                            let dropped = lock(&worker.shared).dropped_rows;
                            let report = lock(&worker.shared).take_report(dropped, Duration::ZERO);
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
            ErrorKind::Internal,
            ErrorOperation::AdmitRows,
            "append stream worker stopped before publishing a final result",
        ),
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
    shared.interval.unknown_rows = shared.interval.unknown_rows.saturating_add(unresolved);
    if unresolved > 0 {
        shared.interval.unknown_batches = shared.interval.unknown_batches.saturating_add(1);
    }
    shared.interval.accepted_rows = shared
        .interval
        .committed_rows
        .saturating_add(shared.interval.failed_rows)
        .saturating_add(shared.interval.unknown_rows);
    let dropped = shared.dropped_rows;
    shared.take_report(dropped, Duration::ZERO);
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
        {
            let mut shared = lock(&self.shared);
            shared.interval.accepted_rows = shared.interval.accepted_rows.saturating_add(1);
        }
        if self.config.options.would_overflow(
            self.current_bytes,
            self.rows.len(),
            record.payload.len(),
        ) {
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
        if self
            .config
            .options
            .is_full(self.current_bytes, self.rows.len())
        {
            self.dispatch_buffered().await;
        }
    }

    async fn dispatch_buffered(&mut self) {
        if self.rows.is_empty() || self.fatal.is_some() {
            return;
        }
        while self.in_flight.len() >= self.config.options.max_in_flight_requests {
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
        let records = mem::take(&mut self.rows);
        let batch_bytes = mem::take(&mut self.current_bytes);
        self.batch_deadline = None;
        let payload = records
            .iter()
            .map(|record| record.payload.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let table = self.config.table.clone();
        let options = self.config.options.clone();
        lock(&self.shared).in_flight_batches += 1;
        self.in_flight.spawn(async move {
            let (result, retries) = append_batch(table, options, payload).await;
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

    async fn complete_barrier(&mut self, barrier: BarrierCommand) -> Option<BarrierCommand> {
        self.dispatch_buffered().await;
        self.drain_in_flight().await;
        if self.fatal.is_some() {
            return Some(barrier);
        }
        let report = lock(&self.shared).take_report(
            barrier.dropped_rows_at_barrier,
            barrier.started_at.elapsed(),
        );
        let _ = barrier.ack.send(Ok(report));
        None
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
            shared.interval.retries = shared
                .interval
                .retries
                .saturating_add(outcome.retries as u64);

            match &outcome.result {
                Ok(result) => {
                    let committed = result.num_rows_inserted.max(0) as u64;
                    shared.committed_rows = shared.committed_rows.saturating_add(committed);
                    shared.interval.committed_rows =
                        shared.interval.committed_rows.saturating_add(committed);
                    shared.interval.committed_batches =
                        shared.interval.committed_batches.saturating_add(1);
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
                        shared.interval.unknown_rows =
                            shared.interval.unknown_rows.saturating_add(rows);
                        shared.interval.unknown_batches =
                            shared.interval.unknown_batches.saturating_add(1);
                    } else {
                        shared.failed_rows = shared.failed_rows.saturating_add(rows);
                        shared.interval.failed_rows =
                            shared.interval.failed_rows.saturating_add(rows);
                        shared.interval.failed_batches =
                            shared.interval.failed_batches.saturating_add(1);
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
        shared.interval.failed_rows = shared.interval.failed_rows.saturating_add(rows);
        drop(shared);
        drop(records);
    }

    async fn finish_fatal(
        &mut self,
        rx: &mut mpsc::Receiver<AppendCommand>,
        barrier: Option<BarrierCommand>,
    ) {
        rx.close();
        let buffered = mem::take(&mut self.rows);
        self.mark_records_failed(buffered);
        self.current_bytes = 0;
        self.batch_deadline = None;

        let mut barriers: Vec<_> = barrier.into_iter().collect();
        while let Some(command) = rx.recv().await {
            match command {
                AppendCommand::Record(record) => {
                    {
                        let mut shared = lock(&self.shared);
                        shared.interval.accepted_rows =
                            shared.interval.accepted_rows.saturating_add(1);
                    }
                    self.mark_records_failed(vec![record]);
                }
                AppendCommand::Flush(barrier) | AppendCommand::Shutdown(barrier) => {
                    barriers.push(barrier);
                }
            }
        }
        // All outstanding barriers observe this settlement; retain the earliest wait.
        let started_at = barriers
            .iter()
            .map(|barrier| barrier.started_at)
            .min()
            .unwrap_or_else(Instant::now);
        self.drain_in_flight().await;

        let fatal = self.fatal.clone().unwrap_or_else(|| {
            Error::new(
                ErrorKind::Internal,
                ErrorOperation::AdmitRows,
                "append stream failed",
            )
        });
        {
            let mut shared = lock(&self.shared);
            let resolved = shared
                .committed_rows
                .saturating_add(shared.failed_rows)
                .saturating_add(shared.unknown_rows);
            let unresolved = shared.accepted_rows.saturating_sub(resolved);
            if unresolved > 0 {
                shared.unknown_rows = shared.unknown_rows.saturating_add(unresolved);
                shared.interval.unknown_rows =
                    shared.interval.unknown_rows.saturating_add(unresolved);
                shared.interval.unknown_batches = shared.interval.unknown_batches.saturating_add(1);
            }
            shared.pending_rows = 0;
            shared.pending_bytes = 0;
            shared.in_flight_batches = 0;
        }
        let dropped = lock(&self.shared).dropped_rows;
        lock(&self.shared).take_report(dropped, started_at.elapsed());
        {
            let mut shared = lock(&self.shared);
            shared.lifecycle = AppendStreamState::Failed;
            shared.fatal = Some(fatal.clone());
            shared.final_result = Some(Err(fatal.clone()));
        }
        for barrier in barriers {
            let _ = barrier.ack.send(Err(fatal.clone()));
        }
        self.pending_bytes.close();
        self.shutdown_notify.notify_waiters();
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

fn unknown_append_error<E>(message: impl Into<String>, source: Option<E>) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    let mut error = Error::new(ErrorKind::Internal, ErrorOperation::Append, message)
        .set_append_details(AppendErrorDetails {
            status: AppendState::Unknown,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        });
    if let Some(source) = source {
        error = error.set_source(source);
    }
    error
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
    let availability_failure = error.is_transient()
        || matches!(
            error.kind(),
            ErrorKind::Unavailable | ErrorKind::Timeout | ErrorKind::InvalidResponse
        );
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
