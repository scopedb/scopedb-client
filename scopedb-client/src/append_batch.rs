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
use std::future::Future;
use std::future::poll_fn;
use std::ops::Range;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use serde::Serialize;

use crate::AppendDeliveryOutcome;
use crate::AppendFailurePolicy;
use crate::AppendRowsResult;
use crate::AppendState;
use crate::Client;
use crate::Error;
use crate::append::AppendBatchRequest;
use crate::append::AppendOptions;
use crate::append::append_batch;
use crate::append::prepare_record;

/// Configures a reusable writer for finite slices of rows.
///
/// Defaults match [`crate::AppendStreamBuilder`]: 4 MiB target batches, at most
/// 200,000 rows per request, four concurrent batches and eight retries of
/// temporary rejections. Unknown outcomes are not replayed by default.
#[derive(Debug, Clone)]
pub struct AppendBatchBuilder {
    client: Client,
    database: String,
    schema: String,
    table: String,
    options: AppendOptions,
    failure_policy: AppendFailurePolicy,
}

impl AppendBatchBuilder {
    pub(crate) fn new(client: Client, database: String, schema: String, table: String) -> Self {
        Self {
            client,
            database,
            schema,
            table,
            options: AppendOptions::default(),
            failure_policy: AppendFailurePolicy::Stop,
        }
    }

    /// Sets the target uncompressed NDJSON size (default 4 MiB, maximum 8 MiB).
    /// A single row may exceed this target up to the 8 MiB protocol limit.
    pub fn target_batch_bytes(mut self, bytes: usize) -> Self {
        self.options.batch_bytes = bytes;
        self
    }

    /// Sets the maximum rows per HTTP request, up to 200,000.
    pub fn max_batch_rows(mut self, rows: usize) -> Self {
        self.options.max_batch_rows = rows;
        self
    }

    /// Sets concurrency per write call (default four). Use one for ordered submission.
    pub fn max_concurrent_batches(mut self, batches: usize) -> Self {
        self.options.max_in_flight_requests = batches;
        self
    }

    /// Sets the number of additional attempts after an initial failure (default eight).
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.options.retry.max_retries = retries;
        self
    }

    /// Opts into replaying transient unknown outcomes, which can duplicate rows.
    /// Client timeouts are never replayed automatically, even when enabled.
    pub fn retry_unknown(mut self, enabled: bool) -> Self {
        self.options.retry.retry_unknown = enabled;
        self
    }

    /// Sets the initial exponential retry delay (default 100 ms).
    pub fn initial_backoff(mut self, delay: Duration) -> Self {
        self.options.retry.initial_backoff = delay;
        self
    }

    /// Caps retry delays, including Retry-After (default five seconds).
    pub fn max_backoff(mut self, delay: Duration) -> Self {
        self.options.retry.max_backoff = delay;
        self
    }

    /// Overrides the client's timeout per HTTP attempt. Must be finite and positive.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.options.attempt_timeout = Some(timeout);
        self
    }

    /// Chooses whether a remote failure stops scheduling later batches (default Stop).
    /// Continue still returns an error if any batch fails. Local encoding failures
    /// stop reading input under either policy.
    pub fn failure_policy(mut self, policy: AppendFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Validates configuration without starting background tasks or making requests.
    pub fn build(self) -> Result<AppendBatch, Error> {
        self.options.validate()?;
        self.client.make_resource_url(&[
            "catalog",
            "databases",
            &self.database,
            "schemas",
            &self.schema,
            "tables",
            &self.table,
            "ingest",
        ])?;
        let request = AppendBatchRequest {
            client: self.client,
            database: self.database,
            schema: self.schema,
            table: self.table,
            options: self.options,
        };
        Ok(AppendBatch {
            request,
            failure_policy: self.failure_policy,
        })
    }
}

/// A reusable writer whose calls batch and settle independently.
///
/// Clones share the HTTP connection pool, not buffers, concurrency limits or reports.
#[derive(Debug, Clone)]
pub struct AppendBatch {
    request: AppendBatchRequest,
    failure_policy: AppendFailurePolicy,
}

/// Complete settlement information for one finite input slice.
///
/// Results are ordered by input range, regardless of completion order. Their
/// ranges cover a contiguous input prefix; [`Self::unsubmitted`] covers the rest.
#[derive(Debug, Clone)]
#[must_use]
pub struct AppendBatchReport {
    input_rows: usize,
    batches: Vec<AppendBatchResult>,
    unsubmitted: Range<usize>,
}

impl AppendBatchReport {
    pub fn input_rows(&self) -> usize {
        self.input_rows
    }

    pub fn batches(&self) -> &[AppendBatchResult] {
        &self.batches
    }

    /// Returns the suffix that never entered single-batch execution.
    /// Its rows are known not to have been committed by this call.
    pub fn unsubmitted(&self) -> Range<usize> {
        self.unsubmitted.clone()
    }

    /// Counts logical input rows acknowledged at least once. Unknown retries
    /// may have inserted additional copies that this count cannot measure.
    pub fn committed_rows(&self) -> usize {
        self.batches
            .iter()
            .filter(|batch| batch.result.is_ok())
            .map(|batch| batch.input_range.len())
            .sum()
    }

    /// Summarizes committed, failed, unknown and unsubmitted input rows.
    pub fn outcome(&self) -> AppendDeliveryOutcome {
        if self.unsubmitted.is_empty() && self.batches.iter().all(|batch| batch.result.is_ok()) {
            AppendDeliveryOutcome::Ok
        } else if self.committed_rows() > 0 {
            AppendDeliveryOutcome::Partial
        } else if self.batches.iter().any(|batch| {
            batch.result.as_ref().is_err_and(|error| {
                error
                    .append_details()
                    .is_some_and(|details| details.status == AppendState::Unknown)
            })
        }) {
            AppendDeliveryOutcome::Unknown
        } else {
            AppendDeliveryOutcome::Failed
        }
    }
}

/// One logical HTTP batch, including all of its retries.
#[derive(Debug, Clone)]
pub struct AppendBatchResult {
    input_range: Range<usize>,
    result: Result<AppendRowsResult, Error>,
    retries: usize,
}

impl AppendBatchResult {
    pub fn input_range(&self) -> Range<usize> {
        self.input_range.clone()
    }

    /// Returns the final result without changing server-relative row-error indexes.
    pub fn result(&self) -> &Result<AppendRowsResult, Error> {
        &self.result
    }

    pub fn retries(&self) -> usize {
        self.retries
    }

    /// Maps a zero-based request row index to the original input slice.
    /// Out-of-range indexes, including malformed server indexes, return None.
    pub fn input_index(&self, row_index: u64) -> Option<usize> {
        let index = self
            .input_range
            .start
            .checked_add(usize::try_from(row_index).ok()?)?;
        self.input_range.contains(&index).then_some(index)
    }
}

/// A finite write that did not confirm every input row.
///
/// The report includes every scheduled batch, including successful batches.
/// Replaying the entire input can duplicate those rows and unknown outcomes.
#[derive(Debug, Clone)]
pub struct AppendBatchError {
    cause: Box<Error>,
    input_index: Option<usize>,
    report: AppendBatchReport,
}

impl AppendBatchError {
    /// Returns the local encoding error, or a failed batch's final error.
    pub fn cause(&self) -> &Error {
        &self.cause
    }

    /// Identifies a local serialization or row-validation failure, when present.
    /// That row belongs to the unsubmitted suffix. Remote row errors are mapped
    /// through [`AppendBatchResult::input_index`] instead.
    pub fn input_index(&self) -> Option<usize> {
        self.input_index
    }

    pub fn report(&self) -> &AppendBatchReport {
        &self.report
    }

    pub fn into_report(self) -> AppendBatchReport {
        self.report
    }
}

impl fmt::Display for AppendBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "finite append confirmed {} of {} input rows: {}",
            self.report.committed_rows(),
            self.report.input_rows,
            self.cause
        )
    }
}

impl StdError for AppendBatchError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.cause.as_ref())
    }
}

type PendingBatch = Pin<Box<dyn Future<Output = AppendBatchResult> + Send>>;

// Poll requests in the caller's task so cancellation drops the requests and
// their retry loops together, without detached workers or another executor.
async fn next_batch(pending: &mut Vec<PendingBatch>) -> AppendBatchResult {
    poll_fn(|cx| {
        for index in 0..pending.len() {
            if let Poll::Ready(result) = pending[index].as_mut().poll(cx) {
                drop(pending.swap_remove(index));
                return Poll::Ready(result);
            }
        }
        Poll::Pending
    })
    .await
}

impl AppendBatch {
    /// Writes a finite slice, returning Ok only when every row is acknowledged.
    ///
    /// Serializes incrementally, using the same row validation, batch limits and
    /// retries as AppendStream. A local encoding failure stops input processing
    /// and leaves any buffered rows in the unsubmitted suffix.
    ///
    /// Stop mode stops scheduling after observing a remote failure; already
    /// scheduled requests are always settled before return. Continue mode
    /// attempts later batches but still returns Err if any batch fails.
    /// Requests may finish out of order; use concurrency one for sequential submission.
    ///
    /// Dropping this future stops scheduling and retrying. Requests already sent
    /// may still commit; cancellation does not roll back writes or return a report.
    /// Memory for encoded rows is bounded by concurrency and batch sizes rather
    /// than the total input size; result metadata grows with the number of batches.
    ///
    /// ```no_run
    /// # async fn example(client: scopedb_client::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let writer = client.table("events").append_batch().build()?;
    /// let rows = [serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
    /// let report = writer.write(&rows).await?;
    /// assert_eq!(report.committed_rows(), rows.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write<T: Serialize>(
        &self,
        rows: &[T],
    ) -> Result<AppendBatchReport, AppendBatchError> {
        let mut report = AppendBatchReport {
            input_rows: rows.len(),
            batches: Vec::new(),
            unsubmitted: 0..rows.len(),
        };
        let mut pending: Vec<PendingBatch> = Vec::new();
        let mut cursor = 0;
        let mut submitted = 0;
        let mut lookahead = None;
        let mut local_failure = None;
        let mut stopped = false;

        loop {
            while !stopped
                && cursor < rows.len()
                && pending.len() < self.request.options.max_in_flight_requests
            {
                let start = cursor;
                let mut payload = String::new();
                let mut count = 0;
                while cursor < rows.len() {
                    let encoded = match lookahead
                        .take()
                        .map(Ok)
                        .unwrap_or_else(|| prepare_record(&rows[cursor]))
                    {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            local_failure = Some((cursor, error));
                            stopped = true;
                            break;
                        }
                    };
                    if self
                        .request
                        .options
                        .would_overflow(payload.len(), count, encoded.len())
                    {
                        lookahead = Some(encoded);
                        break;
                    }
                    if count > 0 {
                        payload.push('\n');
                    }
                    payload.push_str(&encoded);
                    count += 1;
                    cursor += 1;
                    if self.request.options.is_full(payload.len(), count) {
                        break;
                    }
                }
                if stopped {
                    break;
                }
                let input_range = start..cursor;
                let request = self.request.clone();
                pending.push(Box::pin(async move {
                    let (result, retries) = append_batch(request, payload).await;
                    AppendBatchResult {
                        input_range,
                        result,
                        retries,
                    }
                }));
                submitted = cursor;
            }
            if pending.is_empty() {
                break;
            }
            let result = next_batch(&mut pending).await;
            if result.result.is_err() && self.failure_policy == AppendFailurePolicy::Stop {
                stopped = true;
            }
            report.batches.push(result);
        }
        report.batches.sort_by_key(|batch| batch.input_range.start);
        report.unsubmitted = submitted..rows.len();
        if let Some((index, cause)) = local_failure {
            return Err(AppendBatchError {
                cause: Box::new(cause),
                input_index: Some(index),
                report,
            });
        }
        // Prefer an ambiguous failure as the headline error; every other failure
        // remains available in the report for selective reconciliation.
        let cause = report
            .batches
            .iter()
            .filter_map(|batch| batch.result.as_ref().err())
            .find(|error| {
                error
                    .append_details()
                    .is_some_and(|details| details.status == AppendState::Unknown)
            })
            .or_else(|| {
                report
                    .batches
                    .iter()
                    .find_map(|batch| batch.result.as_ref().err())
            })
            .cloned();
        match cause {
            Some(cause) => Err(AppendBatchError {
                cause: Box::new(cause),
                input_index: None,
                report,
            }),
            None => Ok(report),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use serde::Serializer;
    use serde_json::json;

    use super::*;
    use crate::test_support::MockResponse;
    use crate::test_support::MockServer;

    fn rejected() -> MockResponse {
        MockResponse::json(
            422,
            r#"{"message":"bad id","code":"invalid_schema","details":{"status":"rejected","row_errors":[{"row_index":1,"column":"id","message":"invalid integer"}],"row_errors_truncated":true}}"#,
        )
    }

    #[tokio::test]
    async fn continue_preserves_partial_success_and_indexes_after_out_of_order_completion() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let server = MockServer::start({
            let gate = gate.clone();
            move |_, request| {
                let first: serde_json::Value =
                    serde_json::from_str(request.body.lines().next().unwrap()).unwrap();
                match first["id"].as_u64().unwrap() {
                    0 => {
                        // The third batch can only start after the second one settles.
                        // Hold the first response until then to force completion out of order.
                        let (lock, wake) = &*gate;
                        let (open, _) = wake
                            .wait_timeout_while(
                                lock.lock().unwrap(),
                                Duration::from_secs(5),
                                |open| !*open,
                            )
                            .unwrap();
                        assert!(*open, "later batches did not make progress concurrently");
                    }
                    2 => return rejected(),
                    4 => {
                        let (lock, wake) = &*gate;
                        *lock.lock().unwrap() = true;
                        wake.notify_all();
                    }
                    _ => unreachable!(),
                }
                MockResponse::committed(request)
            }
        });
        let writer = server
            .client()
            .table("events")
            .append_batch()
            .max_batch_rows(2)
            .max_concurrent_batches(2)
            .failure_policy(AppendFailurePolicy::Continue)
            .build()
            .unwrap();
        let rows: Vec<_> = (0..6).map(|id| json!({"id": id})).collect();
        let error = writer.write(&rows).await.unwrap_err();
        let report = error.report();
        assert_eq!(report.input_rows(), 6);
        assert_eq!(report.committed_rows(), 4);
        assert_eq!(report.outcome(), AppendDeliveryOutcome::Partial);
        assert_eq!(report.unsubmitted(), 6..6);
        assert_eq!(
            report
                .batches()
                .iter()
                .map(AppendBatchResult::input_range)
                .collect::<Vec<_>>(),
            vec![0..2, 2..4, 4..6]
        );
        assert!(report.batches()[0].result().is_ok());
        assert!(report.batches()[2].result().is_ok());
        let failed = &report.batches()[1];
        let details = failed
            .result()
            .as_ref()
            .unwrap_err()
            .append_details()
            .unwrap();
        assert_eq!(details.status, AppendState::Rejected);
        assert!(details.row_errors_truncated);
        assert_eq!(details.row_errors[0].row_index, 1);
        assert_eq!(failed.input_index(details.row_errors[0].row_index), Some(3));
        assert_eq!(rows[failed.input_index(1).unwrap()]["id"], 3);
        assert_eq!(failed.input_index(2), None);
        assert_eq!(failed.input_index(u64::MAX), None);
        assert_eq!(server.requests().len(), 3);
        assert_eq!(server.max_active(), 2);
    }

    #[tokio::test]
    async fn stop_retains_committed_prefix_and_exact_unsubmitted_suffix() {
        let server = MockServer::start(|index, request| {
            if index == 1 {
                rejected()
            } else {
                MockResponse::committed(request)
            }
        });
        let writer = server
            .client()
            .table("events")
            .append_batch()
            .max_batch_rows(2)
            .max_concurrent_batches(1)
            .build()
            .unwrap();
        let rows: Vec<_> = (0..7).map(|id| json!({"id": id})).collect();
        let error = writer.write(&rows).await.unwrap_err();
        assert_eq!(error.input_index(), None);
        assert_eq!(error.cause().code(), Some("invalid_schema"));
        let report = error.into_report();
        assert_eq!(report.committed_rows(), 2);
        assert_eq!(report.outcome(), AppendDeliveryOutcome::Partial);
        assert_eq!(report.batches().len(), 2);
        assert_eq!(report.batches()[0].input_range(), 0..2);
        assert_eq!(report.batches()[1].input_range(), 2..4);
        assert_eq!(report.unsubmitted(), 4..7);
        assert_eq!(server.requests().len(), 2);
    }

    struct CountedRow {
        id: usize,
        fail: bool,
        calls: Arc<AtomicUsize>,
    }

    impl Serialize for CountedRow {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                return Err(serde::ser::Error::custom("cannot encode input row"));
            }
            json!({"id": self.id}).serialize(serializer)
        }
    }

    #[tokio::test]
    async fn local_encoding_failure_keeps_buffered_rows_in_unsubmitted_suffix() {
        for policy in [AppendFailurePolicy::Stop, AppendFailurePolicy::Continue] {
            let server = MockServer::start(|_, request| MockResponse::committed(request));
            let writer = server
                .client()
                .table("events")
                .append_batch()
                .max_batch_rows(2)
                .max_concurrent_batches(2)
                .failure_policy(policy)
                .build()
                .unwrap();
            let rows: Vec<_> = (0..5)
                .map(|id| CountedRow {
                    id,
                    fail: id == 3,
                    calls: Arc::new(AtomicUsize::new(0)),
                })
                .collect();
            let error = writer.write(&rows).await.unwrap_err();
            assert_eq!(error.input_index(), Some(3));
            assert!(error.cause().message().contains("serialize"));
            let report = error.report();
            assert_eq!(report.committed_rows(), 2);
            assert_eq!(report.batches().len(), 1);
            assert_eq!(report.batches()[0].input_range(), 0..2);
            assert_eq!(report.unsubmitted(), 2..5);
            assert_eq!(server.requests().len(), 1);
            assert_eq!(rows[4].calls.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn invalid_or_oversized_first_row_is_never_submitted() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let writer = server
            .client()
            .table("events")
            .append_batch()
            .build()
            .unwrap();
        for row in [
            json!([1, 2]),
            json!({"data": "x".repeat(crate::client::MAX_APPEND_BODY_BYTES)}),
        ] {
            let error = writer.write(&[row, json!({"id": 2})]).await.unwrap_err();
            assert_eq!(error.input_index(), Some(0));
            assert!(error.report().batches().is_empty());
            assert_eq!(error.report().unsubmitted(), 0..2);
            assert_eq!(error.report().outcome(), AppendDeliveryOutcome::Failed);
        }
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn byte_splitting_serializes_lookahead_once_and_matches_stream_boundaries() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let rows: Vec<_> = (0..3)
            .map(|id| CountedRow {
                id,
                fail: false,
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .collect();
        let table = server
            .client()
            .table("events/archive")
            .with_database("analytics 2026");
        // Two eight-byte rows need one more byte for their NDJSON separator.
        let writer = table
            .append_batch()
            .target_batch_bytes(16)
            .max_concurrent_batches(1)
            .build()
            .unwrap();
        let report = writer.write(&rows).await.unwrap();
        assert_eq!(
            report
                .batches()
                .iter()
                .map(AppendBatchResult::input_range)
                .collect::<Vec<_>>(),
            vec![0..1, 1..2, 2..3]
        );
        assert!(
            rows.iter()
                .all(|row| row.calls.load(Ordering::Relaxed) == 1)
        );
        let finite = server.requests();
        let stream = table
            .append_stream()
            .target_batch_bytes(16)
            .max_concurrent_batches(1)
            .build()
            .unwrap();
        stream
            .send_all((0..3).map(|id| json!({"id": id})))
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let requests = server.requests();
        for (batch, streamed) in finite.iter().zip(&requests[finite.len()..]) {
            assert_eq!(batch.body, streamed.body);
            assert_eq!(batch.target, streamed.target);
            assert_eq!(batch.headers["content-encoding"], "zstd");
        }
        assert_eq!(requests.len(), finite.len() * 2);
    }

    #[tokio::test]
    async fn shared_retries_keep_one_logical_batch_and_original_indexes() {
        let server = MockServer::start(|index, request| {
            if index == 0 {
                MockResponse::json(
                    503,
                    r#"{"message":"busy","retryable":true,"details":{"status":"rejected"}}"#,
                )
            } else {
                MockResponse::committed(request)
            }
        });
        let writer = server
            .client()
            .table("events")
            .append_batch()
            .max_retries(1)
            .initial_backoff(Duration::ZERO)
            .build()
            .unwrap();
        let report = writer
            .write(&[json!({"id": 0}), json!({"id": 1})])
            .await
            .unwrap();
        assert_eq!(report.batches().len(), 1);
        assert_eq!(report.batches()[0].retries(), 1);
        assert_eq!(report.batches()[0].input_range(), 0..2);
        assert_eq!(report.committed_rows(), 2);
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].body, requests[1].body);
    }

    #[tokio::test]
    async fn unknown_outcomes_are_preserved_across_replay_and_rejection() {
        for replay in [false, true] {
            let server = MockServer::start(|index, _| {
                if index == 0 {
                    MockResponse::json(200, "").without_ack()
                } else {
                    rejected()
                }
            });
            let writer = server
                .client()
                .table("events")
                .append_batch()
                .retry_unknown(replay)
                .max_retries(1)
                .initial_backoff(Duration::ZERO)
                .build()
                .unwrap();
            let error = writer
                .write(&[json!({"id": 0}), json!({"id": 1})])
                .await
                .unwrap_err();
            assert_eq!(error.report().outcome(), AppendDeliveryOutcome::Unknown);
            assert_eq!(error.report().unsubmitted(), 2..2);
            assert_eq!(error.report().batches()[0].input_range(), 0..2);
            assert_eq!(
                error.cause().append_details().unwrap().status,
                AppendState::Unknown
            );
            assert_eq!(error.report().batches()[0].retries(), usize::from(replay));
            assert_eq!(server.requests().len(), if replay { 2 } else { 1 });
        }
    }

    #[tokio::test]
    async fn reusable_writer_resets_indexes_and_empty_writes_make_no_requests() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let writer = server
            .client()
            .table("events")
            .append_batch()
            .build()
            .unwrap();
        let empty = writer.write::<serde_json::Value>(&[]).await.unwrap();
        assert_eq!(empty.outcome(), AppendDeliveryOutcome::Ok);
        assert_eq!(empty.input_rows(), 0);
        assert!(empty.batches().is_empty());
        assert_eq!(empty.unsubmitted(), 0..0);
        assert!(server.requests().is_empty());
        let rows = [json!({"id": 0}), json!({"id": 1})];
        let (first, second) = tokio::join!(writer.write(&rows), writer.write(&rows[1..]));
        assert_eq!(first.unwrap().batches()[0].input_range(), 0..2);
        assert_eq!(second.unwrap().batches()[0].input_range(), 0..1);
        assert_eq!(server.requests().len(), 2);
    }

    #[test]
    fn build_validates_shared_limits_without_a_runtime() {
        let table = Client::builder("http://127.0.0.1:1")
            .build()
            .unwrap()
            .table("events");
        assert!(table.append_batch().build().is_ok());
        for builder in [
            table.append_batch().target_batch_bytes(0),
            table
                .append_batch()
                .target_batch_bytes(crate::client::MAX_APPEND_BODY_BYTES + 1),
            table.append_batch().max_batch_rows(0),
            table
                .append_batch()
                .max_batch_rows(crate::client::MAX_APPEND_ROWS + 1),
            table.append_batch().max_concurrent_batches(0),
            table.append_batch().attempt_timeout(Duration::ZERO),
            table.append_batch().attempt_timeout(Duration::MAX),
            table.append_batch().initial_backoff(Duration::MAX),
            table.append_batch().max_backoff(Duration::MAX),
        ] {
            assert_eq!(
                builder.build().unwrap_err().kind(),
                crate::ErrorKind::ConfigInvalid
            );
        }
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::test_support::MockResponse;
    use crate::test_support::MockServer;
    use crate::test_support::ResponseGate;

    fn retryable_response() -> MockResponse {
        MockResponse::json(
            503,
            r#"{"message":"busy","retryable":true,"details":{"status":"rejected"}}"#,
        )
    }

    #[tokio::test]
    async fn stop_settles_concurrent_batches_including_their_retries() {
        for retry_in_flight in [false, true] {
            let gate = Arc::new(ResponseGate::default());
            let rejected = Arc::new(Notify::new());
            let attempts = Arc::new(AtomicUsize::new(0));
            let server = MockServer::start({
                let gate = gate.clone();
                let rejected = rejected.clone();
                let attempts = attempts.clone();
                move |_, request| {
                    let row: serde_json::Value = serde_json::from_str(&request.body).unwrap();
                    match row["id"].as_u64().unwrap() {
                        0 => MockResponse::json(
                            422,
                            r#"{"message":"bad row","details":{"status":"rejected"}}"#,
                        )
                        .notify_when_sent(rejected.clone()),
                        1 => {
                            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                                gate.block();
                                if retry_in_flight {
                                    return retryable_response();
                                }
                            }
                            MockResponse::committed(request)
                        }
                        _ => panic!("Stop submitted a row from the unsubmitted suffix"),
                    }
                }
            });
            let writer = server
                .client()
                .table("events")
                .append_batch()
                .max_batch_rows(1)
                .max_concurrent_batches(2)
                .max_retries(1)
                .initial_backoff(Duration::ZERO)
                .build()
                .unwrap();
            let rows = [json!({"id": 0}), json!({"id": 1}), json!({"id": 2})];
            let mut write = Box::pin(writer.write(&rows));
            tokio::select! {
                result = &mut write => panic!("write returned with an unsettled batch: {result:?}"),
                _ = async {
                    rejected.notified().await;
                    gate.wait_until_blocked().await;
                } => {}
            }
            // Keep the in-flight response gated while the rejection is processed.
            // Timing out this borrow leaves the write itself alive for settlement.
            assert!(
                tokio::time::timeout(Duration::from_millis(50), write.as_mut())
                    .await
                    .is_err()
            );
            gate.release();
            let error = write.await.unwrap_err();
            let report = error.report();
            assert_eq!(report.outcome(), AppendDeliveryOutcome::Partial);
            assert_eq!(report.committed_rows(), 1);
            assert_eq!(report.batches().len(), 2);
            assert_eq!(report.batches()[0].input_range(), 0..1);
            assert!(report.batches()[0].result().is_err());
            assert_eq!(report.batches()[1].input_range(), 1..2);
            assert!(report.batches()[1].result().is_ok());
            assert_eq!(report.batches()[1].retries(), usize::from(retry_in_flight));
            assert_eq!(report.unsubmitted(), 2..3);
            assert_eq!(server.requests().len(), 2 + usize::from(retry_in_flight));
        }
    }

    #[tokio::test]
    async fn cancelling_during_retry_backoff_stops_retries_and_later_batches() {
        let sent = Arc::new(Notify::new());
        let server = MockServer::start({
            let sent = sent.clone();
            move |_, request| {
                let row: serde_json::Value = serde_json::from_str(&request.body).unwrap();
                if row["id"] == 0 {
                    retryable_response().notify_when_sent(sent.clone())
                } else {
                    MockResponse::committed(request)
                }
            }
        });
        let writer = server
            .client()
            .table("events")
            .append_batch()
            .max_batch_rows(1)
            .max_concurrent_batches(1)
            .max_retries(3)
            .attempt_timeout(Duration::from_millis(500))
            .initial_backoff(Duration::from_secs(60))
            .max_backoff(Duration::from_secs(60))
            .build()
            .unwrap();
        let rows = [json!({"id": 0}), json!({"id": 1})];
        let mut write = Box::pin(writer.write(&rows));
        tokio::select! {
            result = &mut write => panic!("write returned before retry backoff: {result:?}"),
            _ = sent.notified() => {}
        }
        // The observation window exceeds the HTTP attempt budget but is shorter
        // than the backoff. A pending write here must have received the rejection
        // and entered backoff; an HTTP timeout would have completed with Err.
        assert!(
            tokio::time::timeout(Duration::from_secs(1), write.as_mut())
                .await
                .is_err()
        );
        assert_eq!(server.requests().len(), 1);
        drop(write);
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(240)).await;
        tokio::task::yield_now().await;
        tokio::time::resume();
        // Reusing the same writer also drives the transport after cancellation.
        let report = writer.write(&rows[1..]).await.unwrap();
        assert_eq!(report.batches()[0].input_range(), 0..1);
        assert_eq!(report.committed_rows(), 1);
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].body, r#"{"id":1}"#);
    }

    #[tokio::test]
    async fn cancelling_while_waiting_for_response_headers_or_body_stops_delivery() {
        for gate_body in [false, true] {
            let gate = Arc::new(ResponseGate::default());
            let sent = Arc::new(Notify::new());
            let server = MockServer::start({
                let gate = gate.clone();
                let sent = sent.clone();
                move |_, request| {
                    let row: serde_json::Value = serde_json::from_str(&request.body).unwrap();
                    if row["id"] != 0 {
                        return MockResponse::committed(request);
                    }
                    let response = retryable_response().notify_when_sent(sent.clone());
                    if gate_body {
                        response.with_body_gate(gate.clone())
                    } else {
                        gate.block();
                        response
                    }
                }
            });
            let writer = server
                .client()
                .table("events")
                .append_batch()
                .max_batch_rows(1)
                .max_concurrent_batches(1)
                .max_retries(3)
                .initial_backoff(Duration::from_secs(60))
                .max_backoff(Duration::from_secs(60))
                .build()
                .unwrap();
            let rows = [json!({"id": 0}), json!({"id": 1})];
            let mut write = Box::pin(writer.write(&rows));
            tokio::select! {
                result = &mut write => panic!("write returned before response was released: {result:?}"),
                _ = gate.wait_until_blocked() => {}
            }
            assert_eq!(server.requests().len(), 1);
            drop(write);
            gate.release();
            sent.notified().await;
            // Drive the released response before advancing time; otherwise a
            // leaked request could time out instead of reaching its retry delay.
            let report = writer.write(&rows[1..]).await.unwrap();
            assert_eq!(report.committed_rows(), 1);
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(240)).await;
            tokio::task::yield_now().await;
            tokio::time::resume();
            let report = writer.write(&[json!({"id": 2})]).await.unwrap();
            assert_eq!(report.committed_rows(), 1);
            let requests = server.requests();
            assert_eq!(requests.len(), 3);
            assert_eq!(requests[1].body, r#"{"id":1}"#);
            assert_eq!(requests[2].body, r#"{"id":2}"#);
        }
    }
}
