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
use crate::Error;
use crate::ErrorOperation;
use crate::Table;
use crate::append::AppendOptions;
use crate::append::append_batch;
use crate::append::encode_record;

/// Configures a reusable writer for finite slices of rows.
///
/// Defaults match [`crate::AppendStreamBuilder`]: 4 MiB target batches, at most
/// 200,000 rows per request, four concurrent batches and eight retries of
/// temporary rejections. Unknown outcomes are not replayed by default.
#[derive(Debug, Clone)]
pub struct AppendWriterBuilder {
    table: Table,
    options: AppendOptions,
    failure_policy: AppendFailurePolicy,
}

impl AppendWriterBuilder {
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
    pub fn build(self) -> Result<AppendWriter, Error> {
        self.options
            .validate()
            .map_err(|error| error.with_operation(ErrorOperation::BuildAppendWriter))?;
        self.table
            .validate_append()
            .map_err(|error| error.with_operation(ErrorOperation::BuildAppendWriter))?;
        Ok(AppendWriter {
            table: self.table,
            options: self.options,
            failure_policy: self.failure_policy,
        })
    }

    pub(crate) fn new(table: Table) -> Self {
        Self {
            table,
            options: AppendOptions::default(),
            failure_policy: AppendFailurePolicy::Stop,
        }
    }
}

/// A reusable writer whose calls batch and settle independently.
///
/// Clones share the HTTP connection pool, not buffers, concurrency limits or reports.
#[derive(Debug, Clone)]
pub struct AppendWriter {
    table: Table,
    options: AppendOptions,
    failure_policy: AppendFailurePolicy,
}

/// Complete settlement information for one finite input slice.
///
/// Results are ordered by input range, regardless of completion order. Their
/// ranges cover a contiguous input prefix; [`Self::unsubmitted`] covers the rest.
#[derive(Debug, Clone)]
#[must_use]
pub struct AppendWriteReport {
    input_rows: usize,
    batches: Vec<AppendRequestResult>,
    unsubmitted: Range<usize>,
}

impl AppendWriteReport {
    pub fn input_rows(&self) -> usize {
        self.input_rows
    }

    pub fn batches(&self) -> &[AppendRequestResult] {
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
    /// Partial can include unknown outcomes. Inspect each batch before deciding
    /// which input can be replayed; the summary alone cannot establish that.
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
pub struct AppendRequestResult {
    input_range: Range<usize>,
    result: Result<AppendRowsResult, Error>,
    retries: usize,
}

impl AppendRequestResult {
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

type PendingBatch = Pin<Box<dyn Future<Output = AppendRequestResult> + Send>>;

// Poll requests in the caller's task so cancellation drops the requests and
// their retry loops together, without detached workers or another executor.
async fn next_batch(pending: &mut Vec<PendingBatch>) -> AppendRequestResult {
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

impl AppendWriter {
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
    /// # async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
    /// let writer = client.table("events").append_writer().build()?;
    /// let rows = [serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
    /// let report = writer.write(&rows).await?;
    /// assert_eq!(report.committed_rows(), rows.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write<T: Serialize>(&self, rows: &[T]) -> Result<AppendWriteReport, Error> {
        let mut report = AppendWriteReport {
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
                && pending.len() < self.options.max_in_flight_requests
            {
                let start = cursor;
                let mut payload = String::new();
                let mut count = 0;
                while cursor < rows.len() {
                    let encoded = match lookahead
                        .take()
                        .map(Ok)
                        .unwrap_or_else(|| encode_record(&rows[cursor]))
                    {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            local_failure = Some((cursor, Error::from(error)));
                            stopped = true;
                            break;
                        }
                    };
                    if self
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
                    if self.options.is_full(payload.len(), count) {
                        break;
                    }
                }
                if stopped {
                    break;
                }
                let input_range = start..cursor;
                let table = self.table.clone();
                let options = self.options.clone();
                pending.push(Box::pin(async move {
                    let (result, retries) = append_batch(table, options, payload).await;
                    AppendRequestResult {
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
            return Err(cause.with_append_write(report, Some(index)));
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
            Some(cause) => Err(cause.with_append_write(report, None)),
            None => Ok(report),
        }
    }
}
