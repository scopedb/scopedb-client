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

/// Builder for an [`AppendWriter`].
///
/// Defaults match [`AppendStreamBuilder`](crate::AppendStreamBuilder): 4 MiB target batches, up to
/// 200,000 rows per batch, 4 concurrent batches, [`AppendFailurePolicy::Stop`],
/// and up to 8 retries of temporary [`AppendState::Rejected`] responses.
#[derive(Debug, Clone)]
pub struct AppendWriterBuilder {
    table: Table,
    options: AppendOptions,
    failure_policy: AppendFailurePolicy,
}

impl AppendWriterBuilder {
    /// Sets the target uncompressed NDJSON batch size (default 4 MiB, maximum 8 MiB).
    ///
    /// A single row larger than this target is sent in its own batch up to the
    /// 8 MiB protocol limit.
    pub fn target_batch_bytes(mut self, bytes: usize) -> Self {
        self.options.batch_bytes = bytes;
        self
    }

    /// Sets the maximum rows per HTTP batch (default and maximum 200,000).
    pub fn max_batch_rows(mut self, rows: usize) -> Self {
        self.options.max_batch_rows = rows;
        self
    }

    /// Sets the maximum in-flight batches per [`AppendWriter::write`] call (default 4). Use `1` for
    /// sequential submission.
    pub fn max_concurrent_batches(mut self, batches: usize) -> Self {
        self.options.max_in_flight_requests = batches;
        self
    }

    /// Sets the maximum retry attempts after an initial batch failure (default 8).
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.options.retry.max_retries = retries;
        self
    }

    /// Enables retrying transient [`AppendState::Unknown`] responses (default `false`).
    ///
    /// Retrying an unknown outcome can insert duplicate rows. Client timeouts
    /// are never replayed automatically.
    pub fn retry_unknown(mut self, enabled: bool) -> Self {
        self.options.retry.retry_unknown = enabled;
        self
    }

    /// Sets the initial retry backoff delay (default 100 ms).
    pub fn initial_backoff(mut self, delay: Duration) -> Self {
        self.options.retry.initial_backoff = delay;
        self
    }

    /// Sets the maximum retry delay, capping exponential backoff and `Retry-After` headers (default
    /// 5 s).
    pub fn max_backoff(mut self, delay: Duration) -> Self {
        self.options.retry.max_backoff = delay;
        self
    }

    /// Overrides the client's [`ClientBuilder::write_timeout`](crate::ClientBuilder::write_timeout)
    /// with a positive per-attempt timeout.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.options.attempt_timeout = Some(timeout);
        self
    }

    /// Sets whether a batch failure stops scheduling subsequent batches (default
    /// [`AppendFailurePolicy::Stop`]).
    pub fn failure_policy(mut self, policy: AppendFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Validates the configuration and returns a reusable [`AppendWriter`].
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

/// Reusable batch writer for finite slices of rows.
///
/// Each [`write`](Self::write) call batches and settles its input slice independently.
/// Clones share the underlying HTTP connection pool.
#[derive(Debug, Clone)]
pub struct AppendWriter {
    table: Table,
    options: AppendOptions,
    failure_policy: AppendFailurePolicy,
}

/// Settlement report for one [`AppendWriter::write`] call.
///
/// [`batches`](Self::batches) are ordered by their [`AppendRequestResult::input_range`]
/// and cover a contiguous prefix of the input slice; [`unsubmitted`](Self::unsubmitted)
/// covers the remaining suffix.
#[derive(Debug, Clone)]
#[must_use]
pub struct AppendWriteReport {
    input_rows: usize,
    batches: Vec<AppendRequestResult>,
    unsubmitted: Range<usize>,
}

impl AppendWriteReport {
    /// Returns the total number of rows in the input slice.
    pub fn input_rows(&self) -> usize {
        self.input_rows
    }

    /// Returns the scheduled batches in input order.
    pub fn batches(&self) -> &[AppendRequestResult] {
        &self.batches
    }

    /// Returns the input index range that was never sent and is known not to have committed.
    pub fn unsubmitted(&self) -> Range<usize> {
        self.unsubmitted.clone()
    }

    /// Returns the number of input rows in batches acknowledged as committed.
    pub fn committed_rows(&self) -> usize {
        self.batches
            .iter()
            .filter(|batch| batch.result.is_ok())
            .map(|batch| batch.input_range.len())
            .sum()
    }

    /// Returns the aggregate [`AppendDeliveryOutcome`] for this write call.
    ///
    /// [`AppendDeliveryOutcome::Partial`] may include batches with
    /// [`AppendState::Unknown`]; inspect [`batches`](Self::batches) before replaying
    /// failed ranges.
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

/// Outcome of one batch within an [`AppendWriteReport`], including any retry attempts.
#[derive(Debug, Clone)]
pub struct AppendRequestResult {
    input_range: Range<usize>,
    result: Result<AppendRowsResult, Error>,
    retries: usize,
}

impl AppendRequestResult {
    /// Returns the input slice range covered by this batch.
    pub fn input_range(&self) -> Range<usize> {
        self.input_range.clone()
    }

    /// Returns the final outcome of the batch.
    pub fn result(&self) -> &Result<AppendRowsResult, Error> {
        &self.result
    }

    /// Returns the number of retry attempts made for this batch.
    pub fn retries(&self) -> usize {
        self.retries
    }

    /// Maps a zero-based [`AppendRowError::row_index`](crate::AppendRowError::row_index) from this
    /// batch to its index in the original input slice. Returns `None` for indexes outside this
    /// batch.
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
    /// Batches and writes `rows`, returning `Ok` when every row is committed.
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
    ///
    /// If any row fails local encoding or any batch fails after retries, returns
    /// [`Error`] with [`Error::append_write_report`] recording every scheduled
    /// batch and the [`AppendWriteReport::unsubmitted`] suffix. Local encoding
    /// errors also set [`Error::append_input_index`], stop input processing under
    /// either failure policy, and leave the partially built batch unsubmitted.
    ///
    /// Under [`AppendFailurePolicy::Stop`], no new batches are scheduled after
    /// a batch failure is observed, while in-flight batches are settled before returning.
    /// Under [`AppendFailurePolicy::Continue`], remaining batches are still
    /// attempted before returning the error. Dropping this future stops
    /// scheduling and retrying, though requests already sent may still commit.
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
