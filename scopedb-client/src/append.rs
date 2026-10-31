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

use std::time::Duration;
use std::time::Instant;

use serde::Serialize;

use crate::AppendErrorDetails;
use crate::AppendRowsResult;
use crate::AppendState;
use crate::Error;
use crate::ErrorKind;
use crate::ErrorOperation;
use crate::Table;
use crate::client::MAX_APPEND_BODY_BYTES;
use crate::client::MAX_APPEND_ROWS;

/// Determines whether to stop scheduling rows after one HTTP batch fails.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendFailurePolicy {
    /// Stop scheduling new batches after observing a failure.
    #[default]
    Stop,
    /// Continue scheduling later batches, retaining every failure in the report.
    Continue,
}

/// The aggregate result of settling append operations.
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

#[derive(Debug, Clone, Copy)]
pub struct RetryOptions {
    pub max_retries: usize,
    pub retry_unknown: bool,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

#[derive(Debug, Clone)]
pub struct AppendOptions {
    pub batch_bytes: usize,
    pub max_batch_rows: usize,
    pub max_in_flight_requests: usize,
    pub retry: RetryOptions,
    pub attempt_timeout: Option<Duration>,
}

impl Default for AppendOptions {
    fn default() -> Self {
        Self {
            batch_bytes: 4 * 1024 * 1024,
            max_batch_rows: MAX_APPEND_ROWS,
            max_in_flight_requests: 4,
            retry: RetryOptions {
                max_retries: 8,
                retry_unknown: false,
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(5),
            },
            attempt_timeout: None,
        }
    }
}

impl AppendOptions {
    pub fn validate(&self) -> Result<(), Error> {
        validate_positive("target_batch_bytes", self.batch_bytes)?;
        if self.batch_bytes > MAX_APPEND_BODY_BYTES {
            return Err(config_error(format!(
                "target_batch_bytes must not exceed {MAX_APPEND_BODY_BYTES}"
            )));
        }
        validate_positive("max_batch_rows", self.max_batch_rows)?;
        if self.max_batch_rows > MAX_APPEND_ROWS {
            return Err(config_error(format!(
                "max_batch_rows must not exceed {MAX_APPEND_ROWS}"
            )));
        }
        validate_positive("max_concurrent_batches", self.max_in_flight_requests)?;
        validate_deadline("initial_backoff", self.retry.initial_backoff)?;
        validate_deadline("max_backoff", self.retry.max_backoff)?;
        if let Some(timeout) = self.attempt_timeout {
            if timeout.is_zero() {
                return Err(config_error("attempt_timeout must be greater than zero"));
            }
            validate_deadline("attempt_timeout", timeout)?;
        }
        Ok(())
    }

    pub fn would_overflow(&self, current_bytes: usize, rows: usize, next_bytes: usize) -> bool {
        rows > 0
            && (rows >= self.max_batch_rows
                || current_bytes.saturating_add(1).saturating_add(next_bytes) > self.batch_bytes)
    }

    pub fn is_full(&self, bytes: usize, rows: usize) -> bool {
        bytes >= self.batch_bytes || rows >= self.max_batch_rows
    }
}

pub fn validate_deadline(name: &str, duration: Duration) -> Result<(), Error> {
    if Instant::now().checked_add(duration).is_none() {
        Err(config_error(format!("{name} is too large")))
    } else {
        Ok(())
    }
}

pub fn validate_positive(name: &str, value: usize) -> Result<(), Error> {
    if value == 0 {
        Err(config_error(format!("{name} must be greater than zero")))
    } else {
        Ok(())
    }
}

pub fn config_error(message: impl Into<String>) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        ErrorOperation::BuildAppendWriter,
        message,
    )
}

#[derive(Debug)]
pub enum RecordError {
    Serialization(serde_json::Error),
    InvalidRecord,
    RecordTooLarge { bytes: usize, limit: usize },
}

impl From<RecordError> for Error {
    fn from(error: RecordError) -> Self {
        match error {
            RecordError::Serialization(error) => Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::AdmitRows,
                "failed to serialize append row",
            )
            .set_source(error),
            RecordError::InvalidRecord => Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::AdmitRows,
                "append row must serialize to a JSON object",
            ),
            RecordError::RecordTooLarge { bytes, limit } => Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::AdmitRows,
                format!("append row is {bytes} bytes, exceeding the {limit}-byte limit"),
            ),
        }
    }
}

pub fn encode_record<T>(row: &T) -> Result<String, RecordError>
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_string(row).map_err(RecordError::Serialization)?;
    validate_record_payload(&payload)?;
    Ok(payload)
}

fn validate_record_payload(payload: &str) -> Result<(), RecordError> {
    let bytes = payload.len();
    if bytes > MAX_APPEND_BODY_BYTES {
        return Err(RecordError::RecordTooLarge {
            bytes,
            limit: MAX_APPEND_BODY_BYTES,
        });
    }
    if !payload.starts_with('{') {
        return Err(RecordError::InvalidRecord);
    }
    Ok(())
}

pub async fn append_batch(
    table: Table,
    options: AppendOptions,
    payload: String,
) -> (Result<AppendRowsResult, Error>, usize) {
    let mut retries = 0usize;
    let mut backoff = options.retry.initial_backoff.min(options.retry.max_backoff);
    let mut saw_unknown = false;
    loop {
        let result = table
            .append_with_timeout(payload.clone(), options.attempt_timeout)
            .await;

        let error = match result {
            Ok(result) => return (Ok(result), retries),
            Err(error) => error,
        };
        saw_unknown |= error
            .append_details()
            .is_some_and(|details| details.status == AppendState::Unknown);
        let retryable = append_retryable(&error, options.retry.retry_unknown);
        if !retryable || retries >= options.retry.max_retries {
            let error = error.with_retry_history(retries, retryable);
            return (Err(preserve_unknown_outcome(error, saw_unknown)), retries);
        }
        let retry_delay = retry_delay(backoff, &error, options.retry.max_backoff);
        if !retry_delay.is_zero() {
            tokio::time::sleep(retry_delay).await;
        }
        retries += 1;
        backoff = next_backoff(backoff, options.retry.max_backoff);
    }
}

fn append_retryable(error: &Error, retry_unknown: bool) -> bool {
    if error.write_details().is_some() || error.server_retryable() == Some(false) {
        return false;
    }
    match error.append_details().map(|details| details.status) {
        Some(AppendState::Rejected) => error.is_retryable(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_object_rows() {
        assert!(matches!(
            encode_record(&vec![1, 2, 3]),
            Err(RecordError::InvalidRecord)
        ));
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
        let error = Error::new(ErrorKind::Unavailable, ErrorOperation::AdmitRows, "busy")
            .set_retry_after(Duration::from_secs(30));
        assert_eq!(
            retry_delay(Duration::from_millis(100), &error, Duration::from_secs(5)),
            Duration::from_secs(5)
        );

        let error = Error::new(ErrorKind::Unavailable, ErrorOperation::AdmitRows, "busy")
            .set_retry_after(Duration::from_millis(200));
        assert_eq!(
            retry_delay(Duration::from_secs(1), &error, Duration::from_secs(5)),
            Duration::from_secs(1)
        );
    }
}
