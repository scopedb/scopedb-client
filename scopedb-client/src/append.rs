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
use crate::AppendTrySendError;
use crate::Client;
use crate::Error;
use crate::ErrorKind;
use crate::client::MAX_APPEND_BODY_BYTES;
use crate::client::MAX_APPEND_ROWS;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryConfig {
    pub(crate) max_retries: usize,
    pub(crate) retry_unknown: bool,
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct AppendOptions {
    pub(crate) batch_bytes: usize,
    pub(crate) max_batch_rows: usize,
    pub(crate) max_in_flight_requests: usize,
    pub(crate) retry: RetryConfig,
    pub(crate) attempt_timeout: Option<Duration>,
}

impl Default for AppendOptions {
    fn default() -> Self {
        Self {
            batch_bytes: 4 * 1024 * 1024,
            max_batch_rows: MAX_APPEND_ROWS,
            max_in_flight_requests: 4,
            retry: RetryConfig {
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
    pub(crate) fn validate(&self) -> Result<(), Error> {
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

    pub(crate) fn would_overflow(
        &self,
        current_bytes: usize,
        rows: usize,
        next_bytes: usize,
    ) -> bool {
        rows > 0
            && (rows >= self.max_batch_rows
                || current_bytes.saturating_add(1).saturating_add(next_bytes) > self.batch_bytes)
    }

    pub(crate) fn is_full(&self, bytes: usize, rows: usize) -> bool {
        bytes >= self.batch_bytes || rows >= self.max_batch_rows
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AppendBatchRequest {
    pub(crate) client: Client,
    pub(crate) database: String,
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) options: AppendOptions,
}

pub(crate) fn validate_deadline(name: &str, duration: Duration) -> Result<(), Error> {
    if Instant::now().checked_add(duration).is_none() {
        Err(config_error(format!("{name} is too large")))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_positive(name: &str, value: usize) -> Result<(), Error> {
    if value == 0 {
        Err(config_error(format!("{name} must be greater than zero")))
    } else {
        Ok(())
    }
}

pub(crate) fn config_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::ConfigInvalid, message)
}

pub(crate) fn prepare_record<T>(row: &T) -> Result<String, Error>
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

pub(crate) fn prepare_record_for_try_send<T>(row: &T) -> Result<String, AppendTrySendError>
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_string(row).map_err(AppendTrySendError::Serialization)?;
    validate_record_payload(&payload)?;
    Ok(payload)
}

pub(crate) fn validate_record_payload(payload: &str) -> Result<(), AppendTrySendError> {
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

pub(crate) async fn append_batch(
    request: AppendBatchRequest,
    payload: String,
) -> (Result<AppendRowsResult, Error>, usize) {
    let mut retries = 0usize;
    let mut backoff = request
        .options
        .retry
        .initial_backoff
        .min(request.options.retry.max_backoff);
    let mut saw_unknown = false;
    loop {
        let result = request
            .client
            .append_rows_with_timeout(
                &request.database,
                &request.schema,
                &request.table,
                payload.clone(),
                request.options.attempt_timeout,
            )
            .await;

        let error = match result {
            Ok(result) => return (Ok(result), retries),
            Err(error) => error,
        };
        saw_unknown |= error
            .append_details()
            .is_some_and(|details| details.status == AppendState::Unknown);
        let retryable = append_retryable(&error, request.options.retry.retry_unknown);
        if !retryable || retries >= request.options.retry.max_retries {
            let error = if retryable {
                error.with_context("retries", retries).set_persistent()
            } else {
                error
            };
            return (Err(preserve_unknown_outcome(error, saw_unknown)), retries);
        }
        let retry_delay = retry_delay(backoff, &error, request.options.retry.max_backoff);
        if !retry_delay.is_zero() {
            tokio::time::sleep(retry_delay).await;
        }
        retries += 1;
        backoff = next_backoff(backoff, request.options.retry.max_backoff);
    }
}

pub(crate) fn append_retryable(error: &Error, retry_unknown: bool) -> bool {
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

pub(crate) fn preserve_unknown_outcome(error: Error, saw_unknown: bool) -> Error {
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

pub(crate) fn next_backoff(current: Duration, max_backoff: Duration) -> Duration {
    if current.is_zero() {
        return Duration::ZERO;
    }
    current
        .checked_mul(2)
        .unwrap_or(max_backoff)
        .min(max_backoff)
}

pub(crate) fn retry_delay(backoff: Duration, error: &Error, max_backoff: Duration) -> Duration {
    backoff
        .max(error.retry_after().unwrap_or(Duration::ZERO))
        .min(max_backoff)
}
