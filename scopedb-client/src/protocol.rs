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
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;

use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::Error;
use crate::ErrorKind;
use crate::ErrorOperation;
use crate::ResultSet;

/// Outcome certainty of a table append request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppendOutcome {
    /// The entire request succeeded; all rows committed.
    #[serde(rename = "committed")]
    Succeeded,
    /// No rows from the attempt took effect, and none can take effect later.
    #[serde(rename = "rejected")]
    Failed,
    /// The attempt may have taken effect or may still take effect later.
    #[serde(rename = "unknown")]
    Ambiguous,
}

impl fmt::Display for AppendOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded => f.write_str("succeeded"),
            Self::Failed => f.write_str("failed"),
            Self::Ambiguous => f.write_str("ambiguous"),
        }
    }
}

/// Result of a committed table append request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRowsResult {
    /// Confirmed commit outcome: [`AppendOutcome::Succeeded`].
    #[serde(default = "succeeded_append_outcome")]
    pub status: AppendOutcome,
    pub num_rows_inserted: i64,
}

fn succeeded_append_outcome() -> AppendOutcome {
    AppendOutcome::Succeeded
}

/// Row-level validation diagnostic for a failed append request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRowError {
    pub row_index: u64,
    pub column: String,
    pub message: String,
}

/// SDK outcome certainty and available row diagnostics for a table append request.
///
/// On [`Error::append_details`], `status` is [`AppendOutcome::Ambiguous`] if any
/// attempt had an ambiguous outcome. A definite failure establishes that no rows
/// took effect or can take effect later. Row diagnostics, when available, describe
/// the last failed response and do not determine the outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendErrorDetails {
    pub status: AppendOutcome,
    /// Optional row diagnostics; an empty list does not imply that every row is valid.
    #[serde(default)]
    pub row_errors: Vec<AppendRowError>,
    #[serde(default)]
    pub row_errors_truncated: bool,
}

/// Decodes outcome certainty separately from the diagnostic error category.
pub fn decode_append_error(status: StatusCode, headers: &HeaderMap, payload: &[u8]) -> Error {
    #[derive(Deserialize)]
    struct ErrorBody {
        code: String,
        message: String,
        details: Option<serde_json::Value>,
        retryable: Option<bool>,
    }

    let decoded = serde_json::from_slice::<serde_json::Value>(payload).and_then(|envelope| {
        let modern = envelope.get("error").is_some();
        let body = envelope.get("error").unwrap_or(&envelope).clone();
        let request_id = envelope
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        serde_json::from_value::<ErrorBody>(body).map(|body| (body, modern, request_id))
    });
    let ambiguous = AppendErrorDetails {
        status: AppendOutcome::Ambiguous,
        row_errors: Vec::new(),
        row_errors_truncated: false,
    };
    let (body, modern, request_id) = match decoded {
        Ok(decoded) => decoded,
        Err(source) => {
            return apply_response_metadata(
                Error::new(
                    ErrorKind::InvalidResponse,
                    ErrorOperation::Append,
                    "failed to decode table append error response",
                )
                .set_source(source)
                .set_append_details(ambiguous),
                status,
                headers,
            );
        }
    };
    let valid = (status.is_client_error() || status.is_server_error())
        && (!modern || valid_http_error_code(&body.code, status));
    let mut details = if modern {
        ambiguous
    } else {
        body.details
            .as_ref()
            .and_then(|value| serde_json::from_value::<AppendErrorDetails>(value.clone()).ok())
            .unwrap_or(ambiguous)
    };
    // Explicit rejection details never override a 5xx ambiguous outcome.
    details.status =
        if valid && status.is_client_error() && (modern || details.status == AppendOutcome::Failed)
        {
            AppendOutcome::Failed
        } else {
            AppendOutcome::Ambiguous
        };
    let kind = if valid {
        ErrorKind::from_http(status)
    } else {
        ErrorKind::InvalidResponse
    };
    let mut error = apply_response_metadata(
        Error::new(kind, ErrorOperation::Append, body.message)
            .set_api_error(Some(body.code), body.details)
            .set_server_retryable(body.retryable)
            .with_transient_failure(
                valid
                    && body.retryable != Some(false)
                    && (status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()),
            )
            .set_append_details(details),
        status,
        headers,
    );
    if !valid {
        error = error.with_context("response", "inconsistent HTTP status and error code");
    }
    if let Some(request_id) = request_id {
        error = error.set_request_id(request_id);
    }
    error
}

fn valid_http_error_code(code: &str, status: StatusCode) -> bool {
    let expected = match code {
        "bad_request" => 400,
        "not_found" => 404,
        "method_not_allowed" => 405,
        "request_timeout" => 408,
        "conflict" => 409,
        "precondition_failed" => 412,
        "too_many_requests" => 429,
        "internal_server_error" => 500,
        "service_unavailable" => 503,
        "statement_timeout" | "invalid_request" | "unauthorized" | "payload_too_large"
        | "timeout" | "unavailable" | "internal_error" => return false,
        _ => return true,
    };
    status.as_u16() == expected
}

pub async fn decode_statement_response(
    r: reqwest::Response,
    operation: ErrorOperation,
) -> Result<StatementStatus, Error> {
    let code = r.status();
    let headers = r.headers().clone();
    let payload = r.bytes().await.map_err(|err| {
        let error = Error::new(
            if err.is_timeout() {
                ErrorKind::Timeout
            } else {
                ErrorKind::Unavailable
            },
            operation,
            "failed to read response body",
        )
        .set_source(err);
        let error = error.with_transient_failure(operation != ErrorOperation::SubmitStatement);
        apply_response_metadata(error, code, &headers)
    })?;
    if code.is_success() {
        let result = serde_json::from_slice(&payload).map_err(|err| {
            apply_response_metadata(
                Error::new(
                    ErrorKind::InvalidResponse,
                    operation,
                    "failed to decode response body",
                )
                .set_source(err),
                code,
                &headers,
            )
        })?;
        return Ok(result);
    }

    Err(decode_http_error(code, &headers, &payload, operation))
}

pub fn decode_http_error(
    status: StatusCode,
    headers: &HeaderMap,
    payload: &[u8],
    operation: ErrorOperation,
) -> Error {
    let parsed = parse_error_payload(payload).unwrap_or_else(|| ParsedErrorPayload {
        code: None,
        details: None,
        message: String::from_utf8_lossy(payload).into_owned(),
        request_id: None,
        retryable: None,
    });
    let mut error = apply_response_metadata(
        Error::new(ErrorKind::from_http(status), operation, parsed.message)
            .set_server_retryable(parsed.retryable)
            .set_api_error(parsed.code, parsed.details),
        status,
        headers,
    );
    if let Some(request_id) = parsed.request_id {
        error = error.set_request_id(request_id);
    }
    let retryable = parsed.retryable.unwrap_or_else(|| {
        matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        ) || status.is_server_error()
    });
    error.with_transient_failure(retryable)
}

#[derive(Debug)]
struct ParsedErrorPayload {
    code: Option<String>,
    details: Option<serde_json::Value>,
    message: String,
    request_id: Option<String>,
    retryable: Option<bool>,
}

fn parse_error_payload(payload: &[u8]) -> Option<ParsedErrorPayload> {
    let payload = serde_json::from_slice::<serde_json::Value>(payload).ok()?;
    let object = payload.as_object()?;
    if let Some(message) = object.get("message").and_then(serde_json::Value::as_str) {
        return Some(ParsedErrorPayload {
            code: object
                .get("code")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            details: object.get("details").cloned(),
            message: message.to_string(),
            request_id: object
                .get("request_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            retryable: object.get("retryable").and_then(serde_json::Value::as_bool),
        });
    }

    let nested = object.get("error")?.as_object()?;
    Some(ParsedErrorPayload {
        code: nested
            .get("code")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        details: nested.get("details").cloned(),
        message: nested.get("message")?.as_str()?.to_string(),
        request_id: object
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        retryable: nested.get("retryable").and_then(serde_json::Value::as_bool),
    })
}

pub fn apply_response_metadata(mut error: Error, status: StatusCode, headers: &HeaderMap) -> Error {
    error = error.set_http_status(status);
    if let Some(request_id) = header_string(headers, "x-request-id") {
        error = error.set_request_id(request_id);
    }
    if let Some(retry_after) = parse_retry_after(headers, SystemTime::now()) {
        error = error.set_retry_after(retry_after);
    }
    error
}

fn header_string(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn parse_retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

#[derive(Debug, Serialize)]
pub struct StatementRequest {
    pub statement: String,
    pub statement_id: Uuid,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_elapsed_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_estimated_seconds: Option<f64>,
    pub format: &'static str,
}

/// Execution status snapshot of a submitted statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum StatementStatus {
    #[serde(rename = "pending")]
    Pending(StatementStatusPending),
    #[serde(rename = "running")]
    Running(StatementStatusRunning),
    #[serde(rename = "finished")]
    Finished(StatementStatusFinished),
    #[serde(rename = "failed")]
    Failed(StatementStatusFailed),
    #[serde(rename = "cancelled")]
    Cancelled(StatementStatusCancelled),
}

/// Status of a statement queued for execution.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusPending {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
}

/// Status of an actively executing statement.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusRunning {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
}

/// Status of a statement that completed successfully.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusFinished {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,

    result_set: StatementResultSet,
}

impl StatementStatusFinished {
    /// Returns the decoded [`ResultSet`].
    pub fn result_set(&self) -> ResultSet {
        ResultSet::from_statement_result_set(self.result_set.clone())
    }
}

/// Status of a statement that terminated with an execution error.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusFailed {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
    pub error: StatementErrorDetails,
}

/// Structured server error for a failed statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatementErrorDetails {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Status of a cancelled statement.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusCancelled {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
}

impl StatementStatus {
    pub fn statement_id(&self) -> Uuid {
        match self {
            StatementStatus::Pending(s) => s.statement_id,
            StatementStatus::Running(s) => s.statement_id,
            StatementStatus::Finished(s) => s.statement_id,
            StatementStatus::Failed(s) => s.statement_id,
            StatementStatus::Cancelled(s) => s.statement_id,
        }
    }

    pub fn created_at(&self) -> jiff::Timestamp {
        match self {
            StatementStatus::Pending(s) => s.created_at,
            StatementStatus::Running(s) => s.created_at,
            StatementStatus::Finished(s) => s.created_at,
            StatementStatus::Failed(s) => s.created_at,
            StatementStatus::Cancelled(s) => s.created_at,
        }
    }

    pub fn progress(&self) -> &StatementEstimatedProgress {
        match self {
            StatementStatus::Pending(s) => &s.progress,
            StatementStatus::Running(s) => &s.progress,
            StatementStatus::Finished(s) => &s.progress,
            StatementStatus::Failed(s) => &s.progress,
            StatementStatus::Cancelled(s) => &s.progress,
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, StatementStatus::Finished(..))
    }

    pub fn is_terminated(&self) -> bool {
        matches!(
            self,
            StatementStatus::Finished(..)
                | StatementStatus::Failed(..)
                | StatementStatus::Cancelled(..)
        )
    }
}

/// Estimated progress and timing metrics for a statement.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct StatementEstimatedProgress {
    /// Total progress in percentage: `[0.0, 100.0]`.
    #[serde(deserialize_with = "deserialize_progress_float")]
    pub total_percentage: f64,
    /// Elapsed nanoseconds since submission.
    pub nanos_from_submitted: i64,
    /// Elapsed nanoseconds since execution started.
    pub nanos_from_started: i64,
    #[serde(flatten)]
    pub details: StatementProgress,
}

/// Stage, partition, row, byte, and compute counters for a statement.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatementProgress {
    /// Compute consumed by the statement, in seconds.
    #[serde(deserialize_with = "deserialize_progress_float")]
    pub compute_seconds: f64,
    pub total_stages: i64,
    pub total_partitions: i64,
    pub total_rows: i64,
    pub total_compressed_bytes: i64,
    pub total_uncompressed_bytes: i64,
    pub scanned_stages: i64,
    pub scanned_partitions: i64,
    pub scanned_rows: i64,
    pub scanned_compressed_bytes: i64,
    pub scanned_uncompressed_bytes: i64,
    pub skipped_partitions: i64,
    pub skipped_rows: i64,
    pub skipped_compressed_bytes: i64,
    pub skipped_uncompressed_bytes: i64,
}

// Internally tagged enums and flattened fields buffer numbers through Serde.
// Number understands serde_json's arbitrary-precision representation in that buffer.
fn deserialize_progress_float<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    serde_json::Number::deserialize(d)?
        .as_f64()
        .ok_or_else(|| serde::de::Error::custom("progress number is outside the f64 range"))
}

impl StatementProgress {
    pub fn total_percentage(&self) -> f64 {
        let scan_progress = if self.total_rows == 0 {
            0.0
        } else {
            (self.scanned_rows + self.skipped_rows) as f64 / self.total_rows as f64 * 100.0
        };

        let stage_progress = if self.total_stages == 0 {
            0.0
        } else {
            self.scanned_stages as f64 / self.total_stages as f64 * 100.0
        };

        scan_progress.max(stage_progress)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResultSet {
    pub metadata: ResultSetMetadata,
    #[serde(flatten)]
    pub data: ResultSetData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format")]
pub enum ResultSetData {
    #[serde(rename = "json")]
    Json { rows: Vec<Vec<Option<String>>> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSetMetadata {
    pub fields: Vec<FieldMetadata>,
    pub num_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub name: String,
    pub data_type: DataType,
}

/// ScopeDB column and result data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    #[serde(rename = "int")]
    Int,
    #[serde(rename = "uint")]
    UInt,
    #[serde(rename = "float")]
    Float,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "string")]
    String,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "timestamp")]
    Timestamp,
    #[serde(rename = "interval")]
    Interval,
    #[serde(rename = "array")]
    Array,
    #[serde(rename = "object")]
    Object,
    #[serde(rename = "any")]
    Any,
    #[serde(rename = "null")]
    Null,
}

impl FromStr for DataType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int" => Ok(Self::Int),
            "uint" => Ok(Self::UInt),
            "float" => Ok(Self::Float),
            "binary" => Ok(Self::Binary),
            "string" => Ok(Self::String),
            "boolean" => Ok(Self::Boolean),
            "timestamp" => Ok(Self::Timestamp),
            "interval" => Ok(Self::Interval),
            "array" => Ok(Self::Array),
            "object" => Ok(Self::Object),
            "any" => Ok(Self::Any),
            "null" => Ok(Self::Null),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::ParseDataType,
                format!("unrecognized data type: {s}"),
            )),
        }
    }
}
