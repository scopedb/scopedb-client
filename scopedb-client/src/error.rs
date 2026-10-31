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
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use uuid::Uuid;

use crate::AppendAdmissionResult;
use crate::AppendDeliveryOutcome;
use crate::AppendDeliveryReport;
use crate::AppendErrorDetails;
use crate::AppendState;
use crate::AppendTrySendError;
use crate::AppendWriteReport;
use crate::CatalogMutationOutcome;
use crate::StatementErrorDetails;

/// Stable failure categories for application policy. Operation identity and
/// commit outcomes are available separately; do not parse diagnostic strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Change invalid configuration, input, or query arguments.
    InvalidInput,
    /// Supply valid authentication credentials.
    Unauthenticated,
    /// Obtain authorization for the operation.
    PermissionDenied,
    /// The selected resource or execution does not exist.
    NotFound,
    /// Refresh state or resolve a version or ownership conflict.
    Conflict,
    /// Reduce pressure and respect the retry delay when retrying is safe.
    RateLimited,
    /// The service or a required worker is unavailable.
    Unavailable,
    /// A client or server deadline expired; inspect write outcomes before replay.
    Timeout,
    /// Execution reached an explicitly cancelled terminal state.
    Cancelled,
    /// A response or result cannot be interpreted under the protocol contract.
    InvalidResponse,
    /// An aggregate operation left input unconfirmed; inspect its progress report.
    Incomplete,
    /// A producer can no longer accept work.
    Closed,
    /// An internal invariant or otherwise unclassified server failure occurred.
    Internal,
}

/// The SDK operation whose failure the caller is handling.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorOperation {
    /// Validate and construct the client and its HTTP configuration.
    BuildClient,
    /// Validate a finite writer and its selected table.
    BuildAppendWriter,
    /// Validate and start a background append stream.
    BuildAppendStream,
    /// Execute one logical append request, including its retry attempts.
    Append,
    /// Settle one finite input slice.
    WriteRows,
    /// Admit rows locally, including bulk and nonblocking admission.
    AdmitRows,
    /// Settle a delivery barrier while keeping the stream open.
    Flush,
    /// Close admission and settle the remaining stream rows.
    Shutdown,
    /// Submit one SQL execution.
    SubmitStatement,
    /// Statement::execute, Client::query, and table SQL operations.
    ExecuteStatement,
    /// Read one execution status snapshot.
    RefreshStatement,
    /// Wait for an existing execution to terminate.
    WaitStatement,
    /// Request cancellation of an existing execution.
    CancelStatement,
    /// Read the complete selected catalog definition.
    ReadCatalog,
    /// Replace or preview replacement of a selected catalog subtree.
    ApplyCatalog,
    /// Delete or preview deletion of a selected catalog resource.
    DeleteCatalog,
    /// Convert query result shapes or cells to Rust values.
    DecodeResult,
    /// Parse a public data-type identifier.
    ParseDataType,
}

impl ErrorKind {
    /// Returns the stable snake-case label used in logs and metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::Unauthenticated => "unauthenticated",
            Self::PermissionDenied => "permission_denied",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::RateLimited => "rate_limited",
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::InvalidResponse => "invalid_response",
            Self::Incomplete => "incomplete",
            Self::Closed => "closed",
            Self::Internal => "internal",
        }
    }

    pub(crate) fn from_http(status: StatusCode) -> Self {
        match status.as_u16() {
            400 | 405 | 413 | 422 => Self::InvalidInput,
            401 => Self::Unauthenticated,
            403 => Self::PermissionDenied,
            404 => Self::NotFound,
            409 | 412 => Self::Conflict,
            408 | 504 => Self::Timeout,
            429 => Self::RateLimited,
            500 => Self::Internal,
            502 | 503 => Self::Unavailable,
            _ if status.is_server_error() => Self::Unavailable,
            _ => Self::Internal,
        }
    }

    pub(crate) fn from_statement_code(code: &str) -> Self {
        match code {
            "bad_request" | "invalid_request" | "payload_too_large" => Self::InvalidInput,
            "not_found" => Self::NotFound,
            "unauthorized" => Self::Unauthenticated,
            "conflict" | "precondition_failed" => Self::Conflict,
            "too_many_requests" => Self::RateLimited,
            "service_unavailable" | "unavailable" => Self::Unavailable,
            "statement_timeout" | "request_timeout" | "timeout" => Self::Timeout,
            _ => Self::Internal,
        }
    }
}

impl ErrorOperation {
    /// Returns the stable snake-case label used in logs and metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BuildClient => "build_client",
            Self::BuildAppendWriter => "build_append_writer",
            Self::BuildAppendStream => "build_append_stream",
            Self::Append => "append",
            Self::WriteRows => "write_rows",
            Self::AdmitRows => "admit_rows",
            Self::Flush => "flush",
            Self::Shutdown => "shutdown",
            Self::SubmitStatement => "submit_statement",
            Self::ExecuteStatement => "execute_statement",
            Self::RefreshStatement => "refresh_statement",
            Self::WaitStatement => "wait_statement",
            Self::CancelStatement => "cancel_statement",
            Self::ReadCatalog => "read_catalog",
            Self::ApplyCatalog => "apply_catalog",
            Self::DeleteCatalog => "delete_catalog",
            Self::DecodeResult => "decode_result",
            Self::ParseDataType => "parse_data_type",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ErrorOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The client-side reason an in-flight write was interrupted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WriteErrorReason {
    /// A client or transport deadline expired.
    Timeout,
}

impl fmt::Display for WriteErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Timeout => "timeout",
        })
    }
}

/// Details of an interrupted write. An unknown outcome may have committed and
/// must not be automatically replayed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct WriteErrorDetails {
    pub reason: WriteErrorReason,
    pub outcome: AppendState,
}

#[derive(Debug, Default, Clone)]
struct RetryFacts {
    transient: bool,
    exhausted: bool,
    retries: usize,
}

#[derive(Debug, Default, Clone)]
struct ErrorDiagnostics {
    code: Option<String>,
    details: Option<serde_json::Value>,
    http_status: Option<StatusCode>,
    request_id: Option<String>,
    retry_after: Option<Duration>,
    server_retryable: Option<bool>,
    context: Vec<(&'static str, String)>,
}

#[derive(Debug, Clone)]
enum FailureDetails {
    Admission(Arc<AppendTrySendError>),
    Append {
        details: AppendErrorDetails,
        interruption: Option<WriteErrorDetails>,
    },
    Statement {
        id: Option<Uuid>,
        details: Option<StatementErrorDetails>,
        submitted: bool,
    },
    Catalog(CatalogMutationOutcome),
}

#[derive(Debug, Clone)]
enum AppendProgress {
    Admission(AppendAdmissionResult),
    Write {
        input_index: Option<usize>,
        report: AppendWriteReport,
    },
    Delivery(AppendDeliveryReport),
}

/// A failed SDK operation with structured recovery data and native diagnostics.
///
/// Partial progress stays on this error when it is returned through `?`.
/// Inspect [`Self::append_admission`], [`Self::append_write_report`], or
/// [`Self::append_delivery_report`] before replaying admitted or submitted rows.
/// Reports can include successful and unknown requests; a representative
/// failure does not describe every row.
///
/// Raw [`Self::details`] and HTTP metadata describe the last failed response.
/// [`Self::append_details`] retains uncertainty across retry attempts. Retry
/// classification is separate from whether a write may have committed.
#[doc = include_str!("../docs/errors.md")]
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    operation: ErrorOperation,
    retry: RetryFacts,
    diagnostics: Option<Box<ErrorDiagnostics>>,
    failure: Option<Box<FailureDetails>>,
    progress: Option<Box<AppendProgress>>,
    source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    /// Returns the failure classification.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the operation-level message, without repeating the source chain.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the original server code, including codes from newer servers.
    pub fn code(&self) -> Option<&str> {
        self.diagnostics
            .as_deref()
            .and_then(|d| d.code.as_deref())
            .or_else(|| self.statement_details().map(|d| d.code.as_str()))
    }

    /// Returns the original structured server details. After retries these
    /// describe the last failed response, not the outcome across attempts.
    pub fn details(&self) -> Option<&serde_json::Value> {
        self.diagnostics
            .as_deref()
            .and_then(|d| d.details.as_ref())
            .or_else(|| self.statement_details().and_then(|d| d.details.as_ref()))
    }

    pub fn http_status(&self) -> Option<StatusCode> {
        self.diagnostics.as_deref().and_then(|d| d.http_status)
    }

    pub fn request_id(&self) -> Option<&str> {
        self.diagnostics
            .as_deref()
            .and_then(|d| d.request_id.as_deref())
    }

    pub fn retry_after(&self) -> Option<Duration> {
        self.diagnostics.as_deref().and_then(|d| d.retry_after)
    }

    /// Returns table-append details across the failed request's attempts.
    /// Earlier unknown attempts keep status Unknown even if the last response
    /// rejected the request. Row diagnostics remain relative to that response.
    pub fn append_details(&self) -> Option<&AppendErrorDetails> {
        match self.failure.as_deref() {
            Some(FailureDetails::Append { details, .. }) => Some(details),
            _ => None,
        }
    }

    /// Returns client timeout details for an append after sending began.
    pub fn write_details(&self) -> Option<&WriteErrorDetails> {
        match self.failure.as_deref() {
            Some(FailureDetails::Append { interruption, .. }) => interruption.as_ref(),
            _ => None,
        }
    }

    /// Returns the outcome of a failed Catalog mutation. Unknown requires
    /// reconciliation even when an intermediary supplies a server error code.
    /// Reads, dry runs, and local validation failures return None.
    pub fn catalog_outcome(&self) -> Option<CatalogMutationOutcome> {
        match self.failure.as_deref() {
            Some(FailureDetails::Catalog(outcome)) => Some(*outcome),
            _ => None,
        }
    }

    /// Returns the execution ID, when known. Inspect that execution rather than
    /// submitting another statement; the ID does not confirm server acceptance.
    pub fn statement_id(&self) -> Option<Uuid> {
        match self.failure.as_deref() {
            Some(FailureDetails::Statement { id, .. }) => *id,
            _ => None,
        }
    }

    pub fn statement_details(&self) -> Option<&StatementErrorDetails> {
        match self.failure.as_deref() {
            Some(FailureDetails::Statement { details, .. }) => details.as_ref(),
            _ => None,
        }
    }

    /// Returns the original local try_send failure after conversion to Error.
    /// Its buffer and record-size details remain typed; serialization failures
    /// also retain their native source through the standard error chain.
    pub fn try_send_error(&self) -> Option<&AppendTrySendError> {
        match self.failure.as_deref() {
            Some(FailureDetails::Admission(error)) => Some(error.as_ref()),
            _ => None,
        }
    }

    /// Returns this failed send_all call's admitted prefix. Those rows remain
    /// owned by the stream; settle it before deciding which rows can be replayed.
    pub fn append_admission(&self) -> Option<&AppendAdmissionResult> {
        match self.progress.as_deref() {
            Some(AppendProgress::Admission(result)) => Some(result),
            _ => None,
        }
    }

    /// Returns the complete report of a failed finite write, including successful
    /// batches, request-relative index mappings, and the unsubmitted suffix.
    pub fn append_write_report(&self) -> Option<&AppendWriteReport> {
        match self.progress.as_deref() {
            Some(AppendProgress::Write { report, .. }) => Some(report),
            _ => None,
        }
    }

    /// Returns a local finite-write encoding failure's original input index.
    /// Remote row indexes are mapped through AppendRequestResult::input_index.
    pub fn append_input_index(&self) -> Option<usize> {
        match self.progress.as_deref() {
            Some(AppendProgress::Write { input_index, .. }) => *input_index,
            _ => None,
        }
    }

    /// Returns the terminal interval of a failed flush or shutdown. Concurrent
    /// terminal waiters share this settlement, including rows admitted after
    /// the failed barrier was enqueued. Unknown rows require reconciliation.
    pub fn append_delivery_report(&self) -> Option<&AppendDeliveryReport> {
        match self.progress.as_deref() {
            Some(AppendProgress::Delivery(report)) => Some(report),
            _ => None,
        }
    }

    /// Returns whether retrying this same public operation is safe and useful.
    /// Aggregate append failures require inspecting their progress, not replaying
    /// the whole input. Query execution errors after submission must be resumed
    /// through their statement ID. Unknown writes are never advertised as safe.
    pub fn is_retryable(&self) -> bool {
        if !self.retry.transient
            || self.retry.exhausted
            || self.progress.is_some()
            || self.kind == ErrorKind::Closed
            || self.catalog_outcome() == Some(CatalogMutationOutcome::Unknown)
            || self
                .append_details()
                .is_some_and(|details| details.status == AppendState::Unknown)
        {
            return false;
        }
        if matches!(
            self.operation,
            ErrorOperation::SubmitStatement | ErrorOperation::ExecuteStatement
        ) {
            let submitted = matches!(
                self.failure.as_deref(),
                Some(FailureDetails::Statement {
                    submitted: true,
                    ..
                })
            );
            return !submitted
                && self.http_status().is_some_and(|status| {
                    status.is_client_error() && status != StatusCode::REQUEST_TIMEOUT
                });
        }
        true
    }

    /// Returns the public operation, independently of its failure category.
    pub fn operation(&self) -> ErrorOperation {
        self.operation
    }

    /// Returns whether the retained failed append request exhausted its budget.
    /// This is independent of whether an attempt may have committed. Finite-write
    /// reports retain this fact separately on each failed batch.
    pub fn retry_exhausted(&self) -> bool {
        self.retry.exhausted
    }

    /// Returns additional attempts for the retained failed append request.
    /// Aggregate reports contain retry counts for the complete write or interval.
    pub fn retries(&self) -> usize {
        self.retry.retries
    }

    pub(crate) fn new(
        kind: ErrorKind,
        operation: ErrorOperation,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            operation,
            message: message.into(),
            retry: RetryFacts::default(),
            diagnostics: None,
            failure: None,
            progress: None,
            source: None,
        }
    }

    pub(crate) fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.diagnostics_mut()
            .context
            .push((key, value.to_string()));
        self
    }

    /// Sets the underlying cause.
    ///
    /// # Panics
    /// Panics if a source is already present.
    pub(crate) fn set_source(
        mut self,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        assert!(self.source.is_none(), "the source error has been set");
        self.source = Some(Arc::new(source));
        self
    }

    pub(crate) fn with_operation(mut self, operation: ErrorOperation) -> Self {
        self.operation = operation;
        self
    }

    pub(crate) fn is_transient(&self) -> bool {
        self.retry.transient
    }

    pub(crate) fn with_transient_failure(mut self, transient: bool) -> Self {
        self.retry.transient = transient;
        self
    }

    pub(crate) fn with_retry_history(mut self, retries: usize, exhausted: bool) -> Self {
        self.retry.exhausted = exhausted;
        self.retry.retries = retries;
        self
    }

    pub(crate) fn after_submission(mut self) -> Self {
        if let Some(FailureDetails::Statement { submitted, .. }) = self.failure.as_deref_mut() {
            *submitted = true;
        }
        self
    }

    pub(crate) fn for_closed_stream(mut self) -> Self {
        self.kind = ErrorKind::Closed;
        self.operation = ErrorOperation::AdmitRows;
        self
    }

    pub(crate) fn with_append_admission(mut self, accepted_rows: u64) -> Self {
        self.operation = ErrorOperation::AdmitRows;
        if accepted_rows > 0 {
            self.kind = ErrorKind::Incomplete;
        }
        self.progress = Some(Box::new(AppendProgress::Admission(AppendAdmissionResult {
            accepted_rows,
        })));
        self
    }

    pub(crate) fn with_append_write(
        mut self,
        report: AppendWriteReport,
        input_index: Option<usize>,
    ) -> Self {
        self.operation = ErrorOperation::WriteRows;
        if !report.batches().is_empty() {
            self.kind = ErrorKind::Incomplete;
        }
        self.progress = Some(Box::new(AppendProgress::Write {
            report,
            input_index,
        }));
        self
    }

    pub(crate) fn with_append_delivery(mut self, report: AppendDeliveryReport) -> Self {
        if report.outcome != AppendDeliveryOutcome::Ok {
            self.kind = ErrorKind::Incomplete;
        }
        self.progress = Some(Box::new(AppendProgress::Delivery(report)));
        self
    }

    pub(crate) fn set_append_details(mut self, details: AppendErrorDetails) -> Self {
        match self.failure.as_deref_mut() {
            Some(FailureDetails::Append {
                details: current, ..
            }) => *current = details,
            None => {
                self.failure = Some(Box::new(FailureDetails::Append {
                    details,
                    interruption: None,
                }))
            }
            _ => unreachable!("append details belong to an append failure"),
        }
        self
    }

    pub(crate) fn set_write_timeout(mut self) -> Self {
        self.kind = ErrorKind::Timeout;
        match self.failure.as_deref_mut() {
            Some(FailureDetails::Append { interruption, .. }) => {
                *interruption = Some(WriteErrorDetails {
                    reason: WriteErrorReason::Timeout,
                    outcome: AppendState::Unknown,
                })
            }
            _ => unreachable!("write interruption belongs to an append failure"),
        }
        self
    }

    pub(crate) fn set_catalog_outcome(mut self, outcome: CatalogMutationOutcome) -> Self {
        self.failure = Some(Box::new(FailureDetails::Catalog(outcome)));
        self
    }

    pub(crate) fn set_statement_id(mut self, id: Uuid) -> Self {
        match self.failure.as_deref_mut() {
            Some(FailureDetails::Statement { id: current, .. }) => *current = Some(id),
            None => {
                self.failure = Some(Box::new(FailureDetails::Statement {
                    id: Some(id),
                    details: None,
                    submitted: false,
                }))
            }
            _ => unreachable!("statement ID belongs to a statement failure"),
        }
        self
    }

    pub(crate) fn set_statement_details(mut self, details: StatementErrorDetails) -> Self {
        match self.failure.as_deref_mut() {
            Some(FailureDetails::Statement {
                details: current, ..
            }) => *current = Some(details),
            None => {
                self.failure = Some(Box::new(FailureDetails::Statement {
                    id: None,
                    details: Some(details),
                    submitted: true,
                }))
            }
            _ => unreachable!("statement details belong to a statement failure"),
        }
        self
    }

    pub(crate) fn set_http_status(mut self, status: StatusCode) -> Self {
        self.diagnostics_mut().http_status = Some(status);
        self
    }
    pub(crate) fn set_request_id(mut self, id: String) -> Self {
        self.diagnostics_mut().request_id = Some(id);
        self
    }
    pub(crate) fn set_retry_after(mut self, delay: Duration) -> Self {
        self.diagnostics_mut().retry_after = Some(delay);
        self
    }
    pub(crate) fn set_server_retryable(mut self, retryable: Option<bool>) -> Self {
        self.diagnostics_mut().server_retryable = retryable;
        self
    }
    pub(crate) fn server_retryable(&self) -> Option<bool> {
        self.diagnostics.as_deref().and_then(|d| d.server_retryable)
    }
    pub(crate) fn set_api_error(
        mut self,
        code: Option<String>,
        details: Option<serde_json::Value>,
    ) -> Self {
        if code.is_some() || details.is_some() {
            let diagnostics = self.diagnostics_mut();
            diagnostics.code = code;
            diagnostics.details = details;
        }
        self
    }
    fn diagnostics_mut(&mut self) -> &mut ErrorDiagnostics {
        self.diagnostics.get_or_insert_with(Default::default)
    }
}

impl From<AppendTrySendError> for Error {
    fn from(error: AppendTrySendError) -> Self {
        let (kind, transient) = match &error {
            AppendTrySendError::BufferFull => (ErrorKind::RateLimited, true),
            AppendTrySendError::CircuitOpen => (ErrorKind::Unavailable, true),
            AppendTrySendError::Closed => (ErrorKind::Closed, false),
            _ => (ErrorKind::InvalidInput, false),
        };
        let message = match &error {
            AppendTrySendError::Serialization(_) => "failed to serialize append row".to_string(),
            _ => error.to_string(),
        };
        let mut failure =
            Self::new(kind, ErrorOperation::AdmitRows, message).with_transient_failure(transient);
        failure.failure = Some(Box::new(FailureDetails::Admission(Arc::new(error))));
        failure
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} failed [{}]: {}",
            self.operation, self.kind, self.message
        )?;
        if let Some(diagnostics) = &self.diagnostics {
            for (key, value) in &diagnostics.context {
                write!(f, ", {key}: {value}")?;
            }
        }
        if let Some(code) = self.code() {
            write!(f, ", code: {code}")?;
        }
        if let Some(id) = self.statement_id() {
            write!(f, ", statement_id: {id}")?;
        }
        if let Some(status) = self.http_status() {
            write!(f, ", http_status: {}", status.as_u16())?;
        }
        if let Some(id) = self.request_id() {
            write!(f, ", request_id: {id}")?;
        }
        if let Some(delay) = self.retry_after() {
            write!(f, ", retry_after: {delay:?}")?;
        }
        if let Some(details) = self.append_details() {
            write!(f, ", append_outcome: {}", details.status)?;
        }
        if let Some(details) = self.write_details() {
            write!(
                f,
                ", write_reason: {}, write_outcome: {}",
                details.reason, details.outcome
            )?;
        }
        if let Some(outcome) = self.catalog_outcome() {
            write!(f, ", catalog_outcome: {outcome:?}")?;
        }
        if let Some(admission) = self.append_admission() {
            write!(f, ", accepted_rows: {}", admission.accepted_rows)?;
        }
        if let Some(report) = self.append_write_report() {
            write!(
                f,
                ", committed_rows: {}, input_rows: {}, outcome: {:?}, unsubmitted: {:?}",
                report.committed_rows(),
                report.input_rows(),
                report.outcome(),
                report.unsubmitted()
            )?;
        }
        if let Some(report) = self.append_delivery_report() {
            write!(
                f,
                ", committed_rows: {}, accepted_rows: {}, unknown_rows: {}, outcome: {:?}",
                report.committed_rows, report.accepted_rows, report.unknown_rows, report.outcome
            )?;
        }
        Ok(())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            return f
                .debug_struct("Error")
                .field("kind", &self.kind)
                .field("message", &self.message)
                .field("operation", &self.operation)
                .field("retry", &self.retry)
                .field("diagnostics", &self.diagnostics)
                .field("failure", &self.failure)
                .field("progress", &self.progress)
                .field("source", &self.source)
                .finish();
        }
        write!(f, "{self}")?;
        let mut cause = std::error::Error::source(self);
        while let Some(error) = cause {
            write!(f, "\n  caused by: {error}")?;
            cause = error.source();
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as _)
            .or_else(|| self.try_send_error().and_then(std::error::Error::source))
    }
}
