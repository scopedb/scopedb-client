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
use crate::AppendDeliveryReport;
use crate::AppendErrorDetails;
use crate::AppendState;
use crate::AppendWriteReport;
use crate::CatalogMutationOutcome;
use crate::StatementErrorDetails;

/// ErrorKind is all kinds of Error of ScopeDB client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The client don't know what happened here, and no actions other than just
    /// returning it back.
    Unexpected,

    /// The config for ScopeDB client is invalid.
    ConfigInvalid,

    /// A table append was rejected or its commit outcome is unknown.
    AppendRowsFailed,

    /// A statement reached a failed terminal state.
    StatementFailed,

    /// A statement reached the cancelled terminal state.
    StatementCancelled,

    /// A declarative catalog operation failed.
    CatalogFailed,
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

impl ErrorKind {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<ErrorKind> for &'static str {
    fn from(v: ErrorKind) -> &'static str {
        match v {
            ErrorKind::Unexpected => "Unexpected",
            ErrorKind::ConfigInvalid => "ConfigInvalid",
            ErrorKind::AppendRowsFailed => "AppendRowsFailed",
            ErrorKind::StatementFailed => "StatementFailed",
            ErrorKind::StatementCancelled => "StatementCancelled",
            ErrorKind::CatalogFailed => "CatalogFailed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorStatus {
    Permanent,
    Temporary,
    Persistent,
}

impl fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Permanent => "permanent",
            Self::Temporary => "temporary",
            Self::Persistent => "persistent",
        })
    }
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
    Append {
        details: AppendErrorDetails,
        interruption: Option<WriteErrorDetails>,
    },
    Statement {
        id: Option<Uuid>,
        details: Option<StatementErrorDetails>,
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
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    status: ErrorStatus,
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

    pub fn is_retryable(&self) -> bool {
        self.is_temporary()
    }
    pub fn is_temporary(&self) -> bool {
        self.status == ErrorStatus::Temporary
    }
    pub fn is_permanent(&self) -> bool {
        self.status == ErrorStatus::Permanent
    }
    pub fn is_persistent(&self) -> bool {
        self.status == ErrorStatus::Persistent
    }

    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            status: ErrorStatus::Permanent,
            diagnostics: None,
            failure: None,
            progress: None,
            source: None,
        }
    }

    pub fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.diagnostics_mut()
            .context
            .push((key, value.to_string()));
        self
    }

    /// Sets the underlying cause.
    ///
    /// # Panics
    /// Panics if a source is already present.
    pub fn set_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        assert!(self.source.is_none(), "the source error has been set");
        self.source = Some(Arc::new(source));
        self
    }

    pub fn set_permanent(mut self) -> Self {
        self.status = ErrorStatus::Permanent;
        self
    }
    pub fn set_temporary(mut self) -> Self {
        self.status = ErrorStatus::Temporary;
        self
    }
    pub fn set_persistent(mut self) -> Self {
        self.status = ErrorStatus::Persistent;
        self
    }

    pub(crate) fn with_append_admission(mut self, accepted_rows: u64) -> Self {
        self.progress = Some(Box::new(AppendProgress::Admission(AppendAdmissionResult {
            accepted_rows,
        })));
        self.set_persistent()
    }

    pub(crate) fn with_append_write(
        mut self,
        report: AppendWriteReport,
        input_index: Option<usize>,
    ) -> Self {
        self.progress = Some(Box::new(AppendProgress::Write {
            report,
            input_index,
        }));
        self.set_persistent()
    }

    pub(crate) fn with_append_delivery(mut self, report: AppendDeliveryReport) -> Self {
        self.progress = Some(Box::new(AppendProgress::Delivery(report)));
        self.set_persistent()
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}) => {}", self.kind, self.status, self.message)?;
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
                ", committed_rows: {}, input_rows: {}",
                report.committed_rows(),
                report.input_rows()
            )?;
        }
        if let Some(report) = self.append_delivery_report() {
            write!(
                f,
                ", committed_rows: {}, accepted_rows: {}",
                report.committed_rows, report.accepted_rows
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
                .field("status", &self.status)
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
        self.source.as_ref().map(|source| source.as_ref() as _)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_keep_the_cause_separate_from_the_operation_message() {
        let error = Error::new(ErrorKind::Unexpected, "failed to read response")
            .set_source(std::io::Error::other("connection interrupted"));
        assert!(!error.to_string().contains("connection interrupted"));
        let cause = std::error::Error::source(&error).unwrap();
        assert_eq!(cause.to_string(), "connection interrupted");
        assert!(cause.is::<std::io::Error>());
        assert!(format!("{error:?}").contains("connection interrupted"));
    }

    #[test]
    #[should_panic(expected = "the source error has been set")]
    fn replacing_source_is_rejected() {
        Error::new(ErrorKind::Unexpected, "operation failed")
            .set_source(std::io::Error::other("original cause"))
            .set_source(std::io::Error::other("replacement"));
    }
}
