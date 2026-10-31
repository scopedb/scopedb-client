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

use crate::AppendErrorDetails;
use crate::AppendState;
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
    /// Permanent means without external changes, the error never changes.
    ///
    /// For example, underlying services returns a not found error.
    ///
    /// Users SHOULD never retry this operation.
    Permanent,
    /// Temporary means this error is returned for temporary.
    ///
    /// For example, underlying services is rate limited or unavailable for temporary.
    ///
    /// Users CAN retry the operation to resolve it.
    Temporary,
    /// Persistent means the error is not classified as retryable.
    ///
    /// For example, a retry budget was exhausted or a write has an unknown outcome.
    ///
    /// Append streams may explicitly replay transient unknown outcomes under
    /// their delivery policy. Client timeouts are never automatically replayed.
    Persistent,
}

#[derive(Debug, Default, Clone)]
struct HttpErrorMetadata {
    status: Option<StatusCode>,
    request_id: Option<String>,
    retry_after: Option<Duration>,
    retryable: Option<bool>,
}

#[derive(Debug, Clone)]
struct ApiErrorDetails {
    code: Option<String>,
    details: Option<serde_json::Value>,
}

#[derive(Debug, Default, Clone)]
struct StatementErrorMetadata {
    id: Option<Uuid>,
    details: Option<StatementErrorDetails>,
}

impl fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorStatus::Permanent => write!(f, "permanent"),
            ErrorStatus::Temporary => write!(f, "temporary"),
            ErrorStatus::Persistent => write!(f, "persistent"),
        }
    }
}

/// A failure with a message, optional HTTP diagnostics, and operation details.
///
/// Start with [`Self::message`], [`Self::http_status`], and [`Self::request_id`].
/// Use [`Self::statement_details`] for a failed query, [`Self::append_details`]
/// for a rejected or uncertain append, and [`Self::write_details`] for a write
/// timeout. A timed-out write may already have committed; reconcile it before
/// replaying.
///
/// HTTP diagnostics and raw [`Self::details`] describe the response retained
/// from the last failed attempt. [`Self::append_details`] describes what is
/// known about delivery across all attempts: a later rejection cannot rule out
/// an earlier commit. Retry classification is separate from delivery outcome.
///
/// [`Self::is_retryable`] describes the failed operation. For a query that was
/// already submitted, resume its [`Self::statement_id`] through
/// [`crate::Client::statement_handle`] instead of submitting another query.
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    message: String,

    status: ErrorStatus,
    context: Vec<(&'static str, String)>,
    api_error: Option<Box<ApiErrorDetails>>,
    append_details: Option<AppendErrorDetails>,
    write_details: Option<WriteErrorDetails>,
    statement_metadata: Option<Box<StatementErrorMetadata>>,
    http_metadata: Option<Box<HttpErrorMetadata>>,

    source: Option<Arc<anyhow::Error>>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.kind, self.status)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                self.context
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
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
        if let Some(request_id) = self.request_id() {
            write!(f, ", request_id: {request_id}")?;
        }
        if let Some(retry_after) = self.retry_after() {
            write!(f, ", retry_after: {retry_after:?}")?;
        }

        if let Some(details) = self.write_details() {
            write!(
                f,
                ", write_reason: {}, write_outcome: {}",
                details.reason, details.outcome
            )?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If alternate has been specified, we will print like Debug.
        if f.alternate() {
            let mut de = f.debug_struct("Error");
            de.field("kind", &self.kind);
            de.field("message", &self.message);
            de.field("api_error", &self.api_error);
            de.field("status", &self.status);
            de.field("context", &self.context);
            de.field("append_details", &self.append_details);
            de.field("write_details", &self.write_details);
            de.field("statement_id", &self.statement_id());
            de.field("statement_details", &self.statement_details());
            de.field("http_metadata", &self.http_metadata);
            de.field("source", &self.source);
            return de.finish();
        }

        write!(f, "{} ({})", self.kind, self.status)?;
        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
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
        if let Some(request_id) = self.request_id() {
            write!(f, ", request_id: {request_id}")?;
        }
        if let Some(retry_after) = self.retry_after() {
            write!(f, ", retry_after: {retry_after:?}")?;
        }
        if let Some(details) = self.write_details() {
            write!(
                f,
                ", write_reason: {}, write_outcome: {}",
                details.reason, details.outcome
            )?;
        }
        writeln!(f)?;

        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "   {k}: {v}")?;
            }
        }
        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source:")?;
            writeln!(f, "   {source:#}")?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref().as_ref())
    }
}

impl Error {
    /// Create a new Error with error kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),

            status: ErrorStatus::Permanent,
            context: Vec::default(),
            api_error: None,
            append_details: None,
            write_details: None,
            statement_metadata: None,
            http_metadata: None,
            source: None,
        }
    }

    /// Add more context in error.
    pub fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    /// Set source for error.
    ///
    /// # Notes
    ///
    /// If the source has been set, we will raise a panic here.
    pub fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");

        self.source = Some(Arc::new(src.into()));
        self
    }

    /// Returns client timeout details for a table append after sending began.
    /// Existing error kinds, sources, and HTTP diagnostics remain available.
    pub fn write_details(&self) -> Option<&WriteErrorDetails> {
        self.write_details.as_ref()
    }

    /// Returns table-append failure details across the operation's attempts.
    /// An earlier unknown attempt keeps the status Unknown even if the last
    /// response rejected the request. Row errors still describe that last response;
    /// use [`Self::details`] for its original, unmodified server payload.
    pub fn append_details(&self) -> Option<&AppendErrorDetails> {
        self.append_details.as_ref()
    }

    /// Return structured statement failure details, when available.
    pub fn statement_details(&self) -> Option<&StatementErrorDetails> {
        self.statement_metadata
            .as_ref()
            .and_then(|m| m.details.as_ref())
    }

    /// Returns the statement identifier, when known. Use [`crate::Client::statement_handle`]
    /// to inspect the existing execution instead of resubmitting it. An identifier
    /// supplied before submission does not confirm that the server accepted it.
    pub fn statement_id(&self) -> Option<Uuid> {
        self.statement_metadata.as_ref().and_then(|m| m.id)
    }

    /// Set permanent status for error.
    pub fn set_permanent(mut self) -> Self {
        self.status = ErrorStatus::Permanent;
        self
    }

    /// Set temporary status for error.
    ///
    /// By set temporary, we indicate this error is retryable.
    pub fn set_temporary(mut self) -> Self {
        self.status = ErrorStatus::Temporary;
        self
    }

    /// Set persistent status for error.
    ///
    /// By setting persistent, we indicate the retry should be stopped.
    pub fn set_persistent(mut self) -> Self {
        self.status = ErrorStatus::Persistent;
        self
    }

    /// Return the kind of this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the retained server error code, including codes introduced by newer
    /// servers. After retries this describes the last failed attempt, not the
    /// delivery outcome across attempts.
    pub fn code(&self) -> Option<&str> {
        self.api_error
            .as_deref()
            .and_then(|e| e.code.as_deref())
            .or_else(|| self.statement_details().map(|e| e.code.as_str()))
    }

    /// Returns the original structured server details, when provided.
    /// After retries these describe the last failed response. They are not
    /// rewritten when an earlier unknown attempt makes the overall append
    /// outcome uncertain; use [`Self::append_details`] for that conclusion.
    pub fn details(&self) -> Option<&serde_json::Value> {
        self.api_error
            .as_deref()
            .and_then(|e| e.details.as_ref())
            .or_else(|| self.statement_details().and_then(|e| e.details.as_ref()))
    }

    /// Return the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Return the HTTP response status when the error came from a server.
    pub fn http_status(&self) -> Option<StatusCode> {
        self.http_metadata
            .as_ref()
            .and_then(|metadata| metadata.status)
    }

    /// Return the request identifier supplied by ScopeDB or an intermediary.
    pub fn request_id(&self) -> Option<&str> {
        self.http_metadata
            .as_ref()
            .and_then(|metadata| metadata.request_id.as_deref())
    }

    /// Return the delay suggested by the HTTP `Retry-After` header.
    pub fn retry_after(&self) -> Option<Duration> {
        self.http_metadata
            .as_ref()
            .and_then(|metadata| metadata.retry_after)
    }

    /// Returns whether retrying this operation is currently appropriate.
    /// This is a policy decision, not a commit acknowledgement: false can mean
    /// a permanent rejection, an exhausted retry budget, or an unknown outcome.
    /// For statement handles, retry on the same handle. Errors from `query()` or
    /// `execute()` after submission never indicate that resubmitting is safe.
    pub fn is_retryable(&self) -> bool {
        self.is_temporary()
    }

    /// Check if this error is temporary.
    pub fn is_temporary(&self) -> bool {
        self.status == ErrorStatus::Temporary
    }

    /// Check if this error is permanent.
    pub fn is_permanent(&self) -> bool {
        self.status == ErrorStatus::Permanent
    }

    /// Check if this error is persistent.
    pub fn is_persistent(&self) -> bool {
        self.status == ErrorStatus::Persistent
    }

    pub(crate) fn set_write_timeout(mut self) -> Self {
        self.write_details = Some(WriteErrorDetails {
            reason: WriteErrorReason::Timeout,
            outcome: AppendState::Unknown,
        });
        self
    }

    pub(crate) fn set_append_details(mut self, details: AppendErrorDetails) -> Self {
        self.append_details = Some(details);
        self
    }

    pub(crate) fn set_statement_details(mut self, details: StatementErrorDetails) -> Self {
        self.statement_metadata
            .get_or_insert_with(Default::default)
            .details = Some(details);
        self
    }

    pub(crate) fn set_statement_id(mut self, id: Uuid) -> Self {
        self.statement_metadata
            .get_or_insert_with(Default::default)
            .id = Some(id);
        self
    }

    pub(crate) fn set_http_status(mut self, status: StatusCode) -> Self {
        self.http_metadata_mut().status = Some(status);
        self
    }

    pub(crate) fn set_request_id(mut self, request_id: String) -> Self {
        self.http_metadata_mut().request_id = Some(request_id);
        self
    }

    pub(crate) fn set_retry_after(mut self, retry_after: Duration) -> Self {
        self.http_metadata_mut().retry_after = Some(retry_after);
        self
    }

    pub(crate) fn set_server_retryable(mut self, retryable: Option<bool>) -> Self {
        self.http_metadata_mut().retryable = retryable;
        self
    }

    pub(crate) fn server_retryable(&self) -> Option<bool> {
        self.http_metadata
            .as_ref()
            .and_then(|metadata| metadata.retryable)
    }

    pub(crate) fn set_api_error(
        mut self,
        code: Option<String>,
        details: Option<serde_json::Value>,
    ) -> Self {
        if code.is_some() || details.is_some() {
            self.api_error = Some(Box::new(ApiErrorDetails { code, details }));
        }
        self
    }

    fn http_metadata_mut(&mut self) -> &mut HttpErrorMetadata {
        self.http_metadata
            .get_or_insert_with(|| Box::new(HttpErrorMetadata::default()))
    }
}
