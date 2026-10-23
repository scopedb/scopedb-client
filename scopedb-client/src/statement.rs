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

use tokio::time::sleep;
use uuid::Uuid;

use crate::Error;
use crate::ErrorKind;
use crate::client::Client;
use crate::protocol::StatementRequest;
use crate::protocol::StatementStatus;
use crate::result::ResultSet;

/// A query with optional execution limits. Use [`Self::execute`] to wait for its
/// result, or [`Self::submit`] to retain a handle for progress and cancellation.
#[derive(Debug)]
pub struct Statement {
    client: Client,
    statement: String,
    statement_id: Option<Uuid>,
    max_elapsed: Option<Duration>,
    max_estimated: Option<Duration>,
}

impl Statement {
    pub fn with_statement_id(mut self, statement_id: Uuid) -> Self {
        self.statement_id = Some(statement_id);
        self
    }

    /// Limits total elapsed time on the server, including queueing.
    pub fn with_max_elapsed(mut self, duration: Duration) -> Self {
        self.max_elapsed = Some(duration);
        self
    }

    /// Rejects execution when its estimated duration exceeds this limit.
    pub fn with_max_estimated(mut self, duration: Duration) -> Self {
        self.max_estimated = Some(duration);
        self
    }

    /// Submits one statement. An interrupted response does not establish whether
    /// execution started. Supply an ID with [`Self::with_statement_id`] to look it
    /// up after such a failure without submitting another execution.
    pub async fn submit(self) -> Result<StatementHandle, Error> {
        if self.max_elapsed.is_some_and(|d| d.is_zero())
            || self.max_estimated.is_some_and(|d| d.is_zero())
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "statement duration limits must be positive",
            ));
        }
        let Statement {
            client,
            statement,
            statement_id,
            max_elapsed,
            max_estimated,
        } = self;

        let snapshot = client
            .submit_statement(StatementRequest {
                statement,
                statement_id,
                max_elapsed_seconds: max_elapsed.map(|v| v.as_secs_f64()),
                max_estimated_seconds: max_estimated.map(|v| v.as_secs_f64()),
                format: "json",
            })
            .await
            .map_err(|error| match statement_id {
                Some(id) => error.set_statement_id(id),
                None => error,
            })?;

        Ok(StatementHandle {
            client,
            statement_id: snapshot.statement_id(),
            snapshot: Some(snapshot),
        })
    }

    /// Submits and waits. Dropping this future stops polling, not server execution.
    /// Retain a submitted handle to request cancellation explicitly.
    /// Errors after submission retain [`Error::statement_id`] and never indicate
    /// that resubmitting is safe. Resume with [`Client::statement_handle`] instead.
    pub async fn execute(self) -> Result<ResultSet, Error> {
        self.submit().await?.wait().await.map_err(|error| {
            if error.is_retryable() {
                error.set_persistent()
            } else {
                error
            }
        })
    }

    pub(crate) fn new(client: Client, statement: String) -> Self {
        Self {
            client,
            statement,
            statement_id: None,
            max_elapsed: None,
            max_estimated: None,
        }
    }
}

/// Tracks one submitted query. Keep the handle to inspect progress, wait for
/// results, or request cancellation.
///
/// | Operation | Method |
/// | --------- | ------ |
/// | Read the last observed status without a request | [`Self::snapshot`] |
/// | Fetch the current status | [`Self::refresh`] |
/// | Wait for results | [`Self::wait`] |
/// | Request cancellation | [`Self::cancel`] |
///
/// Dropping a wait or refresh future stops local waiting without cancelling
/// execution. Terminal statuses are cached, so waiting again returns the result
/// without another request. Retry temporary read failures on this handle rather
/// than submitting another query.
#[derive(Debug)]
pub struct StatementHandle {
    client: Client,
    statement_id: Uuid,
    snapshot: Option<StatementStatus>,
}

impl StatementHandle {
    pub fn statement_id(&self) -> Uuid {
        self.statement_id
    }

    /// Returns the status snapshot received by the most recent request without
    /// making a network request.
    pub fn snapshot(&self) -> Option<&StatementStatus> {
        self.snapshot.as_ref()
    }

    /// Fetches a status snapshot, or returns the cached terminal state.
    /// Requests on a handle are serialized through its mutable borrow.
    pub async fn refresh(&mut self) -> Result<&StatementStatus, Error> {
        if self
            .snapshot
            .as_ref()
            .is_some_and(StatementStatus::is_terminated)
        {
            return Ok(self
                .snapshot
                .as_ref()
                .expect("terminal snapshot is present"));
        }

        self.snapshot = Some(
            self.client
                .fetch_statement(self.statement_id)
                .await
                .map_err(|error| error.set_statement_id(self.statement_id))?,
        );
        Ok(self.snapshot.as_ref().expect("status was just assigned"))
    }

    /// Waits for the statement to terminate and returns its result set.
    /// Dropping this future leaves the handle and server execution available.
    pub async fn wait(&mut self) -> Result<ResultSet, Error> {
        let mut delay = Duration::from_millis(5);
        let max_delay = Duration::from_secs(1);

        loop {
            self.refresh().await?;

            if let Some(status) = self.snapshot.as_ref() {
                match status {
                    StatementStatus::Finished(finished) => return Ok(finished.result_set()),
                    StatementStatus::Failed(failed) => {
                        return Err(Error::new(
                            ErrorKind::StatementFailed,
                            failed.error.message.clone(),
                        )
                        .set_statement_id(self.statement_id)
                        .set_statement_details(failed.error.clone()));
                    }
                    StatementStatus::Cancelled(cancelled) => {
                        return Err(Error::new(
                            ErrorKind::StatementCancelled,
                            format!("statement {} was cancelled", cancelled.statement_id),
                        )
                        .set_statement_id(self.statement_id));
                    }
                    StatementStatus::Pending(..) | StatementStatus::Running(..) => {
                        sleep(delay).await;
                        if delay < max_delay {
                            delay = std::cmp::min(delay.saturating_mul(2), max_delay);
                        }
                    }
                }
            }
        }
    }

    /// Requests cancellation and caches the observed status. Pending or Running
    /// means cancellation has not yet been observed; call wait() to settle it.
    /// A completion race returns the full result or original execution failure.
    pub async fn cancel(&mut self) -> Result<StatementStatus, Error> {
        if let Some(status) = self.snapshot.as_ref().filter(|s| s.is_terminated()) {
            return Ok(status.clone());
        }
        let status = self
            .client
            .cancel_statement(self.statement_id)
            .await
            .map_err(|error| error.set_statement_id(self.statement_id))?;
        self.snapshot = Some(status.clone());
        Ok(status)
    }

    pub(crate) fn new(client: Client, statement_id: Uuid) -> Self {
        Self {
            client,
            statement_id,
            snapshot: None,
        }
    }
}
