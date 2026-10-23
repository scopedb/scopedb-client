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
use crate::protocol::Response;
use crate::protocol::ResultFormat;
use crate::protocol::StatementRequest;
use crate::protocol::StatementRequestParams;
use crate::protocol::StatementStatus;
use crate::result::ResultSet;

#[derive(Debug)]
pub struct Statement {
    client: Client,
    statement: String,
    statement_id: Option<Uuid>,
    max_elapsed: Option<Duration>,
    max_estimated: Option<Duration>,
    format: ResultFormat,
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
            format,
        } = self;

        let resp = client
            .submit_statement(StatementRequest {
                statement,
                statement_id,
                max_elapsed_seconds: max_elapsed.map(|v| v.as_secs_f64()),
                max_estimated_seconds: max_estimated.map(|v| v.as_secs_f64()),
                params: StatementRequestParams { format },
            })
            .await?;

        match resp {
            Response::Success(response) => Ok(StatementHandle {
                client,
                statement_id: response.statement_id(),
                format,
                snapshot: Some(response),
            }),
            Response::Failed(err) => Err(err.into_error(ErrorKind::Unexpected)),
        }
    }

    /// Submits and waits. Dropping this future stops polling, not server execution.
    /// Retain a submitted handle to request cancellation explicitly.
    pub async fn execute(self) -> Result<ResultSet, Error> {
        self.submit().await?.wait().await
    }

    pub(crate) fn new(client: Client, statement: String) -> Self {
        Self {
            client,
            statement,
            statement_id: None,
            max_elapsed: None,
            max_estimated: None,
            format: ResultFormat::Json,
        }
    }
}

#[derive(Debug)]
pub struct StatementHandle {
    client: Client,
    statement_id: Uuid,
    format: ResultFormat,
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

        let (format, statement_id) = (self.format, self.statement_id);
        match self
            .client
            .fetch_statement(statement_id, StatementRequestParams { format })
            .await?
        {
            Response::Success(status) => {
                self.snapshot = Some(status);
                Ok(self.snapshot.as_ref().expect("status was just assigned"))
            }
            Response::Failed(err) => Err(err.into_error(ErrorKind::Unexpected)),
        }
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
                        .set_statement_details(failed.error.clone()));
                    }
                    StatementStatus::Cancelled(cancelled) => {
                        return Err(Error::new(
                            ErrorKind::StatementCancelled,
                            format!("statement {} was cancelled", cancelled.statement_id),
                        ));
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
        match self.client.cancel_statement(self.statement_id).await? {
            Response::Success(status) => {
                self.snapshot = Some(status.clone());
                Ok(status)
            }
            Response::Failed(err) => Err(err.into_error(ErrorKind::Unexpected)),
        }
    }

    pub(crate) fn new(client: Client, statement_id: Uuid, format: ResultFormat) -> Self {
        Self {
            client,
            statement_id,
            format,
            snapshot: None,
        }
    }
}
