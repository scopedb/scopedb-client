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
use std::time::Duration;

use fastrace_reqwest::traceparent_headers;
use reqwest::IntoUrl;
use reqwest::Method;
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::header::AUTHORIZATION;
use reqwest::header::CONTENT_ENCODING;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::HeaderValue;
use serde::Serialize;
use uuid::Uuid;

use crate::AppendErrorDetails;
use crate::AppendRowsResult;
use crate::AppendState;
use crate::Catalog;
use crate::Error;
use crate::ErrorKind;
use crate::ErrorOperation;
use crate::ResultSet;
use crate::Statement;
use crate::StatementHandle;
use crate::StatementStatus;
use crate::Table;
use crate::protocol::StatementRequest;
use crate::protocol::apply_response_metadata;
use crate::protocol::decode_http_error;
use crate::protocol::decode_statement_response;

const JSON_CONTENT_ENCODING: &str = "zstd";
const UNCOMPRESSED_CONTENT_LENGTH: &str = "X-ScopeDB-Uncompressed-Content-Length";
const ZSTD_COMPRESSION_LEVEL: i32 = 3;
pub const MAX_APPEND_BODY_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_APPEND_ROWS: usize = 200_000;
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// An async client for queries, table writes, and catalog operations.
///
/// Start with [`Self::builder`] to configure your endpoint and API key.
/// Clones share the underlying HTTP connection pool.
#[derive(Debug, Clone)]
pub struct Client {
    endpoint: Url,
    client: reqwest::Client,
    authorization: Option<HeaderValue>,
    write_timeout: Duration,
}

/// Builds a ScopeDB client with an optional API key and custom HTTP client.
///
/// The API key is attached by the ScopeDB client itself, so callers can still
/// provide a preconfigured reqwest client for TLS, proxy, timeout, and pooling
/// settings.
pub struct ClientBuilder {
    endpoint: String,
    client: Option<reqwest::Client>,
    api_key: Option<String>,
    write_timeout: Duration,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("endpoint", &self.endpoint)
            .field("client", &self.client)
            .field("has_api_key", &self.api_key.is_some())
            .field("write_timeout", &self.write_timeout)
            .finish()
    }
}

impl ClientBuilder {
    /// Sets the ScopeDB API key sent as a sensitive Bearer credential.
    ///
    /// This per-request value takes precedence over an `Authorization` default
    /// configured on the underlying HTTP client.
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Uses a preconfigured compatible HTTP client.
    ///
    /// Use the re-exported [`crate::reqwest`] version. Disable redirects and
    /// transport retries to prevent replaying writes; the SDK's default client
    /// already does this.
    ///
    /// ```
    /// use scopedb_client::reqwest;
    ///
    /// let http_client = reqwest::Client::builder()
    ///     .redirect(reqwest::redirect::Policy::none())
    ///     .retry(reqwest::retry::never())
    ///     .build()?;
    /// # Ok::<(), reqwest::Error>(())
    /// ```
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Bounds each table append and Catalog mutation HTTP request, including
    /// reading the response body. Defaults to 30 seconds and must be finite and nonzero.
    /// This deadline cannot be disabled; increase it when writes need more time.
    /// The budget starts after local encoding, compression, and request preparation.
    /// Stream `attempt_timeout` settings override this default. A timeout does
    /// not roll back the write; its commit outcome can be unknown.
    ///
    /// This is a per-attempt budget. Waiting for stream capacity, retry backoff,
    /// and the total duration of a flush or shutdown are outside it.
    /// Append timeouts are exposed through [`Error::write_details`] and are
    /// never automatically replayed.
    ///
    /// Shorter timeouts on a custom HTTP client still apply.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Validates the endpoint and authentication settings and builds the client.
    pub fn build(self) -> Result<Client, Error> {
        if self.write_timeout.is_zero() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::BuildClient,
                "write_timeout must be greater than zero",
            ));
        }
        if tokio::time::Instant::now()
            .checked_add(self.write_timeout)
            .is_none()
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::BuildClient,
                "write_timeout is too large",
            ));
        }
        let http_client = match self.client {
            Some(client) => client,
            None => reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|err| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        ErrorOperation::BuildClient,
                        "failed to build HTTP client",
                    )
                    .set_source(err)
                })?,
        };
        let mut client = Client::new(self.endpoint, http_client)?;
        client.write_timeout = self.write_timeout;
        if let Some(api_key) = self.api_key {
            if api_key.is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    ErrorOperation::BuildClient,
                    "api_key must not be empty",
                ));
            }
            let mut authorization =
                HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|err| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        ErrorOperation::BuildClient,
                        "api_key is not a valid HTTP credential",
                    )
                    .set_source(err)
                })?;
            authorization.set_sensitive(true);
            client.authorization = Some(authorization);
        }
        Ok(client)
    }
}

impl Client {
    /// Starts a client builder for the common API-key authentication path.
    pub fn builder(endpoint: impl ToString) -> ClientBuilder {
        ClientBuilder {
            endpoint: endpoint.to_string(),
            client: None,
            api_key: None,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
        }
    }

    /// Creates a ScopeDB client from an endpoint and a compatible HTTP client.
    ///
    /// Use the HTTP types re-exported from [`crate::reqwest`] to avoid a
    /// dependency-version mismatch in applications using another reqwest major.
    /// Configure the HTTP client as described in [`ClientBuilder::http_client`]
    /// to prevent automatic write replay. The write timeout defaults to 30 seconds.
    pub fn new<E: IntoUrl>(endpoint: E, client: reqwest::Client) -> Result<Self, Error> {
        match endpoint.into_url() {
            Ok(mut endpoint) => {
                if !matches!(endpoint.scheme(), "http" | "https")
                    || endpoint.host_str().is_none()
                    || !endpoint.username().is_empty()
                    || endpoint.password().is_some()
                    || endpoint.query().is_some()
                    || endpoint.fragment().is_some()
                {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        ErrorOperation::BuildClient,
                        "endpoint must be an HTTP(S) URL without credentials, query, or fragment",
                    ));
                }
                if !endpoint.path().ends_with('/') {
                    let path = format!("{}/", endpoint.path());
                    endpoint.set_path(&path);
                }
                Ok(Self {
                    endpoint,
                    client,
                    authorization: None,
                    write_timeout: DEFAULT_WRITE_TIMEOUT,
                })
            }
            Err(err) => Err(Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::BuildClient,
                "failed to parse endpoint".to_string(),
            )
            .set_source(err)),
        }
    }

    pub fn statement(&self, statement: impl Into<String>) -> Statement {
        Statement::new(self.clone(), statement.into())
    }

    pub fn statement_handle(&self, statement_id: Uuid) -> StatementHandle {
        StatementHandle::new(self.clone(), statement_id)
    }

    pub fn table(&self, table: impl Into<String>) -> Table {
        Table::new(self.clone(), table.into())
    }

    /// Executes a ScopeQL statement and waits for all result rows.
    /// Errors after submission retain [`Error::statement_id`] for resuming the
    /// existing execution with [`Self::statement_handle`], not resubmitting it.
    pub async fn query(&self, statement: impl Into<String>) -> Result<ResultSet, Error> {
        self.statement(statement).execute().await
    }

    /// Selects the complete declarative catalog or an individual resource.
    pub fn catalog(&self) -> Catalog {
        Catalog::new(self.clone())
    }

    /// Appends at most 8 MiB of uncompressed newline-delimited JSON to a table.
    ///
    /// Sends one request without automatic retries. An unknown outcome remains
    /// non-retryable through [`Error::is_retryable`] because replay may duplicate rows.
    pub async fn append_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        ndjson: impl Into<String>,
    ) -> Result<AppendRowsResult, Error> {
        self.append_rows_with_timeout(database, schema, table, ndjson.into(), None)
            .await
    }

    pub(crate) async fn append_rows_with_timeout(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        ndjson: String,
        timeout: Option<Duration>,
    ) -> Result<AppendRowsResult, Error> {
        if ndjson.len() > MAX_APPEND_BODY_BYTES {
            return Err(append_rejected_error(format!(
                "append payload requires {} bytes, exceeds the {MAX_APPEND_BODY_BYTES}-byte append limit",
                ndjson.len()
            )));
        }
        let expected_rows = ndjson
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        if expected_rows > MAX_APPEND_ROWS {
            return Err(append_rejected_error(format!(
                "append payload exceeds the {MAX_APPEND_ROWS}-row limit",
            )));
        }
        let url = self.make_resource_url(&[
            "catalog",
            "databases",
            database,
            "schemas",
            schema,
            "tables",
            table,
            "ingest",
        ])?;
        let body =
            zstd::stream::encode_all(ndjson.as_bytes(), ZSTD_COMPRESSION_LEVEL).map_err(|err| {
                append_rejected_error("failed to compress table append request body")
                    .set_source(err)
            })?;
        let request = self
            .request(Method::POST, url)
            .headers(traceparent_headers())
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .header(reqwest::header::CONTENT_ENCODING, JSON_CONTENT_ENCODING)
            .body(body);
        let mut status = None;
        let mut headers = reqwest::header::HeaderMap::new();
        tokio::time::timeout(timeout.unwrap_or(self.write_timeout), async {
            let response = request.send().await.map_err(|err| {
                let error = if err.is_timeout() {
                    append_unknown_error(ErrorKind::Timeout, "table append request timed out; commit outcome is unknown")
                        .set_write_timeout()
                } else {
                    append_unknown_error(ErrorKind::Unavailable, "failed to send table append request")
                };
                error.set_source(err)
            })?;
            let response_status = response.status();
            status = Some(response_status);
            headers = response.headers().clone();
            let payload = response.bytes().await.map_err(|err| {
                let error = if err.is_timeout() {
                    append_unknown_error(ErrorKind::Timeout, "table append request timed out; commit outcome is unknown")
                        .set_write_timeout()
                } else {
                    append_unknown_error(ErrorKind::Unavailable, "failed to read table append response")
                };
                error.set_source(err)
            })?;
            let result = decode_append_response(response_status, &headers, &payload)?;
            if result.num_rows_inserted != expected_rows as i64 {
                return Err(append_unknown_error(ErrorKind::InvalidResponse, format!(
                    "table append response reported {} inserted rows for a {expected_rows}-row request",
                    result.num_rows_inserted
                )));
            }
            Ok(result)
        })
        .await
        .unwrap_or_else(|err| {
            Err(
                append_unknown_error(ErrorKind::Timeout, "table append request timed out; commit outcome is unknown")
                    .set_write_timeout()
                    .set_source(err),
            )
        })
        .map_err(|err| match status {
            Some(status) if err.http_status().is_none() => {
                apply_response_metadata(err, status, &headers)
            }
            _ => err,
        })
    }
}

impl Client {
    #[fastrace::trace]
    pub(crate) async fn submit_statement(
        &self,
        request: StatementRequest,
    ) -> Result<StatementStatus, Error> {
        let url = self.make_url("v1/statements")?;
        let response = self
            .post_json(url, &request)?
            .headers(traceparent_headers())
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    if err.is_timeout() {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Unavailable
                    },
                    ErrorOperation::SubmitStatement,
                    "failed to submit statement",
                )
                .set_source(err)
            })?;
        decode_statement_response(response, ErrorOperation::SubmitStatement).await
    }

    #[fastrace::trace]
    pub(crate) async fn fetch_statement(
        &self,
        statement_id: Uuid,
    ) -> Result<StatementStatus, Error> {
        let path = format!("v1/statements/{statement_id}");
        let url = self.make_url(&path)?;
        let response = self
            .request(Method::GET, url)
            .headers(traceparent_headers())
            .query(&[("format", "json")])
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    if err.is_timeout() {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Unavailable
                    },
                    ErrorOperation::RefreshStatement,
                    format!("failed to fetch statement: {statement_id:?}"),
                )
                .set_source(err)
                .with_transient_failure(true)
            })?;
        decode_statement_response(response, ErrorOperation::RefreshStatement).await
    }

    #[fastrace::trace]
    pub(crate) async fn cancel_statement(
        &self,
        statement_id: Uuid,
    ) -> Result<StatementStatus, Error> {
        let path = format!("v1/statements/{statement_id}/cancel");
        let url = self.make_url(&path)?;
        let response = self
            .request(Method::POST, url)
            .headers(traceparent_headers())
            .query(&[("format", "json")])
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    if err.is_timeout() {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Unavailable
                    },
                    ErrorOperation::CancelStatement,
                    format!("failed to cancel statement: {statement_id:?}"),
                )
                .set_source(err)
                .with_transient_failure(true)
            })?;
        decode_statement_response(response, ErrorOperation::CancelStatement).await
    }

    #[track_caller]
    fn make_url(&self, path: &str) -> Result<Url, Error> {
        self.endpoint.join(path).map_err(|err| {
            Error::new(
                ErrorKind::Internal,
                ErrorOperation::Append,
                "failed to construct URL".to_string(),
            )
            .set_source(err)
        })
    }

    pub(crate) fn make_resource_url(&self, segments: &[&str]) -> Result<Url, Error> {
        if segments.iter().any(|s| matches!(*s, "" | "." | "..")) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::Append,
                "resource names must not be empty or dot segments",
            ));
        }
        let mut url = self.make_url("v1/")?;
        let mut path = url.path_segments_mut().map_err(|_| {
            Error::new(
                ErrorKind::Internal,
                ErrorOperation::Append,
                "endpoint cannot be used as a base URL",
            )
        })?;
        path.pop_if_empty();
        path.extend(segments.iter().copied());
        drop(path);
        Ok(url)
    }

    pub(crate) fn write_timeout(&self) -> Duration {
        self.write_timeout
    }

    pub(crate) fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        let request = self
            .client
            .request(method, url)
            .header("X-ScopeDB-Client", "scopedb-client");
        if let Some(authorization) = &self.authorization {
            request.header(AUTHORIZATION, authorization.clone())
        } else {
            request
        }
    }

    fn post_json<T: Serialize>(
        &self,
        url: Url,
        value: &T,
    ) -> Result<reqwest::RequestBuilder, Error> {
        let uncompressed = serde_json::to_vec(value).map_err(|err| {
            Error::new(
                ErrorKind::Internal,
                ErrorOperation::SubmitStatement,
                "failed to serialize JSON request body",
            )
            .set_source(err)
        })?;
        let uncompressed_bytes = uncompressed.len();
        let body = zstd::stream::encode_all(uncompressed.as_slice(), ZSTD_COMPRESSION_LEVEL)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Internal,
                    ErrorOperation::SubmitStatement,
                    "failed to compress JSON request body",
                )
                .set_source(err)
            })?;

        Ok(self
            .request(Method::POST, url)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_ENCODING, JSON_CONTENT_ENCODING)
            .header(UNCOMPRESSED_CONTENT_LENGTH, uncompressed_bytes.to_string())
            .body(body))
    }
}

fn decode_append_response(
    status: StatusCode,
    headers: &reqwest::header::HeaderMap,
    payload: &[u8],
) -> Result<AppendRowsResult, Error> {
    if status == StatusCode::OK {
        return match serde_json::from_slice::<AppendRowsResult>(payload) {
            Ok(result)
                if result.status == AppendState::Committed && result.num_rows_inserted >= 0 =>
            {
                Ok(result)
            }
            Ok(_) => Err(apply_response_metadata(
                append_unknown_error(
                    ErrorKind::InvalidResponse,
                    "table append response has an invalid body",
                ),
                status,
                headers,
            )),
            Err(err) => Err(apply_response_metadata(
                append_unknown_error(
                    ErrorKind::InvalidResponse,
                    "failed to decode table append response",
                )
                .set_source(err),
                status,
                headers,
            )),
        };
    }

    if status.is_success() {
        return Err(apply_response_metadata(
            append_unknown_error(
                ErrorKind::InvalidResponse,
                format!(
                    "table append returned unexpected success status {}",
                    status.as_u16()
                ),
            ),
            status,
            headers,
        ));
    }

    let error = decode_http_error(status, headers, payload, ErrorOperation::Append);
    if let Some(details) = error
        .details()
        .and_then(|value| serde_json::from_value::<AppendErrorDetails>(value.clone()).ok())
        && matches!(details.status, AppendState::Rejected | AppendState::Unknown)
    {
        let error = error.set_append_details(details);

        return Err(error);
    }

    Err(error.set_append_details(AppendErrorDetails {
        status: AppendState::Unknown,
        row_errors: Vec::new(),
        row_errors_truncated: false,
    }))
}

fn append_unknown_error(kind: ErrorKind, message: impl Into<String>) -> Error {
    Error::new(kind, ErrorOperation::Append, message).set_append_details(AppendErrorDetails {
        status: AppendState::Unknown,
        row_errors: Vec::new(),
        row_errors_truncated: false,
    })
}

fn append_rejected_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, ErrorOperation::Append, message).set_append_details(
        AppendErrorDetails {
            status: AppendState::Rejected,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        },
    )
}
