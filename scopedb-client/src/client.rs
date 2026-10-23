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

use crate::Error;
use crate::ErrorKind;
use crate::ResultSet;
use crate::Statement;
use crate::Table;
use crate::protocol::AppendErrorDetails;
use crate::protocol::AppendRowsResult;
use crate::protocol::AppendState;
use crate::protocol::StatementRequest;
use crate::protocol::StatementStatus;
use crate::protocol::apply_response_metadata;
use crate::protocol::decode_statement_response;
use crate::statement::StatementHandle;

const JSON_CONTENT_ENCODING: &str = "zstd";
const UNCOMPRESSED_CONTENT_LENGTH: &str = "X-ScopeDB-Uncompressed-Content-Length";
const ZSTD_COMPRESSION_LEVEL: i32 = 3;
pub(crate) const MAX_APPEND_BODY_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_APPEND_ROWS: usize = 200_000;
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

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

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    /// Disable redirects and automatic retries on this client to prevent replaying writes.
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
    /// Shorter timeouts on a custom HTTP client still apply.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Validates the endpoint and authentication settings and builds the client.
    pub fn build(self) -> Result<Client, Error> {
        if self.write_timeout.is_zero() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "write_timeout must be greater than zero",
            ));
        }
        if tokio::time::Instant::now()
            .checked_add(self.write_timeout)
            .is_none()
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
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
                    Error::new(ErrorKind::ConfigInvalid, "failed to build HTTP client")
                        .set_source(err)
                })?,
        };
        let mut client = Client::new(self.endpoint, http_client)?;
        client.write_timeout = self.write_timeout;
        if let Some(api_key) = self.api_key {
            if api_key.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "api_key must not be empty",
                ));
            }
            let mut authorization =
                HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|err| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
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
                        ErrorKind::ConfigInvalid,
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
                ErrorKind::ConfigInvalid,
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
    pub fn catalog(&self) -> crate::Catalog {
        crate::Catalog::new(self.clone())
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
                    append_unknown_error("table append request timed out; commit outcome is unknown")
                        .set_write_timeout()
                } else {
                    append_unknown_error("failed to send table append request")
                };
                error.set_source(err)
            })?;
            let response_status = response.status();
            status = Some(response_status);
            headers = response.headers().clone();
            let payload = response.bytes().await.map_err(|err| {
                let error = if err.is_timeout() {
                    append_unknown_error("table append request timed out; commit outcome is unknown")
                        .set_write_timeout()
                } else {
                    append_unknown_error("failed to read table append response")
                };
                error.set_source(err)
            })?;
            let result = decode_append_response(response_status, &headers, &payload)?;
            if result.num_rows_inserted != expected_rows as i64 {
                return Err(append_unknown_error(format!(
                    "table append response reported {} inserted rows for a {expected_rows}-row request",
                    result.num_rows_inserted
                )));
            }
            Ok(result)
        })
        .await
        .unwrap_or_else(|err| {
            Err(
                append_unknown_error("table append request timed out; commit outcome is unknown")
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
                Error::new(ErrorKind::Unexpected, "failed to submit statement")
                    .set_source(err)
                    .set_persistent()
            })?;
        decode_statement_response(response, false)
            .await
            .map_err(|error| {
                // A gateway failure or unreadable response can follow successful execution.
                // HTTP status alone cannot authorize another submission in those cases.
                if error.http_status().is_none_or(|status| {
                    status.is_success()
                        || status.is_server_error()
                        || status == StatusCode::REQUEST_TIMEOUT
                }) {
                    error.set_persistent()
                } else {
                    error
                }
            })
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
                    ErrorKind::Unexpected,
                    format!("failed to fetch statement: {statement_id:?}"),
                )
                .set_source(err)
                .set_temporary()
            })?;
        decode_statement_response(response, true).await
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
                    ErrorKind::Unexpected,
                    format!("failed to cancel statement: {statement_id:?}"),
                )
                .set_source(err)
                .set_temporary()
            })?;
        decode_statement_response(response, true).await
    }

    #[track_caller]
    fn make_url(&self, path: &str) -> Result<Url, Error> {
        self.endpoint.join(path).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to construct URL".to_string()).set_source(err)
        })
    }

    pub(crate) fn make_resource_url(&self, segments: &[&str]) -> Result<Url, Error> {
        if segments.iter().any(|s| matches!(*s, "" | "." | "..")) {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "resource names must not be empty or dot segments",
            ));
        }
        let mut url = self.make_url("v1/")?;
        let mut path = url.path_segments_mut().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
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
                ErrorKind::Unexpected,
                "failed to serialize JSON request body",
            )
            .set_source(err)
        })?;
        let uncompressed_bytes = uncompressed.len();
        let body = zstd::stream::encode_all(uncompressed.as_slice(), ZSTD_COMPRESSION_LEVEL)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
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
                append_unknown_error("table append response has an invalid body"),
                status,
                headers,
            )),
            Err(err) => Err(apply_response_metadata(
                append_unknown_error("failed to decode table append response").set_source(err),
                status,
                headers,
            )),
        };
    }

    if status.is_success() {
        return Err(apply_response_metadata(
            append_unknown_error(format!(
                "table append returned unexpected success status {}",
                status.as_u16()
            )),
            status,
            headers,
        ));
    }

    let error =
        crate::protocol::decode_http_error(status, headers, payload, ErrorKind::AppendRowsFailed);
    if let Some(details) = error
        .details()
        .and_then(|value| serde_json::from_value::<AppendErrorDetails>(value.clone()).ok())
        && matches!(details.status, AppendState::Rejected | AppendState::Unknown)
    {
        let status = details.status;
        let error = error.set_append_details(details);

        return Err(if status == AppendState::Unknown {
            error.set_persistent()
        } else {
            error
        });
    }

    Err(error
        .set_append_details(AppendErrorDetails {
            status: AppendState::Unknown,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        })
        .set_persistent())
}

fn append_unknown_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::AppendRowsFailed, message)
        .set_append_details(AppendErrorDetails {
            status: AppendState::Unknown,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        })
        .set_persistent()
}

fn append_rejected_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::AppendRowsFailed, message).set_append_details(AppendErrorDetails {
        status: AppendState::Rejected,
        row_errors: Vec::new(),
        row_errors_truncated: false,
    })
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;
    use serde_json::json;

    use super::Client;
    use super::JSON_CONTENT_ENCODING;
    use super::UNCOMPRESSED_CONTENT_LENGTH;
    use super::decode_append_response;
    use crate::ErrorKind;
    use crate::protocol::AppendState;

    fn decode(status: StatusCode, payload: &[u8]) -> Result<crate::AppendRowsResult, crate::Error> {
        decode_append_response(status, &HeaderMap::new(), payload)
    }

    #[test]
    fn resource_url_preserves_base_path_and_encodes_segments() {
        let client = Client::new("https://example.com/proxy", reqwest::Client::new()).unwrap();

        let url = client
            .make_resource_url(&["databases", "analytics/2026", "schemas", "events archive"])
            .unwrap();

        assert_eq!(
            url.as_str(),
            "https://example.com/proxy/v1/databases/analytics%2F2026/schemas/events%20archive"
        );
    }

    #[test]
    fn client_builder_validates_and_redacts_api_keys() {
        let builder = Client::builder("https://example.com").api_key("secret-api-key");
        assert!(!format!("{builder:?}").contains("secret-api-key"));
        let client = Client::builder("https://example.com")
            .api_key("secret-api-key")
            .build()
            .unwrap();
        assert!(!format!("{client:?}").contains("secret-api-key"));

        let error = Client::builder("https://example.com")
            .api_key("")
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn write_timeout_defaults_and_validation() {
        let client = Client::new("https://example.com", reqwest::Client::new()).unwrap();
        assert_eq!(client.write_timeout, std::time::Duration::from_secs(30));
        let client = Client::builder("https://example.com").build().unwrap();
        assert_eq!(client.write_timeout, std::time::Duration::from_secs(30));
        for timeout in [std::time::Duration::ZERO, std::time::Duration::MAX] {
            let error = Client::builder("https://example.com")
                .write_timeout(timeout)
                .build()
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
        }
    }

    #[test]
    fn json_requests_use_zstd_by_default() {
        let client = Client::new("https://example.com", reqwest::Client::new()).unwrap();
        let value = json!({"statement": "SELECT 1", "format": "json"});
        let expected = serde_json::to_vec(&value).unwrap();
        let request = client
            .post_json(client.make_url("v1/statements").unwrap(), &value)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            request
                .headers()
                .get(reqwest::header::CONTENT_ENCODING)
                .unwrap(),
            JSON_CONTENT_ENCODING
        );
        assert_eq!(
            request
                .headers()
                .get(UNCOMPRESSED_CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap(),
            expected.len().to_string()
        );
        let compressed = request.body().unwrap().as_bytes().unwrap();
        assert_eq!(zstd::stream::decode_all(compressed).unwrap(), expected);
    }

    #[test]
    fn committed_append_response_is_returned() {
        let result = decode(
            StatusCode::OK,
            br#"{"status":"committed","num_rows_inserted":2}"#,
        )
        .unwrap();

        assert_eq!(result.status, AppendState::Committed);
        assert_eq!(result.num_rows_inserted, 2);
    }

    #[test]
    fn rejected_append_error_preserves_row_details_and_retry_status() {
        let error = decode(
            StatusCode::SERVICE_UNAVAILABLE,
            br#"{"message":"validation failed","code":"ingest_failure","details":{"status":"rejected","row_errors":[{"row_index":3,"column":"ts","message":"invalid timestamp"}],"row_errors_truncated":true}}"#,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::AppendRowsFailed);
        assert!(error.is_temporary());
        let details = error.append_details().unwrap();
        assert_eq!(details.status, AppendState::Rejected);
        assert_eq!(details.row_errors[0].row_index, 3);
        assert!(details.row_errors_truncated);
    }

    #[test]
    fn append_error_preserves_server_message_and_http_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "header-request".parse().unwrap());
        headers.insert(reqwest::header::RETRY_AFTER, "3".parse().unwrap());
        let error = decode_append_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &headers,
            br#"{"message":"capacity is temporarily unavailable","request_id":"body-request","retryable":false,"code":"ingest_failure","details":{"status":"rejected"}}"#,
        )
        .unwrap_err();

        assert_eq!(error.message(), "capacity is temporarily unavailable");
        assert_eq!(error.http_status(), Some(StatusCode::SERVICE_UNAVAILABLE));
        assert_eq!(error.request_id(), Some("body-request"));
        assert_eq!(error.retry_after(), Some(std::time::Duration::from_secs(3)));
        assert!(!error.is_retryable());
    }

    #[test]
    fn unknown_append_outcome_is_never_retryable() {
        let error = decode(
            StatusCode::SERVICE_UNAVAILABLE,
            br#"{"message":"coordinator unavailable","code":"ingest_failure","details":{"status":"unknown"}}"#,
        )
        .unwrap_err();

        assert!(error.is_persistent());
        assert!(!error.is_temporary());
        assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
    }

    #[test]
    fn unstructured_append_error_is_treated_as_unknown() {
        let error = decode(StatusCode::BAD_REQUEST, br#"{"message":"bad request"}"#).unwrap_err();

        assert!(error.is_persistent());
        assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
    }

    #[test]
    fn nested_append_error_has_clean_message_and_unknown_outcome() {
        let error = decode(
            StatusCode::NOT_FOUND,
            br#"{"error":{"message":"unsupported path"}}"#,
        )
        .unwrap_err();

        assert_eq!(error.message(), "unsupported path");
        assert!(error.is_persistent());
        assert_eq!(error.append_details().unwrap().status, AppendState::Unknown);
    }

    #[test]
    fn nested_append_rejection_keeps_its_outcome_and_diagnostics() {
        let error = decode(
            StatusCode::SERVICE_UNAVAILABLE,
            br#"{"request_id":"proxy-request","error":{"message":"try later","code":"ingest_failure","retryable":false,"details":{"status":"rejected","row_errors":[{"row_index":1,"column":"id","message":"invalid int"}]}}}"#,
        )
        .unwrap_err();
        assert_eq!(error.code(), Some("ingest_failure"));
        assert_eq!(error.request_id(), Some("proxy-request"));
        assert_eq!(error.http_status(), Some(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!error.is_retryable());
        let details = error.append_details().unwrap();
        assert_eq!(details.status, AppendState::Rejected);
        assert_eq!(details.row_errors[0].row_index, 1);
    }
}
