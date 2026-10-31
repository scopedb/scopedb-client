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

use std::marker::PhantomData;

use fastrace_reqwest::traceparent_headers;
use reqwest::Method;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Client;
use crate::DataType;
use crate::Error;
use crate::ErrorKind;
use crate::ErrorOperation;
use crate::protocol::apply_response_metadata;
use crate::protocol::decode_http_error;

/// Complete tenant catalog definition.
///
/// [`Catalog::apply`] replaces the entire catalog. Typed definitions reject
/// unknown fields; use [`Catalog::raw`] to preserve fields from newer server versions.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDocument {
    #[serde(default)]
    pub databases: Vec<CatalogDatabase>,
}

/// Database definition within a [`CatalogDocument`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDatabase {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub schemas: Vec<CatalogSchema>,
}

/// Schema definition containing tables and views.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSchema {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub tables: Vec<CatalogTable>,
    #[serde(default)]
    pub views: Vec<CatalogView>,
}

/// Complete table definition.
///
/// [`CatalogResource::apply`] replaces the entire table definition; omitted
/// collections become empty and omitted optional fields are cleared. `storage`
/// is accepted only when creating a table and is omitted from exports.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogTable {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub columns: Vec<CatalogColumn>,
    #[serde(default)]
    pub partition_by: Vec<String>,
    #[serde(default)]
    pub cluster_by: Vec<String>,
    #[serde(default)]
    pub distinct: CatalogDistinct,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_days: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<serde_json::Value>,
}

/// Table column definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogColumn {
    pub name: String,
    pub data_type: DataType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub indexes: Vec<CatalogColumnIndex>,
}

/// Secondary column index definition (such as `index_type: "search"` with `analyzer: Some("log")`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogColumnIndex {
    #[serde(rename = "type")]
    pub index_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
}

/// Table deduplication key (`on`) and ordering (`by`) configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDistinct {
    #[serde(default)]
    pub on: Vec<String>,
    #[serde(default)]
    pub by: Vec<String>,
}

/// View definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogView {
    pub name: String,
    pub statement: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Options for [`CatalogResource::apply`] and [`CatalogResource::delete`].
#[derive(Debug, Clone, Default)]
pub struct CatalogOptions {
    /// Validate and preview the change without committing it.
    pub dry_run: bool,
    /// Authorize changes that drop data or columns (`destructive == true`).
    pub allow_destructive: bool,
    /// ETag precondition from a prior [`CatalogResource::get`] or dry-run [`CatalogResult`],
    /// including quotes.
    pub if_match: Option<String>,
}

/// Commit status of a failed catalog mutation, exposed via [`Error::catalog_outcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CatalogMutationOutcome {
    /// The server rejected the mutation without committing it.
    Rejected,
    /// The mutation may have committed; read the resource with [`CatalogResource::get`] before
    /// retrying.
    Unknown,
}

/// Resource definition and opaque ETag returned by [`CatalogResource::get`].
#[derive(Debug, Clone)]
pub struct CatalogSnapshot<T> {
    pub definition: T,
    pub etag: String,
}

/// Result of [`CatalogResource::apply`] or [`CatalogResource::delete`].
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogResult<T> {
    /// Resource definition before the mutation, or `None` if it did not exist.
    pub before: Option<T>,
    /// Resource definition after the mutation, or `None` if it was deleted.
    pub after: Option<T>,
    /// Whether the change drops existing data or schema objects.
    pub destructive: bool,
    /// Base ETag for a dry run, or the new resource ETag after a committed mutation.
    #[serde(skip)]
    pub etag: String,
}

/// Entry point for declarative catalog snapshots and updates.
///
/// Select the smallest resource you need, then call [`CatalogResource::get`].
/// Each read returns the complete definition and an ETag for conditional changes.
///
/// ```no_run
/// # async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
/// let snapshot = client.catalog().schema("scopedb", "public").get().await?;
/// for table in snapshot.definition.tables {
///     println!("{}: {} columns", table.name, table.columns.len());
/// }
/// # Ok(())
/// # }
/// ```
///
/// Use [`CatalogResource::apply`] to replace a definition and
/// [`CatalogResource::delete`] to remove a resource. Both support dry-run
/// previews and ETag preconditions through [`CatalogOptions`].
#[derive(Debug, Clone)]
pub struct Catalog {
    resource: CatalogResource<CatalogDocument>,
}

impl Catalog {
    /// Reads the complete tenant catalog and its ETag.
    pub async fn get(&self) -> Result<CatalogSnapshot<CatalogDocument>, Error> {
        self.resource.get().await
    }

    /// Replaces the complete tenant catalog. An empty [`CatalogDocument`] removes all databases.
    pub async fn apply(
        &self,
        definition: &CatalogDocument,
        options: CatalogOptions,
    ) -> Result<CatalogResult<CatalogDocument>, Error> {
        self.resource.apply(definition, options).await
    }

    /// Returns a raw JSON selector that preserves unknown fields and arbitrary-precision numbers.
    pub fn raw(&self) -> CatalogResource<serde_json::Value> {
        self.resource.raw()
    }

    /// Selects `database`.
    pub fn database(&self, database: &str) -> CatalogResource<CatalogDatabase> {
        CatalogResource::new(
            self.resource.client.clone(),
            &["catalog", "databases", database],
        )
    }

    /// Selects `database.schema`.
    pub fn schema(&self, database: &str, schema: &str) -> CatalogResource<CatalogSchema> {
        CatalogResource::new(
            self.resource.client.clone(),
            &["catalog", "databases", database, "schemas", schema],
        )
    }

    /// Selects `database.schema.table`.
    pub fn table(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> CatalogResource<CatalogTable> {
        CatalogResource::new(
            self.resource.client.clone(),
            &[
                "catalog",
                "databases",
                database,
                "schemas",
                schema,
                "tables",
                table,
            ],
        )
    }

    /// Selects view `database.schema.view`.
    pub fn view(&self, database: &str, schema: &str, view: &str) -> CatalogResource<CatalogView> {
        CatalogResource::new(
            self.resource.client.clone(),
            &[
                "catalog",
                "databases",
                database,
                "schemas",
                schema,
                "views",
                view,
            ],
        )
    }

    pub(crate) fn new(client: Client) -> Self {
        Self {
            resource: CatalogResource::new(client, &["catalog"]),
        }
    }
}

/// Selector for reading, replacing, or deleting a catalog subtree.
///
/// [`Self::apply`] replaces the entire selected subtree (`PUT`), including
/// child resources; start from [`Self::get`] when modifying an existing
/// resource. Typed definitions reject unknown fields;
/// call [`Self::raw`] to read and apply [`serde_json::Value`] definitions with
/// forward-compatible fields and arbitrary-precision numbers.
///
/// Catalog mutations use [`ClientBuilder::write_timeout`](crate::ClientBuilder::write_timeout)
/// and are never retried automatically. On failure, inspect
/// [`Error::catalog_outcome`]: [`CatalogMutationOutcome::Unknown`] means the
/// mutation may have committed (including on timeouts or gateway errors) and
/// requires reading the resource before retrying.
#[derive(Debug, Clone)]
pub struct CatalogResource<T> {
    client: Client,
    path: Vec<String>,
    definition: PhantomData<T>,
}

impl<T> CatalogResource<T> {
    /// Converts this selector to [`serde_json::Value`] to preserve unknown fields and
    /// arbitrary-precision numbers.
    pub fn raw(&self) -> CatalogResource<serde_json::Value> {
        CatalogResource {
            client: self.client.clone(),
            path: self.path.clone(),
            definition: PhantomData,
        }
    }

    fn new(client: Client, path: &[&str]) -> Self {
        Self {
            client,
            path: path.iter().map(|s| (*s).to_string()).collect(),
            definition: PhantomData,
        }
    }

    async fn request<R: DeserializeOwned>(
        &self,
        method: Method,
        body: Option<Vec<u8>>,
        options: CatalogOptions,
    ) -> Result<(R, String), Error> {
        let mutation = method != Method::GET;
        let writing = mutation && !options.dry_run;
        let url = self
            .client
            .make_resource_url(&self.path.iter().map(String::as_str).collect::<Vec<_>>())?;
        let mut request = self
            .client
            .request(method.clone(), url)
            .headers(traceparent_headers());
        if options.dry_run {
            request = request.query(&[("dry_run", true)]);
        }
        if options.allow_destructive {
            request = request.query(&[("allow_destructive", true)]);
        }
        if let Some(etag) = options.if_match {
            let etag = reqwest::header::HeaderValue::from_str(&etag).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    ErrorOperation::ApplyCatalog,
                    "invalid If-Match header",
                )
                .set_source(e)
            })?;
            request = request.header(reqwest::header::IF_MATCH, etag);
        }
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let mut response_metadata = None;
        let read_response = async {
            let response = request.send().await.map_err(|e| {
                catalog_error(
                    writing,
                    if e.is_timeout() {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Unavailable
                    },
                    "failed to send catalog request",
                )
                .set_source(e)
            })?;
            let status = response.status();
            let headers = response.headers().clone();
            response_metadata = Some((status, headers.clone()));
            let bytes = response.bytes().await.map_err(|e| {
                apply_response_metadata(
                    catalog_error(
                        writing,
                        if e.is_timeout() {
                            ErrorKind::Timeout
                        } else {
                            ErrorKind::Unavailable
                        },
                        "failed to read catalog response",
                    )
                    .set_source(e),
                    status,
                    &headers,
                )
            })?;
            Ok::<_, Error>((status, headers, bytes))
        };
        let (status, headers, bytes) = if mutation {
            match tokio::time::timeout(self.client.write_timeout(), read_response).await {
                Ok(result) => result?,
                Err(source) => {
                    let error =
                        catalog_error(writing, ErrorKind::Timeout, "catalog request timed out")
                            .set_source(source);
                    return Err(match response_metadata {
                        Some((status, headers)) => apply_response_metadata(error, status, &headers),
                        None => error,
                    });
                }
            }
        } else {
            read_response.await?
        };
        if !status.is_success() {
            let error = decode_http_error(
                status,
                &headers,
                &bytes,
                match method {
                    Method::GET => ErrorOperation::ReadCatalog,
                    Method::DELETE => ErrorOperation::DeleteCatalog,
                    _ => ErrorOperation::ApplyCatalog,
                },
            );
            return Err(if writing {
                // A coded client rejection establishes failure. A gateway or
                // timeout response can follow a commit, even when it has a code.
                let outcome = if status.is_client_error()
                    && status != StatusCode::REQUEST_TIMEOUT
                    && error.code().is_some()
                {
                    CatalogMutationOutcome::Rejected
                } else {
                    CatalogMutationOutcome::Unknown
                };
                error.set_catalog_outcome(outcome)
            } else {
                error
            });
        }
        let etag = headers
            .get(reqwest::header::ETAG)
            .and_then(|h| h.to_str().ok())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                apply_response_metadata(
                    catalog_error(
                        writing,
                        ErrorKind::InvalidResponse,
                        "catalog response is missing ETag",
                    ),
                    status,
                    &headers,
                )
            })?
            .to_string();
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
            apply_response_metadata(
                catalog_error(
                    writing,
                    ErrorKind::InvalidResponse,
                    "invalid catalog response",
                )
                .set_source(e),
                status,
                &headers,
            )
        })?;
        let valid_resource = |v: &serde_json::Value| v.is_object() || v.is_null();
        if !value.is_object()
            || (mutation
                && !(value.get("before").is_some_and(valid_resource)
                    && value.get("after").is_some_and(valid_resource)
                    && value
                        .get("destructive")
                        .is_some_and(serde_json::Value::is_boolean)))
        {
            return Err(apply_response_metadata(
                catalog_error(
                    writing,
                    ErrorKind::InvalidResponse,
                    "invalid catalog response",
                ),
                status,
                &headers,
            ));
        }
        let result = serde_json::from_value(value).map_err(|e| {
            apply_response_metadata(
                catalog_error(
                    writing,
                    ErrorKind::InvalidResponse,
                    "cannot decode catalog definition; use raw() to retain unsupported fields",
                )
                .set_source(e),
                status,
                &headers,
            )
        })?;
        Ok((result, etag))
    }
}

impl<T: Serialize + DeserializeOwned> CatalogResource<T> {
    /// Reads the complete definition of the selected resource and its ETag.
    pub async fn get(&self) -> Result<CatalogSnapshot<T>, Error> {
        let (definition, etag) = self
            .request(Method::GET, None, CatalogOptions::default())
            .await
            .map_err(|error| error.with_operation(ErrorOperation::ReadCatalog))?;
        Ok(CatalogSnapshot { definition, etag })
    }

    /// Replaces the selected resource definition (`PUT`), clearing any omitted optional fields or
    /// child collections.
    ///
    /// Use [`CatalogOptions`] to preview changes with `dry_run`, guard against
    /// concurrent edits with `if_match`, and authorize destructive changes with
    /// `allow_destructive`.
    ///
    /// ```no_run
    /// use scopedb_client::CatalogOptions;
    ///
    /// # async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
    /// let resource = client.table("events").catalog();
    /// let snapshot = resource.get().await?;
    /// let mut definition = snapshot.definition;
    /// definition.comment = Some("Application events".to_string());
    ///
    /// let preview = resource
    ///     .apply(
    ///         &definition,
    ///         CatalogOptions {
    ///             dry_run: true,
    ///             if_match: Some(snapshot.etag),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    /// println!("destructive = {}", preview.destructive);
    /// resource
    ///     .apply(
    ///         &definition,
    ///         CatalogOptions {
    ///             if_match: Some(preview.etag),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply(
        &self,
        definition: &T,
        options: CatalogOptions,
    ) -> Result<CatalogResult<T>, Error> {
        let body = serde_json::to_vec(definition).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::ApplyCatalog,
                "failed to serialize catalog definition",
            )
            .set_source(e)
        })?;
        let (mut result, etag): (CatalogResult<T>, _) = self
            .request(Method::PUT, Some(body), options)
            .await
            .map_err(|error| error.with_operation(ErrorOperation::ApplyCatalog))?;
        result.etag = etag;
        Ok(result)
    }

    /// Deletes the selected database, schema, table, or view.
    ///
    /// Deleting the root catalog is not supported; apply an empty [`CatalogDocument`] instead.
    pub async fn delete(&self, options: CatalogOptions) -> Result<CatalogResult<T>, Error> {
        if self.path.len() == 1 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                ErrorOperation::DeleteCatalog,
                "the root catalog does not support DELETE; apply an empty document instead",
            ));
        }
        let (mut result, etag): (CatalogResult<T>, _) = self
            .request(Method::DELETE, None, options)
            .await
            .map_err(|error| error.with_operation(ErrorOperation::DeleteCatalog))?;
        result.etag = etag;
        Ok(result)
    }
}

fn catalog_error(writing: bool, kind: ErrorKind, message: &str) -> Error {
    let error = Error::new(kind, ErrorOperation::ApplyCatalog, message)
        .with_transient_failure(matches!(kind, ErrorKind::Unavailable | ErrorKind::Timeout));
    if writing {
        error.set_catalog_outcome(CatalogMutationOutcome::Unknown)
    } else {
        error
    }
}
