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
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Client;
use crate::DataType;
use crate::Error;
use crate::ErrorKind;
use crate::protocol::apply_response_metadata;
use crate::protocol::decode_http_error;

/// Complete tenant catalog. Apply replaces the entire selected subtree.
/// Typed definitions reject unknown fields; use [`Catalog::raw`] for newer definitions.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDocument {
    #[serde(default)]
    pub databases: Vec<CatalogDatabase>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDatabase {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub schemas: Vec<CatalogSchema>,
}

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

/// A complete table definition. Omitted collections are empty and omitted
/// optional fields are cleared. Storage is accepted only at creation.
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

/// Additional column index. Supports type "search" and analyzer "log".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogColumnIndex {
    #[serde(rename = "type")]
    pub index_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDistinct {
    #[serde(default)]
    pub on: Vec<String>,
    #[serde(default)]
    pub by: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogView {
    pub name: String,
    pub statement: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Preview, concurrency preconditions, and explicit data-loss authorization.
#[derive(Debug, Clone, Default)]
pub struct CatalogOptions {
    pub dry_run: bool,
    pub allow_destructive: bool,
    /// Requires the opaque version returned by a prior read or dry run. Pass it unchanged.
    pub expected_version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CatalogSnapshot<T> {
    pub definition: T,
    /// Opaque catalog version. Pass it unchanged as `expected_version` for conditional changes.
    pub version: String,
}

/// Normalized states compared in one transaction; None means absent.
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogResult<T> {
    pub before: Option<T>,
    pub after: Option<T>,
    pub destructive: bool,
    /// The base version for dry-run, or the resulting version after a mutation.
    #[serde(skip)]
    pub version: String,
}

/// Reads database, schema, table, and view definitions.
///
/// Select the smallest resource you need, then call [`CatalogResource::get`].
/// Each read returns the complete definition and an opaque version for conditional changes.
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
/// Use [`CatalogResource::apply`] to change a definition and
/// [`CatalogResource::delete`] to remove a resource. Both support previews and
/// version preconditions through [`CatalogOptions`].
#[derive(Debug, Clone)]
pub struct Catalog {
    resource: CatalogResource<CatalogDocument>,
}

impl Catalog {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            resource: CatalogResource::new(client, &["catalog"]),
        }
    }

    pub async fn get(&self) -> Result<CatalogSnapshot<CatalogDocument>, Error> {
        self.resource.get().await
    }

    /// Replaces the complete tenant catalog. An empty document clears it.
    pub async fn apply(
        &self,
        definition: &CatalogDocument,
        options: CatalogOptions,
    ) -> Result<CatalogResult<CatalogDocument>, Error> {
        self.resource.apply(definition, options).await
    }

    /// Retains fields unknown to this SDK when reading and applying definitions.
    pub fn raw(&self) -> CatalogResource<serde_json::Value> {
        self.resource.raw()
    }

    pub fn database(&self, database: &str) -> CatalogResource<CatalogDatabase> {
        CatalogResource::new(
            self.resource.client.clone(),
            &["catalog", "databases", database],
        )
    }
    pub fn schema(&self, database: &str, schema: &str) -> CatalogResource<CatalogSchema> {
        CatalogResource::new(
            self.resource.client.clone(),
            &["catalog", "databases", database, "schemas", schema],
        )
    }
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
}

/// A selected subtree. Names are unescaped identifiers. Apply replaces the whole
/// definition; it is not a patch. Mutations never retry automatically.
///
/// An interrupted or malformed mutation response reports
/// `catalog_commit_outcome_unknown`. Read the current state before deciding
/// whether to retry. Typed definitions reject unsupported fields; [`Self::raw`]
/// preserves them, including arbitrary-precision JSON numbers.
#[derive(Debug, Clone)]
pub struct CatalogResource<T> {
    client: Client,
    path: Vec<String>,
    definition: PhantomData<T>,
}

impl<T> CatalogResource<T> {
    fn new(client: Client, path: &[&str]) -> Self {
        Self {
            client,
            path: path.iter().map(|s| (*s).to_string()).collect(),
            definition: PhantomData,
        }
    }

    pub fn raw(&self) -> CatalogResource<serde_json::Value> {
        CatalogResource {
            client: self.client.clone(),
            path: self.path.clone(),
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
        if let Some(version) = options.expected_version {
            let version = reqwest::header::HeaderValue::from_str(&version).map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "invalid expected catalog version")
                    .set_source(e)
            })?;
            request = request.header(reqwest::header::IF_MATCH, version);
        }
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let mut response_metadata = None;
        let read_response = async {
            let response = request.send().await.map_err(|e| {
                catalog_transport_error(writing, "failed to send catalog request").set_source(e)
            })?;
            let status = response.status();
            let headers = response.headers().clone();
            response_metadata = Some((status, headers.clone()));
            let bytes = response.bytes().await.map_err(|e| {
                apply_response_metadata(
                    catalog_transport_error(writing, "failed to read catalog response")
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
                    let error = catalog_transport_error(writing, "catalog request timed out")
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
            let error = decode_http_error(status, &headers, &bytes, ErrorKind::CatalogFailed);
            return Err(if writing {
                if error.code().is_none() {
                    error
                        .set_api_error(Some("catalog_commit_outcome_unknown".to_string()), None)
                        .set_persistent()
                } else {
                    error.set_persistent()
                }
            } else {
                error
            });
        }
        let version = headers
            .get(reqwest::header::ETAG)
            .and_then(|h| h.to_str().ok())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                apply_response_metadata(
                    catalog_response_error(writing, "catalog response is missing a version"),
                    status,
                    &headers,
                )
            })?
            .to_string();
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
            apply_response_metadata(
                catalog_response_error(writing, "invalid catalog response").set_source(e),
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
                catalog_response_error(writing, "invalid catalog response"),
                status,
                &headers,
            ));
        }
        let result = serde_json::from_value(value).map_err(|e| {
            apply_response_metadata(
                catalog_response_error(
                    writing,
                    "cannot decode catalog definition; use raw() to retain unsupported fields",
                )
                .set_source(e),
                status,
                &headers,
            )
        })?;
        Ok((result, version))
    }
}

impl<T: Serialize + DeserializeOwned> CatalogResource<T> {
    /// Reads the complete definition and its version.
    pub async fn get(&self) -> Result<CatalogSnapshot<T>, Error> {
        let (definition, version) = self
            .request(Method::GET, None, CatalogOptions::default())
            .await?;
        Ok(CatalogSnapshot {
            definition,
            version,
        })
    }

    /// Replaces the selected definition, including its children. Omitted fields
    /// are cleared. Start from a snapshot to retain fields you are not changing.
    ///
    /// Preview with `dry_run` and use `expected_version` to reject concurrent changes.
    /// Set `allow_destructive` only after reviewing the potential data loss.
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
    ///             expected_version: Some(snapshot.version),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    /// println!("destructive = {}", preview.destructive);
    /// resource
    ///     .apply(
    ///         &definition,
    ///         CatalogOptions {
    ///             expected_version: Some(preview.version),
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
                ErrorKind::ConfigInvalid,
                "failed to serialize catalog definition",
            )
            .set_source(e)
        })?;
        let (mut result, version): (CatalogResult<T>, _) =
            self.request(Method::PUT, Some(body), options).await?;
        result.version = version;
        Ok(result)
    }

    /// Deletes the selected resource. Use `dry_run` to preview the change and
    /// `expected_version` to require the version from a prior read or preview.
    pub async fn delete(&self, options: CatalogOptions) -> Result<CatalogResult<T>, Error> {
        if self.path.len() == 1 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "the root catalog does not support DELETE; apply an empty document instead",
            ));
        }
        let (mut result, version): (CatalogResult<T>, _) =
            self.request(Method::DELETE, None, options).await?;
        result.version = version;
        Ok(result)
    }
}

fn catalog_transport_error(writing: bool, message: &str) -> Error {
    let error = catalog_response_error(writing, message);
    if writing {
        error
    } else {
        error.set_temporary()
    }
}

fn catalog_response_error(writing: bool, message: &str) -> Error {
    let error = Error::new(ErrorKind::CatalogFailed, message);
    if writing {
        error
            .set_api_error(Some("catalog_commit_outcome_unknown".to_string()), None)
            .set_persistent()
    } else {
        error
    }
}
