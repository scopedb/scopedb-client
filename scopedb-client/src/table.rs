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

use crate::AppendRowsResult;
use crate::AppendStreamBuilder;
use crate::AppendWriterBuilder;
use crate::CatalogResource;
use crate::CatalogTable;
use crate::Client;
use crate::Error;

/// A table selected for writes or catalog reads.
///
/// The table must already exist for writes. Database and schema default to
/// `scopedb` and `public`; override them with [`Self::with_database`] and
/// [`Self::with_schema`].
#[derive(Debug, Clone)]
pub struct Table {
    client: Client,
    database: String,
    schema: String,
    table: String,
}

impl Table {
    pub(crate) fn new(client: Client, table: String) -> Self {
        Self {
            client,
            database: "scopedb".to_string(),
            schema: "public".to_string(),
            table,
        }
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn identifier(&self) -> String {
        [
            self.database.as_str(),
            self.schema.as_str(),
            self.table.as_str(),
        ]
        .map(quote_ident)
        .join(".")
    }

    pub async fn drop(&self) -> Result<(), Error> {
        self.client
            .statement(format!("DROP TABLE {}", self.identifier()))
            .execute()
            .await
            .map(|_| ())
    }

    /// Writes newline-delimited JSON to this table in one HTTP attempt.
    ///
    /// Each line must be a JSON object matching the destination columns. A request
    /// allows at most 8 MiB of uncompressed NDJSON and 200,000 rows. The SDK sends
    /// it with zstd compression and does not automatically retry it.
    ///
    /// Integer inputs must fit in a signed 64-bit integer, including values in
    /// `uint` columns and nested JSON. Larger integers are rejected without
    /// narrowing; query results still support the full `u64` range.
    ///
    /// ```no_run
    /// # async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
    /// let result = client
    ///     .table("events")
    ///     .append(r#"{"id": 1, "name": "page_view"}"#)
    ///     .await?;
    /// println!("Written: {} rows", result.num_rows_inserted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn append(&self, ndjson: impl Into<String>) -> Result<AppendRowsResult, Error> {
        self.client
            .append_rows(&self.database, &self.schema, &self.table, ndjson)
            .await
    }

    /// Builds a reusable writer for finite input slices with per-batch input ranges.
    pub fn append_batch(&self) -> AppendWriterBuilder {
        AppendWriterBuilder::new(self.clone())
    }

    /// Builds an asynchronous, bounded and concurrent append stream for this table.
    pub fn append_stream(&self) -> AppendStreamBuilder {
        AppendStreamBuilder::new(self.clone())
    }

    /// Selects this table's complete definition and ETag for conditional changes.
    pub fn catalog(&self) -> CatalogResource<CatalogTable> {
        self.client
            .catalog()
            .table(&self.database, &self.schema, &self.table)
    }

    pub(crate) fn validate_append(&self) -> Result<(), Error> {
        self.client.make_resource_url(&[
            "catalog",
            "databases",
            &self.database,
            "schemas",
            &self.schema,
            "tables",
            &self.table,
            "ingest",
        ])?;
        Ok(())
    }

    pub(crate) async fn append_with_timeout(
        &self,
        payload: String,
        timeout: Option<Duration>,
    ) -> Result<AppendRowsResult, Error> {
        self.client
            .append_rows_with_timeout(&self.database, &self.schema, &self.table, payload, timeout)
            .await
    }
}

fn quote_ident(input: &str) -> String {
    let quote = '`';
    let mut out = String::with_capacity(input.len() + 2);
    out.push(quote);
    for ch in input.chars() {
        match ch {
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\\' => out.push_str("\\\\"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if c < '\u{20}' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MockResponse;
    use crate::test_support::MockServer;

    #[tokio::test]
    async fn writers_and_catalog_keep_the_selected_table_and_base_path() {
        let server = MockServer::start(|_, request| {
            if request.target.ends_with("/ingest") {
                MockResponse::committed(request)
            } else {
                MockResponse::json(200, r#"{"name":"events/blue","columns":[]}"#)
                    .with_header("ETag", "\"version\"")
            }
        });
        let client = Client::builder(format!("{}/proxy/", server.endpoint))
            .http_client(reqwest::Client::builder().no_proxy().build().unwrap())
            .build()
            .unwrap();
        let table = client
            .table("events/blue")
            .with_database("analytics")
            .with_schema("archive");
        let writer = table.append_batch().build().unwrap();
        let stream = table.append_stream().build().unwrap();
        let catalog = table.catalog();
        table.append(r#"{"id":1}"#).await.unwrap();
        // A later selector change must not retarget an already built writer.
        drop(table.with_schema("public"));
        let report = writer.write(&[serde_json::json!({"id":2})]).await.unwrap();
        assert_eq!(report.committed_rows(), 1);
        stream.send(&serde_json::json!({"id":3})).await.unwrap();
        stream.shutdown().await.unwrap();
        catalog.get().await.unwrap();

        let target = "/proxy/v1/catalog/databases/analytics/schemas/archive/tables/events%2Fblue";
        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        for request in &requests[..3] {
            assert_eq!(request.target, format!("{target}/ingest"));
        }
        assert_eq!(requests[3].target, target);
    }

    #[test]
    fn identifiers_always_use_the_same_defaults_as_append_and_catalog() {
        let client = crate::Client::builder("https://example.invalid")
            .build()
            .unwrap();
        assert_eq!(
            client.table("events").identifier(),
            "`scopedb`.`public`.`events`"
        );
        assert_eq!(
            client
                .table("events")
                .with_database("analytics")
                .identifier(),
            "`analytics`.`public`.`events`"
        );
        assert_eq!(
            client.table("events").with_schema("archive").identifier(),
            "`scopedb`.`archive`.`events`"
        );
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("plain"), "`plain`");
        assert_eq!(quote_ident("a`b"), "`a\\`b`");
        assert_eq!(quote_ident("a\nb"), "`a\\nb`");
    }
}
