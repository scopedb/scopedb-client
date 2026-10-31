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

use crate::CatalogTable;
use crate::Client;
use crate::Error;
use crate::append_stream::AppendStreamBuilder;
use crate::protocol::AppendRowsResult;

/// A table selected for writes or catalog reads.
///
/// The table must already exist for writes. Database and schema default to
/// `scopedb` and `public`; override them with [`Self::with_database`] and
/// [`Self::with_schema`].
#[derive(Debug, Clone)]
pub struct Table {
    client: Client,
    database: Option<String>,
    schema: Option<String>,
    table: String,
}

impl Table {
    pub(crate) fn new(client: Client, table: String) -> Self {
        Self {
            client,
            database: None,
            schema: None,
            table,
        }
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn identifier(&self) -> String {
        [
            self.database.as_deref().unwrap_or("scopedb"),
            self.schema.as_deref().unwrap_or("public"),
            &self.table,
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
            .append_rows(
                self.database.as_deref().unwrap_or("scopedb"),
                self.schema.as_deref().unwrap_or("public"),
                &self.table,
                ndjson,
            )
            .await
    }

    /// Builds a reusable writer for finite input slices with per-batch input ranges.
    pub fn append_batch(&self) -> crate::AppendBatchBuilder {
        crate::AppendBatchBuilder::new(
            self.client.clone(),
            self.database
                .clone()
                .unwrap_or_else(|| "scopedb".to_string()),
            self.schema.clone().unwrap_or_else(|| "public".to_string()),
            self.table.clone(),
        )
    }

    /// Builds an asynchronous, bounded and concurrent append stream for this table.
    pub fn append_stream(&self) -> AppendStreamBuilder {
        AppendStreamBuilder::new(
            self.client.clone(),
            self.database
                .clone()
                .unwrap_or_else(|| "scopedb".to_string()),
            self.schema.clone().unwrap_or_else(|| "public".to_string()),
            self.table.clone(),
        )
    }

    /// Selects this table's complete definition and ETag for conditional changes.
    pub fn catalog(&self) -> crate::CatalogResource<CatalogTable> {
        self.client.catalog().table(
            self.database.as_deref().unwrap_or("scopedb"),
            self.schema.as_deref().unwrap_or("public"),
            &self.table,
        )
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
    use super::quote_ident;

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
