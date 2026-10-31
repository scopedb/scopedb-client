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

const DEFAULT_DATABASE: &str = "scopedb";
const DEFAULT_SCHEMA: &str = "public";

/// Reference to a table for appending rows or managing its catalog definition.
///
/// The database and schema default to `scopedb` and `public`; override them with
/// [`with_database`](Self::with_database) and [`with_schema`](Self::with_schema).
#[derive(Debug, Clone)]
pub struct Table {
    client: Client,
    database: String,
    schema: String,
    table: String,
}

impl Table {
    /// Overrides the database name (defaults to `scopedb`).
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    /// Overrides the schema name (defaults to `public`).
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Returns the backtick-quoted `` `database`.`schema`.`table` `` identifier for ScopeQL
    /// statements.
    pub fn identifier(&self) -> String {
        [
            self.database.as_str(),
            self.schema.as_str(),
            self.table.as_str(),
        ]
        .map(quote_ident)
        .join(".")
    }

    /// Executes `DROP TABLE` for this table.
    pub async fn drop(&self) -> Result<(), Error> {
        self.client
            .statement(format!("DROP TABLE {}", self.identifier()))
            .execute()
            .await
            .map(|_| ())
    }

    /// Appends newline-delimited JSON rows to an existing table in one HTTP attempt.
    ///
    /// Each non-empty line must be a JSON object matching the table schema, up to
    /// 8 MiB of uncompressed NDJSON and 200,000 rows. Integer values in NDJSON
    /// input must fit in `i64` (including `uint` columns and nested JSON); query
    /// results support the full `u64` range.
    ///
    /// On failure, inspect [`Error::append_details`] for [`AppendState`](crate::AppendState).
    /// When the status is [`AppendState::Unknown`](crate::AppendState::Unknown), reconcile before
    /// replaying the payload.
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

    /// Returns an [`AppendWriterBuilder`] for writing finite slices of rows with per-batch progress
    /// reports.
    pub fn append_writer(&self) -> AppendWriterBuilder {
        AppendWriterBuilder::new(self.clone())
    }

    /// Returns an [`AppendStreamBuilder`] for buffered, concurrent background streaming.
    pub fn append_stream(&self) -> AppendStreamBuilder {
        AppendStreamBuilder::new(self.clone())
    }

    /// Selects this table's [`CatalogResource`] for reading, updating, or deleting its schema
    /// definition.
    pub fn catalog(&self) -> CatalogResource<CatalogTable> {
        self.client
            .catalog()
            .table(&self.database, &self.schema, &self.table)
    }

    pub(crate) fn new(client: Client, table: String) -> Self {
        Self {
            client,
            database: DEFAULT_DATABASE.to_string(),
            schema: DEFAULT_SCHEMA.to_string(),
            table,
        }
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
