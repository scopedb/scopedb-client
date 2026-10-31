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

#![doc = include_str!("../README.md")]

#[doc = include_str!("../docs/migration-0.4.md")]
pub mod migration {}

mod append;
mod append_stream;
mod append_writer;
mod catalog;
mod client;
mod error;
mod protocol;
mod result;
mod statement;
mod table;
#[cfg(test)]
mod test_support;

/// The HTTP client version used by this SDK.
pub use reqwest;

pub use self::append::AppendDeliveryOutcome;
pub use self::append::AppendFailurePolicy;
pub use self::append_stream::AppendAdmissionError;
pub use self::append_stream::AppendAdmissionResult;
pub use self::append_stream::AppendBatchFailureAction;
pub use self::append_stream::AppendBatchFailureEvent;
pub use self::append_stream::AppendCircuitBreakerOptions;
pub use self::append_stream::AppendCircuitState;
pub use self::append_stream::AppendDeliveryReport;
pub use self::append_stream::AppendDroppedRows;
pub use self::append_stream::AppendLastFailure;
pub use self::append_stream::AppendStream;
pub use self::append_stream::AppendStreamBuilder;
pub use self::append_stream::AppendStreamState;
pub use self::append_stream::AppendStreamStats;
pub use self::append_stream::AppendTrySendError;
pub use self::append_writer::AppendRequestResult;
pub use self::append_writer::AppendWriteError;
pub use self::append_writer::AppendWriteReport;
pub use self::append_writer::AppendWriter;
pub use self::append_writer::AppendWriterBuilder;
pub use self::catalog::Catalog;
pub use self::catalog::CatalogColumn;
pub use self::catalog::CatalogColumnIndex;
pub use self::catalog::CatalogDatabase;
pub use self::catalog::CatalogDistinct;
pub use self::catalog::CatalogDocument;
pub use self::catalog::CatalogOptions;
pub use self::catalog::CatalogResource;
pub use self::catalog::CatalogResult;
pub use self::catalog::CatalogSchema;
pub use self::catalog::CatalogSnapshot;
pub use self::catalog::CatalogTable;
pub use self::catalog::CatalogView;
pub use self::client::Client;
pub use self::client::ClientBuilder;
pub use self::error::Error;
pub use self::error::ErrorKind;
pub use self::error::WriteErrorDetails;
pub use self::error::WriteErrorReason;
pub use self::protocol::AppendErrorDetails;
pub use self::protocol::AppendRowError;
pub use self::protocol::AppendRowsResult;
pub use self::protocol::AppendState;
pub use self::protocol::DataType;
pub use self::protocol::StatementErrorDetails;
pub use self::protocol::StatementEstimatedProgress;
pub use self::protocol::StatementProgress;
pub use self::protocol::StatementStatus;
pub use self::protocol::StatementStatusCancelled;
pub use self::protocol::StatementStatusFailed;
pub use self::protocol::StatementStatusFinished;
pub use self::protocol::StatementStatusPending;
pub use self::protocol::StatementStatusRunning;
pub use self::result::FieldSchema;
pub use self::result::ResultObject;
pub use self::result::ResultSet;
pub use self::result::Schema;
pub use self::result::Value;
pub use self::statement::Statement;
pub use self::statement::StatementHandle;
pub use self::table::Table;
